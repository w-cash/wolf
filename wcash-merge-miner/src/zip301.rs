//! Stock-ASIC compatible ZIP-301 frontend for a proposal-validated native job.

use std::{
    collections::{HashMap, VecDeque},
    io::{BufRead, BufReader, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    sync::{
        atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    thread,
    time::{Duration, Instant},
};

use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use wcash_zcash_aux::{AuxPowError, Target};

use crate::{
    accounting::{AuthenticatedWorker, WorkerAuthenticator},
    MinerError, NativePreparedJob, ValidatedNativeShare, EQUIHASH_SOLUTION_BYTES,
};

/// Maximum ZIP-301 request frame accepted from one ASIC.
pub const MAX_ZIP301_REQUEST_BYTES: usize = 64 * 1024;

/// Default maximum number of simultaneous local ASIC sessions.
pub const DEFAULT_ZIP301_CLIENT_LIMIT: usize = 256;

/// Default maximum number of shares that may be validated and committed concurrently.
pub const DEFAULT_ZIP301_VALIDATION_LIMIT: usize = 4;

/// Default maximum number of simultaneous memory-hard worker authentications.
pub const DEFAULT_ZIP301_AUTHENTICATION_LIMIT: usize = 4;

const NONCE_1_BYTES: usize = 4;
const NONCE_2_BYTES: usize = 32 - NONCE_1_BYTES;
const SOLUTION_PREFIX: [u8; 3] = [0xfd, 0x40, 0x05];
const MAX_SHARES_PER_JOB: usize = 100_000;
const MAX_NETWORK_WINNERS_PER_JOB: usize = MAX_SHARES_PER_JOB;
const MAX_AUTHORIZED_WORKERS_PER_CONNECTION: usize = 16;
const MAX_SUBMISSIONS_PER_SECOND_PER_CONNECTION: usize = 64;
const MAX_AUTHORIZATIONS_PER_MINUTE_PER_CONNECTION: usize = 32;
const MAX_AUTHORIZATIONS_PER_CONNECTION: usize = 64;
const MAX_ZIP301_VALIDATION_LIMIT: usize = 1_024;
const MAX_ZIP301_AUTHENTICATION_LIMIT: usize = 256;
const CLIENT_IO_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_FRAME_ASSEMBLY_TIME: Duration = Duration::from_secs(10);
const ACCEPT_RETRY_DELAY: Duration = Duration::from_millis(5);
const VALIDATION_RETRY_DELAY: Duration = Duration::from_millis(1);
const JOB_MONITOR_INTERVAL: Duration = Duration::from_secs(1);
const SUBMISSION_RATE_WINDOW: Duration = Duration::from_secs(1);
const AUTHORIZATION_RATE_WINDOW: Duration = Duration::from_secs(60);

/// Callback that durably accounts for a valid share and queues any winners.
///
/// Returning an error rejects the share. Implementations should persist share
/// accounting before returning success. Winner submission belongs in the
/// health-monitor path so an unavailable node cannot delay the ASIC ACK;
/// Wcash and Zcash submissions must remain independent and idempotent.
pub trait ShareProcessor: Send + Sync + 'static {
    /// Processes one Equihash-valid, target-valid share.
    fn process(&self, worker: &str, share: &ValidatedNativeShare) -> Result<(), MinerError>;

    /// Processes a share with its canonical authenticated identity and provenance.
    fn process_authenticated(
        &self,
        worker: &AuthenticatedWorker,
        share: &ValidatedNativeShare,
    ) -> Result<(), MinerError> {
        self.process(worker.name(), share)
    }

    /// Fails when this listener's frozen job should be retired immediately.
    fn check_job_health(&self) -> Result<(), MinerError> {
        Ok(())
    }
}

impl<F> ShareProcessor for F
where
    F: Fn(&str, &ValidatedNativeShare) -> Result<(), MinerError> + Send + Sync + 'static,
{
    fn process(&self, worker: &str, share: &ValidatedNativeShare) -> Result<(), MinerError> {
        self(worker, share)
    }
}

/// Security and difficulty policy for one ZIP-301 listener.
#[derive(Clone)]
pub struct Zip301Config {
    share_target: Target,
    authentication: Zip301Authentication,
    maximum_clients: usize,
    maximum_parallel_authentications: usize,
    maximum_parallel_validations: usize,
}

#[derive(Clone)]
enum Zip301Authentication {
    SharedPassword([u8; 32]),
    ExactWorkers(Arc<dyn WorkerAuthenticator>),
}

impl Zip301Config {
    /// Creates a password-protected fixed-difficulty listener configuration.
    pub fn new(share_target: Target, password: &str) -> Result<Self, MinerError> {
        if password.len() < 12 || password.len() > 1_024 {
            return Err(MinerError::InvalidRequest(
                "ZIP-301 password must contain 12..=1024 bytes".to_string(),
            ));
        }
        Ok(Self {
            share_target,
            authentication: Zip301Authentication::SharedPassword(
                Sha256::digest(password.as_bytes()).into(),
            ),
            maximum_clients: DEFAULT_ZIP301_CLIENT_LIMIT,
            maximum_parallel_authentications: DEFAULT_ZIP301_AUTHENTICATION_LIMIT,
            maximum_parallel_validations: DEFAULT_ZIP301_VALIDATION_LIMIT,
        })
    }

    /// Creates a fixed-difficulty listener with independently authenticated workers.
    pub fn new_with_worker_authenticator(
        share_target: Target,
        authenticator: Arc<dyn WorkerAuthenticator>,
    ) -> Self {
        Self {
            share_target,
            authentication: Zip301Authentication::ExactWorkers(authenticator),
            maximum_clients: DEFAULT_ZIP301_CLIENT_LIMIT,
            maximum_parallel_authentications: DEFAULT_ZIP301_AUTHENTICATION_LIMIT,
            maximum_parallel_validations: DEFAULT_ZIP301_VALIDATION_LIMIT,
        }
    }

    /// Sets a strict positive simultaneous-session limit.
    pub fn with_maximum_clients(mut self, maximum_clients: usize) -> Result<Self, MinerError> {
        if maximum_clients == 0 || maximum_clients > 10_000 {
            return Err(MinerError::InvalidRequest(
                "ZIP-301 client limit must be in 1..=10000".to_string(),
            ));
        }
        self.maximum_clients = maximum_clients;
        Ok(self)
    }

    /// Sets a strict positive limit on concurrent validation and durable processing.
    pub fn with_maximum_parallel_validations(
        mut self,
        maximum_parallel_validations: usize,
    ) -> Result<Self, MinerError> {
        if maximum_parallel_validations == 0
            || maximum_parallel_validations > MAX_ZIP301_VALIDATION_LIMIT
        {
            return Err(MinerError::InvalidRequest(format!(
                "ZIP-301 validation limit must be in 1..={MAX_ZIP301_VALIDATION_LIMIT}"
            )));
        }
        self.maximum_parallel_validations = maximum_parallel_validations;
        Ok(self)
    }

    /// Sets a strict bound on concurrent memory-hard worker authentications.
    pub fn with_maximum_parallel_authentications(
        mut self,
        maximum_parallel_authentications: usize,
    ) -> Result<Self, MinerError> {
        if maximum_parallel_authentications == 0
            || maximum_parallel_authentications > MAX_ZIP301_AUTHENTICATION_LIMIT
        {
            return Err(MinerError::InvalidRequest(format!(
                "ZIP-301 authentication limit must be in 1..={MAX_ZIP301_AUTHENTICATION_LIMIT}"
            )));
        }
        self.maximum_parallel_authentications = maximum_parallel_authentications;
        Ok(self)
    }

    /// Returns the configured share target.
    pub const fn share_target(&self) -> Target {
        self.share_target
    }

    /// Returns true when each exact worker has an independent credential.
    pub const fn uses_worker_authenticator(&self) -> bool {
        matches!(&self.authentication, Zip301Authentication::ExactWorkers(_))
    }
}

impl std::fmt::Debug for Zip301Config {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Zip301Config")
            .field("share_target", &self.share_target)
            .field(
                "authentication",
                &match &self.authentication {
                    Zip301Authentication::SharedPassword(_) => "shared-password [REDACTED]",
                    Zip301Authentication::ExactWorkers(_) => "exact-worker credentials [REDACTED]",
                },
            )
            .field("maximum_clients", &self.maximum_clients)
            .field(
                "maximum_parallel_authentications",
                &self.maximum_parallel_authentications,
            )
            .field(
                "maximum_parallel_validations",
                &self.maximum_parallel_validations,
            )
            .finish()
    }
}

/// A loopback ZIP-301 socket that remains bound across sequential job generations.
///
/// Keeping the owning listener open prevents a normal job rotation from having
/// to rebind a port that can still have accepted connections in `TIME_WAIT`.
/// Exactly one generation may accept connections from this listener at a time.
pub struct Zip301LoopbackListener {
    listener: TcpListener,
    generation_active: AtomicBool,
}

impl Zip301LoopbackListener {
    /// Binds a persistent, nonblocking listener to a literal loopback address.
    pub fn bind(bind: SocketAddr) -> Result<Self, MinerError> {
        if !bind.ip().is_loopback() {
            return Err(MinerError::InvalidRequest(
                "the built-in ZIP-301 listener only binds loopback addresses".to_string(),
            ));
        }

        let listener = TcpListener::bind(bind)?;
        listener.set_nonblocking(true)?;
        Ok(Self {
            listener,
            generation_active: AtomicBool::new(false),
        })
    }

    /// Returns the effective bound address, including an OS-assigned port.
    pub fn local_addr(&self) -> Result<SocketAddr, MinerError> {
        self.listener.local_addr().map_err(MinerError::from)
    }

    /// Serves one frozen job while retaining the bound socket for the next generation.
    pub fn serve(
        &self,
        job: NativePreparedJob,
        config: Zip301Config,
        processor: Arc<dyn ShareProcessor>,
    ) -> Result<(), MinerError> {
        let (listener, _generation) = self.begin_generation()?;
        serve_zip301_generation(listener, job, config, processor)
    }

    fn begin_generation(&self) -> Result<(TcpListener, Zip301GenerationGuard<'_>), MinerError> {
        self.generation_active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| {
                MinerError::InvalidRequest(
                    "the ZIP-301 listener already has an active job generation".to_string(),
                )
            })?;
        let generation = Zip301GenerationGuard {
            active: &self.generation_active,
        };
        let listener = self.listener.try_clone()?;
        Ok((listener, generation))
    }
}

impl std::fmt::Debug for Zip301LoopbackListener {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Zip301LoopbackListener")
            .field("local_addr", &self.listener.local_addr())
            .field(
                "generation_active",
                &self.generation_active.load(Ordering::Acquire),
            )
            .finish()
    }
}

struct Zip301GenerationGuard<'a> {
    active: &'a AtomicBool,
}

impl Drop for Zip301GenerationGuard<'_> {
    fn drop(&mut self) {
        self.active.store(false, Ordering::Release);
    }
}

/// Serves one frozen native job over ZIP-301 for ASIC interoperability testing.
///
/// The built-in listener deliberately accepts only a loopback bind. Community
/// deployments should put an authenticated, rate-limited TCP/TLS edge in front
/// of it and rotate jobs through a supervisor whenever either chain tip changes.
/// Supervisors must bind [`Zip301LoopbackListener`] once and call its
/// [`Zip301LoopbackListener::serve`] method for each sequential generation;
/// this convenience function owns the socket for one generation only.
pub fn serve_zip301_loopback(
    bind: SocketAddr,
    job: NativePreparedJob,
    config: Zip301Config,
    processor: Arc<dyn ShareProcessor>,
) -> Result<(), MinerError> {
    Zip301LoopbackListener::bind(bind)?.serve(job, config, processor)
}

fn serve_zip301_generation(
    listener: TcpListener,
    job: NativePreparedJob,
    config: Zip301Config,
    processor: Arc<dyn ShareProcessor>,
) -> Result<(), MinerError> {
    validate_share_target(
        config.share_target,
        job.job().required_target(),
        job.parent_target(),
    )?;
    let maximum_clients = config.maximum_clients;
    let maximum_parallel_authentications = config.maximum_parallel_authentications;
    let maximum_parallel_validations = config.maximum_parallel_validations;
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_reason = Arc::new(Mutex::new(None));
    let state = Arc::new(ServerState {
        job: Arc::new(job),
        config,
        processor,
        next_nonce: AtomicU32::new(1),
        duplicates: Mutex::new(DuplicateCache::new(maximum_clients)),
        authentications: ConnectionLimiter::new(maximum_parallel_authentications),
        validations: ConnectionLimiter::new(maximum_parallel_validations),
        shutdown: Arc::clone(&shutdown),
    });
    let monitor_state = Arc::clone(&state);
    let monitor_shutdown = Arc::clone(&shutdown);
    let monitor_reason = Arc::clone(&shutdown_reason);
    let monitor = thread::Builder::new()
        .name("wcash-job-monitor".to_string())
        .spawn(move || {
            while !monitor_shutdown.load(Ordering::Acquire) {
                thread::sleep(JOB_MONITOR_INTERVAL);
                if monitor_shutdown.load(Ordering::Acquire) {
                    break;
                }
                if let Err(error) = monitor_state.processor.check_job_health() {
                    *monitor_reason
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(error);
                    monitor_shutdown.store(true, Ordering::Release);
                    break;
                }
            }
        })?;
    let result = serve_listener(
        listener,
        state,
        ConnectionLimiter::new(maximum_clients),
        Arc::clone(&shutdown),
    );
    shutdown.store(true, Ordering::Release);
    monitor
        .join()
        .map_err(|_| MinerError::InvalidRequest("native job monitor panicked".to_string()))?;
    result?;
    if let Some(error) = shutdown_reason
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .take()
    {
        return Err(error);
    }
    Ok(())
}

fn validate_share_target(
    share_target: Target,
    child_target: Target,
    parent_target: Target,
) -> Result<(), MinerError> {
    if !share_target.includes(child_target) || !share_target.includes(parent_target) {
        return Err(MinerError::InvalidRequest(
            "ZIP-301 share target must be at least as easy as both network targets".to_string(),
        ));
    }
    Ok(())
}

struct ServerState {
    job: Arc<NativePreparedJob>,
    config: Zip301Config,
    processor: Arc<dyn ShareProcessor>,
    next_nonce: AtomicU32,
    duplicates: Mutex<DuplicateCache>,
    authentications: ConnectionLimiter,
    validations: ConnectionLimiter,
    shutdown: Arc<AtomicBool>,
}

fn serve_listener(
    listener: TcpListener,
    state: Arc<ServerState>,
    limiter: ConnectionLimiter,
    shutdown: Arc<AtomicBool>,
) -> Result<(), MinerError> {
    let mut workers: Vec<thread::JoinHandle<()>> = Vec::new();
    let mut listener_result = Ok(());
    while !shutdown.load(Ordering::Acquire) {
        let mut index = 0;
        while index < workers.len() {
            if workers[index].is_finished() {
                let worker = workers.swap_remove(index);
                let _ = worker.join();
            } else {
                index += 1;
            }
        }

        let stream = match listener.accept() {
            Ok((stream, _)) => stream,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(ACCEPT_RETRY_DELAY);
                continue;
            }
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::Interrupted
                        | std::io::ErrorKind::ConnectionAborted
                        | std::io::ErrorKind::ConnectionReset
                        | std::io::ErrorKind::TimedOut
                ) =>
            {
                continue;
            }
            Err(error) => {
                listener_result = Err(error.into());
                break;
            }
        };
        if stream.set_nonblocking(false).is_err() {
            continue;
        }
        let Some(permit) = limiter.try_acquire() else {
            drop(stream);
            continue;
        };
        let state = Arc::clone(&state);
        let worker = match thread::Builder::new()
            .name("wcash-zip301-client".to_string())
            .spawn(move || {
                let _permit = permit;
                let _result = serve_connection(stream, state);
            }) {
            Ok(worker) => worker,
            Err(error) => {
                listener_result = Err(error.into());
                break;
            }
        };
        workers.push(worker);
    }

    // Do not let process shutdown interrupt an accepted winner between its
    // durable outbox write and either chain submission.
    for worker in workers {
        let _ = worker.join();
    }
    listener_result
}

fn serve_connection(mut stream: TcpStream, state: Arc<ServerState>) -> Result<(), MinerError> {
    stream.set_read_timeout(Some(CLIENT_IO_TIMEOUT))?;
    stream.set_write_timeout(Some(CLIENT_IO_TIMEOUT))?;
    let read_stream = stream.try_clone()?;
    let mut reader = BufReader::new(read_stream);
    let nonce_1 = allocate_session_nonce(&state.next_nonce)?;
    let mut subscribed = false;
    let mut authorized = HashMap::new();
    let mut authorization_policy = AuthorizationPolicy::new(Instant::now());
    let mut submission_rate = SubmissionRateLimiter::new(Instant::now());

    while !state.shutdown.load(Ordering::Acquire) {
        let frame = match read_frame(&mut reader) {
            Ok(Some(frame)) => frame,
            Ok(None) => break,
            Err(MinerError::Io(error))
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                continue;
            }
            Err(error) => return Err(error),
        };
        let request = match serde_json::from_slice::<Value>(&frame) {
            Ok(request) => request,
            Err(_) => {
                write_message(
                    &mut stream,
                    &rpc_error(Value::Null, 20, "invalid JSON request"),
                )?;
                continue;
            }
        };
        let object = match request.as_object() {
            Some(object) => object,
            None => {
                write_message(
                    &mut stream,
                    &rpc_error(Value::Null, 20, "request must be an object"),
                )?;
                continue;
            }
        };
        let id = object.get("id").cloned().unwrap_or(Value::Null);
        let method = object.get("method").and_then(Value::as_str).unwrap_or("");
        let params = object.get("params").unwrap_or(&Value::Null);

        match method {
            "mining.subscribe" => {
                subscribed = true;
                write_message(
                    &mut stream,
                    &rpc_success(id, json!([Value::Null, hex::encode(nonce_1)])),
                )?;
            }
            "mining.authorize" => {
                if !subscribed {
                    write_message(&mut stream, &rpc_error(id, 25, "not subscribed"))?;
                    continue;
                }
                match authorization_policy.admit(Instant::now()) {
                    AuthorizationAdmission::Allowed => {}
                    AuthorizationAdmission::RateLimited => {
                        write_message(
                            &mut stream,
                            &rpc_error(id, 24, "authorization rate limit exceeded"),
                        )?;
                        continue;
                    }
                    AuthorizationAdmission::LifetimeExhausted => {
                        write_message(
                            &mut stream,
                            &rpc_error(id, 24, "authorization attempt limit exceeded"),
                        )?;
                        break;
                    }
                }
                match authorize(params, &state.config, &state.authentications) {
                    Ok(worker) => match insert_authorized_worker(&mut authorized, worker) {
                        Ok(new_worker) => {
                            write_message(&mut stream, &rpc_success(id, Value::Bool(true)))?;
                            if new_worker {
                                write_message(&mut stream, &set_target(state.config.share_target))?;
                                write_message(&mut stream, &notify(&state.job, true))?;
                            }
                        }
                        Err(message) => {
                            write_message(&mut stream, &rpc_error(id, 24, message))?;
                        }
                    },
                    Err(message) => {
                        write_message(&mut stream, &rpc_error(id, 24, message))?;
                    }
                }
            }
            "mining.suggest_target" => {
                if !subscribed {
                    write_message(&mut stream, &rpc_error(id, 25, "not subscribed"))?;
                } else {
                    write_message(&mut stream, &rpc_success(id, Value::Bool(true)))?;
                    write_message(&mut stream, &set_target(state.config.share_target))?;
                }
            }
            "mining.extranonce.subscribe" => {
                // NiceHash-derived clients commonly probe this Bitcoin Stratum
                // extension after authorization. ZIP-301 puts the server nonce
                // prefix in the block-header nonce rather than the coinbase, and
                // this listener rotates it by reconnecting, so changing it on an
                // active connection is deliberately unsupported. Return the
                // extension's documented negative response instead of treating a
                // harmless capability probe as an unknown method.
                write_message(&mut stream, &unsupported_extension(id))?;
            }
            "mining.submit" => {
                let response = if !subscribed {
                    rpc_error(id, 25, "not subscribed")
                } else if !submission_rate.try_acquire(Instant::now()) {
                    rpc_error(id, 20, "submission rate limit exceeded")
                } else {
                    match submit(&state, &authorized, nonce_1, params) {
                        Ok(()) => rpc_success(id, Value::Bool(true)),
                        Err(error) => rpc_error(id, error.code, error.message),
                    }
                };
                write_message(&mut stream, &response)?;
            }
            _ => write_message(&mut stream, &rpc_error(id, 20, "unknown method"))?,
        }
    }
    Ok(())
}

fn authorize(
    params: &Value,
    config: &Zip301Config,
    authentications: &ConnectionLimiter,
) -> Result<WorkerAuthorization, &'static str> {
    let params = params
        .as_array()
        .ok_or("authorize params must be an array")?;
    if params.len() != 2 {
        return Err("authorize requires worker name and password");
    }
    let worker = params[0].as_str().ok_or("worker name must be a string")?;
    let password = params[1]
        .as_str()
        .ok_or("worker password must be a string")?;
    if worker.is_empty() || worker.len() > 128 || worker.bytes().any(|byte| byte.is_ascii_control())
    {
        return Err("worker name is invalid");
    }
    let _authentication_permit = authentications
        .try_acquire()
        .ok_or("authentication capacity is exhausted; retry later")?;
    let identity = match &config.authentication {
        Zip301Authentication::SharedPassword(password_hash) => {
            let supplied: [u8; 32] = Sha256::digest(password.as_bytes()).into();
            if !constant_time_eq(&supplied, password_hash) {
                return Err("authorization failed");
            }
            AuthenticatedWorker::from_shared_secret(worker).map_err(|_| "worker name is invalid")?
        }
        Zip301Authentication::ExactWorkers(authenticator) => authenticator
            .authenticate(worker, password)
            .ok_or("authorization failed")?,
    };
    Ok(WorkerAuthorization {
        requested_name: worker.to_string(),
        identity,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct WorkerAuthorization {
    requested_name: String,
    identity: AuthenticatedWorker,
}

fn insert_authorized_worker(
    authorized: &mut HashMap<String, AuthenticatedWorker>,
    authorization: WorkerAuthorization,
) -> Result<bool, &'static str> {
    if let Some(existing) = authorized.get(&authorization.requested_name) {
        return if existing == &authorization.identity {
            Ok(false)
        } else {
            Err("worker authorization changed during the connection")
        };
    }
    if authorized.len() >= MAX_AUTHORIZED_WORKERS_PER_CONNECTION {
        return Err("too many workers on one connection");
    }
    authorized.insert(authorization.requested_name, authorization.identity);
    Ok(true)
}

fn processor_worker_identity<'a>(
    authorized: &'a HashMap<String, AuthenticatedWorker>,
    requested_name: &str,
) -> Result<&'a AuthenticatedWorker, SubmitError> {
    authorized.get(requested_name).ok_or_else(|| SubmitError {
        code: 24,
        message: "worker is not authorized".to_string(),
    })
}

fn submit(
    state: &ServerState,
    authorized: &HashMap<String, AuthenticatedWorker>,
    nonce_1: [u8; NONCE_1_BYTES],
    params: &Value,
) -> Result<(), SubmitError> {
    let params = params
        .as_array()
        .filter(|params| params.len() == 5)
        .ok_or_else(|| SubmitError::other("submit requires five array parameters"))?;
    let worker = params[0]
        .as_str()
        .ok_or_else(|| SubmitError::other("worker name must be a string"))?;
    let processor_worker = processor_worker_identity(authorized, worker)?;
    let job_id = params[1]
        .as_str()
        .ok_or_else(|| SubmitError::other("job id must be a string"))?;
    if job_id != state.job.job().job_id() {
        return Err(SubmitError {
            code: 21,
            message: "stale or unknown job".to_string(),
        });
    }
    let time = decode_fixed_hex::<4>(
        params[2]
            .as_str()
            .ok_or_else(|| SubmitError::other("time must be hexadecimal"))?,
    )?;
    if time != state.job.job().parent_header_input()[100..104] {
        return Err(SubmitError::other(
            "submitted time differs from the frozen job",
        ));
    }
    let nonce_2 = decode_fixed_hex::<NONCE_2_BYTES>(
        params[3]
            .as_str()
            .ok_or_else(|| SubmitError::other("nonce_2 must be hexadecimal"))?,
    )?;
    let encoded_solution = decode_fixed_hex::<{ EQUIHASH_SOLUTION_BYTES + 3 }>(
        params[4]
            .as_str()
            .ok_or_else(|| SubmitError::other("solution must be hexadecimal"))?,
    )?;
    if encoded_solution[..3] != SOLUTION_PREFIX {
        return Err(SubmitError::other(
            "solution CompactSize prefix is not canonical",
        ));
    }

    let mut nonce = [0; 32];
    nonce[..NONCE_1_BYTES].copy_from_slice(&nonce_1);
    nonce[NONCE_1_BYTES..].copy_from_slice(&nonce_2);
    let solution = &encoded_solution[3..];
    let submitted_identity = Arc::new(SubmittedShareIdentity::new(job_id, time, nonce, solution));
    let parent_block_identity = state
        .job
        .job()
        .parent_header(&nonce, solution)
        .map_err(share_validation_error)?
        .block_hash()
        .into_le_bytes();
    let (share, mut replay_reservation) = validate_reserved_share(
        &state.duplicates,
        &state.validations,
        &state.shutdown,
        submitted_identity,
        parent_block_identity,
        || {
            state
                .job
                .validate_share(&nonce, solution, state.config.share_target)
                .map_err(share_validation_error)
        },
    )?;

    let validated_parent_identity = share.parent_block_hash().into_le_bytes();
    let is_network_winner = share.wcash_candidate().is_some() || share.parent_block().is_some();
    match replay_reservation.promote(validated_parent_identity, is_network_winner)? {
        DuplicateInsert::Inserted => {}
        DuplicateInsert::Duplicate => {
            return Err(SubmitError {
                code: 22,
                message: "duplicate share".to_string(),
            })
        }
        DuplicateInsert::Full => {
            return Err(SubmitError::other(
                "share replay cache is full; rotate the job before accepting more work",
            ))
        }
    }

    if let Err(error) = state
        .processor
        .process_authenticated(processor_worker, &share)
    {
        let code = if matches!(
            &error,
            MinerError::StaleNativeJob(_)
                | MinerError::ChildTipMismatch { .. }
                | MinerError::ParentTipMismatch { .. }
        ) {
            21
        } else {
            20
        };
        return Err(SubmitError {
            code,
            message: format!("share processing failed: {error}"),
        });
    }
    replay_reservation.retain();
    Ok(())
}

fn share_validation_error(error: MinerError) -> SubmitError {
    match error {
        MinerError::AuxPow(AuxPowError::InsufficientParentWork { .. }) => SubmitError {
            code: 23,
            message: "low difficulty share".to_string(),
        },
        other => SubmitError::other(format!("invalid share: {other}")),
    }
}

fn validate_reserved_share<'a, T, F>(
    duplicates: &'a Mutex<DuplicateCache>,
    validations: &ConnectionLimiter,
    shutdown: &AtomicBool,
    submitted_identity: Arc<SubmittedShareIdentity>,
    parent_block_identity: [u8; 32],
    validate: F,
) -> Result<(T, ReplayReservation<'a>), SubmitError>
where
    F: FnOnce() -> Result<T, SubmitError>,
{
    // Reserve the exact canonical submission before waiting for scarce CPU.
    // Replays therefore never occupy an Equihash-validation permit.
    let reservation =
        ReplayReservation::reserve(duplicates, submitted_identity, parent_block_identity)?;
    let validation_permit = loop {
        if let Some(permit) = validations.try_acquire() {
            break permit;
        }
        if shutdown.load(Ordering::Acquire) {
            return Err(SubmitError {
                code: 21,
                message: "active job was retired while waiting for validation".to_string(),
            });
        }
        thread::sleep(VALIDATION_RETRY_DELAY);
    };
    let result = validate();
    // Equihash validation is the CPU-heavy bounded operation. Release this
    // admission slot before serialized journal durability and winner RPCs so a
    // parent-node outage cannot starve validation or hide a later winner.
    drop(validation_permit);
    Ok((result?, reservation))
}

struct AuthorizationPolicy {
    attempts: usize,
    rate: AuthorizationRateLimiter,
}

impl AuthorizationPolicy {
    fn new(now: Instant) -> Self {
        Self {
            attempts: 0,
            rate: AuthorizationRateLimiter::new(now),
        }
    }

    fn admit(&mut self, now: Instant) -> AuthorizationAdmission {
        if self.attempts >= MAX_AUTHORIZATIONS_PER_CONNECTION {
            return AuthorizationAdmission::LifetimeExhausted;
        }
        self.attempts += 1;
        if self.rate.try_acquire(now) {
            AuthorizationAdmission::Allowed
        } else {
            AuthorizationAdmission::RateLimited
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AuthorizationAdmission {
    Allowed,
    RateLimited,
    LifetimeExhausted,
}

struct AuthorizationRateLimiter {
    attempts: VecDeque<Instant>,
}

impl AuthorizationRateLimiter {
    fn new(_now: Instant) -> Self {
        Self {
            attempts: VecDeque::with_capacity(MAX_AUTHORIZATIONS_PER_MINUTE_PER_CONNECTION),
        }
    }

    fn try_acquire(&mut self, now: Instant) -> bool {
        while self.attempts.front().is_some_and(|attempted| {
            now.saturating_duration_since(*attempted) >= AUTHORIZATION_RATE_WINDOW
        }) {
            self.attempts.pop_front();
        }
        if self.attempts.len() >= MAX_AUTHORIZATIONS_PER_MINUTE_PER_CONNECTION {
            return false;
        }
        self.attempts.push_back(now);
        true
    }
}

struct SubmissionRateLimiter {
    submissions: VecDeque<Instant>,
}

impl SubmissionRateLimiter {
    fn new(_now: Instant) -> Self {
        Self {
            submissions: VecDeque::with_capacity(MAX_SUBMISSIONS_PER_SECOND_PER_CONNECTION),
        }
    }

    fn try_acquire(&mut self, now: Instant) -> bool {
        while self.submissions.front().is_some_and(|submitted| {
            now.saturating_duration_since(*submitted) >= SUBMISSION_RATE_WINDOW
        }) {
            self.submissions.pop_front();
        }
        if self.submissions.len() >= MAX_SUBMISSIONS_PER_SECOND_PER_CONNECTION {
            return false;
        }
        self.submissions.push_back(now);
        true
    }
}

fn notify(job: &NativePreparedJob, clean_jobs: bool) -> Value {
    let input = job.job().parent_header_input();
    json!({
        "id": Value::Null,
        "method": "mining.notify",
        "params": [
            job.job().job_id(),
            hex::encode(&input[..4]),
            hex::encode(&input[4..36]),
            hex::encode(&input[36..68]),
            hex::encode(&input[68..100]),
            hex::encode(&input[100..104]),
            hex::encode(&input[104..108]),
            clean_jobs,
        ]
    })
}

fn set_target(target: Target) -> Value {
    let mut big_endian = target.to_le_bytes();
    big_endian.reverse();
    json!({
        "id": Value::Null,
        "method": "mining.set_target",
        "params": [hex::encode(big_endian)],
    })
}

fn rpc_success(id: Value, result: Value) -> Value {
    json!({"id": id, "result": result, "error": Value::Null})
}

fn rpc_error(id: Value, code: i32, message: impl Into<String>) -> Value {
    json!({"id": id, "result": Value::Null, "error": [code, message.into(), Value::Null]})
}

fn unsupported_extension(id: Value) -> Value {
    json!({"id": id, "result": false, "error": [20, "Not supported.", Value::Null]})
}

fn write_message(stream: &mut TcpStream, message: &Value) -> Result<(), MinerError> {
    serde_json::to_writer(&mut *stream, message)?;
    stream.write_all(b"\n")?;
    stream.flush()?;
    Ok(())
}

fn read_frame<R: BufRead>(reader: &mut R) -> Result<Option<Vec<u8>>, MinerError> {
    read_frame_until(reader, Instant::now() + MAX_FRAME_ASSEMBLY_TIME)
}

fn read_frame_until<R: BufRead>(
    reader: &mut R,
    deadline: Instant,
) -> Result<Option<Vec<u8>>, MinerError> {
    let mut frame = Vec::new();
    loop {
        if Instant::now() >= deadline {
            return Err(MinerError::InvalidRequest(format!(
                "ZIP-301 request frame exceeded the {}-second assembly deadline",
                MAX_FRAME_ASSEMBLY_TIME.as_secs()
            )));
        }
        let (chunk, consumed, finished) = {
            let available = reader.fill_buf()?;
            if available.is_empty() {
                return Ok((!frame.is_empty()).then_some(frame));
            }
            match available.iter().position(|byte| *byte == b'\n') {
                Some(position) => (
                    available[..position].to_vec(),
                    position.saturating_add(1),
                    true,
                ),
                None => (available.to_vec(), available.len(), false),
            }
        };
        if frame.len().saturating_add(chunk.len()) > MAX_ZIP301_REQUEST_BYTES {
            return Err(MinerError::RequestTooLarge(MAX_ZIP301_REQUEST_BYTES));
        }
        frame.extend_from_slice(&chunk);
        reader.consume(consumed);
        if finished {
            if frame.last() == Some(&b'\r') {
                frame.pop();
            }
            return Ok(Some(frame));
        }
    }
}

fn decode_fixed_hex<const N: usize>(encoded: &str) -> Result<[u8; N], SubmitError> {
    let bytes = hex::decode(encoded)
        .map_err(|error| SubmitError::other(format!("invalid hexadecimal field: {error}")))?;
    bytes.try_into().map_err(|bytes: Vec<u8>| {
        SubmitError::other(format!(
            "field decoded to {} bytes, expected {N}",
            bytes.len()
        ))
    })
}

fn allocate_session_nonce(counter: &AtomicU32) -> Result<[u8; NONCE_1_BYTES], MinerError> {
    let value = counter
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
            value.checked_add(1)
        })
        .map_err(|_| {
            MinerError::StaleNativeJob(
                "all unique ZIP-301 session nonce prefixes were exhausted; rotate the job"
                    .to_string(),
            )
        })?;
    Ok(value.to_le_bytes())
}

fn constant_time_eq(left: &[u8; 32], right: &[u8; 32]) -> bool {
    left.iter()
        .zip(right)
        .fold(0u8, |difference, (left, right)| difference | (left ^ right))
        == 0
}

#[derive(Debug, Eq, PartialEq)]
struct SubmittedShareIdentity {
    job_id: Box<str>,
    time: [u8; 4],
    nonce: [u8; 32],
    solution: Box<[u8; EQUIHASH_SOLUTION_BYTES]>,
}

impl SubmittedShareIdentity {
    fn new(job_id: &str, time: [u8; 4], nonce: [u8; 32], solution: &[u8]) -> Self {
        let solution = solution
            .try_into()
            .expect("the submitted solution has fixed length after fixed-size ZIP-301 decoding");
        Self {
            job_id: job_id.into(),
            time,
            nonce,
            solution: Box::new(solution),
        }
    }
}

struct DuplicateCache {
    entries: HashMap<[u8; 32], CachedShare>,
    in_flight: usize,
    accepted_shares: usize,
    network_winners: usize,
    maximum_in_flight: usize,
    next_reservation: u64,
}

struct CachedShare {
    reservation: u64,
    state: CachedShareState,
}

enum CachedShareState {
    // Exact canonical wire fields are retained until validation finishes, so
    // a failed pre-validation identity collision can be rolled back safely.
    InFlight(Arc<SubmittedShareIdentity>),
    Accepted,
    NetworkWinner,
}

impl DuplicateCache {
    fn new(maximum_in_flight: usize) -> Self {
        assert!(
            maximum_in_flight > 0,
            "in-flight share limit must be positive"
        );
        Self {
            entries: HashMap::new(),
            in_flight: 0,
            accepted_shares: 0,
            network_winners: 0,
            maximum_in_flight,
            next_reservation: 1,
        }
    }

    fn reserve(
        &mut self,
        submitted: Arc<SubmittedShareIdentity>,
        parent_block: [u8; 32],
    ) -> DuplicateReservation {
        if self.entries.contains_key(&parent_block) {
            return DuplicateReservation::Duplicate;
        }
        if self.in_flight >= self.maximum_in_flight {
            return DuplicateReservation::Full;
        }
        let Some(next_reservation) = self.next_reservation.checked_add(1) else {
            return DuplicateReservation::Full;
        };
        let reservation = self.next_reservation;
        self.next_reservation = next_reservation;
        self.entries.insert(
            parent_block,
            CachedShare {
                reservation,
                state: CachedShareState::InFlight(submitted),
            },
        );
        self.in_flight += 1;
        DuplicateReservation::Reserved(reservation)
    }

    fn promote(
        &mut self,
        submitted: &Arc<SubmittedShareIdentity>,
        reserved_parent_block: [u8; 32],
        validated_parent_block: [u8; 32],
        reservation: u64,
        is_network_winner: bool,
    ) -> Result<DuplicateInsert, &'static str> {
        if reserved_parent_block != validated_parent_block {
            return Err("validated parent identity differs from its replay reservation");
        }
        match self.entries.get(&validated_parent_block) {
            Some(CachedShare {
                reservation: current,
                state: CachedShareState::InFlight(current_submission),
            }) if current == &reservation && current_submission == submitted => {}
            Some(CachedShare {
                state: CachedShareState::Accepted | CachedShareState::NetworkWinner,
                ..
            }) => return Ok(DuplicateInsert::Duplicate),
            _ => return Err("share replay reservation was lost"),
        }
        if is_network_winner {
            if self.network_winners >= MAX_NETWORK_WINNERS_PER_JOB {
                return Ok(DuplicateInsert::Full);
            }
            self.network_winners += 1;
        } else {
            if self.accepted_shares >= MAX_SHARES_PER_JOB {
                return Ok(DuplicateInsert::Full);
            }
            self.accepted_shares += 1;
        }
        self.in_flight = self
            .in_flight
            .checked_sub(1)
            .expect("promotion removes one tracked in-flight reservation");
        self.entries
            .get_mut(&validated_parent_block)
            .expect("the promoted reservation was checked above")
            .state = if is_network_winner {
            CachedShareState::NetworkWinner
        } else {
            CachedShareState::Accepted
        };
        Ok(DuplicateInsert::Inserted)
    }

    fn rollback(
        &mut self,
        submitted: &Arc<SubmittedShareIdentity>,
        parent_block: [u8; 32],
        reservation: u64,
        state: ReplayReservationState,
    ) {
        let matches_reservation = match (self.entries.get(&parent_block), state) {
            (
                Some(CachedShare {
                    reservation: current,
                    state: CachedShareState::InFlight(current_submission),
                }),
                ReplayReservationState::InFlight,
            ) => current == &reservation && current_submission == submitted,
            (
                Some(CachedShare {
                    reservation: current,
                    state: CachedShareState::Accepted,
                }),
                ReplayReservationState::Accepted {
                    is_network_winner: false,
                },
            )
            | (
                Some(CachedShare {
                    reservation: current,
                    state: CachedShareState::NetworkWinner,
                }),
                ReplayReservationState::Accepted {
                    is_network_winner: true,
                },
            ) => current == &reservation,
            _ => false,
        };
        if !matches_reservation {
            return;
        }
        self.entries.remove(&parent_block);
        let counter = match state {
            ReplayReservationState::InFlight => &mut self.in_flight,
            ReplayReservationState::Accepted {
                is_network_winner: false,
            } => &mut self.accepted_shares,
            ReplayReservationState::Accepted {
                is_network_winner: true,
            } => &mut self.network_winners,
            ReplayReservationState::Retained => return,
        };
        *counter = counter
            .checked_sub(1)
            .expect("rollback removes one tracked replay-cache entry");
    }
}

struct ReplayReservation<'a> {
    cache: &'a Mutex<DuplicateCache>,
    submitted: Arc<SubmittedShareIdentity>,
    parent_block: [u8; 32],
    reservation: u64,
    state: ReplayReservationState,
}

impl<'a> ReplayReservation<'a> {
    fn reserve(
        cache: &'a Mutex<DuplicateCache>,
        submitted: Arc<SubmittedShareIdentity>,
        parent_block: [u8; 32],
    ) -> Result<Self, SubmitError> {
        let reservation = cache
            .lock()
            .map_err(|_| {
                SubmitError::other("share replay cache is poisoned; rotate the active job")
            })?
            .reserve(Arc::clone(&submitted), parent_block);
        match reservation {
            DuplicateReservation::Reserved(reservation) => Ok(Self {
                cache,
                submitted,
                parent_block,
                reservation,
                state: ReplayReservationState::InFlight,
            }),
            DuplicateReservation::Duplicate => Err(SubmitError {
                code: 22,
                message: "duplicate share".to_string(),
            }),
            DuplicateReservation::Full => Err(SubmitError::other(
                "share replay cache is full; rotate the job before accepting more work",
            )),
        }
    }

    fn promote(
        &mut self,
        validated_parent_block: [u8; 32],
        is_network_winner: bool,
    ) -> Result<DuplicateInsert, SubmitError> {
        let insertion = self
            .cache
            .lock()
            .map_err(|_| {
                SubmitError::other("share replay cache is poisoned; rotate the active job")
            })?
            .promote(
                &self.submitted,
                self.parent_block,
                validated_parent_block,
                self.reservation,
                is_network_winner,
            )
            .map_err(SubmitError::other)?;
        if insertion == DuplicateInsert::Inserted {
            self.state = ReplayReservationState::Accepted { is_network_winner };
        }
        Ok(insertion)
    }

    fn retain(&mut self) {
        debug_assert!(matches!(
            self.state,
            ReplayReservationState::Accepted { .. }
        ));
        self.state = ReplayReservationState::Retained;
    }
}

impl Drop for ReplayReservation<'_> {
    fn drop(&mut self) {
        if self.state == ReplayReservationState::Retained {
            return;
        }
        let mut cache = self
            .cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        cache.rollback(
            &self.submitted,
            self.parent_block,
            self.reservation,
            self.state,
        );
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum ReplayReservationState {
    InFlight,
    Accepted { is_network_winner: bool },
    Retained,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum DuplicateReservation {
    Reserved(u64),
    Duplicate,
    Full,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum DuplicateInsert {
    Inserted,
    Duplicate,
    Full,
}

#[derive(Debug)]
struct SubmitError {
    code: i32,
    message: String,
}

impl SubmitError {
    fn other(message: impl Into<String>) -> Self {
        Self {
            code: 20,
            message: message.into(),
        }
    }
}

#[derive(Clone, Debug)]
struct ConnectionLimiter {
    active: Arc<AtomicUsize>,
    maximum: usize,
}

impl ConnectionLimiter {
    fn new(maximum: usize) -> Self {
        assert!(maximum > 0, "connection limit must be positive");
        Self {
            active: Arc::new(AtomicUsize::new(0)),
            maximum,
        }
    }

    fn try_acquire(&self) -> Option<ConnectionPermit> {
        self.active
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                (active < self.maximum).then_some(active + 1)
            })
            .ok()?;
        Some(ConnectionPermit {
            limiter: self.clone(),
        })
    }
}

struct ConnectionPermit {
    limiter: ConnectionLimiter,
}

impl Drop for ConnectionPermit {
    fn drop(&mut self) {
        let previous = self.limiter.active.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn accept_with_deadline(listener: &TcpListener) -> TcpStream {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            match listener.accept() {
                Ok((stream, _peer)) => return stream,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(
                        Instant::now() < deadline,
                        "persistent listener did not accept the local connection"
                    );
                    thread::sleep(Duration::from_millis(1));
                }
                Err(error) => panic!("persistent listener accept failed: {error}"),
            }
        }
    }

    #[test]
    fn persistent_listener_owns_one_generation_and_keeps_its_port_bound() {
        let listener = Zip301LoopbackListener::bind(
            "127.0.0.1:0"
                .parse()
                .expect("valid ephemeral loopback address"),
        )
        .expect("bind persistent listener");
        let address = listener.local_addr().expect("bound listener address");

        let (first_generation, first_guard) =
            listener.begin_generation().expect("start first generation");
        assert!(listener.begin_generation().is_err());
        let first_client = TcpStream::connect(address).expect("connect first generation");
        let first_server = accept_with_deadline(&first_generation);
        first_server
            .shutdown(std::net::Shutdown::Both)
            .expect("close first server connection");
        drop(first_server);
        drop(first_client);
        drop(first_generation);
        drop(first_guard);

        // The owning listener never released the port while accepted sockets
        // from the retired generation entered their OS close lifecycle.
        let (second_generation, second_guard) = listener
            .begin_generation()
            .expect("start second generation without rebinding");
        let second_client = TcpStream::connect(address).expect("connect second generation");
        let second_server = accept_with_deadline(&second_generation);
        drop(second_server);
        drop(second_client);
        drop(second_generation);
        drop(second_guard);
    }

    #[test]
    fn persistent_listener_rejects_non_loopback_binds() {
        let bind = "192.0.2.1:8237"
            .parse()
            .expect("valid documentation-only address");
        assert!(Zip301LoopbackListener::bind(bind).is_err());
    }

    #[test]
    fn target_and_nonce_encoding_follow_zip301() {
        let target = Target::from_le_bytes(std::array::from_fn(|index| index as u8 + 1))
            .expect("nonzero target");
        let message = set_target(target);
        assert_eq!(
            message["params"][0],
            hex::encode((1u8..=32).rev().collect::<Vec<_>>())
        );
        let counter = AtomicU32::new(1);
        assert_eq!(
            allocate_session_nonce(&counter).expect("first nonce"),
            1u32.to_le_bytes()
        );
        assert_eq!(
            allocate_session_nonce(&counter).expect("second nonce"),
            2u32.to_le_bytes()
        );
        let exhausted = AtomicU32::new(u32::MAX);
        assert!(allocate_session_nonce(&exhausted).is_err());
    }

    #[test]
    fn extranonce_subscription_probe_has_the_standard_negative_response() {
        assert_eq!(
            unsupported_extension(json!(17)),
            json!({
                "id": 17,
                "result": false,
                "error": [20, "Not supported.", Value::Null],
            })
        );
    }

    #[test]
    fn share_target_cannot_hide_a_network_winner() {
        let target = |most_significant, least_significant| {
            let mut bytes = [0; 32];
            bytes[31] = most_significant;
            bytes[0] = least_significant;
            Target::from_le_bytes(bytes).expect("fixture target is nonzero")
        };
        let child = target(0x20, 0x01);
        let parent = target(0x10, 0xff);
        assert!(validate_share_target(target(0x30, 0), child, parent).is_ok());
        assert!(validate_share_target(child, child, parent).is_ok());
        assert!(validate_share_target(target(0x10, 0xfe), child, parent).is_err());
        assert!(validate_share_target(target(0x20, 0), child, parent).is_err());
    }

    #[test]
    fn credentials_are_hashed_and_compared_in_constant_time_shape() {
        let config = Zip301Config::new(Target::MAX, "correct horse battery")
            .expect("strong fixture password");
        let authentications = ConnectionLimiter::new(1);
        let worker = authorize(
            &json!(["worker.1", "correct horse battery"]),
            &config,
            &authentications,
        )
        .expect("correct shared credential");
        assert_eq!(worker.requested_name, "worker.1");
        assert_eq!(worker.identity.name(), "worker.1");
        assert_eq!(
            worker.identity.provenance(),
            crate::accounting::WorkerAuthenticationProvenance::SharedSecret
        );
        assert!(authorize(
            &json!(["worker.1", "wrong password"]),
            &config,
            &authentications,
        )
        .is_err());
        assert!(!format!("{config:?}").contains("correct horse"));
    }

    struct AliasAuthenticator;

    impl WorkerAuthenticator for AliasAuthenticator {
        fn authenticate(&self, worker: &str, password: &str) -> Option<AuthenticatedWorker> {
            (worker == "login-alias" && password == "independent password")
                .then(|| AuthenticatedWorker::new("account.rig-01").expect("canonical worker"))
        }
    }

    #[test]
    fn authenticated_login_resolves_to_canonical_processor_identity() {
        let config =
            Zip301Config::new_with_worker_authenticator(Target::MAX, Arc::new(AliasAuthenticator));
        assert!(config.uses_worker_authenticator());
        let authentications = ConnectionLimiter::new(1);
        let authorization = authorize(
            &json!(["login-alias", "independent password"]),
            &config,
            &authentications,
        )
        .expect("valid independent worker credential");
        assert_eq!(authorization.requested_name, "login-alias");
        assert_eq!(authorization.identity.name(), "account.rig-01");
        assert_eq!(
            authorization.identity.provenance(),
            crate::accounting::WorkerAuthenticationProvenance::ExactCredential
        );

        let mut authorized = HashMap::new();
        assert_eq!(
            insert_authorized_worker(&mut authorized, authorization),
            Ok(true)
        );
        assert_eq!(
            processor_worker_identity(&authorized, "login-alias")
                .expect("processor identity")
                .name(),
            "account.rig-01"
        );
        assert!(processor_worker_identity(&authorized, "account.rig-01").is_err());
        assert!(authorize(
            &json!(["login-alias", "incorrect password"]),
            &config,
            &authentications,
        )
        .is_err());
        assert!(!format!("{config:?}").contains("login-alias"));
    }

    #[test]
    fn authorization_rate_and_connection_lifetime_are_bounded() {
        let started = Instant::now();
        let mut policy = AuthorizationPolicy::new(started);
        for _ in 0..MAX_AUTHORIZATIONS_PER_MINUTE_PER_CONNECTION {
            assert_eq!(policy.admit(started), AuthorizationAdmission::Allowed);
        }
        for _ in MAX_AUTHORIZATIONS_PER_MINUTE_PER_CONNECTION..MAX_AUTHORIZATIONS_PER_CONNECTION {
            assert_eq!(policy.admit(started), AuthorizationAdmission::RateLimited);
        }
        assert_eq!(
            policy.admit(started + AUTHORIZATION_RATE_WINDOW),
            AuthorizationAdmission::LifetimeExhausted,
        );

        let mut elapsed_window = AuthorizationPolicy::new(started);
        for _ in 0..MAX_AUTHORIZATIONS_PER_MINUTE_PER_CONNECTION {
            assert_eq!(
                elapsed_window.admit(started),
                AuthorizationAdmission::Allowed
            );
        }
        assert_eq!(
            elapsed_window.admit(started + AUTHORIZATION_RATE_WINDOW),
            AuthorizationAdmission::Allowed,
        );
    }

    struct BlockingAuthenticator {
        entered: std::sync::mpsc::Sender<()>,
        release: Arc<(Mutex<bool>, std::sync::Condvar)>,
        calls: Arc<AtomicUsize>,
    }

    impl WorkerAuthenticator for BlockingAuthenticator {
        fn authenticate(&self, worker: &str, password: &str) -> Option<AuthenticatedWorker> {
            self.calls.fetch_add(1, Ordering::AcqRel);
            self.entered.send(()).ok()?;
            let (released, condition) = &*self.release;
            let guard = released.lock().ok()?;
            let _guard = condition.wait_while(guard, |released| !*released).ok()?;
            (password == "independent password")
                .then(|| AuthenticatedWorker::new(worker).expect("valid test worker"))
        }
    }

    #[test]
    fn concurrent_authentication_capacity_is_global_and_released() {
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let release = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let calls = Arc::new(AtomicUsize::new(0));
        let config = Zip301Config::new_with_worker_authenticator(
            Target::MAX,
            Arc::new(BlockingAuthenticator {
                entered: entered_tx,
                release: Arc::clone(&release),
                calls: Arc::clone(&calls),
            }),
        );
        let authentications = ConnectionLimiter::new(1);
        let first_config = config.clone();
        let first_authentications = authentications.clone();
        let first = thread::spawn(move || {
            authorize(
                &json!(["worker.first", "independent password"]),
                &first_config,
                &first_authentications,
            )
        });
        entered_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("first authentication entered its memory-hard provider");

        assert_eq!(
            authorize(
                &json!(["worker.second", "independent password"]),
                &config,
                &authentications,
            ),
            Err("authentication capacity is exhausted; retry later"),
        );
        assert_eq!(calls.load(Ordering::Acquire), 1);

        let (released, condition) = &*release;
        *released.lock().expect("release mutex") = true;
        condition.notify_all();
        assert!(first
            .join()
            .expect("authentication worker did not panic")
            .is_ok());
        assert!(authorize(
            &json!(["worker.second", "independent password"]),
            &config,
            &authentications,
        )
        .is_ok());
        assert_eq!(calls.load(Ordering::Acquire), 2);
    }

    #[test]
    fn worker_authorization_and_frame_assembly_are_bounded() {
        let mut authorized = HashMap::new();
        for index in 0..MAX_AUTHORIZED_WORKERS_PER_CONNECTION {
            let worker = format!("worker.{index}");
            assert_eq!(
                insert_authorized_worker(
                    &mut authorized,
                    WorkerAuthorization {
                        requested_name: worker.clone(),
                        identity: AuthenticatedWorker::new(worker).expect("valid worker"),
                    },
                ),
                Ok(true)
            );
        }
        assert_eq!(
            insert_authorized_worker(
                &mut authorized,
                WorkerAuthorization {
                    requested_name: "worker.0".to_string(),
                    identity: AuthenticatedWorker::new("worker.0").expect("valid worker"),
                },
            ),
            Ok(false)
        );
        assert!(insert_authorized_worker(
            &mut authorized,
            WorkerAuthorization {
                requested_name: "one-too-many".to_string(),
                identity: AuthenticatedWorker::new("one-too-many").expect("valid worker"),
            },
        )
        .is_err());

        let mut complete = std::io::BufReader::new(std::io::Cursor::new(b"{}\n"));
        assert!(read_frame_until(&mut complete, Instant::now()).is_err());
    }

    #[test]
    fn validation_and_per_connection_submission_limits_are_strict() {
        let config = Zip301Config::new(Target::MAX, "correct horse battery")
            .expect("strong fixture password")
            .with_maximum_parallel_validations(2)
            .expect("valid validation limit");
        let limiter = ConnectionLimiter::new(config.maximum_parallel_validations);
        let first = limiter.try_acquire().expect("first validation permit");
        let second = limiter.try_acquire().expect("second validation permit");
        assert!(limiter.try_acquire().is_none());
        drop(first);
        assert!(limiter.try_acquire().is_some());
        drop(second);

        let started = Instant::now();
        let mut rate = SubmissionRateLimiter::new(started);
        for _ in 0..MAX_SUBMISSIONS_PER_SECOND_PER_CONNECTION {
            assert!(rate.try_acquire(started));
        }
        assert!(!rate.try_acquire(started));
        assert!(rate.try_acquire(started + SUBMISSION_RATE_WINDOW));

        assert!(Zip301Config::new(Target::MAX, "correct horse battery")
            .expect("strong fixture password")
            .with_maximum_parallel_validations(0)
            .is_err());
    }

    fn submitted_share(marker: u8) -> Arc<SubmittedShareIdentity> {
        let mut nonce = [0; 32];
        nonce[0] = marker;
        Arc::new(SubmittedShareIdentity::new(
            "test-job",
            [1, 2, 3, 4],
            nonce,
            &[marker; EQUIHASH_SOLUTION_BYTES],
        ))
    }

    #[test]
    fn duplicate_cache_is_bounded_and_rejects_replays() {
        let cache = Mutex::new(DuplicateCache::new(1));
        let first_share = submitted_share(1);
        let first_block = [1; 32];
        let mut first = ReplayReservation::reserve(&cache, first_share, first_block)
            .expect("first reservation has capacity");
        let duplicate = ReplayReservation::reserve(&cache, submitted_share(1), first_block)
            .err()
            .expect("an in-flight replay is rejected");
        assert_eq!(duplicate.code, 22);
        let full = ReplayReservation::reserve(&cache, submitted_share(2), [2; 32])
            .err()
            .expect("unique in-flight work is bounded");
        assert_eq!(full.code, 20);
        assert_eq!(
            first
                .promote(first_block, false)
                .expect("the reserved identity is intact"),
            DuplicateInsert::Inserted
        );
        first.retain();
        let accepted_replay = ReplayReservation::reserve(&cache, submitted_share(1), first_block)
            .err()
            .expect("an accepted replay is rejected");
        assert_eq!(accepted_replay.code, 22);

        {
            let mut cache = cache.lock().expect("replay cache mutex");
            for value in 1..MAX_SHARES_PER_JOB {
                let mut key = [0; 32];
                key[..8].copy_from_slice(
                    &u64::try_from(value)
                        .expect("test range fits in u64")
                        .to_le_bytes(),
                );
                cache.entries.insert(
                    key,
                    CachedShare {
                        reservation: u64::try_from(value).expect("test token"),
                        state: CachedShareState::Accepted,
                    },
                );
                cache.accepted_shares += 1;
            }
        }
        let mut overflow = ReplayReservation::reserve(&cache, submitted_share(3), [0xfe; 32])
            .expect("accepted-share capacity is checked after validation");
        assert_eq!(
            overflow
                .promote([0xfe; 32], false)
                .expect("the reservation is intact"),
            DuplicateInsert::Full
        );
        drop(overflow);

        let mut winner = ReplayReservation::reserve(&cache, submitted_share(4), [0xff; 32])
            .expect("a winner can bypass an ordinary-share full cache");
        assert_eq!(
            winner
                .promote([0xff; 32], true)
                .expect("the winner reservation is intact"),
            DuplicateInsert::Inserted
        );
        winner.retain();
        let cache = cache.lock().expect("replay cache mutex");
        assert_eq!(cache.accepted_shares, MAX_SHARES_PER_JOB);
        assert_eq!(cache.network_winners, 1);
        assert_eq!(cache.in_flight, 0);
        drop(cache);

        let winner_cache = Mutex::new(DuplicateCache::new(1));
        {
            let mut cache = winner_cache.lock().expect("winner replay cache mutex");
            for value in 0..MAX_NETWORK_WINNERS_PER_JOB {
                let mut key = [0; 32];
                key[..8].copy_from_slice(
                    &u64::try_from(value)
                        .expect("test range fits in u64")
                        .to_le_bytes(),
                );
                cache.entries.insert(
                    key,
                    CachedShare {
                        reservation: u64::try_from(value).expect("test token"),
                        state: CachedShareState::NetworkWinner,
                    },
                );
                cache.network_winners += 1;
            }
        }
        let mut winner_overflow =
            ReplayReservation::reserve(&winner_cache, submitted_share(5), [0xfd; 32])
                .expect("winner capacity is checked after validation");
        assert_eq!(
            winner_overflow
                .promote([0xfd; 32], true)
                .expect("the winner reservation is intact"),
            DuplicateInsert::Full
        );
    }

    #[test]
    fn concurrent_replays_do_not_consume_validation_permits_or_repeat_validation() {
        let duplicates = Arc::new(Mutex::new(DuplicateCache::new(8)));
        let validations = ConnectionLimiter::new(2);
        let shutdown = Arc::new(AtomicBool::new(false));
        let submitted = submitted_share(7);
        let parent_block = [7; 32];
        let validation_calls = Arc::new(AtomicUsize::new(0));
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let release = Arc::new((Mutex::new(false), std::sync::Condvar::new()));

        let first_duplicates = Arc::clone(&duplicates);
        let first_validations = validations.clone();
        let first_shutdown = Arc::clone(&shutdown);
        let first_submitted = Arc::clone(&submitted);
        let first_calls = Arc::clone(&validation_calls);
        let first_release = Arc::clone(&release);
        let first = thread::spawn(move || {
            let ((), mut reservation) = validate_reserved_share(
                &first_duplicates,
                &first_validations,
                &first_shutdown,
                first_submitted,
                parent_block,
                || {
                    first_calls.fetch_add(1, Ordering::AcqRel);
                    entered_tx.send(()).expect("test receiver remains live");
                    let (released, condition) = &*first_release;
                    let guard = released.lock().expect("release mutex");
                    let _guard = condition
                        .wait_while(guard, |released| !*released)
                        .expect("release mutex remains healthy");
                    Ok(())
                },
            )?;
            let inserted = reservation.promote(parent_block, false)?;
            assert_eq!(inserted, DuplicateInsert::Inserted);
            reservation.retain();
            Ok::<(), SubmitError>(())
        });
        entered_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("first validation started");

        let mut replays = Vec::new();
        for _ in 0..32 {
            let duplicates = Arc::clone(&duplicates);
            let validations = validations.clone();
            let shutdown = Arc::clone(&shutdown);
            let submitted = Arc::clone(&submitted);
            let calls = Arc::clone(&validation_calls);
            replays.push(thread::spawn(move || {
                validate_reserved_share(
                    &duplicates,
                    &validations,
                    &shutdown,
                    submitted,
                    parent_block,
                    || {
                        calls.fetch_add(1, Ordering::AcqRel);
                        Ok(())
                    },
                )
                .err()
                .map(|error| error.code)
            }));
        }
        for replay in replays {
            let error = replay
                .join()
                .expect("replay worker did not panic")
                .expect("concurrent replay is rejected");
            assert_eq!(error, 22);
        }
        assert_eq!(validation_calls.load(Ordering::Acquire), 1);
        assert_eq!(validations.active.load(Ordering::Acquire), 1);
        let spare = validations
            .try_acquire()
            .expect("replays left the second validation permit available");
        drop(spare);

        let (released, condition) = &*release;
        *released.lock().expect("release mutex") = true;
        condition.notify_all();
        first
            .join()
            .expect("validation worker did not panic")
            .expect("first share was retained");

        let calls = Arc::clone(&validation_calls);
        let replay = validate_reserved_share(
            &duplicates,
            &validations,
            &shutdown,
            submitted,
            parent_block,
            || {
                calls.fetch_add(1, Ordering::AcqRel);
                Ok(())
            },
        )
        .err()
        .expect("accepted replay is rejected before validation");
        assert_eq!(replay.code, 22);
        assert_eq!(validation_calls.load(Ordering::Acquire), 1);
    }

    #[test]
    fn failed_or_colliding_in_flight_identity_does_not_poison_a_valid_retry() {
        let duplicates = Mutex::new(DuplicateCache::new(2));
        let validations = ConnectionLimiter::new(1);
        let shutdown = AtomicBool::new(false);
        let shared_parent_identity = [9; 32];
        let validation_calls = AtomicUsize::new(0);

        let invalid = validate_reserved_share(
            &duplicates,
            &validations,
            &shutdown,
            submitted_share(8),
            shared_parent_identity,
            || {
                validation_calls.fetch_add(1, Ordering::AcqRel);
                Err::<(), _>(SubmitError::other("invalid share fixture"))
            },
        )
        .err()
        .expect("invalid share is rejected");
        assert_eq!(invalid.code, 20);

        // This byte-distinct submission deliberately reuses the precomputed
        // block identity. The failed reservation was rolled back, so it can be
        // validated and promoted rather than being poisoned permanently.
        let ((), mut valid) = validate_reserved_share(
            &duplicates,
            &validations,
            &shutdown,
            submitted_share(9),
            shared_parent_identity,
            || {
                validation_calls.fetch_add(1, Ordering::AcqRel);
                Ok(())
            },
        )
        .expect("failed reservation was removed");
        assert_eq!(
            valid
                .promote(shared_parent_identity, false)
                .expect("validated identity matches the reservation"),
            DuplicateInsert::Inserted
        );
        valid.retain();
        assert_eq!(validation_calls.load(Ordering::Acquire), 2);
    }

    #[test]
    fn promotion_requires_authoritative_identity_and_processing_commit() {
        let cache = Mutex::new(DuplicateCache::new(2));
        let mismatched_block = [0x40; 32];
        let mut mismatched =
            ReplayReservation::reserve(&cache, submitted_share(0x40), mismatched_block)
                .expect("reserve share before validating its block identity");
        let mismatch = mismatched
            .promote([0x41; 32], false)
            .expect_err("post-validation identity mismatch must fail closed");
        assert_eq!(mismatch.code, 20);
        drop(mismatched);
        let retry = ReplayReservation::reserve(&cache, submitted_share(0x40), mismatched_block)
            .expect("mismatched validation did not poison the reservation");
        drop(retry);

        let different_block = [0x43; 32];
        let mut uncommitted =
            ReplayReservation::reserve(&cache, submitted_share(0x43), different_block)
                .expect("reserve a processable share");
        assert_eq!(
            uncommitted
                .promote(different_block, false)
                .expect("promote a processable share"),
            DuplicateInsert::Inserted
        );
        // A processor failure returns before `retain`; dropping must make the
        // same valid share eligible for an idempotent processing retry.
        drop(uncommitted);
        let retry = ReplayReservation::reserve(&cache, submitted_share(0x43), different_block)
            .expect("uncommitted accepted identity was rolled back");
        drop(retry);
    }
}
