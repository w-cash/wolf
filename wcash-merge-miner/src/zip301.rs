//! Stock-ASIC compatible ZIP-301 frontend for a proposal-validated native job.

use std::{
    collections::{HashSet, VecDeque},
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

use crate::{MinerError, NativePreparedJob, ValidatedNativeShare, EQUIHASH_SOLUTION_BYTES};

/// Maximum ZIP-301 request frame accepted from one ASIC.
pub const MAX_ZIP301_REQUEST_BYTES: usize = 64 * 1024;

/// Default maximum number of simultaneous local ASIC sessions.
pub const DEFAULT_ZIP301_CLIENT_LIMIT: usize = 256;

/// Default maximum number of shares that may be validated and committed concurrently.
pub const DEFAULT_ZIP301_VALIDATION_LIMIT: usize = 4;

const NONCE_1_BYTES: usize = 4;
const NONCE_2_BYTES: usize = 32 - NONCE_1_BYTES;
const SOLUTION_PREFIX: [u8; 3] = [0xfd, 0x40, 0x05];
const MAX_SHARES_PER_JOB: usize = 100_000;
const MAX_AUTHORIZED_WORKERS_PER_CONNECTION: usize = 16;
const MAX_SUBMISSIONS_PER_SECOND_PER_CONNECTION: usize = 64;
const MAX_ZIP301_VALIDATION_LIMIT: usize = 1_024;
const CLIENT_IO_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_FRAME_ASSEMBLY_TIME: Duration = Duration::from_secs(10);
const ACCEPT_RETRY_DELAY: Duration = Duration::from_millis(5);
const VALIDATION_RETRY_DELAY: Duration = Duration::from_millis(1);
const JOB_MONITOR_INTERVAL: Duration = Duration::from_secs(1);
const SUBMISSION_RATE_WINDOW: Duration = Duration::from_secs(1);

/// Callback that durably accounts for a valid share and queues any winners.
///
/// Returning an error rejects the share. Implementations should persist share
/// accounting before returning success. Winner submission belongs in the
/// health-monitor path so an unavailable node cannot delay the ASIC ACK;
/// Wcash and Zcash submissions must remain independent and idempotent.
pub trait ShareProcessor: Send + Sync + 'static {
    /// Processes one Equihash-valid, target-valid share.
    fn process(&self, worker: &str, share: &ValidatedNativeShare) -> Result<(), MinerError>;

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
    password_hash: [u8; 32],
    maximum_clients: usize,
    maximum_parallel_validations: usize,
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
            password_hash: Sha256::digest(password.as_bytes()).into(),
            maximum_clients: DEFAULT_ZIP301_CLIENT_LIMIT,
            maximum_parallel_validations: DEFAULT_ZIP301_VALIDATION_LIMIT,
        })
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

    /// Returns the configured share target.
    pub const fn share_target(&self) -> Target {
        self.share_target
    }
}

impl std::fmt::Debug for Zip301Config {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Zip301Config")
            .field("share_target", &self.share_target)
            .field("password", &"[REDACTED]")
            .field("maximum_clients", &self.maximum_clients)
            .field(
                "maximum_parallel_validations",
                &self.maximum_parallel_validations,
            )
            .finish()
    }
}

/// Serves one frozen native job over ZIP-301 for ASIC interoperability testing.
///
/// The built-in listener deliberately accepts only a loopback bind. Community
/// deployments should put an authenticated, rate-limited TCP/TLS edge in front
/// of it and rotate jobs through a supervisor whenever either chain tip changes.
pub fn serve_zip301_loopback(
    bind: SocketAddr,
    job: NativePreparedJob,
    config: Zip301Config,
    processor: Arc<dyn ShareProcessor>,
) -> Result<(), MinerError> {
    if !bind.ip().is_loopback() {
        return Err(MinerError::InvalidRequest(
            "the built-in ZIP-301 listener only binds loopback addresses".to_string(),
        ));
    }
    validate_share_target(
        config.share_target,
        job.job().required_target(),
        job.parent_target(),
    )?;
    let maximum_clients = config.maximum_clients;
    let maximum_parallel_validations = config.maximum_parallel_validations;
    let listener = TcpListener::bind(bind)?;
    listener.set_nonblocking(true)?;
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_reason = Arc::new(Mutex::new(None));
    let state = Arc::new(ServerState {
        job: Arc::new(job),
        config,
        processor,
        next_nonce: AtomicU32::new(1),
        duplicates: Mutex::new(DuplicateCache::default()),
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
                        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(error.to_string());
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
    if let Some(reason) = shutdown_reason
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .take()
    {
        return Err(MinerError::StaleNativeJob(reason));
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
    let mut authorized = HashSet::new();
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
                match authorize(params, &state.config) {
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

fn authorize<'a>(params: &'a Value, config: &Zip301Config) -> Result<&'a str, &'static str> {
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
    let supplied: [u8; 32] = Sha256::digest(password.as_bytes()).into();
    if !constant_time_eq(&supplied, &config.password_hash) {
        return Err("authorization failed");
    }
    Ok(worker)
}

fn insert_authorized_worker(
    authorized: &mut HashSet<String>,
    worker: &str,
) -> Result<bool, &'static str> {
    if authorized.contains(worker) {
        return Ok(false);
    }
    if authorized.len() >= MAX_AUTHORIZED_WORKERS_PER_CONNECTION {
        return Err("too many workers on one connection");
    }
    authorized.insert(worker.to_string());
    Ok(true)
}

fn submit(
    state: &ServerState,
    authorized: &HashSet<String>,
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
    if !authorized.contains(worker) {
        return Err(SubmitError {
            code: 24,
            message: "worker is not authorized".to_string(),
        });
    }
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
    let validation_permit = loop {
        if let Some(permit) = state.validations.try_acquire() {
            break permit;
        }
        if state.shutdown.load(Ordering::Acquire) {
            return Err(SubmitError {
                code: 21,
                message: "active job was retired while waiting for validation".to_string(),
            });
        }
        thread::sleep(VALIDATION_RETRY_DELAY);
    };
    let share = state
        .job
        .validate_share(&nonce, solution, state.config.share_target)
        .map_err(|error| match error {
            MinerError::AuxPow(AuxPowError::InsufficientParentWork { .. }) => SubmitError {
                code: 23,
                message: "low difficulty share".to_string(),
            },
            other => SubmitError::other(format!("invalid share: {other}")),
        })?;
    // Equihash validation is the CPU-heavy bounded operation. Release this
    // admission slot before serialized journal durability and winner RPCs so a
    // parent-node outage cannot starve validation or hide a later winner.
    drop(validation_permit);

    let duplicate_key = duplicate_key(job_id, &time, &nonce, solution);
    let mut duplicates = state
        .duplicates
        .lock()
        .map_err(|_| SubmitError::other("share replay cache is poisoned; rotate the active job"))?;
    let is_network_winner = share.wcash_candidate().is_some() || share.parent_block().is_some();
    match duplicates.insert(duplicate_key, is_network_winner) {
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
    drop(duplicates);

    if let Err(error) = state.processor.process(worker, &share) {
        if let Ok(mut duplicates) = state.duplicates.lock() {
            duplicates.remove(duplicate_key);
        }
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
    Ok(())
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

fn duplicate_key(job_id: &str, time: &[u8; 4], nonce: &[u8; 32], solution: &[u8]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"Wcash/ZIP301/share/v1\0");
    hash.update(job_id.as_bytes());
    hash.update(time);
    hash.update(nonce);
    hash.update(solution);
    hash.finalize().into()
}

fn constant_time_eq(left: &[u8; 32], right: &[u8; 32]) -> bool {
    left.iter()
        .zip(right)
        .fold(0u8, |difference, (left, right)| difference | (left ^ right))
        == 0
}

#[derive(Default)]
struct DuplicateCache {
    keys: HashSet<[u8; 32]>,
    winner_keys: HashSet<[u8; 32]>,
}

impl DuplicateCache {
    fn insert(&mut self, key: [u8; 32], is_network_winner: bool) -> DuplicateInsert {
        if self.keys.contains(&key) || self.winner_keys.contains(&key) {
            return DuplicateInsert::Duplicate;
        }
        if is_network_winner {
            self.winner_keys.insert(key);
            return DuplicateInsert::Inserted;
        }
        if self.keys.len() >= MAX_SHARES_PER_JOB {
            return DuplicateInsert::Full;
        }
        self.keys.insert(key);
        DuplicateInsert::Inserted
    }

    fn remove(&mut self, key: [u8; 32]) {
        self.keys.remove(&key);
        self.winner_keys.remove(&key);
    }
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
        assert_eq!(
            authorize(&json!(["worker.1", "correct horse battery"]), &config),
            Ok("worker.1")
        );
        assert!(authorize(&json!(["worker.1", "wrong password"]), &config).is_err());
        assert!(!format!("{config:?}").contains("correct horse"));
    }

    #[test]
    fn worker_authorization_and_frame_assembly_are_bounded() {
        let mut authorized = HashSet::new();
        for index in 0..MAX_AUTHORIZED_WORKERS_PER_CONNECTION {
            assert_eq!(
                insert_authorized_worker(&mut authorized, &format!("worker.{index}")),
                Ok(true)
            );
        }
        assert_eq!(
            insert_authorized_worker(&mut authorized, "worker.0"),
            Ok(false)
        );
        assert!(insert_authorized_worker(&mut authorized, "one-too-many").is_err());

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

    #[test]
    fn duplicate_cache_is_bounded_and_rejects_replays() {
        let mut cache = DuplicateCache::default();
        assert_eq!(cache.insert([1; 32], false), DuplicateInsert::Inserted);
        assert_eq!(cache.insert([1; 32], true), DuplicateInsert::Duplicate);
        for value in 0..MAX_SHARES_PER_JOB {
            let mut key = [0; 32];
            key[..8].copy_from_slice(
                &u64::try_from(value)
                    .expect("test range fits in u64")
                    .to_le_bytes(),
            );
            cache.insert(key, false);
        }
        assert_eq!(cache.keys.len(), MAX_SHARES_PER_JOB);
        assert_eq!(cache.insert([0xfe; 32], false), DuplicateInsert::Full);
        assert_eq!(cache.insert([0xff; 32], true), DuplicateInsert::Inserted);
        assert_eq!(cache.insert([0xff; 32], true), DuplicateInsert::Duplicate);
    }
}
