//! Command-line entry point for Wcash/Zcash merged-mining operations.

use std::{
    env,
    error::Error,
    io::{self, Write},
    net::SocketAddr,
    path::{Path, PathBuf},
    process,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc,
    },
    thread,
    time::{Duration, Instant, SystemTime},
};

use serde_json::{json, Map, Value};
use wcash_merge_miner::{
    accounting::{hash_worker_password, read_accounting_snapshot, WorkerCredentialStore},
    mine_zip301_once,
    pool_backend::{
        PoolBackendConnectionOutcome, PoolBackendIdentity, PoolBackendIdentityError,
        PoolBackendJournal, PoolBackendJournalError, PoolBackendListener,
        PoolBackendListenerConfig, PoolShareTargetPolicy,
    },
    protocol::serve_loopback,
    rpc::RpcEndpoint,
    zip301::{
        DEFAULT_ZIP301_AUTHENTICATION_LIMIT, DEFAULT_ZIP301_CLIENT_LIMIT,
        DEFAULT_ZIP301_VALIDATION_LIMIT,
    },
    CoordinatorConfig, GenerationRetirement, JobConfig, MinerError, NativeMiningCoordinator,
    NativeMiningSupervisor, NativePoolBackendRetainedJob, NativeZcashConfig, NativeZcashNetwork,
    PoolBackendActor, PoolBackendActorError, PoolBackendRetainedJob, PoolBackendWinnerScheduler,
    PreparedJob, ShareProcessor, WcashIncomingViewingKey, Zip301ClientConfig, Zip301Config,
    Zip301LoopbackListener, NATIVE_JOB_MAX_AGE_SECONDS,
};
use wcash_pool_protocol::{Hex32, JobInvalidationReason, TargetBe, TargetLe};
use wcash_zcash_aux::{Target, WCASH_AUXILIARY_CHAIN_ID};
use zcash_address::ZcashAddress;
use zebra_chain::block::genesis::WCASH_TESTNET_GENESIS_HASH;

const DEFAULT_BIND: &str = "127.0.0.1:28237";
const DEFAULT_SHARE_JOURNAL: &str = ".wcash-share-journal-v2.jsonl";
const INITIAL_NATIVE_PREPARATION_BACKOFF: Duration = Duration::from_secs(1);
const MAX_NATIVE_PREPARATION_BACKOFF: Duration = Duration::from_secs(60);
const TIP_RACE_RETRY_DELAY: Duration = Duration::from_millis(250);
const POOL_BACKEND_MONITOR_INTERVAL: Duration = Duration::from_millis(50);
const POOL_BACKEND_SUPERSEDED_GRACE: Duration = Duration::from_secs(2);
const POOL_BACKEND_RETIREMENT_QUEUE: usize = 4;

const WCASH_RPC_USERNAME: &str = "WCASH_RPC_USERNAME";
const WCASH_RPC_PASSWORD: &str = "WCASH_RPC_PASSWORD";
const ZCASH_TEMPLATE_RPC_USERNAME: &str = "ZCASH_TEMPLATE_RPC_USERNAME";
const ZCASH_TEMPLATE_RPC_PASSWORD: &str = "ZCASH_TEMPLATE_RPC_PASSWORD";
const ZCASH_VALIDATOR_RPC_USERNAME: &str = "ZCASH_VALIDATOR_RPC_USERNAME";
const ZCASH_VALIDATOR_RPC_PASSWORD: &str = "ZCASH_VALIDATOR_RPC_PASSWORD";
const WCASH_STRATUM_PASSWORD: &str = "WCASH_STRATUM_PASSWORD";
const WCASH_WORKER_CREDENTIALS: &str = "WCASH_WORKER_CREDENTIALS";
const WCASH_SHARE_TARGET: &str = "WCASH_SHARE_TARGET";
const WCASH_SHARE_JOURNAL: &str = "WCASH_SHARE_JOURNAL";
const WCASH_PAYOUT_ADDRESS: &str = "WCASH_PAYOUT_ADDRESS";
const WCASH_PAYOUT_IVK_FILE: &str = "WCASH_PAYOUT_IVK_FILE";
const ZCASH_PAYOUT_ADDRESS: &str = "ZCASH_PAYOUT_ADDRESS";
const WCASH_VALIDATION_LIMIT: &str = "WCASH_VALIDATION_LIMIT";
const WCASH_AUTHENTICATION_LIMIT: &str = "WCASH_AUTHENTICATION_LIMIT";
const WCASH_EXPECTED_GENESIS_HASH: &str = "WCASH_EXPECTED_GENESIS_HASH";
const ZCASH_EXPECTED_GENESIS_HASH: &str = "ZCASH_EXPECTED_GENESIS_HASH";
const ZCASH_NETWORK: &str = "ZCASH_NETWORK";
const WCASH_TESTNET_PARENT_TARGET_SAMPLING: &str = "WCASH_TESTNET_PARENT_TARGET_SAMPLING";
const WCASH_POOL_BACKEND_IDENTITY: &str = "WCASH_POOL_BACKEND_IDENTITY";
const WCASH_POOL_BACKEND_JOURNAL: &str = "WCASH_POOL_BACKEND_JOURNAL";
const WCASH_POOL_BACKEND_SOCKET: &str = "WCASH_POOL_BACKEND_SOCKET";
const WCASH_POOL_BACKEND_SUBMIT_UID: &str = "WCASH_POOL_BACKEND_SUBMIT_UID";
const WCASH_POOL_BACKEND_PROJECTOR_UID: &str = "WCASH_POOL_BACKEND_PROJECTOR_UID";
const WCASH_POOL_BACKEND_PAYOUT_UID: &str = "WCASH_POOL_BACKEND_PAYOUT_UID";
const WCASH_POOL_BACKEND_SOCKET_GID: &str = "WCASH_POOL_BACKEND_SOCKET_GID";
const WCASH_POOL_BACKEND_LISTENERS: &str = "WCASH_POOL_BACKEND_LISTENERS";

const USAGE: &str = r#"Wcash/Zcash merged-mining operator CLI

Synthetic development harness:
  wcash-merge-miner job  <child-hash-le> [target-le]
  wcash-merge-miner mine <child-hash-le> [target-le] [max-runs] [start-nonce]
  wcash-merge-miner serve <child-hash-le> [target-le] [127.0.0.1:port]

ZIP-301 reference miner:
  wcash-merge-miner zip301-mine <127.0.0.1:port> <worker>
    [max-runs] [start-nonce]

Pool administration:
  wcash-merge-miner worker-password-hash
  wcash-merge-miner accounting-report <share-journal-path>
  wcash-merge-miner pool-backend-init <wcash-rpc-url> <zcash-template-rpc-url>
    <zcash-validator-rpc-url> <wcash-address> [auxiliary-nonce]

Native node pipeline:
  wcash-merge-miner native-job <wcash-rpc-url> <zcash-template-rpc-url>
    <zcash-validator-rpc-url> <wcash-address> [auxiliary-nonce]
  wcash-merge-miner native-mine <wcash-rpc-url> <zcash-template-rpc-url>
    <zcash-validator-rpc-url> <wcash-address> [max-runs] [start-nonce]
  wcash-merge-miner native-serve-once <wcash-rpc-url> <zcash-template-rpc-url>
    <zcash-validator-rpc-url> <wcash-address> [127.0.0.1:port]
    [max-clients] [auxiliary-nonce]
  wcash-merge-miner native-serve <wcash-rpc-url> <zcash-template-rpc-url>
    <zcash-validator-rpc-url> <wcash-address> [127.0.0.1:port]
    [max-clients] [auxiliary-nonce]
  wcash-merge-miner native-pool-backend <wcash-rpc-url> <zcash-template-rpc-url>
    <zcash-validator-rpc-url> <wcash-address> [auxiliary-nonce]

The Wcash and Zcash template URLs must be loopback endpoints because those nodes
choose the block-reward recipients. The distinct Zcash proposal validator may be
remote over HTTPS; HTTP is accepted only on loopback. RPC credentials may
only be supplied through these optional, paired environment variables:
  WCASH_RPC_USERNAME / WCASH_RPC_PASSWORD
  ZCASH_TEMPLATE_RPC_USERNAME / ZCASH_TEMPLATE_RPC_PASSWORD
  ZCASH_VALIDATOR_RPC_USERNAME / ZCASH_VALIDATOR_RPC_PASSWORD

Every native command requires WCASH_EXPECTED_GENESIS_HASH,
ZCASH_EXPECTED_GENESIS_HASH (64 hex characters in conventional RPC display
order), and ZCASH_NETWORK (`mainnet`, `testnet`, or `regtest`). The selected
standard Zcash schedule must match the parent genesis and payout-address
network, and the parent tip must be on NU6.3 or later. Every node is pinned to
its height-zero hash before work is issued.

Both native-serve commands additionally require WCASH_WORKER_CREDENTIALS (the
path to a private version-1 exact-worker registry) and WCASH_SHARE_TARGET.
Set WCASH_SHARE_TARGET to `network` to derive each generation's advertised
target from the easier of its exact Wcash and Zcash network targets. This mode
captures every possible winner on both chains and rotates immediately after the
first durable network winner. An exact 32-byte conventional big-endian target
hex remains available for controlled fixed-target operation.
The wcash-address argument must be `-`; its value is read from
WCASH_PAYOUT_ADDRESS to keep it out of process listings and preflight logs.
ZCASH_PAYOUT_ADDRESS must exactly match the canonical mining.miner_address on
the parent template node and every proposal validator. Transparent addresses
are the normal pool-integration default on both chains; a Wcash Unified Address
with an Orchard receiver explicitly selects private Ironwood payout. The
coordinator checks a domain-separated private-GBT commitment and verifies the
configured payout in the exact Zcash coinbases. It verifies an exact transparent
Wcash recipient, or trial-decrypts every private Ironwood reward action with the
configured read-only incoming capability. Private payout requires
WCASH_PAYOUT_IVK_FILE, an absolute owner-private credential path containing the
64-byte raw Orchard incoming viewing key as 128 hexadecimal characters. Never
put the key itself in arguments or environment variables; systemd
`LoadCredential=` is recommended. Supplying the credential with a transparent
payout is rejected. Plaintext payout configuration is omitted from diagnostics,
but native preflight prints the exact Zcash coinbase.
pool-backend-init and native-pool-backend additionally require absolute paths in
WCASH_POOL_BACKEND_IDENTITY, WCASH_POOL_BACKEND_JOURNAL, and
WCASH_POOL_BACKEND_SOCKET. The serving command authenticates the submitting,
projector, and payout Unix peer UIDs from WCASH_POOL_BACKEND_SUBMIT_UID,
WCASH_POOL_BACKEND_PROJECTOR_UID, and WCASH_POOL_BACKEND_PAYOUT_UID, and sets
the socket GID from WCASH_POOL_BACKEND_SOCKET_GID. WCASH_POOL_BACKEND_LISTENERS optionally selects
1..=16 accept workers (default 4). Each persistent Unix session occupies one worker;
the complete pool reserves four for public, projector, and concurrent payout bootstrap sessions.
Initialization is explicit and never replaces
existing identity or journal state.
WCASH_VALIDATION_LIMIT optionally sets the global concurrent share-validation
limit (default 4, maximum 1024). WCASH_AUTHENTICATION_LIMIT optionally sets the
global concurrent Argon2id verification limit (default 4, maximum 256).
WCASH_TESTNET_PARENT_TARGET_SAMPLING may be set to exactly `1` only with
ZCASH_NETWORK=testnet, the built-in Wcash Testnet genesis, and a one-client
listener. It permits the advertised share target to sample the abnormally easy
parent target, but never to exclude a Wcash network winner. Without this explicit
bootstrap mode, the share target must include both network targets. Sampling mode
rotates immediately after durably recording a network winner.
WCASH_SHARE_JOURNAL optionally selects the durable JSON-lines share journal; the
default is .wcash-share-journal-v2.jsonl in the current directory (created 0600
on Unix). The default listener is 127.0.0.1:28237 and the default client limit is
256. native-serve automatically rotates proposal-validated jobs and retries
temporary preparation failures with bounded backoff. native-serve-once serves
one frozen job and exits when it becomes stale, for controlled integration tests.

zip301-mine reads its selected worker's password from WCASH_STRATUM_PASSWORD and
acts as a bounded, loopback-only reference ASIC. It reconstructs work exclusively
from ZIP-301 wire messages, solves real Equihash `(200, 9)`, and submits the
result over the pool connection.

worker-password-hash reads WCASH_STRATUM_PASSWORD and emits an Argon2id PHC
string for a private worker-credential file. accounting-report obtains a shared
lock and validates the complete durable ledger before emitting aggregate JSON.

Synthetic hashes and targets are exactly 32 bytes in little-endian numeric/raw
consensus order. Their default target is ff..ff.
"#;

fn main() {
    if let Err(error) = run() {
        eprintln!("wcash-merge-miner: {error}");
        process::exit(2);
    }
}

fn run() -> Result<(), Box<dyn Error>> {
    let mut arguments = env::args().skip(1);
    let Some(command) = arguments.next() else {
        print!("{USAGE}");
        return Ok(());
    };
    if command == "help" || command == "--help" || command == "-h" {
        print!("{USAGE}");
        return Ok(());
    }

    match command.as_str() {
        "job" | "mine" | "serve" => run_synthetic(&command, arguments),
        "native-job" => run_native_job(arguments),
        "native-mine" => run_native_mine(arguments),
        "native-serve-once" => run_native_serve_once(arguments),
        "native-serve" => run_native_serve(arguments),
        "zip301-mine" => run_zip301_mine(arguments),
        "worker-password-hash" => run_worker_password_hash(arguments),
        "accounting-report" => run_accounting_report(arguments),
        "pool-backend-init" => run_pool_backend_init(arguments),
        "native-pool-backend" => run_native_pool_backend(arguments),
        _ => Err(MinerError::InvalidRequest(format!("unknown command {command:?}")).into()),
    }
}

fn run_worker_password_hash(arguments: impl Iterator<Item = String>) -> Result<(), Box<dyn Error>> {
    ensure_no_more(arguments)?;
    let password = required_env(WCASH_STRATUM_PASSWORD)?;
    let password_hash = hash_worker_password(&password)?;
    drop(password);
    print_json(&json!({"password_hash": password_hash}))
}

fn run_accounting_report(
    mut arguments: impl Iterator<Item = String>,
) -> Result<(), Box<dyn Error>> {
    let ledger_path = PathBuf::from(next_required(&mut arguments, "share-journal-path")?);
    ensure_no_more(arguments)?;
    let snapshot = read_accounting_snapshot(ledger_path)?;
    print_json(&serde_json::to_value(snapshot)?)
}

struct PoolBackendRuntimeConfig {
    identity_path: PathBuf,
    journal_path: PathBuf,
    listener: PoolBackendListenerConfig,
    listener_workers: usize,
    target_policy: PoolShareTargetPolicy,
}

struct PoolBackendAuthorityFacts {
    wcash_genesis: Hex32,
    zcash_genesis: Hex32,
    wcash_payout_commitment: Hex32,
    zcash_payout_commitment: Hex32,
}

enum PoolBackendGenerationControl {
    Rotate,
    Shutdown,
    ServiceFailure(String),
}

struct PoolBackendListenerWorkers {
    listener: Arc<PoolBackendListener>,
    threads: Vec<thread::JoinHandle<()>>,
}

struct PoolBackendWinnerWorker {
    shutdown: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}

struct ActivePoolBackendGeneration {
    job_id: Hex32,
    retained: Arc<NativePoolBackendRetainedJob>,
    rotation_deadline: Instant,
}

struct DrainingPoolBackendGeneration {
    job_id: Hex32,
    retained: Arc<NativePoolBackendRetainedJob>,
    close_at: Instant,
}

struct PoolBackendRetirementWorker {
    sender: Option<mpsc::SyncSender<Arc<NativePoolBackendRetainedJob>>>,
    thread: Option<thread::JoinHandle<()>>,
}

impl PoolBackendRetirementWorker {
    fn spawn(failure: mpsc::Sender<String>) -> Result<Self, io::Error> {
        let (sender, receiver) = mpsc::sync_channel(POOL_BACKEND_RETIREMENT_QUEUE);
        let thread = thread::Builder::new()
            .name("wcash-candidate-retirement".to_string())
            .spawn(move || {
                while let Ok(retained) = receiver.recv() {
                    let mut retained = match Arc::try_unwrap(retained) {
                        Ok(retained) => retained,
                        Err(_) => {
                            let _ = failure.send(
                                "closed native generation retained unexpected validation ownership"
                                    .to_string(),
                            );
                            break;
                        }
                    };
                    if let Err(error) = retire_pool_backend_generation(&mut retained) {
                        let _ = failure.send(format!(
                            "closed native generation candidate retirement failed: {error}"
                        ));
                        break;
                    }
                }
            })?;
        Ok(Self {
            sender: Some(sender),
            thread: Some(thread),
        })
    }

    fn enqueue(&self, retained: Arc<NativePoolBackendRetainedJob>) -> Result<(), MinerError> {
        let sender = self.sender.as_ref().ok_or_else(|| {
            MinerError::InvalidRequest("candidate retirement worker is stopped".to_string())
        })?;
        sender.try_send(retained).map_err(|error| match error {
            mpsc::TrySendError::Full(_) => MinerError::InvalidRequest(
                "candidate retirement queue is full; refusing unbounded generation overlap"
                    .to_string(),
            ),
            mpsc::TrySendError::Disconnected(_) => MinerError::InvalidRequest(
                "candidate retirement worker stopped unexpectedly".to_string(),
            ),
        })
    }

    fn request_shutdown(&mut self) -> Result<(), io::Error> {
        self.sender.take();
        if let Some(worker) = self.thread.take() {
            worker.join().map_err(|_| {
                io::Error::other("candidate retirement worker panicked during shutdown")
            })?;
        }
        Ok(())
    }
}

impl Drop for PoolBackendRetirementWorker {
    fn drop(&mut self) {
        let _ = self.request_shutdown();
    }
}

impl PoolBackendWinnerWorker {
    fn spawn(
        supervisor: Arc<NativeMiningSupervisor>,
        actor: Arc<PoolBackendActor>,
        shutdown: Arc<AtomicBool>,
        failure: mpsc::Sender<String>,
    ) -> Result<Self, io::Error> {
        let worker_shutdown = Arc::clone(&shutdown);
        let thread = thread::Builder::new()
            .name("wcash-winner-reconciliation".to_string())
            .spawn(move || {
                let mut scheduler = PoolBackendWinnerScheduler::default();
                let mut pass_healthy = true;
                loop {
                    if worker_shutdown.load(Ordering::Acquire) {
                        break;
                    }
                    let work = match scheduler.next(&actor) {
                        Ok(work) => work,
                        Err(error) => {
                            actor.set_winner_reconciliation_health(false);
                            let _ = failure.send(format!(
                                "winner journal scheduling failed: {error}"
                            ));
                            break;
                        }
                    };
                    if let Some(snapshot) = work.snapshot() {
                        match supervisor.reconcile_pool_backend_winner(snapshot) {
                            Ok(Some(transition)) => {
                                match actor.compare_and_apply_winner_transition(snapshot, transition) {
                                    Ok(_) | Err(PoolBackendActorError::WinnerRevisionConflict) => {}
                                    Err(error) => {
                                        actor.set_winner_reconciliation_health(false);
                                        let _ = failure.send(format!(
                                            "winner lifecycle persistence failed: {error}"
                                        ));
                                        break;
                                    }
                                }
                            }
                            Ok(None) => {}
                            Err(error) if is_retryable_winner_reconciliation_error(&error) => {
                                pass_healthy = false;
                                actor.set_winner_reconciliation_health(false);
                                eprintln!(
                                    "winner reconciliation dependency unavailable; exact bytes remain durable: {error}"
                                );
                            }
                            Err(error) => {
                                actor.set_winner_reconciliation_health(false);
                                let _ = failure.send(format!(
                                    "winner reconciliation failed closed: {error}"
                                ));
                                break;
                            }
                        }
                    }
                    if work.completes_pass() {
                        actor.set_winner_reconciliation_health(pass_healthy);
                        pass_healthy = true;
                    }
                    if sleep_until_shutdown(work.delay(), &worker_shutdown) {
                        break;
                    }
                }
            })?;
        Ok(Self {
            shutdown,
            thread: Some(thread),
        })
    }

    fn request_shutdown(&mut self) -> Result<(), io::Error> {
        self.shutdown.store(true, Ordering::Release);
        if let Some(worker) = self.thread.take() {
            worker.join().map_err(|_| {
                io::Error::other("winner reconciliation worker panicked during shutdown")
            })?;
        }
        Ok(())
    }
}

impl Drop for PoolBackendWinnerWorker {
    fn drop(&mut self) {
        let _ = self.request_shutdown();
    }
}

impl PoolBackendListenerWorkers {
    fn spawn(
        listener: Arc<PoolBackendListener>,
        actor: Arc<PoolBackendActor>,
        worker_count: usize,
        failure: mpsc::Sender<String>,
    ) -> Result<Self, io::Error> {
        let mut workers = Self {
            listener,
            threads: Vec::with_capacity(worker_count),
        };
        for worker in 0..worker_count {
            let actor = Arc::clone(&actor);
            let listener = Arc::clone(&workers.listener);
            let failure = failure.clone();
            let thread = thread::Builder::new()
                .name(format!("wcash-pool-backend-{worker}"))
                .spawn(move || loop {
                    if !listener.is_accepting() {
                        break;
                    }
                    match listener.serve_one(actor.as_ref()) {
                        Ok(PoolBackendConnectionOutcome::Closed { reason }) => {
                            if listener.is_accepting() {
                                eprintln!("private pool backend connection closed: {reason}");
                            }
                        }
                        Ok(_) => {}
                        Err(error) => {
                            if listener.is_accepting() {
                                let _ = failure.send(error.to_string());
                            }
                            break;
                        }
                    }
                })?;
            workers.threads.push(thread);
        }
        Ok(workers)
    }

    fn socket_path(&self) -> &Path {
        self.listener.socket_path()
    }

    fn request_shutdown(&self) -> Result<(), Box<dyn Error>> {
        if self.threads.is_empty() {
            return Ok(());
        }
        self.listener.request_shutdown(self.threads.len())?;
        Ok(())
    }
}

impl Drop for PoolBackendListenerWorkers {
    fn drop(&mut self) {
        if self.threads.is_empty() {
            return;
        }
        if self.listener.request_shutdown(self.threads.len()).is_err() {
            return;
        }
        for thread in self.threads.drain(..) {
            let _ = thread.join();
        }
    }
}

fn run_pool_backend_init(arguments: impl Iterator<Item = String>) -> Result<(), Box<dyn Error>> {
    let arguments = parse_native_job_arguments(arguments)?;
    let configured = configure_native(&arguments.connection)?;
    let runtime = pool_backend_runtime_config()?;
    let facts = pool_backend_authority_facts(&configured)?;

    // A rerun can resume an exclusively-created identity after journal setup
    // failed, but never replaces either durable object.
    let (identity, resumed_identity) = match PoolBackendIdentity::initialize(&runtime.identity_path)
    {
        Ok(identity) => (identity, false),
        Err(PoolBackendIdentityError::AlreadyExists { .. }) => {
            (PoolBackendIdentity::open(&runtime.identity_path)?, true)
        }
        Err(error) => return Err(error.into()),
    };
    let created = PoolBackendJournal::create_new(
        &runtime.journal_path,
        identity.id(),
        facts.wcash_genesis.clone(),
        facts.zcash_genesis.clone(),
        facts.wcash_payout_commitment.clone(),
        facts.zcash_payout_commitment.clone(),
        WCASH_AUXILIARY_CHAIN_ID,
    );
    let (journal, result) = match created {
        Ok(journal) => (
            journal,
            if resumed_identity {
                "resumed_identity"
            } else {
                "initialized"
            },
        ),
        Err(PoolBackendJournalError::AlreadyExists { .. }) if resumed_identity => (
            PoolBackendJournal::open_existing(
                &runtime.journal_path,
                identity.id(),
                facts.wcash_genesis,
                facts.zcash_genesis,
                facts.wcash_payout_commitment,
                facts.zcash_payout_commitment,
                WCASH_AUXILIARY_CHAIN_ID,
            )?,
            "already_initialized",
        ),
        Err(error) => {
            eprintln!(
                "pool backend identity remains durable; correct the journal path or permissions and rerun pool-backend-init to resume safely"
            );
            return Err(error.into());
        }
    };
    let mut output = json!({
        "command": "pool-backend-init",
        "result": result,
        "backend_instance": identity.id(),
        "journal_stream": journal.journal_stream(),
        "event_seq": journal.current_event_seq()?,
        "chain_id": journal.chain_id(),
        "listener_workers": runtime.listener_workers,
    });
    output
        .as_object_mut()
        .expect("the backend initialization result is a JSON object")
        .extend(pool_backend_authority_json_fields(
            journal.wcash_genesis(),
            journal.zcash_genesis(),
            journal.wcash_payout_commitment(),
            journal.zcash_payout_commitment(),
            runtime.target_policy.operator_easiest(),
        ));
    print_json(&output)
}

/// Returns the public, exact authority fields of an initialized backend.
///
/// Genesis hashes use the same raw byte order as the authenticated backend
/// protocol and durable journal. Payout commitments are uninterpreted SHA-256
/// bytes. The numeric share target is converted from the backend's explicit
/// little-endian type into conventional big-endian display order.
fn pool_backend_authority_json_fields(
    wcash_genesis: &Hex32,
    zcash_genesis: &Hex32,
    wcash_payout_commitment: &Hex32,
    zcash_payout_commitment: &Hex32,
    share_target_ceiling: &TargetLe,
) -> Map<String, Value> {
    let share_target_ceiling = TargetBe::from(share_target_ceiling).to_string();
    json!({
        "wcash_genesis": wcash_genesis,
        "zcash_genesis": zcash_genesis,
        "wcash_payout_commitment": wcash_payout_commitment,
        "zcash_payout_commitment": zcash_payout_commitment,
        "share_target_ceiling": share_target_ceiling,
        "share_target_ceiling_byte_order": "big_endian",
    })
    .as_object()
    .expect("the backend authority fields are a JSON object")
    .clone()
}

fn run_native_pool_backend(arguments: impl Iterator<Item = String>) -> Result<(), Box<dyn Error>> {
    let arguments = parse_native_job_arguments(arguments)?;
    let configured = configure_native(&arguments.connection)?;
    let runtime = pool_backend_runtime_config()?;
    let facts = pool_backend_authority_facts(&configured)?;

    // Retain the identity lock for the entire service lifetime. Opening the
    // journal then proves it belongs to this exact installation and payout.
    let identity = PoolBackendIdentity::open(&runtime.identity_path)?;
    let journal = PoolBackendJournal::open_existing(
        &runtime.journal_path,
        identity.id(),
        facts.wcash_genesis,
        facts.zcash_genesis,
        facts.wcash_payout_commitment,
        facts.zcash_payout_commitment,
        WCASH_AUXILIARY_CHAIN_ID,
    )?;
    let actor = Arc::new(PoolBackendActor::new(journal, runtime.target_policy)?);
    let share_journal = share_journal_path()?;
    let supervisor = Arc::new(NativeMiningSupervisor::open(
        configured.config,
        share_journal,
    )?);
    let listener = Arc::new(PoolBackendListener::bind(runtime.listener)?);
    let shutdown = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register(signal_hook::consts::SIGTERM, Arc::clone(&shutdown))?;
    signal_hook::flag::register(signal_hook::consts::SIGINT, Arc::clone(&shutdown))?;
    let (listener_failure_tx, listener_failure_rx) = mpsc::channel::<String>();
    let mut winner_worker = PoolBackendWinnerWorker::spawn(
        Arc::clone(&supervisor),
        Arc::clone(&actor),
        Arc::clone(&shutdown),
        listener_failure_tx.clone(),
    )?;
    let listeners = PoolBackendListenerWorkers::spawn(
        listener,
        Arc::clone(&actor),
        runtime.listener_workers,
        listener_failure_tx.clone(),
    )?;
    let mut retirement_worker = PoolBackendRetirementWorker::spawn(listener_failure_tx.clone())?;
    drop(listener_failure_tx);

    let mut preparation_backoff = INITIAL_NATIVE_PREPARATION_BACKOFF;
    let mut active: Option<ActivePoolBackendGeneration> = None;
    let mut draining = Vec::new();
    loop {
        reap_draining_pool_backend_generations(actor.as_ref(), &retirement_worker, &mut draining)?;
        if shutdown.load(Ordering::Acquire) {
            listeners.request_shutdown()?;
            retire_running_pool_backend_generations(
                actor.as_ref(),
                &retirement_worker,
                active.take(),
                &mut draining,
            )?;
            winner_worker.request_shutdown()?;
            retirement_worker.request_shutdown()?;
            return print_json(&json!({
                "command": "native-pool-backend",
                "result": "stopped",
            }));
        }
        match listener_failure_rx.try_recv() {
            Ok(error) => {
                listeners.request_shutdown()?;
                retire_running_pool_backend_generations(
                    actor.as_ref(),
                    &retirement_worker,
                    active.take(),
                    &mut draining,
                )?;
                winner_worker.request_shutdown()?;
                retirement_worker.request_shutdown()?;
                return Err(MinerError::InvalidRequest(format!(
                    "private pool backend service stopped: {error}"
                ))
                .into());
            }
            Err(mpsc::TryRecvError::Disconnected) => {
                listeners.request_shutdown()?;
                retire_running_pool_backend_generations(
                    actor.as_ref(),
                    &retirement_worker,
                    active.take(),
                    &mut draining,
                )?;
                winner_worker.request_shutdown()?;
                retirement_worker.request_shutdown()?;
                return Err(MinerError::InvalidRequest(
                    "every private backend service worker stopped".to_string(),
                )
                .into());
            }
            Err(mpsc::TryRecvError::Empty) => {}
        }
        let coordinator = match supervisor.prepare_generation() {
            Ok(coordinator) => {
                preparation_backoff = INITIAL_NATIVE_PREPARATION_BACKOFF;
                coordinator
            }
            Err(error) if is_retryable_native_preparation_error(&error) => {
                let retry_delay = native_preparation_retry_delay(&error, preparation_backoff);
                eprintln!(
                    "transient native backend preparation failure: {error}; retrying in {} millisecond(s)",
                    retry_delay.as_millis()
                );
                match wait_for_pool_backend_retry(
                    retry_delay,
                    shutdown.as_ref(),
                    &listener_failure_rx,
                ) {
                    PoolBackendGenerationControl::Rotate => {}
                    PoolBackendGenerationControl::Shutdown => {
                        listeners.request_shutdown()?;
                        retire_running_pool_backend_generations(
                            actor.as_ref(),
                            &retirement_worker,
                            active.take(),
                            &mut draining,
                        )?;
                        winner_worker.request_shutdown()?;
                        retirement_worker.request_shutdown()?;
                        return print_json(&json!({
                            "command": "native-pool-backend",
                            "result": "stopped",
                        }));
                    }
                    PoolBackendGenerationControl::ServiceFailure(error) => {
                        listeners.request_shutdown()?;
                        retire_running_pool_backend_generations(
                            actor.as_ref(),
                            &retirement_worker,
                            active.take(),
                            &mut draining,
                        )?;
                        winner_worker.request_shutdown()?;
                        retirement_worker.request_shutdown()?;
                        return Err(MinerError::InvalidRequest(format!(
                            "private pool backend service stopped: {error}"
                        ))
                        .into());
                    }
                }
                preparation_backoff = next_native_preparation_backoff(&error, preparation_backoff);
                continue;
            }
            Err(error) => return Err(error.into()),
        };
        let retained = Arc::new(NativePoolBackendRetainedJob::new(coordinator)?);
        let descriptor = retained.descriptor();
        let job_id = descriptor.job_id.clone();
        let superseded_grace = if active.is_some() {
            POOL_BACKEND_SUPERSEDED_GRACE
        } else {
            Duration::ZERO
        };
        if let Err(error) = actor.activate_job(retained.clone(), superseded_grace) {
            let mut retained = Arc::try_unwrap(retained).map_err(|_| {
                MinerError::InvalidRequest(
                    "private backend retained a generation after failed activation".to_string(),
                )
            })?;
            if let Err(retirement) = retire_pool_backend_generation(&mut retained) {
                eprintln!(
                    "backend activation also failed to retire its child candidate: {retirement}"
                );
            }
            return Err(error.into());
        }
        let activation_output = print_json(&json!({
            "command": "native-pool-backend",
            "result": "job_activated",
            "job_id": job_id,
            "wcash_height": descriptor.wcash_height,
            "zcash_height": descriptor.zcash_height,
            "max_age_ms": descriptor.max_age_ms,
            "socket": listeners.socket_path(),
        }));
        if let Err(error) = activation_output {
            listeners.request_shutdown()?;
            retire_active_pool_backend_generation(actor.as_ref(), &job_id, retained)?;
            return Err(error);
        }

        if let Some(previous) = active.take() {
            let shares_tips =
                pool_backend_generations_share_tips(&previous.retained.descriptor(), &descriptor);
            if shares_tips {
                let close_after = previous
                    .retained
                    .remaining_lifetime()
                    .unwrap_or_default()
                    .min(POOL_BACKEND_SUPERSEDED_GRACE);
                let close_at = Instant::now().checked_add(close_after).ok_or_else(|| {
                    MinerError::InvalidRequest(
                        "superseded backend generation deadline overflow".to_string(),
                    )
                })?;
                draining.push(DrainingPoolBackendGeneration {
                    job_id: previous.job_id,
                    retained: previous.retained,
                    close_at,
                });
            } else {
                enqueue_closed_pool_backend_generation(
                    actor.as_ref(),
                    &retirement_worker,
                    previous.job_id,
                    previous.retained,
                )?;
            }
        }

        // Rotate before the native 45-second admission lease expires, leaving
        // margin for node health RPCs and a share already inside validation.
        let rotation_deadline = Instant::now()
            .checked_add(Duration::from_secs(
                NATIVE_JOB_MAX_AGE_SECONDS.saturating_sub(15),
            ))
            .ok_or_else(|| {
                MinerError::InvalidRequest("native backend rotation deadline overflow".to_string())
            })?;
        active = Some(ActivePoolBackendGeneration {
            job_id,
            retained,
            rotation_deadline,
        });
        let control = loop {
            reap_draining_pool_backend_generations(
                actor.as_ref(),
                &retirement_worker,
                &mut draining,
            )?;
            if shutdown.load(Ordering::Acquire) {
                break PoolBackendGenerationControl::Shutdown;
            }
            match listener_failure_rx.recv_timeout(POOL_BACKEND_MONITOR_INTERVAL) {
                Ok(error) => break PoolBackendGenerationControl::ServiceFailure(error),
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    break PoolBackendGenerationControl::ServiceFailure(
                        "every private backend service worker stopped".to_string(),
                    )
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
            }
            let current = active.as_ref().ok_or_else(|| {
                MinerError::InvalidRequest("private backend lost its active generation".to_string())
            })?;
            if Instant::now() >= current.rotation_deadline || !current.retained.is_healthy() {
                break PoolBackendGenerationControl::Rotate;
            }
        };

        if !matches!(control, PoolBackendGenerationControl::Rotate) {
            listeners.request_shutdown()?;
            retire_running_pool_backend_generations(
                actor.as_ref(),
                &retirement_worker,
                active.take(),
                &mut draining,
            )?;
        }

        match control {
            PoolBackendGenerationControl::Rotate => {}
            PoolBackendGenerationControl::Shutdown => {
                winner_worker.request_shutdown()?;
                retirement_worker.request_shutdown()?;
                return print_json(&json!({
                    "command": "native-pool-backend",
                    "result": "stopped",
                }));
            }
            PoolBackendGenerationControl::ServiceFailure(error) => {
                winner_worker.request_shutdown()?;
                retirement_worker.request_shutdown()?;
                return Err(MinerError::InvalidRequest(format!(
                    "private pool backend service stopped: {error}"
                ))
                .into());
            }
        }
    }
}

fn wait_for_pool_backend_retry(
    duration: Duration,
    shutdown: &AtomicBool,
    failures: &mpsc::Receiver<String>,
) -> PoolBackendGenerationControl {
    let Some(deadline) = Instant::now().checked_add(duration) else {
        return PoolBackendGenerationControl::Rotate;
    };
    loop {
        if shutdown.load(Ordering::Acquire) {
            return PoolBackendGenerationControl::Shutdown;
        }
        match failures.try_recv() {
            Ok(error) => return PoolBackendGenerationControl::ServiceFailure(error),
            Err(mpsc::TryRecvError::Disconnected) => {
                return PoolBackendGenerationControl::ServiceFailure(
                    "every private backend service worker stopped".to_string(),
                )
            }
            Err(mpsc::TryRecvError::Empty) => {}
        }
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            return PoolBackendGenerationControl::Rotate;
        };
        thread::sleep(remaining.min(Duration::from_millis(100)));
    }
}

fn is_retryable_winner_reconciliation_error(error: &MinerError) -> bool {
    matches!(error, MinerError::WinnerSubmissionDeferred { .. })
        || is_retryable_native_preparation_error(error)
}

fn sleep_until_shutdown(duration: Duration, shutdown: &AtomicBool) -> bool {
    let Some(deadline) = Instant::now().checked_add(duration) else {
        return false;
    };
    loop {
        if shutdown.load(Ordering::Acquire) {
            return true;
        }
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            return false;
        };
        thread::sleep(remaining.min(Duration::from_millis(100)));
    }
}

fn pool_backend_generations_share_tips(
    previous: &wcash_pool_protocol::JobDescriptor,
    replacement: &wcash_pool_protocol::JobDescriptor,
) -> bool {
    previous.wcash_previous_hash_le == replacement.wcash_previous_hash_le
        && previous.zcash_previous_hash_le == replacement.zcash_previous_hash_le
}

fn reap_draining_pool_backend_generations(
    actor: &PoolBackendActor,
    retirement_worker: &PoolBackendRetirementWorker,
    draining: &mut Vec<DrainingPoolBackendGeneration>,
) -> Result<(), Box<dyn Error>> {
    let now = Instant::now();
    let mut index = 0;
    while index < draining.len() {
        if now < draining[index].close_at {
            index += 1;
            continue;
        }
        let generation = draining.swap_remove(index);
        enqueue_closed_pool_backend_generation(
            actor,
            retirement_worker,
            generation.job_id,
            generation.retained,
        )?;
    }
    Ok(())
}

fn enqueue_closed_pool_backend_generation(
    actor: &PoolBackendActor,
    retirement_worker: &PoolBackendRetirementWorker,
    job_id: Hex32,
    retained: Arc<NativePoolBackendRetainedJob>,
) -> Result<(), Box<dyn Error>> {
    match actor.close_job(&job_id) {
        Ok(_) | Err(PoolBackendActorError::JobNotRetained) => {}
        Err(error) => return Err(error.into()),
    }
    retirement_worker.enqueue(retained)?;
    Ok(())
}

fn retire_running_pool_backend_generations(
    actor: &PoolBackendActor,
    retirement_worker: &PoolBackendRetirementWorker,
    active: Option<ActivePoolBackendGeneration>,
    draining: &mut Vec<DrainingPoolBackendGeneration>,
) -> Result<(), Box<dyn Error>> {
    if let Some(active) = active {
        match actor.invalidate_job(&active.job_id, JobInvalidationReason::Age, Duration::ZERO) {
            Ok(_) | Err(PoolBackendActorError::JobNotRetained) => {}
            Err(error) => return Err(error.into()),
        }
        retirement_worker.enqueue(active.retained)?;
    }
    for generation in draining.drain(..) {
        enqueue_closed_pool_backend_generation(
            actor,
            retirement_worker,
            generation.job_id,
            generation.retained,
        )?;
    }
    Ok(())
}

fn retire_active_pool_backend_generation(
    actor: &PoolBackendActor,
    job_id: &Hex32,
    retained: Arc<NativePoolBackendRetainedJob>,
) -> Result<GenerationRetirement, Box<dyn Error>> {
    actor.invalidate_job(job_id, JobInvalidationReason::Age, Duration::ZERO)?;
    let mut retained = Arc::try_unwrap(retained).map_err(|_| {
        MinerError::InvalidRequest(
            "private backend retained a native generation after durable closure".to_string(),
        )
    })?;
    Ok(retire_pool_backend_generation(&mut retained)?)
}

fn retire_pool_backend_generation(
    retained: &mut NativePoolBackendRetainedJob,
) -> Result<GenerationRetirement, MinerError> {
    let deadline = Instant::now()
        .checked_add(Duration::from_secs(30))
        .ok_or_else(|| {
            MinerError::InvalidRequest("native backend retirement deadline overflow".to_string())
        })?;
    let mut backoff = Duration::from_secs(1);
    loop {
        match retained.retire_generation() {
            Ok(retirement) => return Ok(retirement),
            Err(error) if is_retryable_native_preparation_error(&error) => {
                let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                    return Err(error);
                };
                eprintln!(
                    "transient native backend candidate-retirement failure: {error}; retrying in {} second(s)",
                    backoff.min(remaining).as_secs()
                );
                thread::sleep(backoff.min(remaining));
                backoff = (backoff * 2).min(Duration::from_secs(60));
            }
            Err(error) => return Err(error),
        }
    }
}

fn pool_backend_authority_facts(
    configured: &ConfiguredNative,
) -> Result<PoolBackendAuthorityFacts, MinerError> {
    let wcash_genesis = parse_display_hex32(
        &configured.config.expected_wcash_genesis_hash,
        WCASH_EXPECTED_GENESIS_HASH,
    )?;
    let zcash_genesis = parse_display_hex32(
        &required_display_hash_env(ZCASH_EXPECTED_GENESIS_HASH)?,
        ZCASH_EXPECTED_GENESIS_HASH,
    )?;
    Ok(PoolBackendAuthorityFacts {
        wcash_genesis,
        zcash_genesis,
        wcash_payout_commitment: Hex32::new(configured.config.validated_wcash_payout_commitment()?),
        zcash_payout_commitment: Hex32::new(
            configured.config.zcash.expected_parent_payout_commitment(),
        ),
    })
}

fn pool_backend_runtime_config() -> Result<PoolBackendRuntimeConfig, Box<dyn Error>> {
    let identity_path = required_absolute_path_env(WCASH_POOL_BACKEND_IDENTITY)?;
    let journal_path = required_absolute_path_env(WCASH_POOL_BACKEND_JOURNAL)?;
    let socket_path = required_absolute_path_env(WCASH_POOL_BACKEND_SOCKET)?;
    let submit_peer_uid = parse_required_u32_env(WCASH_POOL_BACKEND_SUBMIT_UID)?;
    let projector_peer_uid = parse_required_u32_env(WCASH_POOL_BACKEND_PROJECTOR_UID)?;
    let payout_peer_uid = parse_required_u32_env(WCASH_POOL_BACKEND_PAYOUT_UID)?;
    let expected_socket_gid = parse_required_u32_env(WCASH_POOL_BACKEND_SOCKET_GID)?;
    let listener_workers =
        parse_pool_backend_listener_workers(optional_env(WCASH_POOL_BACKEND_LISTENERS)?)?;
    let target = parse_display_target(&required_env(WCASH_SHARE_TARGET)?, WCASH_SHARE_TARGET)?;
    Ok(PoolBackendRuntimeConfig {
        identity_path,
        journal_path,
        listener: PoolBackendListenerConfig::new(
            socket_path,
            submit_peer_uid,
            expected_socket_gid,
        )?
        .with_read_only_peer_uids(projector_peer_uid, payout_peer_uid)?,
        listener_workers,
        target_policy: PoolShareTargetPolicy::new(TargetLe::new(target.to_le_bytes()))?,
    })
}

fn parse_pool_backend_listener_workers(value: Option<String>) -> Result<usize, MinerError> {
    let workers = parse_optional_usize(value, 4, WCASH_POOL_BACKEND_LISTENERS)?;
    if !(1..=16).contains(&workers) {
        return Err(MinerError::InvalidRequest(format!(
            "environment variable {WCASH_POOL_BACKEND_LISTENERS} must be in 1..=16"
        )));
    }
    Ok(workers)
}

fn parse_display_hex32(encoded: &str, field: &'static str) -> Result<Hex32, MinerError> {
    let mut bytes = parse_hash(encoded.to_string(), field)?;
    bytes.reverse();
    Ok(Hex32::new(bytes))
}

fn parse_required_u32_env(name: &'static str) -> Result<u32, MinerError> {
    required_env(name)?.parse::<u32>().map_err(|error| {
        MinerError::InvalidRequest(format!(
            "environment variable {name} is not a canonical u32: {error}"
        ))
    })
}

fn required_absolute_path_env(name: &'static str) -> Result<PathBuf, MinerError> {
    let path = PathBuf::from(required_env(name)?);
    if !path.is_absolute()
        || path.file_name().is_none()
        || path.components().any(|component| {
            matches!(
                component,
                std::path::Component::CurDir | std::path::Component::ParentDir
            )
        })
        || PathBuf::from_iter(path.components()) != path
    {
        return Err(MinerError::InvalidRequest(format!(
            "environment variable {name} must be an absolute lexically canonical file path"
        )));
    }
    Ok(path)
}

fn run_zip301_mine(mut arguments: impl Iterator<Item = String>) -> Result<(), Box<dyn Error>> {
    let endpoint = next_required(&mut arguments, "ZIP-301 loopback endpoint")?
        .parse::<SocketAddr>()
        .map_err(|error| {
            MinerError::InvalidRequest(format!("invalid ZIP-301 loopback endpoint: {error}"))
        })?;
    let worker = next_required(&mut arguments, "worker")?;
    let maximum_runs = parse_optional_u64(arguments.next(), 64, "max-runs")?;
    let start_nonce = parse_optional_u64(arguments.next(), 0, "start-nonce")?;
    ensure_no_more(arguments)?;
    let password = required_env(WCASH_STRATUM_PASSWORD)?;
    let config = Zip301ClientConfig::new(
        endpoint,
        worker.clone(),
        password,
        maximum_runs,
        start_nonce,
    )?;
    let accepted = mine_zip301_once(config)?;

    print_json(&json!({
        "command": "zip301-mine",
        "result": "accepted",
        "endpoint": endpoint.to_string(),
        "worker": worker,
        "job_id": accepted.job_id(),
        "parent_block_hash": accepted.parent_block_hash(),
        "nonce": hex::encode(accepted.nonce()),
        "share_target": display_target(accepted.share_target()),
        "attempted_nonce_runs": accepted.attempted_nonce_runs(),
    }))?;
    Ok(())
}

fn run_synthetic(
    command: &str,
    mut arguments: impl Iterator<Item = String>,
) -> Result<(), Box<dyn Error>> {
    let child = parse_hash(
        next_required(&mut arguments, "child-hash-le")?,
        "child-hash-le",
    )?;
    let target = match arguments.next() {
        Some(encoded) => Target::from_le_bytes(parse_hash(encoded, "target-le")?)?,
        None => Target::MAX,
    };
    let timestamp = u32::try_from(
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)?
            .as_secs(),
    )
    .map_err(|_| MinerError::InvalidRequest("current time exceeds u32".to_string()))?;
    let config = JobConfig {
        timestamp,
        ..JobConfig::default()
    };
    let job = PreparedJob::new(child, target, config)?;

    match command {
        "job" => {
            ensure_no_more(arguments)?;
            print_json(&json!({
                "job_id": job.job_id(),
                "algorithm": "Equihash(200,9)",
                "child_block_hash_le": hex::encode(job.child_block_hash()),
                "target_le": hex::encode(job.required_target().to_le_bytes()),
                "parent_header_input": hex::encode(job.parent_header_input()),
                "parent_coinbase": hex::encode(job.coinbase_bytes()),
                "parent_coinbase_txid_le": hex::encode(job.coinbase_transaction_id()),
            }))?;
        }
        "mine" => {
            let max_runs = parse_optional_u64(arguments.next(), 16, "max-runs")?;
            let start_nonce = parse_optional_u64(arguments.next(), 0, "start-nonce")?;
            ensure_no_more(arguments)?;
            let solved = job.solve(start_nonce, max_runs)?;
            print_json(&json!({
                "job_id": job.job_id(),
                "parent_block_hash_le": hex::encode(solved.parent_block_hash_le()),
                "nonce_le": hex::encode(solved.nonce()),
                "solution": hex::encode(solved.solution()),
                "auxpow_proof": hex::encode(solved.encoded_proof()),
            }))?;
        }
        "serve" => {
            let bind: SocketAddr = arguments
                .next()
                .unwrap_or_else(|| DEFAULT_BIND.to_string())
                .parse()?;
            ensure_no_more(arguments)?;
            eprintln!("serving synthetic local job {} on {bind}", job.job_id());
            serve_loopback(bind, job)?;
        }
        _ => unreachable!("synthetic commands are filtered by main"),
    }

    Ok(())
}

fn run_native_job(arguments: impl Iterator<Item = String>) -> Result<(), Box<dyn Error>> {
    let arguments = parse_native_job_arguments(arguments)?;
    let journal = share_journal_path()?;
    let configured = configure_native(&arguments.connection)?;
    let mut coordinator = NativeMiningCoordinator::prepare(configured.config, &journal)?;
    let preflight = native_preflight(
        "native-job",
        &arguments.connection,
        &configured.summary,
        &journal,
        &coordinator,
        None,
    );
    let retirement = coordinator.retire_generation();
    match (preflight, retirement) {
        (Ok(preflight), Ok(_)) => print_json(&preflight),
        (Err(error), Ok(_)) => Err(error.into()),
        (Ok(_), Err(error)) => Err(error.into()),
        (Err(error), Err(retirement)) => {
            eprintln!("native-job also failed to retire its child candidate: {retirement}");
            Err(error.into())
        }
    }
}

fn run_native_mine(arguments: impl Iterator<Item = String>) -> Result<(), Box<dyn Error>> {
    let arguments = parse_native_mine_arguments(arguments)?;

    let journal = share_journal_path()?;
    let configured = configure_native(&arguments.connection)?;
    let mut coordinator = NativeMiningCoordinator::prepare(configured.config, &journal)?;
    let result = (|| -> Result<Value, MinerError> {
        let native_job = coordinator.job();
        let solved = native_job
            .job()
            .solve(arguments.start_nonce, arguments.maximum_runs)?;
        let share = native_job.validate_share(&solved.nonce(), solved.solution(), Target::MAX)?;
        let wcash_candidate = share.wcash_candidate().is_some();
        let zcash_candidate = share.parent_block().is_some();
        let job_id = native_job.job().job_id().to_string();
        coordinator.process("local.native-mine", &share)?;
        // Unlike the ZIP-301 server, this one-shot command has no background
        // health monitor. Flush its newly durable winner outbox before exiting.
        coordinator.flush_winner_outbox()?;
        let outbox = coordinator.outbox_status()?;

        Ok(json!({
            "command": "native-mine",
            "result": "processed",
            "job_id": job_id,
            "parent_block_hash": display_hex(solved.parent_block_hash_le()),
            "wcash_candidate": wcash_candidate,
            "zcash_candidate": zcash_candidate,
            "durable_outbox": {
                "pending_wcash_winners": outbox.pending_wcash,
                "pending_zcash_winners": outbox.pending_zcash,
                "observed_best_chain_winners": outbox.observed,
                "quarantined_conflicting_winners": outbox.quarantined,
                "retention_confirmations": outbox.retention_confirmations,
            },
        }))
    })();
    let retirement = coordinator.retire_generation();
    match (result, retirement) {
        (Ok(result), Ok(_)) => print_json(&result),
        (Err(error), Ok(_)) => Err(error.into()),
        (Ok(_), Err(error)) => Err(error.into()),
        (Err(error), Err(retirement)) => {
            eprintln!("native-mine also failed to retire its child candidate: {retirement}");
            Err(error.into())
        }
    }
}

fn run_native_serve_once(arguments: impl Iterator<Item = String>) -> Result<(), Box<dyn Error>> {
    run_native_server(arguments, false)
}

fn run_native_serve(arguments: impl Iterator<Item = String>) -> Result<(), Box<dyn Error>> {
    run_native_server(arguments, true)
}

fn run_native_server(
    arguments: impl Iterator<Item = String>,
    automatic_rotation: bool,
) -> Result<(), Box<dyn Error>> {
    let arguments = parse_native_serve_arguments(arguments)?;

    // Validate the complete worker registry before making any network request.
    // The registry retains only Argon2id password hashes.
    let credential_path = PathBuf::from(required_env(WCASH_WORKER_CREDENTIALS)?);
    let credentials = WorkerCredentialStore::from_path(&credential_path)?;
    let worker_count = credentials.len();
    let share_target_selection = parse_share_target_selection(&required_env(WCASH_SHARE_TARGET)?)?;
    let maximum_parallel_validations = parse_optional_usize(
        optional_env(WCASH_VALIDATION_LIMIT)?,
        DEFAULT_ZIP301_VALIDATION_LIMIT,
        WCASH_VALIDATION_LIMIT,
    )?;
    let maximum_parallel_authentications = parse_optional_usize(
        optional_env(WCASH_AUTHENTICATION_LIMIT)?,
        DEFAULT_ZIP301_AUTHENTICATION_LIMIT,
        WCASH_AUTHENTICATION_LIMIT,
    )?;
    let configured = configure_native(&arguments.connection)?;
    let testnet_parent_target_sampling = parse_testnet_parent_target_sampling(
        optional_env(WCASH_TESTNET_PARENT_TARGET_SAMPLING)?,
        configured.zcash_network,
        &configured.config.expected_wcash_genesis_hash,
    )?;
    if testnet_parent_target_sampling
        && matches!(share_target_selection, ShareTargetSelection::NetworkTargets)
    {
        return Err(MinerError::InvalidRequest(format!(
            "{WCASH_TESTNET_PARENT_TARGET_SAMPLING} must be unset when {WCASH_SHARE_TARGET}=network"
        ))
        .into());
    }
    let credentials = Arc::new(credentials);
    let listener = Zip301LoopbackListener::bind(arguments.bind)?;

    let journal = share_journal_path()?;
    let supervisor = NativeMiningSupervisor::open(configured.config.clone(), &journal)?;
    let command = if automatic_rotation {
        "native-serve"
    } else {
        "native-serve-once"
    };
    let mut preparation_backoff = INITIAL_NATIVE_PREPARATION_BACKOFF;

    loop {
        let mut coordinator = match supervisor.prepare_generation() {
            Ok(coordinator) => {
                preparation_backoff = INITIAL_NATIVE_PREPARATION_BACKOFF;
                coordinator
            }
            Err(error) if automatic_rotation && is_retryable_native_preparation_error(&error) => {
                let retry_delay = native_preparation_retry_delay(&error, preparation_backoff);
                eprintln!(
                    "transient native job preparation failure: {error}; retrying in {} millisecond(s)",
                    retry_delay.as_millis()
                );
                thread::sleep(retry_delay);
                preparation_backoff = next_native_preparation_backoff(&error, preparation_backoff);
                continue;
            }
            Err(error) => return Err(error.into()),
        };
        let job = coordinator.job().clone();
        let share_target =
            share_target_selection.resolve(job.job().required_target(), job.parent_target());
        let mut zip301 =
            Zip301Config::new_with_worker_authenticator(share_target, credentials.clone())
                .with_maximum_clients(arguments.maximum_clients)?
                .with_maximum_parallel_authentications(maximum_parallel_authentications)?
                .with_maximum_parallel_validations(maximum_parallel_validations)?;
        if testnet_parent_target_sampling {
            zip301 = zip301.with_testnet_parent_target_sampling(configured.zcash_network)?;
        } else if matches!(share_target_selection, ShareTargetSelection::NetworkTargets) {
            zip301 = zip301.with_rotation_after_network_winner();
        }

        let preflight = match native_preflight(
            command,
            &arguments.connection,
            &configured.summary,
            &journal,
            &coordinator,
            Some(ServeSummary {
                bind: arguments.bind,
                maximum_clients: arguments.maximum_clients,
                maximum_parallel_authentications,
                maximum_parallel_validations,
                share_target,
                share_target_selection,
                testnet_parent_target_sampling,
                worker_count,
                automatic_rotation,
            }),
        ) {
            Ok(preflight) => preflight,
            Err(error) => {
                if let Err(retirement) =
                    retire_native_generation(&mut coordinator, automatic_rotation)
                {
                    eprintln!(
                        "native preflight also failed to retire its child candidate: {retirement}"
                    );
                }
                return Err(error.into());
            }
        };
        if let Err(error) = print_json(&preflight) {
            if let Err(retirement) = retire_native_generation(&mut coordinator, automatic_rotation)
            {
                eprintln!(
                    "native preflight output also failed to retire its child candidate: {retirement}"
                );
            }
            return Err(error);
        }

        let coordinator = Arc::new(coordinator);
        let processor: Arc<dyn ShareProcessor> = coordinator.clone();
        let serve_result = listener.serve(job, zip301.clone(), processor);
        let mut coordinator = Arc::try_unwrap(coordinator).map_err(|_| {
            MinerError::InvalidRequest(
                "native generation retained a share processor after every handler joined"
                    .to_string(),
            )
        })?;
        let outbox_flush = coordinator.flush_winner_outbox();
        retire_after_outbox_flush(outbox_flush, || {
            retire_native_generation(&mut coordinator, automatic_rotation)
        })?;

        match serve_result {
            Err(error) if automatic_rotation && is_retryable_native_preparation_error(&error) => {
                eprintln!("rotating/retrying native job after a transient failure: {error}");
            }
            Err(error) => return Err(error.into()),
            Ok(()) => return Ok(()),
        }
    }
}

fn retire_after_outbox_flush<T>(
    outbox_flush: Result<(), MinerError>,
    retire: impl FnOnce() -> Result<T, MinerError>,
) -> Result<T, MinerError> {
    outbox_flush?;
    retire()
}

fn retire_native_generation(
    coordinator: &mut NativeMiningCoordinator,
    retry_transient: bool,
) -> Result<GenerationRetirement, MinerError> {
    let mut backoff = Duration::from_secs(1);
    loop {
        match coordinator.retire_generation() {
            Ok(retirement) => return Ok(retirement),
            Err(error) if retry_transient && is_retryable_native_preparation_error(&error) => {
                eprintln!(
                    "transient native candidate-retirement failure: {error}; retrying in {} second(s) before creating new work",
                    backoff.as_secs()
                );
                thread::sleep(backoff);
                backoff = (backoff * 2).min(Duration::from_secs(60));
            }
            Err(error) => return Err(error),
        }
    }
}

/// Returns `true` only when preparing or monitoring a frozen generation failed
/// for a reason that can safely change without an operator or software correction.
///
/// This list is intentionally exhaustive. In particular, malformed templates,
/// proposal rejections, identity/payout mismatches, journal failures, invalid
/// configuration, and malformed RPC responses must stop the supervisor instead
/// of being hidden behind an unbounded retry loop.
fn is_retryable_native_preparation_error(error: &MinerError) -> bool {
    match error {
        MinerError::RpcTransport(error) => is_retryable_rpc_transport(error),
        MinerError::RpcHttpStatus(status) => is_retryable_http_status(*status),
        MinerError::RpcError {
            code: Some(-9 | -10 | -28),
            ..
        } => true,
        MinerError::Io(error) => is_retryable_io_kind(error.kind()),
        MinerError::ParentTipMismatch { .. }
        | MinerError::ChildTipMismatch { .. }
        | MinerError::StaleNativeJob(_) => true,
        _ => false,
    }
}

fn native_preparation_retry_delay(error: &MinerError, backoff: Duration) -> Duration {
    match error {
        MinerError::ParentTipMismatch { .. }
        | MinerError::ChildTipMismatch { .. }
        | MinerError::StaleNativeJob(_) => TIP_RACE_RETRY_DELAY,
        _ => backoff,
    }
}

fn next_native_preparation_backoff(error: &MinerError, backoff: Duration) -> Duration {
    match error {
        MinerError::ParentTipMismatch { .. }
        | MinerError::ChildTipMismatch { .. }
        | MinerError::StaleNativeJob(_) => INITIAL_NATIVE_PREPARATION_BACKOFF,
        _ => (backoff * 2).min(MAX_NATIVE_PREPARATION_BACKOFF),
    }
}

fn is_retryable_rpc_transport(error: &reqwest::Error) -> bool {
    if error.is_timeout() {
        return true;
    }

    // A generic request/build/TLS error can be permanent or security-sensitive.
    // Retry a non-timeout transport failure only when its source chain exposes
    // an I/O kind which is explicitly classified as transient below.
    let mut source = error.source();
    while let Some(cause) = source {
        if cause
            .downcast_ref::<io::Error>()
            .is_some_and(|error| is_retryable_io_kind(error.kind()))
        {
            return true;
        }
        source = cause.source();
    }

    false
}

const fn is_retryable_http_status(status: reqwest::StatusCode) -> bool {
    matches!(status.as_u16(), 408 | 425 | 429 | 500 | 502 | 503 | 504)
}

const fn is_retryable_io_kind(kind: io::ErrorKind) -> bool {
    matches!(
        kind,
        io::ErrorKind::ConnectionRefused
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::HostUnreachable
            | io::ErrorKind::NetworkUnreachable
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::NotConnected
            | io::ErrorKind::NetworkDown
            | io::ErrorKind::BrokenPipe
            | io::ErrorKind::WouldBlock
            | io::ErrorKind::TimedOut
            | io::ErrorKind::Interrupted
    )
}

#[derive(Debug, Eq, PartialEq)]
struct NativeConnectionArguments {
    wcash_rpc_url: String,
    zcash_template_rpc_url: String,
    zcash_validator_rpc_url: String,
    wcash_payout_address: String,
    auxiliary_nonce: u32,
}

#[derive(Debug, Eq, PartialEq)]
struct NativeJobArguments {
    connection: NativeConnectionArguments,
}

#[derive(Debug, Eq, PartialEq)]
struct NativeMineArguments {
    connection: NativeConnectionArguments,
    maximum_runs: u64,
    start_nonce: u64,
}

#[derive(Debug, Eq, PartialEq)]
struct NativeServeArguments {
    connection: NativeConnectionArguments,
    bind: SocketAddr,
    maximum_clients: usize,
}

fn parse_native_job_arguments(
    mut arguments: impl Iterator<Item = String>,
) -> Result<NativeJobArguments, MinerError> {
    let mut connection = parse_native_connection(&mut arguments)?;
    connection.auxiliary_nonce = parse_optional_u32(
        arguments.next(),
        connection.auxiliary_nonce,
        "auxiliary-nonce",
    )?;
    ensure_no_more(arguments)?;
    Ok(NativeJobArguments { connection })
}

fn parse_native_mine_arguments(
    mut arguments: impl Iterator<Item = String>,
) -> Result<NativeMineArguments, MinerError> {
    let connection = parse_native_connection(&mut arguments)?;
    let maximum_runs = parse_optional_u64(arguments.next(), 64, "max-runs")?;
    if maximum_runs == 0 {
        return Err(MinerError::InvalidRequest(
            "max-runs must be positive".to_string(),
        ));
    }
    let start_nonce = parse_optional_u64(arguments.next(), 0, "start-nonce")?;
    ensure_no_more(arguments)?;
    Ok(NativeMineArguments {
        connection,
        maximum_runs,
        start_nonce,
    })
}

fn parse_native_serve_arguments(
    mut arguments: impl Iterator<Item = String>,
) -> Result<NativeServeArguments, MinerError> {
    let mut connection = parse_native_connection(&mut arguments)?;
    let bind = arguments
        .next()
        .unwrap_or_else(|| DEFAULT_BIND.to_string())
        .parse::<SocketAddr>()
        .map_err(|error| {
            MinerError::InvalidRequest(format!("invalid loopback bind address: {error}"))
        })?;
    if !bind.ip().is_loopback() {
        return Err(MinerError::InvalidRequest(
            "native ZIP-301 serving only accepts a loopback bind address".to_string(),
        ));
    }
    let maximum_clients =
        parse_optional_usize(arguments.next(), DEFAULT_ZIP301_CLIENT_LIMIT, "max-clients")?;
    if !(1..=10_000).contains(&maximum_clients) {
        return Err(MinerError::InvalidRequest(
            "max-clients must be in 1..=10000".to_string(),
        ));
    }
    connection.auxiliary_nonce = parse_optional_u32(
        arguments.next(),
        connection.auxiliary_nonce,
        "auxiliary-nonce",
    )?;
    ensure_no_more(arguments)?;

    Ok(NativeServeArguments {
        connection,
        bind,
        maximum_clients,
    })
}

fn parse_native_connection(
    arguments: &mut impl Iterator<Item = String>,
) -> Result<NativeConnectionArguments, MinerError> {
    let connection = NativeConnectionArguments {
        wcash_rpc_url: next_required(arguments, "wcash-rpc-url")?,
        zcash_template_rpc_url: next_required(arguments, "zcash-template-rpc-url")?,
        zcash_validator_rpc_url: next_required(arguments, "zcash-validator-rpc-url")?,
        wcash_payout_address: next_required(arguments, "wcash-address")?,
        auxiliary_nonce: 0,
    };
    if connection.wcash_payout_address != "-" {
        return Err(MinerError::InvalidRequest(format!(
            "wcash-address must be '-' and supplied through {WCASH_PAYOUT_ADDRESS}"
        )));
    }
    Ok(connection)
}

struct ConfiguredNative {
    config: CoordinatorConfig,
    summary: EndpointSummary,
    zcash_network: NativeZcashNetwork,
}

struct EndpointSummary {
    wcash_label: String,
    wcash_authenticated: bool,
    zcash_template_label: String,
    zcash_template_authenticated: bool,
    zcash_validator_label: String,
    zcash_validator_authenticated: bool,
    zcash_network: String,
    wcash_payout_address_source: &'static str,
    zcash_payout_address_source: &'static str,
}

fn configure_native(arguments: &NativeConnectionArguments) -> Result<ConfiguredNative, MinerError> {
    let wcash_payout_address = required_payout_address_env(WCASH_PAYOUT_ADDRESS)?;
    let wcash_payout_incoming_viewing_key = optional_env(WCASH_PAYOUT_IVK_FILE)?
        .map(PathBuf::from)
        .map(WcashIncomingViewingKey::read_private_file)
        .transpose()?;
    let zcash_payout_address = required_payout_address_env(ZCASH_PAYOUT_ADDRESS)?;
    let expected_parent_payout_address: ZcashAddress =
        zcash_payout_address.parse().map_err(|error| {
            MinerError::InvalidRequest(format!(
                "environment variable {ZCASH_PAYOUT_ADDRESS} is not a valid Zcash address: {error}"
            ))
        })?;
    drop(zcash_payout_address);
    let (wcash_node, wcash_authenticated) = endpoint_from_env(
        &arguments.wcash_rpc_url,
        WCASH_RPC_USERNAME,
        WCASH_RPC_PASSWORD,
    )?;
    let (template_node, zcash_template_authenticated) = endpoint_from_env(
        &arguments.zcash_template_rpc_url,
        ZCASH_TEMPLATE_RPC_USERNAME,
        ZCASH_TEMPLATE_RPC_PASSWORD,
    )?;
    let (validator_node, zcash_validator_authenticated) = endpoint_from_env(
        &arguments.zcash_validator_rpc_url,
        ZCASH_VALIDATOR_RPC_USERNAME,
        ZCASH_VALIDATOR_RPC_PASSWORD,
    )?;
    let expected_wcash_genesis_hash = required_display_hash_env(WCASH_EXPECTED_GENESIS_HASH)?;
    let expected_zcash_genesis_hash = required_display_hash_env(ZCASH_EXPECTED_GENESIS_HASH)?;
    let expected_zcash_network = required_zcash_network_env()?;
    let summary = EndpointSummary {
        wcash_label: wcash_node.label().to_string(),
        wcash_authenticated,
        zcash_template_label: template_node.label().to_string(),
        zcash_template_authenticated,
        zcash_validator_label: validator_node.label().to_string(),
        zcash_validator_authenticated,
        zcash_network: expected_zcash_network.to_string(),
        wcash_payout_address_source: WCASH_PAYOUT_ADDRESS,
        zcash_payout_address_source: ZCASH_PAYOUT_ADDRESS,
    };
    let zcash = NativeZcashConfig::new(
        template_node,
        vec![validator_node],
        expected_zcash_network,
        expected_zcash_genesis_hash,
        expected_parent_payout_address,
    )?;

    Ok(ConfiguredNative {
        config: CoordinatorConfig {
            wcash_node,
            expected_wcash_genesis_hash,
            zcash,
            wcash_payout_address,
            wcash_payout_incoming_viewing_key,
            auxiliary_nonce: arguments.auxiliary_nonce,
        },
        summary,
        zcash_network: expected_zcash_network,
    })
}

fn required_display_hash_env(name: &'static str) -> Result<String, MinerError> {
    let encoded = required_env(name)?;
    if encoded.len() != 64 {
        return Err(MinerError::InvalidHexField {
            field: name,
            reason: format!("expected 64 hexadecimal characters, got {}", encoded.len()),
        });
    }
    hex::decode(&encoded).map_err(|error| MinerError::InvalidHexField {
        field: name,
        reason: error.to_string(),
    })?;
    Ok(encoded.to_ascii_lowercase())
}

fn required_zcash_network_env() -> Result<NativeZcashNetwork, MinerError> {
    let configured = required_env(ZCASH_NETWORK)?;
    match configured.as_str() {
        "mainnet" => Ok(NativeZcashNetwork::Mainnet),
        "testnet" => Ok(NativeZcashNetwork::Testnet),
        "regtest" => Ok(NativeZcashNetwork::Regtest),
        _ => Err(MinerError::InvalidRequest(format!(
            "environment variable {ZCASH_NETWORK} must be exactly `mainnet`, `testnet`, or `regtest`"
        ))),
    }
}

fn parse_testnet_parent_target_sampling(
    configured: Option<String>,
    zcash_network: NativeZcashNetwork,
    expected_wcash_genesis_hash: &str,
) -> Result<bool, MinerError> {
    let Some(configured) = configured else {
        return Ok(false);
    };
    if configured != "1" {
        return Err(MinerError::InvalidRequest(format!(
            "environment variable {WCASH_TESTNET_PARENT_TARGET_SAMPLING} must be exactly `1` when set"
        )));
    }
    if zcash_network != NativeZcashNetwork::Testnet {
        return Err(MinerError::InvalidRequest(format!(
            "environment variable {WCASH_TESTNET_PARENT_TARGET_SAMPLING} is allowed only when {ZCASH_NETWORK}=testnet"
        )));
    }
    if !expected_wcash_genesis_hash.eq_ignore_ascii_case(WCASH_TESTNET_GENESIS_HASH) {
        return Err(MinerError::InvalidRequest(format!(
            "environment variable {WCASH_TESTNET_PARENT_TARGET_SAMPLING} is allowed only with the built-in Wcash Testnet genesis"
        )));
    }
    Ok(true)
}

fn endpoint_from_env(
    url: &str,
    username_name: &'static str,
    password_name: &'static str,
) -> Result<(RpcEndpoint, bool), MinerError> {
    let username = optional_env(username_name)?;
    let password = optional_env(password_name)?;
    if username.is_some() != password.is_some() {
        return Err(MinerError::RpcConfiguration(format!(
            "set both {username_name} and {password_name}, or neither"
        )));
    }
    let authenticated = username.is_some();
    Ok((RpcEndpoint::new(url, username, password)?, authenticated))
}

fn optional_env(name: &'static str) -> Result<Option<String>, MinerError> {
    match env::var(name) {
        Ok(value) if value.is_empty() => Err(MinerError::InvalidRequest(format!(
            "environment variable {name} must not be empty"
        ))),
        Ok(value) => Ok(Some(value)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(env::VarError::NotUnicode(_)) => Err(MinerError::InvalidRequest(format!(
            "environment variable {name} is not valid Unicode"
        ))),
    }
}

fn required_env(name: &'static str) -> Result<String, MinerError> {
    optional_env(name)?.ok_or_else(|| {
        MinerError::InvalidRequest(format!("required environment variable {name} is not set"))
    })
}

fn required_payout_address_env(name: &'static str) -> Result<String, MinerError> {
    let value = required_env(name)?;
    if value.len() > 1_024 || value.bytes().any(|byte| byte.is_ascii_control()) {
        return Err(MinerError::InvalidRequest(format!(
            "environment variable {name} is not a bounded single-line address"
        )));
    }
    Ok(value)
}

fn share_journal_path() -> Result<PathBuf, MinerError> {
    match optional_env(WCASH_SHARE_JOURNAL)? {
        Some(path) => Ok(PathBuf::from(path)),
        None => Ok(env::current_dir()?.join(DEFAULT_SHARE_JOURNAL)),
    }
}

#[derive(Clone, Copy)]
struct ServeSummary {
    bind: SocketAddr,
    maximum_clients: usize,
    maximum_parallel_authentications: usize,
    maximum_parallel_validations: usize,
    share_target: Target,
    share_target_selection: ShareTargetSelection,
    testnet_parent_target_sampling: bool,
    worker_count: usize,
    automatic_rotation: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ShareTargetSelection {
    Fixed(Target),
    NetworkTargets,
}

impl ShareTargetSelection {
    fn resolve(self, child: Target, parent: Target) -> Target {
        match self {
            Self::Fixed(target) => target,
            Self::NetworkTargets => {
                if child.includes(parent) {
                    child
                } else {
                    parent
                }
            }
        }
    }

    const fn source(self) -> &'static str {
        match self {
            Self::Fixed(_) => WCASH_SHARE_TARGET,
            Self::NetworkTargets => "easier_network_target_per_generation",
        }
    }
}

fn parse_share_target_selection(configured: &str) -> Result<ShareTargetSelection, MinerError> {
    if configured == "network" {
        Ok(ShareTargetSelection::NetworkTargets)
    } else {
        parse_display_target(configured, WCASH_SHARE_TARGET).map(ShareTargetSelection::Fixed)
    }
}

fn share_target_policy_preflight(
    share_target: Target,
    child_target: Target,
    parent_target: Target,
    parent_network: NativeZcashNetwork,
    testnet_parent_target_sampling: bool,
    rotation_after_network_winner: bool,
) -> Result<Value, MinerError> {
    if testnet_parent_target_sampling && parent_network != NativeZcashNetwork::Testnet {
        return Err(MinerError::InvalidRequest(
            "parent-target sampling is allowed only on Zcash Testnet".to_string(),
        ));
    }
    let captures_all_wcash_winners = share_target.includes(child_target);
    let captures_all_zcash_winners = share_target.includes(parent_target);
    if !captures_all_wcash_winners
        || (!testnet_parent_target_sampling && !captures_all_zcash_winners)
    {
        return Err(MinerError::InvalidRequest(
            if testnet_parent_target_sampling {
                "ZIP-301 Testnet sampling target must include the Wcash child network target"
                    .to_string()
            } else {
                "ZIP-301 share target must be at least as easy as both network targets".to_string()
            },
        ));
    }

    Ok(json!({
        "mode": if testnet_parent_target_sampling {
            "testnet_parent_target_sampling"
        } else {
            "full_network_winner_coverage"
        },
        "opt_in_source": if testnet_parent_target_sampling {
            Value::String(WCASH_TESTNET_PARENT_TARGET_SAMPLING.to_string())
        } else {
            Value::Null
        },
        "captures_all_wcash_winners": captures_all_wcash_winners,
        "captures_all_zcash_winners": captures_all_zcash_winners,
        "degraded_parent_coverage": testnet_parent_target_sampling
            && !captures_all_zcash_winners,
        "rotation_after_first_durable_network_winner": rotation_after_network_winner,
    }))
}

fn native_preflight(
    command: &'static str,
    arguments: &NativeConnectionArguments,
    endpoints: &EndpointSummary,
    journal: &std::path::Path,
    coordinator: &NativeMiningCoordinator,
    serve: Option<ServeSummary>,
) -> Result<Value, MinerError> {
    let native_job = coordinator.job();
    let job = native_job.job();
    let outbox = coordinator.outbox_status()?;
    let rotation_after_network_winner = serve.is_some_and(|serve| {
        serve.testnet_parent_target_sampling
            || matches!(
                serve.share_target_selection,
                ShareTargetSelection::NetworkTargets
            )
    });
    let lifecycle_message = if serve.is_some_and(|serve| serve.automatic_rotation)
        && rotation_after_network_winner
    {
        "one exact proposal-validated generation is active; the process retires it and prepares fresh work after the first durably recorded network winner, whenever either chain tip changes, or when the Wcash candidate expires"
    } else if serve.is_some_and(|serve| serve.automatic_rotation) {
        "one exact proposal-validated generation is active; the process retires it and prepares fresh work whenever either chain tip changes or the Wcash candidate expires"
    } else if serve.is_some() && rotation_after_network_winner {
        "ONE FROZEN JOB ONLY: this process stops after the first durably recorded network winner, when either chain tip changes, or when the Wcash candidate expires"
    } else if serve.is_some() {
        "ONE FROZEN JOB ONLY: this process keeps serving this exact job until stopped and exits when either chain tip changes or the Wcash candidate expires"
    } else {
        "one proposal-validated frozen job was prepared; no listener was started"
    };

    let mut output = json!({
        "command": command,
        "preflight": "passed",
        "algorithm": "Equihash(200,9)",
        "lifecycle": {
            "one_frozen_job": true,
            "automatic_rotation": serve.is_some_and(|serve| serve.automatic_rotation),
            "maximum_generation_age_seconds": NATIVE_JOB_MAX_AGE_SECONDS,
            "message": lifecycle_message,
        },
        "rpc": {
            "wcash": {
                "endpoint": endpoints.wcash_label,
                "authenticated": endpoints.wcash_authenticated,
                "credential_source": format!("{WCASH_RPC_USERNAME} + {WCASH_RPC_PASSWORD}"),
            },
            "zcash_template": {
                "endpoint": endpoints.zcash_template_label,
                "authenticated": endpoints.zcash_template_authenticated,
                "credential_source": format!(
                    "{ZCASH_TEMPLATE_RPC_USERNAME} + {ZCASH_TEMPLATE_RPC_PASSWORD}"
                ),
            },
            "zcash_proposal_validator": {
                "endpoint": endpoints.zcash_validator_label,
                "authenticated": endpoints.zcash_validator_authenticated,
                "credential_source": format!(
                    "{ZCASH_VALIDATOR_RPC_USERNAME} + {ZCASH_VALIDATOR_RPC_PASSWORD}"
                ),
            },
            "template_and_validator_are_distinct": true,
            "proposal_gate": "passed",
        },
        "wcash": {
            "payout_address": {
                "configured": true,
                "source": endpoints.wcash_payout_address_source,
            },
            "auxiliary_nonce": arguments.auxiliary_nonce,
            "child_block_hash": display_hex(job.child_block_hash()),
            "child_target": display_target(job.required_target()),
        },
        "zcash": {
            "network": endpoints.zcash_network,
            "payout_address": {
                "configured": true,
                "source": endpoints.zcash_payout_address_source,
                "template_commitment_verified": true,
            },
            "parent_tip": native_job.parent_tip_display(),
            "parent_height": native_job.parent_height(),
            "parent_target": display_target(native_job.parent_target()),
        },
        "job": {
            "job_id": job.job_id(),
            "parent_header_input": hex::encode(job.parent_header_input()),
            "parent_coinbase": hex::encode(job.coinbase_bytes()),
            "parent_coinbase_txid_le": hex::encode(job.coinbase_transaction_id()),
        },
        "share_journal": {
            "path": journal.display().to_string(),
            "pending_wcash_winners": outbox.pending_wcash,
            "pending_zcash_winners": outbox.pending_zcash,
            "observed_best_chain_winners": outbox.observed,
            "quarantined_conflicting_winners": outbox.quarantined,
            "retention_confirmations": outbox.retention_confirmations,
        },
    });

    if let Some(serve) = serve {
        let share_target_policy = share_target_policy_preflight(
            serve.share_target,
            job.required_target(),
            native_job.parent_target(),
            native_job.parent_network(),
            serve.testnet_parent_target_sampling,
            rotation_after_network_winner,
        )?;
        let degraded_parent_coverage =
            share_target_policy["degraded_parent_coverage"] == Value::Bool(true);
        output["listener"] = json!({
            "bind": serve.bind.to_string(),
            "loopback_only": true,
            "maximum_clients": serve.maximum_clients,
            "maximum_parallel_authentications": serve.maximum_parallel_authentications,
            "maximum_parallel_validations": serve.maximum_parallel_validations,
            "share_target": display_target(serve.share_target),
            "share_target_source": serve.share_target_selection.source(),
            "share_target_policy": share_target_policy,
            "authentication": "exact-worker-argon2id",
            "worker_count": serve.worker_count,
            "worker_credentials_source": WCASH_WORKER_CREDENTIALS,
        });
        if degraded_parent_coverage {
            eprintln!(
                "WARNING: {WCASH_TESTNET_PARENT_TARGET_SAMPLING}=1 is sampling Zcash Testnet parent winners; Wcash winner coverage remains complete and the generation rotates after its first durably recorded network winner"
            );
        }
    }
    Ok(output)
}

fn display_hex(mut raw_little_endian: [u8; 32]) -> String {
    raw_little_endian.reverse();
    hex::encode(raw_little_endian)
}

fn display_target(target: Target) -> String {
    display_hex(target.to_le_bytes())
}

fn parse_display_target(encoded: &str, field: &'static str) -> Result<Target, MinerError> {
    let mut bytes = parse_hash(encoded.to_string(), field)?;
    bytes.reverse();
    Ok(Target::from_le_bytes(bytes)?)
}

fn print_json(value: &Value) -> Result<(), Box<dyn Error>> {
    let stdout = io::stdout();
    let mut output = stdout.lock();
    serde_json::to_writer_pretty(&mut output, value)?;
    writeln!(output)?;
    output.flush()?;
    Ok(())
}

fn next_required(
    arguments: &mut impl Iterator<Item = String>,
    field: &'static str,
) -> Result<String, MinerError> {
    arguments
        .next()
        .ok_or_else(|| MinerError::InvalidRequest(format!("missing {field}")))
}

fn parse_hash(encoded: String, field: &'static str) -> Result<[u8; 32], MinerError> {
    let bytes = hex::decode(encoded).map_err(|error| MinerError::InvalidHexField {
        field,
        reason: error.to_string(),
    })?;
    bytes
        .try_into()
        .map_err(|bytes: Vec<u8>| MinerError::InvalidHexField {
            field,
            reason: format!("decoded to {} bytes, expected 32", bytes.len()),
        })
}

fn parse_optional_u64(
    value: Option<String>,
    default: u64,
    field: &'static str,
) -> Result<u64, MinerError> {
    value.map_or(Ok(default), |value| {
        value
            .parse()
            .map_err(|error| MinerError::InvalidNumericField {
                field,
                reason: format!("expected an unsigned integer: {error}"),
            })
    })
}

fn parse_optional_u32(
    value: Option<String>,
    default: u32,
    field: &'static str,
) -> Result<u32, MinerError> {
    value.map_or(Ok(default), |value| {
        value
            .parse()
            .map_err(|error| MinerError::InvalidNumericField {
                field,
                reason: format!("expected a 32-bit unsigned integer: {error}"),
            })
    })
}

fn parse_optional_usize(
    value: Option<String>,
    default: usize,
    field: &'static str,
) -> Result<usize, MinerError> {
    value.map_or(Ok(default), |value| {
        value
            .parse()
            .map_err(|error| MinerError::InvalidNumericField {
                field,
                reason: format!("expected an unsigned integer: {error}"),
            })
    })
}

fn ensure_no_more(mut arguments: impl Iterator<Item = String>) -> Result<(), MinerError> {
    match arguments.next() {
        Some(argument) => Err(MinerError::InvalidRequest(format!(
            "unexpected extra argument {argument:?}"
        ))),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn arguments(values: &[&str]) -> impl Iterator<Item = String> {
        values
            .iter()
            .map(|value| (*value).to_string())
            .collect::<Vec<_>>()
            .into_iter()
    }

    fn common() -> [&'static str; 4] {
        [
            "http://127.0.0.1:18232",
            "http://127.0.0.1:18233",
            "http://127.0.0.1:18234",
            "-",
        ]
    }

    #[test]
    fn native_job_parser_applies_nonce_default_and_override() {
        let parsed = parse_native_job_arguments(arguments(&common())).expect("valid defaults");
        assert_eq!(parsed.connection.auxiliary_nonce, 0);

        let mut explicit = common().to_vec();
        explicit.push("4294967295");
        let parsed = parse_native_job_arguments(arguments(&explicit)).expect("valid u32 nonce");
        assert_eq!(parsed.connection.auxiliary_nonce, u32::MAX);
    }

    #[test]
    fn native_serve_parser_applies_safe_defaults() {
        let parsed = parse_native_serve_arguments(arguments(&common())).expect("valid defaults");
        assert_eq!(
            parsed.bind,
            DEFAULT_BIND.parse().expect("constant is valid")
        );
        assert_eq!(parsed.maximum_clients, DEFAULT_ZIP301_CLIENT_LIMIT);
        assert_eq!(parsed.connection.auxiliary_nonce, 0);
    }

    #[test]
    fn native_mine_parser_bounds_work_and_rejects_extra_arguments() {
        let parsed = parse_native_mine_arguments(arguments(&common())).expect("valid defaults");
        assert_eq!(parsed.maximum_runs, 64);
        assert_eq!(parsed.start_nonce, 0);

        let mut explicit = common().to_vec();
        explicit.extend(["128", "42"]);
        let parsed = parse_native_mine_arguments(arguments(&explicit)).expect("valid limits");
        assert_eq!(parsed.maximum_runs, 128);
        assert_eq!(parsed.start_nonce, 42);

        let mut zero = common().to_vec();
        zero.push("0");
        assert!(parse_native_mine_arguments(arguments(&zero)).is_err());

        let mut extra = explicit;
        extra.push("unexpected");
        assert!(parse_native_mine_arguments(arguments(&extra)).is_err());
    }

    #[test]
    fn native_serve_parser_accepts_explicit_operational_limits() {
        let mut explicit = common().to_vec();
        explicit.extend(["[::1]:29000", "64", "7"]);
        let parsed = parse_native_serve_arguments(arguments(&explicit)).expect("valid options");
        assert_eq!(
            parsed.bind,
            "[::1]:29000".parse().expect("literal is valid")
        );
        assert_eq!(parsed.maximum_clients, 64);
        assert_eq!(parsed.connection.auxiliary_nonce, 7);
    }

    #[test]
    fn native_serve_parser_rejects_remote_bind_and_bad_limit() {
        let mut remote = common().to_vec();
        remote.push("0.0.0.0:28237");
        assert!(parse_native_serve_arguments(arguments(&remote)).is_err());

        let mut zero_limit = common().to_vec();
        zero_limit.extend([DEFAULT_BIND, "0"]);
        assert!(parse_native_serve_arguments(arguments(&zero_limit)).is_err());
    }

    #[test]
    fn native_parsers_reject_missing_and_extra_arguments() {
        assert!(parse_native_job_arguments(arguments(&common()[..3])).is_err());

        let mut plaintext_address = common();
        plaintext_address[3] = "utest1address-visible-in-process-list";
        assert!(parse_native_job_arguments(arguments(&plaintext_address)).is_err());

        let mut extra = common().to_vec();
        extra.extend(["0", "unexpected"]);
        assert!(parse_native_job_arguments(arguments(&extra)).is_err());
    }

    #[test]
    fn share_target_uses_conventional_big_endian_display_order() {
        let display = (1u8..=32)
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let target = parse_display_target(&display, "target").expect("nonzero target");
        assert_eq!(
            target.to_le_bytes(),
            std::array::from_fn(|index| 32 - index as u8)
        );
        assert_eq!(display_target(target), display);
        assert!(parse_display_target(&"00".repeat(32), "target").is_err());
    }

    #[test]
    fn network_share_target_tracks_the_easier_exact_network_target() {
        let target = |most_significant| {
            let mut bytes = [0; 32];
            bytes[31] = most_significant;
            Target::from_le_bytes(bytes).expect("fixture target is nonzero")
        };
        let child = target(0x20);
        let easier_parent = target(0x30);

        let selection = parse_share_target_selection("network")
            .expect("the full-coverage network policy is valid");
        assert_eq!(selection, ShareTargetSelection::NetworkTargets);
        assert_eq!(selection.resolve(child, easier_parent), easier_parent);
        assert_eq!(selection.resolve(easier_parent, child), easier_parent);
        assert!(selection.resolve(child, easier_parent).includes(child));
        assert!(selection
            .resolve(child, easier_parent)
            .includes(easier_parent));

        let fixed = parse_share_target_selection(&display_target(child))
            .expect("an exact fixed target remains valid");
        assert_eq!(fixed, ShareTargetSelection::Fixed(child));
        assert_eq!(fixed.resolve(easier_parent, easier_parent), child);
        assert!(parse_share_target_selection("auto").is_err());
    }

    #[test]
    fn backend_listener_default_reserves_full_pool_bootstrap_capacity() {
        assert_eq!(parse_pool_backend_listener_workers(None).unwrap(), 4);
        for workers in [1, 2, 3, 4, 16] {
            assert_eq!(
                parse_pool_backend_listener_workers(Some(workers.to_string())).unwrap(),
                workers
            );
        }
        for value in ["0", "17", "", "-1", "four"] {
            assert!(parse_pool_backend_listener_workers(Some(value.to_string())).is_err());
        }
    }

    #[test]
    fn backend_authority_json_preserves_exact_bindings_and_displays_target_big_endian() {
        let wcash_genesis = Hex32::new(std::array::from_fn(|index| index as u8 + 1));
        let zcash_genesis = Hex32::new(std::array::from_fn(|index| index as u8 + 33));
        let wcash_payout = Hex32::new(std::array::from_fn(|index| index as u8 + 65));
        let zcash_payout = Hex32::new(std::array::from_fn(|index| index as u8 + 97));
        let target_display_bytes = std::array::from_fn(|index| index as u8 + 129);
        let mut target_le_bytes = target_display_bytes;
        target_le_bytes.reverse();

        let authority = Value::Object(pool_backend_authority_json_fields(
            &wcash_genesis,
            &zcash_genesis,
            &wcash_payout,
            &zcash_payout,
            &TargetLe::new(target_le_bytes),
        ));

        assert_eq!(authority.as_object().unwrap().len(), 6);
        assert_eq!(authority["wcash_genesis"], wcash_genesis.to_string());
        assert_eq!(authority["zcash_genesis"], zcash_genesis.to_string());
        assert_eq!(
            authority["wcash_payout_commitment"],
            wcash_payout.to_string()
        );
        assert_eq!(
            authority["zcash_payout_commitment"],
            zcash_payout.to_string()
        );
        assert_eq!(
            authority["share_target_ceiling"],
            hex::encode(target_display_bytes)
        );
        assert_ne!(
            authority["share_target_ceiling"],
            hex::encode(target_le_bytes),
            "the nonsymmetric target must not leak backend little-endian order"
        );
        assert_eq!(authority["share_target_ceiling_byte_order"], "big_endian");
    }

    #[test]
    fn parent_target_sampling_requires_exact_testnet_opt_in() {
        let wcash_testnet_genesis = WCASH_TESTNET_GENESIS_HASH;
        for network in [
            NativeZcashNetwork::Mainnet,
            NativeZcashNetwork::Testnet,
            NativeZcashNetwork::Regtest,
        ] {
            assert!(
                !parse_testnet_parent_target_sampling(None, network, wcash_testnet_genesis)
                    .expect("an absent opt-in preserves full coverage")
            );
        }
        assert!(parse_testnet_parent_target_sampling(
            Some("1".to_string()),
            NativeZcashNetwork::Testnet,
            wcash_testnet_genesis,
        )
        .expect("exact Testnet opt-in is accepted"));
        for network in [NativeZcashNetwork::Mainnet, NativeZcashNetwork::Regtest] {
            assert!(parse_testnet_parent_target_sampling(
                Some("1".to_string()),
                network,
                wcash_testnet_genesis,
            )
            .is_err());
        }
        for value in ["", "0", "true", "yes", " 1"] {
            assert!(parse_testnet_parent_target_sampling(
                Some(value.to_string()),
                NativeZcashNetwork::Testnet,
                wcash_testnet_genesis,
            )
            .is_err());
        }
        assert!(parse_testnet_parent_target_sampling(
            Some("1".to_string()),
            NativeZcashNetwork::Testnet,
            &"00".repeat(32),
        )
        .is_err());
    }

    #[test]
    fn preflight_exposes_degraded_parent_coverage_without_weakening_child_coverage() {
        let target = |most_significant| {
            let mut bytes = [0; 32];
            bytes[31] = most_significant;
            Target::from_le_bytes(bytes).expect("fixture target is nonzero")
        };
        let child = target(0x20);
        let easier_parent = target(0x30);

        assert!(share_target_policy_preflight(
            child,
            child,
            easier_parent,
            NativeZcashNetwork::Testnet,
            false,
            false,
        )
        .is_err());
        let sampled = share_target_policy_preflight(
            child,
            child,
            easier_parent,
            NativeZcashNetwork::Testnet,
            true,
            true,
        )
        .expect("explicit sampling keeps complete child coverage");
        assert_eq!(sampled["mode"], "testnet_parent_target_sampling");
        assert_eq!(sampled["captures_all_wcash_winners"], true);
        assert_eq!(sampled["captures_all_zcash_winners"], false);
        assert_eq!(sampled["degraded_parent_coverage"], true);
        assert_eq!(sampled["rotation_after_first_durable_network_winner"], true);
        let full = share_target_policy_preflight(
            easier_parent,
            child,
            easier_parent,
            NativeZcashNetwork::Testnet,
            false,
            true,
        )
        .expect("network-derived target captures both networks");
        assert_eq!(full["mode"], "full_network_winner_coverage");
        assert_eq!(full["captures_all_wcash_winners"], true);
        assert_eq!(full["captures_all_zcash_winners"], true);
        assert_eq!(full["degraded_parent_coverage"], false);
        assert_eq!(full["rotation_after_first_durable_network_winner"], true);
        assert!(share_target_policy_preflight(
            target(0x1f),
            child,
            easier_parent,
            NativeZcashNetwork::Testnet,
            true,
            true,
        )
        .is_err());
        for network in [NativeZcashNetwork::Mainnet, NativeZcashNetwork::Regtest] {
            assert!(share_target_policy_preflight(
                child,
                child,
                easier_parent,
                network,
                true,
                true
            )
            .is_err());
        }
    }

    #[test]
    fn generation_retirement_requires_a_successful_outbox_flush() {
        let retirement_called = std::cell::Cell::new(false);
        let result = retire_after_outbox_flush(
            Err(MinerError::InvalidRequest(
                "fixture durable outbox failure".to_string(),
            )),
            || {
                retirement_called.set(true);
                Ok(GenerationRetirement::Retired)
            },
        );
        assert!(result.is_err());
        assert!(!retirement_called.get());

        let result = retire_after_outbox_flush(Ok(()), || {
            retirement_called.set(true);
            Ok(GenerationRetirement::RetainedForWcashWinner)
        })
        .expect("a successful flush permits safe retirement");
        assert!(retirement_called.get());
        assert_eq!(result, GenerationRetirement::RetainedForWcashWinner);
    }

    #[test]
    fn pool_backend_retry_wait_wakes_for_shutdown_and_worker_failure() {
        let shutdown = AtomicBool::new(true);
        let (_failure_sender, failures) = mpsc::channel();
        assert!(matches!(
            wait_for_pool_backend_retry(Duration::from_secs(1), &shutdown, &failures),
            PoolBackendGenerationControl::Shutdown
        ));

        let shutdown = AtomicBool::new(false);
        let (failure_sender, failures) = mpsc::channel();
        failure_sender
            .send("durable winner journal failed".to_string())
            .expect("failure channel remains open");
        assert!(matches!(
            wait_for_pool_backend_retry(Duration::from_secs(1), &shutdown, &failures),
            PoolBackendGenerationControl::ServiceFailure(error)
                if error == "durable winner journal failed"
        ));

        let (_failure_sender, failures) = mpsc::channel();
        assert!(matches!(
            wait_for_pool_backend_retry(Duration::ZERO, &shutdown, &failures),
            PoolBackendGenerationControl::Rotate
        ));
    }

    #[test]
    fn preparation_retry_accepts_only_explicit_transient_http_and_rpc_failures() {
        assert!(is_retryable_winner_reconciliation_error(
            &MinerError::WinnerSubmissionDeferred { chain: "Wcash" }
        ));
        assert!(!is_retryable_winner_reconciliation_error(
            &MinerError::InvalidParentTemplate("authoritative winner rejection".to_string())
        ));
        for status in [408, 425, 429, 500, 502, 503, 504] {
            let status = reqwest::StatusCode::from_u16(status).expect("valid HTTP status");
            assert!(
                is_retryable_native_preparation_error(&MinerError::RpcHttpStatus(status)),
                "HTTP {status} must be retryable"
            );
        }
        for status in [400, 401, 403, 404, 405, 409, 422, 501, 505] {
            let status = reqwest::StatusCode::from_u16(status).expect("valid HTTP status");
            assert!(
                !is_retryable_native_preparation_error(&MinerError::RpcHttpStatus(status)),
                "HTTP {status} must stop the supervisor"
            );
        }

        for code in [-9, -10, -28] {
            assert!(is_retryable_native_preparation_error(&rpc_error(Some(
                code
            ))));
        }
        for code in [None, Some(-1), Some(-5), Some(-8), Some(-32601)] {
            assert!(
                !is_retryable_native_preparation_error(&rpc_error(code)),
                "RPC code {code:?} must stop the supervisor"
            );
        }
    }

    #[test]
    fn preparation_retry_distinguishes_transient_and_permanent_io() {
        for kind in [
            io::ErrorKind::ConnectionRefused,
            io::ErrorKind::ConnectionReset,
            io::ErrorKind::HostUnreachable,
            io::ErrorKind::NetworkUnreachable,
            io::ErrorKind::ConnectionAborted,
            io::ErrorKind::NotConnected,
            io::ErrorKind::NetworkDown,
            io::ErrorKind::BrokenPipe,
            io::ErrorKind::WouldBlock,
            io::ErrorKind::TimedOut,
            io::ErrorKind::Interrupted,
        ] {
            assert!(
                is_retryable_native_preparation_error(&MinerError::Io(io::Error::from(kind))),
                "I/O kind {kind:?} must be retryable"
            );
        }
        for kind in [
            io::ErrorKind::NotFound,
            io::ErrorKind::PermissionDenied,
            io::ErrorKind::AlreadyExists,
            io::ErrorKind::InvalidInput,
            io::ErrorKind::InvalidData,
            io::ErrorKind::WriteZero,
            io::ErrorKind::Unsupported,
            io::ErrorKind::UnexpectedEof,
            io::ErrorKind::Other,
        ] {
            assert!(
                !is_retryable_native_preparation_error(&MinerError::Io(io::Error::from(kind))),
                "I/O kind {kind:?} must stop the supervisor"
            );
        }
    }

    #[test]
    fn preparation_retry_accepts_timeout_but_rejects_request_builder_errors() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0")
            .expect("loopback listener must be available");
        let address = listener.local_addr().expect("listener has a local address");
        let server = thread::spawn(move || {
            let (_stream, _) = listener.accept().expect("client connects");
            thread::sleep(Duration::from_millis(100));
        });
        let timeout = reqwest::blocking::Client::builder()
            .no_proxy()
            .timeout(Duration::from_millis(20))
            .build()
            .expect("client configuration is valid")
            .get(format!("http://{address}"))
            .send()
            .expect_err("silent server must exceed the request deadline");
        assert!(timeout.is_timeout());
        assert!(is_retryable_native_preparation_error(
            &MinerError::RpcTransport(timeout)
        ));
        server.join().expect("server exits normally");

        let builder_error = reqwest::blocking::Client::new()
            .get("://invalid-url")
            .build()
            .expect_err("invalid URL must fail request construction");
        assert!(builder_error.is_builder());
        assert!(!is_retryable_native_preparation_error(
            &MinerError::RpcTransport(builder_error)
        ));
    }

    #[test]
    fn preparation_retry_allows_tip_races_but_stops_security_failures() {
        assert!(is_retryable_native_preparation_error(
            &MinerError::ParentTipMismatch {
                expected: "parent-a".to_string(),
                endpoint: "validator".to_string(),
                actual: "parent-b".to_string(),
            }
        ));
        assert!(is_retryable_native_preparation_error(
            &MinerError::ChildTipMismatch {
                expected: "child-a".to_string(),
                endpoint: "child".to_string(),
                actual: "child-b".to_string(),
            }
        ));
        assert!(is_retryable_native_preparation_error(
            &MinerError::StaleNativeJob("candidate expired during preparation".to_string())
        ));

        let permanent = [
            MinerError::RpcConfiguration("unsafe endpoint".to_string()),
            MinerError::RpcProtocol("malformed response".to_string()),
            MinerError::RpcResponseTooLarge(8 * 1024 * 1024),
            MinerError::ParentProposalRejected {
                endpoint: "validator".to_string(),
                reason: "rejected".to_string(),
            },
            MinerError::NetworkIdentityMismatch {
                expected: "expected-genesis".to_string(),
                endpoint: "node".to_string(),
                actual: "foreign-genesis".to_string(),
            },
            MinerError::InvalidParentTemplate("payout commitment mismatch".to_string()),
            MinerError::InvalidRequest("share journal is corrupted".to_string()),
            MinerError::CoinbaseSerialization(io::Error::from(io::ErrorKind::ConnectionReset)),
        ];
        for error in permanent {
            assert!(
                !is_retryable_native_preparation_error(&error),
                "{error} must stop the supervisor"
            );
        }
    }

    #[test]
    fn preparation_retry_uses_short_fixed_delay_for_tip_races() {
        let parent_tip_race = MinerError::ParentTipMismatch {
            expected: "parent-a".to_string(),
            endpoint: "validator".to_string(),
            actual: "parent-b".to_string(),
        };
        let child_tip_race = MinerError::ChildTipMismatch {
            expected: "child-a".to_string(),
            endpoint: "child".to_string(),
            actual: "child-b".to_string(),
        };
        let stale_job = MinerError::StaleNativeJob("durably activated job".to_string());

        for error in [&parent_tip_race, &child_tip_race, &stale_job] {
            assert_eq!(
                native_preparation_retry_delay(error, Duration::from_secs(32)),
                TIP_RACE_RETRY_DELAY
            );
            assert_eq!(
                next_native_preparation_backoff(error, Duration::from_secs(32)),
                INITIAL_NATIVE_PREPARATION_BACKOFF
            );
        }

        let unavailable = rpc_error(Some(-28));
        assert_eq!(
            native_preparation_retry_delay(&unavailable, Duration::from_secs(32)),
            Duration::from_secs(32)
        );
        assert_eq!(
            next_native_preparation_backoff(&unavailable, Duration::from_secs(32)),
            MAX_NATIVE_PREPARATION_BACKOFF
        );
    }

    fn rpc_error(code: Option<i64>) -> MinerError {
        MinerError::RpcError {
            endpoint: "node".to_string(),
            code,
            message: "test error".to_string(),
        }
    }
}
