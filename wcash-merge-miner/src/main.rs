//! Command-line entry point for Wcash/Zcash merged-mining operations.

use std::{
    env,
    error::Error,
    io::{self, Write},
    net::SocketAddr,
    path::PathBuf,
    process,
    sync::Arc,
    thread,
    time::{Duration, SystemTime},
};

use serde_json::{json, Value};
use wcash_merge_miner::{
    accounting::{hash_worker_password, read_accounting_snapshot, WorkerCredentialStore},
    mine_zip301_once,
    protocol::serve_loopback,
    rpc::RpcEndpoint,
    zip301::{
        DEFAULT_ZIP301_AUTHENTICATION_LIMIT, DEFAULT_ZIP301_CLIENT_LIMIT,
        DEFAULT_ZIP301_VALIDATION_LIMIT,
    },
    CoordinatorConfig, GenerationRetirement, JobConfig, MinerError, NativeMiningCoordinator,
    NativeMiningSupervisor, NativeZcashConfig, PreparedJob, ShareProcessor, Zip301ClientConfig,
    Zip301Config, Zip301LoopbackListener, NATIVE_JOB_MAX_AGE_SECONDS,
};
use wcash_zcash_aux::Target;
use zcash_address::ZcashAddress;

const DEFAULT_BIND: &str = "127.0.0.1:28237";
const DEFAULT_SHARE_JOURNAL: &str = ".wcash-share-journal-v2.jsonl";

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
const ZCASH_PAYOUT_ADDRESS: &str = "ZCASH_PAYOUT_ADDRESS";
const WCASH_VALIDATION_LIMIT: &str = "WCASH_VALIDATION_LIMIT";
const WCASH_AUTHENTICATION_LIMIT: &str = "WCASH_AUTHENTICATION_LIMIT";
const WCASH_EXPECTED_GENESIS_HASH: &str = "WCASH_EXPECTED_GENESIS_HASH";
const ZCASH_EXPECTED_GENESIS_HASH: &str = "ZCASH_EXPECTED_GENESIS_HASH";

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

The Wcash and Zcash template URLs must be loopback endpoints because those nodes
choose the block-reward recipients. The distinct Zcash proposal validator may be
remote over HTTPS; HTTP is accepted only on loopback. RPC credentials may
only be supplied through these optional, paired environment variables:
  WCASH_RPC_USERNAME / WCASH_RPC_PASSWORD
  ZCASH_TEMPLATE_RPC_USERNAME / ZCASH_TEMPLATE_RPC_PASSWORD
  ZCASH_VALIDATOR_RPC_USERNAME / ZCASH_VALIDATOR_RPC_PASSWORD

Every native command requires WCASH_EXPECTED_GENESIS_HASH and
ZCASH_EXPECTED_GENESIS_HASH (64 hex characters in conventional RPC display
order). Every node is pinned to those height-zero hashes before work is issued.

Both native-serve commands additionally require WCASH_WORKER_CREDENTIALS (the
path to a private version-1 exact-worker registry) and WCASH_SHARE_TARGET
(exactly 32 bytes of conventional big-endian target hex).
The wcash-address argument must be `-`; its value is read from
WCASH_PAYOUT_ADDRESS to keep it out of process listings and preflight logs.
ZCASH_PAYOUT_ADDRESS must exactly match the canonical mining.miner_address on
the parent template node and every proposal validator. Transparent addresses
are the normal pool-integration default on both chains; a Wcash Unified Address
with an Orchard receiver explicitly selects private Ironwood payout. The
coordinator checks a domain-separated private-GBT commitment and verifies the
configured payout in the exact Zcash coinbases. It also verifies an exact
transparent Wcash recipient, or the private-only shape and value of an Ironwood
reward. A private Wcash recipient remains a trust boundary at the loopback
template node. Plaintext payout configuration is omitted from diagnostics, but
native preflight prints the exact Zcash coinbase.
WCASH_VALIDATION_LIMIT optionally sets the global concurrent share-validation
limit (default 4, maximum 1024). WCASH_AUTHENTICATION_LIMIT optionally sets the
global concurrent Argon2id verification limit (default 4, maximum 256).
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
    let share_target =
        parse_display_target(&required_env(WCASH_SHARE_TARGET)?, "WCASH_SHARE_TARGET")?;
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
    let zip301 = Zip301Config::new_with_worker_authenticator(share_target, Arc::new(credentials))
        .with_maximum_clients(arguments.maximum_clients)?
        .with_maximum_parallel_authentications(maximum_parallel_authentications)?
        .with_maximum_parallel_validations(maximum_parallel_validations)?;
    let listener = Zip301LoopbackListener::bind(arguments.bind)?;

    let journal = share_journal_path()?;
    let configured = configure_native(&arguments.connection)?;
    let supervisor = NativeMiningSupervisor::open(configured.config.clone(), &journal)?;
    let command = if automatic_rotation {
        "native-serve"
    } else {
        "native-serve-once"
    };
    let mut preparation_backoff = Duration::from_secs(1);

    loop {
        let mut coordinator = match supervisor.prepare_generation() {
            Ok(coordinator) => {
                preparation_backoff = Duration::from_secs(1);
                coordinator
            }
            Err(error) if automatic_rotation && is_retryable_native_preparation_error(&error) => {
                eprintln!(
                    "transient native job preparation failure: {error}; retrying in {} second(s)",
                    preparation_backoff.as_secs()
                );
                thread::sleep(preparation_backoff);
                preparation_backoff = (preparation_backoff * 2).min(Duration::from_secs(60));
                continue;
            }
            Err(error) => return Err(error.into()),
        };
        let job = coordinator.job().clone();

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
        retire_native_generation(&mut coordinator, automatic_rotation)?;

        match serve_result {
            Err(error) if automatic_rotation && is_retryable_native_preparation_error(&error) => {
                eprintln!("rotating/retrying native job after a transient failure: {error}");
            }
            Err(error) => return Err(error.into()),
            Ok(()) => return Ok(()),
        }
    }
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
}

struct EndpointSummary {
    wcash_label: String,
    wcash_authenticated: bool,
    zcash_template_label: String,
    zcash_template_authenticated: bool,
    zcash_validator_label: String,
    zcash_validator_authenticated: bool,
    wcash_payout_address_source: &'static str,
    zcash_payout_address_source: &'static str,
}

fn configure_native(arguments: &NativeConnectionArguments) -> Result<ConfiguredNative, MinerError> {
    let wcash_payout_address = required_payout_address_env(WCASH_PAYOUT_ADDRESS)?;
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
    let summary = EndpointSummary {
        wcash_label: wcash_node.label().to_string(),
        wcash_authenticated,
        zcash_template_label: template_node.label().to_string(),
        zcash_template_authenticated,
        zcash_validator_label: validator_node.label().to_string(),
        zcash_validator_authenticated,
        wcash_payout_address_source: WCASH_PAYOUT_ADDRESS,
        zcash_payout_address_source: ZCASH_PAYOUT_ADDRESS,
    };
    let expected_wcash_genesis_hash = required_display_hash_env(WCASH_EXPECTED_GENESIS_HASH)?;
    let expected_zcash_genesis_hash = required_display_hash_env(ZCASH_EXPECTED_GENESIS_HASH)?;
    let zcash = NativeZcashConfig::new(
        template_node,
        vec![validator_node],
        expected_zcash_genesis_hash,
        expected_parent_payout_address,
    )?;

    Ok(ConfiguredNative {
        config: CoordinatorConfig {
            wcash_node,
            expected_wcash_genesis_hash,
            zcash,
            wcash_payout_address,
            auxiliary_nonce: arguments.auxiliary_nonce,
        },
        summary,
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
    worker_count: usize,
    automatic_rotation: bool,
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
    let lifecycle_message = if serve.is_some_and(|serve| serve.automatic_rotation) {
        "one exact proposal-validated generation is active; the process retires it and prepares fresh work whenever either chain tip changes or the Wcash candidate expires"
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
        output["listener"] = json!({
            "bind": serve.bind.to_string(),
            "loopback_only": true,
            "maximum_clients": serve.maximum_clients,
            "maximum_parallel_authentications": serve.maximum_parallel_authentications,
            "maximum_parallel_validations": serve.maximum_parallel_validations,
            "share_target": display_target(serve.share_target),
            "authentication": "exact-worker-argon2id",
            "worker_count": serve.worker_count,
            "worker_credentials_source": WCASH_WORKER_CREDENTIALS,
        });
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
    fn preparation_retry_accepts_only_explicit_transient_http_and_rpc_failures() {
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

    fn rpc_error(code: Option<i64>) -> MinerError {
        MinerError::RpcError {
            endpoint: "node".to_string(),
            code,
            message: "test error".to_string(),
        }
    }
}
