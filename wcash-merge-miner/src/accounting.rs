//! Exact worker authentication and read-only accounting for the winner journal.
//!
//! The coordinator's fsynced version-2 journal is the sole source of truth for
//! accepted shares and network winners. Keeping authentication and reporting on
//! that journal avoids a second-ledger crash window between winner durability
//! and worker attribution.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs::{self, File},
    io::{BufRead, BufReader, Read},
    path::Path,
    sync::Arc,
};

use argon2::{
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2,
};
use rand_core::OsRng;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    coordinator::{
        validate_persisted_winner_block, PersistedWinnerBlockKind, MAX_JOURNAL_GENERATIONS,
    },
    MinerError,
};

/// Maximum encoded worker-credential file size.
pub const MAX_CREDENTIAL_FILE_BYTES: u64 = 1024 * 1024;

/// Maximum number of exact worker identities loaded by one pool process.
pub const MAX_WORKER_CREDENTIALS: usize = 10_000;

/// Maximum share journal size accepted by the offline accounting reporter.
pub const MAX_ACCOUNTING_JOURNAL_BYTES: u64 = 1024 * 1024 * 1024;

/// Maximum unique accepted shares retained in one accounting snapshot.
pub const MAX_ACCOUNTING_SHARES: usize = 1_000_000;

/// Maximum number of durable mining generations retained during strict replay.
pub const MAX_ACCOUNTING_GENERATIONS: usize = MAX_JOURNAL_GENERATIONS;

const CREDENTIAL_FILE_VERSION: u8 = 1;
const JOURNAL_VERSION: u8 = 2;
const MAX_JOURNAL_RECORD_BYTES: usize = 10 * 1024 * 1024;
const MIN_PASSWORD_BYTES: usize = 12;
const MAX_PASSWORD_BYTES: usize = 1_024;
const MAX_PASSWORD_HASH_BYTES: usize = 512;
const MAX_WINNER_BLOCK_BYTES: usize = 2_000_000;
const ARGON2_MEMORY_KIB: u32 = 19_456;
const ARGON2_ITERATIONS: u32 = 2;
const ARGON2_LANES: u32 = 1;

/// Non-secret provenance for a canonical worker authentication.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum WorkerAuthenticationProvenance {
    /// An independently passworded worker credential or custom authenticator.
    ExactCredential,
    /// A legacy pool-wide shared secret.
    SharedSecret,
}

impl WorkerAuthenticationProvenance {
    /// Returns the stable value written to the authoritative share journal.
    pub const fn journal_name(self) -> &'static str {
        match self {
            Self::ExactCredential => "exact_credential",
            Self::SharedSecret => "shared_secret",
        }
    }
}

/// A canonical worker identity authenticated by the pool.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct AuthenticatedWorker {
    name: Arc<str>,
    provenance: WorkerAuthenticationProvenance,
}

impl AuthenticatedWorker {
    /// Creates an exact canonical identity for a custom authentication provider.
    pub fn new(name: impl Into<String>) -> Result<Self, MinerError> {
        Self::with_provenance(name, WorkerAuthenticationProvenance::ExactCredential)
    }

    pub(crate) fn from_shared_secret(name: impl Into<String>) -> Result<Self, MinerError> {
        Self::with_provenance(name, WorkerAuthenticationProvenance::SharedSecret)
    }

    fn with_provenance(
        name: impl Into<String>,
        provenance: WorkerAuthenticationProvenance,
    ) -> Result<Self, MinerError> {
        let name = name.into();
        validate_worker_name(&name)?;
        Ok(Self {
            name: Arc::from(name),
            provenance,
        })
    }

    /// Returns the identity written to the authoritative share journal.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the non-secret authentication provenance written to the journal.
    pub const fn provenance(&self) -> WorkerAuthenticationProvenance {
        self.provenance
    }
}

/// Resolves a miner-supplied login and password to one canonical identity.
pub trait WorkerAuthenticator: Send + Sync + 'static {
    /// Returns an authenticated identity without distinguishing an unknown
    /// login from an incorrect password.
    fn authenticate(&self, worker: &str, password: &str) -> Option<AuthenticatedWorker>;
}

/// Immutable, independently passworded worker registry.
#[derive(Clone)]
pub struct WorkerCredentialStore {
    workers: Arc<HashMap<String, String>>,
    dummy_password_hash: Arc<str>,
}

impl std::fmt::Debug for WorkerCredentialStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WorkerCredentialStore")
            .field("worker_count", &self.workers.len())
            .field("password_hashes", &"[REDACTED]")
            .finish()
    }
}

impl WorkerCredentialStore {
    /// Loads a strict credential registry from a private regular file.
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, MinerError> {
        let path = path.as_ref();
        if path.as_os_str().is_empty() {
            return Err(invalid("worker credential path is empty"));
        }
        reject_symlink(path, "worker credential file")?;
        let file = File::open(path)?;
        validate_private_file_and_parent(&file, path, "worker credential file")?;
        let length = file.metadata()?.len();
        if length > MAX_CREDENTIAL_FILE_BYTES {
            return Err(invalid(format!(
                "worker credential file {} exceeds {MAX_CREDENTIAL_FILE_BYTES} bytes",
                path.display()
            )));
        }
        let mut bytes = Vec::with_capacity(usize::try_from(length).unwrap_or(0));
        file.take(MAX_CREDENTIAL_FILE_BYTES + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() as u64 > MAX_CREDENTIAL_FILE_BYTES {
            return Err(invalid(format!(
                "worker credential file {} grew beyond {MAX_CREDENTIAL_FILE_BYTES} bytes while loading",
                path.display()
            )));
        }
        Self::from_json(&bytes)
    }

    /// Loads credential JSON that already crossed a trusted file boundary.
    pub fn from_json(bytes: &[u8]) -> Result<Self, MinerError> {
        if bytes.len() as u64 > MAX_CREDENTIAL_FILE_BYTES {
            return Err(invalid(format!(
                "worker credential JSON exceeds {MAX_CREDENTIAL_FILE_BYTES} bytes"
            )));
        }
        let config: CredentialFile = serde_json::from_slice(bytes)
            .map_err(|error| invalid(format!("worker credential file is invalid: {error}")))?;
        if config.version != CREDENTIAL_FILE_VERSION {
            return Err(invalid(format!(
                "worker credential file version {} is unsupported",
                config.version
            )));
        }
        if config.workers.is_empty() || config.workers.len() > MAX_WORKER_CREDENTIALS {
            return Err(invalid(format!(
                "worker credential file must contain 1..={MAX_WORKER_CREDENTIALS} workers"
            )));
        }

        let mut workers = HashMap::with_capacity(config.workers.len());
        for credential in config.workers {
            validate_worker_name(&credential.name)?;
            validate_password_hash(&credential.password_hash)?;
            if workers
                .insert(credential.name.clone(), credential.password_hash)
                .is_some()
            {
                return Err(invalid(format!(
                    "worker credential file defines {:?} more than once",
                    credential.name
                )));
            }
        }

        let dummy_salt = SaltString::encode_b64(b"wcash-auth-dummy")
            .map_err(|error| invalid(format!("could not initialize authentication: {error}")))?;
        let dummy_password_hash = Argon2::default()
            .hash_password(b"unconfigured worker password", &dummy_salt)
            .map_err(|error| invalid(format!("could not initialize authentication: {error}")))?
            .to_string();
        Ok(Self {
            workers: Arc::new(workers),
            dummy_password_hash: Arc::from(dummy_password_hash),
        })
    }

    /// Authenticates one exact worker with the configured Argon2id credential.
    pub fn authenticate(&self, worker: &str, password: &str) -> Option<AuthenticatedWorker> {
        if validate_worker_name(worker).is_err()
            || !(MIN_PASSWORD_BYTES..=MAX_PASSWORD_BYTES).contains(&password.len())
        {
            return None;
        }
        let configured = self.workers.get(worker);
        let encoded = configured
            .map(String::as_str)
            .unwrap_or(&self.dummy_password_hash);
        let parsed = PasswordHash::new(encoded).ok()?;
        let verified = Argon2::default()
            .verify_password(password.as_bytes(), &parsed)
            .is_ok();
        if configured.is_none() || !verified {
            return None;
        }
        AuthenticatedWorker::new(worker).ok()
    }

    /// Returns the number of configured exact workers.
    pub fn len(&self) -> usize {
        self.workers.len()
    }

    /// Returns true when the registry contains no workers.
    pub fn is_empty(&self) -> bool {
        self.workers.is_empty()
    }
}

impl WorkerAuthenticator for WorkerCredentialStore {
    fn authenticate(&self, worker: &str, password: &str) -> Option<AuthenticatedWorker> {
        Self::authenticate(self, worker, password)
    }
}

/// Generates the exact Argon2id PHC format accepted by a credential registry.
pub fn hash_worker_password(password: &str) -> Result<String, MinerError> {
    if !(MIN_PASSWORD_BYTES..=MAX_PASSWORD_BYTES).contains(&password.len()) {
        return Err(invalid(format!(
            "worker password must contain {MIN_PASSWORD_BYTES}..={MAX_PASSWORD_BYTES} bytes"
        )));
    }
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|hash| hash.to_string())
        .map_err(|error| invalid(format!("could not hash worker password: {error}")))
}

/// A strict aggregate reconstructed from the authoritative version-2 journal.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct AccountingSnapshot {
    /// Number of unique valid accepted-share or winner records.
    pub accepted_shares: u64,
    /// Accepted shares grouped by authentication provenance.
    pub shares_by_authentication: BTreeMap<String, u64>,
    /// Per-worker counters keyed by canonical authenticated identity.
    pub workers: BTreeMap<String, WorkerAccounting>,
}

/// Accounting counters for one canonical worker identity.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct WorkerAccounting {
    /// Number of unique accepted shares.
    pub accepted_shares: u64,
    /// Accepted shares grouped by authentication provenance.
    pub shares_by_authentication: BTreeMap<String, u64>,
    /// Wcash network winners.
    pub wcash_winners: u64,
    /// Zcash network winners.
    pub zcash_winners: u64,
    /// Accepted shares grouped by exact big-endian target.
    pub shares_by_target: BTreeMap<String, u64>,
}

/// Reads a stable accounting snapshot without modifying the share journal.
///
/// A running coordinator holds an exclusive lock, so callers must stop the
/// pool before producing a final payout/accounting report. One unterminated
/// crash tail is ignored; terminated corruption fails closed.
pub fn read_accounting_snapshot(path: impl AsRef<Path>) -> Result<AccountingSnapshot, MinerError> {
    let path = path.as_ref();
    reject_symlink(path, "share journal")?;
    let file = File::open(path)?;
    fs2::FileExt::try_lock_shared(&file).map_err(|error| {
        invalid(format!(
            "share journal {} is in use or cannot be read consistently: {error}",
            path.display()
        ))
    })?;
    validate_private_file_and_parent(&file, path, "share journal")?;
    if file.metadata()?.len() > MAX_ACCOUNTING_JOURNAL_BYTES {
        return Err(invalid(format!(
            "share journal {} exceeds {MAX_ACCOUNTING_JOURNAL_BYTES} bytes",
            path.display()
        )));
    }

    let mut reader = BufReader::new(file);
    read_accounting_snapshot_from_reader(&mut reader)
}

/// Validates and accounts one already-opened journal stream.
///
/// The online coordinator uses this same parser before rebuilding its winner
/// outbox, so a journal accepted for new share ACKs can never be rejected later
/// by the offline payout report because of a schema or history mismatch.
pub(crate) fn read_accounting_snapshot_from_reader(
    reader: &mut impl BufRead,
) -> Result<AccountingSnapshot, MinerError> {
    let mut accepted = HashMap::<[u8; 32], ImmutableShare>::new();
    let mut pending_winners = HashMap::<WinnerKey, PendingWinnerStatus>::new();
    let mut used_jobs = HashSet::<[u8; 32]>::new();
    let mut snapshot = AccountingSnapshot::default();
    while let Some((line, terminated)) = read_bounded_line(reader)? {
        if !terminated {
            break;
        }
        let record: JournalRecord = serde_json::from_slice(&line)
            .map_err(|error| invalid(format!("share journal is corrupted: {error}")))?;
        if record.version != JOURNAL_VERSION {
            return Err(invalid(format!(
                "share journal contains unsupported version {} record {:?}",
                record.version, record.record
            )));
        }
        let job_id = validate_canonical_hex::<32>(&record.job_id, "job_id")?;
        match record.record.as_str() {
            "job_activated" => {
                validate_job_activation(&record)?;
                if used_jobs.contains(&job_id) {
                    return Err(invalid("share journal activates a previously used job ID"));
                }
                ensure_accounting_generation_capacity(used_jobs.len(), false)?;
                used_jobs.insert(job_id);
            }
            "accepted_share" | "winner_outbox" => {
                ensure_accounting_generation_capacity(
                    used_jobs.len(),
                    used_jobs.contains(&job_id),
                )?;
                used_jobs.insert(job_id);
                let mut immutable = immutable_share(&record)?;
                let share_id = validate_share_id(&record, &immutable)?;
                let validated_winners = validate_winner_blocks(&record, share_id)?;
                immutable.winner_fingerprint = validated_winners.fingerprint;
                match accepted.get(&share_id) {
                    Some(previous) if previous == &immutable => continue,
                    Some(_) => {
                        return Err(invalid(
                            "share journal assigns one share ID to conflicting accounting data",
                        ));
                    }
                    None => {}
                }
                if accepted.len() >= MAX_ACCOUNTING_SHARES {
                    return Err(invalid(format!(
                        "share journal contains more than {MAX_ACCOUNTING_SHARES} accepted shares"
                    )));
                }
                apply_share(&mut snapshot, &immutable)?;
                accepted.insert(share_id, immutable);
                for (key, winner) in validated_winners.entries {
                    if pending_winners.insert(key, winner).is_some() {
                        return Err(invalid(
                            "share journal defines one network winner more than once",
                        ));
                    }
                }
            }
            "winner_observed" | "winner_orphaned" | "winner_matured" | "winner_confirmed" => {
                apply_winner_status(&record, &mut pending_winners)?;
            }
            _ => {
                return Err(invalid(format!(
                    "share journal contains unsupported version {} record {:?}",
                    record.version, record.record
                )));
            }
        }
    }
    Ok(snapshot)
}

fn ensure_accounting_generation_capacity(
    count: usize,
    already_seen: bool,
) -> Result<(), MinerError> {
    if !already_seen && count >= MAX_ACCOUNTING_GENERATIONS {
        return Err(invalid(format!(
            "share journal contains more than {MAX_ACCOUNTING_GENERATIONS} mining generations"
        )));
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CredentialFile {
    version: u8,
    workers: Vec<WorkerCredential>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkerCredential {
    name: String,
    password_hash: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct JournalRecord {
    version: u8,
    record: String,
    job_id: String,
    #[serde(default)]
    share_id: Option<String>,
    #[serde(default)]
    accepted_at: Option<u64>,
    #[serde(default)]
    recorded_at: Option<u64>,
    #[serde(default)]
    worker: Option<String>,
    #[serde(default)]
    worker_authentication: Option<String>,
    #[serde(default)]
    share_target: Option<String>,
    #[serde(default)]
    parent_hash_le: Option<String>,
    #[serde(default)]
    wcash_candidate: Option<bool>,
    #[serde(default)]
    zcash_candidate: Option<bool>,
    #[serde(default)]
    wcash_block_hash: Option<String>,
    #[serde(default)]
    wcash_height: Option<u32>,
    #[serde(default)]
    zcash_height: Option<u32>,
    #[serde(default)]
    wcash_block: Option<String>,
    #[serde(default)]
    zcash_block: Option<String>,
    #[serde(default)]
    chain: Option<String>,
    #[serde(default)]
    block_hash: Option<String>,
    #[serde(default)]
    height: Option<u32>,
}

fn validate_job_activation(record: &JournalRecord) -> Result<(), MinerError> {
    if record.recorded_at == Some(0)
        || record.recorded_at.is_none()
        || record.accepted_at.is_some()
        || record.share_id.is_some()
        || record.worker.is_some()
        || record.worker_authentication.is_some()
        || record.share_target.is_some()
        || record.parent_hash_le.is_some()
        || record.wcash_candidate.is_some()
        || record.zcash_candidate.is_some()
        || record.wcash_block.is_some()
        || record.zcash_block.is_some()
        || record.chain.is_some()
        || record.block_hash.is_some()
        || record.height.is_some()
    {
        return Err(invalid("job activation contains malformed metadata"));
    }
    validate_canonical_hex::<32>(
        required_record_field(
            record.wcash_block_hash.as_deref(),
            "job_activated.wcash_block_hash",
        )?,
        "job_activated.wcash_block_hash",
    )?;
    if record.wcash_height.is_none_or(|height| height == 0)
        || record.zcash_height.is_none_or(|height| height == 0)
    {
        return Err(invalid("job activation has no positive chain heights"));
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ImmutableShare {
    accepted_at: u64,
    worker: String,
    authentication: String,
    job_id: String,
    parent_hash_le: String,
    share_target: String,
    wcash_block_hash: String,
    wcash_height: u32,
    zcash_height: u32,
    wcash_candidate: bool,
    zcash_candidate: bool,
    winner_fingerprint: Option<[u8; 32]>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum WinnerChain {
    Wcash,
    Zcash,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct WinnerKey {
    share_id: [u8; 32],
    chain: WinnerChain,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PendingWinnerStatus {
    job_id: String,
    block_hash: String,
    height: u32,
    observed: bool,
}

struct ValidatedWinnerBlocks {
    entries: Vec<(WinnerKey, PendingWinnerStatus)>,
    fingerprint: Option<[u8; 32]>,
}

fn immutable_share(record: &JournalRecord) -> Result<ImmutableShare, MinerError> {
    if record.accepted_at == Some(0) || record.accepted_at.is_none() || record.recorded_at.is_some()
    {
        return Err(invalid(
            "accepted share has an invalid acceptance timestamp",
        ));
    }
    if record.chain.is_some() || record.block_hash.is_some() || record.height.is_some() {
        return Err(invalid("accepted share contains winner-status metadata"));
    }
    let worker = record
        .worker
        .as_deref()
        .ok_or_else(|| invalid("accepted share has no worker"))?;
    let authentication = validate_authentication(record.worker_authentication.as_deref())?;
    if authentication == "legacy_unknown" {
        validate_legacy_worker_name(worker)?;
    } else {
        validate_worker_name(worker)?;
    }
    let parent_hash_le = record
        .parent_hash_le
        .as_deref()
        .ok_or_else(|| invalid("accepted share has no parent_hash_le"))?;
    validate_canonical_hex::<32>(parent_hash_le, "parent_hash_le")?;
    let share_target = record
        .share_target
        .as_deref()
        .ok_or_else(|| invalid("accepted share has no share_target"))?;
    let target = validate_canonical_hex::<32>(share_target, "share_target")?;
    if target == [0; 32] {
        return Err(invalid("accepted share has a zero target"));
    }
    let wcash_block_hash =
        required_record_field(record.wcash_block_hash.as_deref(), "share.wcash_block_hash")?;
    validate_canonical_hex::<32>(wcash_block_hash, "share.wcash_block_hash")?;
    let wcash_height = record
        .wcash_height
        .filter(|height| *height > 0)
        .ok_or_else(|| invalid("accepted share has no positive Wcash height"))?;
    let zcash_height = record
        .zcash_height
        .filter(|height| *height > 0)
        .ok_or_else(|| invalid("accepted share has no positive Zcash height"))?;
    let wcash_candidate = record
        .wcash_candidate
        .ok_or_else(|| invalid("accepted share has no wcash_candidate flag"))?;
    let zcash_candidate = record
        .zcash_candidate
        .ok_or_else(|| invalid("accepted share has no zcash_candidate flag"))?;
    match record.record.as_str() {
        "accepted_share" if wcash_candidate || zcash_candidate => {
            return Err(invalid(
                "accepted_share is incorrectly marked as a network winner",
            ));
        }
        "winner_outbox" if !wcash_candidate && !zcash_candidate => {
            return Err(invalid("winner_outbox contains no network winner"));
        }
        _ => {}
    }
    Ok(ImmutableShare {
        accepted_at: record
            .accepted_at
            .expect("the acceptance timestamp was checked above"),
        worker: worker.to_string(),
        authentication: authentication.to_string(),
        job_id: record.job_id.clone(),
        parent_hash_le: parent_hash_le.to_string(),
        share_target: share_target.to_string(),
        wcash_block_hash: wcash_block_hash.to_string(),
        wcash_height,
        zcash_height,
        wcash_candidate,
        zcash_candidate,
        winner_fingerprint: None,
    })
}

fn validate_authentication(authentication: Option<&str>) -> Result<&str, MinerError> {
    match authentication {
        Some(authentication @ ("operator" | "exact_credential" | "shared_secret")) => {
            Ok(authentication)
        }
        None => Ok("legacy_unknown"),
        Some(authentication) => Err(invalid(format!(
            "accepted share has unknown authentication provenance {authentication:?}"
        ))),
    }
}

fn validate_winner_blocks(
    record: &JournalRecord,
    share_id: [u8; 32],
) -> Result<ValidatedWinnerBlocks, MinerError> {
    if record.record == "accepted_share" {
        if record.wcash_block.is_some() || record.zcash_block.is_some() {
            return Err(invalid(
                "accepted_share contains network-winner block bytes",
            ));
        }
        return Ok(ValidatedWinnerBlocks {
            entries: Vec::new(),
            fingerprint: None,
        });
    }

    let expected_parent_hash = validate_canonical_hex::<32>(
        required_record_field(
            record.parent_hash_le.as_deref(),
            "winner_outbox.parent_hash_le",
        )?,
        "winner_outbox.parent_hash_le",
    )?;
    let mut winners = Vec::with_capacity(2);
    let mut fingerprint = Sha256::new();
    fingerprint.update(b"Wcash/share-journal/winner-record/v1\0");
    if record.wcash_candidate == Some(true) {
        let block_hash = required_record_field(
            record.wcash_block_hash.as_deref(),
            "winner_outbox.wcash_block_hash",
        )?;
        validate_canonical_hex::<32>(block_hash, "winner_outbox.wcash_block_hash")?;
        let height = record
            .wcash_height
            .filter(|height| *height > 0)
            .ok_or_else(|| invalid("winner_outbox has no positive Wcash height"))?;
        let block = decode_bounded_block(
            required_record_field(record.wcash_block.as_deref(), "winner_outbox.wcash_block")?,
            "winner_outbox.wcash_block",
        )?;
        let recovered_parent_hash = validate_persisted_winner_block(
            &block,
            block_hash,
            height,
            PersistedWinnerBlockKind::Wcash,
        )?;
        if recovered_parent_hash != expected_parent_hash {
            return Err(invalid(
                "winner_outbox Wcash block is bound to a different parent header",
            ));
        }
        fingerprint.update(b"wcash\0");
        fingerprint.update(block_hash.as_bytes());
        fingerprint.update(height.to_le_bytes());
        fingerprint.update((block.len() as u64).to_le_bytes());
        fingerprint.update(&block);
        winners.push((
            WinnerKey {
                share_id,
                chain: WinnerChain::Wcash,
            },
            PendingWinnerStatus {
                job_id: record.job_id.clone(),
                block_hash: block_hash.to_string(),
                height,
                observed: false,
            },
        ));
    } else if record.wcash_block.is_some() {
        return Err(invalid(
            "winner_outbox has Wcash block bytes without a Wcash candidate",
        ));
    }

    if record.zcash_candidate == Some(true) {
        let mut parent_hash = expected_parent_hash;
        parent_hash.reverse();
        let block_hash = hex::encode(parent_hash);
        let height = record
            .zcash_height
            .filter(|height| *height > 0)
            .ok_or_else(|| invalid("winner_outbox has no positive Zcash height"))?;
        let block = decode_bounded_block(
            required_record_field(record.zcash_block.as_deref(), "winner_outbox.zcash_block")?,
            "winner_outbox.zcash_block",
        )?;
        let recovered_parent_hash = validate_persisted_winner_block(
            &block,
            &block_hash,
            height,
            PersistedWinnerBlockKind::Zcash,
        )?;
        if recovered_parent_hash != expected_parent_hash {
            return Err(invalid(
                "winner_outbox Zcash block is bound to a different parent header",
            ));
        }
        fingerprint.update(b"zcash\0");
        fingerprint.update(block_hash.as_bytes());
        fingerprint.update(height.to_le_bytes());
        fingerprint.update((block.len() as u64).to_le_bytes());
        fingerprint.update(&block);
        winners.push((
            WinnerKey {
                share_id,
                chain: WinnerChain::Zcash,
            },
            PendingWinnerStatus {
                job_id: record.job_id.clone(),
                block_hash,
                height,
                observed: false,
            },
        ));
    } else if record.zcash_block.is_some() {
        return Err(invalid(
            "winner_outbox has Zcash block bytes without a Zcash candidate",
        ));
    }

    if winners.is_empty() {
        return Err(invalid("winner_outbox contains no network winner"));
    }
    Ok(ValidatedWinnerBlocks {
        entries: winners,
        fingerprint: Some(fingerprint.finalize().into()),
    })
}

fn apply_winner_status(
    record: &JournalRecord,
    pending_winners: &mut HashMap<WinnerKey, PendingWinnerStatus>,
) -> Result<(), MinerError> {
    if record.recorded_at == Some(0)
        || record.recorded_at.is_none()
        || record.accepted_at.is_some()
        || record.worker.is_some()
        || record.worker_authentication.is_some()
        || record.share_target.is_some()
        || record.parent_hash_le.is_some()
        || record.wcash_candidate.is_some()
        || record.zcash_candidate.is_some()
        || record.wcash_block_hash.is_some()
        || record.wcash_height.is_some()
        || record.zcash_height.is_some()
        || record.wcash_block.is_some()
        || record.zcash_block.is_some()
    {
        return Err(invalid("winner status record contains malformed metadata"));
    }
    validate_canonical_hex::<32>(&record.job_id, "status.job_id")?;
    let share_id = validate_canonical_hex::<32>(
        required_record_field(record.share_id.as_deref(), "share_id")?,
        "share_id",
    )?;
    let chain = match record.chain.as_deref() {
        Some("wcash") => WinnerChain::Wcash,
        Some("zcash") => WinnerChain::Zcash,
        _ => return Err(invalid("winner status record has an unknown chain")),
    };
    let key = WinnerKey { share_id, chain };
    let pending = pending_winners.get_mut(&key).ok_or_else(|| {
        invalid("winner status record updates an unknown or already-matured winner")
    })?;
    let block_hash = required_record_field(record.block_hash.as_deref(), "status.block_hash")?;
    validate_canonical_hex::<32>(block_hash, "status.block_hash")?;
    let height = record
        .height
        .filter(|height| *height > 0)
        .ok_or_else(|| invalid("winner status record has no positive height"))?;
    if pending.job_id != record.job_id
        || pending.block_hash != block_hash
        || pending.height != height
    {
        return Err(invalid(
            "winner status metadata differs from its immutable outbox entry",
        ));
    }
    match record.record.as_str() {
        "winner_observed" | "winner_confirmed" if !pending.observed => {
            pending.observed = true;
        }
        "winner_observed" | "winner_confirmed" => {
            return Err(invalid(
                "winner is observed twice without an intervening orphan transition",
            ));
        }
        "winner_orphaned" if pending.observed => {
            pending.observed = false;
        }
        "winner_orphaned" => {
            return Err(invalid("winner is orphaned before best-chain observation"));
        }
        "winner_matured" if pending.observed => {
            pending_winners.remove(&key);
        }
        "winner_matured" => {
            return Err(invalid("winner is matured before best-chain observation"));
        }
        _ => unreachable!("winner status methods are filtered by the caller"),
    }
    Ok(())
}

fn required_record_field<'a>(value: Option<&'a str>, field: &str) -> Result<&'a str, MinerError> {
    value.ok_or_else(|| invalid(format!("share journal has no {field}")))
}

fn decode_bounded_block(encoded: &str, field: &str) -> Result<Vec<u8>, MinerError> {
    if encoded.len() > MAX_WINNER_BLOCK_BYTES.saturating_mul(2) {
        return Err(invalid(format!(
            "{field} exceeds {MAX_WINNER_BLOCK_BYTES} decoded bytes"
        )));
    }
    let bytes = hex::decode(encoded)
        .map_err(|error| invalid(format!("{field} is invalid hexadecimal: {error}")))?;
    if encoded != hex::encode(&bytes) {
        return Err(invalid(format!(
            "{field} is not canonical lowercase hexadecimal"
        )));
    }
    Ok(bytes)
}

fn validate_share_id(
    record: &JournalRecord,
    share: &ImmutableShare,
) -> Result<[u8; 32], MinerError> {
    let actual = validate_canonical_hex::<32>(
        required_record_field(record.share_id.as_deref(), "share_id")?,
        "share_id",
    )?;
    let parent_hash = validate_canonical_hex::<32>(&share.parent_hash_le, "parent_hash_le")?;
    let mut hash = Sha256::new();
    hash.update(b"Wcash/share-journal/v2\0");
    hash.update(
        u64::try_from(share.job_id.len())
            .map_err(|_| invalid("job ID length overflowed"))?
            .to_le_bytes(),
    );
    hash.update(share.job_id.as_bytes());
    hash.update(parent_hash);
    let expected: [u8; 32] = hash.finalize().into();
    if actual != expected {
        return Err(invalid(
            "share ID does not match its immutable job and parent hash",
        ));
    }
    Ok(actual)
}

fn apply_share(
    snapshot: &mut AccountingSnapshot,
    share: &ImmutableShare,
) -> Result<(), MinerError> {
    snapshot.accepted_shares = snapshot
        .accepted_shares
        .checked_add(1)
        .ok_or_else(|| invalid("accepted-share count overflowed"))?;
    increment_bucket(
        &mut snapshot.shares_by_authentication,
        &share.authentication,
        "authentication-provenance count",
    )?;
    let worker = snapshot.workers.entry(share.worker.clone()).or_default();
    worker.accepted_shares = worker
        .accepted_shares
        .checked_add(1)
        .ok_or_else(|| invalid("worker accepted-share count overflowed"))?;
    increment_bucket(
        &mut worker.shares_by_authentication,
        &share.authentication,
        "worker authentication-provenance count",
    )?;
    if share.wcash_candidate {
        worker.wcash_winners = worker
            .wcash_winners
            .checked_add(1)
            .ok_or_else(|| invalid("worker Wcash-winner count overflowed"))?;
    }
    if share.zcash_candidate {
        worker.zcash_winners = worker
            .zcash_winners
            .checked_add(1)
            .ok_or_else(|| invalid("worker Zcash-winner count overflowed"))?;
    }
    increment_bucket(
        &mut worker.shares_by_target,
        &share.share_target,
        "worker target-bucket count",
    )?;
    Ok(())
}

fn increment_bucket(
    buckets: &mut BTreeMap<String, u64>,
    key: &str,
    label: &str,
) -> Result<(), MinerError> {
    let count = buckets.entry(key.to_string()).or_default();
    *count = count
        .checked_add(1)
        .ok_or_else(|| invalid(format!("{label} overflowed")))?;
    Ok(())
}

fn validate_worker_name(worker: &str) -> Result<(), MinerError> {
    if worker.is_empty()
        || worker.len() > 128
        || !worker
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_' | b':'))
    {
        return Err(invalid(
            "worker name must contain 1..=128 ASCII letters, digits, '.', '-', '_', or ':'",
        ));
    }
    Ok(())
}

fn validate_legacy_worker_name(worker: &str) -> Result<(), MinerError> {
    if worker.is_empty() || worker.len() > 128 || worker.bytes().any(|byte| byte.is_ascii_control())
    {
        return Err(invalid(
            "legacy worker name must contain 1..=128 bytes without ASCII control characters",
        ));
    }
    Ok(())
}

fn validate_password_hash(encoded: &str) -> Result<(), MinerError> {
    if encoded.len() > MAX_PASSWORD_HASH_BYTES {
        return Err(invalid("worker password hash is too long"));
    }
    let hash = PasswordHash::new(encoded)
        .map_err(|error| invalid(format!("worker password hash is invalid: {error}")))?;
    if hash.algorithm.as_str() != "argon2id" {
        return Err(invalid("worker password hash must use Argon2id"));
    }
    if hash.version != Some(19) {
        return Err(invalid("worker password hash must use Argon2 version 19"));
    }
    let memory = hash.params.get_decimal("m");
    let iterations = hash.params.get_decimal("t");
    let lanes = hash.params.get_decimal("p");
    if hash.params.iter().count() != 3
        || memory != Some(ARGON2_MEMORY_KIB)
        || iterations != Some(ARGON2_ITERATIONS)
        || lanes != Some(ARGON2_LANES)
    {
        return Err(invalid(format!(
            "worker Argon2id parameters must be exactly m={ARGON2_MEMORY_KIB},t={ARGON2_ITERATIONS},p={ARGON2_LANES}"
        )));
    }
    let mut salt_bytes = [0; 64];
    let valid_salt = hash
        .salt
        .and_then(|salt| salt.decode_b64(&mut salt_bytes).ok())
        .is_some_and(|salt| salt.len() >= 16);
    if !valid_salt
        || hash
            .hash
            .as_ref()
            .is_none_or(|output| output.as_bytes().len() != 32)
    {
        return Err(invalid(
            "worker password hash must contain at least 16 salt bytes and a 32-byte digest",
        ));
    }
    Ok(())
}

fn validate_hex<const N: usize>(encoded: &str, field: &str) -> Result<[u8; N], MinerError> {
    let bytes = hex::decode(encoded)
        .map_err(|error| invalid(format!("{field} is invalid hexadecimal: {error}")))?;
    bytes.try_into().map_err(|bytes: Vec<u8>| {
        invalid(format!(
            "{field} decoded to {} bytes, expected {N}",
            bytes.len()
        ))
    })
}

fn validate_canonical_hex<const N: usize>(
    encoded: &str,
    field: &str,
) -> Result<[u8; N], MinerError> {
    let bytes = validate_hex::<N>(encoded, field)?;
    if encoded != hex::encode(bytes) {
        return Err(invalid(format!(
            "{field} is not canonical lowercase hexadecimal"
        )));
    }
    Ok(bytes)
}

fn read_bounded_line(reader: &mut impl BufRead) -> Result<Option<(Vec<u8>, bool)>, MinerError> {
    let mut line = Vec::new();
    loop {
        let (chunk_len, terminated) = {
            let available = reader.fill_buf()?;
            if available.is_empty() {
                return Ok((!line.is_empty()).then_some((line, false)));
            }
            match available.iter().position(|byte| *byte == b'\n') {
                Some(position) => (position, true),
                None => (available.len(), false),
            }
        };
        if line.len().saturating_add(chunk_len) > MAX_JOURNAL_RECORD_BYTES {
            return Err(invalid(format!(
                "share journal contains a record larger than {MAX_JOURNAL_RECORD_BYTES} bytes"
            )));
        }
        let available = reader.fill_buf()?;
        line.extend_from_slice(&available[..chunk_len]);
        reader.consume(chunk_len + usize::from(terminated));
        if terminated {
            return Ok(Some((line, true)));
        }
    }
}

fn reject_symlink(path: &Path, label: &str) -> Result<(), MinerError> {
    if fs::symlink_metadata(path)?.file_type().is_symlink() {
        return Err(invalid(format!(
            "{label} {} must not be a symbolic link",
            path.display()
        )));
    }
    Ok(())
}

fn validate_private_file_and_parent(
    file: &File,
    path: &Path,
    label: &str,
) -> Result<(), MinerError> {
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        return Err(invalid(format!(
            "{label} {} is not a regular file",
            path.display()
        )));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(invalid(format!(
                "{label} {} must not be accessible by group or other users",
                path.display()
            )));
        }
        let parent = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        if fs::metadata(parent)?.permissions().mode() & 0o022 != 0 {
            return Err(invalid(format!(
                "{label} parent directory {} must not be writable by group or other users",
                parent.display()
            )));
        }
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> MinerError {
    MinerError::InvalidRequest(message.into())
}

#[cfg(test)]
mod tests {
    use argon2::password_hash::SaltString;
    use hex::FromHex;
    use serde_json::{json, Value};
    use tempfile::TempDir;
    use zebra_chain::{
        block::Block,
        serialization::{ZcashDeserializeInto, ZcashSerialize},
    };

    use super::*;

    const TEST_PASSWORD: &str = "correct horse battery staple";

    fn private_temp_dir() -> TempDir {
        let directory = tempfile::tempdir().expect("temporary directory");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700))
                .expect("private temporary directory");
        }
        directory
    }

    fn password_hash(password: &str) -> String {
        let salt = SaltString::encode_b64(b"wcash-test-salt!").expect("test salt");
        Argon2::default()
            .hash_password(password.as_bytes(), &salt)
            .expect("password hash")
            .to_string()
    }

    fn credentials(workers: &[(&str, &str)]) -> Vec<u8> {
        serde_json::to_vec(&json!({
            "version": 1,
            "workers": workers.iter().map(|(name, password)| json!({
                "name": name,
                "password_hash": password_hash(password),
            })).collect::<Vec<_>>(),
        }))
        .expect("credential JSON")
    }

    fn accepted_record(worker: &str, winner: bool) -> Value {
        let job_id = "ab".repeat(32);
        let parent_hash_le = "11".repeat(32);
        let mut hash = Sha256::new();
        hash.update(b"Wcash/share-journal/v2\0");
        hash.update(64u64.to_le_bytes());
        hash.update(job_id.as_bytes());
        hash.update([0x11; 32]);
        let share_id: [u8; 32] = hash.finalize().into();
        json!({
            "version": 2,
            "record": if winner { "winner_outbox" } else { "accepted_share" },
            "accepted_at": 1,
            "share_id": hex::encode(share_id),
            "worker": worker,
            "worker_authentication": "exact_credential",
            "job_id": job_id,
            "parent_hash_le": parent_hash_le,
            "share_target": "ff".repeat(32),
            "wcash_candidate": winner,
            "zcash_candidate": winner,
            "wcash_block_hash": "22".repeat(32),
            "wcash_height": 1,
            "zcash_height": 1,
            "wcash_block": if winner { Some("00") } else { None::<&str> },
            "zcash_block": if winner { Some("00") } else { None::<&str> },
        })
    }

    fn valid_zcash_winner_record(worker: &str) -> Value {
        let block: Block = Vec::from_hex(
            include_str!("../../zebra-test/src/vectors/block-main-0-000-001.txt").trim(),
        )
        .expect("mainnet block fixture is hex")
        .zcash_deserialize_into()
        .expect("mainnet block fixture is a block");
        let parent_hash_le = hex::encode(block.hash().0);
        let job_id = "cd".repeat(32);
        let mut hash = Sha256::new();
        hash.update(b"Wcash/share-journal/v2\0");
        hash.update(64u64.to_le_bytes());
        hash.update(job_id.as_bytes());
        hash.update(block.hash().0);
        let share_id: [u8; 32] = hash.finalize().into();
        json!({
            "version": 2,
            "record": "winner_outbox",
            "accepted_at": 7,
            "share_id": hex::encode(share_id),
            "worker": worker,
            "worker_authentication": "exact_credential",
            "job_id": job_id,
            "parent_hash_le": parent_hash_le,
            "share_target": "ff".repeat(32),
            "wcash_candidate": false,
            "zcash_candidate": true,
            "wcash_block_hash": "23".repeat(32),
            "wcash_height": 1,
            "zcash_height": 1,
            "zcash_block": hex::encode(
                block.zcash_serialize_to_vec().expect("fixture serializes canonically")
            ),
        })
    }

    fn write_private(path: &Path, bytes: &[u8]) {
        fs::write(path, bytes).expect("write fixture");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(path, fs::Permissions::from_mode(0o600)).expect("private fixture");
        }
    }

    #[test]
    fn exact_workers_have_independent_credentials() {
        let store = WorkerCredentialStore::from_json(&credentials(&[
            ("alice.rig-01", TEST_PASSWORD),
            ("alice.rig-02", "another sufficiently long password"),
        ]))
        .expect("valid registry");
        assert_eq!(
            store
                .authenticate("alice.rig-01", TEST_PASSWORD)
                .expect("correct credential")
                .name(),
            "alice.rig-01"
        );
        assert!(store.authenticate("alice.rig-02", TEST_PASSWORD).is_none());
        assert!(store.authenticate("unknown", TEST_PASSWORD).is_none());
        assert!(!format!("{store:?}").contains(TEST_PASSWORD));
    }

    #[test]
    fn credential_schema_cost_and_permissions_fail_closed() {
        assert!(WorkerCredentialStore::from_json(br#"{"version":1,"workers":[]}"#).is_err());
        assert!(WorkerCredentialStore::from_json(&credentials(&[
            ("duplicate", TEST_PASSWORD),
            ("duplicate", TEST_PASSWORD),
        ]))
        .is_err());
        let weak = json!({
            "version": 1,
            "workers": [{
                "name": "worker",
                "password_hash": "$argon2id$v=19$m=8,t=1,p=1$d2Nhc2gtdGVzdC1zYWx0IQ$MTIzNDU2Nzg5MDEyMzQ1Ng"
            }]
        });
        assert!(WorkerCredentialStore::from_json(&serde_json::to_vec(&weak).unwrap()).is_err());
        let wrong_version = password_hash(TEST_PASSWORD).replace("v=19", "v=16");
        let wrong_version = json!({
            "version": 1,
            "workers": [{"name": "worker", "password_hash": wrong_version}],
        });
        assert!(
            WorkerCredentialStore::from_json(&serde_json::to_vec(&wrong_version).unwrap()).is_err()
        );
        let short_salt = SaltString::encode_b64(b"too-short").expect("valid PHC salt");
        let short_salt_hash = Argon2::default()
            .hash_password(TEST_PASSWORD.as_bytes(), &short_salt)
            .expect("short-salt password hash")
            .to_string();
        let short_salt = json!({
            "version": 1,
            "workers": [{"name": "worker", "password_hash": short_salt_hash}],
        });
        assert!(
            WorkerCredentialStore::from_json(&serde_json::to_vec(&short_salt).unwrap()).is_err()
        );

        let directory = private_temp_dir();
        let path = directory.path().join("workers.json");
        write_private(&path, &credentials(&[("worker.1", TEST_PASSWORD)]));
        assert!(WorkerCredentialStore::from_path(&path).is_ok());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
            assert!(WorkerCredentialStore::from_path(&path).is_err());
        }
    }

    #[test]
    fn generated_hash_round_trips() {
        let encoded = hash_worker_password(TEST_PASSWORD).expect("hash password");
        assert!(encoded.starts_with("$argon2id$"));
        assert!(!encoded.contains(TEST_PASSWORD));
        let config = serde_json::to_vec(&json!({
            "version": 1,
            "workers": [{"name": "worker.1", "password_hash": encoded}],
        }))
        .unwrap();
        assert!(WorkerCredentialStore::from_json(&config)
            .expect("generated hash accepted")
            .authenticate("worker.1", TEST_PASSWORD)
            .is_some());
    }

    #[test]
    fn job_activation_is_not_a_share_and_malformed_or_reused_ids_fail_closed() {
        let directory = private_temp_dir();
        let path = directory.path().join("activation.jsonl");
        let activation = json!({
            "version": JOURNAL_VERSION,
            "record": "job_activated",
            "recorded_at": 1,
            "job_id": "91".repeat(32),
            "wcash_block_hash": "92".repeat(32),
            "wcash_height": 1,
            "zcash_height": 1,
        });
        let mut bytes = serde_json::to_vec(&activation).expect("encode activation");
        bytes.push(b'\n');
        write_private(&path, &bytes);
        assert_eq!(
            read_accounting_snapshot(&path)
                .expect("activation is valid accounting metadata")
                .accepted_shares,
            0
        );

        let mut malformed = activation.clone();
        malformed["share_id"] = json!("93".repeat(32));
        let mut bytes = serde_json::to_vec(&malformed).expect("encode malformed activation");
        bytes.push(b'\n');
        write_private(&path, &bytes);
        assert!(read_accounting_snapshot(&path).is_err());

        let mut bytes = serde_json::to_vec(&activation).expect("encode activation");
        bytes.push(b'\n');
        bytes.extend_from_slice(&serde_json::to_vec(&activation).expect("encode reuse"));
        bytes.push(b'\n');
        write_private(&path, &bytes);
        assert!(read_accounting_snapshot(&path).is_err());

        assert!(
            ensure_accounting_generation_capacity(MAX_ACCOUNTING_GENERATIONS - 1, false).is_ok()
        );
        assert!(ensure_accounting_generation_capacity(MAX_ACCOUNTING_GENERATIONS, true).is_ok());
        assert!(ensure_accounting_generation_capacity(MAX_ACCOUNTING_GENERATIONS, false).is_err());
    }

    #[test]
    fn one_journal_is_the_accounting_source_and_deduplicates() {
        let directory = private_temp_dir();
        let path = directory.path().join("journal.jsonl");
        let record = accepted_record("alice.rig-01", false);
        let mut bytes = serde_json::to_vec(&record).unwrap();
        bytes.push(b'\n');
        bytes.extend_from_slice(&serde_json::to_vec(&record).unwrap());
        bytes.push(b'\n');
        write_private(&path, &bytes);

        let snapshot = read_accounting_snapshot(&path).expect("strict report");
        assert_eq!(snapshot.accepted_shares, 1);
        assert_eq!(
            snapshot.shares_by_authentication,
            BTreeMap::from([("exact_credential".to_string(), 1)])
        );
        let worker = &snapshot.workers["alice.rig-01"];
        assert_eq!(worker.accepted_shares, 1);
        assert_eq!(worker.wcash_winners, 0);
        assert_eq!(worker.zcash_winners, 0);
        assert_eq!(worker.shares_by_authentication["exact_credential"], 1);
        assert_eq!(worker.shares_by_target[&"ff".repeat(32)], 1);
    }

    #[test]
    fn duplicate_share_cannot_rewrite_generation_metadata() {
        let directory = private_temp_dir();
        let path = directory.path().join("generation-conflict.jsonl");
        let record = accepted_record("alice.rig-01", false);
        let conflicts = [
            ("wcash_block_hash", json!("24".repeat(32))),
            ("wcash_height", json!(2)),
            ("zcash_height", json!(2)),
        ];
        for (field, value) in conflicts {
            let mut conflict = record.clone();
            conflict[field] = value;
            let mut bytes = serde_json::to_vec(&record).unwrap();
            bytes.push(b'\n');
            bytes.extend_from_slice(&serde_json::to_vec(&conflict).unwrap());
            bytes.push(b'\n');
            write_private(&path, &bytes);
            assert!(
                read_accounting_snapshot(&path).is_err(),
                "duplicate share must not rewrite {field}"
            );
        }
    }

    #[test]
    fn duplicate_winner_requires_exact_timestamp_metadata_and_block() {
        let directory = private_temp_dir();
        let path = directory.path().join("winner-journal.jsonl");
        let record = valid_zcash_winner_record("alice.rig-01");
        let mut bytes = serde_json::to_vec(&record).unwrap();
        bytes.push(b'\n');
        bytes.extend_from_slice(&serde_json::to_vec(&record).unwrap());
        bytes.push(b'\n');
        write_private(&path, &bytes);

        let snapshot = read_accounting_snapshot(&path).expect("exact winner replay deduplicates");
        assert_eq!(snapshot.accepted_shares, 1);
        assert_eq!(snapshot.workers["alice.rig-01"].zcash_winners, 1);

        let mut timestamp_conflict = record.clone();
        timestamp_conflict["accepted_at"] = json!(8);
        let mut bytes = serde_json::to_vec(&record).unwrap();
        bytes.push(b'\n');
        bytes.extend_from_slice(&serde_json::to_vec(&timestamp_conflict).unwrap());
        bytes.push(b'\n');
        write_private(&path, &bytes);
        assert!(read_accounting_snapshot(&path).is_err());

        let mut metadata_conflict = record.clone();
        metadata_conflict["zcash_height"] = json!(2);
        let mut bytes = serde_json::to_vec(&record).unwrap();
        bytes.push(b'\n');
        bytes.extend_from_slice(&serde_json::to_vec(&metadata_conflict).unwrap());
        bytes.push(b'\n');
        write_private(&path, &bytes);
        assert!(read_accounting_snapshot(&path).is_err());
    }

    #[test]
    fn conflicting_attribution_and_terminated_corruption_fail_closed() {
        let directory = private_temp_dir();
        let path = directory.path().join("journal.jsonl");
        let original = accepted_record("alice.rig-01", false);
        let mut conflict = original.clone();
        conflict["worker"] = json!("mallory.rig-01");
        let mut bytes = serde_json::to_vec(&original).unwrap();
        bytes.push(b'\n');
        bytes.extend_from_slice(&serde_json::to_vec(&conflict).unwrap());
        bytes.push(b'\n');
        write_private(&path, &bytes);
        assert!(read_accounting_snapshot(&path).is_err());

        write_private(&path, b"not-json\n");
        assert!(read_accounting_snapshot(&path).is_err());

        let mut provenance_conflict = original.clone();
        provenance_conflict["worker_authentication"] = json!("shared_secret");
        let mut bytes = serde_json::to_vec(&original).unwrap();
        bytes.push(b'\n');
        bytes.extend_from_slice(&serde_json::to_vec(&provenance_conflict).unwrap());
        bytes.push(b'\n');
        write_private(&path, &bytes);
        assert!(read_accounting_snapshot(&path).is_err());
    }

    #[test]
    fn offline_report_ignores_only_one_unterminated_tail_and_honors_lock() {
        let directory = private_temp_dir();
        let path = directory.path().join("journal.jsonl");
        let mut bytes = serde_json::to_vec(&accepted_record("worker.report", false)).unwrap();
        bytes.push(b'\n');
        bytes.extend_from_slice(b"partial");
        write_private(&path, &bytes);
        assert_eq!(
            read_accounting_snapshot(&path)
                .expect("stable prefix")
                .accepted_shares,
            1
        );

        let locked = File::open(&path).unwrap();
        fs2::FileExt::try_lock_exclusive(&locked).unwrap();
        assert!(read_accounting_snapshot(&path).is_err());
        fs2::FileExt::unlock(&locked).unwrap();
    }

    #[test]
    fn malformed_winner_bytes_fail_closed() {
        let directory = private_temp_dir();
        let path = directory.path().join("journal.jsonl");
        let mut bytes = serde_json::to_vec(&accepted_record("worker.1", true)).unwrap();
        bytes.push(b'\n');
        write_private(&path, &bytes);
        assert!(read_accounting_snapshot(&path).is_err());
    }

    #[test]
    fn winner_status_transitions_are_strict() {
        let share_id = [0x44; 32];
        let key = WinnerKey {
            share_id,
            chain: WinnerChain::Wcash,
        };
        let mut pending = HashMap::from([(
            key,
            PendingWinnerStatus {
                job_id: "ab".repeat(32),
                block_hash: "22".repeat(32),
                height: 1,
                observed: false,
            },
        )]);
        let status = |record: &str| {
            serde_json::from_value::<JournalRecord>(json!({
            "version": 2,
                "record": record,
            "recorded_at": 2,
                "share_id": hex::encode(share_id),
            "job_id": "ab".repeat(32),
            "chain": "wcash",
            "block_hash": "22".repeat(32),
            "height": 1,
            }))
            .expect("status fixture")
        };

        apply_winner_status(&status("winner_confirmed"), &mut pending).unwrap();
        assert!(pending[&key].observed);
        assert!(apply_winner_status(&status("winner_observed"), &mut pending).is_err());
        apply_winner_status(&status("winner_orphaned"), &mut pending).unwrap();
        assert!(!pending[&key].observed);
        assert!(apply_winner_status(&status("winner_matured"), &mut pending).is_err());
        apply_winner_status(&status("winner_observed"), &mut pending).unwrap();
        apply_winner_status(&status("winner_matured"), &mut pending).unwrap();
        assert!(!pending.contains_key(&key));
        assert!(apply_winner_status(&status("winner_orphaned"), &mut pending).is_err());
    }
}
