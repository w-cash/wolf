//! End-to-end native Wcash/Zcash job coordination and durable share journaling.

use std::{
    collections::{HashMap, HashSet},
    fmt,
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, MutexGuard,
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use hex::FromHex;
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use wcash_zcash_aux::{AuxPowProof, Target, WCASH_AUXILIARY_CHAIN_ID};
use zcash_address::unified::{Container, Receiver};
use zcash_protocol::consensus::NetworkType;
use zebra_chain::{
    block::{Block, Height},
    parameters::{Network, NetworkKind},
    primitives::{WcashAddress, WcashAddressKind},
    serialization::{ZcashDeserializeInto, ZcashSerialize},
    transparent,
    work::{
        difficulty::{CompactDifficulty, ExpandedDifficulty, U256},
        equihash::{Solution, WCASH_BLOCK_WIRE_VERSION},
    },
};

use crate::{
    accounting::{
        read_accounting_snapshot_from_reader, AuthenticatedWorker, WorkerAuthenticationProvenance,
    },
    rpc::{RpcEndpoint, ZebraRpcClient, DEFAULT_RPC_TIMEOUT},
    MinerError, NativePreparedJob, NativeZcashConfig, NativeZcashProvider, ShareProcessor,
    ValidatedNativeShare,
};

/// Complete native-node configuration for one frozen dual-mining job.
#[derive(Clone)]
pub struct CoordinatorConfig {
    /// Wcash child node exposing `createauxblock` and `submitauxblock`.
    pub wcash_node: RpcEndpoint,
    /// Expected child genesis hash in conventional RPC display order.
    pub expected_wcash_genesis_hash: String,
    /// Parent template and proposal-validation node set.
    pub zcash: NativeZcashConfig,
    /// Wcash address that receives the child coinbase.
    pub wcash_payout_address: String,
    /// Auxiliary-tree nonce; one-child native jobs normally use zero.
    pub auxiliary_nonce: u32,
}

impl fmt::Debug for CoordinatorConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CoordinatorConfig")
            .field("wcash_node", &self.wcash_node)
            .field(
                "expected_wcash_genesis_hash",
                &self.expected_wcash_genesis_hash,
            )
            .field("zcash", &self.zcash)
            .field("wcash_payout_address", &"[REDACTED]")
            .field("auxiliary_nonce", &self.auxiliary_nonce)
            .finish()
    }
}

/// Long-lived owner of native node clients and the authoritative share journal.
///
/// Opening a supervisor performs crash recovery and acquires the journal lock
/// exactly once. Each subsequent generation reuses that validated state.
pub struct NativeMiningSupervisor {
    config: CoordinatorConfig,
    wcash_network: Network,
    wcash_payout_address: WcashAddress,
    wcash_node: ZebraRpcClient,
    zcash: NativeZcashProvider,
    journal: Arc<ShareJournal>,
    generation_preparation: Mutex<()>,
    pending_candidate_retirement: Mutex<Option<ChildCandidateLease>>,
}

/// A fully prepared dual-chain job and its submission backend.
pub struct NativeMiningCoordinator {
    wcash_node: ZebraRpcClient,
    zcash: NativeZcashProvider,
    job: NativePreparedJob,
    child_height: u32,
    child_previous_hash: String,
    child_candidate_lease: ChildCandidateLease,
    candidate_created_at: Instant,
    freshness: Mutex<JobFreshness>,
    last_outbox_retry: Mutex<Instant>,
    outbox_retry_requested: AtomicBool,
    outbox_retry_in_progress: AtomicBool,
    journal: Arc<ShareJournal>,
}

/// Final cache disposition of a stopped native mining generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GenerationRetirement {
    /// The candidate was removed, or was already absent after a retry/restart.
    Retired,
    /// A durable Wcash winner still references this candidate.
    RetainedForWcashWinner,
    /// A canonically decoded submission already began at the Wcash node.
    RetainedForSubmission,
}

#[derive(Clone, Eq, PartialEq)]
struct ChildCandidateLease {
    hash: String,
    retire_token: String,
}

impl fmt::Debug for ChildCandidateLease {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ChildCandidateLease")
            .field("hash", &self.hash)
            .field("retire_token", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
enum RetireAuxBlockResponse {
    Retired,
    AlreadyAbsent,
    SubmissionStarted,
}

/// Operator-visible durable winner-outbox counts.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WinnerOutboxStatus {
    /// Wcash blocks retained until the configured confirmation depth.
    pub pending_wcash: usize,
    /// Zcash blocks retained until the configured confirmation depth.
    pub pending_zcash: usize,
    /// Retained blocks currently observed on a pinned node's best chain.
    pub observed: usize,
    /// Confirmation depth required before an outbox entry is retired.
    pub retention_confirmations: u32,
}

#[derive(Clone, Copy, Debug)]
struct JobFreshness {
    active: bool,
    last_checked: Instant,
}

/// Maximum time one native header generation is advertised before fresh work is prepared.
pub const NATIVE_JOB_MAX_AGE_SECONDS: u64 = 45;

// Refresh before the common 55-second S-NOMP/ASIC liveness rebroadcast interval.
// A fresh child and independently proposal-validated parent are required: merely
// replaying the same header could restart a miner's duplicate nonce search.
const MAX_JOB_AGE: Duration = Duration::from_secs(NATIVE_JOB_MAX_AGE_SECONDS);
const TIP_RECHECK_INTERVAL: Duration = Duration::from_secs(2);
const OUTBOX_RETRY_INTERVAL: Duration = Duration::from_secs(15);
const MAX_CHILD_BLOCK_BYTES: usize = 2_000_000;
const WINNER_RETENTION_CONFIRMATIONS: u32 = 100;

impl NativeMiningSupervisor {
    fn retry_pending_candidate_retirement(&self) -> Result<(), MinerError> {
        let pending = self
            .pending_candidate_retirement
            .lock()
            .map_err(|_| coordinator_mutex_error("pending candidate retirement"))?
            .take();
        let Some(lease) = pending else {
            return Ok(());
        };

        match retire_child_candidate(&self.wcash_node, &lease) {
            Ok(_) => Ok(()),
            Err(error) => {
                *self
                    .pending_candidate_retirement
                    .lock()
                    .map_err(|_| coordinator_mutex_error("pending candidate retirement"))? =
                    Some(lease);
                Err(error)
            }
        }
    }

    fn retire_failed_preparation(&self, lease: ChildCandidateLease) {
        if let Err(error) = retire_child_candidate(&self.wcash_node, &lease) {
            eprintln!(
                "failed to retire an unpublished native child generation: {error}; no new candidate will be requested until retirement is retried"
            );
            match self.pending_candidate_retirement.lock() {
                Ok(mut pending) => {
                    debug_assert!(pending.is_none());
                    *pending = Some(lease);
                }
                Err(_) => eprintln!(
                    "pending candidate retirement mutex is poisoned; stop this supervisor before issuing more work"
                ),
            }
        }
    }

    /// Opens and crash-recovers one durable journal for this process lifetime.
    pub fn open(
        config: CoordinatorConfig,
        journal_path: impl AsRef<Path>,
    ) -> Result<Self, MinerError> {
        if config.wcash_payout_address.is_empty()
            || config.wcash_payout_address.len() > 1_024
            || config
                .wcash_payout_address
                .bytes()
                .any(|byte| byte.is_ascii_control())
        {
            return Err(MinerError::InvalidRequest(
                "Wcash payout address is malformed".to_string(),
            ));
        }
        if !config.wcash_node.is_loopback() {
            return Err(MinerError::RpcConfiguration(
                "the Wcash template node must be loopback: it chooses the child coinbase recipient"
                    .to_string(),
            ));
        }

        parse_display_hash(
            &config.expected_wcash_genesis_hash,
            "expected Wcash genesis hash",
        )?;
        let (wcash_network, wcash_payout_address) = validate_wcash_payout_configuration(
            &config.wcash_payout_address,
            &config.expected_wcash_genesis_hash,
        )?;
        let journal = Arc::new(ShareJournal::open(journal_path)?);
        let wcash_node = ZebraRpcClient::new(config.wcash_node.clone(), DEFAULT_RPC_TIMEOUT)?;
        let zcash = NativeZcashProvider::connect(config.zcash.clone())?;
        Ok(Self {
            config,
            wcash_network,
            wcash_payout_address,
            wcash_node,
            zcash,
            journal,
            generation_preparation: Mutex::new(()),
            pending_candidate_retirement: Mutex::new(None),
        })
    }

    /// Creates, proposal-validates, and durably activates one fresh generation.
    pub fn prepare_generation(&self) -> Result<NativeMiningCoordinator, MinerError> {
        let _preparation = self
            .generation_preparation
            .lock()
            .map_err(|_| coordinator_mutex_error("native generation preparation"))?;
        let config = &self.config;
        let wcash_node = self.wcash_node.clone();
        let zcash = self.zcash.clone();
        let journal = Arc::clone(&self.journal);
        let expected_zcash_genesis_hash = config.zcash.expected_genesis_hash().to_string();

        // Replay each chain independently before asking either node to create
        // fresh work. A template/proposal outage must never strand an already
        // durable winner for the other chain.
        let wcash_identity =
            require_wcash_network_identity(&wcash_node, &config.expected_wcash_genesis_hash);
        let zcash_identity = zcash.require_network_identity(&expected_zcash_genesis_hash);
        let pending = journal.all_pending()?;
        if wcash_identity.is_ok() {
            retry_pending_winners_with(
                &wcash_node,
                &zcash,
                &journal,
                pending
                    .iter()
                    .filter(|winner| winner.key.chain == WinnerChain::Wcash)
                    .cloned()
                    .collect(),
            )?;
        }
        if zcash_identity.is_ok() {
            retry_pending_winners_with(
                &wcash_node,
                &zcash,
                &journal,
                pending
                    .into_iter()
                    .filter(|winner| winner.key.chain == WinnerChain::Zcash)
                    .collect(),
            )?;
        }
        wcash_identity?;
        // A transient failure after a prior `createauxblock` must be resolved
        // before another candidate is requested. This makes preparation
        // retries cache-capacity neutral even during a prolonged parent outage.
        self.retry_pending_candidate_retirement()?;
        zcash_identity?;

        let child: ChildTemplate = wcash_node.call(
            "createauxblock",
            json!([config.wcash_payout_address.clone()]),
        )?;
        let child_hash = parse_display_hash(&child.hash, "createauxblock hash")?;
        parse_canonical_capability(&child.retire_token, "createauxblock retiretoken")?;
        let child_candidate_lease = ChildCandidateLease {
            hash: child.hash.clone(),
            retire_token: child.retire_token.clone(),
        };
        let mut preparation_guard = CandidatePreparationGuard {
            supervisor: self,
            lease: Some(child_candidate_lease.clone()),
        };
        if child.chain_id != WCASH_AUXILIARY_CHAIN_ID {
            return Err(MinerError::InvalidParentTemplate(format!(
                "Wcash node returned chain id 0x{:08x}, expected 0x{WCASH_AUXILIARY_CHAIN_ID:08x}",
                child.chain_id
            )));
        }
        if child.height == 0 {
            return Err(MinerError::InvalidParentTemplate(
                "Wcash auxiliary candidate height is zero".to_string(),
            ));
        }
        let child_candidate_bytes =
            decode_bounded_hex(&child.data, "createauxblock data", MAX_CHILD_BLOCK_BYTES)?;
        let child_candidate: Block = child_candidate_bytes
            .as_slice()
            .zcash_deserialize_into()
            .map_err(|error| {
                MinerError::InvalidParentTemplate(format!(
                    "invalid createauxblock candidate bytes: {error}"
                ))
            })?;
        if child_candidate.zcash_serialize_to_vec()? != child_candidate_bytes {
            return Err(MinerError::InvalidParentTemplate(
                "createauxblock candidate is not canonically serialized".to_string(),
            ));
        }
        if child_candidate.hash().0 != child_hash {
            return Err(MinerError::InvalidParentTemplate(
                "createauxblock data does not match its advertised hash".to_string(),
            ));
        }
        if child_candidate.header.version != WCASH_BLOCK_WIRE_VERSION
            || !child_candidate
                .header
                .solution
                .as_wcash()
                .is_some_and(|witness| witness.is_empty())
        {
            return Err(MinerError::InvalidParentTemplate(
                "createauxblock data is not a proof-free Wcash candidate".to_string(),
            ));
        }
        let child_target = parse_display_target(&child.target)?;
        let child_bits = CompactDifficulty::from_hex(&child.bits).map_err(|error| {
            MinerError::InvalidParentTemplate(format!("invalid createauxblock bits: {error}"))
        })?;
        let child_expanded = ExpandedDifficulty::from_hex(&child.target).map_err(|error| {
            MinerError::InvalidParentTemplate(format!("invalid createauxblock target: {error}"))
        })?;
        if child_bits.to_expanded() != Some(child_expanded) {
            return Err(MinerError::InvalidParentTemplate(
                "createauxblock bits and target describe different difficulties".to_string(),
            ));
        }
        let child_previous_hash = parse_display_hash(
            &child.previous_block_hash,
            "createauxblock previousblockhash",
        )?;
        if child_candidate.header.previous_block_hash.0 != child_previous_hash
            || child_candidate.header.difficulty_threshold != child_bits
            || child_candidate.coinbase_height().map(u32::from) != Some(child.height)
        {
            return Err(MinerError::InvalidParentTemplate(
                "createauxblock metadata does not match its serialized candidate".to_string(),
            ));
        }
        validate_wcash_candidate_payout(
            &child_candidate,
            &self.wcash_payout_address,
            &self.wcash_network,
            child.coinbase_value,
            child.height,
        )?;

        let job = zcash.prepare_job(child_hash, child_target, config.auxiliary_nonce)?;
        let expected_child_height = child.height.checked_sub(1).ok_or_else(|| {
            MinerError::InvalidParentTemplate("child height must be positive".to_string())
        })?;
        let child_tip_height: u32 = wcash_node.call("getblockcount", json!([]))?;
        let child_tip: String = wcash_node.call("getbestblockhash", json!([]))?;
        if child_tip_height != expected_child_height
            || !child_tip.eq_ignore_ascii_case(&child.previous_block_hash)
        {
            return Err(MinerError::ChildTipMismatch {
                expected: format!(
                    "{} at height {expected_child_height}",
                    child.previous_block_hash
                ),
                endpoint: wcash_node.label().to_string(),
                actual: format!("{child_tip} at height {child_tip_height}"),
            });
        }
        journal.activate_job(ActiveJournalJob {
            job_id: job.job().job_id().to_string(),
            child_hash_display: child.hash.clone(),
            child_height: child.height,
            parent_height: job.parent_height(),
            child_candidate_bytes: child_candidate_bytes.into(),
        })?;
        let candidate_created_at = Instant::now();

        let coordinator = NativeMiningCoordinator {
            wcash_node,
            zcash,
            job,
            child_height: child.height,
            child_previous_hash: child.previous_block_hash,
            child_candidate_lease,
            candidate_created_at,
            freshness: Mutex::new(JobFreshness {
                active: true,
                last_checked: Instant::now(),
            }),
            last_outbox_retry: Mutex::new(Instant::now()),
            outbox_retry_requested: AtomicBool::new(false),
            outbox_retry_in_progress: AtomicBool::new(false),
            journal,
        };
        preparation_guard.disarm();
        Ok(coordinator)
    }
}

struct CandidatePreparationGuard<'a> {
    supervisor: &'a NativeMiningSupervisor,
    lease: Option<ChildCandidateLease>,
}

impl CandidatePreparationGuard<'_> {
    fn disarm(&mut self) {
        self.lease = None;
    }
}

impl Drop for CandidatePreparationGuard<'_> {
    fn drop(&mut self) {
        if let Some(lease) = self.lease.take() {
            self.supervisor.retire_failed_preparation(lease);
        }
    }
}

impl NativeMiningCoordinator {
    /// Opens a one-shot supervisor and prepares one proposal-gated generation.
    pub fn prepare(
        config: CoordinatorConfig,
        journal_path: impl AsRef<Path>,
    ) -> Result<Self, MinerError> {
        NativeMiningSupervisor::open(config, journal_path)?.prepare_generation()
    }

    /// Returns the exact proposal-gated solver job.
    pub const fn job(&self) -> &NativePreparedJob {
        &self.job
    }

    /// Stops issuing this generation and releases its node cache slot when safe.
    ///
    /// Callers must first stop the listener and join every accepted share
    /// handler. The mutable borrow enforces that rule for the built-in native
    /// server, whose handlers share this coordinator through an `Arc`.
    pub fn retire_generation(&mut self) -> Result<GenerationRetirement, MinerError> {
        self.freshness
            .get_mut()
            .map_err(|_| coordinator_mutex_error("job freshness"))?
            .active = false;

        if self
            .journal
            .has_pending_wcash_winner(self.job.job().job_id())?
        {
            return Ok(GenerationRetirement::RetainedForWcashWinner);
        }

        retire_child_candidate(&self.wcash_node, &self.child_candidate_lease)
    }

    /// Returns current durable outbox counts for health reporting.
    pub fn outbox_status(&self) -> Result<WinnerOutboxStatus, MinerError> {
        self.journal.status()
    }

    /// Immediately replays the durable winner outbox without checking whether
    /// this coordinator's already-issued mining job is still current.
    ///
    /// One-shot mining uses this after persisting a winner. Submitting that
    /// winner can advance either chain tip, so coupling the flush to
    /// [`Self::assert_current`] would turn a successful submission into a
    /// false stale-job failure.
    pub fn flush_winner_outbox(&self) -> Result<(), MinerError> {
        self.request_outbox_retry();
        self.retry_pending_if_due()
    }

    /// Fails closed if the child candidate expired or either exact chain tip changed.
    pub fn assert_current(&self) -> Result<(), MinerError> {
        let now = Instant::now();
        let mut freshness = self
            .freshness
            .lock()
            .map_err(|_| coordinator_mutex_error("job freshness"))?;
        if !freshness.active {
            return Err(MinerError::StaleNativeJob(
                "the job was already deactivated".to_string(),
            ));
        }
        if now.saturating_duration_since(self.candidate_created_at) >= MAX_JOB_AGE {
            freshness.active = false;
            return Err(MinerError::StaleNativeJob(format!(
                "the native job reached its {}-second fresh-work lifetime",
                MAX_JOB_AGE.as_secs()
            )));
        }
        if now.saturating_duration_since(freshness.last_checked) < TIP_RECHECK_INTERVAL {
            return Ok(());
        }

        let (child, parent) = thread::scope(|scope| {
            let child = scope.spawn(|| self.check_child_tip());
            let parent = scope.spawn(|| self.zcash.assert_current(&self.job));
            (
                child.join().unwrap_or_else(|_| {
                    Err(MinerError::InvalidParentTemplate(
                        "child tip-check worker panicked".to_string(),
                    ))
                }),
                parent.join().unwrap_or_else(|_| {
                    Err(MinerError::InvalidParentTemplate(
                        "parent tip-check worker panicked".to_string(),
                    ))
                }),
            )
        });
        match (child, parent) {
            (Ok(()), Ok(())) => {
                freshness.last_checked = now;
                Ok(())
            }
            (Err(error), _) | (_, Err(error)) => {
                if matches!(
                    &error,
                    MinerError::ChildTipMismatch { .. } | MinerError::ParentTipMismatch { .. }
                ) {
                    freshness.active = false;
                }
                Err(error)
            }
        }
    }

    fn check_child_tip(&self) -> Result<(), MinerError> {
        let expected_height = self.child_height.checked_sub(1).ok_or_else(|| {
            MinerError::InvalidParentTemplate("child height must be positive".to_string())
        })?;
        let actual_height: u32 = self.wcash_node.call("getblockcount", json!([]))?;
        let actual_hash: String = self.wcash_node.call("getbestblockhash", json!([]))?;
        if actual_height != expected_height
            || !actual_hash.eq_ignore_ascii_case(&self.child_previous_hash)
        {
            return Err(MinerError::ChildTipMismatch {
                expected: format!("{} at height {expected_height}", self.child_previous_hash),
                endpoint: self.wcash_node.label().to_string(),
                actual: format!("{actual_hash} at height {actual_height}"),
            });
        }
        Ok(())
    }

    fn retry_pending_winners(&self, winners: Vec<PendingWinner>) -> Result<(), MinerError> {
        retry_pending_winners_with(&self.wcash_node, &self.zcash, &self.journal, winners)
    }

    fn request_outbox_retry(&self) {
        self.outbox_retry_requested.store(true, Ordering::Release);
    }

    fn retry_pending_if_due(&self) -> Result<(), MinerError> {
        if self
            .outbox_retry_in_progress
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Ok(());
        }
        let _in_progress = OutboxRetryGuard(&self.outbox_retry_in_progress);
        let retry_requested = self.outbox_retry_requested.swap(false, Ordering::AcqRel);
        let now = Instant::now();
        let mut last_retry = self
            .last_outbox_retry
            .lock()
            .map_err(|_| coordinator_mutex_error("outbox retry schedule"))?;
        if !retry_requested && now.saturating_duration_since(*last_retry) < OUTBOX_RETRY_INTERVAL {
            return Ok(());
        }
        *last_retry = now;
        drop(last_retry);
        self.retry_pending_winners(self.journal.all_pending()?)
    }
}

struct OutboxRetryGuard<'a>(&'a AtomicBool);

impl Drop for OutboxRetryGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

fn require_wcash_network_identity(
    node: &ZebraRpcClient,
    expected_genesis_hash: &str,
) -> Result<(), MinerError> {
    let actual: String = node.call("getblockhash", json!([0]))?;
    if !actual.eq_ignore_ascii_case(expected_genesis_hash) {
        return Err(MinerError::NetworkIdentityMismatch {
            expected: expected_genesis_hash.to_string(),
            endpoint: node.label().to_string(),
            actual,
        });
    }
    Ok(())
}

fn validate_wcash_payout_configuration(
    encoded: &str,
    expected_genesis_hash: &str,
) -> Result<(Network, WcashAddress), MinerError> {
    let payout = WcashAddress::try_from_encoded(encoded).map_err(|error| {
        MinerError::InvalidRequest(format!("invalid Wcash payout address: {error}"))
    })?;
    let network = match payout.network() {
        NetworkType::Test => Network::new_wcash_testnet(),
        NetworkType::Regtest => Network::new_wcash_regtest(),
        NetworkType::Main => {
            return Err(MinerError::InvalidRequest(
                "Wcash mainnet payouts are disabled".to_string(),
            ))
        }
    };
    if !network
        .genesis_hash()
        .to_string()
        .eq_ignore_ascii_case(expected_genesis_hash)
    {
        return Err(MinerError::InvalidRequest(
            "Wcash payout address network does not match the pinned child genesis".to_string(),
        ));
    }
    match payout.kind() {
        WcashAddressKind::P2pkh(_) | WcashAddressKind::P2sh(_) => {}
        WcashAddressKind::Unified(address)
            if address
                .items()
                .iter()
                .any(|receiver| matches!(receiver, Receiver::Orchard(_))) => {}
        _ => {
            return Err(MinerError::InvalidRequest(
                "Wcash payout must be transparent or Unified with an Orchard receiver".to_string(),
            ))
        }
    }
    Ok((network, payout))
}

fn validate_wcash_candidate_payout(
    candidate: &Block,
    expected_payout: &WcashAddress,
    network: &Network,
    advertised_value: i64,
    height: u32,
) -> Result<(), MinerError> {
    if advertised_value < 0 {
        return Err(MinerError::InvalidParentTemplate(
            "createauxblock advertised a negative coinbase value".to_string(),
        ));
    }
    let coinbase = candidate.transactions.first().ok_or_else(|| {
        MinerError::InvalidParentTemplate(
            "createauxblock candidate has no coinbase transaction".to_string(),
        )
    })?;
    if !coinbase.is_coinbase() {
        return Err(MinerError::InvalidParentTemplate(
            "createauxblock candidate does not start with a coinbase transaction".to_string(),
        ));
    }

    match expected_payout.kind() {
        WcashAddressKind::P2pkh(hash) => {
            let expected_script =
                transparent::Address::from_pub_key_hash(NetworkKind::Mainnet, *hash).script();
            validate_transparent_wcash_payout(coinbase, &expected_script, advertised_value)
        }
        WcashAddressKind::P2sh(hash) => {
            let expected_script =
                transparent::Address::from_script_hash(NetworkKind::Mainnet, *hash).script();
            validate_transparent_wcash_payout(coinbase, &expected_script, advertised_value)
        }
        WcashAddressKind::Unified(_) => {
            if !coinbase.outputs().is_empty()
                || coinbase.has_sapling_shielded_data()
                || coinbase.has_orchard_shielded_data()
                || coinbase.ironwood_actions().next().is_none()
            {
                return Err(MinerError::InvalidParentTemplate(
                    "createauxblock candidate does not use an Ironwood-only private payout"
                        .to_string(),
                ));
            }
            if coinbase
                .ironwood_value_balance()
                .ironwood_amount()
                .zatoshis()
                != advertised_value.checked_neg().ok_or_else(|| {
                    MinerError::InvalidParentTemplate(
                        "createauxblock coinbase value cannot be negated".to_string(),
                    )
                })?
            {
                return Err(MinerError::InvalidParentTemplate(
                    "createauxblock Ironwood value does not match coinbasevalue".to_string(),
                ));
            }
            if !zebra_chain::primitives::zcash_note_encryption::ironwood_outputs_are_private_from_zero_ovk(
                coinbase,
                network,
                Height(height),
            ) {
                return Err(MinerError::InvalidParentTemplate(
                    "createauxblock Ironwood payout is publicly recoverable".to_string(),
                ));
            }
            Ok(())
        }
        WcashAddressKind::Tex(_) => Err(MinerError::InvalidRequest(
            "TEX is not a Wcash coinbase payout mode".to_string(),
        )),
    }
}

fn validate_transparent_wcash_payout(
    coinbase: &zebra_chain::transaction::Transaction,
    expected_script: &transparent::Script,
    advertised_value: i64,
) -> Result<(), MinerError> {
    if coinbase.outputs().is_empty()
        || coinbase.has_sapling_shielded_data()
        || coinbase.has_orchard_shielded_data()
        || coinbase.has_ironwood_shielded_data()
        || coinbase
            .outputs()
            .iter()
            .any(|output| &output.lock_script != expected_script)
    {
        return Err(MinerError::InvalidParentTemplate(
            "createauxblock candidate does not pay only the configured transparent Wcash address"
                .to_string(),
        ));
    }
    let actual_value = coinbase.outputs().iter().try_fold(0i64, |total, output| {
        total.checked_add(output.value().zatoshis())
    });
    if actual_value != Some(advertised_value) {
        return Err(MinerError::InvalidParentTemplate(
            "createauxblock transparent outputs do not match coinbasevalue".to_string(),
        ));
    }
    Ok(())
}

fn retry_pending_winners_with(
    wcash_node: &ZebraRpcClient,
    zcash: &NativeZcashProvider,
    journal: &ShareJournal,
    winners: Vec<PendingWinner>,
) -> Result<(), MinerError> {
    for batch in winners.chunks(2) {
        let results = thread::scope(|scope| {
            batch
                .iter()
                .map(|winner| {
                    (
                        winner,
                        scope.spawn(move || submit_pending_winner_with(wcash_node, zcash, winner)),
                    )
                })
                .collect::<Vec<_>>()
                .into_iter()
                .map(|(winner, worker)| {
                    (
                        winner,
                        worker.join().unwrap_or(WinnerObservation::Unavailable),
                    )
                })
                .collect::<Vec<_>>()
        });
        for (winner, observation) in results {
            match observation {
                WinnerObservation::Present { confirmations } => {
                    journal.mark_status(winner, WinnerStatus::Observed)?;
                    if confirmations >= WINNER_RETENTION_CONFIRMATIONS {
                        journal.mark_status(winner, WinnerStatus::Matured)?;
                    } else if !winner.observed_on_best_chain {
                        eprintln!(
                            "{} winner {} at height {} is on the best chain with {confirmations} confirmation(s); retaining exact bytes until {}",
                            winner.key.chain.as_str(),
                            winner.block_hash_display,
                            winner.height,
                            WINNER_RETENTION_CONFIRMATIONS,
                        );
                    }
                }
                WinnerObservation::Absent if winner.observed_on_best_chain => {
                    journal.mark_status(winner, WinnerStatus::Orphaned)?;
                    eprintln!(
                        "{} winner {} at height {} left the observed best chain; exact bytes remain queued for replay",
                        winner.key.chain.as_str(),
                        winner.block_hash_display,
                        winner.height,
                    );
                }
                WinnerObservation::ConflictingWitness => {
                    if winner.observed_on_best_chain {
                        journal.mark_status(winner, WinnerStatus::Orphaned)?;
                    }
                    eprintln!(
                        "wcash winner {} at height {} conflicts with a different AuxPoW witness for the same block ID; exact bytes remain queued and operator intervention is required",
                        winner.block_hash_display,
                        winner.height,
                    );
                }
                WinnerObservation::Absent | WinnerObservation::Unavailable => {
                    eprintln!(
                        "{} winner {} at height {} is not yet confirmed; exact bytes remain in the durable outbox",
                        winner.key.chain.as_str(),
                        winner.block_hash_display,
                        winner.height,
                    );
                }
            }
        }
    }
    Ok(())
}

fn submit_pending_winner_with(
    wcash_node: &ZebraRpcClient,
    zcash: &NativeZcashProvider,
    winner: &PendingWinner,
) -> WinnerObservation {
    match winner.key.chain {
        WinnerChain::Wcash => {
            let _submission =
                wcash_node.call_value("submitblock", json!([hex::encode(&winner.block_bytes)]));
            wcash_confirmation_depth(
                wcash_node,
                winner.height,
                &winner.block_hash_display,
                &winner.block_bytes,
            )
        }
        WinnerChain::Zcash => {
            let _submission = zcash.submit_parent_bytes(
                &winner.block_bytes,
                winner.height,
                &winner.block_hash_display,
            );
            match zcash.parent_confirmation_depth(winner.height, &winner.block_hash_display) {
                Ok(Some(confirmations)) => WinnerObservation::Present { confirmations },
                Ok(None) => WinnerObservation::Absent,
                Err(_) => WinnerObservation::Unavailable,
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WinnerObservation {
    Unavailable,
    Absent,
    ConflictingWitness,
    Present { confirmations: u32 },
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WcashBestChainMatch {
    DifferentBlock,
    ConflictingWitness,
    Exact,
}

#[derive(Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
enum WcashAuxBlockStatus {
    BestChain { confirmations: u32 },
    SideChain,
    ConflictingWitness,
    Pending,
    Unknown,
}

fn wcash_confirmation_depth(
    node: &ZebraRpcClient,
    height: u32,
    expected_hash: &str,
    expected_block_bytes: &[u8],
) -> WinnerObservation {
    let expected_block: Block = match expected_block_bytes.zcash_deserialize_into() {
        Ok(block) => block,
        Err(_) => return WinnerObservation::Unavailable,
    };
    let Ok(expected_hash_raw) = parse_display_hash(expected_hash, "Wcash winner block hash") else {
        return WinnerObservation::Unavailable;
    };
    let Ok(canonical) = expected_block.zcash_serialize_to_vec() else {
        return WinnerObservation::Unavailable;
    };
    if canonical != expected_block_bytes
        || expected_block.hash().0 != expected_hash_raw
        || expected_block.coinbase_height().map(u32::from) != Some(height)
    {
        return WinnerObservation::Unavailable;
    }
    let Some(witness) = expected_block.header.solution.as_wcash() else {
        return WinnerObservation::Unavailable;
    };
    let Ok(status) = node.call::<WcashAuxBlockStatus>(
        "getauxblockstatus",
        json!([expected_hash, hex::encode(witness.as_bytes())]),
    ) else {
        return WinnerObservation::Unavailable;
    };
    match status {
        WcashAuxBlockStatus::BestChain { confirmations } => {
            // Durable outbox replay uses `submitblock` so it remains valid after
            // the node's proof-free candidate cache expires or the node
            // restarts. Once authoritative status proves the exact witness is
            // on the best chain, repeat the same witness through
            // `submitauxblock`. Its idempotent best-chain path releases the
            // now-obsolete active candidate; otherwise long maturity runs can
            // fill the bounded cache even though every issued job won.
            let _release = node.call_value(
                "submitauxblock",
                json!([expected_hash, hex::encode(witness.as_bytes())]),
            );
            WinnerObservation::Present { confirmations }
        }
        WcashAuxBlockStatus::SideChain | WcashAuxBlockStatus::Unknown => WinnerObservation::Absent,
        WcashAuxBlockStatus::ConflictingWitness => WinnerObservation::ConflictingWitness,
        WcashAuxBlockStatus::Pending => WinnerObservation::Unavailable,
    }
}

#[cfg(test)]
fn classify_wcash_best_chain_block(
    actual_block_bytes: &[u8],
    expected_block_bytes: &[u8],
    expected_hash_display: &str,
    expected_height: u32,
) -> Result<WcashBestChainMatch, MinerError> {
    let actual: Block = actual_block_bytes
        .zcash_deserialize_into()
        .map_err(|error| {
            MinerError::RpcProtocol(format!("getblock returned an invalid Wcash block: {error}"))
        })?;
    if actual.zcash_serialize_to_vec()? != actual_block_bytes
        || actual.coinbase_height().map(u32::from) != Some(expected_height)
    {
        return Err(MinerError::RpcProtocol(
            "getblock returned non-canonical Wcash bytes or a mismatched coinbase height"
                .to_string(),
        ));
    }
    let expected_hash = parse_display_hash(expected_hash_display, "Wcash winner block hash")?;
    if actual.hash().0 != expected_hash {
        return Ok(WcashBestChainMatch::DifferentBlock);
    }
    if actual_block_bytes != expected_block_bytes {
        return Ok(WcashBestChainMatch::ConflictingWitness);
    }
    Ok(WcashBestChainMatch::Exact)
}

impl NativeMiningCoordinator {
    fn process_with_attribution(
        &self,
        worker: &str,
        authentication: JournalWorkerAuthentication,
        share: &ValidatedNativeShare,
    ) -> Result<(), MinerError> {
        let is_network_winner = share.wcash_candidate().is_some() || share.parent_block().is_some();

        // A tip check on one chain must never suppress a valid winner for the
        // other chain. Persist exact winner bytes first, then let each chain's
        // consensus submission RPC make the authoritative decision.
        if is_network_winner {
            self.journal
                .record(worker, authentication, self.job.job().job_id(), share)?;
            // The client ACK depends only on durable accounting. The dedicated
            // health monitor sees this release-store on its next one-second
            // cycle and performs submission without retaining the ASIC's
            // connection permit across node RPC outages.
            self.request_outbox_retry();
            return Ok(());
        }

        self.assert_current()?;
        self.journal
            .record(worker, authentication, self.job.job().job_id(), share)?;
        Ok(())
    }
}

impl ShareProcessor for NativeMiningCoordinator {
    fn process(&self, worker: &str, share: &ValidatedNativeShare) -> Result<(), MinerError> {
        self.process_with_attribution(worker, JournalWorkerAuthentication::Operator, share)
    }

    fn process_authenticated(
        &self,
        worker: &AuthenticatedWorker,
        share: &ValidatedNativeShare,
    ) -> Result<(), MinerError> {
        self.process_with_attribution(worker.name(), worker.provenance().into(), share)
    }

    fn check_job_health(&self) -> Result<(), MinerError> {
        self.retry_pending_if_due()?;
        self.assert_current()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ChildTemplate {
    hash: String,
    #[serde(rename = "retiretoken")]
    retire_token: String,
    data: String,
    #[serde(rename = "chainid")]
    chain_id: u32,
    target: String,
    height: u32,
    #[serde(rename = "previousblockhash")]
    previous_block_hash: String,
    bits: String,
    #[allow(dead_code)]
    #[serde(rename = "coinbasevalue")]
    coinbase_value: i64,
}

struct ShareJournal {
    state: Mutex<JournalState>,
    path: PathBuf,
}

#[derive(Clone)]
struct ActiveJournalJob {
    job_id: String,
    child_hash_display: String,
    child_height: u32,
    parent_height: u32,
    child_candidate_bytes: Arc<[u8]>,
}

struct JournalState {
    file: File,
    bytes_written: u64,
    /// Set after an append or durability failure. A subsequent append could
    /// otherwise terminate a partial JSON line and make crash recovery parse
    /// attacker-controlled concatenated data as a complete record.
    poisoned: bool,
    active_job: Option<Arc<ActiveJournalJob>>,
    seen_job_ids: HashSet<[u8; 32]>,
    active_job_attributions: HashMap<[u8; 32], ShareReplayIdentity>,
    active_winner_attributions: HashMap<[u8; 32], ShareReplayIdentity>,
    pending_winners: HashMap<WinnerKey, PendingWinner>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ShareAttribution {
    worker: String,
    authentication: JournalWorkerAuthentication,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ShareReplayIdentity {
    attribution: ShareAttribution,
    /// Retained so two persisted records cannot rewrite acceptance history.
    /// Live idempotent retries compare the remaining deterministic fields.
    accepted_at: Option<u64>,
    /// Binds every immutable share/winner field except `accepted_at`, which is
    /// generated on first acceptance and cannot be reproduced by a retry.
    immutable_fingerprint: [u8; 32],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum JournalWorkerAuthentication {
    Operator,
    ExactCredential,
    SharedSecret,
    LegacyUnknown,
}

impl JournalWorkerAuthentication {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Operator => "operator",
            Self::ExactCredential => "exact_credential",
            Self::SharedSecret => "shared_secret",
            Self::LegacyUnknown => "legacy_unknown",
        }
    }

    fn from_persisted(value: Option<&str>) -> Result<Self, MinerError> {
        match value {
            Some("operator") => Ok(Self::Operator),
            Some("exact_credential") => Ok(Self::ExactCredential),
            Some("shared_secret") => Ok(Self::SharedSecret),
            None => Ok(Self::LegacyUnknown),
            Some(value) => Err(MinerError::InvalidRequest(format!(
                "share journal contains unknown worker authentication provenance {value:?}"
            ))),
        }
    }
}

impl From<WorkerAuthenticationProvenance> for JournalWorkerAuthentication {
    fn from(provenance: WorkerAuthenticationProvenance) -> Self {
        match provenance {
            WorkerAuthenticationProvenance::ExactCredential => Self::ExactCredential,
            WorkerAuthenticationProvenance::SharedSecret => Self::SharedSecret,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum WinnerChain {
    Wcash,
    Zcash,
}

impl WinnerChain {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Wcash => "wcash",
            Self::Zcash => "zcash",
        }
    }

    fn parse(value: &str) -> Result<Self, MinerError> {
        match value {
            "wcash" => Ok(Self::Wcash),
            "zcash" => Ok(Self::Zcash),
            _ => Err(MinerError::InvalidRequest(format!(
                "share journal contains unknown winner chain {value:?}"
            ))),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct WinnerKey {
    share_id: [u8; 32],
    chain: WinnerChain,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PendingWinner {
    key: WinnerKey,
    job_id: String,
    block_hash_display: String,
    height: u32,
    block_bytes: Vec<u8>,
    observed_on_best_chain: bool,
}

const MAX_SHARES_PER_JOB: usize = 100_000;
const MAX_PENDING_WINNERS: usize = 4_096;
const MAX_REPLAYED_WINNER_SHARES: usize = 1_000_000;
pub(crate) const MAX_JOURNAL_GENERATIONS: usize = 1_000_000;
const MAX_JOURNAL_RECORD_BYTES: usize = 10 * 1024 * 1024;
const MAX_JOURNAL_BYTES: u64 = 1024 * 1024 * 1024;
const JOURNAL_VERSION: u8 = 2;

impl ShareJournal {
    fn open(path: impl AsRef<Path>) -> Result<Self, MinerError> {
        let path = path.as_ref();
        if path.as_os_str().is_empty() {
            return Err(MinerError::InvalidRequest(
                "share journal path is empty".to_string(),
            ));
        }
        reject_existing_journal_symlink(path)?;
        let mut options = OpenOptions::new();
        options.create(true).append(true).read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options.open(path)?;
        fs2::FileExt::try_lock_exclusive(&file).map_err(|error| {
            MinerError::InvalidRequest(format!(
                "share journal {} is already locked or cannot be locked: {error}",
                path.display()
            ))
        })?;
        if !file.metadata()?.is_file() {
            return Err(MinerError::InvalidRequest(format!(
                "share journal {} is not a regular file",
                path.display()
            )));
        }
        reject_existing_journal_symlink(path)?;
        verify_locked_journal_path(&file, path)?;
        let journal_bytes = file.metadata()?.len();
        if journal_bytes > MAX_JOURNAL_BYTES {
            return Err(MinerError::InvalidRequest(format!(
                "share journal {} exceeds the {MAX_JOURNAL_BYTES}-byte safety limit",
                path.display()
            )));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = file.metadata()?.permissions().mode();
            if mode & 0o077 != 0 {
                return Err(MinerError::InvalidRequest(format!(
                    "share journal {} must not be accessible by group or other users",
                    path.display()
                )));
            }
            let parent = journal_parent_directory(path);
            let parent_mode = fs::metadata(parent)?.permissions().mode();
            if parent_mode & 0o022 != 0 {
                return Err(MinerError::InvalidRequest(format!(
                    "share journal parent directory {} must not be writable by group or other users",
                    parent.display()
                )));
            }
        }
        sync_journal_parent_directory(path)?;
        let active_job_attributions = HashMap::new();
        let active_winner_attributions = HashMap::new();
        let mut pending_winners = HashMap::new();
        let mut replayed_winner_identities = HashMap::new();
        let mut seen_job_ids = HashSet::new();
        // The accounting report is the authoritative interpretation of journal
        // history. Run that exact parser before accepting any new share so the
        // online outbox can never continue from a ledger that accounting would
        // later reject. Rewind the cloned descriptor because `File::try_clone`
        // can share its seek position with the locked handle.
        let mut reader = BufReader::new(file.try_clone()?);
        read_accounting_snapshot_from_reader(&mut reader)?;
        let mut replay_file = reader.into_inner();
        replay_file.seek(SeekFrom::Start(0))?;
        let mut reader = BufReader::new(replay_file);
        let mut complete_bytes = 0u64;
        while let Some((line, terminated, consumed)) = read_bounded_journal_line(&mut reader)? {
            if !terminated {
                // A crash can leave one unacknowledged partial append. Discard
                // only that unterminated tail before any new records are added.
                file.set_len(complete_bytes)?;
                file.sync_all()?;
                break;
            }
            complete_bytes = complete_bytes.checked_add(consumed).ok_or_else(|| {
                MinerError::InvalidRequest("share journal length overflowed".to_string())
            })?;
            let record: PersistedShareKey = serde_json::from_slice(&line).map_err(|error| {
                MinerError::InvalidRequest(format!("share journal is corrupted: {error}"))
            })?;
            if record.version != JOURNAL_VERSION {
                return Err(MinerError::InvalidRequest(format!(
                    "share journal contains unsupported version {} record {:?}",
                    record.version, record.record
                )));
            }
            let job_id = parse_job_id(&record.job_id)?;
            match record.record.as_str() {
                "job_activated" => {
                    validate_persisted_job_activation(&record)?;
                    if seen_job_ids.contains(&job_id) {
                        return Err(MinerError::InvalidRequest(
                            "share journal activates a previously used job ID".to_string(),
                        ));
                    }
                    insert_seen_job_id(&mut seen_job_ids, job_id)?;
                }
                "accepted_share" => {
                    required_share_id(&record)?;
                    // A legacy v2 journal might predate explicit activation
                    // records. Its job IDs remain permanently reserved.
                    insert_seen_job_id(&mut seen_job_ids, job_id)?;
                }
                "winner_outbox" => {
                    let share_id = required_share_id(&record)?;
                    insert_seen_job_id(&mut seen_job_ids, job_id)?;
                    let identity = persisted_journal_record_fingerprint(&record);
                    if let Some(previous) = replayed_winner_identities.get(&share_id) {
                        if previous != &identity {
                            return Err(MinerError::InvalidRequest(
                                "share journal redefines a winner share ID with different immutable data"
                                    .to_string(),
                            ));
                        }
                        // An exact duplicate append is idempotent. In
                        // particular, it must not resurrect an already-matured
                        // outbox entry later in the same journal.
                        continue;
                    } else {
                        replayed_winner_identities.insert(share_id, identity);
                        if replayed_winner_identities.len() > MAX_REPLAYED_WINNER_SHARES {
                            return Err(MinerError::InvalidRequest(format!(
                                "share journal contains more than {MAX_REPLAYED_WINNER_SHARES} distinct winner shares"
                            )));
                        }
                    }
                    let winners = persisted_winners(&record, share_id)?;
                    if winners.is_empty() {
                        return Err(MinerError::InvalidRequest(
                            "winner_outbox record contains no network winner".to_string(),
                        ));
                    }
                    for winner in winners {
                        if let Some(previous) = pending_winners.get(&winner.key) {
                            if previous != &winner {
                                return Err(MinerError::InvalidRequest(
                                    "share journal redefines an existing winner with different immutable data"
                                        .to_string(),
                                ));
                            }
                            continue;
                        }
                        pending_winners.insert(winner.key, winner);
                        if pending_winners.len() > MAX_PENDING_WINNERS {
                            return Err(MinerError::InvalidRequest(format!(
                                "share journal contains more than {MAX_PENDING_WINNERS} unconfirmed network winners"
                            )));
                        }
                    }
                }
                "winner_observed" | "winner_orphaned" | "winner_matured" | "winner_confirmed" => {
                    let share_id = required_share_id(&record)?;
                    let chain = WinnerChain::parse(record.chain.as_deref().ok_or_else(|| {
                        MinerError::InvalidRequest("winner status record has no chain".to_string())
                    })?)?;
                    let key = WinnerKey { share_id, chain };
                    let pending = pending_winners.get_mut(&key).ok_or_else(|| {
                        MinerError::InvalidRequest(
                            "share journal updates an unknown or already-matured winner"
                                .to_string(),
                        )
                    })?;
                    let confirmed_hash = record.block_hash.as_deref().ok_or_else(|| {
                        MinerError::InvalidRequest(
                            "winner status record has no block_hash".to_string(),
                        )
                    })?;
                    let confirmed_height = record.height.ok_or_else(|| {
                        MinerError::InvalidRequest("winner status record has no height".to_string())
                    })?;
                    if pending.job_id != record.job_id
                        || !pending
                            .block_hash_display
                            .eq_ignore_ascii_case(confirmed_hash)
                        || pending.height != confirmed_height
                    {
                        return Err(MinerError::InvalidRequest(
                            "winner status metadata does not match its immutable outbox entry"
                                .to_string(),
                        ));
                    }
                    match record.record.as_str() {
                        "winner_observed" | "winner_confirmed" => {
                            if pending.observed_on_best_chain {
                                return Err(MinerError::InvalidRequest(
                                    "share journal observes the same winner twice without an intervening orphan transition"
                                        .to_string(),
                                ));
                            }
                            pending.observed_on_best_chain = true;
                        }
                        "winner_orphaned" => {
                            if !pending.observed_on_best_chain {
                                return Err(MinerError::InvalidRequest(
                                    "share journal orphans a winner that was not observed"
                                        .to_string(),
                                ));
                            }
                            pending.observed_on_best_chain = false;
                        }
                        "winner_matured" => {
                            if !pending.observed_on_best_chain {
                                return Err(MinerError::InvalidRequest(
                                    "share journal matures a winner before best-chain observation"
                                        .to_string(),
                                ));
                            }
                            pending_winners.remove(&key);
                        }
                        _ => unreachable!("status records are filtered by the outer match"),
                    }
                }
                _ => {
                    return Err(MinerError::InvalidRequest(format!(
                        "share journal contains unsupported version {} record {:?}",
                        record.version, record.record
                    )));
                }
            }
        }
        Ok(Self {
            state: Mutex::new(JournalState {
                file,
                bytes_written: complete_bytes,
                poisoned: false,
                active_job: None,
                seen_job_ids,
                active_job_attributions,
                active_winner_attributions,
                pending_winners,
            }),
            path: path.to_path_buf(),
        })
    }

    fn activate_job(&self, active_job: ActiveJournalJob) -> Result<(), MinerError> {
        let job_id = parse_job_id(&active_job.job_id)?;
        parse_canonical_journal_hex(&active_job.child_hash_display, "activated Wcash block hash")?;
        if active_job.child_height == 0 || active_job.parent_height == 0 {
            return Err(MinerError::InvalidRequest(
                "activated native job heights must be positive".to_string(),
            ));
        }
        let mut state = self.lock_state()?;
        if state.seen_job_ids.contains(&job_id) {
            return Err(MinerError::InvalidRequest(format!(
                "native job ID {} was already activated or recorded; refusing unsafe reuse",
                active_job.job_id
            )));
        }
        ensure_job_generation_capacity(state.seen_job_ids.len(), false)?;
        let record = json!({
            "version": JOURNAL_VERSION,
            "record": "job_activated",
            "recorded_at": unix_timestamp()?,
            "job_id": &active_job.job_id,
            "wcash_block_hash": &active_job.child_hash_display,
            "wcash_height": active_job.child_height,
            "zcash_height": active_job.parent_height,
        });
        let mut encoded = serde_json::to_vec(&record)?;
        encoded.push(b'\n');
        append_synced_journal_record(&mut state, &self.path, &encoded)?;
        state.seen_job_ids.insert(job_id);
        state.active_job_attributions.clear();
        state.active_winner_attributions.clear();
        state.active_job = Some(Arc::new(active_job));
        Ok(())
    }

    fn record(
        &self,
        worker: &str,
        authentication: JournalWorkerAuthentication,
        job_id: &str,
        share: &ValidatedNativeShare,
    ) -> Result<[u8; 32], MinerError> {
        validate_attribution_worker(worker)?;
        let mut state = self.lock_state()?;
        let active_job = active_job_for_share(&state, job_id)?;
        let share_id = share_id(job_id, share);
        let is_network_winner = share.wcash_candidate().is_some() || share.parent_block().is_some();
        let wcash_block = share
            .wcash_candidate()
            .map(|winner| {
                complete_wcash_candidate(
                    active_job.child_candidate_bytes.as_ref(),
                    &active_job.child_hash_display,
                    winner.encoded_proof(),
                )
            })
            .transpose()?;
        let zcash_block = share.parent_block().map(<[u8]>::to_vec);
        let timestamp = unix_timestamp()?;
        let record = json!({
            "version": JOURNAL_VERSION,
            "record": if is_network_winner { "winner_outbox" } else { "accepted_share" },
            "accepted_at": timestamp,
            "share_id": hex::encode(share_id),
            "worker": worker,
            "worker_authentication": authentication.as_str(),
            "job_id": job_id,
            "parent_hash_le": hex::encode(share.parent_block_hash().into_le_bytes()),
            "share_target": display_target(share.accepted_target()),
            "wcash_candidate": share.wcash_candidate().is_some(),
            "zcash_candidate": share.parent_block().is_some(),
            "wcash_block_hash": &active_job.child_hash_display,
            "wcash_height": active_job.child_height,
            "zcash_height": active_job.parent_height,
            "wcash_block": wcash_block.as_deref().map(hex::encode),
            "zcash_block": zcash_block.as_deref().map(hex::encode),
        });
        let persisted_record: PersistedShareKey = serde_json::from_value(record.clone())?;
        let identity = persisted_share_identity(&persisted_record)?;
        if is_matching_replay(&state, share_id, &identity)? {
            return Ok(share_id);
        }
        if !is_network_winner && state.active_job_attributions.len() >= MAX_SHARES_PER_JOB {
            return Err(MinerError::InvalidRequest(format!(
                "share journal reached the {MAX_SHARES_PER_JOB}-share per-job safety limit; rotate work before accepting more shares"
            )));
        }
        if is_network_winner {
            let additional_winners = [
                (WinnerChain::Wcash, wcash_block.is_some()),
                (WinnerChain::Zcash, zcash_block.is_some()),
            ]
            .into_iter()
            .filter(|(chain, present)| {
                *present
                    && !state.pending_winners.contains_key(&WinnerKey {
                        share_id,
                        chain: *chain,
                    })
            })
            .count();
            ensure_pending_winner_capacity(state.pending_winners.len(), additional_winners)?;
        }
        let mut encoded = serde_json::to_vec(&record)?;
        encoded.push(b'\n');
        append_synced_journal_record(&mut state, &self.path, &encoded)?;
        if is_network_winner {
            state.active_winner_attributions.insert(share_id, identity);
            if let Some(block_bytes) = wcash_block {
                let key = WinnerKey {
                    share_id,
                    chain: WinnerChain::Wcash,
                };
                state.pending_winners.insert(
                    key,
                    PendingWinner {
                        key,
                        job_id: job_id.to_string(),
                        block_hash_display: active_job.child_hash_display.clone(),
                        height: active_job.child_height,
                        block_bytes,
                        observed_on_best_chain: false,
                    },
                );
            }
            if let Some(block_bytes) = zcash_block {
                let key = WinnerKey {
                    share_id,
                    chain: WinnerChain::Zcash,
                };
                state.pending_winners.insert(
                    key,
                    PendingWinner {
                        key,
                        job_id: job_id.to_string(),
                        block_hash_display: display_hash(share.parent_block_hash().into_le_bytes()),
                        height: active_job.parent_height,
                        block_bytes,
                        observed_on_best_chain: false,
                    },
                );
            }
        } else {
            state.active_job_attributions.insert(share_id, identity);
        }
        Ok(share_id)
    }

    fn all_pending(&self) -> Result<Vec<PendingWinner>, MinerError> {
        Ok(self
            .lock_state()?
            .pending_winners
            .values()
            .cloned()
            .collect())
    }

    fn has_pending_wcash_winner(&self, job_id: &str) -> Result<bool, MinerError> {
        Ok(self
            .lock_state()?
            .pending_winners
            .values()
            .any(|winner| winner.key.chain == WinnerChain::Wcash && winner.job_id == job_id))
    }

    fn status(&self) -> Result<WinnerOutboxStatus, MinerError> {
        let state = self.lock_state()?;
        let mut status = WinnerOutboxStatus {
            retention_confirmations: WINNER_RETENTION_CONFIRMATIONS,
            ..WinnerOutboxStatus::default()
        };
        for winner in state.pending_winners.values() {
            match winner.key.chain {
                WinnerChain::Wcash => status.pending_wcash += 1,
                WinnerChain::Zcash => status.pending_zcash += 1,
            }
            status.observed += usize::from(winner.observed_on_best_chain);
        }
        Ok(status)
    }

    fn mark_status(&self, winner: &PendingWinner, status: WinnerStatus) -> Result<(), MinerError> {
        let mut state = self.lock_state()?;
        let Some(current) = state.pending_winners.get(&winner.key) else {
            return Ok(());
        };
        if current.job_id != winner.job_id
            || current.block_hash_display != winner.block_hash_display
            || current.height != winner.height
            || current.block_bytes != winner.block_bytes
        {
            return Err(MinerError::InvalidRequest(
                "winner status does not match the durable outbox entry".to_string(),
            ));
        }
        match status {
            WinnerStatus::Observed if current.observed_on_best_chain => return Ok(()),
            WinnerStatus::Orphaned if !current.observed_on_best_chain => return Ok(()),
            WinnerStatus::Matured if !current.observed_on_best_chain => {
                return Err(MinerError::InvalidRequest(
                    "cannot mature a winner before best-chain observation".to_string(),
                ))
            }
            _ => {}
        }
        let timestamp = unix_timestamp()?;
        let record = json!({
            "version": JOURNAL_VERSION,
            "record": status.as_record(),
            "recorded_at": timestamp,
            "share_id": hex::encode(winner.key.share_id),
            "job_id": &winner.job_id,
            "chain": winner.key.chain.as_str(),
            "block_hash": &winner.block_hash_display,
            "height": winner.height,
        });
        let mut encoded = serde_json::to_vec(&record)?;
        encoded.push(b'\n');
        append_synced_journal_record(&mut state, &self.path, &encoded)?;
        match status {
            WinnerStatus::Observed => {
                state
                    .pending_winners
                    .get_mut(&winner.key)
                    .expect("winner existence was checked while holding the journal lock")
                    .observed_on_best_chain = true;
            }
            WinnerStatus::Orphaned => {
                state
                    .pending_winners
                    .get_mut(&winner.key)
                    .expect("winner existence was checked while holding the journal lock")
                    .observed_on_best_chain = false;
            }
            WinnerStatus::Matured => {
                state.pending_winners.remove(&winner.key);
            }
        }
        Ok(())
    }

    fn lock_state(&self) -> Result<MutexGuard<'_, JournalState>, MinerError> {
        self.state.lock().map_err(|_| journal_mutex_error())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WinnerStatus {
    Observed,
    Orphaned,
    Matured,
}

impl WinnerStatus {
    const fn as_record(self) -> &'static str {
        match self {
            Self::Observed => "winner_observed",
            Self::Orphaned => "winner_orphaned",
            Self::Matured => "winner_matured",
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedShareKey {
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
    wcash_candidate: bool,
    #[serde(default)]
    zcash_candidate: bool,
    #[serde(default)]
    parent_hash_le: Option<String>,
    #[serde(default)]
    share_target: Option<String>,
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

fn required_share_id(record: &PersistedShareKey) -> Result<[u8; 32], MinerError> {
    parse_share_id(required_journal_field(
        record.share_id.as_deref(),
        "share_id",
    )?)
}

fn validate_persisted_job_activation(record: &PersistedShareKey) -> Result<(), MinerError> {
    parse_job_id(&record.job_id)?;
    if record.recorded_at == Some(0)
        || record.recorded_at.is_none()
        || record.accepted_at.is_some()
        || record.share_id.is_some()
        || record.worker.is_some()
        || record.worker_authentication.is_some()
        || record.parent_hash_le.is_some()
        || record.share_target.is_some()
        || record.wcash_candidate
        || record.zcash_candidate
        || record.wcash_block.is_some()
        || record.zcash_block.is_some()
        || record.chain.is_some()
        || record.block_hash.is_some()
        || record.height.is_some()
    {
        return Err(MinerError::InvalidRequest(
            "share journal contains malformed job activation metadata".to_string(),
        ));
    }
    let child_hash = required_journal_field(
        record.wcash_block_hash.as_deref(),
        "job_activated.wcash_block_hash",
    )?;
    parse_canonical_journal_hex(child_hash, "job_activated.wcash_block_hash")?;
    if record.wcash_height.is_none_or(|height| height == 0)
        || record.zcash_height.is_none_or(|height| height == 0)
    {
        return Err(MinerError::InvalidRequest(
            "share journal job activation has no positive chain heights".to_string(),
        ));
    }
    Ok(())
}

fn active_job_for_share(
    state: &JournalState,
    job_id: &str,
) -> Result<Arc<ActiveJournalJob>, MinerError> {
    let active_job = state.active_job.as_ref().ok_or_else(|| {
        MinerError::InvalidRequest("share journal has no active mining job".to_string())
    })?;
    if active_job.job_id != job_id {
        return Err(MinerError::InvalidRequest(
            "share belongs to a different or retired journal generation".to_string(),
        ));
    }
    Ok(Arc::clone(active_job))
}

fn persisted_share_attribution(record: &PersistedShareKey) -> Result<ShareAttribution, MinerError> {
    let worker = required_journal_field(record.worker.as_deref(), "share.worker")?;
    validate_attribution_worker(worker)?;
    Ok(ShareAttribution {
        worker: worker.to_string(),
        authentication: JournalWorkerAuthentication::from_persisted(
            record.worker_authentication.as_deref(),
        )?,
    })
}

fn persisted_share_identity(record: &PersistedShareKey) -> Result<ShareReplayIdentity, MinerError> {
    if record.accepted_at == Some(0) {
        return Err(MinerError::InvalidRequest(
            "share journal has an invalid zero acceptance timestamp".to_string(),
        ));
    }
    Ok(ShareReplayIdentity {
        attribution: persisted_share_attribution(record)?,
        accepted_at: record.accepted_at,
        immutable_fingerprint: persisted_share_fingerprint(record),
    })
}

fn persisted_journal_record_fingerprint(record: &PersistedShareKey) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"Wcash/share-journal/exact-record/v1\0");
    update_fingerprint_optional_u64(&mut digest, record.accepted_at);
    digest.update(persisted_share_fingerprint(record));
    digest.finalize().into()
}

fn persisted_share_fingerprint(record: &PersistedShareKey) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"Wcash/share-journal/immutable-replay/v1\0");
    update_fingerprint_bytes(&mut digest, record.record.as_bytes());
    update_fingerprint_bytes(&mut digest, record.job_id.as_bytes());
    update_fingerprint_optional(&mut digest, record.worker.as_deref());
    update_fingerprint_optional(&mut digest, record.worker_authentication.as_deref());
    update_fingerprint_optional(&mut digest, record.parent_hash_le.as_deref());
    update_fingerprint_optional(&mut digest, record.share_target.as_deref());
    digest.update([
        u8::from(record.wcash_candidate),
        u8::from(record.zcash_candidate),
    ]);
    update_fingerprint_optional(&mut digest, record.wcash_block_hash.as_deref());
    update_fingerprint_optional_u32(&mut digest, record.wcash_height);
    update_fingerprint_optional_u32(&mut digest, record.zcash_height);
    update_fingerprint_optional(&mut digest, record.wcash_block.as_deref());
    update_fingerprint_optional(&mut digest, record.zcash_block.as_deref());
    digest.finalize().into()
}

fn update_fingerprint_bytes(digest: &mut Sha256, value: &[u8]) {
    digest.update((value.len() as u64).to_le_bytes());
    digest.update(value);
}

fn update_fingerprint_optional(digest: &mut Sha256, value: Option<&str>) {
    digest.update([u8::from(value.is_some())]);
    if let Some(value) = value {
        update_fingerprint_bytes(digest, value.as_bytes());
    }
}

fn update_fingerprint_optional_u32(digest: &mut Sha256, value: Option<u32>) {
    digest.update([u8::from(value.is_some())]);
    if let Some(value) = value {
        digest.update(value.to_le_bytes());
    }
}

fn update_fingerprint_optional_u64(digest: &mut Sha256, value: Option<u64>) {
    digest.update([u8::from(value.is_some())]);
    if let Some(value) = value {
        digest.update(value.to_le_bytes());
    }
}

fn validate_attribution_worker(worker: &str) -> Result<(), MinerError> {
    if worker.is_empty()
        || worker.len() > 128
        || !worker
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_' | b':'))
    {
        return Err(MinerError::InvalidRequest(
            "share worker must contain 1..=128 ASCII letters, digits, '.', '-', '_', or ':'"
                .to_string(),
        ));
    }
    Ok(())
}

fn is_matching_replay(
    state: &JournalState,
    share_id: [u8; 32],
    identity: &ShareReplayIdentity,
) -> Result<bool, MinerError> {
    let existing = state
        .active_job_attributions
        .get(&share_id)
        .or_else(|| state.active_winner_attributions.get(&share_id));
    match existing {
        None => Ok(false),
        Some(existing)
            if existing.attribution == identity.attribution
                && existing.immutable_fingerprint == identity.immutable_fingerprint =>
        {
            Ok(true)
        }
        Some(_) => Err(MinerError::InvalidRequest(
            "share replay conflicts with immutable data in the durable journal entry".to_string(),
        )),
    }
}

fn persisted_winners(
    record: &PersistedShareKey,
    share_id: [u8; 32],
) -> Result<Vec<PendingWinner>, MinerError> {
    let expected_parent_hash = parse_raw_hash(
        required_journal_field(
            record.parent_hash_le.as_deref(),
            "winner_outbox.parent_hash_le",
        )?,
        "winner_outbox.parent_hash_le",
    )?;
    let mut winners = Vec::with_capacity(2);
    if record.wcash_candidate {
        let block_hash_display = required_journal_field(
            record.wcash_block_hash.as_deref(),
            "winner_outbox.wcash_block_hash",
        )?
        .to_string();
        parse_display_hash(&block_hash_display, "winner_outbox.wcash_block_hash")?;
        let height = record.wcash_height.ok_or_else(|| {
            MinerError::InvalidRequest("winner_outbox has no Wcash height".to_string())
        })?;
        let block_bytes = decode_bounded_hex(
            required_journal_field(record.wcash_block.as_deref(), "winner_outbox.wcash_block")?,
            "winner_outbox.wcash_block",
            MAX_CHILD_BLOCK_BYTES,
        )?;
        let recovered_parent_hash = validate_persisted_winner_block(
            &block_bytes,
            &block_hash_display,
            height,
            PersistedWinnerBlockKind::Wcash,
        )?;
        if recovered_parent_hash != expected_parent_hash {
            return Err(MinerError::InvalidRequest(
                "share journal Wcash winner is bound to a different parent header".to_string(),
            ));
        }
        let key = WinnerKey {
            share_id,
            chain: WinnerChain::Wcash,
        };
        winners.push(PendingWinner {
            key,
            job_id: record.job_id.clone(),
            block_hash_display,
            height,
            block_bytes,
            observed_on_best_chain: false,
        });
    }
    if record.zcash_candidate {
        let block_hash_display = display_hash(expected_parent_hash);
        let height = record.zcash_height.ok_or_else(|| {
            MinerError::InvalidRequest("winner_outbox has no Zcash height".to_string())
        })?;
        let block_bytes = decode_bounded_hex(
            required_journal_field(record.zcash_block.as_deref(), "winner_outbox.zcash_block")?,
            "winner_outbox.zcash_block",
            MAX_CHILD_BLOCK_BYTES,
        )?;
        let recovered_parent_hash = validate_persisted_winner_block(
            &block_bytes,
            &block_hash_display,
            height,
            PersistedWinnerBlockKind::Zcash,
        )?;
        if recovered_parent_hash != expected_parent_hash {
            return Err(MinerError::InvalidRequest(
                "share journal Zcash winner is bound to a different parent header".to_string(),
            ));
        }
        let key = WinnerKey {
            share_id,
            chain: WinnerChain::Zcash,
        };
        winners.push(PendingWinner {
            key,
            job_id: record.job_id.clone(),
            block_hash_display,
            height,
            block_bytes,
            observed_on_best_chain: false,
        });
    }
    Ok(winners)
}

fn required_journal_field<'a>(value: Option<&'a str>, field: &str) -> Result<&'a str, MinerError> {
    value.ok_or_else(|| MinerError::InvalidRequest(format!("share journal has no {field}")))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PersistedWinnerBlockKind {
    Wcash,
    Zcash,
}

/// Consensus-validates exact winner bytes and their immutable journal metadata.
pub(crate) fn validate_persisted_winner_block(
    block_bytes: &[u8],
    expected_hash_display: &str,
    expected_height: u32,
    kind: PersistedWinnerBlockKind,
) -> Result<[u8; 32], MinerError> {
    let block: Block = block_bytes.zcash_deserialize_into().map_err(|error| {
        MinerError::InvalidRequest(format!("share journal contains an invalid block: {error}"))
    })?;
    if block.zcash_serialize_to_vec()? != block_bytes {
        return Err(MinerError::InvalidRequest(
            "share journal contains a non-canonical block".to_string(),
        ));
    }
    let expected_hash = parse_display_hash(expected_hash_display, "winner_outbox block hash")?;
    let expected_version = match kind {
        PersistedWinnerBlockKind::Wcash => Some(WCASH_BLOCK_WIRE_VERSION),
        PersistedWinnerBlockKind::Zcash => None,
    };
    if block.hash().0 != expected_hash
        || block.coinbase_height().map(u32::from) != Some(expected_height)
        || expected_version.is_some_and(|version| block.header.version != version)
    {
        return Err(MinerError::InvalidRequest(
            "share journal winner metadata does not match its exact block".to_string(),
        ));
    }
    if expected_version.is_some()
        && block
            .header
            .solution
            .as_wcash()
            .is_none_or(|witness| witness.is_empty())
    {
        return Err(MinerError::InvalidRequest(
            "share journal Wcash winner has no AuxPoW witness".to_string(),
        ));
    }
    let expanded = block
        .header
        .difficulty_threshold
        .to_expanded()
        .ok_or_else(|| {
            MinerError::InvalidRequest(format!(
                "share journal {} winner has an invalid compact target",
                match kind {
                    PersistedWinnerBlockKind::Wcash => "Wcash",
                    PersistedWinnerBlockKind::Zcash => "Zcash",
                }
            ))
        })?;
    if expanded.to_compact() != block.header.difficulty_threshold {
        return Err(MinerError::InvalidRequest(format!(
            "share journal {} winner has a non-canonical compact target",
            match kind {
                PersistedWinnerBlockKind::Wcash => "Wcash",
                PersistedWinnerBlockKind::Zcash => "Zcash",
            }
        )));
    }
    let expanded_value: U256 = expanded.into();
    let target = Target::from_le_bytes(expanded_value.to_little_endian()).map_err(|error| {
        MinerError::InvalidRequest(format!(
            "share journal winner has an invalid target: {error}"
        ))
    })?;

    let parent_hash = if expected_version.is_some() {
        let witness = block
            .header
            .solution
            .as_wcash()
            .expect("the Wcash witness variant and non-empty bytes were checked above");
        let proof = AuxPowProof::decode(witness.as_bytes()).map_err(|error| {
            MinerError::InvalidRequest(format!(
                "share journal Wcash winner contains malformed AuxPoW: {error}"
            ))
        })?;
        proof.validate(block.hash().0, target).map_err(|error| {
            MinerError::InvalidRequest(format!(
                "share journal Wcash winner contains invalid AuxPoW: {error}"
            ))
        })?;
        proof.parent_header().block_hash().into_le_bytes()
    } else {
        if !target.is_met_by_le_hash(block.hash().0) {
            return Err(MinerError::InvalidRequest(
                "share journal Zcash winner does not meet its compact target".to_string(),
            ));
        }
        block
            .header
            .solution
            .check(&block.header)
            .map_err(|error| {
                MinerError::InvalidRequest(format!(
                    "share journal Zcash winner contains invalid Equihash: {error}"
                ))
            })?;
        block.hash().0
    };
    Ok(parent_hash)
}

fn read_bounded_journal_line<R: BufRead>(
    reader: &mut R,
) -> Result<Option<(Vec<u8>, bool, u64)>, MinerError> {
    let mut line = Vec::new();
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            let consumed = u64::try_from(line.len()).map_err(|_| {
                MinerError::InvalidRequest("share journal line length overflowed".to_string())
            })?;
            return Ok((!line.is_empty()).then_some((line, false, consumed)));
        }
        let (chunk_len, terminated) = match available.iter().position(|byte| *byte == b'\n') {
            Some(position) => (position, true),
            None => (available.len(), false),
        };
        if line.len().saturating_add(chunk_len) > MAX_JOURNAL_RECORD_BYTES {
            return Err(MinerError::InvalidRequest(format!(
                "share journal contains a record larger than {MAX_JOURNAL_RECORD_BYTES} bytes"
            )));
        }
        line.extend_from_slice(&available[..chunk_len]);
        let consumed_now = chunk_len + usize::from(terminated);
        reader.consume(consumed_now);
        if terminated {
            let consumed = u64::try_from(line.len().saturating_add(1)).map_err(|_| {
                MinerError::InvalidRequest("share journal line length overflowed".to_string())
            })?;
            return Ok(Some((line, true, consumed)));
        }
    }
}

fn share_id(job_id: &str, share: &ValidatedNativeShare) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"Wcash/share-journal/v2\0");
    hash.update(
        u64::try_from(job_id.len())
            .expect("usize lengths fit in u64 on supported platforms")
            .to_le_bytes(),
    );
    hash.update(job_id.as_bytes());
    hash.update(share.parent_block_hash().into_le_bytes());
    hash.finalize().into()
}

fn unix_timestamp() -> Result<u64, MinerError> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| {
            MinerError::InvalidRequest(format!("system clock precedes Unix epoch: {error}"))
        })?
        .as_secs())
}

fn journal_length_after_append(
    state: &JournalState,
    record_bytes: usize,
) -> Result<u64, MinerError> {
    if state.poisoned {
        return Err(MinerError::InvalidRequest(
            "share journal is poisoned after an append or durability failure; restart the coordinator to recover its unterminated tail"
                .to_string(),
        ));
    }
    if record_bytes > MAX_JOURNAL_RECORD_BYTES {
        return Err(MinerError::InvalidRequest(format!(
            "share journal record exceeds the {MAX_JOURNAL_RECORD_BYTES}-byte safety limit"
        )));
    }
    let record_bytes = u64::try_from(record_bytes).map_err(|_| {
        MinerError::InvalidRequest("share journal record length overflowed".to_string())
    })?;
    let new_length = state
        .bytes_written
        .checked_add(record_bytes)
        .ok_or_else(|| MinerError::InvalidRequest("share journal length overflowed".to_string()))?;
    if new_length > MAX_JOURNAL_BYTES {
        return Err(MinerError::InvalidRequest(format!(
            "share journal reached the {MAX_JOURNAL_BYTES}-byte safety limit; stop and archive it before accepting more work"
        )));
    }
    Ok(new_length)
}

fn append_synced_journal_record(
    state: &mut JournalState,
    path: &Path,
    encoded: &[u8],
) -> Result<(), MinerError> {
    let new_length = journal_length_after_append(state, encoded.len())?;
    if let Err(error) = verify_locked_journal_path(&state.file, path) {
        state.poisoned = true;
        return Err(error);
    }
    if let Err(error) = state
        .file
        .write_all(encoded)
        .and_then(|()| state.file.sync_data())
    {
        state.poisoned = true;
        return Err(MinerError::Io(error));
    }
    if let Err(error) = verify_locked_journal_path(&state.file, path) {
        state.poisoned = true;
        return Err(error);
    }
    state.bytes_written = new_length;
    Ok(())
}

#[cfg(unix)]
fn verify_locked_journal_path(file: &File, path: &Path) -> Result<(), MinerError> {
    use std::os::unix::fs::MetadataExt;

    let locked = file.metadata()?;
    let path_metadata = fs::symlink_metadata(path).map_err(|_| {
        MinerError::InvalidRequest(format!(
            "share journal path {} no longer refers to the locked file; stop the coordinator before rotating it",
            path.display()
        ))
    })?;
    if path_metadata.file_type().is_symlink() || !path_metadata.is_file() {
        return Err(MinerError::InvalidRequest(format!(
            "share journal path {} must remain a regular non-symbolic-link file",
            path.display()
        )));
    }
    let current = fs::metadata(path).map_err(|_| {
        MinerError::InvalidRequest(format!(
            "share journal path {} no longer refers to the locked file; stop the coordinator before rotating it",
            path.display()
        ))
    })?;
    if locked.dev() != current.dev() || locked.ino() != current.ino() {
        return Err(MinerError::InvalidRequest(format!(
            "share journal path {} was replaced while the coordinator was running",
            path.display()
        )));
    }
    Ok(())
}

#[cfg(not(unix))]
fn verify_locked_journal_path(_file: &File, path: &Path) -> Result<(), MinerError> {
    let metadata = fs::symlink_metadata(path).map_err(|_| {
        MinerError::InvalidRequest(format!(
            "share journal path {} no longer refers to the locked file",
            path.display()
        ))
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(MinerError::InvalidRequest(format!(
            "share journal path {} is no longer a regular non-symbolic-link file",
            path.display()
        )));
    }
    Ok(())
}

fn reject_existing_journal_symlink(path: &Path) -> Result<(), MinerError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            Err(MinerError::InvalidRequest(format!(
                "share journal {} must not be a symbolic link",
                path.display()
            )))
        }
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(MinerError::Io(error)),
    }
}

fn journal_mutex_error() -> MinerError {
    MinerError::InvalidRequest(
        "share journal mutex is poisoned after an internal panic; restart the coordinator before accepting more work"
            .to_string(),
    )
}

fn coordinator_mutex_error(component: &str) -> MinerError {
    MinerError::InvalidRequest(format!(
        "{component} mutex is poisoned after an internal panic; restart the coordinator"
    ))
}

#[cfg(unix)]
fn sync_journal_parent_directory(path: &Path) -> Result<(), MinerError> {
    File::open(journal_parent_directory(path))?.sync_all()?;
    Ok(())
}

fn journal_parent_directory(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

#[cfg(not(unix))]
fn sync_journal_parent_directory(_path: &Path) -> Result<(), MinerError> {
    Ok(())
}

fn ensure_pending_winner_capacity(current: usize, additional: usize) -> Result<(), MinerError> {
    let resulting = current
        .checked_add(additional)
        .ok_or_else(|| MinerError::InvalidRequest("pending winner count overflowed".to_string()))?;
    if resulting > MAX_PENDING_WINNERS {
        return Err(MinerError::InvalidRequest(format!(
            "winner outbox reached the {MAX_PENDING_WINNERS}-entry safety limit; stop mining and restore child/parent RPC availability before accepting more winning shares"
        )));
    }
    Ok(())
}

fn complete_wcash_candidate(
    candidate_bytes: &[u8],
    expected_hash_display: &str,
    proof: &[u8],
) -> Result<Vec<u8>, MinerError> {
    wcash_zcash_aux::AuxPowProof::decode(proof)?;
    let mut block: Block = candidate_bytes.zcash_deserialize_into().map_err(|error| {
        MinerError::InvalidParentTemplate(format!("preserved Wcash candidate is invalid: {error}"))
    })?;
    Arc::make_mut(&mut block.header).solution =
        Solution::for_wcash(proof.to_vec()).map_err(MinerError::WcashWitnessEncoding)?;
    let expected_hash = parse_display_hash(expected_hash_display, "Wcash winner block hash")?;
    if block.hash().0 != expected_hash {
        return Err(MinerError::ChildHeaderMismatch);
    }
    let completed = block.zcash_serialize_to_vec()?;
    if completed.len() > MAX_CHILD_BLOCK_BYTES {
        return Err(MinerError::InvalidParentTemplate(format!(
            "completed Wcash block is {} bytes, maximum is {MAX_CHILD_BLOCK_BYTES}",
            completed.len()
        )));
    }
    Ok(completed)
}

fn retire_child_candidate(
    node: &ZebraRpcClient,
    lease: &ChildCandidateLease,
) -> Result<GenerationRetirement, MinerError> {
    parse_display_hash(&lease.hash, "retired Wcash candidate hash")?;
    parse_canonical_capability(&lease.retire_token, "Wcash candidate retire token")?;
    let response: RetireAuxBlockResponse =
        node.call("retireauxblock", json!([lease.hash, lease.retire_token]))?;
    match response {
        RetireAuxBlockResponse::Retired | RetireAuxBlockResponse::AlreadyAbsent => {
            Ok(GenerationRetirement::Retired)
        }
        RetireAuxBlockResponse::SubmissionStarted => {
            Ok(GenerationRetirement::RetainedForSubmission)
        }
    }
}

fn parse_canonical_capability(encoded: &str, field: &'static str) -> Result<[u8; 32], MinerError> {
    let decoded = parse_raw_hash(encoded, field)?;
    if encoded != hex::encode(decoded) {
        return Err(MinerError::InvalidHexField {
            field,
            reason: "expected canonical lowercase hexadecimal".to_string(),
        });
    }
    Ok(decoded)
}

fn parse_job_id(encoded: &str) -> Result<[u8; 32], MinerError> {
    parse_journal_id(encoded, "job")
}

fn insert_seen_job_id(
    seen_job_ids: &mut HashSet<[u8; 32]>,
    job_id: [u8; 32],
) -> Result<(), MinerError> {
    let already_seen = seen_job_ids.contains(&job_id);
    ensure_job_generation_capacity(seen_job_ids.len(), already_seen)?;
    seen_job_ids.insert(job_id);
    Ok(())
}

fn ensure_job_generation_capacity(count: usize, already_seen: bool) -> Result<(), MinerError> {
    if !already_seen && count >= MAX_JOURNAL_GENERATIONS {
        return Err(MinerError::InvalidRequest(format!(
            "share journal contains more than {MAX_JOURNAL_GENERATIONS} mining generations"
        )));
    }
    Ok(())
}

fn parse_share_id(encoded: &str) -> Result<[u8; 32], MinerError> {
    parse_journal_id(encoded, "share")
}

fn parse_canonical_journal_hex(encoded: &str, field: &'static str) -> Result<[u8; 32], MinerError> {
    let decoded = parse_raw_hash(encoded, field)?;
    if encoded != hex::encode(decoded) {
        return Err(MinerError::InvalidRequest(format!(
            "share journal {field} is not canonical lowercase hexadecimal"
        )));
    }
    Ok(decoded)
}

fn parse_journal_id(encoded: &str, kind: &str) -> Result<[u8; 32], MinerError> {
    if encoded.len() != 64 {
        return Err(MinerError::InvalidRequest(format!(
            "share journal contains a non-32-byte {kind} ID"
        )));
    }
    let decoded: [u8; 32] = hex::decode(encoded)
        .map_err(|error| {
            MinerError::InvalidRequest(format!("share journal contains invalid {kind} ID: {error}"))
        })?
        .try_into()
        .map_err(|bytes: Vec<u8>| {
            MinerError::InvalidRequest(format!(
                "share journal {kind} ID decoded to {} bytes, expected 32",
                bytes.len()
            ))
        })?;
    if encoded != hex::encode(decoded) {
        return Err(MinerError::InvalidRequest(format!(
            "share journal {kind} ID is not canonical lowercase hexadecimal"
        )));
    }
    Ok(decoded)
}

fn parse_raw_hash(encoded: &str, field: &'static str) -> Result<[u8; 32], MinerError> {
    if encoded.len() != 64 {
        return Err(MinerError::InvalidHexField {
            field,
            reason: format!("expected 64 hexadecimal characters, got {}", encoded.len()),
        });
    }
    hex::decode(encoded)
        .map_err(|error| MinerError::InvalidHexField {
            field,
            reason: error.to_string(),
        })?
        .try_into()
        .map_err(|bytes: Vec<u8>| MinerError::InvalidHexField {
            field,
            reason: format!("decoded to {} bytes, expected 32", bytes.len()),
        })
}

fn display_hash(mut raw_little_endian: [u8; 32]) -> String {
    raw_little_endian.reverse();
    hex::encode(raw_little_endian)
}

fn display_target(target: Target) -> String {
    display_hash(target.to_le_bytes())
}

fn parse_display_hash(encoded: &str, field: &'static str) -> Result<[u8; 32], MinerError> {
    if encoded.len() != 64 {
        return Err(MinerError::InvalidHexField {
            field,
            reason: format!("expected 64 hexadecimal characters, got {}", encoded.len()),
        });
    }
    let mut bytes: [u8; 32] = hex::decode(encoded)
        .map_err(|error| MinerError::InvalidHexField {
            field,
            reason: error.to_string(),
        })?
        .try_into()
        .map_err(|bytes: Vec<u8>| MinerError::InvalidHexField {
            field,
            reason: format!("decoded to {} bytes, expected 32", bytes.len()),
        })?;
    bytes.reverse();
    Ok(bytes)
}

fn parse_display_target(encoded: &str) -> Result<Target, MinerError> {
    let bytes = parse_display_hash(encoded, "createauxblock target")?;
    Ok(Target::from_le_bytes(bytes)?)
}

fn decode_bounded_hex(
    encoded: &str,
    field: &'static str,
    maximum_bytes: usize,
) -> Result<Vec<u8>, MinerError> {
    let maximum_chars =
        maximum_bytes
            .checked_mul(2)
            .ok_or_else(|| MinerError::InvalidHexField {
                field,
                reason: "configured size bound overflowed".to_string(),
            })?;
    if encoded.len() > maximum_chars || !encoded.len().is_multiple_of(2) {
        return Err(MinerError::InvalidHexField {
            field,
            reason: format!(
                "expected complete bytes totalling at most {maximum_bytes} bytes, got {} hexadecimal characters",
                encoded.len()
            ),
        });
    }
    hex::decode(encoded).map_err(|error| MinerError::InvalidHexField {
        field,
        reason: error.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        io::{ErrorKind, Read},
        net::TcpListener,
    };

    use hex::FromHex;
    use tempfile::tempdir;
    use zcash_address::unified::Encoding;
    use zcash_protocol::consensus::{BranchId, NetworkType};
    use zebra_chain::{
        amount::{Amount, NonNegative},
        block::genesis::wcash_regtest_genesis_block,
        parameters::{NetworkKind, NetworkUpgrade},
        transaction::{LockTime, Transaction},
        transparent,
    };

    use super::*;

    fn mainnet_genesis_block() -> Block {
        Vec::from_hex(include_str!("../../zebra-test/src/vectors/block-main-0-000-000.txt").trim())
            .expect("mainnet genesis fixture is hex")
            .zcash_deserialize_into()
            .expect("mainnet genesis fixture is a block")
    }

    fn mainnet_block_one() -> Block {
        Vec::from_hex(include_str!("../../zebra-test/src/vectors/block-main-0-000-001.txt").trim())
            .expect("mainnet block fixture is hex")
            .zcash_deserialize_into()
            .expect("mainnet block fixture is a block")
    }

    fn read_test_rpc_request(stream: &mut std::net::TcpStream) -> serde_json::Value {
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("set test RPC read timeout");
        let mut request = Vec::new();
        let mut buffer = [0u8; 4_096];
        let (body_start, content_length) = loop {
            let count = stream.read(&mut buffer).expect("read test RPC request");
            assert!(count > 0, "test RPC request ended before its body");
            request.extend_from_slice(&buffer[..count]);
            assert!(request.len() <= 64 * 1_024, "test RPC request is bounded");
            let Some(header_end) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n") else {
                continue;
            };
            let body_start = header_end + 4;
            let headers =
                std::str::from_utf8(&request[..header_end]).expect("test RPC headers are UTF-8");
            let content_length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().expect("valid content length"))
                })
                .expect("test RPC request has a content length");
            if request.len() >= body_start + content_length {
                break (body_start, content_length);
            }
        };
        serde_json::from_slice(&request[body_start..body_start + content_length])
            .expect("test RPC body is JSON")
    }

    fn spawn_wcash_status_server() -> (RpcEndpoint, thread::JoinHandle<Vec<serde_json::Value>>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test RPC server");
        listener
            .set_nonblocking(true)
            .expect("make test RPC listener nonblocking");
        let endpoint = RpcEndpoint::new(
            format!(
                "http://{}/",
                listener.local_addr().expect("test RPC address")
            ),
            None,
            None,
        )
        .expect("loopback test RPC endpoint");
        let server = thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(5);
            let mut requests = Vec::new();
            while requests.len() < 2 && Instant::now() < deadline {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let request = read_test_rpc_request(&mut stream);
                        let id = request["id"].clone();
                        let result = if requests.is_empty() {
                            json!({"state": "best_chain", "confirmations": 1})
                        } else {
                            json!(true)
                        };
                        let response = serde_json::to_vec(&json!({
                            "jsonrpc": "2.0",
                            "id": id,
                            "result": result,
                        }))
                        .expect("serialize test RPC response");
                        write!(
                            stream,
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                            response.len()
                        )
                        .expect("write test RPC headers");
                        stream
                            .write_all(&response)
                            .expect("write test RPC response");
                        requests.push(request);
                    }
                    Err(error) if error.kind() == ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(error) => panic!("test RPC accept failed: {error}"),
                }
            }
            requests
        });
        (endpoint, server)
    }

    fn spawn_retirement_server(
        results: Vec<serde_json::Value>,
    ) -> (RpcEndpoint, thread::JoinHandle<Vec<serde_json::Value>>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test RPC server");
        listener
            .set_nonblocking(true)
            .expect("make test RPC listener nonblocking");
        let endpoint = RpcEndpoint::new(
            format!(
                "http://{}/",
                listener.local_addr().expect("test RPC address")
            ),
            None,
            None,
        )
        .expect("loopback test RPC endpoint");
        let server = thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(10);
            let mut requests = Vec::new();
            for result in results {
                let mut stream = loop {
                    match listener.accept() {
                        Ok((stream, _)) => break stream,
                        Err(error) if error.kind() == ErrorKind::WouldBlock => {
                            assert!(
                                Instant::now() < deadline,
                                "test retirement RPC request timed out"
                            );
                            thread::sleep(Duration::from_millis(5));
                        }
                        Err(error) => panic!("test RPC accept failed: {error}"),
                    }
                };
                let request = read_test_rpc_request(&mut stream);
                let response = serde_json::to_vec(&json!({
                    "jsonrpc": "2.0",
                    "id": request["id"],
                    "result": result,
                }))
                .expect("serialize test RPC response");
                write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    response.len()
                )
                .expect("write test RPC headers");
                stream
                    .write_all(&response)
                    .expect("write test RPC response");
                requests.push(request);
            }
            requests
        });
        (endpoint, server)
    }

    fn journal_share_id(job_id: &str, parent_hash_le: [u8; 32]) -> String {
        let mut hash = Sha256::new();
        hash.update(b"Wcash/share-journal/v2\0");
        hash.update(
            u64::try_from(job_id.len())
                .expect("fixture job length fits u64")
                .to_le_bytes(),
        );
        hash.update(job_id.as_bytes());
        hash.update(parent_hash_le);
        hex::encode(<[u8; 32]>::from(hash.finalize()))
    }

    fn accepted_share_fixture(
        job_id: &str,
        parent_hash_le: [u8; 32],
        worker: &str,
        authentication: Option<&str>,
    ) -> serde_json::Value {
        let mut record = json!({
            "version": JOURNAL_VERSION,
            "record": "accepted_share",
            "accepted_at": 1,
            "job_id": job_id,
            "share_id": journal_share_id(job_id, parent_hash_le),
            "worker": worker,
            "parent_hash_le": hex::encode(parent_hash_le),
            "share_target": "ff".repeat(32),
            "wcash_candidate": false,
            "zcash_candidate": false,
            "wcash_block_hash": "42".repeat(32),
            "wcash_height": 1,
            "zcash_height": 1,
        });
        if let Some(authentication) = authentication {
            record["worker_authentication"] = json!(authentication);
        }
        record
    }

    fn active_job_fixture(byte: u8) -> ActiveJournalJob {
        ActiveJournalJob {
            job_id: format!("{byte:02x}").repeat(32),
            child_hash_display: format!("{:02x}", byte.wrapping_add(1)).repeat(32),
            child_height: 1,
            parent_height: 1,
            child_candidate_bytes: Arc::from([]),
        }
    }

    #[test]
    fn display_hash_and_target_are_reversed_exactly_once() {
        let display = (0u8..32)
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let raw = parse_display_hash(&display, "hash").expect("valid display hash");
        assert_eq!(raw, std::array::from_fn(|index| 31 - index as u8));
        assert!(parse_display_target(&"00".repeat(32)).is_err());
        assert!(parse_display_hash("00", "hash").is_err());
    }

    #[test]
    fn candidate_retirement_is_authenticated_idempotent_and_submission_safe() {
        let results = vec![
            json!({"state": "retired"}),
            json!({"state": "already_absent"}),
            json!({"state": "submission_started"}),
        ];
        let (endpoint, server) = spawn_retirement_server(results);
        let client = ZebraRpcClient::new(endpoint, Duration::from_secs(3))
            .expect("construct test RPC client");
        let lease = ChildCandidateLease {
            hash: "11".repeat(32),
            retire_token: "22".repeat(32),
        };
        let debug = format!("{lease:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains(&"22".repeat(32)));

        assert_eq!(
            retire_child_candidate(&client, &lease).expect("first retirement succeeds"),
            GenerationRetirement::Retired
        );
        assert_eq!(
            retire_child_candidate(&client, &lease).expect("retirement retry is idempotent"),
            GenerationRetirement::Retired
        );
        assert_eq!(
            retire_child_candidate(&client, &lease)
                .expect("a started submission is preserved without an RPC error"),
            GenerationRetirement::RetainedForSubmission
        );

        let requests = server.join().expect("test RPC server exits");
        assert_eq!(requests.len(), 3);
        for request in requests {
            assert_eq!(request["method"], "retireauxblock");
            assert_eq!(request["params"], json!([lease.hash, lease.retire_token]));
        }
    }

    #[test]
    fn confirmed_wcash_winner_releases_the_exact_active_candidate() {
        let (endpoint, server) = spawn_wcash_status_server();
        let client = ZebraRpcClient::new(endpoint, Duration::from_secs(3))
            .expect("construct test RPC client");
        let mut block = wcash_regtest_genesis_block().as_ref().clone();
        let witness = vec![0x51, 0x52, 0x53];
        Arc::make_mut(&mut block.header).solution =
            Solution::for_wcash(witness.clone()).expect("test witness is bounded");
        let block_bytes = block
            .zcash_serialize_to_vec()
            .expect("test Wcash block serializes");
        let block_hash = display_hash(block.hash().0);

        assert_eq!(
            wcash_confirmation_depth(&client, 0, &block_hash, &block_bytes),
            WinnerObservation::Present { confirmations: 1 }
        );

        let requests = server.join().expect("test RPC server exits");
        assert_eq!(
            requests.len(),
            2,
            "status must be followed by cache release"
        );
        assert_eq!(requests[0]["method"], "getauxblockstatus");
        assert_eq!(requests[1]["method"], "submitauxblock");
        let expected_params = json!([block_hash, hex::encode(witness)]);
        assert_eq!(requests[0]["params"], expected_params);
        assert_eq!(requests[1]["params"], expected_params);
    }

    fn orchard_unified_wcash_address(network: NetworkType) -> WcashAddress {
        let zcash_fixture = match network {
            NetworkType::Test => "utest10zg6frxk32ma8980kdv9473e4aclw7clq9hydzcj6l349pkqzxk2mmj3cn7j5x38w6l4wyryv50whnlrw0k9agzpdf5fxyj7kq96ukcp",
            NetworkType::Regtest => "uregtest1pszqlgxaf5w8mu2yd9uygg8cswp0ec4f7eejqnqc35tztw4tk0sxnt3pym2f3s2872cy2ruuc5n8y9cen5q6ngzlmzu8ztrjesv8zm9j",
            NetworkType::Main => unreachable!("Wcash mainnet is disabled"),
        };
        zcash_fixture
            .parse::<zcash_address::ZcashAddress>()
            .expect("the upstream Unified fixture is valid")
            .convert::<WcashAddress>()
            .expect("the Unified fixture is supported by Wcash")
            .with_network(network)
    }

    fn assert_invalid_wcash_payout(encoded: &str, genesis: &str, expected: &str) {
        match validate_wcash_payout_configuration(encoded, genesis) {
            Err(MinerError::InvalidRequest(message)) => assert!(
                message.contains(expected),
                "expected {expected:?} in rejection: {message}"
            ),
            Err(error) => panic!("unexpected payout rejection: {error}"),
            Ok(_) => panic!("unsupported payout was accepted: {encoded}"),
        }
    }

    #[test]
    fn wcash_payout_configuration_accepts_public_and_private_test_network_modes() {
        for (network, network_type) in [
            (Network::new_wcash_testnet(), NetworkType::Test),
            (Network::new_wcash_regtest(), NetworkType::Regtest),
        ] {
            let genesis = network.genesis_hash().to_string();
            for payout in [
                WcashAddress::from_transparent_p2pkh(network_type, [0; 20]),
                WcashAddress::from_transparent_p2sh(network_type, [1; 20]),
                orchard_unified_wcash_address(network_type),
            ] {
                let encoded = payout.encode();
                let (selected_network, selected_payout) =
                    validate_wcash_payout_configuration(&encoded, &genesis)
                        .expect("the matching Wcash payout is accepted");
                assert_eq!(selected_network, network);
                assert_eq!(selected_payout, payout);
            }
        }
    }

    #[test]
    fn wcash_payout_configuration_rejects_wrong_network_and_unsupported_types() {
        let testnet = Network::new_wcash_testnet();
        let regtest = Network::new_wcash_regtest();
        let testnet_genesis = testnet.genesis_hash().to_string();
        let regtest_genesis = regtest.genesis_hash().to_string();
        let testnet_transparent =
            WcashAddress::from_transparent_p2pkh(NetworkType::Test, [0; 20]).encode();

        assert_invalid_wcash_payout(
            &testnet_transparent,
            &regtest_genesis,
            "network does not match",
        );
        assert_invalid_wcash_payout(
            "tmJymvcUCn1ctbghvTJpXBwHiMEB8P6wxNV",
            &testnet_genesis,
            "invalid Wcash payout address",
        );

        let tex = WcashAddress::from_tex(NetworkType::Test, [0; 20]);
        assert_invalid_wcash_payout(
            &tex.encode(),
            &testnet_genesis,
            "transparent or Unified with an Orchard receiver",
        );

        assert_invalid_wcash_payout(
            "wtestsapling1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqwrl5x9",
            &testnet_genesis,
            "invalid Wcash payout address",
        );

        let sapling_only =
            zcash_address::unified::Address::try_from_items(vec![Receiver::Sapling([0; 43])])
                .expect("the Sapling-only Unified fixture is structurally valid");
        assert!(WcashAddress::from_unified(NetworkType::Test, sapling_only).is_err());

        let mainnet = WcashAddress::from_transparent_p2pkh(NetworkType::Main, [0; 20]);
        assert_invalid_wcash_payout(
            &mainnet.encode(),
            &Network::Mainnet.genesis_hash().to_string(),
            "mainnet payouts are disabled",
        );
    }

    fn wcash_coinbase_inputs() -> Vec<transparent::Input> {
        vec![transparent::Input::Coinbase {
            height: Height(1),
            data: vec![0x51],
            sequence: u32::MAX,
        }]
    }

    fn transparent_wcash_candidate(payout_hash: [u8; 20], reward: i64) -> (Block, WcashAddress) {
        let network = Network::new_wcash_regtest();
        let output = transparent::Output::new(
            Amount::<NonNegative>::try_from(reward).expect("fixture reward is non-negative"),
            transparent::Address::from_pub_key_hash(NetworkKind::Mainnet, payout_hash).script(),
        );
        let coinbase = Transaction::test_v6_for_network(
            &network,
            Height(1),
            wcash_coinbase_inputs(),
            vec![output],
            LockTime::unlocked(),
            Height(0),
        );
        let mut candidate = Arc::unwrap_or_clone(wcash_regtest_genesis_block());
        candidate.transactions = vec![Arc::new(coinbase)];
        (
            candidate,
            WcashAddress::from_transparent_p2pkh(NetworkType::Regtest, payout_hash),
        )
    }

    fn assert_invalid_candidate_payout(result: Result<(), MinerError>, expected_message: &str) {
        match result {
            Err(MinerError::InvalidParentTemplate(message)) => assert!(
                message.contains(expected_message),
                "expected {expected_message:?} in rejection: {message}"
            ),
            Err(error) => panic!("unexpected candidate-payout rejection: {error}"),
            Ok(()) => panic!("invalid candidate payout was accepted"),
        }
    }

    #[test]
    fn wcash_candidate_requires_exact_transparent_recipient_and_value() {
        let network = Network::new_wcash_regtest();
        let reward = 625_012_345;
        let (candidate, payout) = transparent_wcash_candidate([0x51; 20], reward);

        validate_wcash_candidate_payout(&candidate, &payout, &network, reward, 1)
            .expect("the exact transparent recipient and value are accepted");

        let wrong_payout = WcashAddress::from_transparent_p2pkh(NetworkType::Regtest, [0x52; 20]);
        assert_invalid_candidate_payout(
            validate_wcash_candidate_payout(&candidate, &wrong_payout, &network, reward, 1),
            "only the configured transparent Wcash address",
        );
        assert_invalid_candidate_payout(
            validate_wcash_candidate_payout(&candidate, &payout, &network, reward + 1, 1),
            "transparent outputs do not match coinbasevalue",
        );
    }

    #[test]
    fn wcash_transparent_candidate_rejects_a_mixed_shielded_pool() {
        use zebra_chain::transaction::arbitrary::fake_bundle_for_branch;

        let network = Network::new_wcash_regtest();
        let reward = 625_012_345;
        let payout_hash = [0x61; 20];
        let payout = WcashAddress::from_transparent_p2pkh(NetworkType::Regtest, payout_hash);
        let output = transparent::Output::new(
            Amount::<NonNegative>::try_from(reward).expect("fixture reward is non-negative"),
            transparent::Address::from_pub_key_hash(NetworkKind::Mainnet, payout_hash).script(),
        );
        let ironwood_bundle =
            fake_bundle_for_branch(BranchId::Nu6_3, orchard::ValuePool::Ironwood, 1, 0x5eed)
                .expect("NU6.3 defines the Ironwood pool");
        let coinbase = Transaction::test_v6_with_bundles(
            NetworkUpgrade::Nu6_3,
            wcash_coinbase_inputs(),
            vec![output],
            LockTime::unlocked(),
            Height(0),
            None,
            Some(ironwood_bundle),
        );
        let mut candidate = Arc::unwrap_or_clone(wcash_regtest_genesis_block());
        candidate.transactions = vec![Arc::new(coinbase)];

        assert_invalid_candidate_payout(
            validate_wcash_candidate_payout(&candidate, &payout, &network, reward, 1),
            "only the configured transparent Wcash address",
        );
    }

    #[test]
    fn coordinator_debug_never_exposes_the_private_payout_receiver() {
        let zcash = NativeZcashConfig::new(
            RpcEndpoint::new("http://127.0.0.1:18232", None, None)
                .expect("valid template endpoint"),
            vec![RpcEndpoint::new("http://127.0.0.1:18242", None, None)
                .expect("valid validator endpoint")],
            "11".repeat(32),
            "tmJymvcUCn1ctbghvTJpXBwHiMEB8P6wxNV"
                .parse()
                .expect("valid Zcash testnet address"),
        )
        .expect("valid parent configuration");
        let secret = "private-wcash-unified-address-fixture";
        let config = CoordinatorConfig {
            wcash_node: RpcEndpoint::new("http://127.0.0.1:28232", None, None)
                .expect("valid child endpoint"),
            expected_wcash_genesis_hash: "22".repeat(32),
            zcash,
            wcash_payout_address: secret.to_string(),
            auxiliary_nonce: 0,
        };
        let debug = format!("{config:?}");
        assert!(!debug.contains(secret));
        assert!(debug.contains("[REDACTED]"));
    }

    #[test]
    fn child_template_accepts_exact_createauxblock_shape() {
        let response = json!({
            "hash": "11".repeat(32),
            "retiretoken": "33".repeat(32),
            "data": "00",
            "chainid": WCASH_AUXILIARY_CHAIN_ID,
            "previousblockhash": "22".repeat(32),
            "coinbasevalue": 1_000_000_000i64,
            "target": "ff".repeat(32),
            "bits": "207fffff",
            "height": 1,
        });
        let parsed: ChildTemplate =
            serde_json::from_value(response).expect("exact RPC response decodes");
        assert_eq!(parsed.chain_id, WCASH_AUXILIARY_CHAIN_ID);
        assert_eq!(parsed.coinbase_value, 1_000_000_000);
        assert_eq!(parsed.retire_token, "33".repeat(32));
    }

    #[test]
    fn persisted_share_ids_are_strictly_decoded() {
        assert_eq!(parse_share_id(&"07".repeat(32)).expect("valid ID"), [7; 32]);
        assert!(parse_share_id("07").is_err());
        assert!(parse_share_id(&"zz".repeat(32)).is_err());
        assert!(parse_job_id(&"AB".repeat(32)).is_err());
        assert!(ensure_job_generation_capacity(MAX_JOURNAL_GENERATIONS - 1, false).is_ok());
        assert!(ensure_job_generation_capacity(MAX_JOURNAL_GENERATIONS, true).is_ok());
        assert!(ensure_job_generation_capacity(MAX_JOURNAL_GENERATIONS, false).is_err());
    }

    #[test]
    fn job_activation_is_durable_and_reuse_fails_after_restart() {
        let directory = tempdir().expect("temporary directory");
        let path = directory.path().join("shares.jsonl");
        let first = active_job_fixture(0x11);
        let journal = ShareJournal::open(&path).expect("new private journal");
        journal
            .activate_job(first.clone())
            .expect("first activation is durable");
        let records = fs::read_to_string(&path).expect("read activation journal");
        let activation: serde_json::Value =
            serde_json::from_str(records.trim()).expect("activation is JSON");
        assert_eq!(activation["record"], "job_activated");
        assert_eq!(activation["job_id"], first.job_id);
        assert!(activation.get("share_id").is_none());
        drop(journal);

        let recovered = ShareJournal::open(&path).expect("recover activation");
        assert!(recovered.activate_job(first).is_err());
    }

    #[test]
    fn rotation_rejects_late_shares_and_a_b_a_job_reuse() {
        let directory = tempdir().expect("temporary directory");
        let path = directory.path().join("shares.jsonl");
        let journal = ShareJournal::open(&path).expect("new private journal");
        let first = active_job_fixture(0x21);
        let second = active_job_fixture(0x22);
        journal
            .activate_job(first.clone())
            .expect("activate generation A");
        journal
            .activate_job(second.clone())
            .expect("activate generation B");
        let state = journal.state.lock().expect("journal state");
        assert!(active_job_for_share(&state, &first.job_id).is_err());
        assert!(active_job_for_share(&state, &second.job_id).is_ok());
        drop(state);
        assert!(journal.activate_job(first).is_err());
        assert_eq!(
            fs::read_to_string(&path)
                .expect("read journal")
                .lines()
                .count(),
            2,
            "failed A-B-A reuse must not append a third activation"
        );
    }

    #[test]
    fn legacy_share_job_id_is_permanently_reserved() {
        let directory = tempdir().expect("temporary directory");
        let path = directory.path().join("shares.jsonl");
        let job = active_job_fixture(0x31);
        let parent_hash_le = [0x41; 32];
        write_journal_records(
            &path,
            &[accepted_share_fixture(
                &job.job_id,
                parent_hash_le,
                "legacy worker",
                None,
            )],
        );
        let journal = ShareJournal::open(&path).expect("legacy journal recovers");
        assert!(journal.activate_job(job).is_err());
        drop(journal);
        let snapshot = crate::accounting::read_accounting_snapshot(&path)
            .expect("the same complete legacy record remains accountable");
        assert_eq!(snapshot.shares_by_authentication["legacy_unknown"], 1);
        assert_eq!(snapshot.workers["legacy worker"].accepted_shares, 1);
    }

    #[test]
    fn online_replay_rejects_every_accounting_invalid_share_shape() {
        let directory = tempdir().expect("temporary directory");
        let base = accepted_share_fixture(
            &"43".repeat(32),
            [0x44; 32],
            "account.rig-01",
            Some("exact_credential"),
        );
        let malformed = vec![
            {
                let mut record = base.clone();
                record.as_object_mut().unwrap().remove("accepted_at");
                record
            },
            {
                let mut record = base.clone();
                record["accepted_at"] = json!(0);
                record
            },
            {
                let mut record = base.clone();
                record.as_object_mut().unwrap().remove("worker");
                record
            },
            {
                let mut record = base.clone();
                record["worker_authentication"] = json!("legacy_unknown");
                record
            },
            {
                let mut record = base.clone();
                record.as_object_mut().unwrap().remove("parent_hash_le");
                record
            },
            {
                let mut record = base.clone();
                record["share_target"] = json!("00".repeat(32));
                record
            },
            {
                let mut record = base.clone();
                record["share_target"] = json!("FF".repeat(32));
                record
            },
            {
                let mut record = base.clone();
                record["share_id"] = json!("45".repeat(32));
                record
            },
            {
                let mut record = base.clone();
                record.as_object_mut().unwrap().remove("wcash_candidate");
                record
            },
            {
                let mut record = base.clone();
                record["wcash_candidate"] = json!(true);
                record
            },
            {
                let mut record = base.clone();
                record.as_object_mut().unwrap().remove("wcash_block_hash");
                record
            },
            {
                let mut record = base.clone();
                record["zcash_height"] = json!(0);
                record
            },
            {
                let mut record = base.clone();
                record["chain"] = json!("zcash");
                record
            },
        ];
        for (index, record) in malformed.into_iter().enumerate() {
            let path = directory
                .path()
                .join(format!("malformed-share-{index}.jsonl"));
            write_journal_records(&path, &[record]);
            assert!(
                ShareJournal::open(&path).is_err(),
                "malformed share fixture {index} must fail online replay"
            );
        }

        let valid_path = directory.path().join("valid-share.jsonl");
        write_journal_records(&valid_path, &[base]);
        assert!(ShareJournal::open(&valid_path).is_ok());
    }

    #[test]
    fn malformed_or_duplicate_activation_fails_closed() {
        let directory = tempdir().expect("temporary directory");
        let malformed_path = directory.path().join("malformed.jsonl");
        let duplicate_path = directory.path().join("duplicate.jsonl");
        let activation = json!({
            "version": JOURNAL_VERSION,
            "record": "job_activated",
            "recorded_at": 1,
            "job_id": "51".repeat(32),
            "wcash_block_hash": "61".repeat(32),
            "wcash_height": 1,
            "zcash_height": 1,
        });
        let mut malformed = activation.clone();
        malformed["share_id"] = json!("71".repeat(32));
        write_journal_records(&malformed_path, &[malformed]);
        assert!(ShareJournal::open(&malformed_path).is_err());
        write_journal_records(&duplicate_path, &[activation.clone(), activation]);
        assert!(ShareJournal::open(&duplicate_path).is_err());
    }

    #[test]
    fn duplicate_winner_share_id_requires_an_exact_record() {
        let directory = tempdir().expect("temporary directory");
        let path = directory.path().join("winner.jsonl");
        let block = mainnet_block_one();
        let raw_hash = block.hash().0;
        let job_id = "81".repeat(32);
        let outbox = json!({
            "version": JOURNAL_VERSION,
            "record": "winner_outbox",
            "accepted_at": 1,
            "job_id": job_id,
            "share_id": journal_share_id(&"81".repeat(32), raw_hash),
            "worker": "account.rig-01",
            "worker_authentication": "exact_credential",
            "parent_hash_le": hex::encode(raw_hash),
            "share_target": "ff".repeat(32),
            "wcash_candidate": false,
            "zcash_candidate": true,
            "wcash_block_hash": "82".repeat(32),
            "wcash_height": 1,
            "zcash_height": 1,
            "zcash_block": hex::encode(
                block.zcash_serialize_to_vec().expect("fixture serializes")
            ),
        });
        write_journal_records(&path, &[outbox.clone(), outbox.clone()]);
        assert!(ShareJournal::open(&path).is_ok());

        let mut conflict = outbox.clone();
        conflict["accepted_at"] = json!(2);
        write_journal_records(&path, &[outbox, conflict]);
        assert!(ShareJournal::open(&path).is_err());
    }

    #[test]
    fn pending_winner_capacity_reserves_dual_winners_atomically() {
        assert!(ensure_pending_winner_capacity(MAX_PENDING_WINNERS - 1, 1).is_ok());
        assert!(ensure_pending_winner_capacity(MAX_PENDING_WINNERS - 1, 2).is_err());
        assert!(ensure_pending_winner_capacity(MAX_PENDING_WINNERS, 1).is_err());
        assert!(ensure_pending_winner_capacity(usize::MAX, 1).is_err());
    }

    #[test]
    fn persisted_wcash_winner_requires_consensus_valid_auxpow() {
        let mut malformed = wcash_regtest_genesis_block().as_ref().clone();
        Arc::make_mut(&mut malformed.header).solution =
            Solution::for_wcash([1, 2, 3]).expect("small malformed witness serializes");
        let malformed_bytes = malformed
            .zcash_serialize_to_vec()
            .expect("malformed Wcash block serializes canonically");
        assert!(
            validate_persisted_winner_block(
                &malformed_bytes,
                &display_hash(malformed.hash().0),
                0,
                PersistedWinnerBlockKind::Wcash,
            )
            .is_err(),
            "a nonempty but invalid AuxPoW witness must not survive journal recovery"
        );
    }

    #[test]
    fn persisted_zcash_winner_requires_target_and_equihash() {
        let valid = mainnet_genesis_block();
        let valid_bytes = valid
            .zcash_serialize_to_vec()
            .expect("mainnet genesis serializes");
        assert_eq!(
            validate_persisted_winner_block(
                &valid_bytes,
                &display_hash(valid.hash().0),
                0,
                PersistedWinnerBlockKind::Zcash,
            )
            .expect("mainnet genesis has valid target and Equihash"),
            valid.hash().0,
        );

        let mut invalid_target_bytes = valid_bytes.clone();
        invalid_target_bytes[104..108].copy_from_slice(&0u32.to_le_bytes());
        let invalid_target: Block = invalid_target_bytes
            .zcash_deserialize_into()
            .expect("zero compact target remains structurally decodable");
        assert!(validate_persisted_winner_block(
            &invalid_target_bytes,
            &display_hash(invalid_target.hash().0),
            0,
            PersistedWinnerBlockKind::Zcash,
        )
        .is_err());

        let mut invalid_equihash = valid;
        Arc::make_mut(&mut invalid_equihash.header).difficulty_threshold =
            CompactDifficulty::from_bytes_in_display_order(&[0x20, 0x7f, 0xff, 0xff])
                .expect("maximum positive compact target");
        let expanded: U256 = invalid_equihash
            .header
            .difficulty_threshold
            .to_expanded()
            .expect("valid compact target")
            .into();
        let target =
            Target::from_le_bytes(expanded.to_little_endian()).expect("valid expanded target");
        *Arc::make_mut(&mut invalid_equihash.header).nonce = [0; 32];
        assert!(target.is_met_by_le_hash(invalid_equihash.hash().0));
        let invalid_equihash_bytes = invalid_equihash
            .zcash_serialize_to_vec()
            .expect("tampered block serializes");
        let error = validate_persisted_winner_block(
            &invalid_equihash_bytes,
            &display_hash(invalid_equihash.hash().0),
            0,
            PersistedWinnerBlockKind::Zcash,
        )
        .expect_err("the valid target must not mask invalid Equihash");
        assert!(error.to_string().contains("invalid Equihash"));
    }

    #[test]
    fn wcash_confirmation_requires_the_exact_auxpow_witness() {
        let exact = wcash_regtest_genesis_block().as_ref().clone();
        let exact_bytes = exact
            .zcash_serialize_to_vec()
            .expect("Wcash block serializes");
        let expected_hash = display_hash(exact.hash().0);
        assert_eq!(
            classify_wcash_best_chain_block(&exact_bytes, &exact_bytes, &expected_hash, 0)
                .expect("exact block classifies"),
            WcashBestChainMatch::Exact,
        );

        let mut conflicting_witness = exact.clone();
        Arc::make_mut(&mut conflicting_witness.header).solution =
            Solution::for_wcash([1, 2, 3]).expect("small witness serializes");
        assert_eq!(conflicting_witness.hash(), exact.hash());
        let conflicting_bytes = conflicting_witness
            .zcash_serialize_to_vec()
            .expect("conflicting Wcash block serializes");
        assert_eq!(
            classify_wcash_best_chain_block(&conflicting_bytes, &exact_bytes, &expected_hash, 0,)
                .expect("conflicting witness classifies"),
            WcashBestChainMatch::ConflictingWitness,
        );

        let mut different_block = exact;
        Arc::make_mut(&mut different_block.header).nonce[0] ^= 1;
        let different_bytes = different_block
            .zcash_serialize_to_vec()
            .expect("different Wcash block serializes");
        assert_eq!(
            classify_wcash_best_chain_block(&different_bytes, &exact_bytes, &expected_hash, 0)
                .expect("different block classifies"),
            WcashBestChainMatch::DifferentBlock,
        );
    }

    #[test]
    fn journal_lock_and_torn_tail_recovery_are_fail_closed() {
        let directory = tempdir().expect("temporary directory");
        let path = directory.path().join("shares.jsonl");
        let journal = ShareJournal::open(&path).expect("new private journal");
        assert!(
            ShareJournal::open(&path).is_err(),
            "a second writer must not acquire the journal"
        );
        drop(journal);

        fs::write(&path, br#"{"version":2,"record":"accepted_share""#).expect("write torn record");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
                .expect("make fixture private");
        }
        let recovered = ShareJournal::open(&path).expect("torn tail is recoverable");
        assert_eq!(
            fs::metadata(&path).expect("journal metadata").len(),
            0,
            "unterminated tail must be truncated before future appends"
        );
        drop(recovered);
    }

    #[test]
    fn poisoned_journal_rejects_every_future_append() {
        let directory = tempdir().expect("temporary directory");
        let path = directory.path().join("shares.jsonl");
        let journal = ShareJournal::open(&path).expect("new private journal");
        let mut state = journal
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.poisoned = true;
        let original_length = state.bytes_written;
        assert!(append_synced_journal_record(&mut state, &path, b"{}\n").is_err());
        assert_eq!(state.bytes_written, original_length);
        assert_eq!(fs::metadata(&path).expect("journal metadata").len(), 0);
    }

    #[cfg(unix)]
    #[test]
    fn journal_rejects_writable_parent_and_live_path_replacement() {
        use std::os::unix::fs::PermissionsExt;

        let writable_directory = tempdir().expect("temporary directory");
        fs::set_permissions(writable_directory.path(), fs::Permissions::from_mode(0o770))
            .expect("make parent group-writable");
        let unsafe_path = writable_directory.path().join("unsafe.jsonl");
        assert!(
            ShareJournal::open(&unsafe_path).is_err(),
            "a group-writable parent must be rejected"
        );

        let directory = tempdir().expect("temporary directory");
        let path = directory.path().join("shares.jsonl");
        let displaced_path = directory.path().join("displaced.jsonl");
        let journal = ShareJournal::open(&path).expect("new private journal");
        fs::rename(&path, &displaced_path).expect("move locked journal inode");
        fs::write(&path, []).expect("replace journal pathname");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
            .expect("make replacement private");

        let mut state = journal
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert!(
            append_synced_journal_record(&mut state, &path, b"{}\n").is_err(),
            "an append must never ACK after the locked pathname is replaced"
        );
        assert!(state.poisoned);
        assert_eq!(
            fs::metadata(&path).expect("replacement metadata").len(),
            0,
            "the replacement file must not receive an ACKed record"
        );
    }

    #[cfg(unix)]
    #[test]
    fn journal_rejects_symbolic_links_and_non_regular_files() {
        use std::os::unix::{fs::symlink, fs::PermissionsExt};

        let directory = tempdir().expect("temporary directory");
        let target = directory.path().join("target.jsonl");
        let link = directory.path().join("shares.jsonl");
        fs::write(&target, []).expect("create journal target");
        fs::set_permissions(&target, fs::Permissions::from_mode(0o600))
            .expect("make target private");
        symlink(&target, &link).expect("create journal symlink");
        assert!(
            ShareJournal::open(&link).is_err(),
            "online replay must reject a symbolic-link journal just like accounting"
        );

        let non_regular = directory.path().join("journal-directory");
        fs::create_dir(&non_regular).expect("create non-regular journal path");
        assert!(ShareJournal::open(&non_regular).is_err());
    }

    #[test]
    fn mutex_poison_prevents_every_future_journal_write() {
        let directory = tempdir().expect("temporary directory");
        let path = directory.path().join("shares.jsonl");
        let journal = ShareJournal::open(&path).expect("new private journal");

        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _state = journal.state.lock().expect("unpoisoned journal mutex");
            panic!("simulate an internal panic while mutating journal state");
        }));
        assert!(panic.is_err());

        let winner = PendingWinner {
            key: WinnerKey {
                share_id: [0x2a; 32],
                chain: WinnerChain::Zcash,
            },
            job_id: "poison-test".to_string(),
            block_hash_display: "00".repeat(32),
            height: 1,
            block_bytes: Vec::new(),
            observed_on_best_chain: false,
        };
        assert!(journal
            .mark_status(&winner, WinnerStatus::Observed)
            .is_err());
        assert!(journal.status().is_err());
        assert_eq!(
            fs::metadata(&path).expect("journal metadata").len(),
            0,
            "mutex poison must never be recovered into a later append"
        );
    }

    #[test]
    fn journal_status_transitions_require_exact_winner_metadata() {
        let directory = tempdir().expect("temporary directory");
        let valid_path = directory.path().join("valid.jsonl");
        let invalid_path = directory.path().join("invalid.jsonl");
        let block = mainnet_block_one();
        let block_bytes = block
            .zcash_serialize_to_vec()
            .expect("genesis block serializes");
        let raw_hash = block.hash().0;
        let display = display_hash(raw_hash);
        let job_id = "91".repeat(32);
        let share_id = journal_share_id(&job_id, raw_hash);
        let outbox = json!({
            "version": JOURNAL_VERSION,
            "record": "winner_outbox",
            "accepted_at": 1,
            "job_id": job_id,
            "share_id": share_id,
            "worker": "account.rig-01",
            "worker_authentication": "exact_credential",
            "parent_hash_le": hex::encode(raw_hash),
            "share_target": "ff".repeat(32),
            "wcash_candidate": false,
            "zcash_candidate": true,
            "wcash_block_hash": "92".repeat(32),
            "wcash_height": 1,
            "zcash_height": 1,
            "zcash_block": hex::encode(block_bytes),
        });
        let observed = json!({
            "version": JOURNAL_VERSION,
            "record": "winner_observed",
            "recorded_at": 2,
            "job_id": "91".repeat(32),
            "share_id": share_id,
            "chain": "zcash",
            "block_hash": display,
            "height": 1,
        });
        let matured = json!({
            "version": JOURNAL_VERSION,
            "record": "winner_matured",
            "recorded_at": 3,
            "job_id": "91".repeat(32),
            "share_id": share_id,
            "chain": "zcash",
            "block_hash": display_hash(raw_hash),
            "height": 1,
        });
        write_journal_records(&valid_path, &[outbox.clone(), observed.clone(), matured]);
        let valid = ShareJournal::open(&valid_path).expect("exact transitions load");
        assert_eq!(valid.status().expect("journal status").pending_zcash, 0);
        drop(valid);

        let forged = json!({
            "version": JOURNAL_VERSION,
            "record": "winner_observed",
            "recorded_at": 2,
            "job_id": "91".repeat(32),
            "share_id": share_id,
            "chain": "zcash",
            "block_hash": "ff".repeat(32),
            "height": 1,
        });
        write_journal_records(&invalid_path, &[outbox.clone(), forged]);
        assert!(
            ShareJournal::open(&invalid_path).is_err(),
            "a status line cannot discard or mutate a different winner"
        );

        let malformed_statuses = vec![
            {
                let mut status = observed.clone();
                status.as_object_mut().unwrap().remove("recorded_at");
                status
            },
            {
                let mut status = observed.clone();
                status["recorded_at"] = json!(0);
                status
            },
            {
                let mut status = observed.clone();
                status["accepted_at"] = json!(1);
                status
            },
            {
                let mut status = observed.clone();
                status["worker"] = json!("account.rig-01");
                status
            },
            {
                let mut status = observed.clone();
                status["block_hash"] = json!(display.to_ascii_uppercase());
                status
            },
            {
                let mut status = observed.clone();
                status["height"] = json!(0);
                status
            },
            {
                let mut status = observed.clone();
                status["wcash_candidate"] = json!(false);
                status
            },
        ];
        for (index, status) in malformed_statuses.into_iter().enumerate() {
            let path = directory
                .path()
                .join(format!("malformed-status-{index}.jsonl"));
            write_journal_records(&path, &[outbox.clone(), status]);
            assert!(
                ShareJournal::open(&path).is_err(),
                "malformed status fixture {index} must fail online replay"
            );
        }
    }

    fn write_journal_records(path: &Path, records: &[serde_json::Value]) {
        let mut options = OpenOptions::new();
        options.create(true).truncate(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(path).expect("create journal fixture");
        for record in records {
            serde_json::to_writer(&mut file, record).expect("encode record");
            file.write_all(b"\n").expect("terminate record");
        }
        file.sync_data().expect("persist fixture");
    }
}
