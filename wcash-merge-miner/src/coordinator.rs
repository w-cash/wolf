//! End-to-end native Wcash/Zcash job coordination and durable share journaling.

use std::{
    collections::{HashMap, HashSet},
    fmt,
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Write},
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
use zebra_chain::{
    block::Block,
    serialization::{ZcashDeserializeInto, ZcashSerialize},
    work::{
        difficulty::{CompactDifficulty, ExpandedDifficulty, U256},
        equihash::{Solution, WCASH_BLOCK_WIRE_VERSION},
    },
};

use crate::{
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
    /// Wcash Unified Address that receives the private child coinbase.
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

/// A fully prepared dual-chain job and its submission backend.
pub struct NativeMiningCoordinator {
    wcash_node: ZebraRpcClient,
    zcash: NativeZcashProvider,
    job: NativePreparedJob,
    child_height: u32,
    child_previous_hash: String,
    candidate_created_at: Instant,
    freshness: Mutex<JobFreshness>,
    last_outbox_retry: Mutex<Instant>,
    outbox_retry_requested: AtomicBool,
    outbox_retry_in_progress: AtomicBool,
    journal: ShareJournal,
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

const MAX_JOB_AGE: Duration = Duration::from_secs(8 * 60);
const TIP_RECHECK_INTERVAL: Duration = Duration::from_secs(2);
const OUTBOX_RETRY_INTERVAL: Duration = Duration::from_secs(15);
const MAX_CHILD_BLOCK_BYTES: usize = 2_000_000;
const WINNER_RETENTION_CONFIRMATIONS: u32 = 100;

impl NativeMiningCoordinator {
    /// Creates a Wcash candidate, builds its parent template, and completes the
    /// independent Zcash proposal gate before returning solver work.
    pub fn prepare(
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
                "the Wcash template node must be loopback: it chooses the private child coinbase recipient"
                    .to_string(),
            ));
        }

        parse_display_hash(
            &config.expected_wcash_genesis_hash,
            "expected Wcash genesis hash",
        )?;
        let mut journal = ShareJournal::open(journal_path)?;
        let wcash_node = ZebraRpcClient::new(config.wcash_node, DEFAULT_RPC_TIMEOUT)?;
        let expected_zcash_genesis_hash = config.zcash.expected_genesis_hash().to_string();
        let zcash = NativeZcashProvider::connect(config.zcash)?;

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
        zcash_identity?;

        let child: ChildTemplate =
            wcash_node.call("createauxblock", json!([config.wcash_payout_address]))?;
        let candidate_created_at = Instant::now();
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
        let child_hash = parse_display_hash(&child.hash, "createauxblock hash")?;
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
            child_candidate_bytes,
        })?;

        let coordinator = Self {
            wcash_node,
            zcash,
            job,
            child_height: child.height,
            child_previous_hash: child.previous_block_hash,
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
        Ok(coordinator)
    }

    /// Returns the exact proposal-gated solver job.
    pub const fn job(&self) -> &NativePreparedJob {
        &self.job
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
                "the Wcash candidate reached its {}-second local safety lifetime",
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

impl ShareProcessor for NativeMiningCoordinator {
    fn process(&self, worker: &str, share: &ValidatedNativeShare) -> Result<(), MinerError> {
        let is_network_winner = share.wcash_candidate().is_some() || share.parent_block().is_some();

        // A tip check on one chain must never suppress a valid winner for the
        // other chain. Persist exact winner bytes first, then let each chain's
        // consensus submission RPC make the authoritative decision.
        if is_network_winner {
            self.journal
                .record(worker, self.job.job().job_id(), share)?;
            // The client ACK depends only on durable accounting. The dedicated
            // health monitor sees this release-store on its next one-second
            // cycle and performs submission without retaining the ASIC's
            // connection permit across node RPC outages.
            self.request_outbox_retry();
            return Ok(());
        }

        self.assert_current()?;
        self.journal
            .record(worker, self.job.job().job_id(), share)?;
        Ok(())
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
    active_job: Option<ActiveJournalJob>,
}

struct ActiveJournalJob {
    job_id: String,
    child_hash_display: String,
    child_height: u32,
    parent_height: u32,
    child_candidate_bytes: Vec<u8>,
}

struct JournalState {
    file: File,
    bytes_written: u64,
    /// Set after an append or durability failure. A subsequent append could
    /// otherwise terminate a partial JSON line and make crash recovery parse
    /// attacker-controlled concatenated data as a complete record.
    poisoned: bool,
    active_job_ids: HashSet<[u8; 32]>,
    active_winner_ids: HashSet<[u8; 32]>,
    pending_winners: HashMap<WinnerKey, PendingWinner>,
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
        }
        sync_journal_parent_directory(path)?;
        let active_job_ids = HashSet::new();
        let active_winner_ids = HashSet::new();
        let mut pending_winners = HashMap::new();
        let mut reader = BufReader::new(file.try_clone()?);
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
            let share_id = parse_share_id(&record.share_id)?;
            match record.record.as_str() {
                "accepted_share" => {
                    // Accepted-share IDs are loaded for the newly prepared job
                    // by activate_job; retaining every historical share here
                    // would allow an old journal to consume unbounded memory.
                }
                "winner_outbox" => {
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
                active_job_ids,
                active_winner_ids,
                pending_winners,
            }),
            path: path.to_path_buf(),
            active_job: None,
        })
    }

    fn activate_job(&mut self, active_job: ActiveJournalJob) -> Result<(), MinerError> {
        let file = File::open(&self.path)?;
        let mut reader = BufReader::new(file);
        let mut active_job_ids = HashSet::new();
        let mut active_winner_ids = HashSet::new();
        while let Some((line, terminated, _consumed)) = read_bounded_journal_line(&mut reader)? {
            if !terminated {
                return Err(MinerError::InvalidRequest(
                    "share journal changed while the active job was being loaded".to_string(),
                ));
            }
            let record: PersistedShareKey = serde_json::from_slice(&line).map_err(|error| {
                MinerError::InvalidRequest(format!("share journal is corrupted: {error}"))
            })?;
            if record.version != JOURNAL_VERSION || record.job_id != active_job.job_id {
                continue;
            }
            let share_id = parse_share_id(&record.share_id)?;
            match record.record.as_str() {
                "accepted_share" => {
                    active_job_ids.insert(share_id);
                }
                "winner_outbox" => {
                    active_winner_ids.insert(share_id);
                }
                "winner_observed" | "winner_orphaned" | "winner_matured" | "winner_confirmed" => {}
                _ => {
                    return Err(MinerError::InvalidRequest(format!(
                        "share journal contains unsupported version {} record {:?}",
                        record.version, record.record
                    )));
                }
            }
            if active_job_ids.len() > MAX_SHARES_PER_JOB {
                return Err(MinerError::InvalidRequest(format!(
                    "share journal already contains more than {MAX_SHARES_PER_JOB} shares for job {}",
                    active_job.job_id
                )));
            }
        }

        let state = self.state.get_mut().map_err(|_| journal_mutex_error())?;
        state.active_job_ids = active_job_ids;
        state.active_winner_ids = active_winner_ids;
        self.active_job = Some(active_job);
        Ok(())
    }

    fn record(
        &self,
        worker: &str,
        job_id: &str,
        share: &ValidatedNativeShare,
    ) -> Result<[u8; 32], MinerError> {
        let active_job = self.active_job.as_ref().ok_or_else(|| {
            MinerError::InvalidRequest("share journal has no active mining job".to_string())
        })?;
        if active_job.job_id != job_id {
            return Err(MinerError::InvalidRequest(
                "share belongs to a different job than the active journal".to_string(),
            ));
        }
        let share_id = share_id(job_id, share);
        let is_network_winner = share.wcash_candidate().is_some() || share.parent_block().is_some();
        let wcash_block = share
            .wcash_candidate()
            .map(|winner| {
                complete_wcash_candidate(
                    &active_job.child_candidate_bytes,
                    &active_job.child_hash_display,
                    winner.encoded_proof(),
                )
            })
            .transpose()?;
        let zcash_block = share.parent_block().map(<[u8]>::to_vec);
        let mut state = self.lock_state()?;
        if state.active_job_ids.contains(&share_id) || state.active_winner_ids.contains(&share_id) {
            return Ok(share_id);
        }
        if !is_network_winner && state.active_job_ids.len() >= MAX_SHARES_PER_JOB {
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

        let timestamp = unix_timestamp()?;
        let record = json!({
            "version": JOURNAL_VERSION,
            "record": if is_network_winner { "winner_outbox" } else { "accepted_share" },
            "accepted_at": timestamp,
            "share_id": hex::encode(share_id),
            "worker": worker,
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
        let mut encoded = serde_json::to_vec(&record)?;
        encoded.push(b'\n');
        append_synced_journal_record(&mut state, &encoded)?;
        if is_network_winner {
            state.active_winner_ids.insert(share_id);
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
            state.active_job_ids.insert(share_id);
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
        append_synced_journal_record(&mut state, &encoded)?;
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
struct PersistedShareKey {
    version: u8,
    record: String,
    job_id: String,
    share_id: String,
    #[serde(default)]
    wcash_candidate: bool,
    #[serde(default)]
    zcash_candidate: bool,
    #[serde(default)]
    parent_hash_le: Option<String>,
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

fn persisted_winners(
    record: &PersistedShareKey,
    share_id: [u8; 32],
) -> Result<Vec<PendingWinner>, MinerError> {
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
        validate_persisted_block(
            &block_bytes,
            &block_hash_display,
            height,
            Some(WCASH_BLOCK_WIRE_VERSION),
        )?;
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
        let raw_parent_hash = parse_raw_hash(
            required_journal_field(
                record.parent_hash_le.as_deref(),
                "winner_outbox.parent_hash_le",
            )?,
            "winner_outbox.parent_hash_le",
        )?;
        let block_hash_display = display_hash(raw_parent_hash);
        let height = record.zcash_height.ok_or_else(|| {
            MinerError::InvalidRequest("winner_outbox has no Zcash height".to_string())
        })?;
        let block_bytes = decode_bounded_hex(
            required_journal_field(record.zcash_block.as_deref(), "winner_outbox.zcash_block")?,
            "winner_outbox.zcash_block",
            MAX_CHILD_BLOCK_BYTES,
        )?;
        validate_persisted_block(&block_bytes, &block_hash_display, height, None)?;
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

fn validate_persisted_block(
    block_bytes: &[u8],
    expected_hash_display: &str,
    expected_height: u32,
    expected_version: Option<u32>,
) -> Result<(), MinerError> {
    let block: Block = block_bytes.zcash_deserialize_into().map_err(|error| {
        MinerError::InvalidRequest(format!("share journal contains an invalid block: {error}"))
    })?;
    if block.zcash_serialize_to_vec()? != block_bytes {
        return Err(MinerError::InvalidRequest(
            "share journal contains a non-canonical block".to_string(),
        ));
    }
    let expected_hash = parse_display_hash(expected_hash_display, "winner_outbox block hash")?;
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
    if expected_version.is_some() {
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
        let expanded = block
            .header
            .difficulty_threshold
            .to_expanded()
            .ok_or_else(|| {
                MinerError::InvalidRequest(
                    "share journal Wcash winner has an invalid compact target".to_string(),
                )
            })?;
        let expanded: U256 = expanded.into();
        let target = Target::from_le_bytes(expanded.to_little_endian()).map_err(|error| {
            MinerError::InvalidRequest(format!(
                "share journal Wcash winner has an invalid target: {error}"
            ))
        })?;
        proof.validate(block.hash().0, target).map_err(|error| {
            MinerError::InvalidRequest(format!(
                "share journal Wcash winner contains invalid AuxPoW: {error}"
            ))
        })?;
    }
    Ok(())
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
    encoded: &[u8],
) -> Result<(), MinerError> {
    let new_length = journal_length_after_append(state, encoded.len())?;
    if let Err(error) = state
        .file
        .write_all(encoded)
        .and_then(|()| state.file.sync_data())
    {
        state.poisoned = true;
        return Err(MinerError::Io(error));
    }
    state.bytes_written = new_length;
    Ok(())
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
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    File::open(parent)?.sync_all()?;
    Ok(())
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

fn parse_share_id(encoded: &str) -> Result<[u8; 32], MinerError> {
    if encoded.len() != 64 {
        return Err(MinerError::InvalidRequest(
            "share journal contains a non-32-byte share ID".to_string(),
        ));
    }
    hex::decode(encoded)
        .map_err(|error| {
            MinerError::InvalidRequest(format!("share journal contains invalid share ID: {error}"))
        })?
        .try_into()
        .map_err(|bytes: Vec<u8>| {
            MinerError::InvalidRequest(format!(
                "share journal ID decoded to {} bytes, expected 32",
                bytes.len()
            ))
        })
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
    use std::fs;

    use tempfile::tempdir;
    use zebra_chain::block::genesis::{regtest_genesis_block, wcash_regtest_genesis_block};

    use super::*;

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
    }

    #[test]
    fn persisted_share_ids_are_strictly_decoded() {
        assert_eq!(parse_share_id(&"07".repeat(32)).expect("valid ID"), [7; 32]);
        assert!(parse_share_id("07").is_err());
        assert!(parse_share_id(&"zz".repeat(32)).is_err());
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
            validate_persisted_block(
                &malformed_bytes,
                &display_hash(malformed.hash().0),
                0,
                Some(WCASH_BLOCK_WIRE_VERSION),
            )
            .is_err(),
            "a nonempty but invalid AuxPoW witness must not survive journal recovery"
        );
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
        assert!(append_synced_journal_record(&mut state, b"{}\n").is_err());
        assert_eq!(state.bytes_written, original_length);
        assert_eq!(fs::metadata(&path).expect("journal metadata").len(), 0);
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
        let block = regtest_genesis_block();
        let block_bytes = block
            .zcash_serialize_to_vec()
            .expect("genesis block serializes");
        let raw_hash = block.hash().0;
        let display = display_hash(raw_hash);
        let share_id = "2a".repeat(32);
        let outbox = json!({
            "version": JOURNAL_VERSION,
            "record": "winner_outbox",
            "job_id": "job-a",
            "share_id": share_id,
            "parent_hash_le": hex::encode(raw_hash),
            "wcash_candidate": false,
            "zcash_candidate": true,
            "zcash_height": 0,
            "zcash_block": hex::encode(block_bytes),
        });
        let observed = json!({
            "version": JOURNAL_VERSION,
            "record": "winner_observed",
            "job_id": "job-a",
            "share_id": "2a".repeat(32),
            "chain": "zcash",
            "block_hash": display,
            "height": 0,
        });
        let matured = json!({
            "version": JOURNAL_VERSION,
            "record": "winner_matured",
            "job_id": "job-a",
            "share_id": "2a".repeat(32),
            "chain": "zcash",
            "block_hash": display_hash(raw_hash),
            "height": 0,
        });
        write_journal_records(&valid_path, &[outbox.clone(), observed, matured]);
        let valid = ShareJournal::open(&valid_path).expect("exact transitions load");
        assert_eq!(valid.status().expect("journal status").pending_zcash, 0);
        drop(valid);

        let forged = json!({
            "version": JOURNAL_VERSION,
            "record": "winner_observed",
            "job_id": "job-a",
            "share_id": "2a".repeat(32),
            "chain": "zcash",
            "block_hash": "ff".repeat(32),
            "height": 0,
        });
        write_journal_records(&invalid_path, &[outbox, forged]);
        assert!(
            ShareJournal::open(&invalid_path).is_err(),
            "a status line cannot discard or mutate a different winner"
        );
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
