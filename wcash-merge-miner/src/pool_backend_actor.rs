//! Serialized authority for the private pool-backend protocol.
//!
//! The actor owns the only live handle to the durable backend journal. Job
//! publication, share validation, durable acknowledgements, snapshots, and
//! event replay are consequently ordered by one mutex. Expensive native
//! validation can move to a bounded worker queue later without changing the
//! journal transaction boundary established here.

use std::{
    collections::{HashMap, VecDeque},
    fmt,
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, Instant},
};

use thiserror::Error;
use wcash_pool_protocol::{
    canonical_attribution_id, canonical_parent_header_hash_le, canonical_share_id, AcceptableJob,
    BackendErrorCode, BackendEvent, BackendMessage, BackendRequest, CanonicalUuid, ChainTip,
    Hex1344, Hex32, Hex4, JobDescriptor, JobInvalidationReason, MergedChain, ProtocolError,
    ShareReceipt, TargetLe, WinnerDescriptor, WorkerIdentity, BACKEND_PROTOCOL_VERSION,
    MAX_EVENT_PAGE_ITEMS, REQUIRED_BACKEND_CAPABILITIES,
};

use crate::{
    pool_backend::{PoolShareTargetPolicy, PoolShareTargetPolicyError},
    pool_backend_connection::BackendRequestKind,
    pool_backend_journal::{
        JournalWinnerBlocks, JournalWinnerLifecycle, JournalWinnerState, JournalWinnerTransition,
        PoolBackendJournal, PoolBackendJournalError,
    },
    pool_backend_listener::{
        PoolBackendAuthority, PoolBackendHandlerError, PoolBackendListenerError,
        PoolBackendRequestContext, PoolBackendRequestHandler,
    },
};

const MAX_RECENT_JOBS: usize = 2;
const MAX_SUPERSEDED_GRACE: Duration = Duration::from_secs(60);

/// A retained exact generation capable of independently validating miner work.
///
/// Implementations must return only after validating the exact header time,
/// nonce, Equihash solution, and assigned target against [`Self::descriptor`].
/// Returning winner bytes asserts that the same validated header independently
/// met that chain's network target. The journal validates and binds those exact
/// blocks again before acknowledging the share.
pub trait PoolBackendRetainedJob: Send + Sync {
    /// Returns the exact proposal-validated descriptor retained by this job.
    fn descriptor(&self) -> JobDescriptor;

    /// Returns the configured Wcash payout commitment authenticated for this job.
    fn wcash_payout_commitment(&self) -> Hex32;

    /// Returns the configured Zcash payout commitment authenticated for this job.
    fn zcash_payout_commitment(&self) -> Hex32;

    /// Returns the remaining native admission lifetime at the instant sampled.
    ///
    /// The actor samples its own monotonic clock before this method, then uses
    /// the returned duration as an upper bound. Implementations must never
    /// restart the underlying generation's lifetime. `None` means the job can
    /// no longer be activated. This method runs under the actor mutex and must
    /// be a bounded local clock read without RPC or other blocking I/O.
    fn remaining_lifetime(&self) -> Option<Duration>;

    /// Returns whether this retained generation can presently validate work.
    fn is_healthy(&self) -> bool;

    /// Performs exact consensus validation and independently classifies both
    /// network targets.
    fn validate_share(
        &self,
        share: PoolBackendShareRequest<'_>,
    ) -> Result<PoolBackendValidatedShare, PoolBackendShareValidationError>;
}

/// Borrowed proof fields supplied to one retained generation.
#[derive(Clone, Copy)]
pub struct PoolBackendShareRequest<'a> {
    time: &'a Hex4,
    nonce: &'a Hex32,
    solution: &'a Hex1344,
    target_le: &'a TargetLe,
}

impl fmt::Debug for PoolBackendShareRequest<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PoolBackendShareRequest")
            .field("time", self.time)
            .field("nonce", &"[REDACTED 32 bytes]")
            .field("solution", &"[REDACTED 1344 bytes]")
            .field("target_le", self.target_le)
            .finish()
    }
}

impl<'a> PoolBackendShareRequest<'a> {
    /// Returns the exact four header-time bytes supplied by the edge.
    pub const fn time(&self) -> &'a Hex4 {
        self.time
    }

    /// Returns the complete reconstructed header nonce.
    pub const fn nonce(&self) -> &'a Hex32 {
        self.nonce
    }

    /// Returns the raw Equihash `(200, 9)` solution.
    pub const fn solution(&self) -> &'a Hex1344 {
        self.solution
    }

    /// Returns the exact assigned little-endian share target.
    pub const fn target_le(&self) -> &'a TargetLe {
        self.target_le
    }
}

/// Exact private winner material produced by one validated share.
///
/// Empty material represents an ordinary accepted pool share. Block contents
/// are redacted from `Debug` and never cross the private backend wire.
#[derive(Clone, Default, Eq, PartialEq)]
pub struct PoolBackendValidatedShare {
    wcash_block: Option<Vec<u8>>,
    zcash_block: Option<Vec<u8>>,
}

impl PoolBackendValidatedShare {
    /// Creates an ordinary accepted share that meets neither network target.
    pub const fn ordinary() -> Self {
        Self {
            wcash_block: None,
            zcash_block: None,
        }
    }

    /// Creates an accepted share with exact independently classified winners.
    pub fn with_winners(wcash_block: Option<Vec<u8>>, zcash_block: Option<Vec<u8>>) -> Self {
        Self {
            wcash_block,
            zcash_block,
        }
    }
}

impl fmt::Debug for PoolBackendValidatedShare {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PoolBackendValidatedShare")
            .field(
                "wcash_block_bytes",
                &self.wcash_block.as_ref().map(Vec::len),
            )
            .field(
                "zcash_block_bytes",
                &self.zcash_block.as_ref().map(Vec::len),
            )
            .finish()
    }
}

/// Authority-bound key for one independently reconciled chain winner.
///
/// Keys are created from actor snapshots rather than arbitrary caller bytes,
/// preventing a pagination cursor or point lookup from crossing journal
/// sequence namespaces accidentally.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PoolBackendWinnerKey {
    journal_stream: CanonicalUuid,
    winner_ordinal: usize,
    share_id: Hex32,
    chain: MergedChain,
}

impl PoolBackendWinnerKey {
    /// Returns the stable accepted-share identity that created this winner.
    pub const fn share_id(&self) -> &Hex32 {
        &self.share_id
    }

    /// Returns the independently reconciled merged-mining chain.
    pub const fn chain(&self) -> MergedChain {
        self.chain
    }
}

/// One exact, bounded winner snapshot returned by the serialized actor.
///
/// The snapshot is also the compare-and-swap token for an external
/// reconciliation attempt. Its exact block is capped by
/// `MAX_WINNER_BLOCK_BYTES`; `Debug` reports only its length.
#[derive(Clone, Eq, PartialEq)]
pub struct PoolBackendWinnerSnapshot {
    journal_stream: CanonicalUuid,
    winner_ordinal: usize,
    state: JournalWinnerState,
}

impl PoolBackendWinnerSnapshot {
    /// Returns an authority-bound key suitable for point lookup or pagination.
    pub fn key(&self) -> PoolBackendWinnerKey {
        PoolBackendWinnerKey {
            journal_stream: self.journal_stream,
            winner_ordinal: self.winner_ordinal,
            share_id: self.state.share_id.clone(),
            chain: self.state.winner.chain,
        }
    }

    /// Returns the stable accepted-share identity that created this winner.
    pub const fn share_id(&self) -> &Hex32 {
        &self.state.share_id
    }

    /// Returns the exact backend generation that created this winner.
    pub const fn job_id(&self) -> &Hex32 {
        &self.state.job_id
    }

    /// Returns the immutable chain, reward, height, and block identity facts.
    pub const fn winner(&self) -> &WinnerDescriptor {
        &self.state.winner
    }

    /// Returns the exact validated parent-header hash shared by both chains.
    pub const fn parent_hash_le(&self) -> &Hex32 {
        &self.state.parent_hash_le
    }

    /// Returns the exact canonical block retained for submission or checking.
    pub fn block_bytes(&self) -> &[u8] {
        &self.state.block_bytes
    }

    /// Returns the latest durable lifecycle state.
    pub const fn lifecycle(&self) -> &JournalWinnerLifecycle {
        &self.state.lifecycle
    }

    /// Returns the per-winner event revision used by compare-and-swap.
    pub const fn revision_event_seq(&self) -> u64 {
        self.state.revision_event_seq
    }
}

impl fmt::Debug for PoolBackendWinnerSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PoolBackendWinnerSnapshot")
            .field("journal_stream", &self.journal_stream)
            .field("winner_ordinal", &self.winner_ordinal)
            .field("share_id", &self.state.share_id)
            .field("job_id", &self.state.job_id)
            .field("winner", &self.state.winner)
            .field("parent_hash_le", &self.state.parent_hash_le)
            .field("block_bytes_len", &self.state.block_bytes.len())
            .field("lifecycle", &self.state.lifecycle)
            .field("revision_event_seq", &self.state.revision_event_seq)
            .finish()
    }
}

/// Exact best-chain result to append for one authority-bound winner snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PoolBackendWinnerTransition {
    /// The exact retained block is on the sampled best chain.
    Observed {
        /// Exact best-chain tip used for this observation.
        tip: ChainTip,
        /// Confirmations computed from the same chain snapshot as `tip`.
        confirmations: u32,
    },
    /// A previously observed or matured block left the sampled best chain.
    Orphaned {
        /// Exact replacement best-chain tip.
        tip: ChainTip,
    },
    /// A conflicting Wcash AuxPoW witness occupies the same child block ID.
    Quarantined {
        /// Exact best-chain tip sampled with the witness conflict.
        tip: ChainTip,
    },
    /// A quarantined Wcash winner is absent and eligible for resubmission.
    Requeued {
        /// Exact best-chain tip sampled before releasing quarantine.
        tip: ChainTip,
    },
    /// An observed reward reached its immutable maturity threshold.
    Matured {
        /// Exact best-chain tip used for this maturity decision.
        tip: ChainTip,
        /// Confirmations computed from the same chain snapshot as `tip`.
        confirmations: u32,
    },
}

impl From<PoolBackendWinnerTransition> for JournalWinnerTransition {
    fn from(transition: PoolBackendWinnerTransition) -> Self {
        match transition {
            PoolBackendWinnerTransition::Observed { tip, confirmations } => {
                Self::Observed { tip, confirmations }
            }
            PoolBackendWinnerTransition::Orphaned { tip } => Self::Orphaned { tip },
            PoolBackendWinnerTransition::Quarantined { tip } => Self::Quarantined { tip },
            PoolBackendWinnerTransition::Requeued { tip } => Self::Requeued { tip },
            PoolBackendWinnerTransition::Matured { tip, confirmations } => {
                Self::Matured { tip, confirmations }
            }
        }
    }
}

/// Stable failure classes returned by a retained consensus validator.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum PoolBackendShareValidationError {
    /// The header hash did not meet the exact assigned pool target.
    #[error("share does not meet its assigned target")]
    LowDifficulty,
    /// The submitted Equihash solution was malformed or invalid.
    #[error("share has an invalid Equihash solution")]
    InvalidEquihash,
    /// The proof fields do not belong to this exact retained generation.
    #[error("share belongs to stale work")]
    StaleJob,
    /// A required consensus dependency is unavailable or inconsistent.
    #[error("retained consensus validator is unavailable")]
    BackendUnavailable,
}

impl PoolBackendShareValidationError {
    fn handler_error(self) -> PoolBackendHandlerError {
        match self {
            Self::LowDifficulty => PoolBackendHandlerError::new(
                BackendErrorCode::LowDifficulty,
                "share does not meet its assigned target",
                false,
            ),
            Self::InvalidEquihash => PoolBackendHandlerError::new(
                BackendErrorCode::InvalidEquihash,
                "share has an invalid Equihash solution",
                false,
            ),
            Self::StaleJob => PoolBackendHandlerError::new(
                BackendErrorCode::StaleJob,
                "share belongs to stale work",
                false,
            ),
            Self::BackendUnavailable => PoolBackendHandlerError::new(
                BackendErrorCode::BackendUnhealthy,
                "consensus validation is unavailable",
                true,
            ),
        }
    }
}

/// Failure to construct or durably mutate the backend actor.
#[derive(Debug, Error)]
pub enum PoolBackendActorError {
    /// The authoritative journal could not be read or durably updated.
    #[error("pool backend journal operation failed")]
    Journal(#[from] PoolBackendJournalError),

    /// Journal identity could not form a complete listener authority.
    #[error("pool backend authority is invalid")]
    Authority(#[source] PoolBackendListenerError),

    /// A retained descriptor violates the backend protocol.
    #[error("retained pool job is invalid")]
    InvalidJob(#[source] ProtocolError),

    /// The deployment target policy cannot safely admit this job.
    #[error("retained pool job violates share-target policy")]
    TargetPolicy(#[source] PoolShareTargetPolicyError),

    /// One job ID cannot identify more than one generation.
    #[error("pool job was already present in the durable journal")]
    JobAlreadyKnown,

    /// A lifecycle operation named a generation unknown to this process.
    #[error("pool job is not retained by this actor")]
    JobNotRetained,

    /// The requested transition does not match the retained job's durable phase.
    #[error("pool job lifecycle does not allow the requested transition")]
    InvalidJobPhase,

    /// Two promised superseded generations are already accepting grace shares.
    #[error("two recent pool jobs are already within their advertised grace periods")]
    RecentJobCapacity,

    /// Superseded grace must fit the protocol's bounded interval.
    #[error("superseded grace must be in 1ms..=60s")]
    InvalidSupersededGrace,

    /// An unhealthy retained validator cannot be advertised to miners.
    #[error("retained pool job is not healthy")]
    RetainedJobUnhealthy,

    /// The retained native job was prepared for another collector authority.
    #[error("retained pool job payout commitments do not match this backend journal")]
    PayoutAuthorityMismatch,

    /// A winner key or snapshot belongs to another journal sequence namespace.
    #[error("winner reconciliation authority does not match this backend journal")]
    WinnerAuthorityMismatch,

    /// A winner snapshot no longer names retained private block material.
    #[error("winner reconciliation snapshot is not retained")]
    WinnerNotRetained,

    /// Another transition changed this winner after the observation began.
    #[error("winner reconciliation snapshot is stale")]
    WinnerRevisionConflict,

    /// Replayed events contradicted the actor projection.
    #[error("durable backend events contain an invalid actor projection: {0}")]
    InvalidReplay(&'static str),

    /// A bounded in-memory projection could not reserve capacity.
    #[error("could not reserve bounded pool backend actor state")]
    Allocation(#[source] std::collections::TryReserveError),

    /// Monotonic lifetime arithmetic overflowed.
    #[error("pool backend monotonic deadline overflowed")]
    DeadlineOverflow,

    /// A panic poisoned the serialized authority; restart is required.
    #[error("pool backend actor mutex is poisoned; restart before serving miners")]
    MutexPoisoned,
}

#[derive(Clone)]
struct AuthorityFields {
    backend_instance: CanonicalUuid,
    journal_stream: CanonicalUuid,
    wcash_genesis: Hex32,
    zcash_genesis: Hex32,
    wcash_payout_commitment: Hex32,
    zcash_payout_commitment: Hex32,
    chain_id: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DurableJobPhase {
    Active,
    Invalidated,
    Closed,
}

struct DurableJob {
    activation_seq: u64,
    phase: DurableJobPhase,
}

#[derive(Clone)]
struct StoredShare {
    receipt: ShareReceipt,
    identity: WorkerIdentity,
    target_le: TargetLe,
}

struct LiveJob {
    descriptor: JobDescriptor,
    validator: Arc<dyn PoolBackendRetainedJob>,
    deadline: Duration,
    phase: DurableJobPhase,
}

trait ActorClock: Send + Sync {
    fn now(&self) -> Duration;
}

struct SystemClock {
    origin: Instant,
}

impl SystemClock {
    fn new() -> Self {
        Self {
            origin: Instant::now(),
        }
    }
}

impl ActorClock for SystemClock {
    fn now(&self) -> Duration {
        self.origin.elapsed()
    }
}

struct ActorState {
    journal: PoolBackendJournal,
    target_policy: PoolShareTargetPolicy,
    durable_jobs: HashMap<Hex32, DurableJob>,
    // This public-event replay index is bounded by MAX_JOURNAL_EVENTS. It keeps
    // only receipt attribution, never Equihash proofs or private winner bytes;
    // those remain solely in the journal's private semantic state.
    shares: HashMap<Hex32, StoredShare>,
    live_jobs: HashMap<Hex32, LiveJob>,
    current: Option<Hex32>,
    /// Oldest to newest. Snapshot responses reverse this order.
    recent: VecDeque<Hex32>,
}

/// Single serialized authority for durable pool jobs and accepted shares.
pub struct PoolBackendActor {
    authority: PoolBackendAuthority,
    authority_fields: AuthorityFields,
    clock: Arc<dyn ActorClock>,
    state: Mutex<ActorState>,
}

impl PoolBackendActor {
    /// Replays one locked journal, closes every abandoned pre-restart job, and
    /// constructs a backend that starts safely paused.
    pub fn new(
        journal: PoolBackendJournal,
        target_policy: PoolShareTargetPolicy,
    ) -> Result<Self, PoolBackendActorError> {
        Self::new_with_clock(journal, target_policy, Arc::new(SystemClock::new()))
    }

    fn new_with_clock(
        journal: PoolBackendJournal,
        target_policy: PoolShareTargetPolicy,
        clock: Arc<dyn ActorClock>,
    ) -> Result<Self, PoolBackendActorError> {
        let authority_fields = AuthorityFields {
            backend_instance: journal.backend_instance(),
            journal_stream: journal.journal_stream(),
            wcash_genesis: journal.wcash_genesis().clone(),
            zcash_genesis: journal.zcash_genesis().clone(),
            wcash_payout_commitment: journal.wcash_payout_commitment().clone(),
            zcash_payout_commitment: journal.zcash_payout_commitment().clone(),
            chain_id: journal.chain_id(),
        };
        let authority = PoolBackendAuthority::new(
            authority_fields.backend_instance,
            authority_fields.journal_stream,
            authority_fields.wcash_genesis.clone(),
            authority_fields.zcash_genesis.clone(),
            authority_fields.wcash_payout_commitment.clone(),
            authority_fields.zcash_payout_commitment.clone(),
            authority_fields.chain_id,
        )
        .map_err(PoolBackendActorError::Authority)?;

        let (durable_jobs, shares) = replay_projection(&journal)?;
        let mut state = ActorState {
            journal,
            target_policy,
            durable_jobs,
            shares,
            live_jobs: HashMap::new(),
            current: None,
            recent: VecDeque::new(),
        };
        close_abandoned_jobs(&mut state)?;

        Ok(Self {
            authority,
            authority_fields,
            clock,
            state: Mutex::new(state),
        })
    }

    /// Durably advertises a healthy exact job, superseding the current job
    /// with the requested grace when necessary.
    ///
    /// A crash after superseding old work but before activating the new job
    /// leaves mining safely paused or on bounded grace work. On restart no old
    /// descriptor is given a new lease.
    pub fn activate_job(
        &self,
        validator: Arc<dyn PoolBackendRetainedJob>,
        superseded_grace: Duration,
    ) -> Result<BackendEvent, PoolBackendActorError> {
        let mut descriptor = validator.descriptor();
        descriptor
            .validate()
            .map_err(PoolBackendActorError::InvalidJob)?;
        if validator.wcash_payout_commitment() != self.authority_fields.wcash_payout_commitment
            || validator.zcash_payout_commitment() != self.authority_fields.zcash_payout_commitment
        {
            return Err(PoolBackendActorError::PayoutAuthorityMismatch);
        }
        if !validator.is_healthy() {
            return Err(PoolBackendActorError::RetainedJobUnhealthy);
        }

        let mut state = self.lock_state()?;
        // Sample the actor clock first. Because the native remaining lifetime
        // is sampled afterwards, adding it to `now` cannot extend the native
        // generation's absolute deadline.
        let now = self.clock.now();
        let remaining_lifetime = validator
            .remaining_lifetime()
            .ok_or(PoolBackendActorError::RetainedJobUnhealthy)?;
        let remaining_ms = u32::try_from(remaining_lifetime.as_millis()).unwrap_or(u32::MAX);
        if remaining_ms == 0 {
            return Err(PoolBackendActorError::RetainedJobUnhealthy);
        }
        descriptor.max_age_ms = descriptor.max_age_ms.min(remaining_ms);

        expire_jobs(&mut state, now)?;
        state
            .target_policy
            .bounds_for_job(&descriptor)
            .map_err(PoolBackendActorError::TargetPolicy)?;
        if state.durable_jobs.contains_key(&descriptor.job_id) {
            return Err(PoolBackendActorError::JobAlreadyKnown);
        }
        let deadline = now
            .checked_add(Duration::from_millis(u64::from(descriptor.max_age_ms)))
            .ok_or(PoolBackendActorError::DeadlineOverflow)?;
        let tip_change = changed_tip_reason(&state, &descriptor);
        if tip_change.is_none() && state.current.is_some() && state.recent.len() >= MAX_RECENT_JOBS
        {
            return Err(PoolBackendActorError::RecentJobCapacity);
        }

        state
            .durable_jobs
            .try_reserve(1)
            .map_err(PoolBackendActorError::Allocation)?;
        state
            .live_jobs
            .try_reserve(1)
            .map_err(PoolBackendActorError::Allocation)?;
        if let Some(reason) = tip_change {
            close_jobs_for_tip_change(&mut state, reason)?;
        } else if state.current.is_some() {
            state
                .recent
                .try_reserve(1)
                .map_err(PoolBackendActorError::Allocation)?;
            let grace_ms = bounded_grace_ms(superseded_grace)?;
            supersede_current(&mut state, now, grace_ms)?;
        }

        let event = state.journal.append_event(BackendEvent::JobActivated {
            event_seq: 0,
            job: descriptor.clone(),
        })?;
        let activation_seq = event.event_seq();
        let job_id = descriptor.job_id.clone();
        state.durable_jobs.insert(
            job_id.clone(),
            DurableJob {
                activation_seq,
                phase: DurableJobPhase::Active,
            },
        );
        state.live_jobs.insert(
            job_id.clone(),
            LiveJob {
                descriptor,
                validator,
                deadline,
                phase: DurableJobPhase::Active,
            },
        );
        state.current = Some(job_id);
        Ok(event)
    }

    /// Durably invalidates retained work. A tip change atomically closes every
    /// advertised generation on the named job's old tip; age expiry closes the
    /// named active generation, while supersession retains bounded grace.
    pub fn invalidate_job(
        &self,
        job_id: &Hex32,
        reason: JobInvalidationReason,
        superseded_grace: Duration,
    ) -> Result<Vec<BackendEvent>, PoolBackendActorError> {
        let mut state = self.lock_state()?;
        let now = self.clock.now();
        expire_jobs(&mut state, now)?;
        let phase = state
            .live_jobs
            .get(job_id)
            .map(|job| job.phase)
            .ok_or(PoolBackendActorError::JobNotRetained)?;

        match reason {
            JobInvalidationReason::Superseded => {
                if phase != DurableJobPhase::Active {
                    return Err(PoolBackendActorError::InvalidJobPhase);
                }
                if state.recent.len() >= MAX_RECENT_JOBS {
                    return Err(PoolBackendActorError::RecentJobCapacity);
                }
                state
                    .recent
                    .try_reserve(1)
                    .map_err(PoolBackendActorError::Allocation)?;
                let grace_ms = bounded_grace_ms(superseded_grace)?;
                let event = invalidate_with_grace(&mut state, job_id, grace_ms, now)?;
                Ok(vec![event])
            }
            JobInvalidationReason::WcashTipChanged | JobInvalidationReason::ZcashTipChanged => {
                if !superseded_grace.is_zero() {
                    return Err(PoolBackendActorError::InvalidSupersededGrace);
                }
                close_matching_tip_jobs(&mut state, job_id, reason)
            }
            JobInvalidationReason::Age => {
                if phase != DurableJobPhase::Active {
                    return Err(PoolBackendActorError::InvalidJobPhase);
                }
                if !superseded_grace.is_zero() {
                    return Err(PoolBackendActorError::InvalidSupersededGrace);
                }
                let invalidated = invalidate_immediately(&mut state, job_id, reason)?;
                let closed = close_retained_job(&mut state, job_id)?;
                Ok(vec![invalidated, closed])
            }
        }
    }

    /// Durably closes an already-invalidated retained job and releases its
    /// validation resource.
    pub fn close_job(&self, job_id: &Hex32) -> Result<BackendEvent, PoolBackendActorError> {
        let mut state = self.lock_state()?;
        expire_jobs(&mut state, self.clock.now())?;
        let phase = state
            .live_jobs
            .get(job_id)
            .map(|job| job.phase)
            .ok_or(PoolBackendActorError::JobNotRetained)?;
        if phase != DurableJobPhase::Invalidated {
            return Err(PoolBackendActorError::InvalidJobPhase);
        }
        close_retained_job(&mut state, job_id)
    }

    /// Returns the next retained winner in durable journal order while cloning
    /// only that winner's bounded exact block.
    ///
    /// Pass the prior snapshot's [`PoolBackendWinnerSnapshot::key`] to advance
    /// a pass. `None` begins a new pass. Matured winners remain visible because
    /// a deep reorganization can still orphan them.
    pub fn next_winner_snapshot(
        &self,
        after: Option<&PoolBackendWinnerKey>,
    ) -> Result<Option<PoolBackendWinnerSnapshot>, PoolBackendActorError> {
        if after.is_some_and(|key| key.journal_stream != self.authority_fields.journal_stream) {
            return Err(PoolBackendActorError::WinnerAuthorityMismatch);
        }
        let state = self.lock_state()?;
        let after_ordinal = after.map(|key| key.winner_ordinal);
        state
            .journal
            .next_winner_state(after_ordinal)
            .map(|winner| {
                winner.map(|(winner_ordinal, state)| PoolBackendWinnerSnapshot {
                    journal_stream: self.authority_fields.journal_stream,
                    winner_ordinal,
                    state,
                })
            })
            .map_err(PoolBackendActorError::Journal)
    }

    /// Returns the latest snapshot for one authority-bound retained winner.
    pub fn winner_snapshot(
        &self,
        key: &PoolBackendWinnerKey,
    ) -> Result<Option<PoolBackendWinnerSnapshot>, PoolBackendActorError> {
        if key.journal_stream != self.authority_fields.journal_stream {
            return Err(PoolBackendActorError::WinnerAuthorityMismatch);
        }
        let state = self.lock_state()?;
        state
            .journal
            .winner_state(&key.share_id, key.chain)
            .map(|winner| {
                winner.map(|state| PoolBackendWinnerSnapshot {
                    journal_stream: self.authority_fields.journal_stream,
                    winner_ordinal: key.winner_ordinal,
                    state,
                })
            })
            .map_err(PoolBackendActorError::Journal)
    }

    /// Durably applies one winner lifecycle result if `expected` is still the
    /// latest snapshot for that exact winner.
    ///
    /// A worker must obtain the snapshot before starting its external node
    /// observation and submit the result through this method afterwards. A
    /// transition committed by another worker changes the per-winner revision,
    /// so this method rejects the stale result without appending an event.
    pub fn compare_and_apply_winner_transition(
        &self,
        expected: &PoolBackendWinnerSnapshot,
        transition: PoolBackendWinnerTransition,
    ) -> Result<BackendEvent, PoolBackendActorError> {
        if expected.journal_stream != self.authority_fields.journal_stream {
            return Err(PoolBackendActorError::WinnerAuthorityMismatch);
        }
        let state = self.lock_state()?;
        state
            .journal
            .compare_and_transition_winner(
                &expected.state.share_id,
                expected.state.winner.chain,
                expected.state.revision_event_seq,
                transition.into(),
            )
            .map_err(winner_transition_error)
    }

    fn lock_state(&self) -> Result<MutexGuard<'_, ActorState>, PoolBackendActorError> {
        self.state
            .lock()
            .map_err(|_| PoolBackendActorError::MutexPoisoned)
    }

    fn dispatch(
        &self,
        backend_session: CanonicalUuid,
        after_live_event_seq: Option<u64>,
        kind: BackendRequestKind,
        request: BackendRequest,
    ) -> Result<Vec<BackendMessage>, PoolBackendHandlerError> {
        request.validate().map_err(|_| invalid_request())?;
        if !request_matches_kind(&request, kind) {
            return Err(invalid_request());
        }

        let mut state = self
            .state
            .lock()
            .map_err(|_| unhealthy_backend("backend actor requires restart"))?;
        let now = self.clock.now();
        expire_jobs(&mut state, now)
            .map_err(|_| unhealthy_backend("job lifecycle persistence failed"))?;

        match request {
            BackendRequest::Hello {
                id, last_event_seq, ..
            } => self.handle_hello(&state, backend_session, id, last_event_seq),
            BackendRequest::SubscribeJobs {
                id,
                after_event_seq,
                ..
            } => self.handle_subscribe(&state, id, after_event_seq, now),
            BackendRequest::SubmitShare {
                id,
                job_id,
                identity,
                target_le,
                time,
                nonce,
                solution,
                ..
            } => self.handle_submit(
                &mut state,
                after_live_event_seq,
                id,
                job_id,
                identity,
                target_le,
                time,
                nonce,
                *solution,
                now,
            ),
            BackendRequest::ReadEvents {
                id,
                after_event_seq,
                limit,
                ..
            } => self.handle_read_events(&state, id, after_event_seq, limit),
            BackendRequest::Health { id, .. } => {
                self.handle_health(&state, after_live_event_seq, id, now)
            }
        }
    }

    fn handle_hello(
        &self,
        state: &ActorState,
        backend_session: CanonicalUuid,
        id: u64,
        last_event_seq: u64,
    ) -> Result<Vec<BackendMessage>, PoolBackendHandlerError> {
        let current = state
            .journal
            .current_event_seq()
            .map_err(|_| unhealthy_backend("journal watermark is unavailable"))?;
        if last_event_seq > current {
            return Err(invalid_request());
        }
        Ok(vec![BackendMessage::HelloOk {
            version: BACKEND_PROTOCOL_VERSION,
            id,
            backend_session,
            backend_instance: self.authority_fields.backend_instance,
            journal_stream: self.authority_fields.journal_stream,
            capabilities: REQUIRED_BACKEND_CAPABILITIES.to_vec(),
            wcash_genesis: self.authority_fields.wcash_genesis.clone(),
            zcash_genesis: self.authority_fields.zcash_genesis.clone(),
            wcash_payout_commitment: self.authority_fields.wcash_payout_commitment.clone(),
            zcash_payout_commitment: self.authority_fields.zcash_payout_commitment.clone(),
            chain_id: self.authority_fields.chain_id,
            current_event_seq: current,
        }])
    }

    fn handle_subscribe(
        &self,
        state: &ActorState,
        id: u64,
        after_event_seq: u64,
        now: Duration,
    ) -> Result<Vec<BackendMessage>, PoolBackendHandlerError> {
        let event_seq = state
            .journal
            .current_event_seq()
            .map_err(|_| unhealthy_backend("journal watermark is unavailable"))?;
        if after_event_seq > event_seq {
            return Err(invalid_request());
        }
        for job_id in state.current.iter().chain(state.recent.iter()) {
            let job = state
                .live_jobs
                .get(job_id)
                .ok_or_else(|| unhealthy_backend("retained job projection requires restart"))?;
            if !job.validator.is_healthy() {
                return Err(unhealthy_backend(
                    "retained job validator requires durable invalidation",
                ));
            }
        }
        let current = state
            .current
            .as_ref()
            .and_then(|job_id| state.live_jobs.get(job_id))
            .and_then(|job| acceptable_job(job, now));
        let recent = state
            .recent
            .iter()
            .rev()
            .filter_map(|job_id| state.live_jobs.get(job_id))
            .filter_map(|job| acceptable_job(job, now))
            .take(MAX_RECENT_JOBS)
            .collect();
        Ok(vec![BackendMessage::JobSnapshot {
            version: BACKEND_PROTOCOL_VERSION,
            id,
            event_seq,
            current,
            recent,
        }])
    }

    #[allow(clippy::too_many_arguments)]
    fn handle_submit(
        &self,
        state: &mut ActorState,
        after_live_event_seq: Option<u64>,
        id: u64,
        job_id: Hex32,
        identity: WorkerIdentity,
        target_le: TargetLe,
        time: Hex4,
        nonce: Hex32,
        solution: Hex1344,
        now: Duration,
    ) -> Result<Vec<BackendMessage>, PoolBackendHandlerError> {
        let cursor = after_live_event_seq.ok_or_else(invalid_request)?;
        let share_id = canonical_share_id(&job_id, &time, &nonce, &solution);
        let attribution_id =
            canonical_attribution_id(&identity, &target_le).map_err(|_| invalid_request())?;

        if let Some(existing) = state.shares.get(&share_id) {
            if existing.receipt.job_id != job_id
                || existing.receipt.attribution_id != attribution_id
                || existing.identity != identity
                || existing.target_le != target_le
            {
                return Err(attribution_conflict());
            }
            let response = BackendMessage::ShareCommitted {
                version: BACKEND_PROTOCOL_VERSION,
                id,
                receipt: existing.receipt.clone(),
                replayed: true,
            };
            return live_response(&state.journal, cursor, response);
        }

        let retained = state.live_jobs.get(&job_id).ok_or_else(stale_job)?;
        let advertised = match retained.phase {
            DurableJobPhase::Active => state.current.as_ref() == Some(&job_id),
            DurableJobPhase::Invalidated => state.recent.contains(&job_id),
            DurableJobPhase::Closed => false,
        };
        if !advertised || acceptable_job(retained, now).is_none() {
            return Err(stale_job());
        }
        state
            .target_policy
            .validate_for_job(&retained.descriptor, &target_le)
            .map_err(|_| {
                PoolBackendHandlerError::new(
                    BackendErrorCode::TargetOutOfRange,
                    "share target is outside the safe job interval",
                    false,
                )
            })?;
        if retained.descriptor.header_input.as_bytes()[100..104] != time.as_bytes()[..] {
            return Err(stale_job());
        }

        let validated = retained
            .validator
            .validate_share(PoolBackendShareRequest {
                time: &time,
                nonce: &nonce,
                solution: &solution,
                target_le: &target_le,
            })
            .map_err(PoolBackendShareValidationError::handler_error)?;
        let parent_hash_le =
            canonical_parent_header_hash_le(&retained.descriptor.header_input, &nonce, &solution);
        let winners = winner_descriptors(
            &retained.descriptor,
            &parent_hash_le,
            validated.wcash_block.is_some(),
            validated.zcash_block.is_some(),
        );
        let winner_blocks = JournalWinnerBlocks {
            wcash: validated.wcash_block,
            zcash: validated.zcash_block,
        };

        state
            .shares
            .try_reserve(1)
            .map_err(|_| unhealthy_backend("share projection capacity is exhausted"))?;
        let commit = state
            .journal
            .append_share_committed(
                job_id,
                share_id.clone(),
                parent_hash_le,
                winners,
                identity.clone(),
                target_le.clone(),
                winner_blocks,
            )
            .map_err(journal_handler_error)?;
        state.shares.insert(
            share_id,
            StoredShare {
                receipt: commit.receipt.clone(),
                identity,
                target_le,
            },
        );
        let response = BackendMessage::ShareCommitted {
            version: BACKEND_PROTOCOL_VERSION,
            id,
            receipt: commit.receipt,
            replayed: commit.replayed,
        };
        live_response(&state.journal, cursor, response)
    }

    fn handle_read_events(
        &self,
        state: &ActorState,
        id: u64,
        after_event_seq: u64,
        limit: u16,
    ) -> Result<Vec<BackendMessage>, PoolBackendHandlerError> {
        let current = state
            .journal
            .current_event_seq()
            .map_err(|_| unhealthy_backend("journal watermark is unavailable"))?;
        if after_event_seq > current {
            return Err(invalid_request());
        }
        let page = state
            .journal
            .read_events(after_event_seq, limit)
            .map_err(journal_handler_error)?;
        Ok(vec![BackendMessage::EventsPage {
            version: BACKEND_PROTOCOL_VERSION,
            id,
            after_event_seq: page.after_event_seq,
            next_event_seq: page.next_event_seq,
            complete: page.complete,
            events: page.events,
        }])
    }

    fn handle_health(
        &self,
        state: &ActorState,
        after_live_event_seq: Option<u64>,
        id: u64,
        now: Duration,
    ) -> Result<Vec<BackendMessage>, PoolBackendHandlerError> {
        // This synchronous snapshot reports durable outbox pressure and share
        // admission health. A future actor-owned reconciliation worker must
        // also gate `healthy` on live winner-submission dependencies; callers
        // must not interpret these counters as proof that submission ran.
        let winner_summary = state
            .journal
            .winner_summary()
            .map_err(journal_handler_error)?;
        let event_seq = state
            .journal
            .current_event_seq()
            .map_err(journal_handler_error)?;
        let current_healthy = state
            .current
            .as_ref()
            .and_then(|job_id| state.live_jobs.get(job_id))
            .is_some_and(|job| job.validator.is_healthy() && acceptable_job(job, now).is_some());
        let recent_healthy = state.recent.iter().all(|job_id| {
            state
                .live_jobs
                .get(job_id)
                .is_some_and(|job| job.validator.is_healthy() && acceptable_job(job, now).is_some())
        });
        let healthy = current_healthy && recent_healthy;
        let response = BackendMessage::HealthStatus {
            version: BACKEND_PROTOCOL_VERSION,
            id,
            event_seq,
            healthy,
            pending_wcash: winner_summary.pending_wcash,
            quarantined_wcash: winner_summary.quarantined_wcash,
            pending_zcash: winner_summary.pending_zcash,
        };
        match after_live_event_seq {
            Some(cursor) => live_response(&state.journal, cursor, response),
            None => Ok(vec![response]),
        }
    }
}

impl PoolBackendRequestHandler for PoolBackendActor {
    fn persistent_authority(&self) -> PoolBackendAuthority {
        self.authority.clone()
    }

    fn handle(
        &self,
        context: PoolBackendRequestContext<'_>,
        kind: BackendRequestKind,
        request: BackendRequest,
    ) -> Result<Vec<BackendMessage>, PoolBackendHandlerError> {
        self.dispatch(
            context.session().backend_session(),
            context.after_live_event_seq(),
            kind,
            request,
        )
    }
}

fn request_matches_kind(request: &BackendRequest, kind: BackendRequestKind) -> bool {
    matches!(
        (request, kind),
        (BackendRequest::Hello { .. }, BackendRequestKind::Hello)
            | (
                BackendRequest::SubscribeJobs { .. },
                BackendRequestKind::SubscribeJobs
            )
            | (
                BackendRequest::SubmitShare { .. },
                BackendRequestKind::SubmitShare
            )
            | (
                BackendRequest::ReadEvents { .. },
                BackendRequestKind::ReadEvents
            )
            | (BackendRequest::Health { .. }, BackendRequestKind::Health)
    )
}

fn replay_projection(
    journal: &PoolBackendJournal,
) -> Result<(HashMap<Hex32, DurableJob>, HashMap<Hex32, StoredShare>), PoolBackendActorError> {
    let mut durable_jobs = HashMap::new();
    let mut shares = HashMap::new();
    let mut cursor = 0u64;
    loop {
        let page = journal.read_events(cursor, MAX_EVENT_PAGE_ITEMS)?;
        durable_jobs
            .try_reserve(page.events.len())
            .map_err(PoolBackendActorError::Allocation)?;
        shares
            .try_reserve(page.events.len())
            .map_err(PoolBackendActorError::Allocation)?;
        for event in page.events {
            match event {
                BackendEvent::JobActivated { event_seq, job } => {
                    let job_id = job.job_id.clone();
                    if durable_jobs
                        .insert(
                            job_id,
                            DurableJob {
                                activation_seq: event_seq,
                                phase: DurableJobPhase::Active,
                            },
                        )
                        .is_some()
                    {
                        return Err(PoolBackendActorError::InvalidReplay(
                            "job ID was activated more than once",
                        ));
                    }
                }
                BackendEvent::JobInvalidated { job_id, .. } => {
                    let job = durable_jobs.get_mut(&job_id).ok_or(
                        PoolBackendActorError::InvalidReplay("invalidation references unknown job"),
                    )?;
                    if job.phase != DurableJobPhase::Active {
                        return Err(PoolBackendActorError::InvalidReplay(
                            "job was invalidated from a non-active state",
                        ));
                    }
                    job.phase = DurableJobPhase::Invalidated;
                }
                BackendEvent::GenerationClosed { job_id, .. } => {
                    let job = durable_jobs.get_mut(&job_id).ok_or(
                        PoolBackendActorError::InvalidReplay("closure references unknown job"),
                    )?;
                    if job.phase != DurableJobPhase::Invalidated {
                        return Err(PoolBackendActorError::InvalidReplay(
                            "job was closed without invalidation",
                        ));
                    }
                    job.phase = DurableJobPhase::Closed;
                }
                BackendEvent::ShareCommitted {
                    receipt,
                    identity,
                    target_le,
                    ..
                } => {
                    let share_id = receipt.share_id.clone();
                    if shares
                        .insert(
                            share_id,
                            StoredShare {
                                receipt,
                                identity,
                                target_le,
                            },
                        )
                        .is_some()
                    {
                        return Err(PoolBackendActorError::InvalidReplay(
                            "share ID was committed more than once",
                        ));
                    }
                }
                BackendEvent::WinnerObserved { .. }
                | BackendEvent::WinnerOrphaned { .. }
                | BackendEvent::WinnerQuarantined { .. }
                | BackendEvent::WinnerRequeued { .. }
                | BackendEvent::WinnerMatured { .. } => {}
            }
        }
        cursor = page.next_event_seq;
        if page.complete {
            break;
        }
        if cursor == page.after_event_seq {
            return Err(PoolBackendActorError::InvalidReplay(
                "event replay made no progress",
            ));
        }
    }
    Ok((durable_jobs, shares))
}

fn close_abandoned_jobs(state: &mut ActorState) -> Result<(), PoolBackendActorError> {
    let mut abandoned = Vec::new();
    abandoned
        .try_reserve(state.durable_jobs.len())
        .map_err(PoolBackendActorError::Allocation)?;
    abandoned.extend(
        state
            .durable_jobs
            .iter()
            .filter(|(_, job)| job.phase != DurableJobPhase::Closed)
            .map(|(job_id, job)| (job.activation_seq, job_id.clone(), job.phase)),
    );
    abandoned.sort_by_key(|(activation_seq, _, _)| *activation_seq);

    for (_, job_id, phase) in abandoned {
        if phase == DurableJobPhase::Active {
            state.journal.append_event(BackendEvent::JobInvalidated {
                event_seq: 0,
                job_id: job_id.clone(),
                reason: JobInvalidationReason::Age,
                accept_for_ms: 0,
            })?;
            state
                .durable_jobs
                .get_mut(&job_id)
                .ok_or(PoolBackendActorError::InvalidReplay(
                    "recovered active job disappeared",
                ))?
                .phase = DurableJobPhase::Invalidated;
        }
        state.journal.append_event(BackendEvent::GenerationClosed {
            event_seq: 0,
            job_id: job_id.clone(),
        })?;
        state
            .durable_jobs
            .get_mut(&job_id)
            .ok_or(PoolBackendActorError::InvalidReplay(
                "recovered invalidated job disappeared",
            ))?
            .phase = DurableJobPhase::Closed;
    }
    Ok(())
}

fn changed_tip_reason(
    state: &ActorState,
    descriptor: &JobDescriptor,
) -> Option<JobInvalidationReason> {
    if state
        .live_jobs
        .values()
        .any(|job| job.descriptor.wcash_previous_hash_le != descriptor.wcash_previous_hash_le)
    {
        Some(JobInvalidationReason::WcashTipChanged)
    } else if state
        .live_jobs
        .values()
        .any(|job| job.descriptor.zcash_previous_hash_le != descriptor.zcash_previous_hash_le)
    {
        Some(JobInvalidationReason::ZcashTipChanged)
    } else {
        None
    }
}

fn close_jobs_for_tip_change(
    state: &mut ActorState,
    reason: JobInvalidationReason,
) -> Result<(), PoolBackendActorError> {
    if let Some(job_id) = state.current.clone() {
        invalidate_immediately(state, &job_id, reason)?;
        close_retained_job(state, &job_id)?;
    }

    while let Some(job_id) = state.recent.front().cloned() {
        close_retained_job(state, &job_id)?;
    }
    Ok(())
}

fn close_matching_tip_jobs(
    state: &mut ActorState,
    anchor_job_id: &Hex32,
    reason: JobInvalidationReason,
) -> Result<Vec<BackendEvent>, PoolBackendActorError> {
    let anchor = state
        .live_jobs
        .get(anchor_job_id)
        .ok_or(PoolBackendActorError::JobNotRetained)?
        .descriptor
        .clone();
    let same_tip = |job: &LiveJob| match reason {
        JobInvalidationReason::WcashTipChanged => {
            job.descriptor.wcash_previous_hash_le == anchor.wcash_previous_hash_le
        }
        JobInvalidationReason::ZcashTipChanged => {
            job.descriptor.zcash_previous_hash_le == anchor.zcash_previous_hash_le
        }
        JobInvalidationReason::Superseded | JobInvalidationReason::Age => false,
    };
    let current = state.current.as_ref().and_then(|job_id| {
        state
            .live_jobs
            .get(job_id)
            .filter(|job| same_tip(job))
            .map(|_| job_id.clone())
    });
    let recent: Vec<_> = state
        .recent
        .iter()
        .filter(|job_id| state.live_jobs.get(*job_id).is_some_and(&same_tip))
        .cloned()
        .collect();
    let mut events = Vec::with_capacity(usize::from(current.is_some()) * 2 + recent.len());

    if let Some(job_id) = current {
        events.push(invalidate_immediately(state, &job_id, reason)?);
        events.push(close_retained_job(state, &job_id)?);
    }
    for job_id in recent {
        events.push(close_retained_job(state, &job_id)?);
    }
    Ok(events)
}

fn bounded_grace_ms(grace: Duration) -> Result<u32, PoolBackendActorError> {
    if grace < Duration::from_millis(1) || grace > MAX_SUPERSEDED_GRACE {
        return Err(PoolBackendActorError::InvalidSupersededGrace);
    }
    u32::try_from(grace.as_millis()).map_err(|_| PoolBackendActorError::InvalidSupersededGrace)
}

fn supersede_current(
    state: &mut ActorState,
    now: Duration,
    requested_grace_ms: u32,
) -> Result<(), PoolBackendActorError> {
    let job_id = state
        .current
        .clone()
        .ok_or(PoolBackendActorError::JobNotRetained)?;
    invalidate_with_grace(state, &job_id, requested_grace_ms, now)?;
    Ok(())
}

fn invalidate_with_grace(
    state: &mut ActorState,
    job_id: &Hex32,
    requested_grace_ms: u32,
    now: Duration,
) -> Result<BackendEvent, PoolBackendActorError> {
    let remaining_ms = state
        .live_jobs
        .get(job_id)
        .and_then(|job| remaining_milliseconds(job.deadline, now))
        .ok_or(PoolBackendActorError::JobNotRetained)?;
    let accept_for_ms = requested_grace_ms.min(remaining_ms);
    if accept_for_ms == 0 {
        return Err(PoolBackendActorError::InvalidSupersededGrace);
    }
    let deadline = now
        .checked_add(Duration::from_millis(u64::from(accept_for_ms)))
        .ok_or(PoolBackendActorError::DeadlineOverflow)?;
    let event = state.journal.append_event(BackendEvent::JobInvalidated {
        event_seq: 0,
        job_id: job_id.clone(),
        reason: JobInvalidationReason::Superseded,
        accept_for_ms,
    })?;
    let live = state
        .live_jobs
        .get_mut(job_id)
        .ok_or(PoolBackendActorError::JobNotRetained)?;
    live.phase = DurableJobPhase::Invalidated;
    live.deadline = deadline;
    state
        .durable_jobs
        .get_mut(job_id)
        .ok_or(PoolBackendActorError::JobNotRetained)?
        .phase = DurableJobPhase::Invalidated;
    if state.current.as_ref() == Some(job_id) {
        state.current = None;
    }
    state.recent.push_back(job_id.clone());
    Ok(event)
}

fn invalidate_immediately(
    state: &mut ActorState,
    job_id: &Hex32,
    reason: JobInvalidationReason,
) -> Result<BackendEvent, PoolBackendActorError> {
    let event = state.journal.append_event(BackendEvent::JobInvalidated {
        event_seq: 0,
        job_id: job_id.clone(),
        reason,
        accept_for_ms: 0,
    })?;
    state
        .live_jobs
        .get_mut(job_id)
        .ok_or(PoolBackendActorError::JobNotRetained)?
        .phase = DurableJobPhase::Invalidated;
    state
        .durable_jobs
        .get_mut(job_id)
        .ok_or(PoolBackendActorError::JobNotRetained)?
        .phase = DurableJobPhase::Invalidated;
    if state.current.as_ref() == Some(job_id) {
        state.current = None;
    }
    Ok(event)
}

fn close_retained_job(
    state: &mut ActorState,
    job_id: &Hex32,
) -> Result<BackendEvent, PoolBackendActorError> {
    let event = state.journal.append_event(BackendEvent::GenerationClosed {
        event_seq: 0,
        job_id: job_id.clone(),
    })?;
    state.live_jobs.remove(job_id);
    state.recent.retain(|recent| recent != job_id);
    if state.current.as_ref() == Some(job_id) {
        state.current = None;
    }
    state
        .durable_jobs
        .get_mut(job_id)
        .ok_or(PoolBackendActorError::JobNotRetained)?
        .phase = DurableJobPhase::Closed;
    Ok(event)
}

fn expire_jobs(state: &mut ActorState, now: Duration) -> Result<(), PoolBackendActorError> {
    if let Some(job_id) = state.current.clone() {
        let deadline = state
            .live_jobs
            .get(&job_id)
            .map(|job| job.deadline)
            .ok_or(PoolBackendActorError::JobNotRetained)?;
        if remaining_milliseconds(deadline, now).is_none() {
            invalidate_immediately(state, &job_id, JobInvalidationReason::Age)?;
            close_retained_job(state, &job_id)?;
        }
    }

    let mut expired_recent = Vec::new();
    expired_recent
        .try_reserve(state.recent.len())
        .map_err(PoolBackendActorError::Allocation)?;
    for job_id in &state.recent {
        let deadline = state
            .live_jobs
            .get(job_id)
            .map(|job| job.deadline)
            .ok_or(PoolBackendActorError::JobNotRetained)?;
        if remaining_milliseconds(deadline, now).is_none() {
            expired_recent.push(job_id.clone());
        }
    }
    for job_id in expired_recent {
        close_retained_job(state, &job_id)?;
    }
    Ok(())
}

fn acceptable_job(job: &LiveJob, now: Duration) -> Option<AcceptableJob> {
    let accept_for_ms = remaining_milliseconds(job.deadline, now)?;
    Some(AcceptableJob {
        job: job.descriptor.clone(),
        accept_for_ms,
    })
}

fn remaining_milliseconds(deadline: Duration, now: Duration) -> Option<u32> {
    let remaining = deadline.checked_sub(now)?;
    let milliseconds = remaining.as_millis();
    if milliseconds == 0 {
        return None;
    }
    u32::try_from(milliseconds.min(u128::from(u32::MAX))).ok()
}

fn winner_descriptors(
    job: &JobDescriptor,
    parent_hash_le: &Hex32,
    wcash: bool,
    zcash: bool,
) -> Vec<WinnerDescriptor> {
    let mut winners = Vec::with_capacity(usize::from(wcash) + usize::from(zcash));
    if wcash {
        winners.push(WinnerDescriptor {
            chain: MergedChain::Wcash,
            block_hash_le: job.wcash_candidate_hash_le.clone(),
            height: job.wcash_height,
            coinbase_txid_le: job.wcash_coinbase_txid_le.clone(),
            reward_zat: job.wcash_reward_zat,
            maturity_confirmations: job.wcash_maturity_confirmations,
        });
    }
    if zcash {
        winners.push(WinnerDescriptor {
            chain: MergedChain::Zcash,
            block_hash_le: parent_hash_le.clone(),
            height: job.zcash_height,
            coinbase_txid_le: job.zcash_coinbase_txid_le.clone(),
            reward_zat: job.zcash_reward_zat,
            maturity_confirmations: job.zcash_maturity_confirmations,
        });
    }
    winners
}

fn live_response(
    journal: &PoolBackendJournal,
    cursor: u64,
    response: BackendMessage,
) -> Result<Vec<BackendMessage>, PoolBackendHandlerError> {
    let page = journal
        .read_events(cursor, MAX_EVENT_PAGE_ITEMS)
        .map_err(journal_handler_error)?;
    if !page.complete {
        return Err(unhealthy_backend(
            "live event backlog requires historical replay",
        ));
    }
    let mut messages = Vec::new();
    messages
        .try_reserve(page.events.len().saturating_add(1))
        .map_err(|_| unhealthy_backend("live event response capacity is exhausted"))?;
    messages.extend(page.events.into_iter().map(|event| BackendMessage::Event {
        version: BACKEND_PROTOCOL_VERSION,
        event,
    }));
    messages.push(response);
    Ok(messages)
}

fn invalid_request() -> PoolBackendHandlerError {
    PoolBackendHandlerError::new(
        BackendErrorCode::InvalidRequest,
        "invalid backend request",
        true,
    )
}

fn stale_job() -> PoolBackendHandlerError {
    PoolBackendHandlerError::new(BackendErrorCode::StaleJob, "job is not acceptable", false)
}

fn attribution_conflict() -> PoolBackendHandlerError {
    PoolBackendHandlerError::new(
        BackendErrorCode::AttributionConflict,
        "share attribution conflicts with its durable receipt",
        false,
    )
}

fn unhealthy_backend(message: &'static str) -> PoolBackendHandlerError {
    PoolBackendHandlerError::new(BackendErrorCode::BackendUnhealthy, message, true)
}

fn journal_handler_error(error: PoolBackendJournalError) -> PoolBackendHandlerError {
    match error {
        PoolBackendJournalError::ShareConflict { .. } => attribution_conflict(),
        _ => unhealthy_backend("authoritative journal operation failed"),
    }
}

fn winner_transition_error(error: PoolBackendJournalError) -> PoolBackendActorError {
    match error {
        PoolBackendJournalError::WinnerNotFound { .. } => PoolBackendActorError::WinnerNotRetained,
        PoolBackendJournalError::WinnerRevisionConflict { .. } => {
            PoolBackendActorError::WinnerRevisionConflict
        }
        error => PoolBackendActorError::Journal(error),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::{Path, PathBuf},
        sync::atomic::{AtomicBool, AtomicUsize, Ordering},
        thread,
    };

    use tempfile::TempDir;
    use wcash_pool_protocol::{Hex108, BACKEND_PROTOCOL_VERSION};
    use zebra_chain::{block::Block, serialization::ZcashDeserializeInto, work::difficulty::U256};

    use super::*;
    use crate::coordinator::{validate_persisted_winner_block, PersistedWinnerBlockKind};

    const TEST_CHAIN_ID: u32 = 0x5743_4153;

    struct TestClock {
        now: Mutex<Duration>,
    }

    impl TestClock {
        fn new() -> Self {
            Self {
                now: Mutex::new(Duration::ZERO),
            }
        }

        fn advance(&self, duration: Duration) {
            let mut now = self.now.lock().expect("test clock mutex is not poisoned");
            *now = now
                .checked_add(duration)
                .expect("test clock duration remains bounded");
        }

        fn set(&self, duration: Duration) {
            *self.now.lock().expect("test clock mutex is not poisoned") = duration;
        }
    }

    impl ActorClock for TestClock {
        fn now(&self) -> Duration {
            *self.now.lock().expect("test clock mutex is not poisoned")
        }
    }

    struct TestRetainedJob {
        descriptor: JobDescriptor,
        wcash_payout_commitment: Hex32,
        zcash_payout_commitment: Hex32,
        healthy: AtomicBool,
        remaining_lifetime: Mutex<Option<Duration>>,
        validated: Mutex<PoolBackendValidatedShare>,
        validations: AtomicUsize,
    }

    impl TestRetainedJob {
        fn new(descriptor: JobDescriptor) -> Self {
            let remaining_lifetime = Some(Duration::from_millis(u64::from(descriptor.max_age_ms)));
            Self {
                descriptor,
                wcash_payout_commitment: Hex32::new([0x33; 32]),
                zcash_payout_commitment: Hex32::new([0x44; 32]),
                healthy: AtomicBool::new(true),
                remaining_lifetime: Mutex::new(remaining_lifetime),
                validated: Mutex::new(PoolBackendValidatedShare::ordinary()),
                validations: AtomicUsize::new(0),
            }
        }

        fn set_remaining_lifetime(&self, remaining: Option<Duration>) {
            *self
                .remaining_lifetime
                .lock()
                .expect("test lifetime mutex is not poisoned") = remaining;
        }

        fn set_validated(&self, validated: PoolBackendValidatedShare) {
            *self
                .validated
                .lock()
                .expect("test validation mutex is not poisoned") = validated;
        }

        fn with_payout_commitments(mut self, wcash: Hex32, zcash: Hex32) -> Self {
            self.wcash_payout_commitment = wcash;
            self.zcash_payout_commitment = zcash;
            self
        }
    }

    impl PoolBackendRetainedJob for TestRetainedJob {
        fn descriptor(&self) -> JobDescriptor {
            self.descriptor.clone()
        }

        fn wcash_payout_commitment(&self) -> Hex32 {
            self.wcash_payout_commitment.clone()
        }

        fn zcash_payout_commitment(&self) -> Hex32 {
            self.zcash_payout_commitment.clone()
        }

        fn remaining_lifetime(&self) -> Option<Duration> {
            *self
                .remaining_lifetime
                .lock()
                .expect("test lifetime mutex is not poisoned")
        }

        fn is_healthy(&self) -> bool {
            self.healthy.load(Ordering::Acquire)
        }

        fn validate_share(
            &self,
            _share: PoolBackendShareRequest<'_>,
        ) -> Result<PoolBackendValidatedShare, PoolBackendShareValidationError> {
            self.validations.fetch_add(1, Ordering::AcqRel);
            Ok(self
                .validated
                .lock()
                .expect("test validation mutex is not poisoned")
                .clone())
        }
    }

    #[derive(Clone)]
    struct TestJournalConfig {
        backend_instance: CanonicalUuid,
        wcash_genesis: Hex32,
        zcash_genesis: Hex32,
        wcash_payout_commitment: Hex32,
        zcash_payout_commitment: Hex32,
    }

    fn uuid(last: u8) -> CanonicalUuid {
        serde_json::from_value(serde_json::Value::String(format!(
            "00000000-0000-4000-8000-{last:012x}"
        )))
        .expect("canonical non-nil test UUID")
    }

    fn private_temp_dir() -> TempDir {
        let canonical_temporary_root =
            fs::canonicalize(std::env::temp_dir()).expect("canonical temporary root");
        let directory = tempfile::Builder::new()
            .tempdir_in(canonical_temporary_root)
            .expect("temporary directory");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700))
                .expect("make temporary directory private");
        }
        directory
    }

    fn config() -> TestJournalConfig {
        TestJournalConfig {
            backend_instance: uuid(1),
            wcash_genesis: Hex32::new([0x11; 32]),
            zcash_genesis: Hex32::new([0x22; 32]),
            wcash_payout_commitment: Hex32::new([0x33; 32]),
            zcash_payout_commitment: Hex32::new([0x44; 32]),
        }
    }

    fn create_journal(path: &Path, config: &TestJournalConfig) -> PoolBackendJournal {
        PoolBackendJournal::create_new(
            path,
            config.backend_instance,
            config.wcash_genesis.clone(),
            config.zcash_genesis.clone(),
            config.wcash_payout_commitment.clone(),
            config.zcash_payout_commitment.clone(),
            TEST_CHAIN_ID,
        )
        .expect("create actor test journal")
    }

    fn open_journal(path: &Path, config: &TestJournalConfig) -> PoolBackendJournal {
        PoolBackendJournal::open_existing(
            path,
            config.backend_instance,
            config.wcash_genesis.clone(),
            config.zcash_genesis.clone(),
            config.wcash_payout_commitment.clone(),
            config.zcash_payout_commitment.clone(),
            TEST_CHAIN_ID,
        )
        .expect("open actor test journal")
    }

    fn job(byte: u8) -> JobDescriptor {
        let mut header_input = [byte; 108];
        header_input[..4].copy_from_slice(&4u32.to_le_bytes());
        header_input[4..36].copy_from_slice(&[byte.wrapping_add(1); 32]);
        header_input[100..104].copy_from_slice(&1u32.to_le_bytes());
        JobDescriptor {
            job_id: Hex32::new([byte; 32]),
            wcash_candidate_hash_le: Hex32::new([byte.wrapping_add(2); 32]),
            header_input: Hex108::new(header_input),
            wcash_previous_hash_le: Hex32::new([byte.wrapping_add(3); 32]),
            zcash_previous_hash_le: Hex32::new([byte.wrapping_add(1); 32]),
            wcash_coinbase_txid_le: Hex32::new([byte.wrapping_add(4); 32]),
            zcash_coinbase_txid_le: Hex32::new([byte.wrapping_add(5); 32]),
            wcash_target_le: TargetLe::new([0xfe; 32]),
            zcash_target_le: TargetLe::new([0xfd; 32]),
            wcash_height: u32::from(byte),
            zcash_height: u32::from(byte).saturating_add(100),
            wcash_reward_zat: 625_000_000,
            zcash_reward_zat: 312_500_000,
            wcash_maturity_confirmations: 100,
            zcash_maturity_confirmations: 100,
            max_age_ms: 60_000,
        }
    }

    fn job_on_same_tips(byte: u8, previous: &JobDescriptor) -> JobDescriptor {
        let mut descriptor = job(byte);
        descriptor.wcash_previous_hash_le = previous.wcash_previous_hash_le.clone();
        descriptor.zcash_previous_hash_le = previous.zcash_previous_hash_le.clone();
        let mut header_input = *descriptor.header_input.as_bytes();
        header_input[4..36].copy_from_slice(previous.zcash_previous_hash_le.as_bytes());
        descriptor.header_input = Hex108::new(header_input);
        descriptor
    }

    fn target_policy() -> PoolShareTargetPolicy {
        PoolShareTargetPolicy::new(TargetLe::new([0xff; 32]))
            .expect("test operator target includes both network targets")
    }

    fn actor(path: &Path, config: &TestJournalConfig, clock: Arc<TestClock>) -> PoolBackendActor {
        PoolBackendActor::new_with_clock(create_journal(path, config), target_policy(), clock)
            .expect("construct actor")
    }

    fn actor_from_journal(journal: PoolBackendJournal, clock: Arc<TestClock>) -> PoolBackendActor {
        PoolBackendActor::new_with_clock(journal, target_policy(), clock)
            .expect("construct actor from retained winner journal")
    }

    fn wcash_winner_journal(
        path: &Path,
        config: &TestJournalConfig,
    ) -> (PoolBackendJournal, WinnerDescriptor, Vec<u8>) {
        let journal = create_journal(path, config);
        let block_bytes =
            hex::decode(include_str!("vectors/wcash-valid-auxpow-block-nonzero-prev.txt").trim())
                .expect("valid Wcash AuxPoW fixture is hex");
        let block: Block = block_bytes
            .as_slice()
            .zcash_deserialize_into()
            .expect("valid Wcash AuxPoW fixture decodes");
        let block_hash = block.hash().0;
        let coinbase_txid = block.transactions[0].hash().0;
        let expanded_target: U256 = block
            .header
            .difficulty_threshold
            .to_expanded()
            .expect("Wcash fixture compact target is valid")
            .into();
        let mut display_hash = block_hash;
        display_hash.reverse();
        let parent_hash = validate_persisted_winner_block(
            &block_bytes,
            &hex::encode(display_hash),
            1,
            PersistedWinnerBlockKind::Wcash,
        )
        .expect("Wcash fixture carries a valid exact AuxPoW parent");

        let mut descriptor = job(9);
        descriptor.wcash_candidate_hash_le = Hex32::new(block_hash);
        descriptor.wcash_previous_hash_le = Hex32::new(block.header.previous_block_hash.0);
        descriptor.wcash_coinbase_txid_le = Hex32::new(coinbase_txid);
        descriptor.wcash_target_le = TargetLe::new(expanded_target.to_little_endian());
        descriptor.wcash_height = 1;
        descriptor.wcash_maturity_confirmations = 2;
        journal
            .append_event(BackendEvent::JobActivated {
                event_seq: 0,
                job: descriptor.clone(),
            })
            .expect("activate exact Wcash fixture job");
        let winner = WinnerDescriptor {
            chain: MergedChain::Wcash,
            block_hash_le: Hex32::new(block_hash),
            height: 1,
            coinbase_txid_le: Hex32::new(coinbase_txid),
            reward_zat: descriptor.wcash_reward_zat,
            maturity_confirmations: 2,
        };
        journal
            .append_share_committed(
                descriptor.job_id,
                Hex32::new([0x81; 32]),
                Hex32::new(parent_hash),
                vec![winner.clone()],
                identity(),
                TargetLe::new([0xff; 32]),
                JournalWinnerBlocks {
                    wcash: Some(block_bytes.clone()),
                    zcash: None,
                },
            )
            .expect("commit exact Wcash fixture winner");
        (journal, winner, block_bytes)
    }

    fn zcash_winner_journal(
        path: &Path,
        config: &TestJournalConfig,
    ) -> (PoolBackendJournal, WinnerDescriptor, Vec<u8>) {
        let journal = create_journal(path, config);
        let (winner, block_bytes) = append_zcash_winner(&journal, Hex32::new([0x82; 32]));
        (journal, winner, block_bytes)
    }

    fn append_zcash_winner(
        journal: &PoolBackendJournal,
        share_id: Hex32,
    ) -> (WinnerDescriptor, Vec<u8>) {
        let block_bytes = hex::decode(
            include_str!("../../zebra-test/src/vectors/block-main-0-000-001.txt").trim(),
        )
        .expect("valid Zcash block-one fixture is hex");
        let block: Block = block_bytes
            .as_slice()
            .zcash_deserialize_into()
            .expect("valid Zcash block-one fixture decodes");
        let block_hash = block.hash().0;
        let coinbase_txid = block.transactions[0].hash().0;
        let expanded_target: U256 = block
            .header
            .difficulty_threshold
            .to_expanded()
            .expect("Zcash fixture compact target is valid")
            .into();

        let mut descriptor = job(10);
        descriptor.zcash_previous_hash_le = Hex32::new(block.header.previous_block_hash.0);
        descriptor.zcash_coinbase_txid_le = Hex32::new(coinbase_txid);
        descriptor.zcash_target_le = TargetLe::new(expanded_target.to_little_endian());
        descriptor.zcash_height = 1;
        descriptor.zcash_maturity_confirmations = 2;
        let mut header_input = descriptor.header_input.into_bytes();
        header_input[4..36].copy_from_slice(&block.header.previous_block_hash.0);
        descriptor.header_input = Hex108::new(header_input);
        journal
            .append_event(BackendEvent::JobActivated {
                event_seq: 0,
                job: descriptor.clone(),
            })
            .expect("activate exact Zcash fixture job");
        let winner = WinnerDescriptor {
            chain: MergedChain::Zcash,
            block_hash_le: Hex32::new(block_hash),
            height: 1,
            coinbase_txid_le: Hex32::new(coinbase_txid),
            reward_zat: descriptor.zcash_reward_zat,
            maturity_confirmations: 2,
        };
        journal
            .append_share_committed(
                descriptor.job_id,
                share_id,
                Hex32::new(block_hash),
                vec![winner.clone()],
                identity(),
                TargetLe::new([0xff; 32]),
                JournalWinnerBlocks {
                    wcash: None,
                    zcash: Some(block_bytes.clone()),
                },
            )
            .expect("commit exact Zcash fixture winner");
        (winner, block_bytes)
    }

    fn identity() -> WorkerIdentity {
        WorkerIdentity {
            account_id: uuid(10),
            worker_id: uuid(11),
            label: "worker-1".to_string(),
        }
    }

    fn submit_request(id: u64, descriptor: &JobDescriptor) -> BackendRequest {
        BackendRequest::SubmitShare {
            version: BACKEND_PROTOCOL_VERSION,
            id,
            job_id: descriptor.job_id.clone(),
            identity: identity(),
            target_le: TargetLe::new([0xff; 32]),
            time: Hex4::new(
                descriptor.header_input.as_bytes()[100..104]
                    .try_into()
                    .expect("header time has four bytes"),
            ),
            nonce: Hex32::new([0x51; 32]),
            solution: Box::new(Hex1344::new([0x52; 1344])),
        }
    }

    fn zcash_winner_fixture(byte: u8) -> (JobDescriptor, BackendRequest, Vec<u8>) {
        let block_bytes = hex::decode(
            include_str!("../../zebra-test/src/vectors/block-main-0-000-001.txt").trim(),
        )
        .expect("Zcash block-one fixture is hex");
        let block: Block = block_bytes
            .as_slice()
            .zcash_deserialize_into()
            .expect("Zcash block-one fixture decodes");
        let mut descriptor = job(byte);
        descriptor.header_input = Hex108::new(
            block_bytes[..108]
                .try_into()
                .expect("Zcash v4 header input has 108 bytes"),
        );
        descriptor.zcash_previous_hash_le = Hex32::new(block.header.previous_block_hash.0);
        descriptor.zcash_coinbase_txid_le = Hex32::new(block.transactions[0].hash().0);
        descriptor.zcash_height = 1;
        descriptor.zcash_maturity_confirmations = 1;
        let expanded_target: U256 = block
            .header
            .difficulty_threshold
            .to_expanded()
            .expect("fixture compact target is valid")
            .into();
        descriptor.zcash_target_le = TargetLe::new(expanded_target.to_little_endian());

        assert_eq!(&block_bytes[140..143], &[0xfd, 0x40, 0x05]);
        let request = BackendRequest::SubmitShare {
            version: BACKEND_PROTOCOL_VERSION,
            id: 2,
            job_id: descriptor.job_id.clone(),
            identity: identity(),
            target_le: TargetLe::new([0xff; 32]),
            time: Hex4::new(
                descriptor.header_input.as_bytes()[100..104]
                    .try_into()
                    .expect("header time has four bytes"),
            ),
            nonce: Hex32::new(
                block_bytes[108..140]
                    .try_into()
                    .expect("Zcash nonce has 32 bytes"),
            ),
            solution: Box::new(Hex1344::new(
                block_bytes[143..1487]
                    .try_into()
                    .expect("Zcash Equihash solution has 1344 bytes"),
            )),
        };

        (descriptor, request, block_bytes)
    }

    fn snapshot(actor: &PoolBackendActor, after_event_seq: u64) -> BackendMessage {
        actor
            .dispatch(
                uuid(20),
                None,
                BackendRequestKind::SubscribeJobs,
                BackendRequest::SubscribeJobs {
                    version: BACKEND_PROTOCOL_VERSION,
                    id: 1,
                    after_event_seq,
                },
            )
            .expect("snapshot succeeds")
            .pop()
            .expect("snapshot response exists")
    }

    #[test]
    fn hello_replay_health_and_snapshot_share_one_watermark() {
        let directory = private_temp_dir();
        let path = directory.path().join("actor.journal");
        let config = config();
        let clock = Arc::new(TestClock::new());
        let actor = actor(&path, &config, clock);

        let hello = actor
            .dispatch(
                uuid(20),
                None,
                BackendRequestKind::Hello,
                BackendRequest::Hello {
                    version: BACKEND_PROTOCOL_VERSION,
                    id: 1,
                    pool_instance: uuid(21),
                    last_event_seq: 0,
                },
            )
            .expect("hello succeeds");
        assert!(matches!(
            hello.as_slice(),
            [BackendMessage::HelloOk {
                current_event_seq: 0,
                ..
            }]
        ));

        assert!(matches!(
            snapshot(&actor, 0),
            BackendMessage::JobSnapshot {
                event_seq: 0,
                current: None,
                ref recent,
                ..
            } if recent.is_empty()
        ));
        let health = actor
            .dispatch(
                uuid(20),
                None,
                BackendRequestKind::Health,
                BackendRequest::Health {
                    version: BACKEND_PROTOCOL_VERSION,
                    id: 2,
                },
            )
            .expect("health succeeds");
        assert!(matches!(
            health.as_slice(),
            [BackendMessage::HealthStatus {
                event_seq: 0,
                healthy: false,
                ..
            }]
        ));
        let page = actor
            .dispatch(
                uuid(20),
                None,
                BackendRequestKind::ReadEvents,
                BackendRequest::ReadEvents {
                    version: BACKEND_PROTOCOL_VERSION,
                    id: 3,
                    after_event_seq: 0,
                    limit: 10,
                },
            )
            .expect("empty replay succeeds");
        assert!(matches!(
            page.as_slice(),
            [BackendMessage::EventsPage {
                next_event_seq: 0,
                complete: true,
                events,
                ..
            }] if events.is_empty()
        ));
    }

    #[test]
    fn snapshot_never_changes_job_roles_at_one_watermark() {
        let directory = private_temp_dir();
        let path = directory.path().join("actor.journal");
        let config = config();
        let actor = actor(&path, &config, Arc::new(TestClock::new()));
        let descriptor = job(1);
        let retained = Arc::new(TestRetainedJob::new(descriptor.clone()));
        actor
            .activate_job(retained.clone(), Duration::from_secs(5))
            .expect("activate healthy job");
        assert!(matches!(
            snapshot(&actor, 1),
            BackendMessage::JobSnapshot {
                event_seq: 1,
                current: Some(AcceptableJob { job, .. }),
                ..
            } if job.job_id == descriptor.job_id
        ));

        retained.healthy.store(false, Ordering::Release);
        let error = actor
            .dispatch(
                uuid(20),
                None,
                BackendRequestKind::SubscribeJobs,
                BackendRequest::SubscribeJobs {
                    version: BACKEND_PROTOCOL_VERSION,
                    id: 2,
                    after_event_seq: 1,
                },
            )
            .expect_err("unhealthy validator cannot silently alter a snapshot");
        assert_eq!(
            error,
            unhealthy_backend("retained job validator requires durable invalidation")
        );
        assert_eq!(
            actor
                .lock_state()
                .expect("actor mutex")
                .journal
                .current_event_seq()
                .expect("journal watermark"),
            1
        );

        retained.healthy.store(true, Ordering::Release);
        assert!(matches!(
            snapshot(&actor, 1),
            BackendMessage::JobSnapshot {
                event_seq: 1,
                current: Some(AcceptableJob { job, .. }),
                ..
            } if job.job_id == descriptor.job_id
        ));
    }

    #[test]
    fn activation_uses_remaining_native_lifetime_without_restarting_it() {
        let directory = private_temp_dir();
        let path = directory.path().join("actor.journal");
        let config = config();
        let clock = Arc::new(TestClock::new());
        let actor = actor(&path, &config, Arc::clone(&clock));
        let descriptor = job(1);
        let retained = Arc::new(TestRetainedJob::new(descriptor));
        retained.set_remaining_lifetime(Some(Duration::from_millis(1_234)));

        let activated = actor
            .activate_job(retained, Duration::from_secs(5))
            .expect("remaining native lifetime activates the job");
        assert!(matches!(
            activated,
            BackendEvent::JobActivated { job, .. } if job.max_age_ms == 1_234
        ));
        assert!(matches!(
            snapshot(&actor, 0),
            BackendMessage::JobSnapshot {
                current: Some(AcceptableJob {
                    job,
                    accept_for_ms: 1_234,
                    ..
                }),
                ..
            } if job.max_age_ms == 1_234
        ));

        clock.advance(Duration::from_millis(1_234));
        assert!(matches!(
            snapshot(&actor, 0),
            BackendMessage::JobSnapshot { current: None, .. }
        ));
    }

    #[test]
    fn activation_rejects_a_job_for_another_payout_authority_without_journal_mutation() {
        let directory = private_temp_dir();
        let path = directory.path().join("actor.journal");
        let config = config();
        let actor = actor(&path, &config, Arc::new(TestClock::new()));
        let retained = Arc::new(TestRetainedJob::new(job(1)).with_payout_commitments(
            Hex32::new([0x99; 32]),
            config.zcash_payout_commitment.clone(),
        ));

        assert!(matches!(
            actor.activate_job(retained, Duration::from_secs(5)),
            Err(PoolBackendActorError::PayoutAuthorityMismatch)
        ));
        assert_eq!(
            actor
                .lock_state()
                .expect("actor mutex")
                .journal
                .current_event_seq()
                .expect("journal watermark"),
            0
        );
    }

    #[test]
    fn exhausted_native_lifetime_cannot_mutate_the_journal() {
        for remaining in [None, Some(Duration::from_micros(999))] {
            let directory = private_temp_dir();
            let path = directory.path().join("actor.journal");
            let config = config();
            let actor = actor(&path, &config, Arc::new(TestClock::new()));
            let retained = Arc::new(TestRetainedJob::new(job(1)));
            retained.set_remaining_lifetime(remaining);

            assert!(matches!(
                actor.activate_job(retained, Duration::from_secs(5)),
                Err(PoolBackendActorError::RetainedJobUnhealthy)
            ));
            assert_eq!(
                actor
                    .lock_state()
                    .expect("actor mutex")
                    .journal
                    .current_event_seq()
                    .expect("journal watermark"),
                0
            );
        }
    }

    #[test]
    fn unhealthy_validator_cannot_be_activated() {
        let directory = private_temp_dir();
        let path = directory.path().join("actor.journal");
        let config = config();
        let actor = actor(&path, &config, Arc::new(TestClock::new()));
        let retained = Arc::new(TestRetainedJob::new(job(1)));
        retained.healthy.store(false, Ordering::Release);

        assert!(matches!(
            actor.activate_job(retained, Duration::from_secs(5)),
            Err(PoolBackendActorError::RetainedJobUnhealthy)
        ));
        assert_eq!(
            actor
                .lock_state()
                .expect("actor mutex")
                .journal
                .current_event_seq()
                .expect("journal watermark"),
            0
        );
    }

    #[test]
    fn unhealthy_status_cannot_suppress_an_exact_retained_winner() {
        let directory = private_temp_dir();
        let path = directory.path().join("actor.journal");
        let config = config();
        let actor = actor(&path, &config, Arc::new(TestClock::new()));
        let (descriptor, request, block_bytes) = zcash_winner_fixture(9);
        let retained = Arc::new(TestRetainedJob::new(descriptor));
        retained.set_validated(PoolBackendValidatedShare::with_winners(
            None,
            Some(block_bytes),
        ));
        actor
            .activate_job(retained.clone(), Duration::from_secs(5))
            .expect("activate exact winner fixture");

        retained.healthy.store(false, Ordering::Release);
        let response = actor
            .dispatch(uuid(20), Some(1), BackendRequestKind::SubmitShare, request)
            .expect("winner validation runs despite unhealthy status");
        assert!(matches!(
            response.last(),
            Some(BackendMessage::ShareCommitted {
                receipt: ShareReceipt { winners, .. },
                replayed: false,
                ..
            }) if matches!(winners.as_slice(), [WinnerDescriptor { chain: MergedChain::Zcash, .. }])
        ));
        assert_eq!(retained.validations.load(Ordering::Acquire), 1);
        assert!(matches!(
            actor
                .dispatch(
                    uuid(20),
                    None,
                    BackendRequestKind::Health,
                    BackendRequest::Health {
                        version: BACKEND_PROTOCOL_VERSION,
                        id: 3,
                    },
                )
                .expect("health response succeeds")
                .as_slice(),
            [BackendMessage::HealthStatus { healthy: false, .. }]
        ));
    }

    #[test]
    fn rotation_retains_two_jobs_and_closes_expired_grace() {
        let directory = private_temp_dir();
        let path = directory.path().join("actor.journal");
        let config = config();
        let clock = Arc::new(TestClock::new());
        let actor = actor(&path, &config, Arc::clone(&clock));
        let first = job(1);
        let second = job_on_same_tips(2, &first);
        let first_validator = Arc::new(TestRetainedJob::new(first.clone()));
        actor
            .activate_job(first_validator.clone(), Duration::from_secs(5))
            .expect("activate first job");
        actor
            .activate_job(
                Arc::new(TestRetainedJob::new(second.clone())),
                Duration::from_secs(5),
            )
            .expect("rotate to second job");

        assert!(matches!(
            snapshot(&actor, 0),
            BackendMessage::JobSnapshot {
                event_seq: 3,
                current: Some(AcceptableJob { job, .. }),
                recent,
                ..
            } if job.job_id == second.job_id
                && recent.len() == 1
                && recent[0].job.job_id == first.job_id
        ));

        first_validator.healthy.store(false, Ordering::Release);
        let health = actor
            .dispatch(
                uuid(20),
                None,
                BackendRequestKind::Health,
                BackendRequest::Health {
                    version: BACKEND_PROTOCOL_VERSION,
                    id: 2,
                },
            )
            .expect("health response succeeds");
        assert!(matches!(
            health.as_slice(),
            [BackendMessage::HealthStatus { healthy: false, .. }]
        ));
        first_validator.healthy.store(true, Ordering::Release);

        clock.advance(Duration::from_secs(5));
        assert!(matches!(
            snapshot(&actor, 0),
            BackendMessage::JobSnapshot {
                event_seq: 4,
                current: Some(AcceptableJob { job, .. }),
                recent,
                ..
            } if job.job_id == second.job_id && recent.is_empty()
        ));
    }

    #[test]
    fn tip_change_closes_current_and_all_grace_work_before_activation() {
        let directory = private_temp_dir();
        let path = directory.path().join("actor.journal");
        let config = config();
        let actor = actor(&path, &config, Arc::new(TestClock::new()));
        let first = job(1);
        let second = job_on_same_tips(2, &first);
        let changed = job(3);

        actor
            .activate_job(
                Arc::new(TestRetainedJob::new(first.clone())),
                Duration::from_secs(5),
            )
            .expect("activate first job");
        actor
            .activate_job(
                Arc::new(TestRetainedJob::new(second.clone())),
                Duration::from_secs(5),
            )
            .expect("retain first job as same-tip grace work");
        let activated = actor
            .activate_job(
                Arc::new(TestRetainedJob::new(changed.clone())),
                Duration::from_secs(5),
            )
            .expect("activate changed-tip job");
        assert_eq!(activated.event_seq(), 7);

        assert!(matches!(
            snapshot(&actor, 0),
            BackendMessage::JobSnapshot {
                event_seq: 7,
                current: Some(AcceptableJob { job, .. }),
                recent,
                ..
            } if job.job_id == changed.job_id && recent.is_empty()
        ));
        let events = actor
            .lock_state()
            .expect("actor mutex")
            .journal
            .read_events(0, 10)
            .expect("read tip-change lifecycle")
            .events;
        assert!(matches!(
            events.as_slice(),
            [
                BackendEvent::JobActivated { .. },
                BackendEvent::JobInvalidated {
                    reason: JobInvalidationReason::Superseded,
                    ..
                },
                BackendEvent::JobActivated { .. },
                BackendEvent::JobInvalidated {
                    reason: JobInvalidationReason::WcashTipChanged,
                    accept_for_ms: 0,
                    ..
                },
                BackendEvent::GenerationClosed { .. },
                BackendEvent::GenerationClosed { .. },
                BackendEvent::JobActivated { .. },
            ]
        ));

        let stale = actor
            .dispatch(
                uuid(20),
                Some(7),
                BackendRequestKind::SubmitShare,
                submit_request(2, &first),
            )
            .expect_err("old-tip grace work is terminally stale");
        assert_eq!(stale, stale_job());
    }

    #[test]
    fn observed_tip_change_closes_grace_work_before_replacement_exists() {
        let directory = private_temp_dir();
        let path = directory.path().join("actor.journal");
        let config = config();
        let actor = actor(&path, &config, Arc::new(TestClock::new()));
        let first = job(1);
        let second = job_on_same_tips(2, &first);

        actor
            .activate_job(
                Arc::new(TestRetainedJob::new(first.clone())),
                Duration::from_secs(5),
            )
            .expect("activate first job");
        actor
            .activate_job(
                Arc::new(TestRetainedJob::new(second.clone())),
                Duration::from_secs(5),
            )
            .expect("retain first job as same-tip grace work");

        let invalidation = actor
            .invalidate_job(
                &second.job_id,
                JobInvalidationReason::ZcashTipChanged,
                Duration::ZERO,
            )
            .expect("invalidate every job on the observed parent tip");
        assert!(matches!(
            invalidation.as_slice(),
            [
                BackendEvent::JobInvalidated {
                    reason: JobInvalidationReason::ZcashTipChanged,
                    accept_for_ms: 0,
                    ..
                },
                BackendEvent::GenerationClosed { .. },
                BackendEvent::GenerationClosed { .. },
            ]
        ));
        assert!(matches!(
            snapshot(&actor, 0),
            BackendMessage::JobSnapshot {
                event_seq: 6,
                current: None,
                recent,
                ..
            } if recent.is_empty()
        ));

        let stale = actor
            .dispatch(
                uuid(20),
                Some(6),
                BackendRequestKind::SubmitShare,
                submit_request(2, &first),
            )
            .expect_err("old-parent grace work is terminally stale");
        assert_eq!(stale, stale_job());
    }

    #[test]
    fn deadline_overflow_never_persists_job_activation() {
        let directory = private_temp_dir();
        let path = directory.path().join("actor.journal");
        let config = config();
        let clock = Arc::new(TestClock::new());
        let actor = actor(&path, &config, Arc::clone(&clock));
        clock.set(
            Duration::MAX
                .checked_sub(Duration::from_secs(1))
                .expect("one second is less than Duration::MAX"),
        );

        assert!(matches!(
            actor.activate_job(
                Arc::new(TestRetainedJob::new(job(1))),
                Duration::from_secs(5),
            ),
            Err(PoolBackendActorError::DeadlineOverflow)
        ));
        assert_eq!(
            actor
                .lock_state()
                .expect("actor mutex")
                .journal
                .current_event_seq()
                .expect("journal watermark"),
            0
        );
    }

    #[test]
    fn restart_closes_abandoned_generation_without_renewing_lease() {
        let directory = private_temp_dir();
        let path = directory.path().join("actor.journal");
        let config = config();
        let descriptor = job(1);
        {
            let actor = actor(&path, &config, Arc::new(TestClock::new()));
            actor
                .activate_job(
                    Arc::new(TestRetainedJob::new(descriptor.clone())),
                    Duration::from_secs(5),
                )
                .expect("activate generation before restart");
        }

        let recovered = PoolBackendActor::new_with_clock(
            open_journal(&path, &config),
            target_policy(),
            Arc::new(TestClock::new()),
        )
        .expect("recover actor");
        assert!(matches!(
            snapshot(&recovered, 0),
            BackendMessage::JobSnapshot {
                event_seq: 3,
                current: None,
                ref recent,
                ..
            } if recent.is_empty()
        ));
        assert_eq!(
            recovered
                .lock_state()
                .expect("actor mutex")
                .journal
                .job_descriptors()
                .expect("journal jobs"),
            vec![(descriptor, true)]
        );
        let events = recovered
            .lock_state()
            .expect("actor mutex")
            .journal
            .read_events(0, 10)
            .expect("read recovered lifecycle")
            .events;
        assert!(matches!(
            events.as_slice(),
            [
                BackendEvent::JobActivated { .. },
                BackendEvent::JobInvalidated {
                    reason: JobInvalidationReason::Age,
                    accept_for_ms: 0,
                    ..
                },
                BackendEvent::GenerationClosed { .. }
            ]
        ));
    }

    #[test]
    fn restart_replays_exact_share_without_retaining_old_work() {
        let directory = private_temp_dir();
        let path = directory.path().join("actor.journal");
        let config = config();
        let descriptor = job(1);
        let request = submit_request(2, &descriptor);
        let original_receipt = {
            let actor = actor(&path, &config, Arc::new(TestClock::new()));
            actor
                .activate_job(
                    Arc::new(TestRetainedJob::new(descriptor)),
                    Duration::from_secs(5),
                )
                .expect("activate generation before share");
            let messages = actor
                .dispatch(
                    uuid(20),
                    Some(1),
                    BackendRequestKind::SubmitShare,
                    request.clone(),
                )
                .expect("commit original share");
            match messages.last() {
                Some(BackendMessage::ShareCommitted {
                    receipt,
                    replayed: false,
                    ..
                }) => receipt.clone(),
                _ => panic!("fresh share returns one durable receipt"),
            }
        };

        let recovered = PoolBackendActor::new_with_clock(
            open_journal(&path, &config),
            target_policy(),
            Arc::new(TestClock::new()),
        )
        .expect("recover actor");
        let watermark = recovered
            .lock_state()
            .expect("actor mutex")
            .journal
            .current_event_seq()
            .expect("journal watermark");
        assert_eq!(watermark, 4);
        let replay = recovered
            .dispatch(
                uuid(21),
                Some(watermark),
                BackendRequestKind::SubmitShare,
                request,
            )
            .expect("exact historical retry succeeds");
        assert!(matches!(
            replay.as_slice(),
            [BackendMessage::ShareCommitted {
                receipt,
                replayed: true,
                ..
            }] if receipt == &original_receipt
        ));
    }

    #[test]
    fn concurrent_identical_share_is_validated_and_committed_once() {
        let directory = private_temp_dir();
        let path: PathBuf = directory.path().join("actor.journal");
        let config = config();
        let actor = Arc::new(actor(&path, &config, Arc::new(TestClock::new())));
        let descriptor = job(1);
        let retained = Arc::new(TestRetainedJob::new(descriptor.clone()));
        actor
            .activate_job(retained.clone(), Duration::from_secs(5))
            .expect("activate share job");
        let request = submit_request(2, &descriptor);

        let mut threads = Vec::new();
        for index in 0..8u8 {
            let actor = Arc::clone(&actor);
            let mut request = request.clone();
            if let BackendRequest::SubmitShare { id, .. } = &mut request {
                *id = u64::from(index).saturating_add(2);
            }
            threads.push(thread::spawn(move || {
                actor
                    .dispatch(
                        uuid(index.saturating_add(30)),
                        Some(1),
                        BackendRequestKind::SubmitShare,
                        request,
                    )
                    .expect("concurrent share succeeds")
            }));
        }

        let mut fresh = 0usize;
        let mut replayed = 0usize;
        for thread in threads {
            let messages = thread.join().expect("share thread does not panic");
            assert!(matches!(
                messages.first(),
                Some(BackendMessage::Event {
                    event: BackendEvent::ShareCommitted { .. },
                    ..
                })
            ));
            match messages.last() {
                Some(BackendMessage::ShareCommitted {
                    replayed: false, ..
                }) => fresh += 1,
                Some(BackendMessage::ShareCommitted { replayed: true, .. }) => replayed += 1,
                _ => panic!("share response has the required terminal message"),
            }
        }
        assert_eq!(fresh, 1);
        assert_eq!(replayed, 7);
        assert_eq!(retained.validations.load(Ordering::Acquire), 1);
        assert_eq!(
            actor
                .lock_state()
                .expect("actor mutex")
                .journal
                .current_event_seq()
                .expect("journal watermark"),
            2
        );
    }

    #[test]
    fn wcash_winner_cas_covers_quarantine_requeue_maturity_and_restart() {
        let directory = private_temp_dir();
        let path = directory.path().join("actor.journal");
        let config = config();
        let (journal, winner, block_bytes) = wcash_winner_journal(&path, &config);
        let actor = actor_from_journal(journal, Arc::new(TestClock::new()));

        let pending = actor
            .next_winner_snapshot(None)
            .expect("enumerate winner")
            .expect("Wcash winner is retained");
        assert_eq!(pending.winner(), &winner);
        assert_eq!(pending.block_bytes(), block_bytes);
        assert_eq!(pending.revision_event_seq(), 2);
        assert!(matches!(
            pending.lifecycle(),
            JournalWinnerLifecycle::Pending
        ));
        let key = pending.key();
        assert!(actor
            .next_winner_snapshot(Some(&key))
            .expect("advance bounded winner pass")
            .is_none());
        let debug = format!("{pending:?}");
        assert!(debug.contains(&format!("block_bytes_len: {}", block_bytes.len())));
        assert!(!debug.contains(&hex::encode(&block_bytes[..16])));

        let winner_tip = ChainTip {
            block_hash_le: winner.block_hash_le.clone(),
            height: winner.height,
        };
        let quarantined_event = actor
            .compare_and_apply_winner_transition(
                &pending,
                PoolBackendWinnerTransition::Quarantined {
                    tip: winner_tip.clone(),
                },
            )
            .expect("quarantine exact conflicting Wcash witness");
        assert!(matches!(
            quarantined_event,
            BackendEvent::WinnerQuarantined { event_seq: 5, .. }
        ));

        assert!(matches!(
            actor.compare_and_apply_winner_transition(
                &pending,
                PoolBackendWinnerTransition::Observed {
                    tip: winner_tip.clone(),
                    confirmations: 1,
                },
            ),
            Err(PoolBackendActorError::WinnerRevisionConflict)
        ));

        let quarantined = actor
            .winner_snapshot(&key)
            .expect("refresh quarantined winner")
            .expect("winner remains retained");
        assert_eq!(quarantined.revision_event_seq(), 5);
        assert!(matches!(
            quarantined.lifecycle(),
            JournalWinnerLifecycle::Quarantined { tip } if tip == &winner_tip
        ));

        let replacement_tip = ChainTip {
            block_hash_le: Hex32::new([0xa1; 32]),
            height: winner.height + 1,
        };
        actor
            .compare_and_apply_winner_transition(
                &quarantined,
                PoolBackendWinnerTransition::Requeued {
                    tip: replacement_tip.clone(),
                },
            )
            .expect("release exact Wcash block for resubmission");
        let requeued = actor
            .winner_snapshot(&key)
            .expect("refresh requeued winner")
            .expect("winner remains retained");
        assert!(matches!(
            requeued.lifecycle(),
            JournalWinnerLifecycle::Requeued { tip } if tip == &replacement_tip
        ));

        actor
            .compare_and_apply_winner_transition(
                &requeued,
                PoolBackendWinnerTransition::Observed {
                    tip: winner_tip,
                    confirmations: 1,
                },
            )
            .expect("observe exact Wcash winner");
        let observed = actor
            .winner_snapshot(&key)
            .expect("refresh observed winner")
            .expect("winner remains retained");
        actor
            .compare_and_apply_winner_transition(
                &observed,
                PoolBackendWinnerTransition::Matured {
                    tip: replacement_tip.clone(),
                    confirmations: 2,
                },
            )
            .expect("mature observed Wcash reward");
        drop(actor);

        let recovered =
            actor_from_journal(open_journal(&path, &config), Arc::new(TestClock::new()));
        let matured = recovered
            .winner_snapshot(&key)
            .expect("read winner after restart")
            .expect("matured winner remains retained");
        assert_eq!(matured.block_bytes(), block_bytes);
        assert_eq!(matured.revision_event_seq(), 8);
        assert!(matches!(
            matured.lifecycle(),
            JournalWinnerLifecycle::Matured {
                tip,
                confirmations: 2,
            } if tip == &replacement_tip
        ));
        let orphan_tip = ChainTip {
            block_hash_le: Hex32::new([0xa2; 32]),
            height: winner.height + 2,
        };
        recovered
            .compare_and_apply_winner_transition(
                &matured,
                PoolBackendWinnerTransition::Orphaned {
                    tip: orphan_tip.clone(),
                },
            )
            .expect("deep reorganization can orphan matured Wcash reward");
        let orphaned = recovered
            .winner_snapshot(&key)
            .expect("refresh orphaned winner")
            .expect("orphaned winner remains retained");
        assert_eq!(orphaned.revision_event_seq(), 9);
        assert!(matches!(
            orphaned.lifecycle(),
            JournalWinnerLifecycle::Orphaned { tip } if tip == &orphan_tip
        ));
    }

    #[test]
    fn mixed_winner_journal_order_cursor_is_exact_and_replay_stable() {
        let directory = private_temp_dir();
        let path = directory.path().join("actor.journal");
        let config = config();
        let (journal, wcash_winner, wcash_block) = wcash_winner_journal(&path, &config);
        let (zcash_winner, zcash_block) = append_zcash_winner(&journal, Hex32::new([0x01; 32]));

        let (first_ordinal, first) = journal
            .next_winner_state(None)
            .expect("begin mixed winner pass")
            .expect("Wcash winner is first in journal order");
        let (second_ordinal, second) = journal
            .next_winner_state(Some(first_ordinal))
            .expect("advance mixed winner pass")
            .expect("Zcash winner is second in journal order");
        assert_eq!((first_ordinal, second_ordinal), (0, 1));
        assert_eq!(first.winner, wcash_winner);
        assert_eq!(first.block_bytes, wcash_block);
        assert_eq!(second.winner, zcash_winner);
        assert_eq!(second.block_bytes, zcash_block);
        assert!(first.share_id.as_bytes() > second.share_id.as_bytes());
        assert!(journal
            .next_winner_state(Some(second_ordinal))
            .expect("finish mixed winner pass")
            .is_none());
        drop(journal);

        let reopened = open_journal(&path, &config);
        let (replayed_first_ordinal, replayed_first) = reopened
            .next_winner_state(None)
            .expect("begin replayed mixed winner pass")
            .expect("replayed Wcash winner is retained");
        let (replayed_second_ordinal, replayed_second) = reopened
            .next_winner_state(Some(replayed_first_ordinal))
            .expect("advance replayed mixed winner pass")
            .expect("replayed Zcash winner is retained");
        assert_eq!(
            (replayed_first_ordinal, replayed_second_ordinal),
            (first_ordinal, second_ordinal)
        );
        assert_eq!(replayed_first, first);
        assert_eq!(replayed_second, second);
        assert!(reopened
            .next_winner_state(Some(replayed_second_ordinal))
            .expect("finish replayed mixed winner pass")
            .is_none());
    }

    #[test]
    fn zcash_winner_rejects_invalid_and_concurrent_stale_cas_transitions() {
        let directory = private_temp_dir();
        let path = directory.path().join("actor.journal");
        let config = config();
        let (journal, winner, _) = zcash_winner_journal(&path, &config);
        let actor = Arc::new(actor_from_journal(journal, Arc::new(TestClock::new())));
        let pending = actor
            .next_winner_snapshot(None)
            .expect("enumerate winner")
            .expect("Zcash winner is retained");
        let other_directory = private_temp_dir();
        let other = actor_from_journal(
            create_journal(&other_directory.path().join("other-actor.journal"), &config),
            Arc::new(TestClock::new()),
        );
        assert!(matches!(
            other.next_winner_snapshot(Some(&pending.key())),
            Err(PoolBackendActorError::WinnerAuthorityMismatch)
        ));
        assert!(matches!(
            other.winner_snapshot(&pending.key()),
            Err(PoolBackendActorError::WinnerAuthorityMismatch)
        ));
        assert!(matches!(
            other.compare_and_apply_winner_transition(
                &pending,
                PoolBackendWinnerTransition::Observed {
                    tip: ChainTip {
                        block_hash_le: winner.block_hash_le.clone(),
                        height: winner.height,
                    },
                    confirmations: 1,
                },
            ),
            Err(PoolBackendActorError::WinnerAuthorityMismatch)
        ));
        let winner_tip = ChainTip {
            block_hash_le: winner.block_hash_le.clone(),
            height: winner.height,
        };

        assert!(matches!(
            actor.compare_and_apply_winner_transition(
                &pending,
                PoolBackendWinnerTransition::Quarantined {
                    tip: winner_tip.clone(),
                },
            ),
            Err(PoolBackendActorError::Journal(
                PoolBackendJournalError::InvalidEvent { .. }
            ))
        ));
        assert!(matches!(
            actor.compare_and_apply_winner_transition(
                &pending,
                PoolBackendWinnerTransition::Observed {
                    tip: winner_tip.clone(),
                    confirmations: 2,
                },
            ),
            Err(PoolBackendActorError::Journal(
                PoolBackendJournalError::InvalidEvent { .. }
            ))
        ));
        assert_eq!(
            actor
                .winner_snapshot(&pending.key())
                .expect("refresh after rejected transitions")
                .expect("winner remains retained")
                .revision_event_seq(),
            2
        );

        let expected = Arc::new(pending);
        let mut threads = Vec::new();
        for _ in 0..2 {
            let actor = Arc::clone(&actor);
            let expected = Arc::clone(&expected);
            let tip = winner_tip.clone();
            threads.push(thread::spawn(move || {
                actor.compare_and_apply_winner_transition(
                    &expected,
                    PoolBackendWinnerTransition::Observed {
                        tip,
                        confirmations: 1,
                    },
                )
            }));
        }
        let mut committed = 0usize;
        let mut stale = 0usize;
        for thread in threads {
            match thread.join().expect("CAS thread does not panic") {
                Ok(BackendEvent::WinnerObserved { event_seq: 5, .. }) => committed += 1,
                Err(PoolBackendActorError::WinnerRevisionConflict) => stale += 1,
                result => panic!("unexpected concurrent CAS result: {result:?}"),
            }
        }
        assert_eq!((committed, stale), (1, 1));

        let key = expected.key();
        let observed = actor
            .winner_snapshot(&key)
            .expect("refresh observed Zcash winner")
            .expect("winner remains retained");
        assert!(matches!(
            observed.lifecycle(),
            JournalWinnerLifecycle::Observed {
                tip,
                confirmations: 1,
            } if tip == &winner_tip
        ));
        let mature_tip = ChainTip {
            block_hash_le: Hex32::new([0xb1; 32]),
            height: winner.height + 1,
        };
        actor
            .compare_and_apply_winner_transition(
                &observed,
                PoolBackendWinnerTransition::Matured {
                    tip: mature_tip.clone(),
                    confirmations: 2,
                },
            )
            .expect("mature observed Zcash reward");
        let matured = actor
            .winner_snapshot(&key)
            .expect("refresh matured Zcash winner")
            .expect("winner remains retained");
        actor
            .compare_and_apply_winner_transition(
                &matured,
                PoolBackendWinnerTransition::Orphaned {
                    tip: ChainTip {
                        block_hash_le: Hex32::new([0xb2; 32]),
                        height: winner.height + 2,
                    },
                },
            )
            .expect("deep reorganization can orphan matured Zcash reward");
        assert!(matches!(
            actor
                .winner_snapshot(&key)
                .expect("refresh orphaned Zcash winner")
                .expect("winner remains retained")
                .lifecycle(),
            JournalWinnerLifecycle::Orphaned { .. }
        ));
    }
}
