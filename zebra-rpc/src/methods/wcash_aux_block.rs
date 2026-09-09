//! Bounded, exact-candidate storage for Wcash AuxPoW mining.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use rand::{rngs::OsRng, RngCore};
use subtle::ConstantTimeEq;
use zebra_chain::{
    block::{self, Block},
    work::equihash::Solution,
};
use zebra_consensus::{BlockError, RouterError, VerifyBlockError, VerifyCheckpointError};
use zebra_state::KnownBlock;

/// Maximum number of independently constructed child candidates retained.
///
/// Transactions are reference counted inside each block, but independently
/// parsed templates can still consume close to the block-size limit. A small
/// fixed cap prevents unauthenticated RPC polling from growing memory without
/// bound while allowing pools to overlap several in-flight jobs.
pub(crate) const AUX_BLOCK_CACHE_CAPACITY: usize = 16;

/// Maximum age of a cached candidate, independent of chain-tip invalidation.
pub(crate) const AUX_BLOCK_CACHE_TTL: Duration = Duration::from_secs(10 * 60);

/// Maximum time one AuxPoW RPC waits for proposal or commit verification.
///
/// State commits are explicitly non-cancellable and can wait for an absent
/// parent, so every public request needs a finite response deadline. A timed
/// out submission is always reported as inconclusive and remains retryable.
pub(crate) const AUX_BLOCK_VERIFY_TIMEOUT: Duration = Duration::from_secs(60);

/// A failed exact-candidate lookup.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum CandidateLookupError {
    /// The ID was never issued or is no longer retained.
    Unknown,
    /// The issued candidate exceeded the wall-clock-independent cache TTL.
    Expired,
}

/// A failure to reserve a cache slot for a newly issued candidate.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum CandidateInsertError {
    /// Every bounded slot contains an unexpired mining job.
    Full,
    /// Another request is still publishing the same exact candidate.
    Publishing,
}

/// An authenticated lease for one exact candidate-cache entry.
#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) struct CandidateLease {
    id: block::Hash,
    retire_token: [u8; 32],
    needs_publication: bool,
}

impl std::fmt::Debug for CandidateLease {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CandidateLease")
            .field("id", &self.id)
            .field("retire_token", &"[REDACTED]")
            .field("needs_publication", &self.needs_publication)
            .finish()
    }
}

impl CandidateLease {
    /// Returns the proof-independent candidate ID.
    pub(crate) const fn id(&self) -> block::Hash {
        self.id
    }

    /// Returns the unguessable capability required to retire this entry.
    pub(crate) const fn retire_token(&self) -> [u8; 32] {
        self.retire_token
    }
}

/// Result of an authenticated candidate-retirement request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CandidateRetirement {
    /// The exact published entry was removed.
    Retired,
    /// No entry remains, including after an idempotent retry or node restart.
    AlreadyAbsent,
    /// A decoded AuxPoW submission already referenced this candidate.
    SubmissionStarted,
}

/// A failed authenticated candidate-retirement request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CandidateRetirementError {
    /// The supplied retirement capability did not match the active entry.
    Unauthorized,
}

/// Current state-service location of an exact submitted Wcash candidate.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum CandidateChainState {
    /// The candidate is in the finalized or non-finalized best chain.
    BestChain,
    /// The candidate is committed only to a side chain.
    SideChain,
    /// Validation or state writing is still in progress.
    Pending,
    /// The state service has no record of this candidate.
    Unknown,
}

/// Exact, witness-bound state of a `submitauxblock` candidate.
#[derive(Clone, Debug)]
pub(crate) enum CandidateSubmissionState {
    /// The exact witness was observed in one atomic best-chain block read.
    BestChain(Arc<Block>),
    /// The exact witness was observed only in an any-chain fallback read.
    SideChain,
    /// The witness-independent ID exists with different witness bytes.
    ConflictingWitness,
    /// State may still be validating or writing this ID.
    Pending,
    /// Neither committed state nor the visible pending sets contain this ID.
    Unknown,
}

impl From<Option<KnownBlock>> for CandidateChainState {
    fn from(location: Option<KnownBlock>) -> Self {
        match location {
            Some(KnownBlock::Finalized | KnownBlock::BestChain) => Self::BestChain,
            Some(KnownBlock::SideChain) => Self::SideChain,
            Some(KnownBlock::WriteChannel | KnownBlock::Queue) => Self::Pending,
            None => Self::Unknown,
        }
    }
}

impl CandidateChainState {
    /// Classifies a block returned by the any-chain read service.
    pub(crate) fn committed(is_best_chain: bool) -> Self {
        if is_best_chain {
            Self::BestChain
        } else {
            Self::SideChain
        }
    }
}

/// Returns true only when `block` contains the exact submitted Wcash witness.
///
/// Wcash block IDs intentionally exclude the AuxPoW witness, so comparing the
/// hash alone cannot establish idempotent `submitauxblock` success.
pub(crate) fn has_exact_wcash_witness(block: &Block, witness: &[u8]) -> bool {
    block
        .header
        .solution
        .as_wcash()
        .is_some_and(|committed| committed.as_bytes() == witness)
}

/// Binds a committed block snapshot to the submitted witness and its observed
/// chain location.
pub(crate) fn classify_committed_wcash_submission(
    block: Arc<Block>,
    witness: &[u8],
    is_best_chain: bool,
) -> CandidateSubmissionState {
    if !has_exact_wcash_witness(&block, witness) {
        CandidateSubmissionState::ConflictingWitness
    } else if is_best_chain {
        CandidateSubmissionState::BestChain(block)
    } else {
        CandidateSubmissionState::SideChain
    }
}

/// Returns the largest possible completed block size for a canonically
/// serialized proof-free Wcash candidate.
///
/// Replacing an empty witness changes both the witness bytes and its CompactSize
/// prefix, so adding only the raw proof-byte limit undercounts by four bytes.
pub(crate) fn maximum_completed_block_size(proof_free_block_size: usize) -> Option<usize> {
    proof_free_block_size
        .checked_sub(Solution::WCASH_MIN_SERIALIZED_SIZE)?
        .checked_add(Solution::WCASH_MAX_SERIALIZED_SIZE)
}

/// Returns true only for the exact deterministic consensus error produced by
/// an invalid Wcash AuxPoW witness.
///
/// Every state, queue, shutdown, and unknown verifier failure remains
/// inconclusive so a pool can safely retry the same candidate.
pub(crate) fn is_invalid_wcash_auxpow(error: &RouterError) -> bool {
    fn invalid_block(error: &VerifyBlockError) -> bool {
        matches!(
            error,
            VerifyBlockError::Block {
                source: BlockError::InvalidWcashAuxPow { .. }
            }
        )
    }

    match error {
        RouterError::Block { source } => invalid_block(source),
        RouterError::Checkpoint { source } => {
            matches!(&**source, VerifyCheckpointError::VerifyBlock(error) if invalid_block(error))
        }
    }
}

#[derive(Clone)]
pub(crate) struct AuxBlockCandidateCache {
    inner: Arc<Mutex<CacheInner>>,
    capacity: usize,
    ttl: Duration,
}

#[derive(Default)]
struct CacheInner {
    entries: HashMap<block::Hash, CacheEntry>,
}

struct CacheEntry {
    candidate: Arc<Block>,
    created_at: Instant,
    retire_token: [u8; 32],
    state: CacheEntryState,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CacheEntryState {
    /// Reserved inside `createauxblock`, but not returned to its caller yet.
    Publishing,
    /// Published to a pool and eligible for authenticated retirement.
    Published,
    /// At least one canonically decoded AuxPoW submission has begun.
    SubmissionStarted,
}

impl Default for AuxBlockCandidateCache {
    fn default() -> Self {
        Self::new(AUX_BLOCK_CACHE_CAPACITY, AUX_BLOCK_CACHE_TTL)
    }
}

impl AuxBlockCandidateCache {
    fn new(capacity: usize, ttl: Duration) -> Self {
        assert!(
            capacity > 0,
            "the AuxPoW candidate cache must retain an entry"
        );
        Self {
            inner: Arc::new(Mutex::new(CacheInner::default())),
            capacity,
            ttl,
        }
    }

    /// Reserves one exact, proof-free candidate and returns its stable lease.
    ///
    /// Unexpired issued jobs are never evicted to make room for a later caller:
    /// doing so could discard parent-chain work already committed to their IDs.
    pub(crate) fn insert(
        &self,
        candidate: Arc<Block>,
    ) -> Result<CandidateLease, CandidateInsertError> {
        self.insert_at(candidate, Instant::now())
    }

    fn insert_at(
        &self,
        candidate: Arc<Block>,
        now: Instant,
    ) -> Result<CandidateLease, CandidateInsertError> {
        let id = candidate.hash();
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        Self::purge_expired(&mut inner, now, self.ttl);

        if let Some(existing) = inner.entries.get(&id) {
            if existing.state == CacheEntryState::Publishing {
                return Err(CandidateInsertError::Publishing);
            }

            return Ok(CandidateLease {
                id,
                retire_token: existing.retire_token,
                needs_publication: false,
            });
        }

        if inner.entries.len() >= self.capacity {
            return Err(CandidateInsertError::Full);
        }

        let mut retire_token = [0; 32];
        OsRng.fill_bytes(&mut retire_token);
        inner.entries.insert(
            id,
            CacheEntry {
                candidate,
                created_at: now,
                retire_token,
                state: CacheEntryState::Publishing,
            },
        );
        Ok(CandidateLease {
            id,
            retire_token,
            needs_publication: true,
        })
    }

    /// Marks a reserved entry as externally visible.
    ///
    /// A concurrent reissue can only observe the lease after this transition.
    pub(crate) fn publish(&self, lease: CandidateLease) {
        if !lease.needs_publication {
            return;
        }

        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let entry = inner
            .entries
            .get_mut(&lease.id)
            .expect("a candidate reservation must exist until publication");
        assert!(
            bool::from(entry.retire_token.ct_eq(&lease.retire_token)),
            "a candidate reservation token must not change"
        );
        assert_eq!(
            entry.state,
            CacheEntryState::Publishing,
            "a candidate cannot be submitted before publication"
        );
        entry.state = CacheEntryState::Published;
    }

    /// Discards a reservation that was never returned to its caller.
    pub(crate) fn discard_unpublished(&self, lease: CandidateLease) {
        if !lease.needs_publication {
            return;
        }

        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let should_remove = inner.entries.get(&lease.id).is_some_and(|entry| {
            entry.state == CacheEntryState::Publishing
                && bool::from(entry.retire_token.ct_eq(&lease.retire_token))
        });
        if should_remove {
            inner.entries.remove(&lease.id);
        }
    }

    /// Protects and returns an exact unexpired candidate for submission.
    ///
    /// The caller validates the candidate's parent against any committed chain
    /// before consensus submission. Keeping this cache tip-independent lets a
    /// late solution be committed to a shallow side chain and survive a reorg.
    pub(crate) fn begin_submission(
        &self,
        id: block::Hash,
    ) -> Result<Arc<Block>, CandidateLookupError> {
        self.begin_submission_at(id, Instant::now())
    }

    /// Releases an issued job after its exact witness is confirmed on the best
    /// chain and the block is successfully queued for gossip.
    pub(crate) fn remove(&self, id: block::Hash) {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .entries
            .remove(&id);
    }

    /// Retires one published entry only for its unguessable capability.
    ///
    /// Once any decoded submission begins, explicit retirement is refused so
    /// a concurrent valid share remains retryable. The normal TTL still bounds
    /// retention if that submission never reaches a definitive result.
    pub(crate) fn retire(
        &self,
        id: block::Hash,
        retire_token: [u8; 32],
    ) -> Result<CandidateRetirement, CandidateRetirementError> {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Self::purge_expired(&mut inner, Instant::now(), self.ttl);
        let Some(entry) = inner.entries.get(&id) else {
            return Ok(CandidateRetirement::AlreadyAbsent);
        };
        if !bool::from(entry.retire_token.ct_eq(&retire_token)) {
            return Err(CandidateRetirementError::Unauthorized);
        }
        match entry.state {
            CacheEntryState::Publishing => Err(CandidateRetirementError::Unauthorized),
            CacheEntryState::Published => {
                inner.entries.remove(&id);
                Ok(CandidateRetirement::Retired)
            }
            CacheEntryState::SubmissionStarted => Ok(CandidateRetirement::SubmissionStarted),
        }
    }

    fn begin_submission_at(
        &self,
        id: block::Hash,
        now: Instant,
    ) -> Result<Arc<Block>, CandidateLookupError> {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        let Some(entry) = inner.entries.get_mut(&id) else {
            return Err(CandidateLookupError::Unknown);
        };

        if now
            .checked_duration_since(entry.created_at)
            .unwrap_or_default()
            >= self.ttl
        {
            inner.entries.remove(&id);
            return Err(CandidateLookupError::Expired);
        }

        if entry.state == CacheEntryState::Publishing {
            return Err(CandidateLookupError::Unknown);
        }
        entry.state = CacheEntryState::SubmissionStarted;
        Ok(Arc::clone(&entry.candidate))
    }

    fn purge_expired(inner: &mut CacheInner, now: Instant, ttl: Duration) {
        inner.entries.retain(|_, entry| {
            now.checked_duration_since(entry.created_at)
                .unwrap_or_default()
                < ttl
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zebra_chain::block::genesis::wcash_regtest_genesis_block;

    fn candidate(tag: u8, previous_block_hash: block::Hash) -> Arc<Block> {
        let mut candidate = wcash_regtest_genesis_block().as_ref().clone();
        let header = Arc::make_mut(&mut candidate.header);
        header.previous_block_hash = previous_block_hash;
        header.nonce = [tag; 32].into();
        Arc::new(candidate)
    }

    #[test]
    fn returns_the_exact_unexpired_candidate() {
        let cache = AuxBlockCandidateCache::new(2, Duration::from_secs(10));
        let now = Instant::now();
        let tip = block::Hash([0x11; 32]);
        let candidate = candidate(1, tip);
        let lease = cache
            .insert_at(Arc::clone(&candidate), now)
            .expect("cache has room");
        cache.publish(lease);

        let cached = cache
            .begin_submission_at(lease.id(), now)
            .expect("unexpired candidate is cached");
        assert!(Arc::ptr_eq(&candidate, &cached));
    }

    #[test]
    fn rejects_expired_candidates_but_keeps_shallow_reorg_candidates() {
        let cache = AuxBlockCandidateCache::new(2, Duration::from_secs(10));
        let now = Instant::now();
        let tip = block::Hash([0x11; 32]);

        let expired = cache
            .insert_at(candidate(1, tip), now)
            .expect("cache has room");
        cache.publish(expired);
        assert_eq!(
            cache.begin_submission_at(expired.id(), now + Duration::from_secs(10)),
            Err(CandidateLookupError::Expired)
        );
        assert_eq!(
            cache.begin_submission_at(expired.id(), now + Duration::from_secs(10)),
            Err(CandidateLookupError::Unknown)
        );

        let reorg_candidate = cache
            .insert_at(candidate(2, tip), now)
            .expect("expired slot was purged");
        cache.publish(reorg_candidate);
        assert!(
            cache.begin_submission_at(reorg_candidate.id(), now).is_ok(),
            "a bounded candidate remains available while its parent changes chain status"
        );
    }

    #[test]
    fn never_evicts_unexpired_issued_jobs_at_capacity() {
        let cache = AuxBlockCandidateCache::new(2, Duration::from_secs(10));
        let now = Instant::now();
        let tip = block::Hash([0x11; 32]);

        let first = cache
            .insert_at(candidate(1, tip), now)
            .expect("first slot is free");
        cache.publish(first);
        let second = cache
            .insert_at(candidate(2, tip), now + Duration::from_secs(1))
            .expect("second slot is free");
        cache.publish(second);
        assert_eq!(
            cache.insert_at(candidate(3, tip), now + Duration::from_secs(2)),
            Err(CandidateInsertError::Full),
            "a later caller must not evict a job that may already have parent work"
        );

        assert!(cache
            .begin_submission_at(first.id(), now + Duration::from_secs(2))
            .is_ok());
        assert!(cache
            .begin_submission_at(second.id(), now + Duration::from_secs(2))
            .is_ok());

        cache.remove(first.id());
        let third = cache
            .insert_at(candidate(3, tip), now + Duration::from_secs(3))
            .expect("explicit removal releases capacity");
        cache.publish(third);

        // An expired reservation is purged atomically by the next insertion.
        assert!(cache
            .insert_at(candidate(4, tip), now + Duration::from_secs(11))
            .is_ok());
    }

    #[test]
    fn authenticated_retirement_soaks_more_than_sixteen_unsolved_rotations() {
        let cache = AuxBlockCandidateCache::new(2, Duration::from_secs(600));
        let now = Instant::now();
        let tip = block::Hash([0x31; 32]);

        for generation in 0..64u8 {
            let lease = cache
                .insert_at(candidate(generation, tip), now)
                .expect("a retired unsolved generation must release its slot");
            cache.publish(lease);
            assert_eq!(
                cache
                    .retire(lease.id(), lease.retire_token())
                    .expect("the exact capability authorizes retirement"),
                CandidateRetirement::Retired
            );
            assert_eq!(
                cache
                    .retire(lease.id(), lease.retire_token())
                    .expect("retirement retries are idempotent"),
                CandidateRetirement::AlreadyAbsent
            );
        }
    }

    #[test]
    fn candidate_lease_debug_redacts_the_retirement_capability() {
        let cache = AuxBlockCandidateCache::new(1, Duration::from_secs(600));
        let lease = cache
            .insert_at(candidate(1, block::Hash([0x61; 32])), Instant::now())
            .expect("cache has room");
        let debug = format!("{lease:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains(&hex::encode(lease.retire_token())));
    }

    #[test]
    fn retirement_never_discards_a_started_submission() {
        let cache = AuxBlockCandidateCache::new(2, Duration::from_secs(600));
        let now = Instant::now();
        let tip = block::Hash([0x41; 32]);
        let lease = cache
            .insert_at(candidate(1, tip), now)
            .expect("cache has room");

        assert_eq!(
            cache
                .retire(lease.id(), lease.retire_token())
                .expect_err("an unpublished reservation has no external retirement authority"),
            CandidateRetirementError::Unauthorized
        );
        cache.publish(lease);
        assert_eq!(
            cache.retire(lease.id(), [0xff; 32]),
            Err(CandidateRetirementError::Unauthorized)
        );
        let submitted = cache
            .begin_submission_at(lease.id(), now)
            .expect("published candidate begins submission");
        assert_eq!(submitted.hash(), lease.id());
        assert_eq!(
            cache
                .retire(lease.id(), lease.retire_token())
                .expect("the correct capability is recognized"),
            CandidateRetirement::SubmissionStarted
        );
        assert!(
            cache.begin_submission_at(lease.id(), now).is_ok(),
            "the exact candidate remains retryable"
        );
    }

    #[test]
    fn unpublished_tip_race_cleanup_does_not_leak_capacity() {
        let cache = AuxBlockCandidateCache::new(1, Duration::from_secs(600));
        let now = Instant::now();
        let tip = block::Hash([0x51; 32]);
        let abandoned = cache
            .insert_at(candidate(1, tip), now)
            .expect("reserve unpublished candidate");
        cache.discard_unpublished(abandoned);

        let replacement = cache
            .insert_at(candidate(2, tip), now)
            .expect("unpublished cleanup must release capacity immediately");
        cache.publish(replacement);
        assert_eq!(
            cache
                .retire(replacement.id(), replacement.retire_token())
                .expect("published candidate retires"),
            CandidateRetirement::Retired
        );
    }

    #[test]
    fn classifies_candidate_locations_for_exact_rpc_results() {
        assert_eq!(
            CandidateChainState::from(Some(KnownBlock::Finalized)),
            CandidateChainState::BestChain
        );
        assert_eq!(
            CandidateChainState::from(Some(KnownBlock::BestChain)),
            CandidateChainState::BestChain
        );
        assert_eq!(
            CandidateChainState::from(Some(KnownBlock::SideChain)),
            CandidateChainState::SideChain
        );
        assert_eq!(
            CandidateChainState::from(Some(KnownBlock::Queue)),
            CandidateChainState::Pending
        );
        assert_eq!(
            CandidateChainState::from(Some(KnownBlock::WriteChannel)),
            CandidateChainState::Pending
        );
        assert_eq!(
            CandidateChainState::from(None),
            CandidateChainState::Unknown
        );
        assert_eq!(
            CandidateChainState::committed(true),
            CandidateChainState::BestChain
        );
        assert_eq!(
            CandidateChainState::committed(false),
            CandidateChainState::SideChain
        );
    }

    #[test]
    fn exact_witness_binding_is_not_implied_by_the_block_id() {
        let mut block = wcash_regtest_genesis_block().as_ref().clone();
        let block_id = block.hash();
        Arc::make_mut(&mut block.header).solution =
            Solution::for_wcash(vec![0x11, 0x22]).expect("short Wcash witness");

        assert_eq!(
            block.hash(),
            block_id,
            "the Wcash ID deliberately excludes its AuxPoW witness"
        );
        assert!(has_exact_wcash_witness(&block, &[0x11, 0x22]));
        assert!(!has_exact_wcash_witness(&block, &[0x11, 0x23]));
        assert!(!has_exact_wcash_witness(&block, &[]));

        assert!(matches!(
            classify_committed_wcash_submission(Arc::new(block.clone()), &[0x11, 0x22], true),
            CandidateSubmissionState::BestChain(_)
        ));
        assert!(matches!(
            classify_committed_wcash_submission(Arc::new(block.clone()), &[0x11, 0x22], false),
            CandidateSubmissionState::SideChain
        ));
        assert!(matches!(
            classify_committed_wcash_submission(Arc::new(block), &[0x99], true),
            CandidateSubmissionState::ConflictingWitness
        ));
    }

    #[test]
    fn maximum_completed_size_includes_the_compact_size_growth() {
        assert_eq!(
            wcash_zcash_aux::MAX_PROOF_BYTES,
            zebra_chain::work::equihash::MAX_WCASH_AUXPOW_BYTES,
            "RPC and block-wire witness limits must stay identical"
        );
        let proof_free_size = 1_000_000;
        assert_eq!(
            maximum_completed_block_size(proof_free_size),
            Some(
                proof_free_size - Solution::WCASH_MIN_SERIALIZED_SIZE
                    + Solution::WCASH_MAX_SERIALIZED_SIZE
            )
        );
        assert_eq!(maximum_completed_block_size(0), None);
        assert_eq!(maximum_completed_block_size(usize::MAX), None);
    }

    #[test]
    fn only_invalid_auxpow_is_a_definitive_submission_rejection() {
        let invalid_auxpow = RouterError::Block {
            source: Box::new(VerifyBlockError::Block {
                source: BlockError::InvalidWcashAuxPow {
                    height: zebra_chain::block::Height(1),
                    hash: block::Hash([0x11; 32]),
                    source: wcash_zcash_aux::AuxPowError::InvalidProofMagic([0; 4]),
                },
            }),
        };
        assert!(is_invalid_wcash_auxpow(&invalid_auxpow));

        let transient = RouterError::Block {
            source: Box::new(VerifyBlockError::StateService {
                source: std::io::Error::other("temporary state failure").into(),
                hash: block::Hash([0x22; 32]),
            }),
        };
        assert!(!is_invalid_wcash_auxpow(&transient));
    }
}
