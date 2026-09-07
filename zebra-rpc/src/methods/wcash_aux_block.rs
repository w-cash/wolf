//! Bounded, exact-candidate storage for Wcash AuxPoW mining.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

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

    /// Reserves one exact, proof-free candidate and returns its stable ID.
    ///
    /// Unexpired issued jobs are never evicted to make room for a later caller:
    /// doing so could discard parent-chain work already committed to their IDs.
    pub(crate) fn insert(
        &self,
        candidate: Arc<Block>,
    ) -> Result<block::Hash, CandidateInsertError> {
        self.insert_at(candidate, Instant::now())
    }

    fn insert_at(
        &self,
        candidate: Arc<Block>,
        now: Instant,
    ) -> Result<block::Hash, CandidateInsertError> {
        let id = candidate.hash();
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        Self::purge_expired(&mut inner, now, self.ttl);

        if !inner.entries.contains_key(&id) && inner.entries.len() >= self.capacity {
            return Err(CandidateInsertError::Full);
        }

        // Reissuing the exact candidate refreshes its bounded lifetime without
        // consuming another slot.
        inner.entries.insert(
            id,
            CacheEntry {
                candidate,
                created_at: now,
            },
        );
        Ok(id)
    }

    /// Returns an exact unexpired candidate.
    ///
    /// The caller validates the candidate's parent against any committed chain
    /// before consensus submission. Keeping this cache tip-independent lets a
    /// late solution be committed to a shallow side chain and survive a reorg.
    pub(crate) fn get(&self, id: block::Hash) -> Result<Arc<Block>, CandidateLookupError> {
        self.get_at(id, Instant::now())
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

    fn get_at(&self, id: block::Hash, now: Instant) -> Result<Arc<Block>, CandidateLookupError> {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        let Some(entry) = inner.entries.get(&id) else {
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
        let id = cache
            .insert_at(Arc::clone(&candidate), now)
            .expect("cache has room");

        let cached = cache
            .get_at(id, now)
            .expect("unexpired candidate is cached");
        assert!(Arc::ptr_eq(&candidate, &cached));
    }

    #[test]
    fn rejects_expired_candidates_but_keeps_shallow_reorg_candidates() {
        let cache = AuxBlockCandidateCache::new(2, Duration::from_secs(10));
        let now = Instant::now();
        let tip = block::Hash([0x11; 32]);

        let expired_id = cache
            .insert_at(candidate(1, tip), now)
            .expect("cache has room");
        assert_eq!(
            cache.get_at(expired_id, now + Duration::from_secs(10)),
            Err(CandidateLookupError::Expired)
        );
        assert_eq!(
            cache.get_at(expired_id, now + Duration::from_secs(10)),
            Err(CandidateLookupError::Unknown)
        );

        let reorg_candidate_id = cache
            .insert_at(candidate(2, tip), now)
            .expect("expired slot was purged");
        assert!(
            cache.get_at(reorg_candidate_id, now).is_ok(),
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
        let second = cache
            .insert_at(candidate(2, tip), now + Duration::from_secs(1))
            .expect("second slot is free");
        assert_eq!(
            cache.insert_at(candidate(3, tip), now + Duration::from_secs(2)),
            Err(CandidateInsertError::Full),
            "a later caller must not evict a job that may already have parent work"
        );

        assert!(cache.get_at(first, now + Duration::from_secs(2)).is_ok());
        assert!(cache.get_at(second, now + Duration::from_secs(2)).is_ok());

        cache.remove(first);
        assert!(cache
            .insert_at(candidate(3, tip), now + Duration::from_secs(3))
            .is_ok());

        // An expired reservation is purged atomically by the next insertion.
        assert!(cache
            .insert_at(candidate(4, tip), now + Duration::from_secs(11))
            .is_ok());
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
