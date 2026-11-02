//! Independent parent authority for bounded late Zcash proofs and work observations.

use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use wcash_pool_protocol::{Hex32, JobDescriptor, TargetLe};
use zebra_chain::work::difficulty::U256;

use crate::{
    native::{NativeChainTip, NativeZcashConfig, NativeZcashProvider},
    MinerError,
};

const AUTHORITY_MAX_AGE: Duration = Duration::from_secs(1);

/// Cached, identity-bound predecessor observation near backend share admission.
///
/// This does not prove the tip at ASIC discovery time or guarantee a reward.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PoolBackendZcashWorkObservation {
    /// A fresh authenticated node set matches the job's exact predecessor and height.
    Current,
    /// A fresh authenticated node set reports a different predecessor or height.
    Stale,
    /// Evidence is absent, expired, contended, failed or closed.
    Unknown,
}

/// Nonblocking cached authority, separate from immutable proof bytes and WEC leases.
pub trait PoolBackendZcashRecoveryAuthority: Send + Sync {
    /// Identity in the backend journal's little-endian hash representation.
    fn expected_genesis(&self) -> Hex32;

    /// False after permanent revocation; temporary unknown evidence can still defer.
    fn is_open(&self) -> bool {
        true
    }

    /// Reads a bounded local observation; must never issue RPC or wait for a lock.
    fn observe(&self, job: &JobDescriptor) -> PoolBackendZcashWorkObservation;
}

#[derive(Clone, Copy)]
struct RecoverySample {
    attempt: u64,
    started: Instant,
    completed: Instant,
    tip: Option<NativeChainTip>,
}

struct RecoveryState {
    closed: AtomicBool,
    next_attempt: AtomicU64,
    sample: Mutex<Option<RecoverySample>>,
}

/// Process-global ZEC authority; owns metadata clients, never a mining candidate.
#[derive(Clone)]
pub struct NativePoolZcashRecoveryAuthority {
    provider: NativeZcashProvider,
    genesis: Hex32,
    state: Arc<RecoveryState>,
}

impl NativePoolZcashRecoveryAuthority {
    /// Builds independently bounded metadata clients without issuing any request.
    pub fn new(config: &NativeZcashConfig) -> Result<Self, MinerError> {
        let (provider, genesis) = config.recovery_observation_provider()?;
        Ok(Self {
            provider,
            genesis: Hex32::new(genesis),
            state: Arc::new(RecoveryState {
                closed: AtomicBool::new(false),
                next_attempt: AtomicU64::new(0),
                sample: Mutex::new(None),
            }),
        })
    }

    pub(crate) fn refresh(&self) {
        if self.state.closed.load(Ordering::Acquire) {
            return;
        }
        let Ok(previous) =
            self.state
                .next_attempt
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |attempt| {
                    attempt.checked_add(1)
                })
        else {
            self.close();
            return;
        };
        let started = Instant::now();
        let result = self.provider.recovery_chain_tip();
        let completed = Instant::now();
        if let Err(error) = &result {
            crate::diagnostics::native_backend_warning("zcash_recovery_observation", Some(error));
        } else if completed.duration_since(started) >= AUTHORITY_MAX_AGE {
            crate::diagnostics::native_zcash_recovery_sample_expired(
                completed.duration_since(started),
                AUTHORITY_MAX_AGE,
            );
        }
        let tip = result.ok();
        self.publish(RecoverySample {
            attempt: previous + 1,
            started,
            completed,
            tip,
        });
    }

    fn publish(&self, sample: RecoverySample) {
        if self.state.closed.load(Ordering::Acquire) {
            return;
        }
        if let Ok(mut current) = self.state.sample.lock() {
            if !self.state.closed.load(Ordering::Acquire)
                && current.is_none_or(|old| sample.attempt > old.attempt)
            {
                *current = Some(sample);
            }
        }
    }

    /// Permanently revokes authority before shutdown; late results cannot reopen it.
    pub fn close(&self) {
        self.state.closed.store(true, Ordering::Release);
    }

    fn cached_tip(&self, now: Instant) -> Option<NativeChainTip> {
        if self.state.closed.load(Ordering::Acquire) {
            return None;
        }
        let guard = self.state.sample.try_lock().ok()?;
        let sample = (*guard)?;
        if self.state.closed.load(Ordering::Acquire)
            || sample.started > sample.completed
            || sample.completed > now
            || now
                .checked_duration_since(sample.started)
                .is_none_or(|age| age >= AUTHORITY_MAX_AGE)
        {
            return None;
        }
        sample.tip
    }

    pub(crate) fn observe_tip(&self, expected: NativeChainTip) -> PoolBackendZcashWorkObservation {
        use PoolBackendZcashWorkObservation::{Current, Stale, Unknown};
        match self.cached_tip(Instant::now()) {
            Some(tip) if tip == expected => Current,
            Some(_) => Stale,
            None => Unknown,
        }
    }

    fn observe_at(&self, job: &JobDescriptor, now: Instant) -> PoolBackendZcashWorkObservation {
        use PoolBackendZcashWorkObservation::{Current, Stale, Unknown};
        let Some(tip) = self.cached_tip(now) else {
            return Unknown;
        };
        if job.zcash_height.checked_sub(1) == Some(tip.height)
            && job.zcash_previous_hash_le.as_bytes() == &tip.block_hash_le
        {
            Current
        } else {
            Stale
        }
    }
}

impl PoolBackendZcashRecoveryAuthority for NativePoolZcashRecoveryAuthority {
    fn expected_genesis(&self) -> Hex32 {
        self.genesis.clone()
    }

    fn is_open(&self) -> bool {
        !self.state.closed.load(Ordering::Acquire)
    }

    fn observe(&self, job: &JobDescriptor) -> PoolBackendZcashWorkObservation {
        self.observe_at(job, Instant::now())
    }
}

/// Process-local accepted target-work split; accounting and journals are unchanged.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PoolBackendZcashWorkStats {
    /// New durable shares in Current, Stale and Unknown order; replays are excluded.
    pub shares: [u64; 3],
    /// Exact cumulative floor(2^256/(share_target+1)) in the same order, little-endian.
    pub target_work_le: [[u8; 32]; 3],
    /// First newly committed share observed by this process, or zero before any share.
    pub first_event_seq: u64,
    /// Last newly committed share observed by this process.
    pub last_event_seq: u64,
    /// A counter saturated; consumers must not present saturated totals as exact work.
    pub overflowed: bool,
}

impl PoolBackendZcashWorkStats {
    pub(crate) fn record(
        &mut self,
        observation: PoolBackendZcashWorkObservation,
        target: &TargetLe,
        event_seq: u64,
    ) {
        let index = match observation {
            PoolBackendZcashWorkObservation::Current => 0,
            PoolBackendZcashWorkObservation::Stale => 1,
            PoolBackendZcashWorkObservation::Unknown => 2,
        };
        let target = U256::from_little_endian(target.as_bytes());
        if target.is_zero() {
            self.overflowed = true;
            return;
        }
        let work = if target == U256::max_value() {
            U256::one()
        } else {
            (U256::max_value() - target) / (target + U256::one()) + U256::one()
        };
        let accumulated = U256::from_little_endian(&self.target_work_le[index]);
        let next = accumulated.checked_add(work).unwrap_or_else(|| {
            self.overflowed = true;
            U256::max_value()
        });
        self.target_work_le[index] = next.to_little_endian();
        self.shares[index] = self.shares[index].checked_add(1).unwrap_or_else(|| {
            self.overflowed = true;
            u64::MAX
        });
        if self.first_event_seq == 0 {
            self.first_event_seq = event_seq;
        }
        self.last_event_seq = event_seq;
    }
}

#[cfg(test)]
mod tests;
