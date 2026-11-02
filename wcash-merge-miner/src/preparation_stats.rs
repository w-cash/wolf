//! Fixed-size preparation statistics, independent of lossy console diagnostics.
//!
//! These process-local atomics never enter ordinary share validation. Snapshots
//! are independently sampled fields, not an atomic ledger or durable history.

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, OnceLock,
};

use crate::preparation_timing::{PreparationResult, PHASE_COUNT};

/// Exclusive upper bounds for completed preparation latency buckets, in ms.
/// The final bucket includes every value above the last bound.
pub const PREPARATION_LATENCY_BOUNDS_MS: [u64; 7] = [10, 50, 100, 250, 1_000, 5_000, 15_000];
const BUCKETS: usize = PREPARATION_LATENCY_BOUNDS_MS.len() + 1;
static COUNTERS: OnceLock<Arc<PreparationCounters>> = OnceLock::new();

/// Cumulative, process-local observations; fields are not one atomic snapshot.
///
/// A process restart resets these counters. An absent/stalled exporter is
/// unknown coverage, not a zero. Only completed attempts enter latency buckets;
/// dropped timing owners increment `abandoned`, without destructor I/O.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PreparationStatistics {
    /// Timing owners created in this process.
    pub started: u64,
    /// Explicitly finished attempts, including fast successes omitted from logs.
    pub completed: u64,
    /// Explicit successful activations.
    pub activated: u64,
    /// Explicit error or activation failures.
    pub failed: u64,
    /// Timing owners dropped without an explicit result.
    pub abandoned: u64,
    /// Noncumulative exclusive latency buckets; each completed attempt enters one.
    pub elapsed_buckets: [u64; BUCKETS],
    /// Sum of completed attempt elapsed milliseconds, saturating at `u64::MAX`.
    pub elapsed_sum_ms: u64,
    /// Maximum completed attempt elapsed milliseconds.
    pub elapsed_max_ms: u64,
    /// Exclusive phase sums over completed attempts, in `PreparationPhase` order.
    pub phase_sum_ms: [u64; PHASE_COUNT],
}

/// Reads fixed-size cached counters without RPC, I/O, formatting, or locks.
///
/// This is an in-process diagnostic API. It cannot establish visibility if the
/// caller/exporter is unavailable and does not replace authoritative receipts.
pub fn preparation_statistics() -> PreparationStatistics {
    COUNTERS
        .get()
        .map_or_else(PreparationStatistics::default, |c| c.snapshot())
}

pub(crate) fn global_counters() -> Arc<PreparationCounters> {
    Arc::clone(COUNTERS.get_or_init(|| Arc::new(PreparationCounters::default())))
}

#[derive(Default)]
pub(crate) struct PreparationCounters {
    started: AtomicU64,
    completed: AtomicU64,
    activated: AtomicU64,
    failed: AtomicU64,
    abandoned: AtomicU64,
    elapsed_buckets: [AtomicU64; BUCKETS],
    elapsed_sum_ms: AtomicU64,
    elapsed_max_ms: AtomicU64,
    phase_sum_ms: [AtomicU64; PHASE_COUNT],
}

fn add(counter: &AtomicU64, value: u64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
        Some(old.saturating_add(value))
    });
}

impl PreparationCounters {
    pub(crate) fn started(&self) {
        add(&self.started, 1);
    }
    pub(crate) fn abandoned(&self) {
        add(&self.abandoned, 1);
    }

    pub(crate) fn completed(
        &self,
        result: PreparationResult,
        elapsed: u64,
        phases: [u64; PHASE_COUNT],
    ) {
        match result {
            PreparationResult::Activated => add(&self.activated, 1),
            PreparationResult::Failed(_) => add(&self.failed, 1),
        }
        let bucket = PREPARATION_LATENCY_BOUNDS_MS
            .iter()
            .position(|bound| elapsed < *bound)
            .unwrap_or(PREPARATION_LATENCY_BOUNDS_MS.len());
        add(&self.elapsed_buckets[bucket], 1);
        add(&self.elapsed_sum_ms, elapsed);
        self.elapsed_max_ms.fetch_max(elapsed, Ordering::Relaxed);
        for (counter, value) in self.phase_sum_ms.iter().zip(phases) {
            add(counter, value);
        }
        add(&self.completed, 1);
    }

    pub(crate) fn snapshot(&self) -> PreparationStatistics {
        PreparationStatistics {
            started: self.started.load(Ordering::Relaxed),
            completed: self.completed.load(Ordering::Relaxed),
            activated: self.activated.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            abandoned: self.abandoned.load(Ordering::Relaxed),
            elapsed_buckets: std::array::from_fn(|i| {
                self.elapsed_buckets[i].load(Ordering::Relaxed)
            }),
            elapsed_sum_ms: self.elapsed_sum_ms.load(Ordering::Relaxed),
            elapsed_max_ms: self.elapsed_max_ms.load(Ordering::Relaxed),
            phase_sum_ms: std::array::from_fn(|i| self.phase_sum_ms[i].load(Ordering::Relaxed)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exclusive_bucket_edges_and_saturation_remain_explicit() {
        let counters = PreparationCounters::default();
        for elapsed in [
            0,
            9,
            10,
            49,
            50,
            99,
            100,
            249,
            250,
            999,
            1_000,
            4_999,
            5_000,
            14_999,
            15_000,
            u64::MAX,
        ] {
            counters.started();
            counters.completed(PreparationResult::Activated, elapsed, [0; PHASE_COUNT]);
        }
        counters.started();
        counters.abandoned();
        let s = counters.snapshot();
        assert_eq!(s.elapsed_buckets, [2; BUCKETS]);
        assert_eq!(s.completed, 16);
        assert_eq!(s.activated, 16);
        assert_eq!(s.failed, 0);
        assert_eq!(s.started, 17);
        assert_eq!(s.abandoned, 1);
        assert_eq!(s.elapsed_sum_ms, u64::MAX);
        assert_eq!(s.elapsed_max_ms, u64::MAX);
    }
}
