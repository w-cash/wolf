//! Bounded control workers and detached exact-generation currentness probes.
//!
//! Probes retain RPC clients and immutable predecessor identities, never a
//! coordinator, candidate lease, prepared block, or share-validation owner.

use std::{
    io,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering},
        mpsc, Arc,
    },
    thread,
    time::{Duration, Instant},
};

use serde_json::json;

use crate::{
    native::{NativeChainTip, NativeZcashProvider},
    rpc::ZebraRpcClient,
    MinerError, NATIVE_JOB_MAX_AGE_SECONDS,
};

const WCASH_CURRENT: u8 = 1;
const ZCASH_CURRENT: u8 = 2;
const CLOSED: u8 = 4;
const BOTH_CURRENT: u8 = WCASH_CURRENT | ZCASH_CURRENT;
const OBSERVATION_RESULT_BITS: u32 = 2;
const OBSERVATION_RESULT_MASK: u64 = (1 << OBSERVATION_RESULT_BITS) - 1;

/// The result of one last completed chain probe, not a current admission verdict.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeCurrentnessObservationResult {
    /// No completed probe is available, or its completion cannot be ordered.
    Unknown,
    /// The probe found its exact expected chain tip at completion.
    Current,
    /// The probe proved that its expected chain tip differs from the node tip.
    TipMismatch,
    /// The probe failed without establishing a different tip.
    ProbeFailed,
}

impl NativeCurrentnessObservationResult {
    /// A fixed, public diagnostic label without endpoint or error text.
    pub const fn label(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Current => "current",
            Self::TipMismatch => "tip_mismatch",
            Self::ProbeFailed => "probe_failed",
        }
    }

    const fn tag(self) -> u64 {
        match self {
            Self::Unknown => 0,
            Self::Current => 1,
            Self::TipMismatch => 2,
            Self::ProbeFailed => 3,
        }
    }

    fn from_result(result: &Result<(), MinerError>) -> Self {
        match result {
            Ok(()) => Self::Current,
            Err(MinerError::ChildTipMismatch { .. } | MinerError::ParentTipMismatch { .. }) => {
                Self::TipMismatch
            }
            Err(_) => Self::ProbeFailed,
        }
    }
}

/// An independently sampled chain result and the age of that exact result.
///
/// An old positive result is not evidence that the parent remains current. A
/// missing sample is unknown even when initial admission permits both chains.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NativeChainCurrentnessObservation {
    /// Last published result; this does not gate share or winner admission.
    /// Equal completion microseconds conservatively prefer a failure/mismatch.
    pub result: NativeCurrentnessObservationResult,
    /// Monotonic age of the same result, or `None` when there is no usable sample.
    pub sample_age_ms: Option<u64>,
}

/// Lease/closure status sampled after the independent chain observations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeGenerationObservationStatus {
    /// The original generation lease has not expired or been closed.
    Active,
    /// Cached admission has been permanently closed.
    Closed,
    /// The original generation lease has expired.
    Expired,
}

impl NativeGenerationObservationStatus {
    /// A fixed, public diagnostic label.
    pub const fn label(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Closed => "closed",
            Self::Expired => "expired",
        }
    }
}

/// A bounded diagnostic view of one generation's last-known probe observations.
///
/// Chain samples are independent, not an atomic dual-chain observation or a
/// measurement of physical miner work. The view does not change admission,
/// extend the lease, prove ASIC notification, or issue a new network request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NativePoolCurrentnessObservation {
    /// Elapsed monotonic time since this exact generation was created.
    pub generation_age_ms: u64,
    /// Explicit terminal status, separate from any historical positive sample.
    pub status: NativeGenerationObservationStatus,
    /// Last completed Wcash probe and its own sample age.
    pub wcash: NativeChainCurrentnessObservation,
    /// Last completed Zcash probe and its own sample age.
    pub zcash: NativeChainCurrentnessObservation,
}

struct CurrentnessState {
    created_at: Instant,
    bits: AtomicU8,
    // Each atomic publishes one complete result/timestamp pair relative to this
    // generation. Independent chains never wait for the other chain's sample.
    wcash_observation: AtomicU64,
    zcash_observation: AtomicU64,
}

impl CurrentnessState {
    fn new(created_at: Instant) -> Self {
        Self {
            created_at,
            bits: AtomicU8::new(BOTH_CURRENT),
            wcash_observation: AtomicU64::new(0),
            zcash_observation: AtomicU64::new(0),
        }
    }

    fn live_bits(&self) -> u8 {
        if self.created_at.elapsed() >= Duration::from_secs(NATIVE_JOB_MAX_AGE_SECONDS) {
            return 0;
        }
        let bits = self.bits.load(Ordering::Acquire);
        if bits & CLOSED != 0 {
            0
        } else {
            bits & BOTH_CURRENT
        }
    }

    fn record(&self, chain: u8, current: bool) {
        // The legacy boolean result does not establish why a check failed.
        self.record_observation(
            chain,
            current,
            if current {
                NativeCurrentnessObservationResult::Current
            } else {
                NativeCurrentnessObservationResult::ProbeFailed
            },
        );
    }

    fn record_result(&self, chain: u8, result: &Result<(), MinerError>) {
        self.record_observation(
            chain,
            result.is_ok(),
            NativeCurrentnessObservationResult::from_result(result),
        );
    }

    fn record_observation(
        &self,
        chain: u8,
        current: bool,
        observation: NativeCurrentnessObservationResult,
    ) {
        let elapsed = self.created_at.elapsed();
        self.record_observation_at(chain, current, observation, elapsed);
    }

    fn record_observation_at(
        &self,
        chain: u8,
        current: bool,
        observation: NativeCurrentnessObservationResult,
        elapsed: Duration,
    ) {
        if elapsed >= Duration::from_secs(NATIVE_JOB_MAX_AGE_SECONDS) {
            return;
        }
        // The CLOSED bit and current bits share one atomic. A response racing
        // closure cannot clear CLOSED or reopen this generation.
        let published = self
            .bits
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |bits| {
                if bits & CLOSED != 0 {
                    return None;
                }
                let updated = if current { bits | chain } else { bits & !chain };
                // Preserve terminal fail-closed admission when neither chain
                // remains current; only a newly prepared generation can reopen.
                Some(if updated & BOTH_CURRENT == 0 {
                    updated | CLOSED
                } else {
                    updated
                })
            });
        if published.is_err() {
            return;
        }
        // A completion is accepted only within the 45-second lease, so shifting
        // its microsecond timestamp cannot overflow this fixed-size field.
        let completed = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
        let sample = (completed << OBSERVATION_RESULT_BITS) | observation.tag();
        let destination = if chain == WCASH_CURRENT {
            &self.wcash_observation
        } else {
            &self.zcash_observation
        };
        // Keep the newest completion even if an older publisher was delayed.
        // Completion time dominates the packed value; equal-microsecond ties
        // conservatively prefer failed/mismatched over positive observations.
        destination.fetch_max(sample, Ordering::Release);
    }

    fn observation_snapshot(&self) -> NativePoolCurrentnessObservation {
        let wcash = self.wcash_observation.load(Ordering::Acquire);
        let zcash = self.zcash_observation.load(Ordering::Acquire);
        // Read time after samples so a concurrent newer completion cannot be
        // described as a future sample with a fabricated zero age.
        self.observation_snapshot_at(wcash, zcash, self.created_at.elapsed())
    }

    fn observation_snapshot_at(
        &self,
        wcash: u64,
        zcash: u64,
        elapsed: Duration,
    ) -> NativePoolCurrentnessObservation {
        let status = if elapsed >= Duration::from_secs(NATIVE_JOB_MAX_AGE_SECONDS) {
            NativeGenerationObservationStatus::Expired
        } else if self.bits.load(Ordering::Acquire) & CLOSED != 0 {
            NativeGenerationObservationStatus::Closed
        } else {
            NativeGenerationObservationStatus::Active
        };
        NativePoolCurrentnessObservation {
            generation_age_ms: milliseconds(elapsed),
            status,
            wcash: decode_observation(wcash, elapsed),
            zcash: decode_observation(zcash, elapsed),
        }
    }
}

fn decode_observation(sample: u64, elapsed: Duration) -> NativeChainCurrentnessObservation {
    let unknown = NativeChainCurrentnessObservation {
        result: NativeCurrentnessObservationResult::Unknown,
        sample_age_ms: None,
    };
    let result = match sample & OBSERVATION_RESULT_MASK {
        1 => NativeCurrentnessObservationResult::Current,
        2 => NativeCurrentnessObservationResult::TipMismatch,
        3 => NativeCurrentnessObservationResult::ProbeFailed,
        _ => return unknown,
    };
    let completed = Duration::from_micros(sample >> OBSERVATION_RESULT_BITS);
    let Some(age) = elapsed.checked_sub(completed) else {
        return unknown;
    };
    NativeChainCurrentnessObservation {
        result,
        sample_age_ms: Some(milliseconds(age)),
    }
}

fn milliseconds(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// An immutable probe bound to one exact generation and its original 45s lease.
#[derive(Clone)]
pub struct NativePoolCurrentnessProbe {
    wcash_node: ZebraRpcClient,
    zcash: NativeZcashProvider,
    wcash_tip: NativeChainTip,
    zcash_tip: NativeChainTip,
    state: Arc<CurrentnessState>,
}

impl NativePoolCurrentnessProbe {
    pub(crate) fn new(
        wcash_node: ZebraRpcClient,
        zcash: NativeZcashProvider,
        wcash_tip: NativeChainTip,
        zcash_tip: NativeChainTip,
        created_at: Instant,
    ) -> Self {
        Self {
            wcash_node,
            zcash,
            wcash_tip,
            zcash_tip,
            state: Arc::new(CurrentnessState::new(created_at)),
        }
    }

    /// Checks only WEC. Call from its own bounded worker, never share ingress.
    pub fn check_wcash(&self) {
        let result = self.check_wcash_tip();
        self.state.record_result(WCASH_CURRENT, &result);
    }

    fn check_wcash_tip(&self) -> Result<(), MinerError> {
        let height: u32 = self.wcash_node.call("getblockcount", json!([]))?;
        let hash: String = self.wcash_node.call("getbestblockhash", json!([]))?;
        let expected_hash = hex::encode(
            self.wcash_tip
                .block_hash_le
                .iter()
                .rev()
                .copied()
                .collect::<Vec<_>>(),
        );
        if height != self.wcash_tip.height || !hash.eq_ignore_ascii_case(&expected_hash) {
            return Err(MinerError::ChildTipMismatch {
                expected: format!("{expected_hash} at height {}", self.wcash_tip.height),
                endpoint: self.wcash_node.label().to_string(),
                actual: format!("{hash} at height {height}"),
            });
        }
        Ok(())
    }

    /// Checks the existing ZEC quorum policy independently of WEC completion.
    pub fn check_zcash(&self) {
        let result = self.zcash.assert_tip_current(self.zcash_tip);
        self.state.record_result(ZCASH_CURRENT, &result);
    }

    /// Reads cached diagnostic observations without RPC, locks or formatting.
    ///
    /// Call from control/diagnostics code, not ordinary share admission. Results
    /// include their ages and never assert a freshness threshold or physical
    /// useful work. The two chain samples are independently timed.
    pub fn observation_snapshot(&self) -> NativePoolCurrentnessObservation {
        self.state.observation_snapshot()
    }

    /// Cached admission only; no RPC, mutex, formatting, or logging.
    pub fn is_healthy(&self) -> bool {
        self.state.live_bits() != 0
    }

    /// Cached parent state for late ZEC rescue; no WEC completion dependency.
    pub fn is_zcash_current(&self) -> bool {
        self.state.live_bits() & ZCASH_CURRENT != 0
    }

    /// Requests replacement if either independently checked chain is unavailable.
    pub fn replacement_requested(&self) -> bool {
        self.state.live_bits() != BOTH_CURRENT
    }

    /// Permanently closes this state before its candidate enters retirement.
    pub fn close(&self) {
        self.state.bits.fetch_or(CLOSED, Ordering::AcqRel);
    }

    pub(crate) fn record_legacy_currentness(&self, wcash: bool, zcash: bool) {
        self.state.record(WCASH_CURRENT, wcash);
        self.state.record(ZCASH_CURRENT, zcash);
    }
}

/// Independent, fixed WEC and ZEC probe lanes paced at the existing 250ms interval.
/// The controller calls `tick` during preparation and backoff as well as idle time.
pub struct NativePoolCurrentnessWorkers {
    wcash: PoolBackendControlWorker<NativePoolCurrentnessProbe, ()>,
    zcash: PoolBackendControlWorker<NativePoolCurrentnessProbe, ()>,
    next_wcash: Instant,
    next_zcash: Instant,
}

impl NativePoolCurrentnessWorkers {
    /// Starts exactly two lanes; no per-controller-tick thread creation.
    pub fn spawn() -> Result<Self, io::Error> {
        Ok(Self {
            wcash: PoolBackendControlWorker::spawn(
                "wcash-currentness",
                |probe: NativePoolCurrentnessProbe| probe.check_wcash(),
                |_| {},
            )?,
            zcash: PoolBackendControlWorker::spawn(
                "zcash-currentness",
                |probe: NativePoolCurrentnessProbe| probe.check_zcash(),
                |_| {},
            )?,
            next_wcash: Instant::now(),
            next_zcash: Instant::now(),
        })
    }

    /// Polls and schedules without waiting for either node or the other chain.
    pub fn tick(&mut self, probe: Option<&NativePoolCurrentnessProbe>) -> Result<(), io::Error> {
        const INTERVAL: Duration = Duration::from_millis(250);
        if self.wcash.poll()?.is_some() {
            self.next_wcash = Instant::now() + INTERVAL;
        }
        if self.zcash.poll()?.is_some() {
            self.next_zcash = Instant::now() + INTERVAL;
        }
        if let Some(probe) = probe {
            if !self.wcash.is_busy() && Instant::now() >= self.next_wcash {
                self.wcash.request(probe.clone())?;
            }
            if !self.zcash.is_busy() && Instant::now() >= self.next_zcash {
                self.zcash.request(probe.clone())?;
            }
        }
        Ok(())
    }

    /// Requests fresh probes after activation without overlapping an in-flight check.
    pub fn generation_changed(&mut self) {
        self.next_wcash = Instant::now();
        self.next_zcash = Instant::now();
    }

    #[cfg(test)]
    pub(crate) fn wcash_is_busy_for_test(&self) -> bool {
        self.wcash.is_busy()
    }

    /// Joins both bounded RPC lanes after listeners have stopped.
    pub fn request_shutdown(&mut self) -> Result<(), io::Error> {
        let wcash = self.wcash.request_shutdown();
        let zcash = self.zcash.request_shutdown();
        wcash?;
        zcash
    }
}

/// One fixed worker, one in-flight request, and a rendezvous result handoff.
///
/// The worker owns an unconsumed result until the controller polls it. Closing
/// the result receiver returns that result to `abandon` on the worker thread;
/// dropping the controller never drops a prepared candidate on the main thread.
/// Call shutdown only after stopping listeners: in-flight RPCs retain their
/// configured timeout and cannot be force-cancelled safely.
pub struct PoolBackendControlWorker<Q, R> {
    requests: Option<mpsc::SyncSender<Q>>,
    results: Option<mpsc::Receiver<R>>,
    thread: Option<thread::JoinHandle<()>>,
    in_flight: bool,
    shutdown: Arc<AtomicBool>,
}

impl<Q: Send + 'static, R: Send + 'static> PoolBackendControlWorker<Q, R> {
    /// Spawns one bounded worker. `abandon` must preserve owned cleanup duties.
    pub fn spawn(
        name: &str,
        mut work: impl FnMut(Q) -> R + Send + 'static,
        mut abandon: impl FnMut(R) + Send + 'static,
    ) -> Result<Self, io::Error> {
        let (request_tx, request_rx) = mpsc::sync_channel(1);
        let (result_tx, result_rx) = mpsc::sync_channel(0);
        let shutdown = Arc::new(AtomicBool::new(false));
        let stopped = Arc::clone(&shutdown);
        let thread = thread::Builder::new()
            .name(name.to_string())
            .spawn(move || {
                while let Ok(request) = request_rx.recv() {
                    if stopped.load(Ordering::Acquire) {
                        break;
                    }
                    if let Err(error) = result_tx.send(work(request)) {
                        abandon(error.0);
                        break;
                    }
                }
            })?;
        Ok(Self {
            requests: Some(request_tx),
            results: Some(result_rx),
            thread: Some(thread),
            in_flight: false,
            shutdown,
        })
    }

    /// Queues at most one request; a busy worker coalesces controller ticks.
    pub fn request(&mut self, request: Q) -> Result<bool, io::Error> {
        if self.in_flight {
            return Ok(false);
        }
        self.requests
            .as_ref()
            .ok_or_else(|| io::Error::other("control worker stopped"))?
            .try_send(request)
            .map_err(|_| io::Error::other("control worker request channel unavailable"))?;
        self.in_flight = true;
        Ok(true)
    }

    /// Nonblocking result handoff. No RPC or thread join occurs here.
    pub fn poll(&mut self) -> Result<Option<R>, io::Error> {
        match self
            .results
            .as_ref()
            .ok_or_else(|| io::Error::other("control worker stopped"))?
            .try_recv()
        {
            Ok(result) => {
                self.in_flight = false;
                Ok(Some(result))
            }
            Err(mpsc::TryRecvError::Empty) => Ok(None),
            Err(mpsc::TryRecvError::Disconnected) => {
                Err(io::Error::other("control worker stopped unexpectedly"))
            }
        }
    }

    /// Whether this worker already owns a pending request or result.
    pub const fn is_busy(&self) -> bool {
        self.in_flight
    }
}

impl<Q, R> PoolBackendControlWorker<Q, R> {
    /// Stops request intake and returns any unconsumed result to its worker.
    pub fn request_shutdown(&mut self) -> Result<(), io::Error> {
        self.shutdown.store(true, Ordering::Release);
        self.results.take();
        self.requests.take();
        if let Some(worker) = self.thread.take() {
            worker
                .join()
                .map_err(|_| io::Error::other("control worker panicked during shutdown"))?;
        }
        Ok(())
    }
}

impl<Q, R> Drop for PoolBackendControlWorker<Q, R> {
    fn drop(&mut self) {
        let _ = self.request_shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn late_or_expired_samples_cannot_reopen_a_generation() {
        let state = CurrentnessState::new(Instant::now());
        state.bits.fetch_or(CLOSED, Ordering::AcqRel);
        state.record(ZCASH_CURRENT, true);
        assert_eq!(state.live_bits(), 0);
        assert_ne!(state.bits.load(Ordering::Acquire) & CLOSED, 0);
        let state =
            CurrentnessState::new(Instant::now() - Duration::from_secs(NATIVE_JOB_MAX_AGE_SECONDS));
        state.record(ZCASH_CURRENT, true);
        assert_eq!(state.live_bits(), 0);
        assert_eq!(state.zcash_observation.load(Ordering::Acquire), 0);
    }

    #[test]
    fn independent_samples_preserve_one_chain_work_without_reviving_closed_work() {
        let state = CurrentnessState::new(Instant::now());
        state.record(WCASH_CURRENT, false);
        assert_eq!(state.live_bits(), ZCASH_CURRENT);
        assert!(state.wcash_observation.load(Ordering::Acquire) > 0);
        state.record(ZCASH_CURRENT, false);
        assert_eq!(state.live_bits(), 0);
        state.record(WCASH_CURRENT, true);
        assert_eq!(
            state.live_bits(),
            0,
            "neither-chain failure remains terminal"
        );
        state.bits.fetch_or(CLOSED, Ordering::AcqRel);
        state.record(ZCASH_CURRENT, true);
        assert_eq!(state.live_bits(), 0);
    }

    #[test]
    fn initial_observations_are_unknown_without_changing_initial_admission() {
        let state = CurrentnessState::new(Instant::now());
        assert_eq!(state.live_bits(), BOTH_CURRENT);
        let snapshot = state.observation_snapshot();
        assert_eq!(snapshot.status, NativeGenerationObservationStatus::Active);
        for chain in [snapshot.wcash, snapshot.zcash] {
            assert_eq!(chain.result, NativeCurrentnessObservationResult::Unknown);
            assert_eq!(chain.sample_age_ms, None);
        }
    }

    #[test]
    fn observation_samples_retain_independent_results_and_exact_ages() {
        let state = CurrentnessState::new(Instant::now());
        state.record_observation_at(
            WCASH_CURRENT,
            false,
            NativeCurrentnessObservationResult::TipMismatch,
            Duration::from_millis(2),
        );
        assert_eq!(state.live_bits(), ZCASH_CURRENT);
        let wcash = state.wcash_observation.load(Ordering::Acquire);
        let snapshot = state.observation_snapshot_at(wcash, 0, Duration::from_millis(7));
        assert_eq!(
            snapshot.wcash,
            NativeChainCurrentnessObservation {
                result: NativeCurrentnessObservationResult::TipMismatch,
                sample_age_ms: Some(5),
            }
        );
        assert_eq!(
            snapshot.zcash.result,
            NativeCurrentnessObservationResult::Unknown
        );

        state.record_observation_at(
            ZCASH_CURRENT,
            true,
            NativeCurrentnessObservationResult::Current,
            Duration::from_millis(4),
        );
        let snapshot = state.observation_snapshot_at(
            state.wcash_observation.load(Ordering::Acquire),
            state.zcash_observation.load(Ordering::Acquire),
            Duration::from_millis(9),
        );
        assert_eq!(snapshot.wcash.sample_age_ms, Some(7));
        assert_eq!(snapshot.zcash.sample_age_ms, Some(5));
        assert_eq!(
            snapshot.zcash.result,
            NativeCurrentnessObservationResult::Current
        );
        assert_eq!(state.live_bits(), ZCASH_CURRENT);
        assert_eq!(snapshot.status, NativeGenerationObservationStatus::Active);
    }

    #[test]
    fn wcash_only_observation_preserves_the_other_chains_failed_sample() {
        let state = CurrentnessState::new(Instant::now());
        state.record_observation_at(
            WCASH_CURRENT,
            true,
            NativeCurrentnessObservationResult::Current,
            Duration::ZERO,
        );
        state.record_observation_at(
            ZCASH_CURRENT,
            false,
            NativeCurrentnessObservationResult::ProbeFailed,
            Duration::from_millis(1),
        );
        let snapshot = state.observation_snapshot_at(
            state.wcash_observation.load(Ordering::Acquire),
            state.zcash_observation.load(Ordering::Acquire),
            Duration::from_millis(8),
        );
        assert_eq!(state.live_bits(), WCASH_CURRENT);
        assert_eq!(
            snapshot.wcash.result,
            NativeCurrentnessObservationResult::Current
        );
        assert_eq!(snapshot.wcash.sample_age_ms, Some(8));
        assert_eq!(
            snapshot.zcash.result,
            NativeCurrentnessObservationResult::ProbeFailed
        );
        assert_eq!(snapshot.zcash.sample_age_ms, Some(7));
        assert_eq!(snapshot.status.label(), "active");
    }

    #[test]
    fn only_proven_tip_errors_are_observed_as_mismatch() {
        let secret = "PRIVATE_ENDPOINT_OR_ERROR";
        for error in [
            MinerError::ChildTipMismatch {
                expected: secret.into(),
                endpoint: secret.into(),
                actual: secret.into(),
            },
            MinerError::ParentTipMismatch {
                expected: secret.into(),
                endpoint: secret.into(),
                actual: secret.into(),
            },
        ] {
            assert_eq!(
                NativeCurrentnessObservationResult::from_result(&Err(error)),
                NativeCurrentnessObservationResult::TipMismatch
            );
        }
        for error in [
            MinerError::RpcProtocol(secret.into()),
            MinerError::RpcError {
                endpoint: secret.into(),
                code: Some(-32001),
                message: secret.into(),
            },
        ] {
            let state = CurrentnessState::new(Instant::now());
            state.record_result(WCASH_CURRENT, &Err(error));
            let snapshot = state.observation_snapshot();
            assert_eq!(
                snapshot.wcash.result,
                NativeCurrentnessObservationResult::ProbeFailed
            );
            assert_eq!(state.live_bits(), ZCASH_CURRENT);
            assert!(!snapshot.wcash.result.label().contains(secret));
        }
        assert_eq!(
            NativeCurrentnessObservationResult::from_result(&Ok(())),
            NativeCurrentnessObservationResult::Current
        );
    }

    #[test]
    fn closed_and_expired_snapshots_never_report_live_admission() {
        let state = CurrentnessState::new(Instant::now());
        state.record_observation_at(
            ZCASH_CURRENT,
            true,
            NativeCurrentnessObservationResult::Current,
            Duration::from_millis(3),
        );
        state.bits.fetch_or(CLOSED, Ordering::AcqRel);
        state.record(ZCASH_CURRENT, true);
        let zcash = state.zcash_observation.load(Ordering::Acquire);
        let closed = state.observation_snapshot_at(0, zcash, Duration::from_millis(5));
        assert_eq!(closed.status, NativeGenerationObservationStatus::Closed);
        assert_eq!(
            closed.zcash.result,
            NativeCurrentnessObservationResult::Current
        );
        assert_eq!(closed.zcash.sample_age_ms, Some(2));
        assert_eq!(state.live_bits(), 0);

        let expired = state.observation_snapshot_at(
            0,
            zcash,
            Duration::from_secs(NATIVE_JOB_MAX_AGE_SECONDS),
        );
        assert_eq!(expired.status, NativeGenerationObservationStatus::Expired);
        assert_eq!(
            expired.generation_age_ms,
            NATIVE_JOB_MAX_AGE_SECONDS * 1_000
        );
        assert_eq!(
            expired.zcash.sample_age_ms,
            Some(NATIVE_JOB_MAX_AGE_SECONDS * 1_000 - 3)
        );
        assert_eq!(
            decode_observation(zcash, Duration::ZERO).sample_age_ms,
            None
        );
        assert_eq!(
            decode_observation(zcash, Duration::ZERO).result,
            NativeCurrentnessObservationResult::Unknown
        );
    }

    #[test]
    fn concurrent_publication_never_tears_a_result_and_its_completion_time() {
        let state = Arc::new(CurrentnessState::new(Instant::now()));
        let gate = Arc::new(std::sync::Barrier::new(2));
        let done = Arc::new(AtomicBool::new(false));
        let writer = {
            let state = Arc::clone(&state);
            let gate = Arc::clone(&gate);
            let done = Arc::clone(&done);
            thread::spawn(move || {
                gate.wait();
                for _ in 0..10_000 {
                    state.record_observation_at(
                        WCASH_CURRENT,
                        true,
                        NativeCurrentnessObservationResult::Current,
                        Duration::from_millis(1),
                    );
                    state.record_observation_at(
                        WCASH_CURRENT,
                        false,
                        NativeCurrentnessObservationResult::TipMismatch,
                        Duration::from_millis(2),
                    );
                }
                done.store(true, Ordering::Release);
            })
        };
        gate.wait();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let sample = state.wcash_observation.load(Ordering::Acquire);
            let observation = decode_observation(sample, Duration::from_millis(3));
            match observation.result {
                NativeCurrentnessObservationResult::Unknown => {
                    assert_eq!(observation.sample_age_ms, None);
                }
                NativeCurrentnessObservationResult::Current => {
                    assert_eq!(observation.sample_age_ms, Some(2));
                }
                NativeCurrentnessObservationResult::TipMismatch => {
                    assert_eq!(observation.sample_age_ms, Some(1));
                }
                NativeCurrentnessObservationResult::ProbeFailed => {
                    panic!("the fixture publishes no probe failures");
                }
            }
            if done.load(Ordering::Acquire) {
                break;
            }
            assert!(Instant::now() < deadline);
            thread::yield_now();
        }
        writer.join().unwrap();
        assert_eq!(state.live_bits(), ZCASH_CURRENT);
    }

    #[test]
    fn delayed_publication_cannot_replace_a_newer_observation() {
        let state = CurrentnessState::new(Instant::now());
        state.record_observation_at(
            WCASH_CURRENT,
            false,
            NativeCurrentnessObservationResult::ProbeFailed,
            Duration::from_millis(4),
        );
        state.record_observation_at(
            WCASH_CURRENT,
            true,
            NativeCurrentnessObservationResult::Current,
            Duration::from_millis(2),
        );
        let observation = decode_observation(
            state.wcash_observation.load(Ordering::Acquire),
            Duration::from_millis(9),
        );
        assert_eq!(
            observation.result,
            NativeCurrentnessObservationResult::ProbeFailed
        );
        assert_eq!(observation.sample_age_ms, Some(5));
        // Observation publication is independent of the unchanged admission
        // update; it does not restore or override any admission bit.
        assert_eq!(state.live_bits(), BOTH_CURRENT);

        state.record_observation_at(
            WCASH_CURRENT,
            true,
            NativeCurrentnessObservationResult::Current,
            Duration::from_millis(4),
        );
        assert_eq!(
            decode_observation(
                state.wcash_observation.load(Ordering::Acquire),
                Duration::from_millis(9)
            ),
            observation,
            "equal-time positive publication cannot hide a failure"
        );
    }

    #[test]
    fn held_wcash_observation_never_blocks_a_zcash_sample_or_snapshot() {
        let state = Arc::new(CurrentnessState::new(Instant::now()));
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let wcash = {
            let state = Arc::clone(&state);
            thread::spawn(move || {
                entered_tx.send(()).unwrap();
                release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                state.record(WCASH_CURRENT, true);
            })
        };
        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        state.record_result(ZCASH_CURRENT, &Ok(()));
        let snapshot = state.observation_snapshot();
        // Always release and join owned work before asserting observations.
        release_tx.send(()).unwrap();
        wcash.join().unwrap();
        assert_eq!(
            snapshot.wcash.result,
            NativeCurrentnessObservationResult::Unknown
        );
        assert_eq!(snapshot.wcash.sample_age_ms, None);
        assert_eq!(
            snapshot.zcash.result,
            NativeCurrentnessObservationResult::Current
        );
        assert!(snapshot.zcash.sample_age_ms.is_some());
        assert_eq!(state.live_bits(), BOTH_CURRENT);
    }

    #[test]
    fn held_preparation_and_wec_probe_leave_zec_samples_and_controller_responsive() {
        let (prep_entered_tx, prep_entered) = mpsc::channel();
        let (prep_release, prep_gate) = mpsc::channel();
        let abandoned = Arc::new(AtomicUsize::new(0));
        let abandoned_worker = abandoned.clone();
        let mut prep = PoolBackendControlWorker::spawn(
            "fixture-preparation",
            move |()| {
                prep_entered_tx.send(()).unwrap();
                prep_gate.recv_timeout(Duration::from_secs(5)).unwrap();
                7u32
            },
            move |value| {
                assert_eq!(value, 7);
                abandoned_worker.fetch_add(1, Ordering::AcqRel);
            },
        )
        .unwrap();
        let (wec_entered_tx, wec_entered) = mpsc::channel();
        let (wec_release, wec_gate) = mpsc::channel();
        let mut wec = PoolBackendControlWorker::spawn(
            "fixture-wec",
            move |()| {
                wec_entered_tx.send(()).unwrap();
                wec_gate.recv_timeout(Duration::from_secs(5)).unwrap();
            },
            |_| {},
        )
        .unwrap();
        let samples = Arc::new(AtomicUsize::new(0));
        let worker_samples = samples.clone();
        let mut zec = PoolBackendControlWorker::spawn(
            "fixture-zec",
            move |()| {
                worker_samples.fetch_add(1, Ordering::AcqRel);
            },
            |_| {},
        )
        .unwrap();
        assert!(prep.request(()).unwrap());
        assert!(wec.request(()).unwrap());
        prep_entered.recv_timeout(Duration::from_secs(1)).unwrap();
        wec_entered.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(!prep.request(()).unwrap());
        for _ in 0..2 {
            assert!(zec.request(()).unwrap());
            let deadline = Instant::now() + Duration::from_secs(1);
            while zec.poll().unwrap().is_none() {
                assert!(Instant::now() < deadline);
                thread::yield_now();
            }
        }
        assert_eq!(samples.load(Ordering::Acquire), 2);
        assert!(prep.poll().unwrap().is_none());
        assert!(wec.poll().unwrap().is_none());
        // Controller shutdown closes the result receiver before releasing an
        // in-flight preparation. Its exact owned result is abandoned once.
        prep.results.take();
        prep_release.send(()).unwrap();
        wec_release.send(()).unwrap();
        prep.request_shutdown().unwrap();
        wec.request_shutdown().unwrap();
        zec.request_shutdown().unwrap();
        assert_eq!(abandoned.load(Ordering::Acquire), 1);
    }
}
