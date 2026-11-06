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
    zcash_recovery::{NativePoolZcashRecoveryAuthority, PoolBackendZcashWorkObservation},
    MinerError, NATIVE_JOB_MAX_AGE_SECONDS,
};

const WCASH_CURRENT: u8 = 1;
const ZCASH_CURRENT: u8 = 2;
const CLOSED: u8 = 4;
const BOTH_CURRENT: u8 = WCASH_CURRENT | ZCASH_CURRENT;

struct CurrentnessState {
    created_at: Instant,
    bits: AtomicU8,
    // Monotonic completion timestamps relative to this exact generation.
    // Separate atomics mean WEC completion can never hold up a ZEC sample.
    wcash_sample_micros: AtomicU64,
    zcash_sample_micros: AtomicU64,
}

impl CurrentnessState {
    fn new(created_at: Instant) -> Self {
        Self {
            created_at,
            bits: AtomicU8::new(BOTH_CURRENT),
            wcash_sample_micros: AtomicU64::new(0),
            zcash_sample_micros: AtomicU64::new(0),
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
        let elapsed = self.created_at.elapsed();
        if elapsed >= Duration::from_secs(NATIVE_JOB_MAX_AGE_SECONDS) {
            return;
        }
        // The CLOSED bit and current bits share one atomic. A response racing
        // closure cannot clear CLOSED or reopen this generation.
        let _ = self
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
        let completed = u64::try_from(elapsed.as_micros())
            .unwrap_or(u64::MAX)
            .max(1);
        let timestamp = if chain == WCASH_CURRENT {
            &self.wcash_sample_micros
        } else {
            &self.zcash_sample_micros
        };
        timestamp.store(completed, Ordering::Release);
    }
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
        self.state.record(WCASH_CURRENT, result.is_ok());
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
        self.state.record(ZCASH_CURRENT, result.is_ok());
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
    zcash: PoolBackendControlWorker<Option<NativePoolCurrentnessProbe>, ()>,
    recovery_authority: Option<NativePoolZcashRecoveryAuthority>,
    next_wcash: Instant,
    next_zcash: Instant,
}

impl NativePoolCurrentnessWorkers {
    /// Starts exactly two lanes; no per-controller-tick thread creation.
    pub fn spawn() -> Result<Self, io::Error> {
        Self::spawn_inner(None)
    }

    /// Reuses the ZEC lane for authenticated recovery evidence even without a live job.
    /// Mainnet reuses the same quorum poll. Opt-in lagging Testnet keeps its
    /// separate ordinary-admission policy; recovery remains strict.
    pub fn spawn_with_zcash_recovery_authority(
        authority: NativePoolZcashRecoveryAuthority,
    ) -> Result<Self, io::Error> {
        Self::spawn_inner(Some(authority))
    }

    fn spawn_inner(
        recovery_authority: Option<NativePoolZcashRecoveryAuthority>,
    ) -> Result<Self, io::Error> {
        let refreshing = recovery_authority.clone();
        Ok(Self {
            wcash: PoolBackendControlWorker::spawn(
                "wcash-currentness",
                |probe: NativePoolCurrentnessProbe| probe.check_wcash(),
                |_| {},
            )?,
            zcash: PoolBackendControlWorker::spawn(
                "zcash-currentness",
                move |probe: Option<NativePoolCurrentnessProbe>| {
                    if let Some(authority) = refreshing.as_ref() {
                        authority.refresh();
                        if let Some(probe) = probe {
                            if probe.zcash.uses_lagging_validator_policy() {
                                probe.check_zcash();
                            } else {
                                probe.state.record(
                                    ZCASH_CURRENT,
                                    authority.observe_tip(probe.zcash_tip)
                                        == PoolBackendZcashWorkObservation::Current,
                                );
                            }
                        }
                    } else if let Some(probe) = probe {
                        probe.check_zcash();
                    }
                },
                |_| {},
            )?,
            recovery_authority,
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
        }
        if (probe.is_some() || self.recovery_authority.is_some())
            && !self.zcash.is_busy()
            && Instant::now() >= self.next_zcash
        {
            self.zcash.request(probe.cloned())?;
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
        if let Some(authority) = self.recovery_authority.as_ref() {
            authority.close();
        }
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
        work: impl FnMut(Q) -> R + Send + 'static,
        abandon: impl FnMut(R) + Send + 'static,
    ) -> Result<Self, io::Error> {
        Self::spawn_with_owned_requests(name, work, abandon, drop)
    }

    /// Preserves accepted request ownership if shutdown happens before work starts.
    pub fn spawn_with_owned_requests(
        name: &str,
        mut work: impl FnMut(Q) -> R + Send + 'static,
        mut abandon: impl FnMut(R) + Send + 'static,
        mut abandon_request: impl FnMut(Q) + Send + 'static,
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
                        abandon_request(request);
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

    /// Returns the exact request owner when this bounded handoff cannot accept it.
    pub fn request_owned(&mut self, request: Q) -> Result<(), (Q, io::Error)> {
        if self.in_flight {
            return Err((request, io::Error::other("control worker is busy")));
        }
        let Some(sender) = self.requests.as_ref() else {
            return Err((request, io::Error::other("control worker stopped")));
        };
        match sender.try_send(request) {
            Ok(()) => {
                self.in_flight = true;
                Ok(())
            }
            Err(mpsc::TrySendError::Full(request) | mpsc::TrySendError::Disconnected(request)) => {
                Err((
                    request,
                    io::Error::other("control worker request channel unavailable"),
                ))
            }
        }
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
    #[path = "owned_prefetch_tests.rs"]
    mod owned_prefetch_tests;
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
        assert_eq!(state.zcash_sample_micros.load(Ordering::Acquire), 0);
    }

    #[test]
    fn independent_samples_preserve_one_chain_work_without_reviving_closed_work() {
        let state = CurrentnessState::new(Instant::now());
        state.record(WCASH_CURRENT, false);
        assert_eq!(state.live_bits(), ZCASH_CURRENT);
        assert!(state.wcash_sample_micros.load(Ordering::Acquire) > 0);
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
