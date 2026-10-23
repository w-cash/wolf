//! Bounded winner submission and audit lanes, independent for each merged chain.
//!
//! The durable journal remains the queue. No journal/actor/claim mutex is held
//! across reconciliation RPCs, and one chain cannot occupy another's worker.

use std::{
    error::Error,
    io,
    panic::{self, AssertUnwindSafe},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc, Arc, Mutex,
    },
    thread,
};

use wcash_pool_protocol::MergedChain;

use crate::{
    MinerError, NativeMiningSupervisor, PoolBackendActor, PoolBackendActorError,
    PoolBackendLiveWinnerScheduler, PoolBackendWinnerAuditScheduler, PoolBackendWinnerKey,
    PoolBackendWinnerSnapshot,
};

type ReconcileWinner = dyn Fn(&PoolBackendActor, &PoolBackendWinnerSnapshot) -> Result<(), String>
    + Send
    + Sync
    + 'static;

/// Four fixed workers: a live submission lane and paced audit lane per chain.
pub struct PoolBackendWinnerWorkers {
    shutdown: Arc<AtomicBool>,
    threads: Vec<thread::JoinHandle<()>>,
}

struct PoolBackendWinnerClaims {
    // Four workers can each own at most one key. This is not a winner queue.
    keys: Mutex<Vec<PoolBackendWinnerKey>>,
}

impl Default for PoolBackendWinnerClaims {
    fn default() -> Self {
        Self {
            keys: Mutex::new(Vec::with_capacity(4)),
        }
    }
}

struct PoolBackendWinnerClaim {
    claims: Arc<PoolBackendWinnerClaims>,
    key: PoolBackendWinnerKey,
}

impl PoolBackendWinnerClaims {
    fn claim(
        self: &Arc<Self>,
        key: PoolBackendWinnerKey,
    ) -> Result<Option<PoolBackendWinnerClaim>, io::Error> {
        let mut keys = self
            .keys
            .lock()
            .map_err(|_| io::Error::other("winner reconciliation claims mutex is poisoned"))?;
        if keys.contains(&key) {
            return Ok(None);
        }
        keys.push(key.clone());
        Ok(Some(PoolBackendWinnerClaim {
            claims: Arc::clone(self),
            key,
        }))
    }
}

impl Drop for PoolBackendWinnerClaim {
    fn drop(&mut self) {
        if let Ok(mut keys) = self.claims.keys.lock() {
            if let Some(position) = keys.iter().position(|key| key == &self.key) {
                keys.swap_remove(position);
            }
        }
    }
}

impl PoolBackendWinnerWorkers {
    /// Starts independent Wcash and Zcash submission and audit lanes.
    ///
    /// Registration and all four thread creations finish before any lane can
    /// issue an RPC. Partial startup failures stop and join every created lane.
    pub fn spawn(
        supervisor: Arc<NativeMiningSupervisor>,
        actor: Arc<PoolBackendActor>,
        shutdown: Arc<AtomicBool>,
        failure: mpsc::Sender<String>,
    ) -> Result<Self, io::Error> {
        let reconcile: Arc<ReconcileWinner> = Arc::new(move |actor, snapshot| {
            reconcile_pool_backend_snapshot(&supervisor, actor, snapshot)
        });
        Self::spawn_with_reconciler(actor, shutdown, failure, reconcile)
    }

    fn spawn_with_reconciler(
        actor: Arc<PoolBackendActor>,
        shutdown: Arc<AtomicBool>,
        failure: mpsc::Sender<String>,
        reconcile: Arc<ReconcileWinner>,
    ) -> Result<Self, io::Error> {
        let mut workers = Self {
            shutdown,
            threads: Vec::with_capacity(4),
        };
        let ready = Arc::new(AtomicBool::new(false));
        let claims = Arc::new(PoolBackendWinnerClaims::default());
        let mut live_wakes = Vec::with_capacity(2);

        for chain in [MergedChain::Wcash, MergedChain::Zcash] {
            let audit_before = Arc::new(AtomicUsize::new(0));
            for live in [true, false] {
                let actor = Arc::clone(&actor);
                let shutdown = Arc::clone(&workers.shutdown);
                let ready = Arc::clone(&ready);
                let claims = Arc::clone(&claims);
                let audit_before = Arc::clone(&audit_before);
                let failure = failure.clone();
                let reconcile = Arc::clone(&reconcile);
                let name = format!(
                    "{}-winner-{}",
                    match chain {
                        MergedChain::Wcash => "wcash",
                        MergedChain::Zcash => "zcash",
                    },
                    if live { "live" } else { "audit" },
                );
                let thread = thread::Builder::new().name(name.clone()).spawn(move || {
                    while !ready.load(Ordering::Acquire) {
                        if shutdown.load(Ordering::Acquire) {
                            return;
                        }
                        thread::park();
                    }
                    let result = panic::catch_unwind(AssertUnwindSafe(|| {
                        if live {
                            run_live_lane(
                                chain,
                                &actor,
                                &shutdown,
                                &claims,
                                &audit_before,
                                reconcile.as_ref(),
                            )
                        } else {
                            run_audit_lane(
                                chain,
                                &actor,
                                &shutdown,
                                &claims,
                                &audit_before,
                                reconcile.as_ref(),
                            )
                        }
                    }));
                    match result {
                        Ok(Ok(())) => {}
                        Ok(Err(error)) => {
                            let _ = failure.send(error);
                        }
                        Err(payload) => {
                            let _ = failure.send(format!("{name} worker panicked"));
                            panic::resume_unwind(payload);
                        }
                    }
                })?;
                if live {
                    live_wakes.push(thread.thread().clone());
                }
                workers.threads.push(thread);
            }
        }

        actor.register_winner_workers(live_wakes).map_err(|error| {
            io::Error::other(format!("live winner worker registration failed: {error}"))
        })?;
        ready.store(true, Ordering::Release);
        for worker in &workers.threads {
            worker.thread().unpark();
        }
        Ok(workers)
    }

    /// Stops and joins every lane, including when an earlier join found a panic.
    /// In-flight reconciliation retains the configured bounded RPC timeouts.
    pub fn request_shutdown(&mut self) -> Result<(), io::Error> {
        self.shutdown.store(true, Ordering::Release);
        for worker in &self.threads {
            worker.thread().unpark();
        }
        let mut panicked = false;
        for worker in self.threads.drain(..) {
            panicked |= worker.join().is_err();
        }
        if panicked {
            Err(io::Error::other(
                "winner reconciliation worker panicked during shutdown",
            ))
        } else {
            Ok(())
        }
    }
}

impl Drop for PoolBackendWinnerWorkers {
    fn drop(&mut self) {
        let _ = self.request_shutdown();
    }
}

fn run_live_lane(
    chain: MergedChain,
    actor: &PoolBackendActor,
    shutdown: &AtomicBool,
    claims: &Arc<PoolBackendWinnerClaims>,
    audit_before: &AtomicUsize,
    reconcile: &ReconcileWinner,
) -> Result<(), String> {
    let mut scheduler = PoolBackendLiveWinnerScheduler::for_chain(chain);
    while !shutdown.load(Ordering::Acquire) {
        let snapshot = scheduler
            .next(actor)
            .map_err(|error| format!("live winner scheduling failed: {error}"))?;
        let Some(snapshot) = snapshot else {
            audit_before.store(scheduler.audit_before(), Ordering::Release);
            if scheduler.has_pending_scan() {
                // A bounded mixed-chain scan can contain only the other chain.
                // Its tail is already durable; no future wakeup is required.
                thread::yield_now();
            } else {
                thread::park();
            }
            continue;
        };
        let Some(_claim) = claims
            .claim(snapshot.key())
            .map_err(|error| error.to_string())?
        else {
            continue;
        };
        audit_before.store(scheduler.audit_before(), Ordering::Release);
        reconcile(actor, &snapshot)?;
    }
    Ok(())
}

fn run_audit_lane(
    chain: MergedChain,
    actor: &PoolBackendActor,
    shutdown: &AtomicBool,
    claims: &Arc<PoolBackendWinnerClaims>,
    audit_before: &AtomicUsize,
    reconcile: &ReconcileWinner,
) -> Result<(), String> {
    let mut scheduler = PoolBackendWinnerAuditScheduler::for_chain(chain);
    while !shutdown.load(Ordering::Acquire) {
        let work = scheduler
            .next(actor, audit_before.load(Ordering::Acquire))
            .map_err(|error| format!("winner audit scheduling failed: {error}"))?;
        if let Some(snapshot) = work.snapshot() {
            if let Some(_claim) = claims
                .claim(snapshot.key())
                .map_err(|error| error.to_string())?
            {
                reconcile(actor, snapshot)?;
            }
        }
        thread::park_timeout(work.delay());
    }
    Ok(())
}

fn reconcile_pool_backend_snapshot(
    supervisor: &NativeMiningSupervisor,
    actor: &PoolBackendActor,
    snapshot: &PoolBackendWinnerSnapshot,
) -> Result<(), String> {
    match supervisor.reconcile_pool_backend_winner(snapshot) {
        Ok(Some(transition)) => {
            match actor.compare_and_apply_winner_transition(snapshot, transition) {
                Ok(_) | Err(PoolBackendActorError::WinnerRevisionConflict) => Ok(()),
                Err(_) => {
                    crate::record_native_winner_persistence_failure(snapshot);
                    Err(
                        "winner lifecycle persistence failed; inspect bounded winner diagnostics"
                            .into(),
                    )
                }
            }
        }
        Ok(None) => Ok(()),
        Err(error) if is_retryable_winner_reconciliation_error(error.miner_error()) => {
            crate::record_native_winner_reconciliation(snapshot, Some(&error));
            Ok(())
        }
        Err(error) => {
            crate::record_native_winner_reconciliation(snapshot, Some(&error));
            Err("winner reconciliation failed closed; inspect bounded winner diagnostics".into())
        }
    }
}

/// Preserves the existing fail-closed winner reconciliation retry allowlist.
/// Protocol, identity, payout, invalid-proof and persistence errors stay fatal.
pub fn is_retryable_winner_reconciliation_error(error: &MinerError) -> bool {
    match error {
        MinerError::WinnerSubmissionDeferred { .. } => true,
        MinerError::RpcError {
            code: Some(-1 | -5 | -9 | -10 | -28 | -32_001),
            ..
        } => true,
        MinerError::RpcTransport(error) => {
            if error.is_timeout() {
                return true;
            }
            let mut source = error.source();
            while let Some(cause) = source {
                if cause
                    .downcast_ref::<io::Error>()
                    .is_some_and(|error| is_retryable_io_kind(error.kind()))
                {
                    return true;
                }
                source = cause.source();
            }
            false
        }
        MinerError::RpcHttpStatus(status) => {
            matches!(status.as_u16(), 408 | 425 | 429 | 500 | 502 | 503 | 504)
        }
        MinerError::Io(error) => is_retryable_io_kind(error.kind()),
        MinerError::ParentTipMismatch { .. }
        | MinerError::ChildTipMismatch { .. }
        | MinerError::StaleNativeJob(_) => true,
        _ => false,
    }
}

const fn is_retryable_io_kind(kind: io::ErrorKind) -> bool {
    matches!(
        kind,
        io::ErrorKind::ConnectionRefused
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::HostUnreachable
            | io::ErrorKind::NetworkUnreachable
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::NotConnected
            | io::ErrorKind::NetworkDown
            | io::ErrorKind::BrokenPipe
            | io::ErrorKind::WouldBlock
            | io::ErrorKind::TimedOut
            | io::ErrorKind::Interrupted
    )
}

#[cfg(test)]
mod tests;
