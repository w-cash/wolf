use std::{sync::Condvar, time::Duration};

use super::*;
use crate::{
    pool_backend_actor::tests::independent_submission::IndependentWinnerFixture,
    PoolBackendWinnerTransition,
};
use wcash_pool_protocol::ChainTip;

const DEADLINE: Duration = Duration::from_secs(5);

/// A blocked mock transport with a finite deadline and unconditional release.
/// Tests call the actual production worker/scheduler/journal code around it.
#[derive(Default)]
struct ReleaseGate {
    released: Mutex<bool>,
    changed: Condvar,
}

impl ReleaseGate {
    fn wait(&self) -> Result<(), String> {
        let released = self.released.lock().unwrap();
        let (released, timeout) = self
            .changed
            .wait_timeout_while(released, DEADLINE, |released| !*released)
            .unwrap();
        if timeout.timed_out() && !*released {
            Err("test reconciliation gate timed out".into())
        } else {
            Ok(())
        }
    }

    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.changed.notify_all();
    }
}

impl Drop for ReleaseGate {
    fn drop(&mut self) {
        self.release();
    }
}

fn observe(actor: &PoolBackendActor, snapshot: &PoolBackendWinnerSnapshot) -> Result<(), String> {
    let winner = snapshot.winner();
    actor
        .compare_and_apply_winner_transition(
            snapshot,
            PoolBackendWinnerTransition::Observed {
                tip: ChainTip {
                    block_hash_le: winner.block_hash_le.clone(),
                    height: winner.height,
                },
                confirmations: 1,
            },
        )
        .map(|_| ())
        .map_err(|error| error.to_string())
}

fn independent_dispatch(blocked_chain: MergedChain, recovered: bool) {
    let fixture = if recovered {
        IndependentWinnerFixture::retained_dual()
    } else {
        IndependentWinnerFixture::empty()
    };
    let expected_wcash = fixture.wcash_bytes.clone();
    let expected_zcash = fixture.zcash_bytes.clone();
    let gate = Arc::new(ReleaseGate::default());
    let callback_gate = Arc::clone(&gate);
    let (entered_tx, entered_rx) = mpsc::channel();
    let (failure_tx, failure_rx) = mpsc::channel();
    let active_blocked = Arc::new(AtomicUsize::new(0));
    let callback_active = Arc::clone(&active_blocked);
    let reconcile: Arc<ReconcileWinner> = Arc::new(move |actor, snapshot| {
        let chain = snapshot.winner().chain;
        assert_eq!(
            snapshot.block_bytes(),
            match chain {
                MergedChain::Wcash => &expected_wcash,
                MergedChain::Zcash => &expected_zcash,
            }
        );
        if chain == blocked_chain {
            assert_eq!(
                callback_active.fetch_add(1, Ordering::SeqCst),
                0,
                "audit cannot reconcile the live lane's claimed exact key concurrently"
            );
            entered_tx.send(chain).unwrap();
            callback_gate.wait()?;
            callback_active.fetch_sub(1, Ordering::SeqCst);
        } else {
            entered_tx.send(chain).unwrap();
        }
        observe(actor, snapshot)
    });
    let shutdown = Arc::new(AtomicBool::new(false));
    let mut workers = PoolBackendWinnerWorkers::spawn_with_reconciler(
        Arc::clone(&fixture.actor),
        shutdown,
        failure_tx,
        reconcile,
    )
    .unwrap();
    assert_eq!(workers.threads.len(), 4);
    if !recovered {
        fixture.append(2, true, true);
    }
    // Capture results before assertions and release before worker Drop. Even
    // under the old serialized behavior a failing test exits promptly/safely.
    let first = entered_rx.recv_timeout(DEADLINE);
    let second = entered_rx.recv_timeout(DEADLINE);
    // Wake both real audit lanes after publication of the live bounds, rather
    // than depending on their initial idle one-second pacing interval.
    workers.threads[1].thread().unpark();
    workers.threads[3].thread().unpark();
    // Give the audit lane an opportunity to contend for the held exact key.
    let extra = entered_rx.recv_timeout(Duration::from_millis(250));
    gate.release();
    let stopped = workers.request_shutdown();
    assert!(
        stopped.is_ok(),
        "all actual worker lanes must stop cleanly: {stopped:?}"
    );
    let mut entered = vec![first.unwrap(), second.unwrap()];
    entered.sort_by_key(|chain| match chain {
        MergedChain::Wcash => 0,
        MergedChain::Zcash => 1,
    });
    assert_eq!(
        entered,
        [MergedChain::Wcash, MergedChain::Zcash],
        "the other chain must enter reconciliation before the blocked chain is released"
    );
    // The unblocked chain can be audited again after the normal pacing delay;
    // a second blocked entry would prove overlapping same-key reconciliation.
    if let Ok(chain) = extra {
        assert_ne!(chain, blocked_chain);
    }
    assert!(
        failure_rx.try_recv().is_err(),
        "normal transport blocking is not a fatal worker failure"
    );
    assert!(workers.threads.is_empty());
}

#[test]
fn blocked_wcash_submission_does_not_delay_same_proof_zcash_submission() {
    independent_dispatch(MergedChain::Wcash, false);
}

#[test]
fn blocked_zcash_submission_does_not_delay_same_proof_wcash_submission() {
    independent_dispatch(MergedChain::Zcash, false);
}

#[test]
fn recovered_dual_proofs_dispatch_independently_after_journal_reopen() {
    independent_dispatch(MergedChain::Wcash, true);
}

#[test]
fn prior_wcash_receipt_blocks_only_its_own_live_lane_and_not_later_zcash() {
    let fixture = IndependentWinnerFixture::empty();
    let gate = Arc::new(ReleaseGate::default());
    let callback_gate = Arc::clone(&gate);
    let (entered_tx, entered_rx) = mpsc::channel();
    let (failure_tx, failure_rx) = mpsc::channel();
    let reconcile: Arc<ReconcileWinner> = Arc::new(move |actor, snapshot| {
        entered_tx
            .send((snapshot.winner().chain, snapshot.share_id().clone()))
            .unwrap();
        if snapshot.share_id().as_bytes()[..4] == 1u32.to_le_bytes() {
            callback_gate.wait()?;
        }
        observe(actor, snapshot)
    });
    let mut workers = PoolBackendWinnerWorkers::spawn_with_reconciler(
        Arc::clone(&fixture.actor),
        Arc::new(AtomicBool::new(false)),
        failure_tx,
        reconcile,
    )
    .unwrap();
    let first_child = fixture.append(1, true, false);
    let first_entry = entered_rx.recv_timeout(DEADLINE);
    // Distinct durable receipt keys intentionally reuse the static proof bytes;
    // this tests queue isolation, not distinct newly solved hardware shares.
    let later_child = fixture.append(2, true, false);
    let later_parent = fixture.append(3, false, true);
    let mut later_parent_entered = false;
    let mut later_child_entered_before_release = false;
    let deadline = std::time::Instant::now() + DEADLINE;
    while std::time::Instant::now() < deadline {
        match entered_rx.recv_timeout(deadline.saturating_duration_since(std::time::Instant::now()))
        {
            Ok((chain, share)) if chain == MergedChain::Zcash && share == later_parent => {
                later_parent_entered = true;
                break;
            }
            Ok((_, share)) if share == later_child => later_child_entered_before_release = true,
            Ok(_) => {}
            Err(_) => break,
        }
    }
    gate.release();
    let mut later_child_entered = later_child_entered_before_release;
    let deadline = std::time::Instant::now() + DEADLINE;
    while !later_child_entered && std::time::Instant::now() < deadline {
        match entered_rx.recv_timeout(deadline.saturating_duration_since(std::time::Instant::now()))
        {
            Ok((MergedChain::Wcash, share)) if share == later_child => later_child_entered = true,
            Ok(_) => {}
            Err(_) => break,
        }
    }
    let stopped = workers.request_shutdown();
    assert_eq!(first_entry.unwrap(), (MergedChain::Wcash, first_child));
    assert!(
        later_parent_entered,
        "later Zcash work must run before the prior Wcash transport is released"
    );
    assert!(
        !later_child_entered_before_release,
        "a chain's live worker stays serial and bounded"
    );
    assert!(
        later_child_entered,
        "same-chain later work resumes when its prior request returns"
    );
    assert!(stopped.is_ok());
    assert!(failure_rx.try_recv().is_err());
}

fn audit_isolation(blocked_chain: MergedChain) {
    let fixture = IndependentWinnerFixture::retained_dual();
    let gate = Arc::new(ReleaseGate::default());
    let callback_gate = Arc::clone(&gate);
    let blocked = Arc::new(AtomicBool::new(false));
    let callback_blocked = Arc::clone(&blocked);
    let (entered_tx, entered_rx) = mpsc::channel();
    let (failure_tx, failure_rx) = mpsc::channel();
    let reconcile: Arc<ReconcileWinner> = Arc::new(move |_, snapshot| {
        let chain = snapshot.winner().chain;
        let is_audit = thread::current()
            .name()
            .is_some_and(|name| name.ends_with("-audit"));
        if chain == blocked_chain && is_audit {
            callback_blocked.store(true, Ordering::Release);
            entered_tx
                .send((chain, snapshot.share_id().clone(), is_audit, true))
                .unwrap();
            callback_gate.wait()?;
        } else {
            entered_tx
                .send((
                    chain,
                    snapshot.share_id().clone(),
                    is_audit,
                    callback_blocked.load(Ordering::Acquire),
                ))
                .unwrap();
        }
        // Retain Pending to exercise the actual submission retry tier rather
        // than turning the audit into a canonical-confirmation fixture.
        Ok(())
    });
    let mut workers = PoolBackendWinnerWorkers::spawn_with_reconciler(
        Arc::clone(&fixture.actor),
        Arc::new(AtomicBool::new(false)),
        failure_tx,
        reconcile,
    )
    .unwrap();
    let mut audit_blocked = false;
    let deadline = std::time::Instant::now() + DEADLINE;
    while std::time::Instant::now() < deadline {
        match entered_rx.recv_timeout(deadline.saturating_duration_since(std::time::Instant::now()))
        {
            Ok((chain, _, true, true)) if chain == blocked_chain => {
                audit_blocked = true;
                break;
            }
            Ok(_) => {}
            Err(_) => break,
        }
    }
    let other_chain = match blocked_chain {
        MergedChain::Wcash => MergedChain::Zcash,
        MergedChain::Zcash => MergedChain::Wcash,
    };
    let fresh = fixture.append(
        2,
        other_chain == MergedChain::Wcash,
        other_chain == MergedChain::Zcash,
    );
    workers.threads[if other_chain == MergedChain::Wcash {
        1
    } else {
        3
    }]
    .thread()
    .unpark();
    let mut other_live = false;
    let mut other_retry = false;
    let deadline = std::time::Instant::now() + DEADLINE;
    while !(other_live && other_retry) && std::time::Instant::now() < deadline {
        match entered_rx.recv_timeout(deadline.saturating_duration_since(std::time::Instant::now()))
        {
            Ok((chain, share, is_audit, true)) if chain == other_chain => {
                other_live |= !is_audit && share == fresh;
                other_retry |= is_audit;
            }
            Ok(_) => {}
            Err(_) => break,
        }
    }
    gate.release();
    let stopped = workers.request_shutdown();
    assert!(
        audit_blocked,
        "the actual pending-winner audit worker must reach the blocked transport"
    );
    assert!(
        other_live,
        "the other chain's live lane must proceed during the held retry"
    );
    assert!(
        other_retry,
        "the other chain's pending-winner retry lane must also proceed"
    );
    assert!(stopped.is_ok());
    assert!(failure_rx.try_recv().is_err());
}

#[test]
fn blocked_wcash_retry_does_not_occupy_zcash_live_or_retry_workers() {
    audit_isolation(MergedChain::Wcash);
}

#[test]
fn blocked_zcash_retry_does_not_occupy_wcash_live_or_retry_workers() {
    audit_isolation(MergedChain::Zcash);
}

#[test]
fn failed_registration_cannot_issue_rpc_or_leave_started_lanes() {
    let fixture = IndependentWinnerFixture::retained_dual();
    fixture
        .actor
        .register_winner_worker(thread::current())
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let callback_calls = Arc::clone(&calls);
    let reconcile: Arc<ReconcileWinner> = Arc::new(move |_, _| {
        callback_calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    });
    let shutdown = Arc::new(AtomicBool::new(false));
    let (failure_tx, failure_rx) = mpsc::channel();
    let result = PoolBackendWinnerWorkers::spawn_with_reconciler(
        Arc::clone(&fixture.actor),
        Arc::clone(&shutdown),
        failure_tx,
        reconcile,
    );
    assert!(result.is_err());
    assert!(shutdown.load(Ordering::Acquire));
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "the startup barrier fences every RPC until all lane registration succeeds"
    );
    assert!(failure_rx.try_recv().is_err());
}

#[test]
fn panic_reports_failure_and_shutdown_still_joins_all_lanes() {
    let fixture = IndependentWinnerFixture::retained_dual();
    let reconcile: Arc<ReconcileWinner> = Arc::new(move |actor, snapshot| {
        if snapshot.winner().chain == MergedChain::Wcash {
            panic!("controlled test winner reconciliation panic");
        }
        observe(actor, snapshot)
    });
    let (failure_tx, failure_rx) = mpsc::channel();
    let mut workers = PoolBackendWinnerWorkers::spawn_with_reconciler(
        Arc::clone(&fixture.actor),
        Arc::new(AtomicBool::new(false)),
        failure_tx,
        reconcile,
    )
    .unwrap();
    let failure = failure_rx.recv_timeout(DEADLINE);
    let stopped = workers.request_shutdown();
    assert!(failure.unwrap().contains("worker panicked"));
    assert!(stopped.is_err());
    assert!(
        workers.threads.is_empty(),
        "panic in an early lane must not abandon later joins"
    );
    assert!(
        workers.request_shutdown().is_ok(),
        "a second shutdown has nothing left to join"
    );
}

#[test]
fn retry_classifier_preserves_explicit_allowlist_and_hard_failures() {
    let rpc = |code| MinerError::RpcError {
        endpoint: "test-node".into(),
        code: Some(code),
        message: "test".into(),
    };
    for code in [-1, -5, -9, -10, -28, -32_001] {
        assert!(is_retryable_winner_reconciliation_error(&rpc(code)));
    }
    for code in [0, -22, -25, -26, -32_603] {
        assert!(!is_retryable_winner_reconciliation_error(&rpc(code)));
    }
    for status in [408, 425, 429, 500, 502, 503, 504] {
        assert!(is_retryable_winner_reconciliation_error(
            &MinerError::RpcHttpStatus(reqwest::StatusCode::from_u16(status).unwrap())
        ));
    }
    for status in [400, 401, 403, 404, 409, 422, 501, 505] {
        assert!(!is_retryable_winner_reconciliation_error(
            &MinerError::RpcHttpStatus(reqwest::StatusCode::from_u16(status).unwrap())
        ));
    }
    assert!(!is_retryable_winner_reconciliation_error(
        &MinerError::InvalidParentTemplate("test hard rejection".into())
    ));
    assert!(!is_retryable_winner_reconciliation_error(&MinerError::Io(
        io::Error::new(io::ErrorKind::PermissionDenied, "test")
    )));
    assert!(is_retryable_winner_reconciliation_error(&MinerError::Io(
        io::Error::new(io::ErrorKind::ConnectionReset, "test")
    )));
}
