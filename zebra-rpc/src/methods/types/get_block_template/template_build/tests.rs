use std::{
    sync::{mpsc, Arc},
    thread,
    time::Duration,
};

use tokio::{sync::oneshot, task::JoinHandle, time::timeout};
use zebra_chain::{amount::Amount, block::Height, chain_sync_status::MockSyncStatus};
use zebra_node_services::BoxError;
use zebra_test::mock_service::MockService;

use crate::{
    config::mining::{default_miner_address_for_network, Config, MinerAddressType},
    methods::tests::utils::consensus_profile_network,
};

use super::{super::CoinbaseCache, *};

const TEST_LIMIT: Duration = Duration::from_secs(5);

#[test]
fn template_build_pending_error_identification_is_exact() {
    for message in [TEMPLATE_BUILD_BUSY, TEMPLATE_BUILD_PENDING] {
        assert!(is_pending_template_build(&ErrorObject::borrowed(
            -28, message, None,
        )));
        assert!(!is_pending_template_build(&ErrorObject::borrowed(
            -1, message, None,
        )));
        assert!(!is_pending_template_build(&ErrorObject::owned(
            -28,
            message,
            Some("unrelated error data"),
        )));
    }
    for message in [
        "node warming up",
        "block template construction is pending; retry later",
        "prefix: block template construction is busy; retry",
    ] {
        assert!(!is_pending_template_build(&ErrorObject::borrowed(
            -28, message, None,
        )));
    }
}

async fn held_build(
    executor: &TemplateBuildExecutor,
) -> (mpsc::Sender<()>, JoinHandle<RpcResult<usize>>) {
    let executor = executor.clone();
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let task = tokio::spawn(async move {
        executor
            .run(move || {
                let _ = entered_tx.send(());
                release_rx
                    .recv_timeout(TEST_LIMIT)
                    .expect("the bounded test releases the blocking build");
                Ok(7)
            })
            .await
    });
    timeout(TEST_LIMIT, entered_rx)
        .await
        .expect("the blocking build starts promptly")
        .expect("the blocking build reports entry");
    (release_tx, task)
}

async fn finish_build(release: mpsc::Sender<()>, task: JoinHandle<RpcResult<usize>>) {
    release.send(()).expect("the blocking build is waiting");
    assert_eq!(
        timeout(TEST_LIMIT, task)
            .await
            .expect("the released build finishes")
            .expect("the async task finishes")
            .expect("the build succeeds"),
        7
    );
}

#[tokio::test(flavor = "current_thread")]
async fn template_build_clones_share_capacity_and_recover_after_completion() {
    let executor = TemplateBuildExecutor::default();
    let (first_release, first) = held_build(&executor).await;
    let (second_release, second) = held_build(&executor.clone()).await;

    let error = executor
        .clone()
        .run(|| -> RpcResult<usize> { panic!("saturated work must never start") })
        .await
        .expect_err("the third foreground build is rejected without queueing");
    assert_eq!(error.code(), -28);

    finish_build(first_release, first).await;
    assert_eq!(executor.run(|| Ok(11)).await.unwrap(), 11);
    finish_build(second_release, second).await;
}

#[tokio::test(flavor = "current_thread")]
async fn template_build_cancelled_waiter_retains_running_capacity() {
    let executor = TemplateBuildExecutor::default();
    let (first_release, first) = held_build(&executor).await;
    let (second_release, second) = held_build(&executor).await;

    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    let error = executor
        .run(|| -> RpcResult<usize> { panic!("cancelled proof still owns its permit") })
        .await
        .expect_err("aborting the waiter cannot free running proof capacity");
    assert_eq!(error.code(), -28);

    first_release
        .send(())
        .expect("the detached blocking build still owns its receiver");
    timeout(TEST_LIMIT, async {
        while executor.permits.available_permits() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("capacity returns only after the detached build finishes");
    assert_eq!(executor.run(|| Ok(19)).await.unwrap(), 19);
    finish_build(second_release, second).await;
}

#[tokio::test(flavor = "current_thread")]
async fn template_build_pending_deadline_retains_capacity_until_completion() {
    let executor = TemplateBuildExecutor::default();
    let (first_release, first) = held_build(&executor).await;
    let (second_release, second) = held_build(&executor).await;

    // Use the real server deadline: blocking workers and Tokio then share the
    // same clock, while their external five-second release guard stays bounded.
    for task in [first, second] {
        let error = timeout(Duration::from_secs(4), task)
            .await
            .expect("the two-second pending response precedes the external release guard")
            .expect("the server-side waiter returns normally")
            .expect_err("the server reports pending before its proof finishes");
        assert_eq!(error.code(), -28);
        assert_eq!(
            error.message(),
            "block template construction is pending; retry"
        );
    }
    assert_eq!(executor.permits.available_permits(), 0);
    let error = executor
        .run(|| -> RpcResult<usize> { panic!("pending work still owns both permits") })
        .await
        .expect_err("a pending response must not create unlimited replacement work");
    assert_eq!(error.code(), -28);

    first_release.send(()).unwrap();
    timeout(TEST_LIMIT, async {
        while executor.permits.available_permits() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("one completed detached build releases one slot");
    assert_eq!(executor.run(|| Ok(29)).await.unwrap(), 29);
    second_release.send(()).unwrap();
    timeout(TEST_LIMIT, async {
        while executor.permits.available_permits() != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("both detached builds eventually release their slots");
}

#[tokio::test(flavor = "current_thread")]
async fn template_build_preserves_rpc_errors_and_releases_capacity() {
    let executor = TemplateBuildExecutor::default();
    let expected = ErrorObject::owned(-8, "fixture validation error", Some("fixture data"));
    let supplied = expected.clone();
    let error = executor
        .run(move || -> RpcResult<usize> { Err(supplied) })
        .await
        .expect_err("validation failures remain RPC failures");
    assert_eq!(error, expected);

    let (first_release, first) = held_build(&executor).await;
    let (second_release, second) = held_build(&executor).await;
    finish_build(first_release, first).await;
    finish_build(second_release, second).await;
}

#[tokio::test(flavor = "current_thread")]
async fn template_build_panics_release_capacity_without_exposing_payload() {
    let executor = TemplateBuildExecutor::default();
    let error = executor
        .run(|| -> RpcResult<usize> { panic!("private fixture panic payload") })
        .await
        .expect_err("a failed blocking worker becomes an RPC error");
    assert_eq!(error.code(), ErrorCode::InternalError.code());
    assert!(!error.message().contains("private fixture"));

    let (first_release, first) = held_build(&executor).await;
    let (second_release, second) = held_build(&executor).await;
    finish_build(first_release, first).await;
    finish_build(second_release, second).await;
}

#[tokio::test(flavor = "current_thread")]
async fn template_build_handler_clones_share_the_foreground_limit() {
    let router: MockService<_, _, _, BoxError> = MockService::build().for_unit_tests();
    let handler = super::super::GetBlockTemplateHandler::new(
        &consensus_profile_network(),
        Config::default(),
        router,
        MockSyncStatus::default(),
        None,
    );
    let mut builds = Vec::new();
    for _ in 0..2 {
        let handler = handler.clone();
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let task = tokio::spawn(async move {
            handler
                .build_template(move || {
                    let _ = entered_tx.send(());
                    release_rx
                        .recv_timeout(TEST_LIMIT)
                        .expect("the test releases this handler's build");
                    Ok(7)
                })
                .await
        });
        timeout(TEST_LIMIT, entered_rx).await.unwrap().unwrap();
        builds.push((release_tx, task));
    }
    let error = handler
        .clone()
        .build_template(|| -> RpcResult<usize> { panic!("cloning cannot create extra capacity") })
        .await
        .expect_err("all RPC handler clones use the same foreground limit");
    assert_eq!(error.code(), -28);
    for (release, task) in builds {
        finish_build(release, task).await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn template_build_real_coinbase_wait_does_not_block_async_heartbeat() {
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::super::{MinerParams, TransactionTemplate};

    let network = consensus_profile_network();
    let height = Height(2_000_000);
    let fee = Amount::zero();
    let address = default_miner_address_for_network(&network, &MinerAddressType::Transparent)
        .expect("both consensus profiles have a transparent fixture");
    let params = MinerParams::new(
        &network,
        Config {
            miner_address: Some(
                address
                    .parse()
                    .expect("the matching address namespace is valid"),
            ),
            ..Default::default()
        },
    )
    .expect("the fixture payout matches the compiled consensus profile");
    let expected = TransactionTemplate::new_coinbase(&network, height, &params, fee)
        .expect("the transparent fixture requires no private proof");
    let cache = CoinbaseCache::default();
    let held = Arc::new(AtomicBool::new(true));
    let (warming_tx, warming_rx) = oneshot::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let warming_cache = cache.clone();
    let warming_coinbase = expected.clone();
    let warming_held = Arc::clone(&held);
    let warmer = thread::spawn(move || {
        warming_cache.try_get_or_build(height, fee, || {
            let _ = warming_tx.send(());
            let released = release_rx.recv_timeout(TEST_LIMIT);
            warming_held.store(false, Ordering::Release);
            released.expect("the heartbeat test releases the held proof");
            Ok::<_, &'static str>(warming_coinbase)
        })
    });
    timeout(TEST_LIMIT, warming_rx).await.unwrap().unwrap();

    let executor = TemplateBuildExecutor::default();
    let (waiting_tx, waiting_rx) = oneshot::channel();
    let request = tokio::spawn(async move {
        executor
            .run(move || {
                let _ = waiting_tx.send(());
                Ok(cache.get_or_build(height, fee, || {
                    panic!("a same-key request must join the held proof")
                }))
            })
            .await
    });
    timeout(TEST_LIMIT, waiting_rx).await.unwrap().unwrap();

    let heartbeat = tokio::spawn(async { 23 });
    assert_eq!(
        timeout(Duration::from_secs(2), heartbeat)
            .await
            .expect("the single async worker remains responsive")
            .unwrap(),
        23
    );
    assert!(
        held.load(Ordering::Acquire),
        "the async heartbeat completed while the real cache proof was still held"
    );
    assert!(!request.is_finished());

    release_tx.send(()).unwrap();
    assert_eq!(
        timeout(TEST_LIMIT, request)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        expected
    );
    assert_eq!(warmer.join().unwrap().unwrap(), expected);
}

#[test]
fn template_diagnostics_link_busy_deadline_and_late_singleflight_completion() {
    use super::super::{coinbase_diagnostics as diagnostics, MinerParams, TransactionTemplate};
    use diagnostics::{Event, Outcome, Phase};

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let network = consensus_profile_network();
    let height = Height(2_000_000);
    let fee = Amount::zero();
    let address =
        default_miner_address_for_network(&network, &MinerAddressType::Transparent).unwrap();
    let miner = MinerParams::new(
        &network,
        Config {
            miner_address: Some(address.parse().unwrap()),
            ..Default::default()
        },
    )
    .unwrap();
    let expected = TransactionTemplate::new_coinbase(&network, height, &miner, fee).unwrap();
    let cache = CoinbaseCache::default();
    let owner_cache = cache.clone();
    let owner_coinbase = expected.clone();
    let owner_id = diagnostics::next_id();
    let (owner_entered_tx, owner_entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (owner_records_tx, owner_records_rx) = mpsc::channel();
    let owner = thread::spawn(move || {
        let records = diagnostics::capture_records(|| {
            let _context = diagnostics::enter(owner_id, Phase::Prewarm);
            owner_cache.get_or_build(height, fee, || {
                owner_entered_tx.send(()).unwrap();
                release_rx
                    .recv_timeout(TEST_LIMIT)
                    .expect("test releases the original proof");
                owner_coinbase
            });
        });
        owner_records_tx.send(records).unwrap();
    });
    let owner_entered = owner_entered_rx.recv_timeout(TEST_LIMIT);
    let executor = TemplateBuildExecutor::default();
    // One explicit owned permit plus the joining request exercises the real busy path.
    let reserved_permit = executor.permits.clone().try_acquire_owned().unwrap();
    let request_executor = executor.clone();
    let joining_cache = cache.clone();
    let joining_expected = expected.clone();
    let (joining_tx, joining_rx) = oneshot::channel();
    let (completion_tx, completion_rx) = mpsc::channel();
    let mut observed = None;
    let records = diagnostics::capture_records(|| {
        runtime.block_on(async {
            let request = tokio::spawn(async move {
                request_executor
                    .run(move || {
                        let records = diagnostics::capture_records(|| {
                            let _phase = diagnostics::phase(Phase::Sizing);
                            let _ = joining_tx.send(diagnostics::current());
                            let actual = joining_cache.get_or_build(height, fee, || {
                                panic!("the detached waiter cannot construct a duplicate proof")
                            });
                            assert_eq!(actual, joining_expected);
                        });
                        completion_tx.send(records).unwrap();
                        Ok(7)
                    })
                    .await
            });
            let context = timeout(TEST_LIMIT, joining_rx).await.unwrap().unwrap();
            let busy = executor
                .run(|| -> RpcResult<usize> {
                    panic!("the saturated executor must not run another constructor")
                })
                .await;
            let deadline = timeout(TEST_LIMIT, request).await.unwrap().unwrap();
            let permits_after_deadline = executor.permits.available_permits();
            let completion_before_release = completion_rx.try_recv().is_ok();
            let _ = release_tx.send(());
            owner.join().unwrap();
            let owner_records = owner_records_rx.recv_timeout(TEST_LIMIT).unwrap();
            let joined_records = completion_rx.recv_timeout(TEST_LIMIT).unwrap();
            timeout(TEST_LIMIT, async {
                while executor.permits.available_permits() != 1 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            observed = Some((
                context,
                busy,
                deadline,
                permits_after_deadline,
                completion_before_release,
                owner_records,
                joined_records,
            ));
        })
    });
    drop(reserved_permit);
    let (
        context,
        busy,
        deadline,
        permits_after_deadline,
        completed_early,
        owner_records,
        joined_records,
    ) = observed.unwrap();
    assert!(owner_entered.is_ok());
    assert_eq!(busy.unwrap_err().message(), TEMPLATE_BUILD_BUSY);
    assert_eq!(deadline.unwrap_err().message(), TEMPLATE_BUILD_PENDING);
    assert_eq!(permits_after_deadline, 0);
    assert!(!completed_early);
    assert_eq!(executor.permits.available_permits(), 2);
    assert_eq!(context.phase, Phase::Sizing);
    let busy = records
        .iter()
        .find(|record| record.event == Event::TemplateWorker && record.outcome == Outcome::Busy)
        .unwrap();
    let deadline = records
        .iter()
        .find(|record| record.event == Event::TemplateWorker && record.outcome == Outcome::Deadline)
        .unwrap();
    assert_eq!(deadline.request_id, context.request_id);
    assert_ne!(busy.request_id, deadline.request_id);
    assert_eq!(
        (busy.phase, deadline.phase),
        (Phase::Template, Phase::Template)
    );
    assert!(deadline.elapsed_ms > 0);
    let constructor_started = owner_records
        .iter()
        .find(|record| record.event == Event::Constructor && record.outcome == Outcome::Started)
        .unwrap();
    let constructor_completed = owner_records
        .iter()
        .find(|record| record.event == Event::Constructor && record.outcome == Outcome::Ok)
        .unwrap();
    assert_eq!(
        (
            constructor_started.request_id,
            constructor_completed.request_id
        ),
        (owner_id, owner_id)
    );
    assert_eq!(constructor_started.entry_id, constructor_completed.entry_id);
    assert_eq!(constructor_completed.phase, Phase::Prewarm);
    assert!(constructor_completed.elapsed_ms >= deadline.elapsed_ms);
    let joined = joined_records
        .iter()
        .find(|record| {
            record.event == Event::CacheLookup && record.outcome == Outcome::JoinBuilding
        })
        .unwrap();
    assert_eq!(
        (joined.request_id, joined.phase),
        (context.request_id, Phase::Sizing)
    );
    assert_eq!(joined.entry_id, constructor_completed.entry_id);
    assert_eq!(joined.cache_id, constructor_completed.cache_id);
    assert!(!joined_records
        .iter()
        .any(|record| record.event == Event::Constructor));
    assert_eq!((joined.height, joined.fee_zat), (Some(height.0), Some(0)));
}

async fn wait_for_all_template_capacity(executor: &TemplateBuildExecutor) {
    timeout(TEST_LIMIT, async {
        while executor.permits.available_permits() != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("released owned workers return the original two slots");
}

#[tokio::test(flavor = "current_thread")]
async fn ordinary_template_waiter_does_not_inherit_speculative_deadline() {
    let executor = TemplateBuildExecutor::default();
    let (release, mut task) = held_build(&executor).await;
    let early = timeout(Duration::from_millis(250), &mut task).await;
    release.send(()).unwrap();
    assert!(
        early.is_err(),
        "ordinary work must retain its two-second wait"
    );
    assert_eq!(
        timeout(TEST_LIMIT, task).await.unwrap().unwrap().unwrap(),
        7
    );
    wait_for_all_template_capacity(&executor).await;
}

#[tokio::test(flavor = "current_thread")]
async fn template_old_key_cannot_block_demanded_warm_key_or_duplicate_proofs() {
    use super::super::{MinerParams, TransactionTemplate};
    use std::sync::atomic::{AtomicUsize, Ordering};

    let network = consensus_profile_network();
    let old_height = Height(2_000_000);
    let next_height = Height(2_000_001);
    let fee = Amount::zero();
    let address =
        default_miner_address_for_network(&network, &MinerAddressType::Transparent).unwrap();
    let params = MinerParams::new(
        &network,
        Config {
            miner_address: Some(address.parse().unwrap()),
            ..Default::default()
        },
    )
    .unwrap();
    let old_coinbase =
        TransactionTemplate::new_coinbase(&network, old_height, &params, fee).unwrap();
    let next_coinbase =
        TransactionTemplate::new_coinbase(&network, next_height, &params, fee).unwrap();
    let cache = CoinbaseCache::default();
    let builds = Arc::new(AtomicUsize::new(0));
    let seeded = next_coinbase.clone();
    cache.get_or_build(next_height, fee, || {
        builds.fetch_add(1, Ordering::SeqCst);
        seeded
    });
    let executor = TemplateBuildExecutor::default();
    let working = executor.clone();
    let old_cache = cache.clone();
    let building = Arc::clone(&builds);
    let old_expected = old_coinbase.clone();
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (completed_tx, mut completed_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        working
            .run(move || {
                let result = old_cache.get_or_build(old_height, fee, || {
                    building.fetch_add(1, Ordering::SeqCst);
                    let _ = entered_tx.send(());
                    release_rx.recv_timeout(TEST_LIMIT).unwrap();
                    old_expected
                });
                let _ = completed_tx.send(result);
                Ok(7)
            })
            .await
    });
    timeout(TEST_LIMIT, entered_rx).await.unwrap().unwrap();
    assert_eq!(
        timeout(Duration::from_secs(3), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err()
            .message(),
        TEMPLATE_BUILD_PENDING
    );
    assert!(completed_rx.try_recv().is_err());
    let next_cache = cache.clone();
    let ready = timeout(
        Duration::from_secs(1),
        executor.run(move || {
            Ok(next_cache.get_or_build(next_height, fee, || {
                panic!("the demanded different key is already warm")
            }))
        }),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(ready, next_coinbase);
    assert!(completed_rx.try_recv().is_err());
    assert_eq!(builds.load(Ordering::SeqCst), 2);
    release_tx.send(()).unwrap();
    assert_eq!(
        timeout(TEST_LIMIT, completed_rx).await.unwrap().unwrap(),
        old_coinbase
    );
    wait_for_all_template_capacity(&executor).await;
    let warmed = executor
        .run(move || {
            Ok(cache.get_or_build(old_height, fee, || {
                panic!("the detached constructor's exact key must stay warm")
            }))
        })
        .await
        .unwrap();
    assert_eq!(warmed, old_coinbase);
    assert_eq!(builds.load(Ordering::SeqCst), 2);
}

#[tokio::test(flavor = "current_thread")]
async fn speculative_obsolete_watch_requires_observed_snapshot_then_changed_hash() {
    use zebra_chain::{block::Hash, chain_tip::mock::MockChainTip};

    let (tip, sender) = MockChainTip::new();
    let snapshot = Hash([0x31; 32]);
    sender.send_best_tip_hash(Hash([0x30; 32]));
    sender.send_best_tip_height(Height(1));
    let obsolete = wait_for_obsolete_snapshot(tip, snapshot);
    tokio::pin!(obsolete);
    assert!(futures::poll!(obsolete.as_mut()).is_pending());
    // A lagging watch naming another hash cannot cancel the newer read-state
    // snapshot. Catching up arms the fence without declaring obsolescence.
    sender.send_best_tip_hash(snapshot);
    assert!(futures::poll!(obsolete.as_mut()).is_pending());
    sender.send_best_tip_hash(snapshot);
    sender.send_best_tip_height(Height(2));
    assert!(futures::poll!(obsolete.as_mut()).is_pending());
    // Updating before the next poll is not a lost wake; an exact-hash change
    // proves a same-height reorg as well as a height advance.
    sender.send_best_tip_hash(Hash([0x32; 32]));
    assert!(futures::poll!(obsolete.as_mut()).is_ready());
}

#[tokio::test(flavor = "current_thread")]
async fn speculative_closed_tip_watch_keeps_normal_build_result() {
    use zebra_chain::{block::Hash, chain_tip::mock::MockChainTip};

    let (tip, sender) = MockChainTip::new();
    let snapshot = Hash([0x41; 32]);
    sender.send_best_tip_hash(snapshot);
    let obsolete = wait_for_obsolete_snapshot(tip, snapshot);
    tokio::pin!(obsolete);
    assert!(futures::poll!(obsolete.as_mut()).is_pending());
    drop(sender);
    // Closed watch errors park once; they neither prove a changed tip nor
    // spin, and the original bounded builder remains able to finish.
    assert!(futures::poll!(obsolete.as_mut()).is_pending());
    let executor = TemplateBuildExecutor::default();
    tokio::select! {
        result = executor.run(|| Ok(47)) => assert_eq!(result.unwrap(),47),
        () = obsolete => panic!("a closed watch cannot manufacture obsolescence"),
    }
    wait_for_all_template_capacity(&executor).await;
}
