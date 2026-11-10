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

fn controlled_context() -> (
    TemplateBuildContext,
    zebra_chain::chain_tip::mock::MockChainTipSender,
) {
    use zebra_chain::{block::Hash, chain_tip::mock::MockChainTip};
    let (tip, sender) = MockChainTip::new();
    let snapshot = Hash([0x71; 32]);
    sender.send_best_tip_hash(snapshot);
    (TemplateBuildContext::new(tip, snapshot), sender)
}

fn controlled_coinbase(
    height: Height,
    fee: u64,
) -> super::super::TransactionTemplate<zebra_chain::amount::NegativeOrZero> {
    use super::super::{MinerParams, TransactionTemplate};
    let network = consensus_profile_network();
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
    TransactionTemplate::new_coinbase(&network, height, &params, Amount::try_from(fee).unwrap())
        .unwrap()
}

#[test]
fn template_ready_only_exact_keys_never_build_join_or_mutate_cache() {
    use super::super::{CoinbaseCacheEntry, CoinbaseCacheState};
    let cache = CoinbaseCache::default();
    let height = Height(2_000_000);
    let zero = Amount::zero();
    let fee = Amount::try_from(15_000).unwrap();
    let (mut context, _sender) = controlled_context();
    context.ready_only = true;
    let missing = cache.get_for_request(height, zero, Some(&context), || {
        panic!("ready-only sizing cannot start a constructor")
    });
    assert!(matches!(missing, Err(TemplateBuildError::Pending)));
    assert!(cache.0.lock().unwrap().foreground.is_empty());

    let sizing = controlled_coinbase(height, 0);
    cache.store(height, zero, sizing.clone());
    assert_eq!(
        cache
            .get_for_request(height, zero, Some(&context), || panic!(
                "ready sizing reused"
            ))
            .unwrap(),
        sizing
    );
    let missing = cache.get_for_request(height, fee, Some(&context), || {
        panic!("a ready zero-fee sizing entry cannot substitute for selected fees")
    });
    assert!(matches!(missing, Err(TemplateBuildError::Pending)));
    assert_eq!(cache.0.lock().unwrap().foreground.len(), 1);

    // A real entry state is used here; the ownership/notification tests below
    // separately keep a live constructor behind this same Building state.
    let entry = Arc::new(CoinbaseCacheEntry::building());
    cache
        .0
        .lock()
        .unwrap()
        .foreground
        .insert((height, fee), Arc::clone(&entry));
    assert!(matches!(
        cache.get_for_request(height, fee, Some(&context), || panic!("no duplicate proof")),
        Err(TemplateBuildError::Pending)
    ));
    assert!(matches!(
        *entry.state.lock().unwrap(),
        CoinbaseCacheState::Building
    ));
    assert!(Arc::ptr_eq(
        cache
            .0
            .lock()
            .unwrap()
            .foreground
            .get(&(height, fee))
            .unwrap(),
        &entry
    ));

    let paid = controlled_coinbase(height, 15_000);
    cache.store(height, fee, paid.clone());
    assert_eq!(
        cache
            .get_for_request(height, fee, Some(&context), || panic!(
                "exact selected proof reused"
            ))
            .unwrap(),
        paid
    );
    assert_eq!(cache.0.lock().unwrap().foreground.len(), 2);

    let sizing_cache = CoinbaseCache::default();
    let sizing_entry = Arc::new(CoinbaseCacheEntry::building());
    sizing_cache
        .0
        .lock()
        .unwrap()
        .foreground
        .insert((height, zero), Arc::clone(&sizing_entry));
    assert!(matches!(
        sizing_cache.get_for_request(height, zero, Some(&context), || panic!(
            "ready-only cannot wait for sizing"
        )),
        Err(TemplateBuildError::Pending)
    ));
    assert!(matches!(
        *sizing_entry.state.lock().unwrap(),
        CoinbaseCacheState::Building
    ));
    assert_eq!(sizing_cache.0.lock().unwrap().foreground.len(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn template_ready_lane_has_one_owner_and_cancellation_keeps_its_bound() {
    use zebra_chain::{block::Hash, chain_tip::mock::MockChainTip};
    let executor = TemplateBuildExecutor::default();
    let (first_release, first) = held_build(&executor).await;
    let (second_release, second) = held_build(&executor).await;
    let (tip, sender) = MockChainTip::new();
    let snapshot = Hash([0x72; 32]);
    sender.send_best_tip_hash(snapshot);
    let working = executor.clone();
    let task_tip = tip.clone();
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let ready = tokio::spawn(async move {
        working
            .run_for_tip(task_tip, snapshot, move |context| {
                assert!(context.ready_only());
                let _ = entered_tx.send(());
                release_rx.recv_timeout(TEST_LIMIT).unwrap();
                Ok(23)
            })
            .await
    });
    timeout(TEST_LIMIT, entered_rx).await.unwrap().unwrap();
    assert_eq!(executor.permits.available_permits(), 0);
    assert_eq!(executor.ready_permits.available_permits(), 0);
    ready.abort();
    assert!(ready.await.unwrap_err().is_cancelled());
    let error = executor
        .run_for_tip(tip, snapshot, |_| -> RpcResult<usize> {
            panic!("a second assembly-only worker cannot be queued")
        })
        .await
        .unwrap_err();
    assert_eq!(error.message(), TEMPLATE_BUILD_BUSY);
    assert_eq!(executor.ready_permits.available_permits(), 0);
    release_tx.send(()).unwrap();
    timeout(TEST_LIMIT, async {
        while executor.ready_permits.available_permits() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    finish_build(first_release, first).await;
    finish_build(second_release, second).await;
    wait_for_all_template_capacity(&executor).await;
}

#[tokio::test(flavor = "current_thread")]
async fn template_obsolete_cache_join_releases_worker_without_cancelling_owner() {
    use zebra_chain::{block::Hash, chain_tip::mock::MockChainTip};
    let cache = CoinbaseCache::default();
    let height = Height(2_000_000);
    let expected = controlled_coinbase(height, 15_000);
    let owner_cache = cache.clone();
    let owned = expected.clone();
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let owner = thread::spawn(move || {
        owner_cache.get_or_build(height, Amount::try_from(15_000).unwrap(), || {
            let _ = entered_tx.send(());
            release_rx.recv_timeout(TEST_LIMIT).unwrap();
            owned
        })
    });
    timeout(TEST_LIMIT, entered_rx).await.unwrap().unwrap();
    let (tip, sender) = MockChainTip::new();
    let snapshot = Hash([0x73; 32]);
    sender.send_best_tip_hash(snapshot);
    let executor = TemplateBuildExecutor::default();
    let working = executor.clone();
    let joining = cache.clone();
    let (joining_tx, joining_rx) = oneshot::channel();
    let waiter = tokio::spawn(async move {
        working
            .run_for_tip(tip, snapshot, move |context| {
                let _ = joining_tx.send(());
                joining
                    .get_for_request(
                        height,
                        Amount::try_from(15_000).unwrap(),
                        Some(&context),
                        || panic!("the exact proof has a live owner"),
                    )
                    .map_err(TemplateBuildError::into_rpc_error)
            })
            .await
    });
    timeout(TEST_LIMIT, joining_rx).await.unwrap().unwrap();
    sender.send_best_tip_hash(Hash([0x74; 32]));
    let result = timeout(Duration::from_secs(1), waiter).await;
    // Release ownership before assertions so an unexpected failure cannot strand a worker.
    release_tx.send(()).unwrap();
    assert_eq!(owner.join().unwrap(), expected);
    let error = result.unwrap().unwrap().unwrap_err();
    assert_eq!(
        error.code(),
        crate::methods::WCASH_AUX_TIP_CHANGED_ERROR_CODE
    );
    wait_for_all_template_capacity(&executor).await;
    assert_eq!(
        cache.get(height, Amount::try_from(15_000).unwrap()),
        Some(expected)
    );
}

#[tokio::test(flavor = "current_thread")]
async fn template_stale_sizing_finishes_exact_entry_without_starting_selected_proof() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use zebra_chain::{block::Hash, chain_tip::mock::MockChainTip};
    let executor = TemplateBuildExecutor::default();
    let cache = CoinbaseCache::default();
    let working_cache = cache.clone();
    let height = Height(2_000_000);
    let sizing = controlled_coinbase(height, 0);
    let built = sizing.clone();
    let selected = Arc::new(AtomicUsize::new(0));
    let selected_work = Arc::clone(&selected);
    let (tip, sender) = MockChainTip::new();
    let snapshot = Hash([0x75; 32]);
    sender.send_best_tip_hash(snapshot);
    let working = executor.clone();
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let task = tokio::spawn(async move {
        working
            .run_for_tip(tip, snapshot, move |context| {
                working_cache
                    .get_for_request(height, Amount::zero(), Some(&context), || {
                        let _ = entered_tx.send(());
                        release_rx.recv_timeout(TEST_LIMIT).unwrap();
                        Ok(built)
                    })
                    .map_err(TemplateBuildError::into_rpc_error)?;
                working_cache
                    .get_for_request(
                        height,
                        Amount::try_from(15_000).unwrap(),
                        Some(&context),
                        || {
                            selected_work.fetch_add(1, Ordering::SeqCst);
                            panic!("a replaced snapshot cannot start its selected-fee proof")
                        },
                    )
                    .map_err(TemplateBuildError::into_rpc_error)
            })
            .await
    });
    timeout(TEST_LIMIT, entered_rx).await.unwrap().unwrap();
    sender.send_best_tip_hash(Hash([0x76; 32]));
    release_tx.send(()).unwrap();
    let error = timeout(TEST_LIMIT, task)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(
        error.code(),
        crate::methods::WCASH_AUX_TIP_CHANGED_ERROR_CODE
    );
    assert_eq!(selected.load(Ordering::SeqCst), 0);
    assert_eq!(cache.get(height, Amount::zero()), Some(sizing));
    assert!(cache
        .get(height, Amount::try_from(15_000).unwrap())
        .is_none());
    wait_for_all_template_capacity(&executor).await;
}

#[tokio::test(flavor = "current_thread")]
async fn template_cancelled_sizing_finishes_entry_without_starting_selected_proof() {
    use zebra_chain::{block::Hash, chain_tip::mock::MockChainTip};
    let executor = TemplateBuildExecutor::default();
    let cache = CoinbaseCache::default();
    let worker_cache = cache.clone();
    let height = Height(2_000_000);
    let sizing = controlled_coinbase(height, 0);
    let built = sizing.clone();
    let (tip, sender) = MockChainTip::new();
    let snapshot = Hash([0x7a; 32]);
    sender.send_best_tip_hash(snapshot);
    let working = executor.clone();
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (completed_tx, completed_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        working
            .run_for_tip(tip, snapshot, move |context| {
                let result = (|| {
                    worker_cache.get_for_request(height, Amount::zero(), Some(&context), || {
                        let _ = entered_tx.send(());
                        release_rx.recv_timeout(TEST_LIMIT).unwrap();
                        Ok(built)
                    })?;
                    worker_cache.get_for_request(
                        height,
                        Amount::try_from(15_000).unwrap(),
                        Some(&context),
                        || panic!("a cancelled waiter cannot start another proof phase"),
                    )
                })()
                .map_err(TemplateBuildError::into_rpc_error);
                let _ = completed_tx.send(result.as_ref().unwrap_err().code());
                result
            })
            .await
    });
    timeout(TEST_LIMIT, entered_rx).await.unwrap().unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(executor.permits.available_permits(), 1);
    release_tx.send(()).unwrap();
    assert_eq!(
        timeout(TEST_LIMIT, completed_rx).await.unwrap().unwrap(),
        -28
    );
    wait_for_all_template_capacity(&executor).await;
    assert_eq!(cache.get(height, Amount::zero()), Some(sizing));
    assert!(cache
        .get(height, Amount::try_from(15_000).unwrap())
        .is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn template_join_deadline_releases_waiter_but_keeps_exact_owner() {
    let cache = CoinbaseCache::default();
    let height = Height(2_000_000);
    let fee = Amount::try_from(15_000).unwrap();
    let expected = controlled_coinbase(height, 15_000);
    let built = expected.clone();
    let worker_cache = cache.clone();
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let owner = thread::spawn(move || {
        worker_cache.get_or_build(height, fee, || {
            let _ = entered_tx.send(());
            release_rx.recv_timeout(TEST_LIMIT).unwrap();
            built
        })
    });
    timeout(TEST_LIMIT, entered_rx).await.unwrap().unwrap();
    let (mut context, _sender) = controlled_context();
    context.deadline = Instant::now() + Duration::from_millis(50);
    let executor = TemplateBuildExecutor::default();
    let joining_cache = cache.clone();
    let result = timeout(
        Duration::from_secs(1),
        executor.run(move || {
            joining_cache
                .get_for_request(height, fee, Some(&context), || {
                    panic!("live exact owner cannot be replaced")
                })
                .map_err(TemplateBuildError::into_rpc_error)
        }),
    )
    .await;
    let still_building = cache.get(height, fee).is_none();
    release_tx.send(()).unwrap();
    assert_eq!(owner.join().unwrap(), expected);
    assert_eq!(
        result.unwrap().unwrap_err().message(),
        TEMPLATE_BUILD_PENDING
    );
    assert!(still_building);
    wait_for_all_template_capacity(&executor).await;
    assert_eq!(cache.get(height, fee), Some(expected));
}

#[tokio::test(flavor = "current_thread")]
async fn zcash_template_path_keeps_original_two_worker_admission() {
    use zebra_chain::{block::Hash, chain_tip::mock::MockChainTip};
    let router: MockService<_, _, _, BoxError> = MockService::build().for_unit_tests();
    let handler = super::super::GetBlockTemplateHandler::new(
        &consensus_profile_network(),
        Config::default(),
        router,
        MockSyncStatus::default(),
        None,
    );
    let (first_release, first) = held_build(&handler.template_builder).await;
    let (second_release, second) = held_build(&handler.template_builder).await;
    let (tip, _sender) = MockChainTip::new();
    let error = handler
        .build_template_for_tip(tip, Hash([0x7b; 32]), false, |_| -> RpcResult<usize> {
            panic!("the Zcash path cannot use the Wcash ready-only lane")
        })
        .await
        .unwrap_err();
    finish_build(first_release, first).await;
    finish_build(second_release, second).await;
    assert_eq!(error.message(), TEMPLATE_BUILD_BUSY);
    assert_eq!(
        handler.template_builder.ready_permits.available_permits(),
        1
    );
}

#[test]
fn template_context_requires_observed_snapshot_before_latching_obsolescence() {
    use zebra_chain::{block::Hash, chain_tip::mock::MockChainTip};
    let (tip, sender) = MockChainTip::new();
    let snapshot = Hash([0x77; 32]);
    sender.send_best_tip_hash(Hash([0x76; 32]));
    let context = TemplateBuildContext::new(tip, snapshot);
    assert!(context.check().is_ok());
    sender.send_best_tip_hash(snapshot);
    assert!(context.check().is_ok());
    sender.send_best_tip_hash(Hash([0x78; 32]));
    assert!(matches!(context.check(), Err(TemplateBuildError::Obsolete)));
    sender.send_best_tip_hash(snapshot);
    assert!(matches!(context.check(), Err(TemplateBuildError::Obsolete)));
}

#[tokio::test(flavor = "current_thread")]
async fn template_stable_cold_phases_warm_across_deadlines_and_eventually_finish() {
    use zebra_chain::{block::Hash, chain_tip::mock::MockChainTip};
    let executor = TemplateBuildExecutor::default();
    let cache = CoinbaseCache::default();
    let height = Height(2_000_000);
    let zero = Amount::zero();
    let fee = Amount::try_from(15_000).unwrap();
    let snapshot = Hash([0x79; 32]);
    let (tip, sender) = MockChainTip::new();
    sender.send_best_tip_hash(snapshot);
    for (amount, value) in [
        (zero, controlled_coinbase(height, 0)),
        (fee, controlled_coinbase(height, 15_000)),
    ] {
        let working = executor.clone();
        let task_tip = tip.clone();
        let task_cache = cache.clone();
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (completed_tx, completed_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            working
                .run_for_tip(task_tip, snapshot, move |context| {
                    if amount != zero {
                        task_cache
                            .get_for_request(height, zero, Some(&context), || {
                                panic!("sizing was warmed by the prior request")
                            })
                            .map_err(TemplateBuildError::into_rpc_error)?;
                    }
                    let result = task_cache
                        .get_for_request(height, amount, Some(&context), || {
                            let _ = entered_tx.send(());
                            release_rx.recv_timeout(TEST_LIMIT).unwrap();
                            Ok(value)
                        })
                        .map_err(TemplateBuildError::into_rpc_error);
                    let _ = completed_tx.send(());
                    result
                })
                .await
        });
        timeout(TEST_LIMIT, entered_rx).await.unwrap().unwrap();
        let result = timeout(Duration::from_secs(3), task).await;
        // The actual two-second RPC wait expires while each constructor retains ownership.
        release_tx.send(()).unwrap();
        assert_eq!(
            result.unwrap().unwrap().unwrap_err().message(),
            TEMPLATE_BUILD_PENDING
        );
        timeout(TEST_LIMIT, completed_rx).await.unwrap().unwrap();
        wait_for_all_template_capacity(&executor).await;
        assert!(cache.get(height, amount).is_some());
    }
    let final_cache = cache.clone();
    executor
        .run_for_tip(tip, snapshot, move |context| {
            for amount in [zero, fee] {
                final_cache
                    .get_for_request(height, amount, Some(&context), || {
                        panic!("both exact phases are warm")
                    })
                    .map_err(TemplateBuildError::into_rpc_error)?;
            }
            Ok(31)
        })
        .await
        .unwrap();
    wait_for_all_template_capacity(&executor).await;
}

#[tokio::test(flavor = "current_thread")]
async fn template_publication_handoff_keeps_exact_reserve_arcs_bounds_and_failed_window() {
    use super::super::{GetBlockTemplateHandler, MinerParams, TransactionTemplate};
    use zebra_chain::parameters::Network;
    let network = Network::new_wcash_regtest();
    let address =
        default_miner_address_for_network(&network, &MinerAddressType::Transparent).unwrap();
    let config = Config {
        miner_address: Some(address.parse().unwrap()),
        ..Default::default()
    };
    let params = MinerParams::new(&network, config.clone()).unwrap();
    let build =
        |height, fee| TransactionTemplate::new_coinbase(&network, height, &params, fee).unwrap();
    let cache = CoinbaseCache::default();
    let height = Height(100);
    let zero = Amount::zero();
    let fee = Amount::try_from(15_000).unwrap();
    // Use the normal foreground APIs to reach the fee bound at two adjacent heights.
    for current in [height, Height(height.0 + 1)] {
        for value in 1..=4 {
            let amount = Amount::try_from(value).unwrap();
            cache.store(current, amount, build(current, amount));
        }
    }
    assert!(cache.configure_auxiliary_reserve(Height(height.0 - 1), fee));
    for amount in [zero, fee] {
        cache
            .try_prewarm_reserved(height, amount, || Ok::<_, ()>(build(height, amount)))
            .unwrap()
            .unwrap();
    }
    let (sizing, selected, reserve_keys) = {
        let storage = cache.0.lock().unwrap();
        assert_eq!(storage.foreground.len(), 8);
        assert!(!storage.foreground.contains_key(&(height, zero)));
        assert!(!storage.foreground.contains_key(&(height, fee)));
        (
            Arc::clone(storage.reserve.get(&(height, zero)).unwrap()),
            Arc::clone(storage.reserve.get(&(height, fee)).unwrap()),
            storage.reserve_keys.clone(),
        )
    };
    let router: MockService<_, _, _, BoxError> = MockService::build().for_unit_tests();
    let mut handler =
        GetBlockTemplateHandler::new(&network, config, router, MockSyncStatus::default(), None);
    handler.coinbase_cache = cache.clone();
    // The second entry is contended while the storage lock remains available.
    // If optional registration ran after a failed handoff, it could acquire
    // storage and discard these current reserve entries. The actual handler must skip it.
    let selected_guard = selected.state.lock().unwrap();
    handler.prewarm_after_auxiliary_publication(&network, height, fee);
    {
        let storage = cache.0.lock().unwrap();
        assert_eq!(storage.reserve_keys, reserve_keys);
        assert_eq!(storage.foreground.len(), 8);
        assert!(!storage.foreground.contains_key(&(height, zero)));
        assert!(!storage.foreground.contains_key(&(height, fee)));
        assert!(Arc::ptr_eq(
            storage.reserve.get(&(height, zero)).unwrap(),
            &sizing
        ));
        assert!(Arc::ptr_eq(
            storage.reserve.get(&(height, fee)).unwrap(),
            &selected
        ));
    }
    drop(selected_guard);
    {
        let storage_guard = cache.0.lock().unwrap();
        assert!(!cache.retain_published_coinbases(height, fee));
        assert_eq!(storage_guard.reserve_keys, reserve_keys);
    }
    assert!(cache.retain_published_coinbases(height, fee));
    assert!(cache.configure_auxiliary_reserve(height, fee));
    {
        let storage = cache.0.lock().unwrap();
        assert!(Arc::ptr_eq(
            storage.foreground.get(&(height, zero)).unwrap(),
            &sizing
        ));
        assert!(Arc::ptr_eq(
            storage.foreground.get(&(height, fee)).unwrap(),
            &selected
        ));
        assert!(!storage.reserve.contains_key(&(height, zero)));
        assert!(!storage.reserve.contains_key(&(height, fee)));
        assert_eq!(
            storage
                .foreground
                .keys()
                .filter(|(h, _)| *h == height)
                .count(),
            4
        );
        assert_eq!(storage.foreground.len(), 8);
    }
    let (mut context, _sender) = controlled_context();
    context.ready_only = true;
    for amount in [zero, fee] {
        assert_eq!(
            cache
                .get_for_request(height, amount, Some(&context), || panic!(
                    "publication retained the exact proof"
                ))
                .unwrap(),
            build(height, amount)
        );
    }
    // A later genuine publication handoff applies the existing adjacent-height
    // retention rule, rather than growing an unbounded second foreground cache.
    let later = Height(height.0 + 2);
    for amount in [zero, fee] {
        cache
            .try_prewarm_reserved(later, amount, || Ok::<_, ()>(build(later, amount)))
            .unwrap()
            .unwrap();
    }
    assert!(cache.retain_published_coinbases(later, fee));
    assert!(
        cache.retain_published_coinbases(later, zero),
        "zero selected fees use one exact entry without locking it twice"
    );
    let storage = cache.0.lock().unwrap();
    assert!(storage
        .foreground
        .keys()
        .all(|(h, _)| h.0.abs_diff(later.0) <= 1));
    assert_eq!(storage.foreground.len(), 6);
    for current in [Height(later.0 - 1), later, Height(later.0 + 1)] {
        assert!(
            storage
                .foreground
                .keys()
                .filter(|(h, _)| *h == current)
                .count()
                <= 4
        );
    }
    assert!(storage.reserve.len() <= 4 && storage.reserve_keys.len() <= 4);
}
