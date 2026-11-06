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

    // Start the fake clock only after both real blocking workers have entered.
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(2)).await;
    for task in [first, second] {
        let error = task
            .await
            .expect("the server-side waiter returns normally")
            .expect_err("the server reports pending before its proof finishes");
        assert_eq!(error.code(), -28);
        assert_eq!(
            error.message(),
            "block template construction is pending; retry"
        );
    }
    tokio::time::resume();

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
