//! Bounded block-gossip delivery and recovery regressions.

#![allow(clippy::unwrap_in_result)]

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use futures::{future::BoxFuture, task::AtomicWaker, FutureExt};
use zebra_chain::parameters::Network;
use zebra_state::ChainTipSender;

use super::*;

#[derive(Default)]
struct NetworkState {
    ready: bool,
    permanent_error: bool,
    active_requests: usize,
    max_active_requests: usize,
    requests: Vec<block::Hash>,
    mined_requests: Vec<block::Hash>,
    ordinary_requests: Vec<block::Hash>,
    stalled_responses: HashSet<block::Hash>,
    mined_reports: HashMap<block::Hash, zn::MinedBlockAdvertisement>,
}

#[derive(Clone, Default)]
struct ControlledNetwork {
    state: Arc<Mutex<NetworkState>>,
    readiness_waker: Arc<AtomicWaker>,
    readiness_polled: Arc<tokio::sync::Notify>,
    request_started: Arc<tokio::sync::Notify>,
    response_released: Arc<tokio::sync::Notify>,
}

impl ControlledNetwork {
    fn set_ready(&self) {
        self.state.lock().unwrap().ready = true;
        self.readiness_waker.wake();
    }

    fn requests(&self) -> Vec<block::Hash> {
        self.state.lock().unwrap().requests.clone()
    }
}

impl Service<zn::Request> for ControlledNetwork {
    type Response = zn::Response;
    type Error = BoxError;
    type Future = BoxFuture<'static, Result<zn::Response, BoxError>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.readiness_waker.register(cx.waker());
        self.readiness_polled.notify_one();
        if self.state.lock().unwrap().permanent_error {
            return Poll::Ready(Err(std::io::Error::other("fixture peer set stopped").into()));
        }
        if self.state.lock().unwrap().ready {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }

    fn call(&mut self, request: zn::Request) -> Self::Future {
        let mined = matches!(request, zn::Request::AdvertiseBlockToAll(_));
        let hash = match request {
            zn::Request::AdvertiseBlockToAll(hash) | zn::Request::AdvertiseBlock(hash, _) => hash,
            _ => panic!("block gossip only sends block advertisements"),
        };
        let mut state = self.state.lock().unwrap();
        state.requests.push(hash);
        if mined {
            state.mined_requests.push(hash);
        } else {
            state.ordinary_requests.push(hash);
        }
        state.active_requests += 1;
        state.max_active_requests = state.max_active_requests.max(state.active_requests);
        let stalled = state.stalled_responses.contains(&hash);
        let response =
            if mined {
                zn::Response::MinedBlockAdvertisement(
                    state.mined_reports.get(&hash).copied().unwrap_or(
                        zn::MinedBlockAdvertisement {
                            non_local_sent: 1,
                            ..Default::default()
                        },
                    ),
                )
            } else {
                zn::Response::Nil
            };
        drop(state);
        self.request_started.notify_one();
        let guard = ActiveRequest(Arc::clone(&self.state));
        let response_released = Arc::clone(&self.response_released);
        async move {
            let _guard = guard;
            if stalled {
                response_released.notified().await;
            }
            Ok(response)
        }
        .boxed()
    }
}

struct ActiveRequest(Arc<Mutex<NetworkState>>);

impl Drop for ActiveRequest {
    fn drop(&mut self) {
        self.0.lock().unwrap().active_requests -= 1;
    }
}

fn start_gossip(
    network: ControlledNetwork,
    receiver: mpsc::Receiver<(block::Hash, block::Height)>,
) -> (
    tokio::task::JoinHandle<Result<(), BlockGossipError>>,
    ChainTipSender,
    crate::components::sync::RecentSyncLengths,
) {
    start_gossip_with_policy(network, receiver, false)
}

fn start_gossip_with_policy(
    network: ControlledNetwork,
    receiver: mpsc::Receiver<(block::Hash, block::Height)>,
    prioritize_fresh_mined: bool,
) -> (
    tokio::task::JoinHandle<Result<(), BlockGossipError>>,
    ChainTipSender,
    crate::components::sync::RecentSyncLengths,
) {
    let (sync_status, recent_syncs) = SyncStatus::new();
    let (tip_sender, _, chain_tip) = ChainTipSender::new(None, &Network::Mainnet);
    let task = tokio::spawn(gossip_best_tip_block_hashes(
        sync_status,
        chain_tip,
        network,
        Some(receiver),
        prioritize_fresh_mined,
    ));
    (task, tip_sender, recent_syncs)
}

async fn yield_tasks() {
    for _ in 0..32 {
        tokio::task::yield_now().await;
    }
}

/// A fresh RPC-origin notification must not wait for an unrelated tip's deadline.
#[tokio::test(start_paused = true)]
async fn fresh_zcash_mined_notification_starts_while_ordinary_attempt_is_stalled() {
    let ordinary = block::Hash([110; 32]);
    let mined = block::Hash([111; 32]);
    let network = ControlledNetwork::default();
    network.set_ready();
    network
        .state
        .lock()
        .unwrap()
        .stalled_responses
        .insert(ordinary);
    let (sender, receiver) = mpsc::channel(4);
    let (task, mut tip, mut syncs) = start_gossip_with_policy(network.clone(), receiver, true);
    syncs.push_obtain_tips_length(0);
    tip.set_finalized_tip(zebra_state::ChainTipBlock {
        hash: ordinary,
        height: block::Height(110),
        time: std::time::SystemTime::UNIX_EPOCH.into(),
        transactions: Vec::new(),
        transaction_hashes: Arc::from([]),
        previous_block_hash: block::Hash([109; 32]),
    });
    tokio::time::timeout(
        PEER_GOSSIP_DELAY + Duration::from_secs(1),
        network.request_started.notified(),
    )
    .await
    .expect("the ordinary request starts before introducing the mined notification");
    assert_eq!(network.requests(), vec![ordinary]);

    sender.try_send((mined, block::Height(111))).unwrap();
    let started = tokio::time::timeout(
        Duration::from_millis(100),
        network.request_started.notified(),
    )
    .await;
    yield_tasks().await;
    let requests = network.requests();
    let (active, maximum) = {
        let state = network.state.lock().unwrap();
        (state.active_requests, state.max_active_requests)
    };
    task.abort();
    let _ = task.await;
    assert!(
        started.is_ok(),
        "fresh Zcash mined notification waited behind an ordinary request instead of starting within 100 ms"
    );
    assert_eq!(requests, vec![ordinary, mined]);
    assert_eq!(active, 1, "the stalled ordinary attempt remains owned");
    assert_eq!(maximum, 2, "Zcash owns at most two concurrent attempts");
    assert_eq!(network.state.lock().unwrap().active_requests, 0);
}

#[tokio::test(start_paused = true)]
async fn fresh_zcash_mined_notification_preserves_an_older_stalled_attempt_and_retry() {
    let older = block::Hash([112; 32]);
    let fresh = block::Hash([113; 32]);
    let network = ControlledNetwork::default();
    network.set_ready();
    network
        .state
        .lock()
        .unwrap()
        .stalled_responses
        .insert(older);
    let (sender, receiver) = mpsc::channel(4);
    let (task, _tip, _syncs) = start_gossip_with_policy(network.clone(), receiver, true);
    sender.try_send((older, block::Height(112))).unwrap();
    network.request_started.notified().await;

    sender.try_send((fresh, block::Height(113))).unwrap();
    tokio::time::timeout(
        Duration::from_millis(100),
        network.request_started.notified(),
    )
    .await
    .expect("fresh work starts without waiting for an older mined attempt");
    yield_tasks().await;
    assert_eq!(network.requests(), vec![older, fresh]);
    {
        let state = network.state.lock().unwrap();
        assert_eq!(
            state.active_requests, 1,
            "the older future was not canceled"
        );
        assert_eq!(state.max_active_requests, 2);
    }

    tokio::time::advance(TIPS_RESPONSE_TIMEOUT).await;
    yield_tasks().await;
    assert_eq!(network.requests(), vec![older, fresh]);
    assert_eq!(network.state.lock().unwrap().active_requests, 0);
    network
        .state
        .lock()
        .unwrap()
        .stalled_responses
        .remove(&older);
    tokio::time::advance(GOSSIP_RETRY_DELAY).await;
    yield_tasks().await;
    assert_eq!(network.requests(), vec![older, fresh, older]);
    assert_eq!(network.state.lock().unwrap().active_requests, 0);
    task.abort();
    let _ = task.await;
}

#[tokio::test(start_paused = true)]
async fn duplicate_zcash_notifications_do_not_fill_the_fresh_lane_or_cancel_an_attempt() {
    let hash = block::Hash([114; 32]);
    let network = ControlledNetwork::default();
    network.set_ready();
    network.state.lock().unwrap().stalled_responses.insert(hash);
    let (sender, receiver) = mpsc::channel(4);
    let (task, _tip, _syncs) = start_gossip_with_policy(network.clone(), receiver, true);
    sender.try_send((hash, block::Height(114))).unwrap();
    network.request_started.notified().await;
    for _ in 0..4 {
        sender.try_send((hash, block::Height(114))).unwrap();
    }
    yield_tasks().await;
    assert_eq!(network.requests(), vec![hash]);
    {
        let state = network.state.lock().unwrap();
        assert_eq!(state.active_requests, 1);
        assert_eq!(state.max_active_requests, 1);
    }
    assert_eq!(sender.capacity(), 4);
    task.abort();
    let _ = task.await;
    assert_eq!(network.state.lock().unwrap().active_requests, 0);
}

#[tokio::test(start_paused = true)]
async fn wcash_policy_keeps_the_existing_single_attempt_lane() {
    let older = block::Hash([115; 32]);
    let fresh = block::Hash([116; 32]);
    let network = ControlledNetwork::default();
    network.set_ready();
    network
        .state
        .lock()
        .unwrap()
        .stalled_responses
        .insert(older);
    let (sender, receiver) = mpsc::channel(4);
    let (task, _tip, _syncs) = start_gossip_with_policy(network.clone(), receiver, false);
    sender.try_send((older, block::Height(115))).unwrap();
    network.request_started.notified().await;
    sender.try_send((fresh, block::Height(116))).unwrap();
    assert!(tokio::time::timeout(
        Duration::from_millis(100),
        network.request_started.notified(),
    )
    .await
    .is_err());
    assert_eq!(network.requests(), vec![older]);
    assert_eq!(network.state.lock().unwrap().active_requests, 1);

    tokio::time::advance(TIPS_RESPONSE_TIMEOUT).await;
    yield_tasks().await;
    assert_eq!(network.requests(), vec![older, fresh]);
    assert_eq!(network.state.lock().unwrap().max_active_requests, 1);
    task.abort();
    let _ = task.await;
}

#[tokio::test(start_paused = true)]
async fn zcash_priority_keeps_two_owned_attempts_and_retains_work_when_both_are_busy() {
    let first = block::Hash([117; 32]);
    let second = block::Hash([118; 32]);
    let third = block::Hash([119; 32]);
    let network = ControlledNetwork::default();
    network.set_ready();
    network
        .state
        .lock()
        .unwrap()
        .stalled_responses
        .extend([first, second]);
    let (sender, receiver) = mpsc::channel(4);
    let (task, _tip, _syncs) = start_gossip_with_policy(network.clone(), receiver, true);
    sender.try_send((first, block::Height(117))).unwrap();
    network.request_started.notified().await;
    sender.try_send((second, block::Height(118))).unwrap();
    network.request_started.notified().await;
    sender.try_send((third, block::Height(119))).unwrap();
    yield_tasks().await;
    assert_eq!(network.requests(), vec![first, second]);
    assert_eq!(network.state.lock().unwrap().active_requests, 2);

    tokio::time::advance(TIPS_RESPONSE_TIMEOUT).await;
    yield_tasks().await;
    assert!(network.requests().contains(&third));
    network.state.lock().unwrap().stalled_responses.clear();
    tokio::time::advance(GOSSIP_RETRY_DELAY).await;
    yield_tasks().await;
    let requests = network.requests();
    assert_eq!(requests.iter().filter(|hash| **hash == first).count(), 2);
    assert_eq!(requests.iter().filter(|hash| **hash == second).count(), 2);
    assert_eq!(requests.iter().filter(|hash| **hash == third).count(), 1);
    assert_eq!(network.state.lock().unwrap().max_active_requests, 2);
    assert_eq!(network.state.lock().unwrap().active_requests, 0);
    task.abort();
    let _ = task.await;
}

#[tokio::test(start_paused = true)]
async fn unready_network_retains_mined_hashes_and_keeps_receiving() {
    let (sync_status, _recent_syncs) = SyncStatus::new();
    let (_tip_sender, _, chain_tip) = ChainTipSender::new(None, &Network::Mainnet);
    let network = ControlledNetwork::default();
    let (sender, receiver) = mpsc::channel(4);
    let first = block::Hash([1; 32]);
    let second = block::Hash([2; 32]);
    let task = tokio::spawn(gossip_best_tip_block_hashes(
        sync_status,
        chain_tip,
        network.clone(),
        Some(receiver),
        false,
    ));

    sender.send((first, block::Height(1))).await.unwrap();
    network.readiness_polled.notified().await;
    sender.send((second, block::Height(2))).await.unwrap();
    tokio::time::advance(TIPS_RESPONSE_TIMEOUT + Duration::from_secs(2)).await;
    yield_tasks().await;
    assert_eq!(
        sender.capacity(),
        4,
        "waiting for a ready peer must not stop receiving mined blocks"
    );
    assert!(network.requests().is_empty());
    assert!(!task.is_finished());

    network.set_ready();
    tokio::time::advance(Duration::from_secs(2)).await;
    yield_tasks().await;
    let requests = network.requests();
    assert!(requests.contains(&first));
    assert!(requests.contains(&second));
    task.abort();
}

#[tokio::test(start_paused = true)]
async fn stalled_response_does_not_block_a_fresh_mined_hash_or_leak_tasks() {
    let first = block::Hash([3; 32]);
    let second = block::Hash([4; 32]);
    let network = ControlledNetwork::default();
    network.set_ready();
    network
        .state
        .lock()
        .unwrap()
        .stalled_responses
        .insert(first);
    let (sender, receiver) = mpsc::channel(4);
    let (task, _tip, _syncs) = start_gossip(network.clone(), receiver);

    sender.send((first, block::Height(3))).await.unwrap();
    network.request_started.notified().await;
    sender.send((second, block::Height(4))).await.unwrap();
    tokio::time::advance(TIPS_RESPONSE_TIMEOUT).await;
    yield_tasks().await;
    assert!(network.requests().contains(&second));
    assert_eq!(network.state.lock().unwrap().max_active_requests, 1);

    network
        .state
        .lock()
        .unwrap()
        .stalled_responses
        .remove(&first);
    tokio::time::advance(GOSSIP_RETRY_DELAY).await;
    yield_tasks().await;
    assert_eq!(
        network
            .requests()
            .iter()
            .filter(|hash| **hash == first)
            .count(),
        2
    );
    assert_eq!(network.state.lock().unwrap().active_requests, 0);
    task.abort();
}

#[tokio::test(start_paused = true)]
async fn ordinary_timeout_is_best_effort_while_mined_timeout_is_retained() {
    for mined in [false, true] {
        let hash = block::Hash([21 + u8::from(mined); 32]);
        let network = ControlledNetwork::default();
        network.set_ready();
        network.state.lock().unwrap().stalled_responses.insert(hash);
        let mut pending = PendingGossip::default();
        if mined {
            pending.enqueue_mined(hash, block::Height(21), Instant::now());
        } else {
            pending.enqueue_committed(hash, block::Height(21), Instant::now());
        }
        let advertisement = pending.take_ready(Instant::now()).unwrap();
        let attempt = tokio::spawn(broadcast_attempt(
            network.clone(),
            advertisement,
            AttemptLane::General,
        ));
        network.request_started.notified().await;
        tokio::time::advance(TIPS_RESPONSE_TIMEOUT).await;
        assert!(matches!(
            attempt.await.unwrap().outcome,
            AttemptOutcome::Incomplete("network_request_timeout")
        ));
        assert_eq!(network.state.lock().unwrap().active_requests, 0);

        pending.retry(advertisement, Instant::now());
        let retained = pending.take_ready(Instant::now() + GOSSIP_RETRY_DELAY);
        if mined {
            assert_eq!(retained.unwrap().hash, hash);
            assert!(pending.mined_hashes.contains(&hash));
        } else {
            assert!(
                retained.is_none(),
                "ordinary failures must not race delayed newer-tip observation with a stale retry"
            );
            assert!(pending.next_deadline().is_none());
        }
    }
}

#[tokio::test(start_paused = true)]
async fn attempt_timings_separate_queue_readiness_and_request_waits() {
    let hash = block::Hash([128; 32]);
    let network = ControlledNetwork::default();
    network.state.lock().unwrap().stalled_responses.insert(hash);
    let mut pending = PendingGossip::default();
    pending.enqueue_mined(hash, block::Height(128), Instant::now());
    tokio::time::advance(Duration::from_millis(25)).await;
    let advertisement = pending.take_ready(Instant::now()).unwrap();
    let attempt = tokio::spawn(broadcast_attempt(
        network.clone(),
        advertisement,
        AttemptLane::General,
    ));
    network.readiness_polled.notified().await;
    tokio::time::advance(Duration::from_millis(40)).await;
    network.set_ready();
    network.request_started.notified().await;
    tokio::time::advance(Duration::from_millis(75)).await;
    network.response_released.notify_one();
    let report = attempt.await.unwrap();
    assert!(matches!(report.outcome, AttemptOutcome::Completed));
    assert_eq!(
        report.timing,
        AttemptTiming {
            queue_wait_ms: 25,
            readiness_wait_ms: 40,
            request_elapsed_ms: 75,
            total_elapsed_ms: 140,
        }
    );
    assert_eq!(network.state.lock().unwrap().active_requests, 0);
}

#[tokio::test(start_paused = true)]
async fn attempt_timings_keep_readiness_and_request_within_one_deadline() {
    for ready_after in [None, Some(Duration::from_millis(40))] {
        let hash = block::Hash([129; 32]);
        let network = ControlledNetwork::default();
        network.state.lock().unwrap().stalled_responses.insert(hash);
        let mut pending = PendingGossip::default();
        pending.enqueue_mined(hash, block::Height(129), Instant::now());
        tokio::time::advance(Duration::from_millis(25)).await;
        let advertisement = pending.take_ready(Instant::now()).unwrap();
        let attempt = tokio::spawn(broadcast_attempt(
            network.clone(),
            advertisement,
            AttemptLane::General,
        ));
        network.readiness_polled.notified().await;
        if let Some(readiness_wait) = ready_after {
            tokio::time::advance(readiness_wait).await;
            network.set_ready();
            network.request_started.notified().await;
            tokio::time::advance(TIPS_RESPONSE_TIMEOUT - readiness_wait).await;
        } else {
            tokio::time::advance(TIPS_RESPONSE_TIMEOUT).await;
        }
        let report = attempt.await.unwrap();
        let (reason, readiness_wait_ms, request_elapsed_ms) = if ready_after.is_some() {
            ("network_request_timeout", 40, 5_960)
        } else {
            ("service_readiness_timeout", 6_000, 0)
        };
        assert!(matches!(report.outcome, AttemptOutcome::Incomplete(actual) if actual == reason));
        assert_eq!(
            report.timing,
            AttemptTiming {
                queue_wait_ms: 25,
                readiness_wait_ms,
                request_elapsed_ms,
                total_elapsed_ms: 6_025,
            }
        );
        assert_eq!(network.state.lock().unwrap().active_requests, 0);
    }
}

#[tokio::test(start_paused = true)]
async fn retry_queue_timing_excludes_pacing_but_total_preserves_lifecycle_age() {
    let hash = block::Hash([130; 32]);
    let network = ControlledNetwork::default();
    network.set_ready();
    let mut pending = PendingGossip::default();
    let now = Instant::now();
    pending.enqueue_mined(hash, block::Height(130), now);
    let first = pending.take_ready(now).unwrap();
    pending.retry(first, now);
    tokio::time::advance(GOSSIP_RETRY_DELAY + Duration::from_millis(30)).await;
    let retry = pending.take_ready(Instant::now()).unwrap();
    let report = broadcast_attempt(network, retry, AttemptLane::General).await;
    assert!(matches!(report.outcome, AttemptOutcome::Completed));
    assert_eq!(
        report.timing,
        AttemptTiming {
            queue_wait_ms: 30,
            readiness_wait_ms: 0,
            request_elapsed_ms: 0,
            total_elapsed_ms: 1_030,
        }
    );
}

#[tokio::test(start_paused = true)]
async fn closing_mined_input_does_not_cancel_a_pending_retry() {
    let hash = block::Hash([5; 32]);
    let network = ControlledNetwork::default();
    let (sender, receiver) = mpsc::channel(1);
    let (task, _tip, _syncs) = start_gossip(network.clone(), receiver);
    sender.send((hash, block::Height(5))).await.unwrap();
    network.readiness_polled.notified().await;
    drop(sender);
    tokio::time::advance(TIPS_RESPONSE_TIMEOUT).await;
    yield_tasks().await;
    network.set_ready();
    tokio::time::advance(GOSSIP_RETRY_DELAY).await;
    yield_tasks().await;
    assert_eq!(network.requests(), vec![hash]);
    assert!(!task.is_finished());
    task.abort();
}

#[tokio::test(start_paused = true)]
async fn permanent_readiness_failure_is_reported() {
    let network = ControlledNetwork::default();
    network.state.lock().unwrap().permanent_error = true;
    let (sender, receiver) = mpsc::channel(1);
    let (task, _tip, _syncs) = start_gossip(network, receiver);
    sender
        .send((block::Hash([6; 32]), block::Height(6)))
        .await
        .unwrap();
    assert!(matches!(
        task.await.unwrap(),
        Err(BlockGossipError::PeerSetReadiness(_))
    ));
}

#[test]
fn pending_mined_capacity_includes_in_flight_and_deduplicates_hashes() {
    let now = Instant::now();
    let mut pending = PendingGossip::default();
    for byte in 0..MAX_PENDING_MINED_BLOCKS {
        // Queue capacity is 64, so these test indices fit in u8 and u32.
        pending.enqueue_mined(
            block::Hash([byte as u8; 32]),
            block::Height(byte as u32),
            now,
        );
    }
    assert!(!pending.has_capacity());
    let advertisement = pending.take_ready(now).unwrap();
    assert!(
        !pending.has_capacity(),
        "in-flight obligations still use capacity"
    );
    pending.enqueue_mined(advertisement.hash, advertisement.height, now);
    assert_eq!(pending.mined_hashes.len(), MAX_PENDING_MINED_BLOCKS);
    pending.complete(advertisement);
    assert!(pending.has_capacity());
    assert_eq!(pending.mined_hashes.len(), MAX_PENDING_MINED_BLOCKS - 1);
}

#[test]
fn pending_capacity_includes_both_owned_zcash_lanes() {
    let now = Instant::now();
    let mut pending = PendingGossip::default();
    for byte in 0..MAX_PENDING_MINED_BLOCKS {
        // The fixed queue capacity is below both integer type limits.
        pending.enqueue_mined(
            block::Hash([byte as u8; 32]),
            block::Height(byte as u32),
            now,
        );
    }
    let general = pending.take_ready(now).unwrap();
    let fresh = pending.take_fresh().unwrap();
    assert_ne!(general.hash, fresh.hash);
    assert!(!pending.has_capacity());
    pending.enqueue_mined(general.hash, general.height, now);
    pending.enqueue_mined(fresh.hash, fresh.height, now);
    assert_eq!(pending.mined_hashes.len(), MAX_PENDING_MINED_BLOCKS);
    pending.retry(general, now);
    pending.retry(fresh, now);
    assert!(
        !pending.has_capacity(),
        "retrying does not release an obligation"
    );
    assert_eq!(pending.retries.len(), 2);
}

#[test]
fn fresh_mined_hashes_and_retained_retries_are_fair() {
    let now = Instant::now();
    let mut pending = PendingGossip::default();
    let old_hash = block::Hash([7; 32]);
    pending.enqueue_mined(old_hash, block::Height(7), now);
    let old = pending.take_ready(now).unwrap();
    pending.retry(old, now);
    for byte in 8..=12 {
        pending.enqueue_mined(block::Hash([byte; 32]), block::Height(u32::from(byte)), now);
    }
    let due = now + GOSSIP_RETRY_DELAY;
    let mut attempts = Vec::new();
    for _ in 0..=FRESH_ATTEMPTS_PER_RETRY {
        let advertisement = pending.take_ready(due).unwrap();
        attempts.push(advertisement.hash);
        pending.complete(advertisement);
    }
    assert!(
        attempts.contains(&old_hash),
        "fresh arrivals cannot starve a due retry"
    );
    assert!(attempts.iter().any(|hash| *hash != old_hash));
    assert!(
        pending.take_ready(due).is_some(),
        "new work keeps progressing after retry"
    );
}

#[test]
fn fresh_priority_attempts_preserve_general_lane_retry_and_tip_fairness() {
    let now = Instant::now();
    let mut pending = PendingGossip::default();
    let older = block::Hash([120; 32]);
    let ordinary = block::Hash([121; 32]);
    pending.enqueue_mined(older, block::Height(120), now);
    let attempt = pending.take_ready(now).unwrap();
    pending.retry(attempt, now);
    pending.enqueue_committed(ordinary, block::Height(121), now);
    for byte in 122..=127 {
        pending.enqueue_mined(block::Hash([byte; 32]), block::Height(u32::from(byte)), now);
    }
    for _ in 0..FRESH_ATTEMPTS_PER_RETRY {
        let fresh = pending.take_fresh().unwrap();
        pending.complete(fresh);
    }
    let due = now + GOSSIP_RETRY_DELAY;
    let committed = pending.take_ready(due).unwrap();
    assert_eq!(committed.hash, ordinary);
    pending.complete(committed);
    let fresh = pending.take_fresh().unwrap();
    pending.complete(fresh);
    let retry = pending.take_ready(due).unwrap();
    assert_eq!(retry.hash, older);
    assert_eq!(retry.attempts, 2);
    assert!(
        !pending.fresh.is_empty(),
        "fresh work did not starve the due retry"
    );
}

#[test]
fn repeated_mined_timeouts_cannot_starve_a_committed_tip() {
    let now = Instant::now();
    let mut pending = PendingGossip::default();
    for byte in 13..=14 {
        pending.enqueue_mined(block::Hash([byte; 32]), block::Height(u32::from(byte)), now);
        let advertisement = pending.take_ready(now).unwrap();
        pending.retry(advertisement, now);
    }
    let tip = block::Hash([15; 32]);
    pending.enqueue_committed(tip, block::Height(15), now);
    let mut now = now + GOSSIP_RETRY_DELAY;
    let mut saw_committed = false;
    for _ in 0..=MINED_ATTEMPTS_PER_COMMITTED {
        let advertisement = pending.take_ready(now).unwrap();
        if !advertisement.mined {
            assert_eq!(advertisement.hash, tip);
            saw_committed = true;
            break;
        }
        // A timed-out request is retained and the other retry is already due.
        now += TIPS_RESPONSE_TIMEOUT;
        pending.retry(advertisement, now);
    }
    assert!(saw_committed);
    assert_eq!(pending.mined_hashes.len(), 2);
}

#[test]
fn ordinary_tips_coalesce_without_replacing_mined_obligations() {
    let now = Instant::now();
    let mut pending = PendingGossip::default();
    let mined = block::Hash([16; 32]);
    pending.enqueue_mined(mined, block::Height(16), now);
    pending.enqueue_committed(block::Hash([17; 32]), block::Height(17), now);
    pending.enqueue_committed(block::Hash([18; 32]), block::Height(18), now);
    let advertisement = pending.take_ready(now).unwrap();
    assert!(advertisement.mined);
    assert_eq!(advertisement.hash, mined);
    pending.retry(advertisement, now);
    let tip = pending.take_ready(now).unwrap();
    assert!(!tip.mined);
    assert_eq!(tip.hash, block::Hash([18; 32]));
    pending.enqueue_committed(block::Hash([19; 32]), block::Height(19), now);
    pending.retry(tip, now);
    assert_eq!(pending.take_ready(now).unwrap().hash, block::Hash([19; 32]));
    assert!(pending.mined_hashes.contains(&mined));
}

#[test]
fn completed_mined_request_suppresses_the_same_ordinary_tip() {
    let now = Instant::now();
    let mut pending = PendingGossip::default();
    let hash = block::Hash([20; 32]);
    pending.enqueue_mined(hash, block::Height(20), now);
    pending.enqueue_committed(hash, block::Height(20), now);
    let advertisement = pending.take_ready(now).unwrap();
    pending.complete(advertisement);
    assert!(pending.take_ready(now).is_none());
    assert!(pending.next_deadline().is_none());
}

#[tokio::test(start_paused = true)]
async fn saturated_pending_storage_applies_backpressure_and_recovers_without_loss() {
    let network = ControlledNetwork::default();
    let (sender, receiver) = mpsc::channel(4);
    let (task, _tip, _syncs) = start_gossip(network.clone(), receiver);
    for index in 0..MAX_PENDING_MINED_BLOCKS {
        // At most 68 test notifications are used, so the identifiers fit in u8/u32.
        sender
            .send((block::Hash([index as u8; 32]), block::Height(index as u32)))
            .await
            .unwrap();
    }
    yield_tasks().await;
    assert_eq!(sender.capacity(), 4);
    for index in MAX_PENDING_MINED_BLOCKS..MAX_PENDING_MINED_BLOCKS + 4 {
        // These identifiers are below 68 and fit in u8/u32.
        sender
            .try_send((block::Hash([index as u8; 32]), block::Height(index as u32)))
            .unwrap();
    }
    yield_tasks().await;
    assert_eq!(
        sender.capacity(),
        0,
        "a full pending queue stops draining RPC notifications"
    );
    assert!(matches!(
        sender.try_send((block::Hash([100; 32]), block::Height(100))),
        Err(mpsc::error::TrySendError::Full(_))
    ));
    network.set_ready();
    yield_tasks().await;
    let requests = network.requests();
    assert_eq!(requests.len(), MAX_PENDING_MINED_BLOCKS + 4);
    assert_eq!(
        requests.iter().copied().collect::<HashSet<_>>().len(),
        MAX_PENDING_MINED_BLOCKS + 4
    );
    assert_eq!(network.state.lock().unwrap().max_active_requests, 1);
    assert_eq!(sender.capacity(), 4);
    task.abort();
}

#[tokio::test(start_paused = true)]
async fn failed_and_local_only_mined_transport_retries_survive_ordinary_observation() {
    let hash = block::Hash([104; 32]);
    let network = ControlledNetwork::default();
    network.set_ready();
    network.state.lock().unwrap().mined_reports.insert(
        hash,
        zn::MinedBlockAdvertisement {
            failed: 1,
            ..Default::default()
        },
    );
    let (sender, receiver) = mpsc::channel(4);
    let (task, mut tip, mut syncs) = start_gossip(network.clone(), receiver);
    sender.try_send((hash, block::Height(104))).unwrap();
    tokio::time::timeout(Duration::from_secs(1), network.request_started.notified())
        .await
        .unwrap();
    yield_tasks().await;
    assert_eq!(network.state.lock().unwrap().mined_requests.len(), 1);

    // A local sidecar can now receive the inventory, including ordinary tip gossip.
    // Its observation must not complete the distinct mined delivery obligation.
    network.state.lock().unwrap().mined_reports.insert(
        hash,
        zn::MinedBlockAdvertisement {
            local_sent: 1,
            ..Default::default()
        },
    );
    syncs.push_obtain_tips_length(0);
    tip.set_finalized_tip(zebra_state::ChainTipBlock {
        hash,
        height: block::Height(104),
        time: std::time::SystemTime::UNIX_EPOCH.into(),
        transactions: Vec::new(),
        transaction_hashes: Arc::from([]),
        previous_block_hash: block::Hash([103; 32]),
    });
    for _ in 0..10 {
        tokio::time::advance(GOSSIP_RETRY_DELAY).await;
        yield_tasks().await;
    }
    let before_recovery = {
        let state = network.state.lock().unwrap();
        assert!(state.ordinary_requests.contains(&hash));
        assert!(
            state.mined_requests.len() > 2,
            "local-only sends remain retryable"
        );
        assert_eq!(state.max_active_requests, 1);
        state.mined_requests.len()
    };

    // A partial fanout with a non-local transport success can finish this attempt.
    // The durable backend still needs independent receipt/adoption evidence.
    network.state.lock().unwrap().mined_reports.insert(
        hash,
        zn::MinedBlockAdvertisement {
            non_local_sent: 1,
            local_sent: 1,
            failed: 1,
        },
    );
    tokio::time::timeout(GOSSIP_RETRY_DELAY + Duration::from_secs(1), async {
        loop {
            if network.state.lock().unwrap().mined_requests.len() > before_recovery {
                break;
            }
            network.request_started.notified().await;
        }
    })
    .await
    .expect("the retained winner retries after a non-local peer recovers");
    yield_tasks().await;
    tokio::time::advance(PEER_GOSSIP_DELAY + TIPS_RESPONSE_TIMEOUT).await;
    yield_tasks().await;
    assert_eq!(
        network.state.lock().unwrap().mined_requests.len(),
        before_recovery + 1
    );
    assert!(!task.is_finished());
    task.abort();
    let _ = task.await;
}

/// Exercise the tip watcher and dispatch loop together while both traffic kinds stall.
#[tokio::test(start_paused = true)]
async fn newer_committed_tip_progresses_while_mined_delivery_retries() {
    let first_tip = block::Hash([101; 32]);
    let newer_tip = block::Hash([102; 32]);
    let mined = block::Hash([103; 32]);
    let network = ControlledNetwork::default();
    network.set_ready();
    network
        .state
        .lock()
        .unwrap()
        .stalled_responses
        .extend([first_tip, mined]);
    let (sender, receiver) = mpsc::channel(4);
    let (task, mut tip_sender, mut recent_syncs) = start_gossip(network.clone(), receiver);
    recent_syncs.push_obtain_tips_length(0);

    // The watcher only consumes tip metadata, so no validation/state fixture is needed.
    let tip_block = |hash, height, previous_block_hash| zebra_state::ChainTipBlock {
        hash,
        height,
        time: std::time::SystemTime::UNIX_EPOCH.into(),
        transactions: Vec::new(),
        transaction_hashes: Arc::from([]),
        previous_block_hash,
    };
    tip_sender.set_finalized_tip(tip_block(
        first_tip,
        block::Height(101),
        block::Hash([100; 32]),
    ));
    tokio::time::timeout(
        PEER_GOSSIP_DELAY + Duration::from_secs(1),
        network.request_started.notified(),
    )
    .await
    .expect("the first real tip change reaches an ordinary gossip attempt");
    assert_eq!(network.requests(), vec![first_tip]);

    // Queue mined work while A's ordinary request is still stalled, then advance to B.
    sender.try_send((mined, block::Height(103))).unwrap();
    tip_sender.set_finalized_tip(tip_block(newer_tip, block::Height(102), first_tip));
    tokio::time::timeout(
        TIPS_RESPONSE_TIMEOUT + Duration::from_secs(1),
        network.request_started.notified(),
    )
    .await
    .expect("the timed-out ordinary request releases the dispatcher for mined work");
    assert_eq!(network.requests(), vec![first_tip, mined]);

    tokio::time::timeout(PEER_GOSSIP_DELAY + TIPS_RESPONSE_TIMEOUT * 5, async {
        loop {
            if network.requests().contains(&newer_tip) {
                break;
            }
            network.request_started.notified().await;
        }
    })
    .await
    .expect("the newer committed tip progresses while mined requests remain incomplete");

    // Keep the mined request stalled through another retry opportunity. A must stay dropped.
    tokio::time::timeout(
        TIPS_RESPONSE_TIMEOUT + GOSSIP_RETRY_DELAY + Duration::from_secs(1),
        async {
            loop {
                if network
                    .requests()
                    .iter()
                    .filter(|hash| **hash == mined)
                    .count()
                    >= 2
                {
                    break;
                }
                network.request_started.notified().await;
            }
        },
    )
    .await
    .expect("the mined notification survives its timeout and is retried");
    let requests = network.requests();
    assert_eq!(
        requests.iter().filter(|hash| **hash == first_tip).count(),
        1,
        "an obsolete ordinary tip is never retried after the chain advances"
    );
    assert_eq!(
        requests.iter().filter(|hash| **hash == newer_tip).count(),
        1,
        "the current tip receives its ordinary best-effort attempt"
    );
    assert_eq!(network.state.lock().unwrap().max_active_requests, 1);
    assert!(!task.is_finished());
    task.abort();
    let _ = task.await;
}
