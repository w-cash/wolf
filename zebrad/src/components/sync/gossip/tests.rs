//! Bounded block-gossip delivery and recovery regressions.

#![allow(clippy::unwrap_in_result)]

use std::{
    collections::HashSet,
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
    stalled_responses: HashSet<block::Hash>,
}

#[derive(Clone, Default)]
struct ControlledNetwork {
    state: Arc<Mutex<NetworkState>>,
    readiness_waker: Arc<AtomicWaker>,
    readiness_polled: Arc<tokio::sync::Notify>,
    request_started: Arc<tokio::sync::Notify>,
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
        let hash = match request {
            zn::Request::AdvertiseBlockToAll(hash) | zn::Request::AdvertiseBlock(hash, _) => hash,
            _ => panic!("block gossip only sends block advertisements"),
        };
        let mut state = self.state.lock().unwrap();
        state.requests.push(hash);
        state.active_requests += 1;
        state.max_active_requests = state.max_active_requests.max(state.active_requests);
        let stalled = state.stalled_responses.contains(&hash);
        drop(state);
        self.request_started.notify_one();
        let guard = ActiveRequest(Arc::clone(&self.state));
        async move {
            let _guard = guard;
            if stalled {
                std::future::pending().await
            } else {
                Ok(zn::Response::Nil)
            }
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
    let (sync_status, recent_syncs) = SyncStatus::new();
    let (tip_sender, _, chain_tip) = ChainTipSender::new(None, &Network::Mainnet);
    let task = tokio::spawn(gossip_best_tip_block_hashes(
        sync_status,
        chain_tip,
        network,
        Some(receiver),
    ));
    (task, tip_sender, recent_syncs)
}

async fn yield_tasks() {
    for _ in 0..32 {
        tokio::task::yield_now().await;
    }
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
