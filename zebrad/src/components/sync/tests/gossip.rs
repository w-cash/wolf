//! Tests for the block hash gossip task.
//!
//! Mined hashes are retained in bounded storage until local non-loopback writes complete.
//! The RPC channel applies backpressure instead of coalescing distinct mined obligations.

use std::{
    collections::HashSet,
    net::SocketAddr,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
    time::Duration,
};

use chrono::Utc;
use futures::future;
use tokio::sync::mpsc;
use tower::Service;

use zebra_chain::{
    block::{self, Height},
    parameters::Network,
};
use zebra_network as zn;
use zebra_rpc::SubmitBlockChannel;
use zebra_state::{ChainTipBlock, ChainTipSender};

use crate::{
    components::sync::{
        gossip_best_tip_block_hashes, BlockGossipError, SyncStatus, PEER_GOSSIP_DELAY,
        TIPS_RESPONSE_TIMEOUT,
    },
    BoxError,
};

/// More mined block announcements than the submit block channel can hold.
const MORE_THAN_CHANNEL_CAPACITY: u32 = 10_500;

/// The readiness of a [`GatedPeerSet`].
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum Readiness {
    Pending,
    Ready,
    Failed,
}

#[derive(Debug)]
struct GateState {
    readiness: Readiness,
    wakers: Vec<Waker>,
}

/// A fake peer set whose `poll_ready()` is controlled by the test,
/// and which forwards all requests it receives to the test.
#[derive(Clone, Debug)]
struct GatedPeerSet {
    state: Arc<Mutex<GateState>>,
    requests: mpsc::UnboundedSender<zn::Request>,
}

impl GatedPeerSet {
    fn new() -> (Self, mpsc::UnboundedReceiver<zn::Request>) {
        let (requests, request_receiver) = mpsc::unbounded_channel();
        let peer_set = GatedPeerSet {
            state: Arc::new(Mutex::new(GateState {
                readiness: Readiness::Pending,
                wakers: Vec::new(),
            })),
            requests,
        };

        (peer_set, request_receiver)
    }

    fn set_readiness(&self, readiness: Readiness) {
        let mut state = self.state.lock().expect("lock is not poisoned");
        state.readiness = readiness;
        state.wakers.drain(..).for_each(Waker::wake);
    }
}

impl Service<zn::Request> for GatedPeerSet {
    type Response = zn::Response;
    type Error = BoxError;
    type Future = future::Ready<Result<zn::Response, BoxError>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), BoxError>> {
        let mut state = self.state.lock().expect("lock is not poisoned");
        match state.readiness {
            Readiness::Pending => {
                state.wakers.push(cx.waker().clone());
                Poll::Pending
            }
            Readiness::Ready => Poll::Ready(Ok(())),
            Readiness::Failed => Poll::Ready(Err("permanent peer set failure".into())),
        }
    }

    fn call(&mut self, request: zn::Request) -> Self::Future {
        let response = if matches!(request, zn::Request::AdvertiseBlockToAll(_)) {
            zn::Response::MinedBlockAdvertisement(zn::MinedBlockAdvertisement {
                non_local_sent: 1,
                ..Default::default()
            })
        } else {
            zn::Response::Nil
        };
        let _ = self.requests.send(request);
        future::ok(response)
    }
}

fn mined_block(n: u32) -> (block::Hash, Height) {
    let mut hash = [0; 32];
    hash[..4].copy_from_slice(&n.to_le_bytes());
    (block::Hash(hash), Height(n))
}

/// More than 10,000 distinct submissions resume without loss after bounded backpressure.
#[tokio::test(start_paused = true)]
async fn gossip_backpressures_and_recovers_more_than_channel_capacity_without_loss() {
    let _init_guard = zebra_test::init();
    let (_tip, _, chain_tip_change) = ChainTipSender::new(None, &Network::Mainnet);
    let (sync_status, _recent_syncs) = SyncStatus::new();
    let (peer_set, mut requests) = GatedPeerSet::new();
    let channel = SubmitBlockChannel::new();
    let observer = channel.sender();
    let sender = observer.clone();
    let gossip_task = tokio::spawn(gossip_best_tip_block_hashes(
        sync_status,
        chain_tip_change,
        peer_set.clone(),
        Some(channel.receiver()),
    ));
    let producer = tokio::spawn(async move {
        for n in 0..MORE_THAN_CHANNEL_CAPACITY {
            sender
                .send(mined_block(n))
                .await
                .expect("the bounded receiver remains alive");
        }
    });
    for _ in 0..128 {
        tokio::task::yield_now().await;
    }
    assert!(
        !producer.is_finished(),
        "the producer must wait when retained storage and input are full"
    );
    assert_eq!(observer.capacity(), 0);
    assert!(requests.try_recv().is_err());
    assert!(!gossip_task.is_finished());
    peer_set.set_readiness(Readiness::Ready);
    let mut seen = HashSet::new();
    tokio::time::timeout(TIPS_RESPONSE_TIMEOUT * 4, async {
        while seen.len() < usize::try_from(MORE_THAN_CHANNEL_CAPACITY).unwrap() {
            let request = requests
                .recv()
                .await
                .expect("the gossip task remains alive");
            let zn::Request::AdvertiseBlockToAll(hash) = request else {
                panic!("only mined hashes were submitted");
            };
            seen.insert(hash);
        }
    })
    .await
    .expect("all retained and backpressured submissions resume after peers recover");
    producer
        .await
        .expect("the producer completed every submission");
    for n in 0..MORE_THAN_CHANNEL_CAPACITY {
        assert!(seen.contains(&mined_block(n).0));
    }
    gossip_task.abort();
}

/// Distinct mined obligations survive readiness failures and both resume when peers recover.
#[tokio::test(start_paused = true)]
async fn gossip_broadcasts_retained_mined_blocks_when_peers_become_ready() {
    let _init_guard = zebra_test::init();

    let network = Network::Mainnet;
    let (_chain_tip_sender, _latest_chain_tip, chain_tip_change) =
        ChainTipSender::new(None, &network);
    let (sync_status, _recent_syncs) = SyncStatus::new();
    let (peer_set, mut requests) = GatedPeerSet::new();

    let channel = SubmitBlockChannel::new();
    let mined_block_sender = channel.sender();

    let gossip_task = tokio::spawn(gossip_best_tip_block_hashes(
        sync_status,
        chain_tip_change,
        peer_set.clone(),
        Some(channel.receiver()),
    ));

    mined_block_sender
        .try_send(mined_block(1))
        .expect("channel has capacity");

    // The latest block is announced just before the first block has waited for the entire
    // response timeout, so it is still fresh when peers become ready.
    tokio::time::sleep(TIPS_RESPONSE_TIMEOUT - Duration::from_millis(1)).await;
    let (latest_hash, latest_height) = mined_block(2);
    mined_block_sender
        .try_send((latest_hash, latest_height))
        .expect("channel has capacity");

    tokio::time::sleep(TIPS_RESPONSE_TIMEOUT * 2).await;
    assert!(
        requests.try_recv().is_err(),
        "no peers were ready, so nothing should have been broadcast"
    );

    peer_set.set_readiness(Readiness::Ready);

    let mut retained = HashSet::new();
    for _ in 0..2 {
        let request = tokio::time::timeout(TIPS_RESPONSE_TIMEOUT, requests.recv())
            .await
            .expect("retained mined blocks resume without a new submission")
            .expect("peer set is still alive");
        let zn::Request::AdvertiseBlockToAll(hash) = request else {
            panic!("only mined hashes were submitted");
        };
        retained.insert(hash);
    }
    assert_eq!(retained, HashSet::from([mined_block(1).0, latest_hash]));

    tokio::time::sleep(TIPS_RESPONSE_TIMEOUT * 2).await;
    assert!(
        requests.try_recv().is_err(),
        "completed mined obligations do not keep retrying"
    );
    assert!(!gossip_task.is_finished());

    gossip_task.abort();
}

/// The real advertiser retains all mined hashes until a non-local fixture peer connects.
#[tokio::test(start_paused = true)]
async fn gossip_broadcasts_retained_mined_blocks_when_an_eligible_peer_connects() {
    let _init_guard = zebra_test::init();
    for second_submission in [false, true] {
        let mut peers = zn::test_support::PeerSetHarness::new(&[], None).await;
        let (_tip, _, chain_tip_change) = ChainTipSender::new(None, &Network::Mainnet);
        let (sync_status, _recent_syncs) = SyncStatus::new();
        let channel = SubmitBlockChannel::new();
        let sender = channel.sender();
        let gossip_task = tokio::spawn(gossip_best_tip_block_hashes(
            sync_status,
            chain_tip_change,
            peers.block_advertiser.clone(),
            Some(channel.receiver()),
        ));
        sender.try_send(mined_block(1)).unwrap();
        tokio::time::sleep(TIPS_RESPONSE_TIMEOUT * 2).await;
        if second_submission {
            sender.try_send(mined_block(2)).unwrap();
            tokio::task::yield_now().await;
        }
        let peer = peers.connect_peer_at(
            SocketAddr::from(([198, 51, 100, 1], 8233)).into(),
            zn::types::PeerServices::NODE_NETWORK,
        );
        let mut seen = HashSet::new();
        let expected = if second_submission { 2 } else { 1 };
        tokio::time::timeout(TIPS_RESPONSE_TIMEOUT * 3, async {
            while seen.len() < expected {
                if let Some((hash, write)) = peers.next_block_advert(peer).await {
                    seen.insert(hash);
                    write.send(Ok(())).unwrap();
                }
            }
        })
        .await
        .expect("all retained hashes reach the newly eligible connection");
        assert!(seen.contains(&mined_block(1).0));
        if second_submission {
            assert!(seen.contains(&mined_block(2).0));
        }
        tokio::time::sleep(PEER_GOSSIP_DELAY * 2).await;
        assert!(peers.next_block_advert(peer).await.is_none());
        assert!(!gossip_task.is_finished());
        gossip_task.abort();
    }
}

/// A chain tip change that happens while there are no ready peers is broadcast when peers
/// become ready, without waiting for another tip change.
#[tokio::test(start_paused = true)]
async fn gossip_broadcasts_tip_change_when_peers_become_ready() {
    let _init_guard = zebra_test::init();

    let network = Network::Mainnet;
    let (mut chain_tip_sender, _latest_chain_tip, chain_tip_change) =
        ChainTipSender::new(None, &network);
    let (sync_status, mut recent_syncs) = SyncStatus::new();
    let (peer_set, mut requests) = GatedPeerSet::new();

    // Make the sync status close to the tip, so tip changes are gossiped.
    recent_syncs.push_extend_tips_length(0);

    let gossip_task = tokio::spawn(gossip_best_tip_block_hashes(
        sync_status,
        chain_tip_change,
        peer_set.clone(),
        None,
    ));

    let (hash, height) = mined_block(1);
    chain_tip_sender.set_finalized_tip(ChainTipBlock {
        hash,
        height,
        time: Utc::now(),
        transactions: Vec::new(),
        transaction_hashes: Arc::new([]),
        previous_block_hash: block::Hash([0xff; 32]),
    });

    tokio::time::sleep(PEER_GOSSIP_DELAY + Duration::from_millis(1)).await;
    assert!(
        requests.try_recv().is_err(),
        "no peers were ready, so nothing should have been broadcast"
    );

    peer_set.set_readiness(Readiness::Ready);

    let request = tokio::time::timeout(TIPS_RESPONSE_TIMEOUT, requests.recv())
        .await
        .expect("tip change is broadcast once peers are ready")
        .expect("peer set is still alive");
    assert_eq!(request, zn::Request::AdvertiseBlock(hash, None));
    assert!(!gossip_task.is_finished());

    gossip_task.abort();
}

/// If a mined block is announced while a chain tip broadcast is waiting for ready peers, and the
/// mined block is not the best tip (for example, it is on a side chain), both the mined block and
/// the pending chain tip are broadcast when peers become ready.
#[tokio::test(start_paused = true)]
async fn gossip_keeps_pending_tip_change_when_a_side_chain_block_is_mined() {
    let _init_guard = zebra_test::init();

    let network = Network::Mainnet;
    let (mut chain_tip_sender, _latest_chain_tip, chain_tip_change) =
        ChainTipSender::new(None, &network);
    let (sync_status, mut recent_syncs) = SyncStatus::new();
    let (peer_set, mut requests) = GatedPeerSet::new();

    // Make the sync status close to the tip, so tip changes are gossiped.
    recent_syncs.push_extend_tips_length(0);

    let channel = SubmitBlockChannel::new();
    let mined_block_sender = channel.sender();

    let gossip_task = tokio::spawn(gossip_best_tip_block_hashes(
        sync_status,
        chain_tip_change,
        peer_set.clone(),
        Some(channel.receiver()),
    ));

    let (tip_hash, tip_height) = mined_block(1);
    chain_tip_sender.set_finalized_tip(ChainTipBlock {
        hash: tip_hash,
        height: tip_height,
        time: Utc::now(),
        transactions: Vec::new(),
        transaction_hashes: Arc::new([]),
        previous_block_hash: block::Hash([0xff; 32]),
    });

    // Wait until the chain tip broadcast is waiting for ready peers.
    tokio::time::sleep(PEER_GOSSIP_DELAY + Duration::from_millis(1)).await;

    // This block doesn't change the best tip.
    let (side_chain_hash, side_chain_height) = mined_block(2);
    mined_block_sender
        .try_send((side_chain_hash, side_chain_height))
        .expect("channel has capacity");

    tokio::task::yield_now().await;
    assert!(
        requests.try_recv().is_err(),
        "no peers were ready, so nothing should have been broadcast"
    );

    peer_set.set_readiness(Readiness::Ready);

    let mut broadcasts = Vec::new();
    for _ in 0..2 {
        let request = tokio::time::timeout(TIPS_RESPONSE_TIMEOUT, requests.recv())
            .await
            .expect("both blocks are broadcast once peers are ready")
            .expect("peer set is still alive");
        broadcasts.push(request);
    }

    assert!(broadcasts.contains(&zn::Request::AdvertiseBlockToAll(side_chain_hash)));
    assert!(broadcasts.contains(&zn::Request::AdvertiseBlock(tip_hash, None)));
    assert!(!gossip_task.is_finished());

    gossip_task.abort();
}

/// Permanent peer set readiness errors still stop the gossip task.
#[tokio::test(start_paused = true)]
async fn gossip_returns_permanent_peer_set_errors() {
    let _init_guard = zebra_test::init();

    let network = Network::Mainnet;
    let (_chain_tip_sender, _latest_chain_tip, chain_tip_change) =
        ChainTipSender::new(None, &network);
    let (sync_status, _recent_syncs) = SyncStatus::new();
    let (peer_set, _requests) = GatedPeerSet::new();
    peer_set.set_readiness(Readiness::Failed);

    let channel = SubmitBlockChannel::new();
    channel
        .sender()
        .try_send(mined_block(1))
        .expect("channel has capacity");

    let result = tokio::time::timeout(
        TIPS_RESPONSE_TIMEOUT * 2,
        gossip_best_tip_block_hashes(
            sync_status,
            chain_tip_change,
            peer_set,
            Some(channel.receiver()),
        ),
    )
    .await
    .expect("gossip task exits on a permanent peer set error");

    assert!(
        matches!(result, Err(BlockGossipError::PeerSetReadiness(_))),
        "unexpected gossip result: {result:?}"
    );
}
