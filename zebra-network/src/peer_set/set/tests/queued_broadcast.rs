//! Queued broadcast response futures must remain alive until peer delivery completes.

use std::time::Duration;

use futures::FutureExt;
use tower::{Service, ServiceExt};

use zebra_chain::{block, parameters::Network};

use crate::{
    constants::CURRENT_NETWORK_PROTOCOL_VERSION, peer::MinimumPeerVersion, Request, Response,
};

use super::{PeerSetBuilder, PeerVersions};

#[test]
fn failed_peer_broadcasts_preserve_best_effort_return_semantics() {
    let (runtime, _init_guard) = zebra_test::init_async();
    let _guard = runtime.enter();
    let peers = PeerVersions {
        peer_versions: vec![CURRENT_NETWORK_PROTOCOL_VERSION],
    };
    let (discovered, mut handles) = peers.mock_peer_discovery();
    let (minimum_version, _best_tip) = MinimumPeerVersion::with_mock_chain_tip(&Network::Mainnet);
    runtime.block_on(async move {
        let (mut peer_set, _peer_set_guard) = PeerSetBuilder::new()
            .with_discover(discovered)
            .with_minimum_peer_version(minimum_version)
            .build();
        let broadcast = peer_set
            .ready()
            .await
            .unwrap()
            .call(Request::AdvertiseBlockToAll(block::Hash([9; 32])));
        for handle in &mut handles {
            let request = handle
                .try_to_receive_outbound_client_request()
                .request()
                .unwrap();
            // Cancel each actual service response. Diagnostic changes must not
            // turn a completed best-effort fanout into a new retry policy.
            drop(request.tx);
        }
        assert_eq!(broadcast.await.unwrap(), Response::Nil);
    });
}

#[test]
fn queued_mined_broadcast_keeps_peer_request_alive() {
    let (runtime, _init_guard) = zebra_test::init_async();
    let _guard = runtime.enter();
    let peers = PeerVersions {
        peer_versions: vec![CURRENT_NETWORK_PROTOCOL_VERSION],
    };
    let (discovered, mut handles) = peers.mock_peer_discovery();
    let mut peer = handles.pop().unwrap();
    let (minimum_version, _best_tip) = MinimumPeerVersion::with_mock_chain_tip(&Network::Mainnet);

    runtime.block_on(async move {
        let (mut peer_set, _peer_set_guard) = PeerSetBuilder::new()
            .with_discover(discovered)
            .with_minimum_peer_version(minimum_version)
            .build();
        let find = peer_set.ready().await.unwrap().call(Request::FindBlocks {
            known_blocks: Vec::new(),
            stop: None,
        });
        let hash = block::Hash([1; 32]);
        // The peer's request channel is full, so it must be owed the advert.
        let broadcast = peer_set.broadcast_all(Request::AdvertiseBlockToAll(hash));
        let broadcast = tokio::spawn(broadcast);
        let initial = peer
            .try_to_receive_outbound_client_request()
            .request()
            .unwrap();
        initial
            .tx
            .send(Ok(Response::BlockHashes(Vec::new())))
            .unwrap();
        find.await.unwrap();

        // Drive the real peer-set queue while deliberately leaving the mock
        // connection's next request unread. This removes the wire-task race.
        let _ = peer_set.ready().now_or_never();
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        let queued = peer
            .try_to_receive_outbound_client_request()
            .request()
            .expect("the recovered peer was sent the queued mined-block advert");
        assert_eq!(queued.request, Request::AdvertiseBlockToAll(hash));
        assert!(
            !queued.tx.is_canceled(),
            "dropping an unpolled queued response future cancels peer delivery"
        );
        queued.tx.send(Ok(Response::Nil)).unwrap();
        tokio::time::timeout(Duration::from_secs(1), broadcast)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    });
}

#[test]
fn ready_mined_broadcast_does_not_queue_the_same_peer_again() {
    let (runtime, _init_guard) = zebra_test::init_async();
    let _guard = runtime.enter();
    let peers = PeerVersions {
        peer_versions: vec![CURRENT_NETWORK_PROTOCOL_VERSION],
    };
    let (discovered, mut handles) = peers.mock_peer_discovery();
    let mut peer = handles.pop().unwrap();
    let (minimum_version, _best_tip) = MinimumPeerVersion::with_mock_chain_tip(&Network::Mainnet);

    runtime.block_on(async move {
        let (mut peer_set, _peer_set_guard) = PeerSetBuilder::new()
            .with_discover(discovered)
            .with_minimum_peer_version(minimum_version)
            .build();
        let hash = block::Hash([2; 32]);
        let broadcast = peer_set
            .ready()
            .await
            .unwrap()
            .call(Request::AdvertiseBlockToAll(hash));
        let request = peer
            .try_to_receive_outbound_client_request()
            .request()
            .expect("the initially ready peer receives the advert");
        assert_eq!(request.request, Request::AdvertiseBlockToAll(hash));
        request.tx.send(Ok(Response::Nil)).unwrap();

        // No further peer-set poll should be needed to complete a broadcast
        // whose only peer was already ready and has completed its request.
        tokio::time::timeout(Duration::from_millis(100), broadcast)
            .await
            .expect("initially ready peers must not be queued for duplicate delivery")
            .unwrap();
        assert!(peer
            .try_to_receive_outbound_client_request()
            .request()
            .is_none());
    });
}
