//! Queued broadcast response futures must remain alive until peer delivery completes.

use std::time::Duration;

use futures::FutureExt;
use tower::{Service, ServiceExt};

use zebra_chain::{block, parameters::Network};

use crate::{
    constants::CURRENT_NETWORK_PROTOCOL_VERSION, peer::MinimumPeerVersion, Config,
    MinedBlockAdvertisement, Request, Response,
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
            .call(Request::AdvertiseBlock(block::Hash([9; 32]), None));
        for handle in &mut handles {
            let request = handle
                .try_to_receive_outbound_client_request()
                .request()
                .unwrap();
            // Ordinary broadcasts must keep their best-effort return contract.
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

#[test]
fn disconnected_queued_peer_does_not_hold_completed_broadcast_open() {
    let (runtime, _init_guard) = zebra_test::init_async();
    let _guard = runtime.enter();
    let peers = PeerVersions {
        peer_versions: vec![CURRENT_NETWORK_PROTOCOL_VERSION; 2],
    };
    let (discovered, mut handles) = peers.mock_peer_discovery();
    let mut busy = handles.pop().unwrap();
    let mut ready = handles.pop().unwrap();
    let (minimum_version, _tip) = MinimumPeerVersion::with_mock_chain_tip(&Network::Mainnet);

    runtime.block_on(async move {
        let (mut peer_set, _guard) = PeerSetBuilder::new()
            .with_discover(discovered)
            .with_minimum_peer_version(minimum_version)
            .max_conns_per_ip(2)
            .build();
        peer_set.ready().await.unwrap();
        let busy_key = "127.0.0.1:2".parse().unwrap();
        let mut service = peer_set.take_ready_service(&busy_key).unwrap();
        let find = service.call(Request::FindBlocks {
            known_blocks: Vec::new(),
            stop: None,
        });
        peer_set.push_unready(busy_key, service);
        let held = busy
            .try_to_receive_outbound_client_request()
            .request()
            .unwrap();

        let broadcast = peer_set.broadcast_all(Request::AdvertiseBlockToAll(block::Hash([41; 32])));
        ready
            .try_to_receive_outbound_client_request()
            .request()
            .unwrap()
            .tx
            .send(Ok(Response::Nil))
            .unwrap();
        // This is the same removal operation used by discovery disconnects.
        peer_set.remove(&busy_key);
        peer_set.broadcast_all_queued();
        let result = tokio::time::timeout(Duration::from_millis(100), broadcast).await;
        drop(held);
        drop(find);
        assert!(
            result.is_ok(),
            "a disconnected queued peer kept the completed fanout waiting"
        );
        assert!(result.unwrap().is_ok());
    });
}

#[test]
fn removing_last_queued_peer_releases_broadcast_without_ready_peers() {
    let (runtime, _init_guard) = zebra_test::init_async();
    let _guard = runtime.enter();
    let peers = PeerVersions {
        peer_versions: vec![CURRENT_NETWORK_PROTOCOL_VERSION],
    };
    let (discovered, mut handles) = peers.mock_peer_discovery();
    let mut peer = handles.pop().unwrap();
    let (minimum_version, _tip) = MinimumPeerVersion::with_mock_chain_tip(&Network::Mainnet);
    runtime.block_on(async move {
        let (mut peer_set, _guard) = PeerSetBuilder::new()
            .with_discover(discovered)
            .with_minimum_peer_version(minimum_version)
            .build();
        let find = peer_set.ready().await.unwrap().call(Request::FindBlocks {
            known_blocks: Vec::new(),
            stop: None,
        });
        let held = peer
            .try_to_receive_outbound_client_request()
            .request()
            .unwrap();
        let broadcast = peer_set.broadcast_all(Request::AdvertiseBlockToAll(block::Hash([42; 32])));
        let key = "127.0.0.1:1".parse().unwrap();
        peer_set.remove(&key);
        assert!(peer_set.ready_services.is_empty());
        // No future ready-peer poll is available to perform delayed cleanup.
        let result = tokio::time::timeout(Duration::from_millis(100), broadcast).await;
        drop(held);
        drop(find);
        assert!(
            result.is_ok(),
            "the last disconnected peer retained the fanout channel"
        );
        assert!(result.unwrap().is_ok());
    });
}

#[test]
fn cancelled_broadcast_releases_queue_without_cancelling_busy_peer() {
    let (runtime, _init_guard) = zebra_test::init_async();
    let _guard = runtime.enter();
    let peers = PeerVersions {
        peer_versions: vec![CURRENT_NETWORK_PROTOCOL_VERSION],
    };
    let (discovered, mut handles) = peers.mock_peer_discovery();
    let mut peer = handles.pop().unwrap();
    let (minimum_version, _tip) = MinimumPeerVersion::with_mock_chain_tip(&Network::Mainnet);
    runtime.block_on(async move {
        let (mut peer_set, _guard) = PeerSetBuilder::new()
            .with_discover(discovered)
            .with_minimum_peer_version(minimum_version)
            .build();
        let find = peer_set.ready().await.unwrap().call(Request::FindBlocks {
            known_blocks: Vec::new(),
            stop: None,
        });
        let held = peer
            .try_to_receive_outbound_client_request()
            .request()
            .unwrap();
        let broadcast = peer_set.broadcast_all(Request::AdvertiseBlockToAll(block::Hash([43; 32])));
        drop(broadcast);
        peer_set.broadcast_all_queued();
        let released = peer_set.queued_broadcast_all.is_empty();
        let key = "127.0.0.1:1".parse().unwrap();
        assert!(peer_set.cancel_handles.contains_key(&key));
        assert!(!held.tx.is_canceled());
        held.tx.send(Ok(Response::BlockHashes(Vec::new()))).unwrap();
        find.await.unwrap();
        assert!(
            released,
            "a cancelled aggregate left a stale queued broadcast behind"
        );
    });
}

#[test]
fn concurrent_zcash_mined_hashes_wait_for_busy_peer_and_deliver_in_fifo_order() {
    let (runtime, _init_guard) = zebra_test::init_async();
    let _guard = runtime.enter();
    let peers = PeerVersions {
        peer_versions: vec![CURRENT_NETWORK_PROTOCOL_VERSION],
    };
    let (discovered, mut handles) = peers.mock_peer_discovery();
    let mut peer = handles.pop().unwrap();
    let (minimum_version, _tip) = MinimumPeerVersion::with_mock_chain_tip(&Network::Mainnet);

    runtime.block_on(async move {
        let (mut peer_set, _guard) = PeerSetBuilder::new()
            .with_config(Config {
                network: Network::Mainnet,
                ..Default::default()
            })
            .with_discover(discovered)
            .with_minimum_peer_version(minimum_version)
            .build();
        let find = peer_set.ready().await.unwrap().call(Request::FindBlocks {
            known_blocks: Vec::new(),
            stop: None,
        });
        let held = peer
            .try_to_receive_outbound_client_request()
            .request()
            .unwrap();
        let first_hash = block::Hash([51; 32]);
        let second_hash = block::Hash([52; 32]);
        let mut first = peer_set.broadcast_all(Request::AdvertiseBlockToAll(first_hash));
        let mut second = peer_set.broadcast_all(Request::AdvertiseBlockToAll(second_hash));

        // A second live hash must not replace the first obligation or make its
        // aggregate report completion before this connected peer recovers.
        assert!(first.as_mut().now_or_never().is_none());
        assert!(second.as_mut().now_or_never().is_none());
        assert_eq!(peer_set.queued_broadcast_all.len(), 2);
        held.tx.send(Ok(Response::BlockHashes(Vec::new()))).unwrap();
        find.await.unwrap();

        let _ = peer_set.ready().now_or_never();
        assert!(first.as_mut().now_or_never().is_none());
        let first_request = peer
            .try_to_receive_outbound_client_request()
            .request()
            .expect("the first hash must reach the recovered peer first");
        assert_eq!(
            first_request.request,
            Request::AdvertiseBlockToAll(first_hash)
        );
        assert!(!first_request.tx.is_canceled());
        assert!(second.as_mut().now_or_never().is_none());
        assert!(peer
            .try_to_receive_outbound_client_request()
            .request()
            .is_none());
        first_request.tx.send(Ok(Response::Nil)).unwrap();
        let expected = Response::MinedBlockAdvertisement(MinedBlockAdvertisement {
            local_sent: 1,
            ..Default::default()
        });
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), first)
                .await
                .unwrap()
                .unwrap(),
            expected
        );

        let _ = peer_set.ready().now_or_never();
        assert!(second.as_mut().now_or_never().is_none());
        let second_request = peer
            .try_to_receive_outbound_client_request()
            .request()
            .expect("the second hash remains owed until the first delivery completes");
        assert_eq!(
            second_request.request,
            Request::AdvertiseBlockToAll(second_hash)
        );
        assert!(!second_request.tx.is_canceled());
        second_request.tx.send(Ok(Response::Nil)).unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), second)
                .await
                .unwrap()
                .unwrap(),
            expected
        );
        assert!(peer_set.queued_broadcast_all.is_empty());
    });
}

#[test]
fn zcash_queued_broadcast_capacity_preserves_live_hashes_and_reclaims_closed_callers() {
    let (runtime, _init_guard) = zebra_test::init_async();
    let _guard = runtime.enter();
    let peers = PeerVersions {
        peer_versions: vec![CURRENT_NETWORK_PROTOCOL_VERSION],
    };
    let (discovered, mut handles) = peers.mock_peer_discovery();
    let mut peer = handles.pop().unwrap();
    let (minimum_version, _tip) = MinimumPeerVersion::with_mock_chain_tip(&Network::Mainnet);

    runtime.block_on(async move {
        let (mut peer_set, _guard) = PeerSetBuilder::new()
            .with_config(Config {
                network: Network::Mainnet,
                ..Default::default()
            })
            .with_discover(discovered)
            .with_minimum_peer_version(minimum_version)
            .build();
        let find = peer_set.ready().await.unwrap().call(Request::FindBlocks {
            known_blocks: Vec::new(),
            stop: None,
        });
        let held = peer
            .try_to_receive_outbound_client_request()
            .request()
            .unwrap();
        let mut live = Vec::new();
        for value in 0_u8..64 {
            let mut broadcast =
                peer_set.broadcast_all(Request::AdvertiseBlockToAll(block::Hash([value; 32])));
            assert!(broadcast.as_mut().now_or_never().is_none());
            live.push(broadcast);
        }
        let expected: Vec<_> = (0_u8..64)
            .map(|value| Request::AdvertiseBlockToAll(block::Hash([value; 32])))
            .collect();
        let queued: Vec<_> = peer_set
            .queued_broadcast_all
            .iter()
            .map(|(request, _, _)| request.clone())
            .collect();
        assert_eq!(queued, expected);

        let overflow = peer_set
            .broadcast_all(Request::AdvertiseBlockToAll(block::Hash([200; 32])))
            .await
            .expect_err("capacity must reject the 65th caller rather than replace a live hash");
        assert!(overflow.to_string().contains("retry required"));
        assert_eq!(peer_set.queued_broadcast_all.len(), 64);
        for broadcast in &mut live {
            assert!(broadcast.as_mut().now_or_never().is_none());
        }

        // Closing one caller reclaims precisely its slot. The remaining live
        // obligations retain their ordering and are not marked complete.
        drop(live.remove(0));
        let replacement_hash = block::Hash([201; 32]);
        let mut replacement =
            peer_set.broadcast_all(Request::AdvertiseBlockToAll(replacement_hash));
        assert!(replacement.as_mut().now_or_never().is_none());
        let queued: Vec<_> = peer_set
            .queued_broadcast_all
            .iter()
            .map(|(request, _, _)| request.clone())
            .collect();
        let mut expected = expected[1..].to_vec();
        expected.push(Request::AdvertiseBlockToAll(replacement_hash));
        assert_eq!(queued, expected);
        for broadcast in &mut live {
            assert!(broadcast.as_mut().now_or_never().is_none());
        }

        drop(live);
        drop(replacement);
        peer_set.broadcast_all_queued();
        assert!(peer_set.queued_broadcast_all.is_empty());
        assert!(!held.tx.is_canceled());
        held.tx.send(Ok(Response::BlockHashes(Vec::new()))).unwrap();
        find.await.unwrap();
    });
}

#[test]
fn wcash_queued_broadcasts_keep_legacy_latest_hash_replacement() {
    let (runtime, _init_guard) = zebra_test::init_async();
    let _guard = runtime.enter();
    let peers = PeerVersions {
        peer_versions: vec![CURRENT_NETWORK_PROTOCOL_VERSION],
    };
    let (discovered, mut handles) = peers.mock_peer_discovery();
    let mut peer = handles.pop().unwrap();
    let network = Network::new_wcash_regtest();
    let (minimum_version, _tip) = MinimumPeerVersion::with_mock_chain_tip(&network);

    runtime.block_on(async move {
        let (mut peer_set, _guard) = PeerSetBuilder::new()
            .with_config(Config {
                network,
                ..Default::default()
            })
            .with_discover(discovered)
            .with_minimum_peer_version(minimum_version)
            .build();
        let find = peer_set.ready().await.unwrap().call(Request::FindBlocks {
            known_blocks: Vec::new(),
            stop: None,
        });
        let held = peer
            .try_to_receive_outbound_client_request()
            .request()
            .unwrap();
        let mut first = peer_set.broadcast_all(Request::AdvertiseBlockToAll(block::Hash([61; 32])));
        let second_hash = block::Hash([62; 32]);
        let mut second = peer_set.broadcast_all(Request::AdvertiseBlockToAll(second_hash));
        assert!(matches!(
            first.as_mut().now_or_never(),
            Some(Ok(Response::Nil))
        ));
        assert!(second.as_mut().now_or_never().is_none());
        assert_eq!(peer_set.queued_broadcast_all.len(), 1);
        held.tx.send(Ok(Response::BlockHashes(Vec::new()))).unwrap();
        find.await.unwrap();
        let _ = peer_set.ready().now_or_never();
        assert!(second.as_mut().now_or_never().is_none());
        let queued = peer
            .try_to_receive_outbound_client_request()
            .request()
            .expect("Wcash retains only its latest best-effort queued hash");
        assert_eq!(queued.request, Request::AdvertiseBlockToAll(second_hash));
        assert!(!queued.tx.is_canceled());
        queued.tx.send(Ok(Response::Nil)).unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), second)
                .await
                .unwrap()
                .unwrap(),
            Response::Nil
        );
    });
}
