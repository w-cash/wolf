//! Real peer-set fanout reports must distinguish local-only and failed mined sends.

use std::time::Duration;

use futures::{stream, FutureExt, Stream, StreamExt};
use tower::{discover::Change, Service, ServiceExt};

use zebra_chain::{block, parameters::Network};

use crate::{
    constants::CURRENT_NETWORK_PROTOCOL_VERSION,
    peer::{ClientTestHarness, ConnectedAddr, LoadTrackedClient, MinimumPeerVersion},
    BoxError, Config, MinedBlockAdvertisement, PeerSocketAddr, Request, Response,
};

use super::PeerSetBuilder;

fn mock_discovery(
    addresses: &[PeerSocketAddr],
) -> (
    impl Stream<Item = Result<Change<PeerSocketAddr, LoadTrackedClient>, BoxError>>,
    Vec<ClientTestHarness>,
) {
    let mut peers = Vec::new();
    let mut handles = Vec::new();
    for address in addresses {
        let (client, handle) = ClientTestHarness::build()
            .with_version(CURRENT_NETWORK_PROTOCOL_VERSION)
            .with_connected_addr(ConnectedAddr::new_inbound_direct(*address))
            .finish();
        peers.push(Ok(Change::Insert(*address, client.into())));
        handles.push(handle);
    }
    (stream::iter(peers).chain(stream::pending()), handles)
}

#[tokio::test]
async fn zcash_mined_reports_distinguish_failures_sidecars_and_non_local_sends() {
    let _init_guard = zebra_test::init();
    let public: PeerSocketAddr = "203.0.113.1:8233".parse().unwrap();
    let sidecar: PeerSocketAddr = "203.0.113.2:8233".parse().unwrap();
    let loopback: PeerSocketAddr = "127.0.0.1:8233".parse().unwrap();
    let mapped_loopback: PeerSocketAddr = "[::ffff:127.0.0.2]:8233".parse().unwrap();
    for (addresses, successes, sidecar_ips, expected) in [
        (
            vec![public],
            vec![false],
            vec![],
            MinedBlockAdvertisement {
                failed: 1,
                ..Default::default()
            },
        ),
        (
            vec![loopback],
            vec![true],
            vec![],
            MinedBlockAdvertisement {
                local_sent: 1,
                ..Default::default()
            },
        ),
        (
            vec![mapped_loopback],
            vec![true],
            vec![],
            MinedBlockAdvertisement {
                local_sent: 1,
                ..Default::default()
            },
        ),
        (
            vec![sidecar],
            vec![true],
            vec![sidecar.ip()],
            MinedBlockAdvertisement {
                local_sent: 1,
                ..Default::default()
            },
        ),
        (
            vec![public, sidecar, loopback],
            vec![true, true, false],
            vec![sidecar.ip()],
            MinedBlockAdvertisement {
                non_local_sent: 1,
                local_sent: 1,
                failed: 1,
            },
        ),
    ] {
        let (discovered, mut handles) = mock_discovery(&addresses);
        let (minimum_version, _tip) = MinimumPeerVersion::with_mock_chain_tip(&Network::Mainnet);
        let (mut peer_set, _guard) = PeerSetBuilder::new()
            .with_config(Config {
                network: Network::Mainnet,
                ..Default::default()
            })
            .with_discover(discovered)
            .with_block_gossip_peer_ips(sidecar_ips)
            .with_minimum_peer_version(minimum_version)
            .build();
        let broadcast = peer_set
            .ready()
            .await
            .unwrap()
            .call(Request::AdvertiseBlockToAll(block::Hash([31; 32])));
        for (handle, succeeds) in handles.iter_mut().zip(successes) {
            let request = handle
                .try_to_receive_outbound_client_request()
                .request()
                .unwrap();
            if succeeds {
                request.tx.send(Ok(Response::Nil)).unwrap();
            } else {
                drop(request.tx);
            }
        }
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), broadcast)
                .await
                .unwrap()
                .unwrap(),
            Response::MinedBlockAdvertisement(expected)
        );
    }
}

#[tokio::test]
async fn mined_no_candidates_and_wcash_best_effort_are_distinct() {
    let _init_guard = zebra_test::init();
    for network in [Network::Mainnet, Network::new_wcash_regtest()] {
        let (discovered, _handles) = mock_discovery(&[]);
        let (minimum_version, _tip) = MinimumPeerVersion::with_mock_chain_tip(&network);
        let (mut peer_set, _guard) = PeerSetBuilder::new()
            .with_config(Config {
                network: network.clone(),
                ..Default::default()
            })
            .with_discover(discovered)
            .with_minimum_peer_version(minimum_version)
            .build();
        let response = peer_set
            .broadcast_all(Request::AdvertiseBlockToAll(block::Hash([32; 32])))
            .await
            .unwrap();
        assert_eq!(
            response,
            if network.uses_wcash_consensus() {
                Response::Nil
            } else {
                Response::MinedBlockAdvertisement(MinedBlockAdvertisement::default())
            }
        );
    }

    let network = Network::new_wcash_regtest();
    let addresses = ["203.0.113.1:8233".parse().unwrap()];
    let (discovered, mut handles) = mock_discovery(&addresses);
    let (minimum_version, _tip) = MinimumPeerVersion::with_mock_chain_tip(&network);
    let (mut peer_set, _guard) = PeerSetBuilder::new()
        .with_config(Config {
            network,
            ..Default::default()
        })
        .with_discover(discovered)
        .with_minimum_peer_version(minimum_version)
        .build();
    let broadcast = peer_set
        .ready()
        .await
        .unwrap()
        .call(Request::AdvertiseBlockToAll(block::Hash([33; 32])));
    drop(
        handles[0]
            .try_to_receive_outbound_client_request()
            .request()
            .unwrap()
            .tx,
    );
    assert_eq!(broadcast.await.unwrap(), Response::Nil);
}

#[tokio::test]
async fn queued_mined_report_preserves_ready_and_recovered_peer_outcomes() {
    let _init_guard = zebra_test::init();
    let local: PeerSocketAddr = "127.0.0.1:8233".parse().unwrap();
    let remote: PeerSocketAddr = "203.0.113.1:8233".parse().unwrap();
    let (discovered, mut handles) = mock_discovery(&[local, remote]);
    let (minimum_version, _tip) = MinimumPeerVersion::with_mock_chain_tip(&Network::Mainnet);
    let (mut peer_set, _guard) = PeerSetBuilder::new()
        .with_config(Config {
            network: Network::Mainnet,
            ..Default::default()
        })
        .with_discover(discovered)
        .with_minimum_peer_version(minimum_version)
        .build();
    peer_set.ready().await.unwrap();
    let mut remote_service = peer_set.take_ready_service(&remote).unwrap();
    let find = remote_service.call(Request::FindBlocks {
        known_blocks: Vec::new(),
        stop: None,
    });
    peer_set.push_unready(remote, remote_service);
    let hash = block::Hash([34; 32]);
    let broadcast = tokio::spawn(peer_set.broadcast_all(Request::AdvertiseBlockToAll(hash)));
    handles[0]
        .try_to_receive_outbound_client_request()
        .request()
        .unwrap()
        .tx
        .send(Ok(Response::Nil))
        .unwrap();
    handles[1]
        .try_to_receive_outbound_client_request()
        .request()
        .unwrap()
        .tx
        .send(Ok(Response::BlockHashes(Vec::new())))
        .unwrap();
    find.await.unwrap();
    let _ = peer_set.ready().now_or_never();
    let queued = handles[1]
        .try_to_receive_outbound_client_request()
        .request()
        .unwrap();
    assert_eq!(queued.request, Request::AdvertiseBlockToAll(hash));
    assert!(!queued.tx.is_canceled());
    queued.tx.send(Ok(Response::Nil)).unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), broadcast)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        Response::MinedBlockAdvertisement(MinedBlockAdvertisement {
            non_local_sent: 1,
            local_sent: 1,
            failed: 0
        })
    );
}

#[tokio::test]
async fn timed_out_mined_fanout_cancels_peer_work_and_next_attempt_recovers() {
    let _init_guard = zebra_test::init();
    let (discovered, mut handles) = mock_discovery(&["203.0.113.1:8233".parse().unwrap()]);
    let (minimum_version, _tip) = MinimumPeerVersion::with_mock_chain_tip(&Network::Mainnet);
    let (mut peer_set, _guard) = PeerSetBuilder::new()
        .with_config(Config {
            network: Network::Mainnet,
            ..Default::default()
        })
        .with_discover(discovered)
        .with_minimum_peer_version(minimum_version)
        .build();
    let hash = block::Hash([35; 32]);
    let broadcast = peer_set
        .ready()
        .await
        .unwrap()
        .call(Request::AdvertiseBlockToAll(hash));
    let stalled = handles[0]
        .try_to_receive_outbound_client_request()
        .request()
        .unwrap();
    assert!(tokio::time::timeout(Duration::from_millis(10), broadcast)
        .await
        .is_err());
    assert!(
        stalled.tx.is_canceled(),
        "a dropped aggregate must not detach peer requests"
    );
    drop(stalled);
    let retry = peer_set
        .ready()
        .await
        .unwrap()
        .call(Request::AdvertiseBlockToAll(hash));
    handles[0]
        .try_to_receive_outbound_client_request()
        .request()
        .unwrap()
        .tx
        .send(Ok(Response::Nil))
        .unwrap();
    assert_eq!(
        retry.await.unwrap(),
        Response::MinedBlockAdvertisement(MinedBlockAdvertisement {
            non_local_sent: 1,
            ..Default::default()
        })
    );
}
