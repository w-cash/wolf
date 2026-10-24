//! Wcash template readiness follows observed sync and live full-node connectivity.

use std::{collections::HashSet, net::IpAddr};

use chrono::Utc;
use jsonrpsee::core::RpcResult;
use jsonrpsee_types::ErrorObject;
use zebra_chain::{
    block::Height, chain_sync_status::ChainSyncStatus, chain_tip::ChainTip, parameters::Network,
};
use zebra_network::{
    types::{MetaAddr, PeerServices},
    AddressBookPeers,
};

use super::{check_synced_to_tip, constants::NOT_SYNCED_ERROR_CODE};

/// Preserve ordinary network policy; only postgenesis Wcash templates use peer evidence.
pub(crate) fn check_synced_to_tip_for_template<Tip, SyncStatus, AddressBook>(
    network: &Network,
    latest_chain_tip: Tip,
    sync_status: SyncStatus,
    address_book: &AddressBook,
) -> RpcResult<()>
where
    Tip: ChainTip + Clone + Send + Sync + 'static,
    SyncStatus: ChainSyncStatus + Clone + Send + Sync + 'static,
    AddressBook: AddressBookPeers,
{
    if !network.is_wcash_mainnet()
        || latest_chain_tip
            .best_tip_height()
            .is_none_or(|height| height == Height(0))
    {
        return check_synced_to_tip(network, latest_chain_tip, sync_status);
    }

    if latest_chain_tip.best_tip_height_and_hash().is_none() || !sync_status.is_close_to_tip() {
        return Err(ErrorObject::borrowed(
            NOT_SYNCED_ERROR_CODE.code(),
            "Wcash mining requires a committed tip and completed peer synchronization",
            None,
        ));
    }

    let now = Utc::now();
    let mut seen = HashSet::with_capacity(2);
    let ready = address_book
        .recently_live_peers(now)
        .into_iter()
        .filter_map(|peer| recently_live_full_node_ip(&peer, now))
        .filter(|ip| seen.insert(*ip))
        .take(2)
        .count()
        == 2;

    if !ready {
        return Err(ErrorObject::borrowed(
            NOT_SYNCED_ERROR_CODE.code(),
            "Wcash mining requires two recently responding external full-node peers",
            None,
        ));
    }

    // A quiet canonical chain can remain at one height for hours. Its header's
    // age is not evidence of missing blocks. Parent/proposal checks remain in
    // the ordinary template and createauxblock paths.
    Ok(())
}

fn recently_live_full_node_ip(peer: &MetaAddr, now: chrono::DateTime<Utc>) -> Option<IpAddr> {
    // was_recently_live requires Responded and genuine last_response within the
    // network heartbeat bound (119 seconds); gossiped last_seen is insufficient.
    if !peer.was_recently_live(now)
        || peer.last_seen()?.checked_elapsed(now).is_none()
        || !peer
            .services()
            .is_some_and(|services| services.contains(PeerServices::NODE_NETWORK))
    {
        return None;
    }
    let ip = match peer.addr().ip() {
        IpAddr::V6(ip) => ip
            .to_ipv4_mapped()
            .map(IpAddr::V4)
            .unwrap_or(IpAddr::V6(ip)),
        ip => ip,
    };
    external_ip(ip).then_some(ip)
}

fn external_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            let [a, b, _, _] = ip.octets();
            !ip.is_private()
                && !ip.is_loopback()
                && !ip.is_link_local()
                && !ip.is_documentation()
                && !ip.is_broadcast()
                && (1..224).contains(&a)
                && !(a == 100 && (64..128).contains(&b))
                && !(a == 198 && (b == 18 || b == 19))
        }
        IpAddr::V6(ip) => {
            if let Some(mapped) = ip.to_ipv4_mapped() {
                return external_ip(IpAddr::V4(mapped));
            }
            let segments = ip.segments();
            !(ip.is_unspecified()
                || ip.is_loopback()
                || ip.is_multicast()
                || ip.is_unique_local()
                || ip.is_unicast_link_local()
                || segments[0] == 0x2001 && segments[1] == 0x0db8)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use chrono::Duration;
    use zebra_chain::{
        block,
        chain_sync_status::MockSyncStatus,
        chain_tip::{mock::MockChainTip, NoChainTip},
        serialization::DateTime32,
    };
    use zebra_network::{address_book_peers::MockAddressBookPeers, PeerSocketAddr, Version};

    use super::*;

    fn connected(address: &str, services: PeerServices, seconds_ago: i64) -> MetaAddr {
        let now = Utc::now() - Duration::seconds(seconds_ago);
        MetaAddr::new_connected(
            address.parse().unwrap(),
            &services,
            false,
            "/quiet-chain-fixture/".to_owned(),
            Version(170160),
        )
        .into_new_meta_addr(Instant::now(), now.try_into().unwrap())
    }

    fn peers() -> Vec<MetaAddr> {
        ["8.8.8.8:48233", "1.1.1.1:48233"]
            .into_iter()
            .map(|addr| connected(addr, PeerServices::NODE_NETWORK, 5))
            .collect()
    }

    fn run(network: Network, peers: Vec<MetaAddr>, synchronized: bool) -> RpcResult<()> {
        let (tip, sender) = MockChainTip::new();
        sender.send_best_tip_height(Height(38628));
        sender.send_best_tip_hash(block::Hash([17; 32]));
        sender.send_best_tip_block_time(Utc::now() - Duration::hours(6));
        sender.send_estimated_distance_to_network_chain_tip(Some(288));
        let mut sync = MockSyncStatus::default();
        sync.set_is_close_to_tip(synchronized);
        check_synced_to_tip_for_template(&network, tip, sync, &MockAddressBookPeers::new(peers))
    }

    #[test]
    fn quiet_wcash_requires_real_sync_and_two_live_full_nodes() {
        let network = Network::new_wcash_mainnet_for_tests();
        assert!(run(network.clone(), peers(), true).is_ok());
        assert!(run(network.clone(), peers(), false).is_err());
        assert!(run(network.clone(), vec![], true).is_err());
        assert!(run(network, peers().into_iter().take(1).collect(), true).is_err());
    }

    #[test]
    fn duplicate_ip_ports_do_not_count_as_two_peers() {
        let peers = ["8.8.8.8:48233", "8.8.8.8:12345"]
            .into_iter()
            .map(|addr| connected(addr, PeerServices::NODE_NETWORK, 5))
            .collect();
        assert!(run(Network::new_wcash_mainnet_for_tests(), peers, true).is_err());
    }

    #[test]
    fn mapped_ipv4_does_not_count_as_a_second_ip() {
        let peers = ["8.8.8.8:48233", "[::ffff:8.8.8.8]:12345"]
            .into_iter()
            .map(|addr| connected(addr, PeerServices::NODE_NETWORK, 5))
            .collect();
        assert!(run(Network::new_wcash_mainnet_for_tests(), peers, true).is_err());
    }

    #[test]
    fn gossip_stale_light_private_and_future_peers_do_not_admit_work() {
        let network = Network::new_wcash_mainnet_for_tests();
        for excluded in [
            connected("1.1.1.1:48233", PeerServices::NODE_NETWORK, 400),
            connected("1.1.1.1:48233", PeerServices::empty(), 5),
            connected("127.0.0.1:48233", PeerServices::NODE_NETWORK, 5),
            connected("192.168.2.5:48233", PeerServices::NODE_NETWORK, 5),
            connected("1.1.1.1:48233", PeerServices::NODE_NETWORK, -30),
            MetaAddr::new_gossiped_meta_addr(
                "1.1.1.1:48233".parse().unwrap(),
                PeerServices::NODE_NETWORK,
                DateTime32::now(),
            ),
        ] {
            assert!(run(network.clone(), vec![peers().remove(0), excluded], true).is_err());
        }
    }

    #[test]
    fn failed_or_connecting_peers_do_not_admit_work() {
        let addr: PeerSocketAddr = "1.1.1.1:48233".parse().unwrap();
        for change in [
            MetaAddr::new_reconnect(addr),
            MetaAddr::new_errored(addr, None),
        ] {
            let excluded = change
                .apply_to_meta_addr(
                    connected("1.1.1.1:48233", PeerServices::NODE_NETWORK, 5),
                    Instant::now(),
                    Utc::now(),
                )
                .unwrap();
            assert!(run(
                Network::new_wcash_mainnet_for_tests(),
                vec![peers().remove(0), excluded],
                true
            )
            .is_err());
        }
    }

    #[test]
    fn old_zcash_template_policy_and_no_tip_remain_closed() {
        assert!(run(Network::Mainnet, peers(), true).is_err());
        assert!(check_synced_to_tip_for_template(
            &Network::new_wcash_mainnet_for_tests(),
            NoChainTip,
            MockSyncStatus::default(),
            &MockAddressBookPeers::new(peers())
        )
        .is_err());
    }

    #[test]
    fn genesis_bootstrap_keeps_its_exact_existing_exception() {
        let (tip, sender) = MockChainTip::new();
        sender.send_best_tip_height(Height(0));
        assert!(check_synced_to_tip_for_template(
            &Network::new_wcash_mainnet_for_tests(),
            tip,
            MockSyncStatus::default(),
            &MockAddressBookPeers::default()
        )
        .is_ok());
    }

    #[test]
    fn losing_live_peers_closes_the_next_request() {
        let network = Network::new_wcash_mainnet_for_tests();
        assert!(run(network.clone(), peers(), true).is_ok());
        assert!(run(network, vec![], true).is_err());
    }

    #[test]
    fn external_address_filter_covers_mapped_private_and_documentation() {
        for value in [
            "127.2.3.4",
            "10.0.0.1",
            "100.64.1.2",
            "169.254.2.3",
            "192.0.2.1",
            "198.18.0.1",
            "224.0.0.1",
            "240.0.0.1",
            "::",
            "::1",
            "fc00::1",
            "fe80::1",
            "ff02::1",
            "2001:db8::1",
            "::ffff:192.168.1.1",
        ] {
            assert!(!external_ip(value.parse().unwrap()), "{value}");
        }
        for value in ["8.8.8.8", "1.1.1.1", "2606:4700:4700::1111"] {
            assert!(external_ip(value.parse().unwrap()), "{value}");
        }
    }

    #[test]
    fn zcash_template_policy_never_reads_wcash_peer_book() {
        struct MustNotReadBook;
        impl AddressBookPeers for MustNotReadBook {
            fn recently_live_peers(&self, _: chrono::DateTime<Utc>) -> Vec<MetaAddr> {
                panic!("ordinary Zcash admission must not read Wcash peer evidence")
            }
            fn add_peer(&mut self, _: PeerSocketAddr) -> bool {
                panic!("template readiness never adds peers")
            }
        }
        let (tip, sender) = MockChainTip::new();
        sender.send_best_tip_height(Height(3527900));
        sender.send_best_tip_hash(block::Hash([34; 32]));
        sender.send_estimated_distance_to_network_chain_tip(Some(0));
        let mut sync = MockSyncStatus::default();
        sync.set_is_close_to_tip(true);
        assert!(check_synced_to_tip_for_template(
            &Network::Mainnet,
            tip.clone(),
            sync.clone(),
            &MustNotReadBook
        )
        .is_ok());
        sender.send_estimated_distance_to_network_chain_tip(Some(288));
        assert!(
            check_synced_to_tip_for_template(&Network::Mainnet, tip, sync, &MustNotReadBook)
                .is_err()
        );
    }
}
