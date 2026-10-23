//! Fixed test vectors for the precomputed block template cache.

use std::time::Duration;

use zebra_chain::{
    block,
    parameters::{Network, NetworkUpgrade},
    serialization::DateTime32,
    work::difficulty::{CompactDifficulty, ExpandedDifficulty, U256},
};
use zebra_state::GetBlockTemplateChainInfo;

use crate::{
    config::mining::{default_miner_address, Config, MinerAddressType},
    methods::tests::utils::fake_history_tree,
    methods::types::get_block_template::WcashAuxRequest,
};

use super::*;

/// Returns a template to publish. Its contents don't matter here, only that publishing it is a
/// change.
fn template() -> BlockTemplateResponse {
    let net = Network::Mainnet;
    let tip_height = NetworkUpgrade::Nu5
        .activation_height(&net)
        .expect("Nu5 is active on Mainnet");

    let miner_params = MinerParams::new(
        &net,
        Config {
            miner_address: Some(
                default_miner_address(net.kind(), &MinerAddressType::Transparent)
                    .parse()
                    .expect("hard-coded transparent address is valid"),
            ),
            ..Default::default()
        },
    )
    .expect("the miner parameters are valid");

    let chain_info = GetBlockTemplateChainInfo {
        expected_difficulty: CompactDifficulty::from(ExpandedDifficulty::from(U256::one())),
        tip_height,
        tip_hash: block::Hash([0xab; 32]),
        cur_time: DateTime32::from(1654008617),
        min_time: DateTime32::from(1654008606),
        max_time: DateTime32::from(1654008719),
        chain_history_root: fake_history_tree(&net).hash(),
    };

    let long_poll_id = LongPollInput::new(
        chain_info.tip_height,
        chain_info.tip_hash,
        chain_info.max_time,
        std::iter::empty(),
    )
    .generate_id();

    BlockTemplateResponse::new_internal(
        &net,
        &CoinbaseCache::default(),
        &miner_params,
        None,
        &chain_info,
        long_poll_id,
        vec![],
        None,
    )
    .expect("the test template is valid")
}

/// Child commitments are applied to clones of the cached parent template. This
/// keeps ordinary Zcash work child-independent and prevents one Wcash job from
/// leaking its commitment into the next job.
#[test]
fn wcash_aux_transforms_are_isolated_from_the_cached_template() {
    let net = Network::Mainnet;
    let miner_params = MinerParams::new(
        &net,
        Config {
            miner_address: Some(
                default_miner_address(net.kind(), &MinerAddressType::Transparent)
                    .parse()
                    .expect("hard-coded transparent address is valid"),
            ),
            ..Default::default()
        },
    )
    .expect("the miner parameters are valid");
    let cached = template();

    let first = cached
        .clone()
        .with_wcash_aux(
            &net,
            &miner_params,
            WcashAuxRequest::new(block::Hash([0x11; 32]), 1),
        )
        .expect("the first child commitment is valid");
    let second = cached
        .clone()
        .with_wcash_aux(
            &net,
            &miner_params,
            WcashAuxRequest::new(block::Hash([0x22; 32]), 2),
        )
        .expect("the second child commitment is valid");

    assert!(cached.wcash_parent_payout_commitment.is_none());
    assert!(first.wcash_parent_payout_commitment.is_some());
    assert!(second.wcash_parent_payout_commitment.is_some());

    assert_eq!(cached.coinbase_txn.hash(), first.coinbase_txn.hash());
    assert_eq!(cached.coinbase_txn.hash(), second.coinbase_txn.hash());
    assert_ne!(
        cached.coinbase_txn.auth_digest(),
        first.coinbase_txn.auth_digest()
    );
    assert_ne!(
        first.coinbase_txn.auth_digest(),
        second.coinbase_txn.auth_digest()
    );

    assert_eq!(
        cached.default_roots.merkle_root(),
        first.default_roots.merkle_root(),
        "ZIP-244 keeps the transaction ID Merkle root stable",
    );
    assert_ne!(
        cached.default_roots.auth_data_root(),
        first.default_roots.auth_data_root(),
    );
    assert_ne!(
        first.default_roots.auth_data_root(),
        second.default_roots.auth_data_root(),
    );
    assert_ne!(first.block_commitments_hash, second.block_commitments_hash);

    assert_eq!(
        cached,
        template(),
        "transforming clones must not mutate or contaminate the cached template",
    );
}

/// Rapid consecutive tips must consume a reserve of distinct height-specific coinbases instead
/// of waiting for a new private proof after every block.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn coinbase_lookahead_rolls_forward_without_duplicate_heights() {
    let _init_guard = zebra_test::init();

    let network = Network::Mainnet;
    let miner_params = MinerParams::new(
        &network,
        Config {
            miner_address: Some(
                default_miner_address(network.kind(), &MinerAddressType::Transparent)
                    .parse()
                    .expect("hard-coded transparent address is valid"),
            ),
            ..Default::default()
        },
    )
    .expect("the miner parameters are valid");
    let current_template_height = block::Height(1_000_000);
    let expected = coinbase_lookahead_heights(current_template_height);
    assert_eq!(expected.len(), COINBASE_LOOKAHEAD_DEPTH);

    let mut future_coinbases = CoinbasePrecomputations::new();
    start_precomputing_coinbases(
        &mut future_coinbases,
        &network,
        &miner_params,
        current_template_height,
    );
    assert_eq!(
        future_coinbases.tasks.keys().copied().collect::<Vec<_>>(),
        expected
    );

    // Refill attempts for the same tip must retain the existing singleflight tasks.
    start_precomputing_coinbases(
        &mut future_coinbases,
        &network,
        &miner_params,
        current_template_height,
    );
    assert_eq!(future_coinbases.tasks.len(), COINBASE_LOOKAHEAD_DEPTH);

    let coinbase_cache = CoinbaseCache::default();
    tokio::time::timeout(Duration::from_secs(10), async {
        while !future_coinbases.tasks.values().all(JoinHandle::is_finished) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the initial lookahead reserve must finish");

    // Consume the entire initial reserve as a rapid four-block burst. Refilling after each tip
    // must preserve every still-needed completed proof and add exactly one farther future height.
    for next_height in expected {
        assert!(
            future_coinbases
                .tasks
                .get(&next_height)
                .is_some_and(JoinHandle::is_finished),
            "each proof in the warmed burst reserve must already be complete",
        );
        store_precomputed_coinbase(&mut future_coinbases, next_height, &coinbase_cache).await;
        assert!(
            coinbase_cache.get(next_height, Amount::zero()).is_some(),
            "the consumed proof must enter the shared coinbase cache",
        );

        start_precomputing_coinbases(&mut future_coinbases, &network, &miner_params, next_height);
        assert_eq!(
            future_coinbases.tasks.keys().copied().collect::<Vec<_>>(),
            coinbase_lookahead_heights(next_height),
            "advancing one height must preserve the remaining reserve and add one future proof",
        );
    }

    // Do not leave blocking tasks detached from this test's runtime.
    while let Some((_, task)) = future_coinbases.tasks.pop_first() {
        task.await.expect("future coinbase proof task must finish");
    }
}

/// Checks that a subscription taken before a template is published still reports it.
///
/// `getblocktemplate` reads the cache, decides the client already has that template, and only then
/// waits. A subscription taken at the point of waiting would mark anything published in between as
/// seen, and the caller's other wake conditions are a chain tip change and `max_time`, neither of
/// which fires when the mempool alone changes: it would sit on a template it had already been told
/// about until the updater's backstop.
#[tokio::test]
async fn a_subscription_reports_a_template_published_before_the_wait() {
    let _init_guard = zebra_test::init();

    let cache = TemplateCache::default();

    // The order the RPC uses: subscribe, read, then wait.
    let mut changes = cache.subscribe();
    assert!(cache.is_empty(), "nothing is published yet");

    cache.publish(template());

    tokio::time::timeout(Duration::from_secs(10), changes.changed())
        .await
        .expect("a template published before the wait should still end it");
}

/// Checks that a subscription reports each later publish, so a long poll that loops keeps waiting
/// on templates it hasn't seen rather than on the one it just read.
#[tokio::test]
async fn a_subscription_reports_each_later_publish() {
    let _init_guard = zebra_test::init();

    let cache = TemplateCache::default();
    let mut changes = cache.subscribe();

    for _ in 0..3 {
        cache.publish(template());

        tokio::time::timeout(Duration::from_secs(10), changes.changed())
            .await
            .expect("each publish should end a wait");
    }

    // With nothing published since, the next wait doesn't return.
    assert!(
        tokio::time::timeout(Duration::from_millis(100), changes.changed())
            .await
            .is_err(),
        "a subscription that has seen every publish should keep waiting",
    );
}

/// Exercise the private cached-return path so initial RPC admission cannot hide
/// a missing late readiness check. No updater, proof solver or network is run.
#[tokio::test(flavor = "multi_thread")]
async fn quiet_wcash_cached_return_rechecks_peers_and_actual_sync() {
    use crate::{config::mining::default_miner_address_for_network, methods::RpcImpl};
    use tower::buffer::Buffer;
    use zebra_chain::{chain_sync_status::MockSyncStatus, chain_tip::mock::MockChainTip};
    use zebra_network::{
        address_book_peers::MockAddressBookPeers,
        types::{MetaAddr, PeerServices},
        Version,
    };
    use zebra_node_services::{mempool, BoxError};
    use zebra_state::{ReadRequest, ReadResponse};
    use zebra_test::mock_service::MockService;

    let _init_guard = zebra_test::init();
    let network = Network::new_wcash_mainnet_for_tests();
    let tip_height = Height(38628);
    let tip_hash = block::Hash([17; 32]);
    let now = chrono::Utc::now();
    let peers = || {
        MockAddressBookPeers::new(
            ["8.8.8.8:48233", "1.1.1.1:48233"]
                .into_iter()
                .map(|addr| {
                    MetaAddr::new_connected(
                        addr.parse().unwrap(),
                        &PeerServices::NODE_NETWORK,
                        false,
                        "/quiet-cache-fixture/".to_owned(),
                        Version(170160),
                    )
                    .into_new_meta_addr(std::time::Instant::now(), now.try_into().unwrap())
                })
                .collect(),
        )
    };
    let configuration = Config {
        miner_address: Some(
            default_miner_address_for_network(&network, &MinerAddressType::Transparent)
                .unwrap()
                .parse()
                .unwrap(),
        ),
        ..Default::default()
    };
    let miner_params = MinerParams::new(&network, configuration.clone()).unwrap();
    let chain_info = GetBlockTemplateChainInfo {
        expected_difficulty: CompactDifficulty::from(ExpandedDifficulty::from(U256::one())),
        tip_height,
        tip_hash,
        cur_time: DateTime32::from(1792764000),
        min_time: DateTime32::from(1792763999),
        max_time: DateTime32::from(1792764060),
        chain_history_root: Some([23; 32].into()),
    };
    let long_poll_id = LongPollInput::new(
        tip_height,
        tip_hash,
        chain_info.max_time,
        std::iter::empty(),
    )
    .generate_id();
    let template = BlockTemplateResponse::new_internal(
        &network,
        &CoinbaseCache::default(),
        &miner_params,
        None,
        &chain_info,
        long_poll_id,
        vec![],
        None,
    )
    .unwrap();

    let (tip, tip_sender) = MockChainTip::new();
    tip_sender.send_best_tip_height(tip_height);
    tip_sender.send_best_tip_hash(tip_hash);
    tip_sender.send_best_tip_block_time(now - chrono::Duration::hours(6));
    tip_sender.send_estimated_distance_to_network_chain_tip(Some(288));
    let mut sync = MockSyncStatus::default();
    sync.set_is_close_to_tip(true);
    let mempool: MockService<mempool::Request, mempool::Response, _, BoxError> =
        MockService::build().for_unit_tests();
    let state: MockService<_, _, _, BoxError> = MockService::build().for_unit_tests();
    let mut read_state: MockService<_, _, _, BoxError> = MockService::build().for_unit_tests();
    let (_log_sender, log_receiver) = tokio::sync::watch::channel(None);
    let (mut rpc, queue) = RpcImpl::new(
        network,
        configuration,
        false,
        "fixture",
        "RPC test",
        Buffer::new(mempool, 1),
        state,
        Buffer::new(read_state.clone(), 1),
        MockService::build().for_unit_tests(),
        sync.clone(),
        tip,
        peers(),
        log_receiver,
        None,
    );
    rpc.gbt.template_cache().unwrap().publish(template);
    let responder = tokio::spawn(async move {
        for _ in 0..3 {
            read_state
                .expect_request(ReadRequest::Tip)
                .await
                .respond(ReadResponse::Tip(Some((tip_height, tip_hash))));
        }
    });

    let first = rpc
        .precomputed_block_template(None, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.previous_block_hash, tip_hash);
    rpc.address_book = MockAddressBookPeers::default();
    assert_eq!(
        rpc.precomputed_block_template(None, None)
            .await
            .unwrap_err()
            .code(),
        -10
    );
    rpc.address_book = peers();
    sync.set_is_close_to_tip(false);
    assert_eq!(
        rpc.precomputed_block_template(None, None)
            .await
            .unwrap_err()
            .code(),
        -10
    );
    responder.await.unwrap();
    queue.abort();
}
