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
