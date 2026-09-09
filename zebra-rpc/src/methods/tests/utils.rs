//! Utility functions for RPC method tests.

use std::sync::Arc;
use zebra_chain::{
    block::Block,
    history_tree::{HistoryTree, NonEmptyHistoryTree},
    parameters::Network,
    primitives::zcash_history::BlockCommitmentTreeRoots,
    sapling::tree::Root,
    serialization::ZcashDeserialize,
};

/// Returns a network compatible with the consensus profile selected for this test binary.
pub fn consensus_profile_network() -> Network {
    let wcash_regtest = Network::new_wcash_regtest();
    if wcash_regtest.is_compatible_with_compiled_consensus() {
        wcash_regtest
    } else {
        Network::Mainnet
    }
}

/// Returns a Regtest network compatible with the consensus profile selected
/// for this test binary.
pub fn consensus_profile_regtest_network() -> Network {
    let wcash_regtest = Network::new_wcash_regtest();
    if wcash_regtest.is_compatible_with_compiled_consensus() {
        wcash_regtest
    } else {
        Network::new_regtest(Default::default())
    }
}

/// Returns a continuous block fixture compatible with `network`.
///
/// Wcash currently freezes only genesis, so tests that require post-genesis
/// state must remain scoped to Zebra's historical Zcash fixture suite.
pub fn consensus_profile_blocks(network: &Network) -> Vec<Arc<Block>> {
    assert!(network.is_compatible_with_compiled_consensus());

    if network.uses_wcash_consensus() {
        vec![zebra_chain::block::genesis::wcash_regtest_genesis_block()]
    } else {
        zebra_test::vectors::CONTINUOUS_MAINNET_BLOCKS
            .values()
            .map(|block_bytes| {
                Arc::<Block>::zcash_deserialize(&block_bytes[..])
                    .expect("the frozen Mainnet block fixture is valid")
            })
            .collect()
    }
}

/// Returns `true` when frozen Zcash post-genesis fixtures match this binary's profile.
pub fn zcash_historical_fixtures_are_compatible() -> bool {
    Network::Mainnet.is_compatible_with_compiled_consensus()
}

/// Create a history tree with one single block for a network by using Zebra test vectors.
pub fn fake_history_tree(network: &Network) -> Arc<HistoryTree> {
    let (block, sapling_root) = network.test_block_sapling_roots(1046400, 1116000).unwrap();

    let block = Arc::<Block>::zcash_deserialize(block).expect("block should deserialize");
    let first_sapling_root = Root::try_from(sapling_root).unwrap();

    let history_tree = NonEmptyHistoryTree::from_block(
        &Network::Mainnet,
        block,
        BlockCommitmentTreeRoots {
            sapling: &first_sapling_root,
            orchard: &Default::default(),
            ironwood: &Default::default(),
        },
    )
    .unwrap();

    Arc::new(HistoryTree::from(history_tree))
}
