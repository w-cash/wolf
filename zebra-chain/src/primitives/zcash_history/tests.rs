//! Tests for Zebra history trees

#![allow(clippy::unwrap_in_result)]

#[cfg(test)]
mod vectors;

#[test]
fn wcash_v3_history_tree_uses_exact_transaction_domain_and_rebuilds() {
    use std::sync::Arc;

    use crate::{
        block::{Block, Height},
        history_tree::NonEmptyHistoryTree,
        orchard,
        parameters::{testnet::ConfiguredActivationHeights, Network, NetworkUpgrade},
        primitives::zcash_history::BlockCommitmentTreeRoots,
        sapling,
        transaction::{LockTime, Transaction},
        transparent,
    };

    fn coinbase_input(height: Height) -> transparent::Input {
        transparent::Input::Coinbase {
            height,
            data: Vec::new(),
            sequence: u32::MAX,
        }
    }

    fn block(network: &Network, height: Height) -> Arc<Block> {
        Arc::new(Block {
            header: crate::block::genesis::wcash_testnet_genesis_block()
                .header
                .clone(),
            transactions: vec![Arc::new(Transaction::test_v6_for_network(
                network,
                height,
                vec![coinbase_input(height)],
                Vec::new(),
                LockTime::unlocked(),
                height,
            ))],
        })
    }

    let wcash = Network::new_wcash_testnet();
    let zcash = Network::new_regtest(
        ConfiguredActivationHeights {
            nu6_3: Some(1),
            ..Default::default()
        }
        .into(),
    );
    assert!(!zcash.uses_wcash_consensus());
    assert_eq!(
        NetworkUpgrade::current(&zcash, Height(1)),
        NetworkUpgrade::Nu6_3
    );

    let sapling_root = sapling::tree::Root::default();
    let orchard_root = orchard::tree::Root::default();
    let ironwood_root = orchard::tree::Root::default();
    let roots = BlockCommitmentTreeRoots {
        sapling: &sapling_root,
        orchard: &orchard_root,
        ironwood: &ironwood_root,
    };

    let mut wcash_tree = NonEmptyHistoryTree::from_block(&wcash, block(&wcash, Height(1)), roots)
        .expect("a Wcash V3 history leaf is valid");
    let mut zcash_tree = NonEmptyHistoryTree::from_block(&zcash, block(&zcash, Height(1)), roots)
        .expect("a Zcash-domain V3 history leaf is valid");
    assert_ne!(
        wcash_tree.hash(),
        zcash_tree.hash(),
        "the exact transaction branch ID must domain-separate history roots",
    );

    wcash_tree
        .push(block(&wcash, Height(2)), roots)
        .expect("the second Wcash leaf appends");
    zcash_tree
        .push(block(&zcash, Height(2)), roots)
        .expect("the second Zcash-domain leaf appends");
    assert_eq!(wcash_tree.size(), 3);
    assert_eq!(wcash_tree.peaks().len(), 1, "the tree prunes to one peak");
    assert_ne!(wcash_tree.hash(), zcash_tree.hash());

    let rebuilt_wcash = NonEmptyHistoryTree::from_cache(
        &wcash,
        wcash_tree.size(),
        wcash_tree.peaks().clone(),
        wcash_tree.current_height(),
    )
    .expect("the Wcash-domain history cache rebuilds");
    let rebuilt_zcash = NonEmptyHistoryTree::from_cache(
        &zcash,
        zcash_tree.size(),
        zcash_tree.peaks().clone(),
        zcash_tree.current_height(),
    )
    .expect("the standard Zcash-domain history cache rebuilds");
    assert_eq!(rebuilt_wcash.hash(), wcash_tree.hash());
    assert_eq!(rebuilt_zcash.hash(), zcash_tree.hash());
    assert_ne!(rebuilt_wcash.hash(), rebuilt_zcash.hash());
}
