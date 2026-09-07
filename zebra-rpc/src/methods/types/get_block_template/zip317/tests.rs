//! Tests for ZIP-317 transaction selection for block template production

#![allow(clippy::unwrap_in_result)]

use wcash_zcash_aux::MINER_DATA_COMMITMENT_BYTES;
use zcash_keys::address::Address;
use zcash_transparent::address::TransparentAddress;

use zebra_chain::{
    amount::Amount,
    block::{Header, Height, MAX_BLOCK_BYTES},
    parameters::Network,
    transaction,
    transparent::OutPoint,
};
use zebra_consensus::MAX_BLOCK_SIGOPS;
use zebra_node_services::mempool::TransactionDependencies;

use crate::methods::types::{
    get_block_template::{MinerParams, WcashAuxRequest},
    transaction::TransactionTemplate,
};

use super::{max_transaction_count_size, select_mempool_transactions, CoinbaseCache};

#[test]
fn excludes_tx_with_unselected_dependencies() {
    let network = Network::Mainnet;
    let mut mempool_tx_deps = TransactionDependencies::default();

    let unmined_tx = network
        .unmined_transactions_in_blocks(..)
        .next()
        .expect("should not be empty");

    mempool_tx_deps.add(
        unmined_tx.transaction.id.mined_id(),
        vec![OutPoint::from_usize(transaction::Hash([0; 32]), 0)],
    );

    assert_eq!(
        select_mempool_transactions(
            &network,
            Height(1_000_000),
            &MinerParams::from(Address::from(TransparentAddress::PublicKeyHash([0x7e; 20]))),
            vec![unmined_tx],
            mempool_tx_deps,
            None,
            None,
        )
        .expect("ordinary transaction selection succeeds"),
        vec![],
        "should not select any transactions when dependencies are unavailable"
    );
}

#[test]
fn includes_tx_with_selected_dependencies() {
    let network = Network::Mainnet;
    let unmined_txs: Vec<_> = network.unmined_transactions_in_blocks(..).take(3).collect();

    let dependent_tx1 = unmined_txs.first().expect("should have 3 txns");
    let dependent_tx2 = unmined_txs.get(1).expect("should have 3 txns");
    let independent_tx_id = unmined_txs
        .get(2)
        .expect("should have 3 txns")
        .transaction
        .id
        .mined_id();

    let mut mempool_tx_deps = TransactionDependencies::default();
    mempool_tx_deps.add(
        dependent_tx1.transaction.id.mined_id(),
        vec![OutPoint::from_usize(independent_tx_id, 0)],
    );
    mempool_tx_deps.add(
        dependent_tx2.transaction.id.mined_id(),
        vec![
            OutPoint::from_usize(independent_tx_id, 0),
            OutPoint::from_usize(transaction::Hash([0; 32]), 0),
        ],
    );

    let selected_txs = select_mempool_transactions(
        &network,
        Height(1_000_000),
        &MinerParams::from(Address::from(TransparentAddress::PublicKeyHash([0x7e; 20]))),
        unmined_txs.clone(),
        mempool_tx_deps.clone(),
        None,
        None,
    )
    .expect("ordinary transaction selection succeeds");

    assert_eq!(
        selected_txs.len(),
        2,
        "should select the independent transaction and 1 of the dependent txs, selected: {selected_txs:?}"
    );

    let selected_tx_by_id = |id| {
        selected_txs
            .iter()
            .find(|(_, tx)| tx.transaction.id.mined_id() == id)
    };

    let (dependency_depth, _) =
        selected_tx_by_id(independent_tx_id).expect("should select the independent tx");

    assert_eq!(
        *dependency_depth, 0,
        "should return a dependency depth of 0 for the independent tx"
    );

    let (dependency_depth, _) = selected_tx_by_id(dependent_tx1.transaction.id.mined_id())
        .expect("should select dependent_tx1");

    assert_eq!(
        *dependency_depth, 1,
        "should return a dependency depth of 1 for the dependent tx"
    );
}

/// Checks that transaction selection reserves space for the block header and the transaction
/// count, which [`MAX_BLOCK_BYTES`] covers: a transaction exactly filling the remaining safe
/// budget is selected, and a transaction one byte larger is not (GHSA-95m2-vx53-v2jw).
#[test]
fn reserves_space_for_block_header_and_transaction_count() {
    let network = Network::Mainnet;
    let height = Height(1_000_000);
    let miner_params =
        MinerParams::from(Address::from(TransparentAddress::PublicKeyHash([0x7e; 20])));

    let coinbase_tx_size =
        TransactionTemplate::new_coinbase(&network, height, &miner_params, Amount::zero())
            .expect("valid coinbase transaction template")
            .data
            .as_ref()
            .len();

    let safe_budget = usize::try_from(MAX_BLOCK_BYTES).expect("fits in memory")
        - Header::serialized_size(&network)
        - max_transaction_count_size()
        - coinbase_tx_size;

    let mut unmined_tx = network
        .unmined_transactions_in_blocks(..)
        .next()
        .expect("should not be empty");

    unmined_tx.transaction.size = safe_budget;

    assert_eq!(
        select_mempool_transactions(
            &network,
            height,
            &miner_params,
            vec![unmined_tx.clone()],
            TransactionDependencies::default(),
            None,
            None,
        )
        .expect("ordinary transaction selection succeeds")
        .len(),
        1,
        "should select a transaction exactly filling the safe block budget"
    );

    unmined_tx.transaction.size = safe_budget + 1;

    assert_eq!(
        select_mempool_transactions(
            &network,
            height,
            &miner_params,
            vec![unmined_tx],
            TransactionDependencies::default(),
            None,
            None,
        )
        .expect("ordinary transaction selection succeeds"),
        vec![],
        "should not select a transaction one byte over the safe block budget"
    );
}

/// A Wcash parent-template request must charge its exact child-specific
/// coinbase mutation before ZIP-317 selects transactions. Otherwise a template
/// near either consensus limit can become invalid after the 44-byte carrier is
/// appended.
#[test]
fn wcash_aux_reserves_exact_coinbase_bytes_and_sigops() {
    let network = Network::Mainnet;
    let height = Height(2_000_000);
    let miner_params =
        MinerParams::from(Address::from(TransparentAddress::PublicKeyHash([0x7e; 20])));
    let ordinary_coinbase =
        TransactionTemplate::new_coinbase(&network, height, &miner_params, Amount::zero())
            .expect("valid ZIP-244 coinbase transaction template");
    let coinbase_cache = CoinbaseCache::default();
    coinbase_cache.store(height, Amount::zero(), ordinary_coinbase.clone());
    let mut block_hash = [0x42; 32];
    block_hash[..4].copy_from_slice(&7u32.to_le_bytes());
    let wcash_aux = WcashAuxRequest::new(zebra_chain::block::Hash(block_hash), 7);
    let committed_coinbase = ordinary_coinbase
        .clone()
        .with_wcash_aux(wcash_aux.block_hash().0, wcash_aux.nonce())
        .expect("the exact Wcash carrier fits in the fake coinbase");
    assert_eq!(
        committed_coinbase.data.as_ref().len(),
        ordinary_coinbase.data.as_ref().len() + MINER_DATA_COMMITMENT_BYTES,
        "the selection budget must reserve the frozen raw 44-byte suffix"
    );
    assert!(
        committed_coinbase.sigops > ordinary_coinbase.sigops,
        "the test vector must expose child-specific coinbase sigops"
    );

    let safe_byte_budget = usize::try_from(MAX_BLOCK_BYTES).expect("fits in memory")
        - Header::serialized_size(&network)
        - max_transaction_count_size()
        - committed_coinbase.data.as_ref().len();
    let mut byte_limited_tx = network
        .unmined_transactions_in_blocks(..)
        .next()
        .expect("should not be empty");
    byte_limited_tx.transaction.size = safe_byte_budget;
    assert_eq!(
        select_mempool_transactions(
            &network,
            height,
            &miner_params,
            vec![byte_limited_tx.clone()],
            TransactionDependencies::default(),
            Some(&coinbase_cache),
            Some(wcash_aux),
        )
        .expect("Wcash transaction selection succeeds")
        .len(),
        1,
        "a transaction exactly filling the Wcash-aware byte budget is selected"
    );
    byte_limited_tx.transaction.size = safe_byte_budget + 1;
    assert_eq!(
        select_mempool_transactions(
            &network,
            height,
            &miner_params,
            vec![byte_limited_tx],
            TransactionDependencies::default(),
            Some(&coinbase_cache),
            Some(wcash_aux),
        )
        .expect("Wcash transaction selection succeeds"),
        vec![],
        "one byte beyond the Wcash-aware budget is rejected"
    );

    let mut sigop_limited_tx = network
        .unmined_transactions_in_blocks(..)
        .next()
        .expect("should not be empty");
    sigop_limited_tx.legacy_sigop_count = MAX_BLOCK_SIGOPS - committed_coinbase.sigops;
    sigop_limited_tx.p2sh_sigop_count = 0;
    assert_eq!(
        select_mempool_transactions(
            &network,
            height,
            &miner_params,
            vec![sigop_limited_tx.clone()],
            TransactionDependencies::default(),
            Some(&coinbase_cache),
            Some(wcash_aux),
        )
        .expect("Wcash transaction selection succeeds")
        .len(),
        1,
        "a transaction exactly filling the Wcash-aware sigop budget is selected"
    );
    sigop_limited_tx.legacy_sigop_count += 1;
    assert_eq!(
        select_mempool_transactions(
            &network,
            height,
            &miner_params,
            vec![sigop_limited_tx],
            TransactionDependencies::default(),
            Some(&coinbase_cache),
            Some(wcash_aux),
        )
        .expect("Wcash transaction selection succeeds"),
        vec![],
        "one sigop beyond the Wcash-aware budget is rejected"
    );
    assert_eq!(
        coinbase_cache.get(height, Amount::zero()),
        Some(ordinary_coinbase),
        "child-specific selection must not contaminate the shared coinbase cache"
    );
}
