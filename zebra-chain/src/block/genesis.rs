//! Frozen Zcash and Wcash genesis blocks.

use std::sync::Arc;

use hex::FromHex;

use crate::{
    block::Block,
    serialization::ZcashDeserializeInto,
    transparent::{self, Input},
    work::difficulty::CompactDifficulty,
    work::equihash::{Solution, WCASH_BLOCK_WIRE_VERSION},
};

/// Frozen display-order block ID of the public Wcash Testnet genesis block.
pub const WCASH_TESTNET_GENESIS_HASH: &str =
    "d95a9f2f1daf07d48fb3c863ad7334ec630a4a7077da98c8f7e65f8c0e277cf1";

/// Genesis block for Regtest, copied from zcashd via `getblock 0 0` RPC method
pub fn regtest_genesis_block() -> Arc<Block> {
    let regtest_genesis_block_bytes =
        <Vec<u8>>::from_hex(include_str!("genesis/block-regtest-0-000-000.txt").trim())
            .expect("Block bytes are in valid hex representation");

    regtest_genesis_block_bytes
        .zcash_deserialize_into()
        .map(Arc::new)
        .expect("hard-coded Regtest genesis block data must deserialize successfully")
}

/// Returns the deterministic local Wcash Regtest genesis block.
///
/// Its coinbase text and header commitment bind the local chain to the frozen
/// Bitcoin block in [`wcash_genesis::REGTEST_ANCHOR`]. Public Wcash Testnet
/// uses its own separately reviewed and frozen Bitcoin anchor; Wcash mainnet
/// remains disabled.
pub fn wcash_regtest_genesis_block() -> Arc<Block> {
    wcash_genesis_block(
        wcash_genesis::REGTEST_ANCHOR,
        wcash_genesis::LOCAL_REGTEST_BITCOIN_TIME,
        transparent::WCASH_REGTEST_GENESIS_COINBASE_SCRIPT_SIG,
        None,
    )
}

/// Returns the frozen public Wcash Testnet genesis block.
///
/// The complete block is deterministically derived from immutable fields and
/// checked against a byte-for-byte source vector in this module's tests. Its
/// Bitcoin anchor is testnet-specific, and mainnet remains disabled.
pub fn wcash_testnet_genesis_block() -> Arc<Block> {
    let bytes = <Vec<u8>>::from_hex(include_str!("genesis/block-wcash-testnet-0.txt").trim())
        .expect("the frozen Wcash Testnet genesis vector is valid hex");
    bytes
        .zcash_deserialize_into()
        .map(Arc::new)
        .expect("the frozen Wcash Testnet genesis block must deserialize")
}

#[cfg(test)]
fn independently_constructed_wcash_testnet_genesis_block() -> Arc<Block> {
    let difficulty_threshold = CompactDifficulty::from_bytes_in_display_order(
        &wcash_genesis::PUBLIC_TESTNET_POW_LIMIT_BITS.to_be_bytes(),
    )
    .expect("the frozen Wcash Testnet proof-of-work limit is canonical");

    wcash_genesis_block(
        wcash_genesis::TESTNET_ANCHOR,
        wcash_genesis::PUBLIC_TESTNET_BITCOIN_TIME,
        transparent::WCASH_TESTNET_GENESIS_COINBASE_SCRIPT_SIG,
        Some(difficulty_threshold),
    )
}

fn wcash_genesis_block(
    anchor: wcash_genesis::BitcoinAnchor,
    bitcoin_time: u32,
    coinbase_script: &[u8],
    difficulty_threshold: Option<CompactDifficulty>,
) -> Arc<Block> {
    let mut block = regtest_genesis_block().as_ref().clone();
    let coinbase = Arc::make_mut(
        block
            .transactions
            .first_mut()
            .expect("the inherited Regtest genesis block has a coinbase"),
    );
    assert_eq!(
        coinbase.version(),
        1,
        "the inherited Regtest genesis coinbase uses transaction version 1"
    );
    let mut inputs = coinbase.inputs();
    let input = inputs
        .first_mut()
        .expect("the inherited Regtest genesis coinbase has an input");
    let Input::Coinbase { height, .. } = input else {
        panic!("the inherited Regtest genesis input is a coinbase")
    };
    assert!(height.is_min(), "the genesis coinbase height is zero");
    *coinbase = coinbase
        .clone()
        .with_coinbase_script(coinbase_script.to_vec())
        .expect("the inherited Regtest genesis transaction is a single-input coinbase");

    let merkle_root = block.transactions.iter().map(|tx| tx.hash()).collect();
    let header = Arc::make_mut(&mut block.header);
    header.version = WCASH_BLOCK_WIRE_VERSION;
    header.merkle_root = merkle_root;
    header.commitment_bytes = anchor.commitment().into();
    header.time = chrono::DateTime::from_timestamp(i64::from(bitcoin_time), 0)
        .expect("the Wcash genesis timestamp is representable");
    if let Some(difficulty_threshold) = difficulty_threshold {
        header.difficulty_threshold = difficulty_threshold;
    }
    header.nonce = [0; 32].into();
    header.solution = Solution::for_wcash(Vec::new())
        .expect("an empty Wcash genesis witness is within the size limit");

    Arc::new(block)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::{
        amount::{DeferredPoolBalanceChange, NegativeAllowed},
        serialization::{ZcashDeserializeInto, ZcashSerialize},
        value_balance::ValueBalance,
    };

    use super::*;

    #[test]
    fn wcash_regtest_genesis_is_deterministic_and_round_trips() {
        let block = wcash_regtest_genesis_block();
        assert_eq!(block.coinbase_height(), Some(crate::block::Height::MIN));
        assert_eq!(block.header.version, WCASH_BLOCK_WIRE_VERSION);
        assert_eq!(
            &block.header.commitment_bytes[..],
            &wcash_genesis::REGTEST_ANCHOR.commitment()
        );
        assert_eq!(
            block.transactions[0].inputs()[0]
                .miner_data()
                .expect("genesis input is a coinbase"),
            wcash_genesis::REGTEST_ANCHOR.genesis_statement().as_bytes()
        );
        assert!(block
            .header
            .solution
            .as_wcash()
            .expect("Wcash genesis has a Wcash solution variant")
            .is_empty());
        assert_eq!(
            block.hash().to_string(),
            "70bf0bab17eff361a6331bb825b3b7253c8c96ff96407f948161d2912658bb1c",
            "the local Wcash genesis ID is a frozen interoperability vector",
        );

        let bytes = block
            .zcash_serialize_to_vec()
            .expect("Wcash genesis serializes");
        let round_trip: Block = bytes
            .zcash_deserialize_into()
            .expect("Wcash genesis deserializes");
        assert_eq!(round_trip, *block);
        assert_eq!(round_trip.hash(), block.hash());
    }

    #[test]
    fn wcash_testnet_genesis_is_deterministic_and_round_trips() {
        let block = wcash_testnet_genesis_block();
        assert_eq!(block.coinbase_height(), Some(crate::block::Height::MIN));
        assert_eq!(block.header.version, WCASH_BLOCK_WIRE_VERSION);
        assert_eq!(
            &block.header.commitment_bytes[..],
            &wcash_genesis::TESTNET_ANCHOR.commitment()
        );
        assert_eq!(
            block.transactions[0].inputs()[0]
                .miner_data()
                .expect("genesis input is a coinbase"),
            wcash_genesis::TESTNET_ANCHOR.genesis_statement().as_bytes()
        );
        assert_eq!(
            block.header.difficulty_threshold.to_string(),
            format!("{:08x}", wcash_genesis::PUBLIC_TESTNET_POW_LIMIT_BITS)
        );
        assert!(block
            .header
            .solution
            .as_wcash()
            .expect("Wcash genesis has a Wcash solution variant")
            .is_empty());
        let genesis_transaction = &block.transactions[0];
        let genesis_outputs = genesis_transaction.outputs();
        assert_eq!(genesis_outputs.len(), 1);
        assert_eq!(
            genesis_outputs[0].value.zatoshis(),
            0,
            "the inherited genesis output must create no spendable supply"
        );
        assert_eq!(
            block
                .chain_value_pool_change(&HashMap::new(), DeferredPoolBalanceChange::zero())
                .expect("the genesis value-pool change is defined"),
            ValueBalance::<NegativeAllowed>::zero(),
            "state and RPC supply accounting must begin at exactly zero"
        );

        let bytes = block
            .zcash_serialize_to_vec()
            .expect("Wcash genesis serializes");
        assert_eq!(
            block.hash().to_string(),
            WCASH_TESTNET_GENESIS_HASH,
            "the public Wcash Testnet genesis ID is a frozen interoperability vector",
        );
        let frozen_bytes =
            <Vec<u8>>::from_hex(include_str!("genesis/block-wcash-testnet-0.txt").trim())
                .expect("the frozen Wcash Testnet genesis vector is valid hex");
        assert_eq!(
            bytes, frozen_bytes,
            "the constructed Wcash Testnet genesis must match its complete frozen vector"
        );
        assert_eq!(
            *block,
            *independently_constructed_wcash_testnet_genesis_block(),
            "independent deterministic construction must reproduce the decoded frozen vector"
        );
        let round_trip: Block = bytes
            .zcash_deserialize_into()
            .expect("Wcash genesis deserializes");
        assert_eq!(round_trip, *block);
        assert_eq!(round_trip.hash(), block.hash());
    }

    #[test]
    fn wcash_child_header_ids_commit_to_the_selected_network_lineage() {
        let testnet_genesis = wcash_testnet_genesis_block();
        let regtest_genesis = wcash_regtest_genesis_block();

        let mut testnet_child = testnet_genesis.as_ref().clone();
        Arc::make_mut(&mut testnet_child.header).previous_block_hash = testnet_genesis.hash();
        let mut regtest_child = testnet_child.clone();
        Arc::make_mut(&mut regtest_child.header).previous_block_hash = regtest_genesis.hash();

        assert_ne!(testnet_genesis.hash(), regtest_genesis.hash());
        assert_ne!(testnet_child.hash(), regtest_child.hash());
    }
}
