//! Zcash and Wcash Regtest genesis blocks.

use std::sync::Arc;

use hex::FromHex;

use crate::{
    block::Block,
    serialization::ZcashDeserializeInto,
    transparent::{self, Input},
    work::equihash::{Solution, WCASH_BLOCK_WIRE_VERSION},
};

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
/// Bitcoin block in [`wcash_genesis::REGTEST_ANCHOR`]. Public Wcash networks
/// use a separately reviewed Bitcoin anchor and remain disabled until it is
/// finalized.
pub fn wcash_regtest_genesis_block() -> Arc<Block> {
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
        .with_coinbase_script(transparent::WCASH_REGTEST_GENESIS_COINBASE_SCRIPT_SIG.to_vec())
        .expect("the inherited Regtest genesis transaction is a single-input coinbase");

    let merkle_root = block.transactions.iter().map(|tx| tx.hash()).collect();
    let header = Arc::make_mut(&mut block.header);
    header.version = WCASH_BLOCK_WIRE_VERSION;
    header.merkle_root = merkle_root;
    header.commitment_bytes = wcash_genesis::REGTEST_ANCHOR.commitment().into();
    header.time =
        chrono::DateTime::from_timestamp(i64::from(wcash_genesis::LOCAL_REGTEST_BITCOIN_TIME), 0)
            .expect("the Wcash genesis timestamp is representable");
    header.nonce = [0; 32].into();
    header.solution = Solution::for_wcash(Vec::new())
        .expect("an empty Wcash genesis witness is within the size limit");

    Arc::new(block)
}

#[cfg(test)]
mod tests {
    use crate::serialization::{ZcashDeserializeInto, ZcashSerialize};

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
            "b0ebe8618354e0563091d10b73ba03842cb3c112a801012616489269e58dbd61",
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
}
