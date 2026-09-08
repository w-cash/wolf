//! Tests for Zebra blocks

#![allow(clippy::unwrap_in_result)]

// TODO: generate should be rewritten as strategies
#[cfg(any(test, feature = "bench", feature = "proptest-impl"))]
pub mod generate;
#[cfg(test)]
mod preallocate;
#[cfg(test)]
mod prop;
#[cfg(test)]
mod vectors;

#[test]
fn block_rejects_cross_chain_branch_ids_including_coinbase() {
    use std::sync::Arc;

    use crate::{
        block::{error::BlockError, Height},
        parameters::{Network, NetworkUpgrade},
        transaction::{LockTime, Transaction},
        transparent,
    };

    fn coinbase_input(height: Height) -> transparent::Input {
        transparent::Input::Coinbase {
            height,
            // Height 1 is a one-byte script item; one byte of inert miner data
            // keeps the complete script within the consensus 2..=100 bound.
            data: vec![0],
            sequence: u32::MAX,
        }
    }

    let wcash = Network::new_wcash_testnet();
    let wcash_height = Height(1);
    let header = crate::block::genesis::wcash_testnet_genesis_block()
        .header
        .clone();
    let wcash_coinbase = Transaction::test_v6_for_network(
        &wcash,
        wcash_height,
        vec![coinbase_input(wcash_height)],
        Vec::new(),
        LockTime::Height(Height::MIN),
        wcash_height,
    );
    let zcash_coinbase = Transaction::test_v6(
        NetworkUpgrade::Nu6_3,
        vec![coinbase_input(wcash_height)],
        Vec::new(),
        LockTime::Height(Height::MIN),
        wcash_height,
    );

    let valid_wcash = super::Block {
        header: header.clone(),
        transactions: vec![Arc::new(wcash_coinbase.clone())],
    };
    assert_eq!(
        valid_wcash.check_transaction_network_upgrade_consistency(&wcash),
        Ok(())
    );

    let zcash_on_wcash = super::Block {
        header: header.clone(),
        transactions: vec![Arc::new(zcash_coinbase)],
    };
    assert!(matches!(
        zcash_on_wcash.check_transaction_network_upgrade_consistency(&wcash),
        Err(BlockError::WrongTransactionConsensusBranchId)
    ));

    let mainnet = Network::Mainnet;
    let mainnet_height = NetworkUpgrade::Nu6_3
        .activation_height(&mainnet)
        .expect("NU6.3 is active on Zcash Mainnet");
    let wcash_on_zcash = super::Block {
        header,
        transactions: vec![Arc::new(Transaction::test_v6_for_network(
            &wcash,
            wcash_height,
            vec![coinbase_input(mainnet_height)],
            Vec::new(),
            LockTime::Height(Height::MIN),
            mainnet_height,
        ))],
    };
    assert!(matches!(
        wcash_on_zcash.check_transaction_network_upgrade_consistency(&mainnet),
        Err(BlockError::WrongTransactionConsensusBranchId)
    ));
}
