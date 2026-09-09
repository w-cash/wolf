//! Tests for block verification

#![allow(clippy::unwrap_in_result)]

use color_eyre::eyre::{eyre, Report};
use once_cell::sync::Lazy;
use tower::{buffer::Buffer, util::BoxService};

use zebra_chain::{
    amount::{DeferredPoolBalanceChange, MAX_MONEY, MAX_WCASH_COINBASE_VALUE},
    block::{
        tests::generate::{
            large_multi_transaction_block, large_single_transaction_block_many_inputs,
        },
        Block, Height,
    },
    parameters::{subsidy::block_subsidy, NetworkUpgrade},
    serialization::{ZcashDeserialize, ZcashDeserializeInto},
    transaction::{
        arbitrary::{fake_bundle_for_branch, transaction_to_fake_v5},
        LockTime, Transaction,
    },
    transparent,
    work::{
        difficulty::{ParameterDifficulty as _, INVALID_COMPACT_DIFFICULTY},
        equihash::{Solution, WCASH_BLOCK_WIRE_VERSION},
    },
};
use zebra_script::Sigops;
use zebra_test::transcript::{ExpectedTranscriptError, Transcript};

use crate::{block::check::subsidy_is_valid, transaction};

use super::*;

static VALID_BLOCK_TRANSCRIPT: Lazy<Vec<(Request, Result<block::Hash, ExpectedTranscriptError>)>> =
    Lazy::new(|| {
        let block: Arc<_> =
            Block::zcash_deserialize(&zebra_test::vectors::BLOCK_MAINNET_GENESIS_BYTES[..])
                .unwrap()
                .into();
        let hash = Ok(block.as_ref().into());
        vec![(Request::Commit(block), hash)]
    });

static INVALID_TIME_BLOCK_TRANSCRIPT: Lazy<
    Vec<(Request, Result<block::Hash, ExpectedTranscriptError>)>,
> = Lazy::new(|| {
    let mut block: Block =
        Block::zcash_deserialize(&zebra_test::vectors::BLOCK_MAINNET_GENESIS_BYTES[..]).unwrap();

    // Modify the block's time
    // Changing the block header also invalidates the header hashes, but
    // those checks should be performed later in validation, because they
    // are more expensive.
    let three_hours_in_the_future = Utc::now()
        .checked_add_signed(chrono::Duration::hours(3))
        .ok_or_else(|| eyre!("overflow when calculating 3 hours in the future"))
        .unwrap();
    Arc::make_mut(&mut block.header).time = three_hours_in_the_future;

    vec![(
        Request::Commit(Arc::new(block)),
        Err(ExpectedTranscriptError::Any),
    )]
});

static INVALID_HEADER_SOLUTION_TRANSCRIPT: Lazy<
    Vec<(Request, Result<block::Hash, ExpectedTranscriptError>)>,
> = Lazy::new(|| {
    let mut block: Block =
        Block::zcash_deserialize(&zebra_test::vectors::BLOCK_MAINNET_GENESIS_BYTES[..]).unwrap();

    // Change nonce to something invalid
    Arc::make_mut(&mut block.header).nonce = [0; 32].into();

    vec![(
        Request::Commit(Arc::new(block)),
        Err(ExpectedTranscriptError::Any),
    )]
});

static INVALID_COINBASE_TRANSCRIPT: Lazy<
    Vec<(Request, Result<block::Hash, ExpectedTranscriptError>)>,
> = Lazy::new(|| {
    let header = block::Header::zcash_deserialize(&zebra_test::vectors::DUMMY_HEADER[..]).unwrap();

    // Test 1: Empty transaction
    let block1 = Block {
        header: header.clone().into(),
        transactions: Vec::new(),
    };

    // Test 2: Transaction at first position is not coinbase
    let mut transactions = Vec::new();
    let tx = zebra_test::vectors::DUMMY_TX1
        .zcash_deserialize_into()
        .unwrap();
    transactions.push(tx);
    let block2 = Block {
        header: header.into(),
        transactions,
    };

    // Test 3: Invalid coinbase position
    let mut block3 =
        Block::zcash_deserialize(&zebra_test::vectors::BLOCK_MAINNET_GENESIS_BYTES[..]).unwrap();
    assert_eq!(block3.transactions.len(), 1);

    // Extract the coinbase transaction from the block
    let coinbase_transaction = block3.transactions.first().unwrap().clone();

    // Add another coinbase transaction to block
    block3.transactions.push(coinbase_transaction);
    assert_eq!(block3.transactions.len(), 2);

    vec![
        (
            Request::Commit(Arc::new(block1)),
            Err(ExpectedTranscriptError::Any),
        ),
        (
            Request::Commit(Arc::new(block2)),
            Err(ExpectedTranscriptError::Any),
        ),
        (
            Request::Commit(Arc::new(block3)),
            Err(ExpectedTranscriptError::Any),
        ),
    ]
});

// TODO: enable this test after implementing contextual verification
// #[tokio::test]
// #[ignore]
#[allow(dead_code)]
async fn check_transcripts_test() -> Result<(), Report> {
    check_transcripts().await
}

#[allow(dead_code)]
#[spandoc::spandoc]
async fn check_transcripts() -> Result<(), Report> {
    let _init_guard = zebra_test::init();

    let network = Network::Mainnet;
    let state_service = zebra_state::init_test(&network).await;

    let transaction = transaction::BlockTxVerifier::new(&network, state_service.clone());
    let transaction = Buffer::new(BoxService::new(transaction), 1);
    let block_verifier = Buffer::new(
        SemanticBlockVerifier::new(&network, state_service.clone(), transaction),
        1,
    );

    for transcript_data in &[
        &VALID_BLOCK_TRANSCRIPT,
        &INVALID_TIME_BLOCK_TRANSCRIPT,
        &INVALID_HEADER_SOLUTION_TRANSCRIPT,
        &INVALID_COINBASE_TRANSCRIPT,
    ] {
        let transcript = Transcript::from(transcript_data.iter().cloned());
        transcript.check(block_verifier.clone()).await.unwrap();
    }
    Ok(())
}

#[test]
fn coinbase_is_first_for_historical_blocks() -> Result<(), Report> {
    let _init_guard = zebra_test::init();

    let block_iter = zebra_test::vectors::BLOCKS.iter();

    for block in block_iter {
        let block = block
            .zcash_deserialize_into::<Block>()
            .expect("block is structurally valid");

        check::coinbase_is_first(&block)
            .expect("the coinbase in a historical block should be valid");
    }

    Ok(())
}

#[test]
fn difficulty_is_valid_for_historical_blocks() -> Result<(), Report> {
    let _init_guard = zebra_test::init();
    for network in Network::iter() {
        difficulty_is_valid_for_network(network)?;
    }

    Ok(())
}

fn difficulty_is_valid_for_network(network: Network) -> Result<(), Report> {
    let block_iter = network.block_iter();

    for (&height, block) in block_iter {
        let block = block
            .zcash_deserialize_into::<Block>()
            .expect("block is structurally valid");

        check::difficulty_is_valid(&block.header, &network, &Height(height), &block.hash())
            .expect("the difficulty from a historical block should be valid");
    }

    Ok(())
}

#[test]
fn difficulty_validation_failure() -> Result<(), Report> {
    let _init_guard = zebra_test::init();

    // Get a block in the mainnet, and mangle its difficulty field
    let block =
        Arc::<Block>::zcash_deserialize(&zebra_test::vectors::BLOCK_MAINNET_415000_BYTES[..])
            .expect("block should deserialize");
    let mut block = Arc::try_unwrap(block).expect("block should unwrap");
    let height = block.coinbase_height().unwrap();
    let hash = block.hash();

    // Set the difficulty field to an invalid value
    Arc::make_mut(&mut block.header).difficulty_threshold = INVALID_COMPACT_DIFFICULTY;

    // Validate the block
    let result =
        check::difficulty_is_valid(&block.header, &Network::Mainnet, &height, &hash).unwrap_err();
    let expected = BlockError::InvalidDifficulty(height, hash);
    assert_eq!(expected, result);

    // Get a block in the testnet, but tell the validator it's from the mainnet
    let block =
        Arc::<Block>::zcash_deserialize(&zebra_test::vectors::BLOCK_TESTNET_925483_BYTES[..])
            .expect("block should deserialize");
    let block = Arc::try_unwrap(block).expect("block should unwrap");
    let height = block.coinbase_height().unwrap();
    let hash = block.hash();
    let difficulty_threshold = block.header.difficulty_threshold.to_expanded().unwrap();

    // Validate the block as if it is a mainnet block
    let result =
        check::difficulty_is_valid(&block.header, &Network::Mainnet, &height, &hash).unwrap_err();
    let expected = BlockError::TargetDifficultyLimit(
        height,
        hash,
        difficulty_threshold,
        Network::Mainnet,
        Network::Mainnet.target_difficulty_limit(),
    );
    assert_eq!(expected, result);

    // Get a block in the mainnet, but pass an easy hash to the validator
    let block =
        Arc::<Block>::zcash_deserialize(&zebra_test::vectors::BLOCK_MAINNET_415000_BYTES[..])
            .expect("block should deserialize");
    let block = Arc::try_unwrap(block).expect("block should unwrap");
    let height = block.coinbase_height().unwrap();
    let bad_hash = block::Hash([0xff; 32]);
    let difficulty_threshold = block.header.difficulty_threshold.to_expanded().unwrap();

    // Validate the block
    let result = check::difficulty_is_valid(&block.header, &Network::Mainnet, &height, &bad_hash)
        .unwrap_err();
    let expected =
        BlockError::DifficultyFilter(height, bad_hash, difficulty_threshold, Network::Mainnet);
    assert_eq!(expected, result);

    Ok(())
}

#[test]
fn equihash_is_valid_for_historical_blocks() -> Result<(), Report> {
    let _init_guard = zebra_test::init();

    let block_iter = zebra_test::vectors::BLOCKS.iter();

    for block in block_iter {
        let block = block
            .zcash_deserialize_into::<Block>()
            .expect("block is structurally valid");

        check::equihash_solution_is_valid(&block.header)
            .expect("the equihash solution from a historical block should be valid");
    }

    Ok(())
}

#[test]
fn wcash_auxpow_header_and_placeholder_rules() -> Result<(), Report> {
    let _init_guard = zebra_test::init();
    let network = Network::new_wcash_regtest();
    let genesis = zebra_chain::block::genesis::wcash_regtest_genesis_block();
    let genesis_hash = genesis.hash();

    check::wcash_auxpow_is_valid(&genesis.header, &network, &Height::MIN, &genesis_hash)?;

    let mut header = genesis.header.as_ref().clone();
    header.version = 4;
    let hash = block::Hash::from(&header);
    assert!(matches!(
        check::wcash_auxpow_is_valid(&header, &network, &Height(1), &hash),
        Err(BlockError::InvalidWcashHeaderVersion { .. })
    ));

    header.version = WCASH_BLOCK_WIRE_VERSION;
    header.solution = Solution::Regtest([0; 36]);
    let hash = block::Hash::from(&header);
    assert!(matches!(
        check::wcash_auxpow_is_valid(&header, &network, &Height(1), &hash),
        Err(BlockError::MissingWcashAuxPowVariant { .. })
    ));

    header.solution = Solution::for_wcash(Vec::new())?;
    let hash = block::Hash::from(&header);
    assert_eq!(
        check::wcash_auxpow_is_valid(&header, &network, &Height(1), &hash),
        Err(BlockError::MissingWcashAuxPow {
            height: Height(1),
            hash,
        })
    );
    check::wcash_auxpow_proposal_is_valid(&header, &network, &Height(1), &hash)?;

    header.solution = Solution::for_wcash(vec![0])?;
    let hash = block::Hash::from(&header);
    assert!(matches!(
        check::wcash_auxpow_is_valid(&header, &network, &Height(1), &hash),
        Err(BlockError::InvalidWcashAuxPow {
            source: wcash_zcash_aux::AuxPowError::UnexpectedEnd {
                field: "proof magic",
                ..
            },
            ..
        })
    ));

    assert_eq!(
        check::wcash_auxpow_proposal_is_valid(&header, &network, &Height::MIN, &hash),
        Err(BlockError::UnexpectedWcashGenesisAuxPow { hash })
    );
    assert_eq!(
        check::wcash_auxpow_is_valid(&header, &network, &Height::MIN, &hash),
        Err(BlockError::UnexpectedWcashGenesisAuxPow { hash })
    );

    Ok(())
}

#[test]
fn subsidy_is_valid_for_historical_blocks() -> Result<(), Report> {
    let _init_guard = zebra_test::init();
    for network in Network::iter() {
        subsidy_is_valid_for_network(network)?;
    }

    Ok(())
}

fn subsidy_is_valid_for_network(network: Network) -> Result<(), Report> {
    let block_iter = network.block_iter();

    for (&height, block) in block_iter {
        let height = block::Height(height);
        let block = block
            .zcash_deserialize_into::<Block>()
            .expect("block is structurally valid");

        let canopy_activation_height = NetworkUpgrade::Canopy
            .activation_height(&network)
            .expect("Canopy activation height is known");

        // TODO: first halving, second halving, third halving, and very large halvings
        if height >= canopy_activation_height {
            let expected_block_subsidy =
                zebra_chain::parameters::subsidy::block_subsidy(height, &network)
                    .expect("valid block subsidy");

            check::subsidy_is_valid(&block, &network, expected_block_subsidy)
                .expect("subsidies should pass for this block");
        }
    }

    Ok(())
}

#[test]
fn coinbase_validation_failure() -> Result<(), Report> {
    let _init_guard = zebra_test::init();
    let network = Network::Mainnet;

    // Get a block in the mainnet that is inside the funding stream period,
    // and delete the coinbase transaction
    let block =
        Arc::<Block>::zcash_deserialize(&zebra_test::vectors::BLOCK_MAINNET_1046400_BYTES[..])
            .expect("block should deserialize");
    let mut block = Arc::try_unwrap(block).expect("block should unwrap");

    let expected_block_subsidy = zebra_chain::parameters::subsidy::block_subsidy(
        block
            .coinbase_height()
            .expect("block should have coinbase height"),
        &network,
    )
    .expect("valid block subsidy");

    // Remove coinbase transaction
    block.transactions.remove(0);

    // Validate the block using coinbase_is_first
    let result = check::coinbase_is_first(&block).unwrap_err();
    let expected = BlockError::NoTransactions;
    assert_eq!(expected, result);

    let result = check::subsidy_is_valid(&block, &network, expected_block_subsidy).unwrap_err();
    let expected = BlockError::Transaction(TransactionError::Subsidy(SubsidyError::NoCoinbase));
    assert_eq!(expected, result);

    // Get another funding stream block, and delete the coinbase transaction
    let block =
        Arc::<Block>::zcash_deserialize(&zebra_test::vectors::BLOCK_MAINNET_1046401_BYTES[..])
            .expect("block should deserialize");
    let mut block = Arc::try_unwrap(block).expect("block should unwrap");

    let expected_block_subsidy = zebra_chain::parameters::subsidy::block_subsidy(
        block
            .coinbase_height()
            .expect("block should have coinbase height"),
        &network,
    )
    .expect("valid block subsidy");

    // Remove coinbase transaction
    block.transactions.remove(0);

    // Validate the block using coinbase_is_first
    let result = check::coinbase_is_first(&block).unwrap_err();
    let expected = BlockError::Transaction(TransactionError::CoinbasePosition);
    assert_eq!(expected, result);

    let result = check::subsidy_is_valid(&block, &network, expected_block_subsidy).unwrap_err();
    let expected = BlockError::Transaction(TransactionError::Subsidy(SubsidyError::NoCoinbase));
    assert_eq!(expected, result);

    // Get another funding stream, and duplicate the coinbase transaction
    let block =
        Arc::<Block>::zcash_deserialize(&zebra_test::vectors::BLOCK_MAINNET_1180900_BYTES[..])
            .expect("block should deserialize");
    let mut block = Arc::try_unwrap(block).expect("block should unwrap");

    // Remove coinbase transaction
    block.transactions.push(
        block
            .transactions
            .first()
            .expect("block has coinbase")
            .clone(),
    );

    // Validate the block using coinbase_is_first
    let result = check::coinbase_is_first(&block).unwrap_err();
    let expected = BlockError::Transaction(TransactionError::CoinbaseAfterFirst);
    assert_eq!(expected, result);

    let expected_block_subsidy = zebra_chain::parameters::subsidy::block_subsidy(
        block
            .coinbase_height()
            .expect("block should have coinbase height"),
        &network,
    )
    .expect("valid block subsidy");

    check::subsidy_is_valid(&block, &network, expected_block_subsidy)
        .expect("subsidy does not check for extra coinbase transactions");

    Ok(())
}

#[test]
fn funding_stream_validation() -> Result<(), Report> {
    let _init_guard = zebra_test::init();
    for network in Network::iter() {
        funding_stream_validation_for_network(network)?;
    }

    Ok(())
}

fn funding_stream_validation_for_network(network: Network) -> Result<(), Report> {
    let block_iter = network.block_iter();

    let canopy_activation_height = NetworkUpgrade::Canopy
        .activation_height(&network)
        .expect("Canopy activation height is known");

    for (&height, block) in block_iter {
        let height = Height(height);

        if height >= canopy_activation_height {
            let block = Block::zcash_deserialize(&block[..]).expect("block should deserialize");
            let expected_block_subsidy =
                zebra_chain::parameters::subsidy::block_subsidy(height, &network)
                    .expect("valid block subsidy");

            // Validate
            let result = check::subsidy_is_valid(&block, &network, expected_block_subsidy);
            assert!(result.is_ok());
        }
    }

    Ok(())
}

#[test]
fn funding_stream_validation_failure() -> Result<(), Report> {
    let _init_guard = zebra_test::init();
    let network = Network::Mainnet;

    // Get a block in the mainnet that is inside the funding stream period.
    let block =
        Arc::<Block>::zcash_deserialize(&zebra_test::vectors::BLOCK_MAINNET_1046400_BYTES[..])
            .expect("block should deserialize");

    // Build the new transaction with modified coinbase outputs
    let tx = block
        .transactions
        .first()
        .map(|transaction| {
            let mut output = transaction.outputs()[0].clone();
            output.value = Amount::try_from(i32::MAX).unwrap();
            Transaction::test_v4(
                transaction.inputs().to_vec(),
                vec![output],
                transaction.lock_time().unwrap_or_else(LockTime::unlocked),
                Height(0),
            )
        })
        .unwrap();

    // Build new block
    let transactions: Vec<Arc<zebra_chain::transaction::Transaction>> = vec![Arc::new(tx)];
    let block = Block {
        header: block.header.clone(),
        transactions,
    };

    // Validate it
    let expected_block_subsidy = zebra_chain::parameters::subsidy::block_subsidy(
        block
            .coinbase_height()
            .expect("block should have coinbase height"),
        &network,
    )
    .expect("valid block subsidy");

    let result = check::subsidy_is_valid(&block, &network, expected_block_subsidy);
    let expected = Err(BlockError::Transaction(TransactionError::Subsidy(
        SubsidyError::FundingStreamNotFound,
    )));
    assert_eq!(expected, result);

    Ok(())
}

#[test]
fn miner_fees_validation_success() -> Result<(), Report> {
    let _init_guard = zebra_test::init();
    for network in Network::iter() {
        miner_fees_validation_for_network(network)?;
    }

    Ok(())
}

fn miner_fees_validation_for_network(network: Network) -> Result<(), Report> {
    let block_iter = network.block_iter();

    for (&height, block) in block_iter {
        let height = Height(height);
        if height > network.slow_start_shift() {
            let block = Block::zcash_deserialize(&block[..]).expect("block should deserialize");
            let coinbase_tx = check::coinbase_is_first(&block)?;

            let expected_block_subsidy = block_subsidy(height, &network)?;
            // See [ZIP-1015](https://zips.z.cash/zip-1015).
            let deferred_pool_balance_change =
                match NetworkUpgrade::Canopy.activation_height(&network) {
                    Some(activation_height) if height >= activation_height => {
                        subsidy_is_valid(&block, &network, expected_block_subsidy)?
                    }
                    _other => DeferredPoolBalanceChange::zero(),
                };

            assert!(check::miner_fees_are_valid(
                &coinbase_tx,
                height,
                // Set the miner fees to a high-enough amount.
                Amount::try_from(MAX_MONEY / 2).unwrap(),
                expected_block_subsidy,
                deferred_pool_balance_change,
                &network,
            )
            .is_ok(),);
        }
    }

    Ok(())
}

#[test]
fn miner_fees_validation_failure() -> Result<(), Report> {
    let _init_guard = zebra_test::init();
    let network = Network::Mainnet;
    let block = Block::zcash_deserialize(&zebra_test::vectors::BLOCK_MAINNET_347499_BYTES[..])
        .expect("block should deserialize");
    let height = block.coinbase_height().expect("valid coinbase height");
    let expected_block_subsidy = block_subsidy(height, &network)?;
    // See [ZIP-1015](https://zips.z.cash/zip-1015).
    let deferred_pool_balance_change = match NetworkUpgrade::Canopy.activation_height(&network) {
        Some(activation_height) if height >= activation_height => {
            subsidy_is_valid(&block, &network, expected_block_subsidy)?
        }
        _other => DeferredPoolBalanceChange::zero(),
    };

    assert_eq!(
        check::miner_fees_are_valid(
            check::coinbase_is_first(&block)?.as_ref(),
            height,
            // Set the miner fee to an invalid amount.
            Amount::zero(),
            expected_block_subsidy,
            deferred_pool_balance_change,
            &network
        ),
        Err(BlockError::Transaction(TransactionError::Subsidy(
            SubsidyError::InvalidMinerFees,
        )))
    );

    Ok(())
}

#[test]
fn wcash_coinbase_accepts_transparent_payout_and_enforces_value_limits() -> Result<(), Report> {
    let _init_guard = zebra_test::init();
    let network = Network::new_wcash_regtest();
    let height = Height(1);
    let expected_block_subsidy = block_subsidy(height, &network)?;
    let deferred = DeferredPoolBalanceChange::zero();

    let block = Block::zcash_deserialize(&zebra_test::vectors::BLOCK_MAINNET_1046400_BYTES[..])
        .expect("block should deserialize");
    let transparent_coinbase = block
        .transactions
        .first()
        .expect("block contains a coinbase");

    let transparent_output = transparent::Output {
        value: expected_block_subsidy,
        lock_script: transparent_coinbase
            .outputs()
            .first()
            .expect("fixture coinbase has a transparent output")
            .lock_script
            .clone(),
    };
    let valid_transparent_coinbase = Transaction::test_v4(
        transparent_coinbase.inputs(),
        vec![transparent_output.clone()],
        transparent_coinbase
            .lock_time()
            .unwrap_or_else(LockTime::unlocked),
        Height::MIN,
    );

    assert_eq!(
        check::miner_fees_are_valid(
            &valid_transparent_coinbase,
            height,
            Amount::try_from(MAX_WCASH_COINBASE_VALUE)?,
            expected_block_subsidy,
            deferred,
            &network,
        ),
        Err(BlockError::Transaction(TransactionError::Subsidy(
            SubsidyError::WcashCoinbaseValueTooLarge,
        )))
    );

    check::miner_fees_are_valid(
        &valid_transparent_coinbase,
        height,
        Amount::zero(),
        expected_block_subsidy,
        deferred,
        &network,
    )?;

    let coinbase_input = vec![transparent::Input::Coinbase {
        height,
        data: vec![1, 1],
        sequence: u32::MAX,
    }];
    let ironwood = fake_bundle_for_branch(
        zcash_protocol::consensus::BranchId::Nu6_3,
        ::orchard::ValuePool::Ironwood,
        1,
        11,
    )
    .expect("NU6.3 defines the Ironwood pool");
    let mixed_coinbase = Transaction::test_v6_with_bundles(
        NetworkUpgrade::Nu6_3,
        coinbase_input.clone(),
        vec![transparent_output.clone()],
        LockTime::unlocked(),
        height,
        None,
        Some(ironwood),
    );
    assert_eq!(
        check::miner_fees_are_valid(
            &mixed_coinbase,
            height,
            Amount::zero(),
            expected_block_subsidy,
            deferred,
            &network,
        ),
        Err(BlockError::Transaction(TransactionError::Subsidy(
            SubsidyError::WcashMixedCoinbaseOutputs,
        )))
    );

    let orchard = fake_bundle_for_branch(
        zcash_protocol::consensus::BranchId::Nu6_3,
        ::orchard::ValuePool::Orchard,
        1,
        12,
    )
    .expect("NU6.3 defines the Orchard pool");
    let legacy_orchard_coinbase = Transaction::test_v6_with_bundles(
        NetworkUpgrade::Nu6_3,
        coinbase_input,
        vec![transparent_output],
        LockTime::unlocked(),
        height,
        Some(orchard),
        None,
    );
    assert_eq!(
        check::miner_fees_are_valid(
            &legacy_orchard_coinbase,
            height,
            Amount::zero(),
            expected_block_subsidy,
            deferred,
            &network,
        ),
        Err(BlockError::Transaction(TransactionError::Subsidy(
            SubsidyError::WcashOrchardCoinbaseOutput,
        )))
    );

    let legacy_sapling_coinbase = Network::Mainnet
        .block_iter()
        .map(|(_, bytes)| {
            Block::zcash_deserialize(*bytes)
                .expect("the fixed Mainnet block vector must deserialize")
        })
        .filter_map(|block| block.transactions.first().cloned())
        .find(|coinbase| coinbase.has_sapling_shielded_data())
        .expect("the fixed Mainnet vectors include a Sapling coinbase");
    assert_eq!(
        check::miner_fees_are_valid(
            &legacy_sapling_coinbase,
            height,
            Amount::zero(),
            expected_block_subsidy,
            deferred,
            &network,
        ),
        Err(BlockError::Transaction(TransactionError::Subsidy(
            SubsidyError::WcashSaplingCoinbaseOutput,
        )))
    );

    let no_output_coinbase = Transaction::test_v4(
        transparent_coinbase.inputs(),
        Vec::new(),
        transparent_coinbase
            .lock_time()
            .unwrap_or_else(LockTime::unlocked),
        Height::MIN,
    );
    assert_eq!(
        check::miner_fees_are_valid(
            &no_output_coinbase,
            height,
            Amount::zero(),
            expected_block_subsidy,
            deferred,
            &network,
        ),
        Err(BlockError::Transaction(TransactionError::Subsidy(
            SubsidyError::WcashCoinbaseOutputMissing,
        )))
    );

    Ok(())
}

#[test]
fn wcash_coinbase_payout_modes_are_exclusive() {
    use check::WcashCoinbasePayoutMode;

    assert_eq!(
        check::wcash_coinbase_payout_mode(true, false),
        Ok(WcashCoinbasePayoutMode::Transparent)
    );
    assert_eq!(
        check::wcash_coinbase_payout_mode(false, true),
        Ok(WcashCoinbasePayoutMode::Ironwood)
    );
    assert_eq!(
        check::wcash_coinbase_payout_mode(false, false),
        Err(SubsidyError::WcashCoinbaseOutputMissing)
    );
    assert_eq!(
        check::wcash_coinbase_payout_mode(true, true),
        Err(SubsidyError::WcashMixedCoinbaseOutputs)
    );
}

#[test]
fn time_is_valid_for_historical_blocks() -> Result<(), Report> {
    let _init_guard = zebra_test::init();

    let block_iter = zebra_test::vectors::BLOCKS.iter();
    let now = Utc::now();

    for block in block_iter {
        let block = block
            .zcash_deserialize_into::<Block>()
            .expect("block is structurally valid");

        // This check is non-deterministic, but the block test vectors are
        // all in the past. So it's unlikely that the test machine will have
        // a clock that's far enough in the past for the test to fail.
        check::time_is_valid_at(
            &block.header,
            now,
            &block
                .coinbase_height()
                .expect("block test vector height should be valid"),
            &block.hash(),
        )
        .expect("the header time from a historical block should be valid, based on the test machine's local clock. Hint: check the test machine's time, date, and timezone.");
    }

    Ok(())
}

#[test]
fn merkle_root_is_valid() -> Result<(), Report> {
    let _init_guard = zebra_test::init();

    // test all original blocks available, all blocks validate
    for network in Network::iter() {
        merkle_root_is_valid_for_network(network)?;
    }

    // create and test fake blocks with v5 transactions, all blocks fail validation
    for network in Network::iter() {
        merkle_root_fake_v5_for_network(network)?;
    }

    Ok(())
}

fn merkle_root_is_valid_for_network(network: Network) -> Result<(), Report> {
    let block_iter = network.block_iter();

    for (_height, block) in block_iter {
        let block = block
            .zcash_deserialize_into::<Block>()
            .expect("block is structurally valid");

        let transaction_hashes = block
            .transactions
            .iter()
            .map(|tx| tx.hash())
            .collect::<Vec<_>>();

        check::merkle_root_validity(&network, &block, &transaction_hashes)
            .expect("merkle root should be valid for this block");
    }

    Ok(())
}

fn merkle_root_fake_v5_for_network(network: Network) -> Result<(), Report> {
    let block_iter = network.block_iter();

    for (height, block) in block_iter {
        let mut block = block
            .zcash_deserialize_into::<Block>()
            .expect("block is structurally valid");

        // skip blocks that are before overwinter as they will not have a valid consensus branch id
        if *height
            < NetworkUpgrade::Overwinter
                .activation_height(&network)
                .expect("a valid overwinter activation height")
                .0
        {
            continue;
        }

        // convert all transactions from the block to V5
        let transactions: Vec<Arc<Transaction>> = block
            .transactions
            .iter()
            .map(AsRef::as_ref)
            .map(|t| transaction_to_fake_v5(t, &network, Height(*height)))
            .map(Into::into)
            .collect();

        block.transactions = transactions;

        let transaction_hashes = block
            .transactions
            .iter()
            .map(|tx| tx.hash())
            .collect::<Vec<_>>();

        // Replace the merkle root so that it matches the modified transactions.
        // This test provides some transaction id and merkle root coverage,
        // but we also need to test against zcashd test vectors.
        Arc::make_mut(&mut block.header).merkle_root = transaction_hashes.iter().cloned().collect();

        check::merkle_root_validity(&network, &block, &transaction_hashes)
            .expect("merkle root should be valid for this block");
    }

    Ok(())
}

/// A block whose transaction list contains the same `transaction::Hash`
/// twice must be rejected with [`BlockError::DuplicateTransaction`].
///
/// This guards against Bitcoin's Merkle tree malleability (CVE-2012-2459),
/// where two identical transaction hashes can produce the same Merkle root
/// as the underlying unique list.
#[test]
fn merkle_root_validity_rejects_duplicate_transaction_hash() -> Result<(), Report> {
    let _init_guard = zebra_test::init();

    let network = Network::Mainnet;

    let mut block = Block::zcash_deserialize(&zebra_test::vectors::BLOCK_MAINNET_1180900_BYTES[..])
        .expect("block should deserialize");

    // Duplicate the coinbase transaction so the block contains two
    // transactions with the same hash.
    let duplicate = block
        .transactions
        .first()
        .expect("block has coinbase")
        .clone();
    block.transactions.push(duplicate);

    let transaction_hashes: Vec<_> = block.transactions.iter().map(|tx| tx.hash()).collect();

    // Recompute the Merkle root from the duplicated transaction list so
    // that `merkle_root_validity` passes the `BadMerkleRoot` check and
    // reaches the duplicate-hash check.
    Arc::make_mut(&mut block.header).merkle_root = transaction_hashes.iter().cloned().collect();

    let result = check::merkle_root_validity(&network, &block, &transaction_hashes);
    assert_eq!(result, Err(BlockError::DuplicateTransaction));

    Ok(())
}

#[test]
fn legacy_sigops_count_for_large_generated_blocks() {
    let _init_guard = zebra_test::init();

    // We can't test sigops using the transaction verifier, because it looks up UTXOs.

    let block = large_single_transaction_block_many_inputs();
    let mut legacy_sigop_count = 0;
    for tx in block.transactions {
        let tx_sigop_count = tx.sigops().expect("unexpected invalid sigop count");
        assert_eq!(tx_sigop_count, 0);
        legacy_sigop_count += tx_sigop_count;
    }
    // We know this block has no sigops.
    assert_eq!(legacy_sigop_count, 0);

    let block = large_multi_transaction_block();
    let mut sigops = 0;
    for tx in block.transactions {
        let tx_sigop_count = tx.sigops().expect("unexpected invalid sigop count");
        assert_eq!(tx_sigop_count, 1);
        sigops += tx_sigop_count;
    }
    // Test that large blocks can actually fail the sigops check.
    assert!(sigops > MAX_BLOCK_SIGOPS);
}

#[test]
fn legacy_sigops_count_for_historic_blocks() {
    let _init_guard = zebra_test::init();

    // We can't test sigops using the transaction verifier, because it looks up UTXOs.

    for block in zebra_test::vectors::BLOCKS.iter() {
        let mut sigops = 0;

        let block: Block = block
            .zcash_deserialize_into()
            .expect("block test vector is valid");
        for tx in block.transactions {
            sigops += tx.sigops().expect("unexpected invalid sigop count");
        }

        // Test that historic blocks pass the sigops check.
        assert!(sigops <= MAX_BLOCK_SIGOPS);
    }
}

#[test]
fn transaction_expiration_height_validation() -> Result<(), Report> {
    let _init_guard = zebra_test::init();

    for network in Network::iter() {
        transaction_expiration_height_for_network(&network)?;
    }

    Ok(())
}

fn transaction_expiration_height_for_network(network: &Network) -> Result<(), Report> {
    let block_iter = network.block_iter();

    for (&height, block) in block_iter {
        let block = Block::zcash_deserialize(&block[..]).expect("block should deserialize");

        for (n, transaction) in block.transactions.iter().enumerate() {
            if n == 0 {
                // coinbase
                let result = transaction::check::coinbase_expiry_height(
                    &Height(height),
                    transaction,
                    network,
                );
                assert!(result.is_ok());
            } else {
                // non coinbase
                let result =
                    transaction::check::non_coinbase_expiry_height(&Height(height), transaction);
                assert!(result.is_ok());
            }
        }
    }

    Ok(())
}

#[test]
fn block_error_misbehavior_scores() {
    use crate::error::BlockError;

    assert_eq!(BlockError::NoTransactions.misbehavior_score(), 100);
    assert_eq!(
        BlockError::BadMerkleRoot {
            actual: zebra_chain::block::merkle::Root([0; 32]),
            expected: zebra_chain::block::merkle::Root([1; 32]),
        }
        .misbehavior_score(),
        100
    );
    assert_eq!(
        BlockError::WrongTransactionConsensusBranchId.misbehavior_score(),
        100
    );
    assert_eq!(
        BlockError::MissingHeight(zebra_chain::block::Hash([0; 32])).misbehavior_score(),
        100
    );
}

#[test]
fn verify_block_error_misbehavior_scores() {
    let dup_err = zebra_state::CommitBlockError::Duplicate {
        hash_or_height: None,
        location: zebra_state::KnownBlock::BestChain,
    };
    assert_eq!(VerifyBlockError::Commit(dup_err).misbehavior_score(), 0);
}

/// Duplicate block errors must stay classified as duplicate requests after the
/// state wraps them, so they don't restart the syncer or turn `submitblock`
/// duplicates into rejections.
#[test]
fn state_commit_duplicate_errors_are_duplicate_requests() {
    let duplicate = zs::CommitBlockError::Duplicate {
        hash_or_height: None,
        location: zs::KnownBlock::BestChain,
    };

    // Box the error the same way the state's `CommitSemanticallyVerifiedBlock`
    // handler does. This mirrors the wrapping manually, so it won't fail
    // automatically if the state changes its error type — keep it in sync by hand.
    let source: BoxError = Box::new(zs::CommitSemanticallyVerifiedError::from(duplicate));

    let err = map_commit_error(source, block::Hash([0; 32]));

    assert!(
        matches!(err, VerifyBlockError::Commit(_)),
        "state commit errors must be unwrapped into VerifyBlockError::Commit, got: {err:?}"
    );
    assert!(err.is_duplicate_request());
    assert_eq!(err.misbehavior_score(), 0);
}
