#![allow(clippy::unwrap_in_result)]

mod prop;
mod vectors;

use color_eyre::Report;

use super::Network;
use crate::{
    amount::{Amount, NonNegative, COIN, MAX_MONEY},
    block::{genesis::WCASH_TESTNET_GENESIS_HASH, Height},
    parameters::{
        subsidy::{
            block_subsidy, constants::POST_BLOSSOM_HALVING_INTERVAL, founders_reward,
            founders_reward_address, funding_stream_values, halving, halving_divisor,
            height_for_halving, miner_subsidy, ParameterSubsidy as _, WCASH_HALVING_INTERVAL,
            WCASH_INITIAL_BLOCK_SUBSIDY, WCASH_REGTEST_FIRST_HALVING_HEIGHT,
            WCASH_TESTNET_FIRST_HALVING_HEIGHT, WCASH_TESTNET_SLOW_START_INTERVAL,
            WCASH_TESTNET_SLOW_START_SHIFT,
        },
        ConsensusBranchId, NetworkKind, NetworkUpgrade, WCASH_MAINNET_V1_BRANCH_ID,
        WCASH_REGTEST_V1_BRANCH_ID, WCASH_TESTNET_V1_BRANCH_ID, WCASH_TESTNET_V3_BRANCH_ID,
    },
    serialization::DateTime32,
    work::difficulty::ParameterDifficulty as _,
};

#[test]
fn compiled_consensus_profile_is_mutually_exclusive() {
    let zcash = Network::Mainnet;
    let wcash_networks = [
        Network::new_wcash_mainnet_for_tests(),
        Network::new_wcash_testnet(),
        Network::new_wcash_regtest(),
    ];

    assert_eq!(
        zcash.is_compatible_with_compiled_consensus(),
        !cfg!(feature = "wcash-consensus")
    );
    for wcash in &wcash_networks {
        assert_eq!(
            wcash.is_compatible_with_compiled_consensus(),
            cfg!(feature = "wcash-consensus")
        );
    }

    if cfg!(feature = "wcash-consensus") {
        for wcash in &wcash_networks {
            wcash.assert_compatible_with_compiled_consensus();
        }
        assert!(
            std::panic::catch_unwind(|| zcash.assert_compatible_with_compiled_consensus()).is_err()
        );
    } else {
        zcash.assert_compatible_with_compiled_consensus();
        for wcash in wcash_networks {
            assert!(
                std::panic::catch_unwind(|| wcash.assert_compatible_with_compiled_consensus())
                    .is_err()
            );
        }
    }
}

#[test]
fn wcash_mainnet_profile_is_fully_wired_with_frozen_anchor() -> Result<(), Report> {
    let mainnet = Network::try_new_wcash_mainnet().expect("mainnet anchor is frozen");
    assert_eq!(
        mainnet.genesis_hash(),
        crate::block::genesis::wcash_mainnet_genesis_block()
            .expect("mainnet genesis is frozen")
            .hash()
    );
    assert!(mainnet.uses_wcash_consensus());
    assert!(mainnet.is_wcash_mainnet());
    assert!(!mainnet.is_wcash_testnet());
    assert!(!mainnet.is_wcash_regtest());
    assert!(!mainnet.is_a_test_network());
    assert_eq!(mainnet.kind(), NetworkKind::Mainnet);
    assert_eq!(mainnet.t_addr_kind(), NetworkKind::Mainnet);
    assert_eq!(mainnet.to_string(), "WcashMainnet");
    assert_eq!(mainnet.lowercase_name(), "wcashmainnet-v1");
    assert_eq!(mainnet.default_port(), 48233);
    assert_eq!(mainnet.wcash_default_rpc_port(), Some(48232));
    assert_eq!(
        mainnet.magic().0,
        wcash_genesis::network_identity(wcash_genesis::WcashNetwork::Mainnet).p2p_magic()
    );
    assert_eq!(
        mainnet.target_difficulty_limit().to_compact().to_string(),
        format!("{:08x}", wcash_genesis::PUBLIC_MAINNET_POW_LIMIT_BITS)
    );
    assert_eq!(
        block_subsidy(Height::MIN, &mainnet)?,
        Amount::<NonNegative>::zero()
    );
    assert_eq!(
        block_subsidy(Height(40_000), &mainnet)?.zatoshis(),
        2_199_023_255
    );
    assert_eq!(
        block_subsidy(Height(40_001), &mainnet)?.zatoshis(),
        2_199_023_255
    );
    assert_eq!(ConsensusBranchId::current(&mainnet, Height::MIN), None);
    assert_eq!(
        ConsensusBranchId::current(&mainnet, Height(1)),
        Some(WCASH_MAINNET_V1_BRANCH_ID)
    );
    assert_eq!(
        NetworkUpgrade::try_from(u32::from(WCASH_MAINNET_V1_BRANCH_ID)),
        Ok(NetworkUpgrade::Nu6_3)
    );
    assert_ne!(WCASH_MAINNET_V1_BRANCH_ID, WCASH_TESTNET_V3_BRANCH_ID);
    assert_ne!(WCASH_MAINNET_V1_BRANCH_ID, WCASH_REGTEST_V1_BRANCH_ID);
    assert_eq!(
        zcash_protocol::consensus::BranchId::for_height(
            &mainnet,
            zcash_protocol::consensus::BlockHeight::from_u32(1),
        ),
        zcash_protocol::consensus::BranchId::WcashMainnetV1
    );

    Ok(())
}

#[test]
fn wcash_testnet_has_frozen_isolated_network_identity() -> Result<(), Report> {
    let testnet = Network::new_wcash_testnet();
    let regtest = Network::new_wcash_regtest();
    let zcash_networks = [
        Network::Mainnet,
        Network::new_default_testnet(),
        Network::new_regtest(Default::default()),
    ];

    assert!(testnet.uses_wcash_consensus());
    assert!(testnet.is_wcash_testnet());
    assert!(!testnet.is_wcash_regtest());
    assert!(!testnet.is_regtest());
    assert_eq!(testnet.to_string(), "WcashTestnet");
    assert_eq!(testnet.default_port(), 38233);
    assert_eq!(testnet.wcash_default_rpc_port(), Some(38232));
    assert_eq!(
        testnet.magic().0,
        wcash_genesis::network_identity(wcash_genesis::WcashNetwork::Testnet).p2p_magic()
    );

    let genesis = crate::block::genesis::wcash_testnet_genesis_block();
    assert_eq!(genesis.hash().to_string(), WCASH_TESTNET_GENESIS_HASH);
    assert_eq!(testnet.genesis_hash(), genesis.hash());
    assert_eq!(
        testnet.checkpoint_list().hash(Height::MIN),
        Some(genesis.hash())
    );
    assert_eq!(
        genesis.header.difficulty_threshold,
        testnet.target_difficulty_limit().to_compact()
    );
    assert_eq!(
        genesis
            .header
            .difficulty_threshold
            .to_work()
            .expect("the Wcash Testnet launch target has non-zero work")
            .as_u128(),
        351_933,
        "the frozen launch target must retain its independently checked work value"
    );
    assert_eq!(
        genesis.header.difficulty_threshold.to_string(),
        format!("{:08x}", wcash_genesis::PUBLIC_TESTNET_POW_LIMIT_BITS)
    );
    assert_eq!(
        block_subsidy(Height::MIN, &testnet)?,
        Amount::<NonNegative>::zero(),
        "Wcash genesis contributes no scheduled issuance"
    );
    assert_eq!(
        testnet.slow_start_interval(),
        WCASH_TESTNET_SLOW_START_INTERVAL
    );
    assert_eq!(testnet.slow_start_shift(), WCASH_TESTNET_SLOW_START_SHIFT);

    assert_ne!(testnet, regtest);
    assert_ne!(testnet.to_string(), regtest.to_string());
    assert_ne!(testnet.magic(), regtest.magic());
    assert_ne!(testnet.genesis_hash(), regtest.genesis_hash());
    assert_ne!(testnet.default_port(), regtest.default_port());
    assert_ne!(
        testnet.wcash_default_rpc_port(),
        regtest.wcash_default_rpc_port()
    );
    assert_eq!(
        ConsensusBranchId::current(&testnet, Height::MIN),
        None,
        "Wcash genesis has no post-Overwinter transaction branch"
    );
    assert_eq!(
        ConsensusBranchId::current(&testnet, Height(1)),
        Some(WCASH_TESTNET_V3_BRANCH_ID)
    );
    assert_eq!(
        ConsensusBranchId::current(&regtest, Height(1)),
        Some(WCASH_REGTEST_V1_BRANCH_ID)
    );
    assert_eq!(
        NetworkUpgrade::try_from(u32::from(WCASH_TESTNET_V3_BRANCH_ID)),
        Ok(NetworkUpgrade::Nu6_3)
    );
    assert_eq!(
        NetworkUpgrade::try_from(u32::from(WCASH_REGTEST_V1_BRANCH_ID)),
        Ok(NetworkUpgrade::Nu6_3)
    );
    assert_eq!(
        zcash_protocol::consensus::BranchId::for_height(
            &testnet,
            zcash_protocol::consensus::BlockHeight::from_u32(1),
        ),
        zcash_protocol::consensus::BranchId::WcashTestnetV3
    );
    assert_eq!(
        zcash_protocol::consensus::BranchId::for_height(
            &regtest,
            zcash_protocol::consensus::BlockHeight::from_u32(1),
        ),
        zcash_protocol::consensus::BranchId::WcashRegtestV1
    );
    assert_ne!(WCASH_TESTNET_V3_BRANCH_ID, WCASH_TESTNET_V1_BRANCH_ID);
    assert_ne!(WCASH_TESTNET_V3_BRANCH_ID, WCASH_REGTEST_V1_BRANCH_ID);

    for zcash in zcash_networks {
        assert!(!zcash.uses_wcash_consensus());
        assert_ne!(testnet.to_string(), zcash.to_string());
        assert_ne!(testnet.magic(), zcash.magic());
        assert_ne!(testnet.genesis_hash(), zcash.genesis_hash());
        assert_ne!(testnet.default_port(), zcash.default_port());
    }

    Ok(())
}

#[test]
fn wcash_testnet_uses_linear_slow_start_subsidy() -> Result<(), Report> {
    let testnet = Network::new_wcash_testnet();

    for (height, expected_atomic_units) in [
        (0, 0),
        (1, 15_625),
        (10_000, 156_250_000),
        (20_000, 312_500_000),
        (30_000, 468_750_000),
        (39_999, 624_984_375),
        (40_000, 625_000_000),
        (40_001, 625_000_000),
    ] {
        let expected_subsidy = Amount::<NonNegative>::try_from(expected_atomic_units)?;
        assert_eq!(
            block_subsidy(Height(height), &testnet)?,
            expected_subsidy,
            "unexpected Wcash Testnet subsidy at height {height}"
        );
        assert_eq!(
            miner_subsidy(Height(height), &testnet, expected_subsidy)?,
            expected_subsidy,
            "the miner must receive the entire Wcash Testnet subsidy at height {height}"
        );
    }

    // The Wcash rule is the direct integer formula requested for every ramp
    // height, without Zcash's midpoint adjustment.
    for height in 0..=WCASH_TESTNET_SLOW_START_INTERVAL.0 {
        let expected_atomic_units = WCASH_INITIAL_BLOCK_SUBSIDY * u64::from(height)
            / u64::from(WCASH_TESTNET_SLOW_START_INTERVAL);
        assert_eq!(
            block_subsidy(Height(height), &testnet)?,
            Amount::<NonNegative>::try_from(expected_atomic_units)?,
            "linear slow-start formula changed at height {height}"
        );
    }

    let full_subsidy = Amount::<NonNegative>::try_from(WCASH_INITIAL_BLOCK_SUBSIDY)?;
    let half_subsidy = Amount::<NonNegative>::try_from(WCASH_INITIAL_BLOCK_SUBSIDY / 2)?;
    assert_eq!(
        block_subsidy(
            (WCASH_TESTNET_FIRST_HALVING_HEIGHT - 1)
                .expect("the first halving has a previous height"),
            &testnet,
        )?,
        full_subsidy
    );
    assert_eq!(
        block_subsidy(WCASH_TESTNET_FIRST_HALVING_HEIGHT, &testnet)?,
        half_subsidy
    );
    assert_eq!(block_subsidy(Height(3_379_999), &testnet)?, half_subsidy);
    assert_eq!(
        block_subsidy(Height(3_380_000), &testnet)?,
        Amount::<NonNegative>::try_from(WCASH_INITIAL_BLOCK_SUBSIDY / 4)?
    );
    assert_eq!(
        height_for_halving(1, &testnet),
        Some(WCASH_TESTNET_FIRST_HALVING_HEIGHT)
    );
    assert_eq!(height_for_halving(2, &testnet), Some(Height(3_380_000)));
    assert_eq!(
        testnet.height_for_first_halving(),
        WCASH_TESTNET_FIRST_HALVING_HEIGHT
    );

    Ok(())
}

#[test]
fn wcash_testnet_launch_difficulty_and_time_rules_start_at_height_one() {
    let network = Network::new_wcash_testnet();
    let previous_time: chrono::DateTime<chrono::Utc> = DateTime32::from(1_600_000_000).into();
    let at_threshold: chrono::DateTime<chrono::Utc> = DateTime32::from(1_600_000_450).into();
    let past_threshold: chrono::DateTime<chrono::Utc> = DateTime32::from(1_600_000_451).into();

    assert_eq!(
        NetworkUpgrade::minimum_difficulty_spacing_for_height(&network, Height::MIN),
        None
    );
    assert_eq!(
        NetworkUpgrade::minimum_difficulty_spacing_for_height(&network, Height(1))
            .expect("Wcash Testnet minimum difficulty activates at height 1")
            .num_seconds(),
        450
    );
    assert!(!NetworkUpgrade::is_testnet_min_difficulty_block(
        &network,
        Height(1),
        at_threshold,
        previous_time,
    ));
    assert!(NetworkUpgrade::is_testnet_min_difficulty_block(
        &network,
        Height(1),
        past_threshold,
        previous_time,
    ));
    assert!(!network.is_max_block_time_enforced(Height::MIN));
    assert!(network.is_max_block_time_enforced(Height(1)));
    assert!(network.is_max_block_time_enforced(Height::MAX));
}

#[test]
fn wcash_consensus_parameters_and_issuance() -> Result<(), Report> {
    let _init_guard = zebra_test::init();
    let network = Network::new_wcash_regtest();

    assert!(network.uses_wcash_consensus());
    assert!(network.is_regtest());
    assert!(network.is_wcash_regtest());
    assert!(!network.is_wcash_testnet());
    assert_eq!(network.to_string(), "WcashRegtest");
    assert_eq!(network.default_port(), 28233);
    assert_eq!(network.wcash_default_rpc_port(), Some(28232));
    assert_eq!(
        network.magic().0,
        wcash_genesis::network_identity(wcash_genesis::WcashNetwork::Regtest).p2p_magic()
    );
    let genesis_hash = crate::block::genesis::wcash_regtest_genesis_block().hash();
    assert_eq!(network.genesis_hash(), genesis_hash);
    assert_eq!(
        network.checkpoint_list().hash(Height::MIN),
        Some(genesis_hash)
    );
    assert!(
        !network.disable_pow(),
        "Wcash AuxPoW must be checked on Regtest"
    );
    assert_eq!(network.slow_start_interval(), Height::MIN);
    assert_eq!(network.slow_start_shift(), Height::MIN);
    assert!(!network.should_allow_unshielded_coinbase_spends());
    assert!(network.all_funding_streams().is_empty());
    assert!(network.lockbox_disbursements(Height(1)).is_empty());
    assert_eq!(
        network.lockbox_disbursement_total_amount(Height(1)),
        Amount::<NonNegative>::zero()
    );

    assert_eq!(
        NetworkUpgrade::current(&network, Height::MIN),
        NetworkUpgrade::Genesis
    );
    assert_eq!(
        NetworkUpgrade::current(&network, Height(1)),
        NetworkUpgrade::Nu6_3
    );
    // Zebra treats omitted earlier upgrades as implicit activations at the
    // first explicitly configured later upgrade. Protect every historical
    // activation queried by wallet scanning, history-tree, checkpoint, and
    // transaction-invariant code, without pretending that multiple upgrades
    // are separately activated in the canonical height-to-upgrade map.
    for upgrade in [
        NetworkUpgrade::Overwinter,
        NetworkUpgrade::Sapling,
        NetworkUpgrade::Blossom,
        NetworkUpgrade::Heartwood,
        NetworkUpgrade::Canopy,
        NetworkUpgrade::Nu5,
        NetworkUpgrade::Nu6,
        NetworkUpgrade::Nu6_1,
        NetworkUpgrade::Nu6_2,
        NetworkUpgrade::Nu6_3,
    ] {
        assert_eq!(upgrade.activation_height(&network), Some(Height(1)));
    }
    assert_eq!(network.mandatory_checkpoint_height(), Height::MIN);
    assert_eq!(
        NetworkUpgrade::target_spacing_for_height(&network, Height(1)).num_seconds(),
        75
    );
    assert_eq!(
        NetworkUpgrade::target_spacing_for_height(&network, Height::MAX).num_seconds(),
        75
    );
    assert_eq!(WCASH_INITIAL_BLOCK_SUBSIDY, 625_000_000);
    assert_eq!(COIN, 100_000_000, "Wcash must retain Zcash precision");
    assert_eq!(
        WCASH_INITIAL_BLOCK_SUBSIDY,
        u64::try_from(6 * COIN + COIN / 4).unwrap(),
        "the initial subsidy must be exactly 6.25000000 WEC or TWC, according to network"
    );
    assert_eq!(WCASH_HALVING_INTERVAL, 1_680_000);
    assert_eq!(
        u64::try_from(WCASH_HALVING_INTERVAL).unwrap() * 75,
        210_000 * 10 * 60,
        "Wcash and Bitcoin must have the same nominal halving duration"
    );

    let zero = Amount::<NonNegative>::zero();
    let initial_subsidy = Amount::<NonNegative>::try_from(WCASH_INITIAL_BLOCK_SUBSIDY)?;
    let halved_subsidy = Amount::<NonNegative>::try_from(WCASH_INITIAL_BLOCK_SUBSIDY / 2)?;
    assert_eq!(block_subsidy(Height::MIN, &network)?, zero);
    assert_eq!(block_subsidy(Height(1), &network)?, initial_subsidy);
    assert_eq!(block_subsidy(Height(1_680_000), &network)?, initial_subsidy);
    assert_eq!(
        block_subsidy(WCASH_REGTEST_FIRST_HALVING_HEIGHT, &network)?,
        halved_subsidy
    );
    assert_eq!(halving(Height(1_680_000), &network), 0);
    assert_eq!(halving(WCASH_REGTEST_FIRST_HALVING_HEIGHT, &network), 1);
    assert_eq!(
        height_for_halving(1, &network),
        Some(WCASH_REGTEST_FIRST_HALVING_HEIGHT)
    );
    assert_eq!(
        network.height_for_first_halving(),
        WCASH_REGTEST_FIRST_HALVING_HEIGHT
    );
    assert_eq!(
        network.post_blossom_halving_interval(),
        WCASH_HALVING_INTERVAL
    );
    assert_eq!(
        miner_subsidy(Height(1), &network, initial_subsidy)?,
        initial_subsidy
    );
    assert_eq!(founders_reward(&network, Height(1)), zero);
    assert_eq!(founders_reward_address(&network, Height(1)), None);
    assert!(funding_stream_values(Height(1), &network, initial_subsidy)?.is_empty());

    // Integer truncation leaves 1,249,999,989 zatoshi of per-block subsidy across all eras.
    // Each era contains exactly 1,680,000 blocks, so the scheduled total is
    // 20,999,999.81520000 WEC or TWC (2,099,999,981,520,000 zatoshi), according
    // to network. Integer-zatoshi truncation leaves the schedule 0.1848 whole
    // units below the 21 million hard cap.
    let per_block_era_sum: u64 = (0..64)
        .map(|era| WCASH_INITIAL_BLOCK_SUBSIDY.checked_shr(era).unwrap_or(0))
        .sum();
    assert_eq!(per_block_era_sum, 1_249_999_989);
    let scheduled_supply = per_block_era_sum * (WCASH_HALVING_INTERVAL as u64);
    assert_eq!(scheduled_supply, 2_099_999_981_520_000);
    assert!(
        u64::try_from(MAX_MONEY).expect("MAX_MONEY is positive") > scheduled_supply,
        "the Wcash technical amount bound must exceed the legacy Regtest schedule"
    );
    assert!(Amount::<NonNegative>::try_from(MAX_MONEY).is_ok());
    assert!(Amount::<NonNegative>::try_from(MAX_MONEY + 1).is_err());
    assert_eq!(block_subsidy(Height(50_400_000), &network)?.zatoshis(), 1);
    assert_eq!(block_subsidy(Height(50_400_001), &network)?, zero);

    for zcash_network in [
        Network::Mainnet,
        Network::new_default_testnet(),
        Network::new_regtest(Default::default()),
    ] {
        assert!(!zcash_network.uses_wcash_consensus());
    }

    Ok(())
}

#[test]
fn halving_test() -> Result<(), Report> {
    let _init_guard = zebra_test::init();
    for network in Network::iter() {
        halving_for_network(&network)?;
    }

    Ok(())
}

fn halving_for_network(network: &Network) -> Result<(), Report> {
    let blossom_height = NetworkUpgrade::Blossom.activation_height(network).unwrap();
    let first_halving_height = network.height_for_first_halving();

    assert_eq!(
        1,
        halving_divisor((network.slow_start_interval() + 1).unwrap(), network).unwrap()
    );
    assert_eq!(
        1,
        halving_divisor((blossom_height - 1).unwrap(), network).unwrap()
    );
    assert_eq!(1, halving_divisor(blossom_height, network).unwrap());
    assert_eq!(
        1,
        halving_divisor((first_halving_height - 1).unwrap(), network).unwrap()
    );

    assert_eq!(2, halving_divisor(first_halving_height, network).unwrap());
    assert_eq!(
        2,
        halving_divisor((first_halving_height + 1).unwrap(), network).unwrap()
    );

    assert_eq!(
        4,
        halving_divisor(
            (first_halving_height + POST_BLOSSOM_HALVING_INTERVAL).unwrap(),
            network
        )
        .unwrap()
    );
    assert_eq!(
        8,
        halving_divisor(
            (first_halving_height + (POST_BLOSSOM_HALVING_INTERVAL * 2)).unwrap(),
            network
        )
        .unwrap()
    );

    assert_eq!(
        1024,
        halving_divisor(
            (first_halving_height + (POST_BLOSSOM_HALVING_INTERVAL * 9)).unwrap(),
            network
        )
        .unwrap()
    );
    assert_eq!(
        1024 * 1024,
        halving_divisor(
            (first_halving_height + (POST_BLOSSOM_HALVING_INTERVAL * 19)).unwrap(),
            network
        )
        .unwrap()
    );
    assert_eq!(
        1024 * 1024 * 1024,
        halving_divisor(
            (first_halving_height + (POST_BLOSSOM_HALVING_INTERVAL * 29)).unwrap(),
            network
        )
        .unwrap()
    );
    assert_eq!(
        1024 * 1024 * 1024 * 1024,
        halving_divisor(
            (first_halving_height + (POST_BLOSSOM_HALVING_INTERVAL * 39)).unwrap(),
            network
        )
        .unwrap()
    );

    // The largest possible integer divisor
    assert_eq!(
        (i64::MAX as u64 + 1),
        halving_divisor(
            (first_halving_height + (POST_BLOSSOM_HALVING_INTERVAL * 62)).unwrap(),
            network
        )
        .unwrap(),
    );

    // Very large divisors which should also result in zero amounts
    assert_eq!(
        None,
        halving_divisor(
            (first_halving_height + (POST_BLOSSOM_HALVING_INTERVAL * 63)).unwrap(),
            network,
        ),
    );

    assert_eq!(
        None,
        halving_divisor(
            (first_halving_height + (POST_BLOSSOM_HALVING_INTERVAL * 64)).unwrap(),
            network,
        ),
    );

    assert_eq!(
        None,
        halving_divisor(Height(Height::MAX_AS_U32 / 4), network),
    );

    assert_eq!(
        None,
        halving_divisor(Height(Height::MAX_AS_U32 / 2), network),
    );

    assert_eq!(None, halving_divisor(Height::MAX, network));

    Ok(())
}

#[test]
fn block_subsidy_test() -> Result<(), Report> {
    let _init_guard = zebra_test::init();

    for network in Network::iter() {
        block_subsidy_for_network(&network)?;
    }

    Ok(())
}

fn block_subsidy_for_network(network: &Network) -> Result<(), Report> {
    let blossom_height = NetworkUpgrade::Blossom.activation_height(network).unwrap();
    let first_halving_height = network.height_for_first_halving();

    // After slow-start mining and before Blossom the block subsidy is 12.5 ZEC
    // https://z.cash/support/faq/#what-is-slow-start-mining
    assert_eq!(
        Amount::<NonNegative>::try_from(1_250_000_000)?,
        block_subsidy((network.slow_start_interval() + 1).unwrap(), network)?
    );
    assert_eq!(
        Amount::<NonNegative>::try_from(1_250_000_000)?,
        block_subsidy((blossom_height - 1).unwrap(), network)?
    );

    // After Blossom the block subsidy is reduced to 6.25 ZEC without halving
    // https://z.cash/upgrade/blossom/
    assert_eq!(
        Amount::<NonNegative>::try_from(625_000_000)?,
        block_subsidy(blossom_height, network)?
    );

    // After the 1st halving, the block subsidy is reduced to 3.125 ZEC
    // https://z.cash/upgrade/canopy/
    assert_eq!(
        Amount::<NonNegative>::try_from(312_500_000)?,
        block_subsidy(first_halving_height, network)?
    );

    // After the 2nd halving, the block subsidy is reduced to 1.5625 ZEC
    // See "7.8 Calculation of Block Subsidy and Founders' Reward"
    assert_eq!(
        Amount::<NonNegative>::try_from(156_250_000)?,
        block_subsidy(
            (first_halving_height + POST_BLOSSOM_HALVING_INTERVAL).unwrap(),
            network
        )?
    );

    // After the 7th halving, the block subsidy is reduced to 0.04882812 ZEC
    // Check that the block subsidy rounds down correctly, and there are no errors
    assert_eq!(
        Amount::<NonNegative>::try_from(4_882_812)?,
        block_subsidy(
            (first_halving_height + (POST_BLOSSOM_HALVING_INTERVAL * 6)).unwrap(),
            network
        )?
    );

    // After the 29th halving, the block subsidy is 1 zatoshi
    // Check that the block subsidy is calculated correctly at the limit
    assert_eq!(
        Amount::<NonNegative>::try_from(1)?,
        block_subsidy(
            (first_halving_height + (POST_BLOSSOM_HALVING_INTERVAL * 28)).unwrap(),
            network
        )?
    );

    // After the 30th halving, there is no block subsidy
    // Check that there are no errors
    assert_eq!(
        Amount::<NonNegative>::try_from(0)?,
        block_subsidy(
            (first_halving_height + (POST_BLOSSOM_HALVING_INTERVAL * 29)).unwrap(),
            network
        )?
    );

    assert_eq!(
        Amount::<NonNegative>::try_from(0)?,
        block_subsidy(
            (first_halving_height + (POST_BLOSSOM_HALVING_INTERVAL * 39)).unwrap(),
            network
        )?
    );

    assert_eq!(
        Amount::<NonNegative>::try_from(0)?,
        block_subsidy(
            (first_halving_height + (POST_BLOSSOM_HALVING_INTERVAL * 49)).unwrap(),
            network
        )?
    );

    assert_eq!(
        Amount::<NonNegative>::try_from(0)?,
        block_subsidy(
            (first_halving_height + (POST_BLOSSOM_HALVING_INTERVAL * 59)).unwrap(),
            network
        )?
    );

    // The largest possible integer divisor
    assert_eq!(
        Amount::<NonNegative>::try_from(0)?,
        block_subsidy(
            (first_halving_height + (POST_BLOSSOM_HALVING_INTERVAL * 62)).unwrap(),
            network
        )?
    );

    // Other large divisors which should also result in zero
    assert_eq!(
        Amount::<NonNegative>::try_from(0)?,
        block_subsidy(
            (first_halving_height + (POST_BLOSSOM_HALVING_INTERVAL * 63)).unwrap(),
            network
        )?
    );

    assert_eq!(
        Amount::<NonNegative>::try_from(0)?,
        block_subsidy(
            (first_halving_height + (POST_BLOSSOM_HALVING_INTERVAL * 64)).unwrap(),
            network
        )?
    );

    assert_eq!(
        Amount::<NonNegative>::try_from(0)?,
        block_subsidy(Height(Height::MAX_AS_U32 / 4), network)?
    );

    assert_eq!(
        Amount::<NonNegative>::try_from(0)?,
        block_subsidy(Height(Height::MAX_AS_U32 / 2), network)?
    );

    assert_eq!(
        Amount::<NonNegative>::try_from(0)?,
        block_subsidy(Height::MAX, network)?
    );

    Ok(())
}

#[test]
fn check_height_for_num_halvings() {
    for network in Network::iter() {
        for h in 1..1000 {
            let Some(height_for_halving) = height_for_halving(h, &network) else {
                panic!("could not find height for halving {h}");
            };

            let prev_height = height_for_halving
                .previous()
                .expect("there should be a previous height");

            assert_eq!(
                h,
                halving(height_for_halving, &network),
                "num_halvings should match the halving index"
            );

            assert_eq!(
                h - 1,
                halving(prev_height, &network),
                "num_halvings for the prev height should be 1 less than the halving index"
            );
        }
    }
}
