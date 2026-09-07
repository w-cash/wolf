#![allow(clippy::unwrap_in_result)]

mod prop;
mod vectors;

use color_eyre::Report;

use super::Network;
use crate::{
    amount::{Amount, NonNegative, MAX_MONEY},
    block::Height,
    parameters::{
        subsidy::{
            block_subsidy, constants::POST_BLOSSOM_HALVING_INTERVAL, founders_reward,
            founders_reward_address, funding_stream_values, halving, halving_divisor,
            height_for_halving, miner_subsidy, ParameterSubsidy as _, WCASH_FIRST_HALVING_HEIGHT,
            WCASH_HALVING_INTERVAL, WCASH_INITIAL_BLOCK_SUBSIDY,
        },
        NetworkUpgrade,
    },
};

#[test]
fn compiled_consensus_profile_is_mutually_exclusive() {
    let zcash = Network::Mainnet;
    let wcash = Network::new_wcash_regtest();

    assert_eq!(
        zcash.is_compatible_with_compiled_consensus(),
        !cfg!(feature = "wcash-consensus")
    );
    assert_eq!(
        wcash.is_compatible_with_compiled_consensus(),
        cfg!(feature = "wcash-consensus")
    );

    if cfg!(feature = "wcash-consensus") {
        wcash.assert_compatible_with_compiled_consensus();
        assert!(
            std::panic::catch_unwind(|| zcash.assert_compatible_with_compiled_consensus()).is_err()
        );
    } else {
        zcash.assert_compatible_with_compiled_consensus();
        assert!(
            std::panic::catch_unwind(|| wcash.assert_compatible_with_compiled_consensus()).is_err()
        );
    }
}

#[test]
fn wcash_consensus_parameters_and_issuance() -> Result<(), Report> {
    let _init_guard = zebra_test::init();
    let network = Network::new_wcash_regtest();

    assert!(network.uses_wcash_consensus());
    assert!(network.is_regtest());
    assert_eq!(network.to_string(), "Wcash");
    assert_eq!(network.default_port(), 28233);
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
    // first explicitly configured later upgrade. These assertions protect the
    // history-tree, mandatory-checkpoint, and transaction-invariant callers
    // that require Heartwood and Canopy activation heights to exist.
    assert_eq!(
        NetworkUpgrade::Heartwood.activation_height(&network),
        Some(Height(1))
    );
    assert_eq!(
        NetworkUpgrade::Canopy.activation_height(&network),
        Some(Height(1))
    );
    assert_eq!(network.mandatory_checkpoint_height(), Height::MIN);
    assert_eq!(
        NetworkUpgrade::target_spacing_for_height(&network, Height(1)).num_seconds(),
        75
    );
    assert_eq!(
        NetworkUpgrade::target_spacing_for_height(&network, Height::MAX).num_seconds(),
        75
    );

    let zero = Amount::<NonNegative>::zero();
    let ten = Amount::<NonNegative>::try_from(WCASH_INITIAL_BLOCK_SUBSIDY)?;
    let five = Amount::<NonNegative>::try_from(WCASH_INITIAL_BLOCK_SUBSIDY / 2)?;
    assert_eq!(block_subsidy(Height::MIN, &network)?, zero);
    assert_eq!(block_subsidy(Height(1), &network)?, ten);
    assert_eq!(block_subsidy(Height(1_680_000), &network)?, ten);
    assert_eq!(block_subsidy(WCASH_FIRST_HALVING_HEIGHT, &network)?, five);
    assert_eq!(halving(Height(1_680_000), &network), 0);
    assert_eq!(halving(WCASH_FIRST_HALVING_HEIGHT, &network), 1);
    assert_eq!(
        height_for_halving(1, &network),
        Some(WCASH_FIRST_HALVING_HEIGHT)
    );
    assert_eq!(
        network.height_for_first_halving(),
        WCASH_FIRST_HALVING_HEIGHT
    );
    assert_eq!(
        network.post_blossom_halving_interval(),
        WCASH_HALVING_INTERVAL
    );
    assert_eq!(miner_subsidy(Height(1), &network, ten)?, ten);
    assert_eq!(founders_reward(&network, Height(1)), zero);
    assert_eq!(founders_reward_address(&network, Height(1)), None);
    assert!(funding_stream_values(Height(1), &network, ten)?.is_empty());

    // Integer truncation leaves 1,999,999,987 zatoshi of per-block subsidy across all eras.
    // Each era contains exactly 1,680,000 blocks, so the scheduled total is
    // 33,599,999.78160000 WCASH (3,359,999,978,160,000 zatoshi).
    let per_block_era_sum: u64 = (0..64)
        .map(|era| WCASH_INITIAL_BLOCK_SUBSIDY.checked_shr(era).unwrap_or(0))
        .sum();
    assert_eq!(per_block_era_sum, 1_999_999_987);
    let scheduled_supply = per_block_era_sum * (WCASH_HALVING_INTERVAL as u64);
    assert_eq!(scheduled_supply, 3_359_999_978_160_000);

    #[cfg(feature = "wcash-consensus")]
    {
        assert_eq!(
            MAX_MONEY,
            i64::try_from(scheduled_supply).expect("the scheduled Wcash supply fits in i64")
        );
        assert!(Amount::<NonNegative>::try_from(MAX_MONEY).is_ok());
        assert!(Amount::<NonNegative>::try_from(MAX_MONEY + 1).is_err());
    }

    #[cfg(not(feature = "wcash-consensus"))]
    {
        assert_eq!(MAX_MONEY, 21_000_000 * crate::amount::COIN);
        assert!(scheduled_supply > u64::try_from(MAX_MONEY).expect("MAX_MONEY is positive"));
        assert!(Amount::<NonNegative>::try_from(
            i64::try_from(scheduled_supply).expect("the scheduled Wcash supply fits in i64")
        )
        .is_err());
    }
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
