//! Fixed Wcash foundation allocation, independent of Zcash funding streams.

use crate::{
    amount::{Amount, NonNegative},
    block::Height,
    parameters::Network,
    transparent::{Address, Output},
};

use super::SubsidyError;

/// First Mainnet block paying the foundation allocation, inclusive.
pub const WCASH_FOUNDATION_START_HEIGHT: Height = Height(33_000);
/// First Mainnet block after the allocation expires, exclusive.
pub const WCASH_FOUNDATION_EXPIRY_HEIGHT: Height = Height(621_455);
/// Numerator of the fixed foundation share of gross subsidy.
pub const WCASH_FOUNDATION_NUMERATOR: u64 = 3;
/// Denominator of the fixed foundation share of gross subsidy.
pub const WCASH_FOUNDATION_DENOMINATOR: u64 = 10;
/// Fixed Mainnet transparent P2PKH destination for the entire allocation term.
pub const WCASH_FOUNDATION_ADDRESS: &str = "W1ZRudbQKz7jQdKmAarbaJsegscnZzXjpu2";
/// Fixed local test recipient, recovered by the disposable wallet seed `00..1f`.
///
/// This address is used only with `wcash-funding-test-fixture` on Wcash Regtest.
pub const WCASH_FOUNDATION_TEST_ADDRESS: &str = "WRHUq9CTLZFa52NmAyHH5usN21Q8jZVskN1";
/// Whether this dependency graph contains the isolated Regtest funding fixture.
///
/// Executables must inspect this shared value: Cargo can enable dependency
/// features directly without enabling a correspondingly named executable feature.
pub const WCASH_FOUNDATION_TEST_FIXTURE_ENABLED: bool =
    cfg!(feature = "wcash-funding-test-fixture");

/// The one mandatory transparent output for a Wcash candidate block.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WcashFoundationAllocation {
    /// Thirty percent of the gross subsidy, rounded down in atomic units.
    pub amount: Amount<NonNegative>,
    /// The fixed transparent recipient for this network.
    pub address: Address,
}

impl WcashFoundationAllocation {
    /// Construct the exact mandatory transparent output.
    pub fn output(&self) -> Output {
        Output::new(self.amount, self.address.script())
    }
}

/// Return the fixed activation height, including the explicitly enabled local fixture.
pub fn wcash_foundation_activation_height(network: &Network) -> Option<Height> {
    if network.is_wcash_mainnet() {
        Some(WCASH_FOUNDATION_START_HEIGHT)
    } else if WCASH_FOUNDATION_TEST_FIXTURE_ENABLED && network.is_wcash_regtest() {
        Some(Height(10))
    } else {
        None
    }
}

/// Return the first unfunded height, including the explicitly enabled local fixture.
pub fn wcash_foundation_expiry_height(network: &Network) -> Option<Height> {
    if network.is_wcash_mainnet() {
        Some(WCASH_FOUNDATION_EXPIRY_HEIGHT)
    } else if WCASH_FOUNDATION_TEST_FIXTURE_ENABLED && network.is_wcash_regtest() {
        Some(Height(20))
    } else {
        None
    }
}

/// Return the mandatory allocation for a network and candidate height.
///
/// Callers supply the independently computed gross block subsidy. Fees are never
/// included. Mainnet parameters cannot be overridden by runtime configuration.
pub fn wcash_foundation_fund(
    height: Height,
    network: &Network,
    gross_subsidy: Amount<NonNegative>,
) -> Option<WcashFoundationAllocation> {
    if height < wcash_foundation_activation_height(network)?
        || height >= wcash_foundation_expiry_height(network)?
    {
        return None;
    }

    let encoded = if network.is_wcash_mainnet() {
        WCASH_FOUNDATION_ADDRESS
    } else {
        WCASH_FOUNDATION_TEST_ADDRESS
    };
    let gross =
        u128::try_from(gross_subsidy.zatoshis()).expect("a nonnegative amount fits in u128");
    // Widen before multiplying: three times a valid Wcash amount can exceed
    // i64, while its thirty-percent allocation remains within the original bound.
    let amount =
        gross * u128::from(WCASH_FOUNDATION_NUMERATOR) / u128::from(WCASH_FOUNDATION_DENOMINATOR);
    Some(WcashFoundationAllocation {
        amount: Amount::try_from(
            u64::try_from(amount).expect("thirty percent of a nonnegative i64 fits u64"),
        )
        .expect("thirty percent cannot exceed the original amount bound"),
        address: Address::parse_wcash(encoded, network)
            .expect("the fixed foundation address matches its consensus network"),
    })
}

/// Locate one exact mandatory output without changing the serialized transaction.
///
/// Only this occurrence is allocated to the foundation. Additional identical
/// transparent outputs remain part of the miner payout and total value checks.
pub fn wcash_foundation_fund_output_index(
    height: Height,
    network: &Network,
    gross_subsidy: Amount<NonNegative>,
    outputs: &[Output],
) -> Result<Option<usize>, SubsidyError> {
    let Some(allocation) = wcash_foundation_fund(height, network, gross_subsidy) else {
        return Ok(None);
    };
    let required = allocation.output();
    outputs
        .iter()
        .position(|output| *output == required)
        .map(Some)
        .ok_or(SubsidyError::WcashFoundationOutputMissing)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parameters::{subsidy::block_subsidy, NetworkKind};

    fn mainnet() -> Network {
        Network::try_new_wcash_mainnet().expect("the frozen Mainnet parameters are valid")
    }

    #[test]
    fn wcash_foundation_mainnet_recipient_is_frozen_and_network_bound() {
        let network = mainnet();
        let address = Address::parse_wcash(WCASH_FOUNDATION_ADDRESS, &network).unwrap();
        assert_eq!(
            address.encode_wcash(&network).unwrap(),
            WCASH_FOUNDATION_ADDRESS
        );
        assert_eq!(
            hex::encode(address.script().as_raw_bytes()),
            "76a9148708e78a0203f93b2691057281e1773fd263702088ac"
        );
        assert_eq!(address.network_kind(), NetworkKind::Mainnet);
        assert!(!address.is_script_hash());
        for other in [Network::new_wcash_testnet(), Network::new_wcash_regtest()] {
            assert!(Address::parse_wcash(WCASH_FOUNDATION_ADDRESS, &other).is_err());
        }
    }

    #[test]
    fn wcash_foundation_boundaries_and_golden_amounts() {
        let network = mainnet();
        for (height, gross, fund, miner) in [
            (32_999, 1_814_139_209, 0, 1_814_139_209),
            (33_000, 1_814_194_185, 544_258_255, 1_269_935_930),
            (33_001, 1_814_249_160, 544_274_748, 1_269_974_412),
            (39_999, 2_198_968_279, 659_690_483, 1_539_277_796),
            (40_000, 2_199_023_255, 659_706_976, 1_539_316_279),
            (40_001, 2_199_023_255, 659_706_976, 1_539_316_279),
            (40_002, 2_199_020_634, 659_706_190, 1_539_314_444),
            (621_454, 1_099_512_482, 329_853_744, 769_658_738),
            (621_455, 1_099_511_171, 0, 1_099_511_171),
            (621_456, 1_099_509_860, 0, 1_099_509_860),
        ] {
            let height = Height(height);
            let subsidy = block_subsidy(height, &network).unwrap();
            assert_eq!(subsidy.zatoshis(), gross);
            let allocation = wcash_foundation_fund(height, &network, subsidy);
            assert_eq!(
                allocation.map_or(0, |allocation| allocation.amount.zatoshis()),
                fund
            );
            assert_eq!(
                super::super::miner_subsidy(height, &network, subsidy)
                    .unwrap()
                    .zatoshis(),
                miner
            );
            assert_eq!(gross, fund + miner);
        }
    }

    #[test]
    fn wcash_foundation_multiplies_before_rounding_and_preserves_amount_bounds() {
        let network = mainnet();
        for (gross, expected) in [
            (0, 0),
            (1, 0),
            (2, 0),
            (3, 0),
            (4, 1),
            (5, 1),
            (6, 1),
            (7, 2),
            (8, 2),
            (9, 2),
            (10, 3),
            (11, 3),
            (12, 3),
            (13, 3),
            (14, 4),
            (15, 4),
            (16, 4),
            (17, 5),
            (18, 5),
            (19, 5),
            (20, 6),
        ] {
            let subsidy = Amount::<NonNegative>::try_from(gross).unwrap();
            let allocation =
                wcash_foundation_fund(WCASH_FOUNDATION_START_HEIGHT, &network, subsidy).unwrap();
            assert_eq!(allocation.amount.zatoshis(), expected);
            assert_eq!(
                (allocation.amount
                    + super::super::miner_subsidy(
                        WCASH_FOUNDATION_START_HEIGHT,
                        &network,
                        subsidy,
                    )
                    .unwrap())
                .unwrap(),
                subsidy
            );
        }
        let maximum = Amount::<NonNegative>::try_from(crate::amount::MAX_MONEY).unwrap();
        #[cfg(feature = "wcash-consensus")]
        let expected = 1_383_505_805_528_216_370;
        #[cfg(not(feature = "wcash-consensus"))]
        let expected = 630_000_000_000_000;
        let allocation =
            wcash_foundation_fund(WCASH_FOUNDATION_START_HEIGHT, &network, maximum).unwrap();
        assert_eq!(allocation.amount.zatoshis(), expected);
        #[cfg(feature = "wcash-consensus")]
        assert!(maximum.zatoshis().checked_mul(3).is_none());
        assert_eq!(
            (allocation.amount
                + super::super::miner_subsidy(WCASH_FOUNDATION_START_HEIGHT, &network, maximum,)
                    .unwrap())
            .unwrap(),
            maximum
        );
    }

    #[test]
    fn wcash_foundation_independent_recurrence_proves_first_half_reward_and_total() {
        // Deliberately use the reviewed recurrence's literal constants and the
        // first-half predicate, rather than production endpoint or subsidy helpers.
        let mut emitted = 0u128;
        let mut height = 40_001u32;
        let mut total_gross = 0u128;
        let mut total_fund = 0u128;
        for height in 33_000u128..=40_000 {
            let reward = 2_199_023_255 * height / 40_000;
            total_gross += reward;
            total_fund += reward * 3 / 10;
        }
        assert_eq!(total_gross, 14_048_267_646_534);
        assert_eq!(total_fund, 4_214_480_290_741);
        let mut preceding = 0;
        loop {
            let reward = 5 * (1_844_674_407_370_955u128 - emitted) / 4_194_304;
            if reward * 2 <= 2_199_023_255 {
                assert_eq!(height, 621_455);
                assert_eq!(Height(height), WCASH_FOUNDATION_EXPIRY_HEIGHT);
                assert_eq!(preceding, 1_099_512_482);
                assert_eq!(reward, 1_099_511_171);
                assert!(preceding * 2 > 2_199_023_255);
                break;
            }
            preceding = reward;
            total_gross += reward;
            total_fund += reward * 3 / 10;
            emitted += reward;
            height += 1;
        }
        assert_eq!(height - 33_000, 588_455);
        assert_eq!(emitted, 922_337_586_294_287);
        assert_eq!(total_gross, 936_385_853_940_821);
        assert_eq!(total_fund, 280_915_755_917_299);
        assert_eq!(total_gross - total_fund, 655_470_098_023_522);
        assert_ne!(total_gross * 3 / 10, total_fund);
    }

    #[test]
    fn wcash_foundation_does_not_change_curve_counter_or_include_fees() {
        use super::super::wcash_mainnet::{advance_curve_counter, validate_and_advance};

        let network = mainnet();
        for (height, before, gross, after, expected_fund) in [
            (33_000, 0, 1_814_194_185u64, 0, 544_258_255),
            (39_999, 0, 2_198_968_279, 0, 659_690_483),
            (40_000, 0, 2_199_023_255, 0, 659_706_976),
            (40_001, 0, 2_199_023_255u64, 2_199_023_255, 659_706_976),
            (
                40_002,
                2_199_023_255,
                2_199_020_634,
                4_398_043_889,
                659_706_190,
            ),
        ] {
            let subsidy = Amount::try_from(gross).unwrap();
            let fund = wcash_foundation_fund(Height(height), &network, subsidy).unwrap();
            let fees = 123_457;
            let miner =
                ((subsidy - fund.amount).unwrap() + Amount::try_from(fees).unwrap()).unwrap();
            assert_eq!(
                (fund.amount + miner).unwrap(),
                Amount::<NonNegative>::try_from(gross + fees).unwrap()
            );
            assert_eq!(advance_curve_counter(height, before, gross), Ok(after));
            assert_eq!(
                validate_and_advance(height, before, fees, gross + fees),
                Ok(after)
            );
            assert_eq!(fund.amount.zatoshis(), expected_fund);
        }
    }

    #[test]
    fn wcash_foundation_exact_matching_removes_only_one_occurrence() {
        let network = mainnet();
        let height = WCASH_FOUNDATION_START_HEIGHT;
        let gross = block_subsidy(height, &network).unwrap();
        let allocation = wcash_foundation_fund(height, &network, gross).unwrap();
        let required = allocation.output();
        assert_eq!(
            wcash_foundation_fund_output_index(height, &network, gross, &[]),
            Err(SubsidyError::WcashFoundationOutputMissing)
        );
        let wrong_script = Output::new(
            required.value,
            Address::from_pub_key_hash(network.kind(), [7; 20]).script(),
        );
        for invalid in [
            wrong_script,
            Output::new(
                (required.value - Amount::try_from(1).unwrap()).unwrap(),
                required.lock_script.clone(),
            ),
            Output::new(
                (required.value + Amount::try_from(1).unwrap()).unwrap(),
                required.lock_script.clone(),
            ),
            Output::new(gross, required.lock_script.clone()),
            Output::new((gross / 5).unwrap(), required.lock_script.clone()),
        ] {
            assert_eq!(
                wcash_foundation_fund_output_index(height, &network, gross, &[invalid]),
                Err(SubsidyError::WcashFoundationOutputMissing)
            );
        }
        let miner = Output::new(
            (gross - allocation.amount).unwrap(),
            required.lock_script.clone(),
        );
        assert_eq!(
            wcash_foundation_fund_output_index(
                height,
                &network,
                gross,
                &[miner.clone(), required.clone(), required.clone()]
            ),
            Ok(Some(1))
        );
        assert_eq!(
            wcash_foundation_fund_output_index(height, &network, gross, &[required.clone(), miner]),
            Ok(Some(0))
        );
        assert_eq!(
            wcash_foundation_fund_output_index(
                WCASH_FOUNDATION_EXPIRY_HEIGHT,
                &network,
                gross,
                &[]
            ),
            Ok(None)
        );
    }

    #[test]
    fn wcash_foundation_other_profiles_are_unchanged() {
        for network in [
            Network::Mainnet,
            Network::new_default_testnet(),
            Network::new_wcash_testnet(),
        ] {
            assert_eq!(wcash_foundation_activation_height(&network), None);
            for height in [Height(10), Height(33_000), Height(40_001), Height(621_454)] {
                assert_eq!(
                    wcash_foundation_fund(height, &network, Amount::try_from(625_000_000).unwrap()),
                    None
                );
            }
        }
        if !cfg!(feature = "wcash-funding-test-fixture") {
            let network = Network::new_wcash_regtest();
            assert_eq!(wcash_foundation_activation_height(&network), None);
            assert_eq!(wcash_foundation_expiry_height(&network), None);
        }
    }

    #[cfg(feature = "wcash-funding-test-fixture")]
    #[test]
    fn wcash_foundation_fixture_uses_shared_policy_and_wallet_recipient() {
        let network = Network::new_wcash_regtest();
        assert_eq!(
            wcash_foundation_activation_height(&network),
            Some(Height(10))
        );
        assert_eq!(wcash_foundation_expiry_height(&network), Some(Height(20)));
        let mut paid = 0;
        for height in [9, 10, 11, 19, 20, 21] {
            let height = Height(height);
            let gross = block_subsidy(height, &network).unwrap();
            let allocation = wcash_foundation_fund(height, &network, gross);
            if (10..20).contains(&height.0) {
                let allocation = allocation.unwrap();
                assert_eq!(allocation.amount.zatoshis(), 187_500_000);
                assert_eq!(
                    allocation.address.encode_wcash(&network).unwrap(),
                    WCASH_FOUNDATION_TEST_ADDRESS
                );
                assert_eq!(
                    hex::encode(allocation.address.script().as_raw_bytes()),
                    "76a9147d437bac720b610692ae373e43fa654c50c196f288ac"
                );
                assert_eq!(
                    super::super::miner_subsidy(height, &network, gross)
                        .unwrap()
                        .zatoshis(),
                    437_500_000
                );
            } else {
                assert!(allocation.is_none());
            }
        }
        for height in 10..20 {
            let height = Height(height);
            paid +=
                wcash_foundation_fund(height, &network, block_subsidy(height, &network).unwrap())
                    .unwrap()
                    .amount
                    .zatoshis();
        }
        assert_eq!(paid, 1_875_000_000);
    }
}
