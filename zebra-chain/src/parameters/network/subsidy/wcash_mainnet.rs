//! Wcash Mainnet emission v2, option B.
//!
//! Consensus uses integer atomic units throughout. Slow-start issuance is real
//! supply but is deliberately excluded from the post-ramp curve counter.
//!
//! After slow start, Wcash uses a rate-scaled adaptation of Monero's
//! unpenalized base reward, rather than a Monero consensus rule for a new
//! target: `floor(5 * (M - emitted) / 2^22)`. Monero implements 60- and
//! 120-second targets with shifts 20 and 19; Wcash's 75-second target therefore
//! requires this exact rational form. The 0.3-coin-per-minute tail is likewise
//! scaled to 0.375 WEC per target block. Wcash does not adopt Monero's
//! block-weight penalty.
//!
//! Monero sources:
//! <https://github.com/monero-project/monero/blob/9e3a31032ee2cf3cb65c908e107a9952d03bbc4f/src/cryptonote_config.h#L52-L80>
//! <https://github.com/monero-project/monero/blob/9e3a31032ee2cf3cb65c908e107a9952d03bbc4f/src/cryptonote_basic/cryptonote_basic_impl.cpp#L81-L125>

use std::sync::OnceLock;

use crate::{
    amount::{Amount, NonNegative},
    block::Height,
};

use super::SubsidyError;

/// Atomic units per WEC.
pub const COIN: u64 = 100_000_000;
/// Permanent target block spacing in seconds.
pub const TARGET_SECONDS: u64 = 75;
/// Last block in the linear slow-start ramp.
pub const SLOW_START_BLOCKS: u32 = 40_000;
/// `floor((2^64 - 1) / 10_000)`, converting Monero's 12-decimal
/// remaining-emission reference scale to eight-decimal WEC atoms.
///
/// This emission parameter is not a supply cap.
pub const MAIN_EMISSION_SCALE: u64 = 1_844_674_407_370_955;
/// Smooth-curve rational numerator.
pub const EMISSION_NUMERATOR: u64 = 5;
/// Smooth-curve rational denominator.
pub const EMISSION_DENOMINATOR: u64 = 4_194_304;
/// Permanent tail subsidy in atomic units (0.375 WEC).
pub const TAIL_SUBSIDY: u64 = 37_500_000;
/// Full slow-start peak and first smooth reward (21.99023255 WEC).
pub const RAMP_PEAK: u64 = 2_199_023_255;
/// First canonical block whose smooth reward reaches the tail floor.
pub const FIRST_TAIL_HEIGHT: u32 = 3_455_360;

/// Errors returned by the stateful emission recurrence.
#[derive(thiserror::Error, Clone, Copy, Debug, Eq, PartialEq)]
pub enum EmissionError {
    /// The curve counter exceeds its reference scale.
    #[error("Wcash curve counter exceeds the main emission scale")]
    CounterOutOfRange,
    /// Genesis or slow start was evaluated with a non-zero curve counter.
    #[error("Wcash curve counter must be zero through slow start")]
    CounterBeforeCurve,
    /// Genesis included transaction fees.
    #[error("Wcash genesis must have zero fees")]
    GenesisFees,
    /// Subsidy plus fees overflowed the fixed amount representation.
    #[error("Wcash subsidy plus fees overflowed")]
    AmountOverflow,
    /// Coinbase did not claim exactly subsidy plus fees.
    #[error("Wcash coinbase must claim exactly subsidy plus fees")]
    IncorrectCoinbaseClaim,
}

/// Return the exact subsidy for `height` and its parent branch's curve counter.
///
/// `curve_emitted_before` excludes every slow-start reward and is capped at
/// [`MAIN_EMISSION_SCALE`]. Multiplication uses `u128` before division.
pub fn subsidy_atoms(height: u32, curve_emitted_before: u64) -> Result<u64, EmissionError> {
    if curve_emitted_before > MAIN_EMISSION_SCALE {
        return Err(EmissionError::CounterOutOfRange);
    }

    if height <= SLOW_START_BLOCKS {
        if curve_emitted_before != 0 {
            return Err(EmissionError::CounterBeforeCurve);
        }

        // Wcash-specific ramp: floor(RAMP_PEAK * height / 40_000), for
        // heights 0 through 40,000 inclusive. It adopts Zcash's slow-start
        // rationale, but not Zcash's consensus formula: Zcash uses rate *
        // height before the midpoint and rate * (height + 1) afterward,
        // skipping the midpoint payout to compensate its later schedule.
        // Under Wcash option B, ramp issuance does not reduce the post-ramp
        // curve counter.
        //
        // Formula: <https://zips.z.cash/zip-0208#specification>
        // Rationale: <https://github.com/zcash/zcash/issues/762>
        return u64::try_from(
            u128::from(RAMP_PEAK) * u128::from(height) / u128::from(SLOW_START_BLOCKS),
        )
        .map_err(|_| EmissionError::AmountOverflow);
    }

    let remaining = MAIN_EMISSION_SCALE - curve_emitted_before;
    let curve_reward = u64::try_from(
        u128::from(EMISSION_NUMERATOR) * u128::from(remaining) / u128::from(EMISSION_DENOMINATOR),
    )
    .map_err(|_| EmissionError::AmountOverflow)?;

    Ok(curve_reward.max(TAIL_SUBSIDY))
}

/// Advance the capped curve counter after an exactly validated block.
pub fn advance_curve_counter(
    height: u32,
    curve_emitted_before: u64,
    subsidy: u64,
) -> Result<u64, EmissionError> {
    if curve_emitted_before > MAIN_EMISSION_SCALE {
        return Err(EmissionError::CounterOutOfRange);
    }

    if height <= SLOW_START_BLOCKS {
        if curve_emitted_before != 0 {
            return Err(EmissionError::CounterBeforeCurve);
        }
        return Ok(0);
    }

    Ok(curve_emitted_before
        .checked_add(subsidy)
        .ok_or(EmissionError::AmountOverflow)?
        .min(MAIN_EMISSION_SCALE))
}

/// Validate an exact coinbase claim and return the next capped curve counter.
///
/// Fees transfer existing value and therefore do not alter the emission
/// counter. The node verifier independently computes `coinbase_value` across
/// all enabled transparent and shielded value components.
pub fn validate_and_advance(
    height: u32,
    curve_emitted_before: u64,
    fees: u64,
    coinbase_value: u64,
) -> Result<u64, EmissionError> {
    if height == 0 && fees != 0 {
        return Err(EmissionError::GenesisFees);
    }

    let subsidy = subsidy_atoms(height, curve_emitted_before)?;
    let expected = subsidy
        .checked_add(fees)
        .ok_or(EmissionError::AmountOverflow)?;
    if coinbase_value != expected {
        return Err(EmissionError::IncorrectCoinbaseClaim);
    }

    advance_curve_counter(height, curve_emitted_before, subsidy)
}

/// Return the canonical height-derived Wcash Mainnet subsidy.
///
/// Exact coinbase claims make the counter deterministic by height. The table is
/// built once from the normative recurrence, then mining templates and block
/// validation both use O(1) lookups. Heights at and after the first tail block
/// return the permanent tail directly.
pub(super) fn block_subsidy(height: Height) -> Result<Amount<NonNegative>, SubsidyError> {
    static PRE_TAIL_SUBSIDIES: OnceLock<Vec<u32>> = OnceLock::new();

    let subsidy = if height.0 >= FIRST_TAIL_HEIGHT {
        TAIL_SUBSIDY
    } else {
        let subsidies = PRE_TAIL_SUBSIDIES.get_or_init(build_pre_tail_subsidies);
        u64::from(subsidies[height.0 as usize])
    };

    Ok(Amount::try_from(subsidy)?)
}

fn build_pre_tail_subsidies() -> Vec<u32> {
    let mut subsidies = Vec::with_capacity(FIRST_TAIL_HEIGHT as usize);
    let mut curve_counter = 0;

    for height in 0..FIRST_TAIL_HEIGHT {
        let subsidy =
            subsidy_atoms(height, curve_counter).expect("the canonical emission counter is valid");
        subsidies.push(
            u32::try_from(subsidy).expect("Wcash subsidies fit in an unsigned 32-bit integer"),
        );
        curve_counter = advance_curve_counter(height, curve_counter, subsidy)
            .expect("the canonical emission counter is valid");
    }

    debug_assert_eq!(subsidies.len(), FIRST_TAIL_HEIGHT as usize);
    debug_assert_eq!(
        subsidy_atoms(FIRST_TAIL_HEIGHT, curve_counter),
        Ok(TAIL_SUBSIDY)
    );
    subsidies
}

#[cfg(test)]
mod tests {
    use super::*;

    const CANONICAL: &[(u32, u64, u64, u64, u64)] = &[
        (0, 0, 0, 0, 0),
        (1, 0, 54_975, 0, 54_975),
        (2, 0, 109_951, 0, 164_926),
        (100, 0, 5_497_558, 0, 277_626_635),
        (1_000, 0, 54_975_581, 0, 27_515_277_977),
        (10_000, 0, 549_755_813, 0, 2_749_053_941_655),
        (20_000, 0, 1_099_511_627, 0, 10_995_666_020_811),
        (39_999, 0, 2_198_968_279, 0, 43_979_365_568_375),
        (40_000, 0, 2_199_023_255, 0, 43_981_564_591_630),
        (40_001, 0, 2_199_023_255, 2_199_023_255, 43_983_763_614_885),
        (
            40_002,
            2_199_023_255,
            2_199_020_634,
            4_398_043_889,
            43_985_962_635_519,
        ),
        (
            420_480,
            672_645_675_686_408,
            1_397_167_124,
            672_647_072_853_532,
            716_628_637_445_162,
        ),
        (
            840_960,
            1_134_692_160_236_742,
            846_364_792,
            1_134_693_006_601_534,
            1_178_674_571_193_164,
        ),
        (
            1_261_440,
            1_414_587_006_187_917,
            512_704_135,
            1_414_587_518_892_052,
            1_458_569_083_483_682,
        ),
        (
            1_681_920,
            1_584_139_480_688_572,
            310_581_835,
            1_584_139_791_270_407,
            1_628_121_355_862_037,
        ),
        (
            2_102_400,
            1_686_849_630_679_903,
            188_141_795,
            1_686_849_818_821_698,
            1_730_831_383_413_328,
        ),
        (
            2_522_880,
            1_749_068_568_753_820,
            113_971_040,
            1_749_068_682_724_860,
            1_793_050_247_316_490,
        ),
        (
            2_943_360,
            1_786_759_062_394_175,
            69_040_471,
            1_786_759_131_434_646,
            1_830_740_696_026_276,
        ),
        (
            3_363_840,
            1_809_590_909_976_675,
            41_822_787,
            1_809_590_951_799_462,
            1_853_572_516_391_092,
        ),
        (
            3_455_359,
            1_813_217_089_246_204,
            37_500_045,
            1_813_217_126_746_249,
            1_857_198_691_337_879,
        ),
        (
            3_455_360,
            1_813_217_126_746_249,
            37_500_000,
            1_813_217_164_246_249,
            1_857_198_728_837_879,
        ),
        (
            3_455_361,
            1_813_217_164_246_249,
            37_500_000,
            1_813_217_201_746_249,
            1_857_198_766_337_879,
        ),
        (
            3_784_320,
            1_825_553_126_746_249,
            37_500_000,
            1_825_553_164_246_249,
            1_869_534_728_837_879,
        ),
        (
            4_204_800,
            1_841_321_126_746_249,
            37_500_000,
            1_841_321_164_246_249,
            1_885_302_728_837_879,
        ),
        (
            4_625_280,
            MAIN_EMISSION_SCALE,
            37_500_000,
            MAIN_EMISSION_SCALE,
            1_901_070_728_837_879,
        ),
        (
            5_045_760,
            MAIN_EMISSION_SCALE,
            37_500_000,
            MAIN_EMISSION_SCALE,
            1_916_838_728_837_879,
        ),
    ];

    #[test]
    fn constants_and_synthetic_vectors_match_specification() {
        assert_eq!(MAIN_EMISSION_SCALE, (u64::MAX / 10_000));
        assert_eq!(TAIL_SUBSIDY * 1_152, 432 * COIN);
        assert_eq!(TAIL_SUBSIDY * 420_480, 157_680 * COIN);
        assert_eq!(u64::from(SLOW_START_BLOCKS) * TARGET_SECONDS, 3_000_000);
        assert_eq!(subsidy_atoms(40_001, 0), Ok(2_199_023_255));

        for (counter, expected) in [
            (0, 2_199_023_255),
            (922_337_203_685_477, 1_099_511_627),
            (1_813_217_126_532_094, 37_500_001),
            (1_813_217_126_532_095, 37_500_000),
            (1_813_217_126_532_096, 37_500_000),
            (MAIN_EMISSION_SCALE - 1, 37_500_000),
            (MAIN_EMISSION_SCALE, 37_500_000),
        ] {
            assert_eq!(subsidy_atoms(40_001, counter), Ok(expected));
        }
    }

    #[test]
    fn complete_twelve_year_schedule_matches_every_canonical_vector() {
        let mut curve_counter = 0;
        let mut total_issued = 0;
        let mut first_tail = None;
        let mut vector_index = 0;
        let mut previous_curve_reward = RAMP_PEAK;

        for height in 0..=5_045_760 {
            let before = curve_counter;
            let subsidy = subsidy_atoms(height, before).expect("canonical state is valid");
            curve_counter =
                advance_curve_counter(height, before, subsidy).expect("canonical state is valid");
            total_issued += subsidy;

            if height > SLOW_START_BLOCKS {
                assert!(subsidy <= previous_curve_reward);
                if first_tail.is_none() && subsidy == TAIL_SUBSIDY {
                    first_tail = Some(height);
                }
                previous_curve_reward = subsidy;
            }

            if let Some(&(
                expected_height,
                expected_before,
                expected_subsidy,
                expected_after,
                expected_total,
            )) = CANONICAL.get(vector_index)
            {
                if height == expected_height {
                    assert_eq!(before, expected_before, "counter before at height {height}");
                    assert_eq!(subsidy, expected_subsidy, "subsidy at height {height}");
                    assert_eq!(
                        curve_counter, expected_after,
                        "counter after at height {height}"
                    );
                    assert_eq!(total_issued, expected_total, "total at height {height}");
                    vector_index += 1;
                }
            }
        }

        assert_eq!(vector_index, CANONICAL.len());
        assert_eq!(first_tail, Some(FIRST_TAIL_HEIGHT));
        assert_eq!(curve_counter, MAIN_EMISSION_SCALE);
    }

    #[test]
    fn ramp_and_full_claim_boundaries_are_exact() {
        let mut ramp_total = 0;
        let mut previous = 0;
        for height in 1..=SLOW_START_BLOCKS {
            let subsidy = subsidy_atoms(height, 0).expect("ramp state is valid");
            assert!(subsidy >= previous);
            ramp_total += subsidy;
            previous = subsidy;
        }
        assert_eq!(ramp_total, 43_981_564_591_630);
        assert_eq!(subsidy_atoms(40_000, 0), Ok(RAMP_PEAK));
        assert_eq!(subsidy_atoms(40_001, 0), Ok(RAMP_PEAK));
        assert_eq!(subsidy_atoms(40_002, RAMP_PEAK), Ok(2_199_020_634));

        let fee = 123_456_789;
        let claim = RAMP_PEAK + fee;
        assert_eq!(validate_and_advance(40_001, 0, fee, claim), Ok(RAMP_PEAK));
        assert_eq!(
            validate_and_advance(40_001, 0, fee, claim - 1),
            Err(EmissionError::IncorrectCoinbaseClaim)
        );
        assert_eq!(
            validate_and_advance(40_001, 0, fee, claim + 1),
            Err(EmissionError::IncorrectCoinbaseClaim)
        );
        assert_eq!(
            validate_and_advance(0, 0, 1, 1),
            Err(EmissionError::GenesisFees)
        );
    }

    #[test]
    fn invalid_and_saturated_counters_are_safe() {
        assert_eq!(subsidy_atoms(1, 1), Err(EmissionError::CounterBeforeCurve));
        assert_eq!(
            subsidy_atoms(40_001, MAIN_EMISSION_SCALE + 1),
            Err(EmissionError::CounterOutOfRange)
        );
        assert_eq!(
            validate_and_advance(4_000_000, MAIN_EMISSION_SCALE, 0, TAIL_SUBSIDY),
            Ok(MAIN_EMISSION_SCALE)
        );
        assert_eq!(
            validate_and_advance(40_001, 0, u64::MAX, u64::MAX),
            Err(EmissionError::AmountOverflow)
        );
    }

    #[test]
    fn canonical_lookup_uses_the_same_recurrence() {
        for &(height, _, subsidy, _, _) in CANONICAL {
            assert_eq!(
                block_subsidy(Height(height))
                    .expect("canonical subsidy fits")
                    .zatoshis(),
                i64::try_from(subsidy).expect("subsidy fits in i64")
            );
        }
    }
}
