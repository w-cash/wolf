//! Construction of the exact parent coinbase transaction used by local jobs.

use std::io::Cursor;

use wcash_zcash_aux::{commitment_payload, validate_miner_data_commitment, AuxPowError};
use zebra_chain::{
    amount::{Amount, NonNegative},
    block::Height,
    parameters::NetworkUpgrade,
    serialization::{ZcashDeserialize, ZcashSerialize},
    transaction::{LockTime, Transaction},
    transparent::{Input, Output, Script},
};

use crate::MinerError;

/// Zatoshis in the fixed 21-million-ZEC parent-chain monetary range.
const MAX_PARENT_MONEY: u64 = 21_000_000 * 100_000_000;

/// One optional non-commitment transparent output in a synthetic local coinbase.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParentOutput {
    /// Output value in zatoshis.
    pub value_zatoshis: u64,
    /// Exact scriptPubKey bytes, without a CompactSize length prefix.
    pub script_pubkey: Vec<u8>,
}

/// Canonical data read back from a locally constructed parent coinbase.
pub(crate) struct CanonicalCoinbase {
    pub(crate) transaction_id: [u8; 32],
    pub(crate) authorizing_data_digest: [u8; 32],
    pub(crate) miner_data: Vec<u8>,
}

/// Constructs a canonical NU6.3/v6 Zcash coinbase carrying one Wcash commitment.
///
/// The v2 commitment is appended to the coinbase miner data, which is covered
/// by ZIP-244's authorizing-data root without changing the v5/v6 transaction
/// ID. Supplied outputs are preserved exactly. This synthetic helper proves
/// structural canonicality only; a native production job must derive its full
/// coinbase and funding-stream outputs from the parent `getblocktemplate`.
pub fn build_parent_coinbase(
    height: u32,
    extra_data: &[u8],
    parent_outputs: &[ParentOutput],
    auxiliary_block_hash: [u8; 32],
    auxiliary_nonce: u32,
) -> Result<Vec<u8>, MinerError> {
    if height == 0 {
        return Err(MinerError::ZeroParentHeight);
    }

    let auxiliary_branch = [];
    let mut miner_data = extra_data.to_vec();
    miner_data.extend_from_slice(&commitment_payload(
        auxiliary_block_hash,
        &auxiliary_branch,
        0,
        auxiliary_nonce,
    )?);

    let coinbase_input = Input::Coinbase {
        height: Height(height),
        data: miner_data,
        sequence: u32::MAX,
    };
    let coinbase_script_len = coinbase_input
        .coinbase_script()
        .map(|script| script.len())
        .ok_or(MinerError::CoinbaseDataTooLong)?;
    if !(2..=100).contains(&coinbase_script_len) {
        return Err(MinerError::CoinbaseDataTooLong);
    }

    let mut total = 0u64;
    let mut outputs = Vec::with_capacity(parent_outputs.len());
    for output in parent_outputs {
        if output.value_zatoshis > MAX_PARENT_MONEY {
            return Err(MinerError::InvalidParentAmount(output.value_zatoshis));
        }
        total = total
            .checked_add(output.value_zatoshis)
            .ok_or(MinerError::ParentOutputTotalTooLarge)?;
        if total > MAX_PARENT_MONEY {
            return Err(MinerError::ParentOutputTotalTooLarge);
        }
        let amount = Amount::<NonNegative>::try_from(output.value_zatoshis)
            .map_err(|_| MinerError::InvalidParentAmount(output.value_zatoshis))?;
        outputs.push(Output::new(amount, Script::new(&output.script_pubkey)));
    }

    let transaction = Transaction::V6 {
        network_upgrade: NetworkUpgrade::Nu6_3,
        lock_time: LockTime::unlocked(),
        expiry_height: Height(height),
        inputs: vec![coinbase_input],
        outputs,
        sapling_shielded_data: None,
        orchard_shielded_data: None,
        ironwood_shielded_data: None,
    };

    let bytes = transaction
        .zcash_serialize_to_vec()
        .map_err(MinerError::CoinbaseSerialization)?;

    // Read the result back through Zebra rather than trusting construction.
    // This is template hygiene only; final work still goes through the pinned
    // production parser inside `AuxPowProof::validate`.
    let summary = canonical_parent_coinbase(&bytes)?;
    validate_miner_data_commitment(
        &summary.miner_data,
        auxiliary_block_hash,
        &auxiliary_branch,
        0,
    )?;

    Ok(bytes)
}

/// Parses one local coinbase with Zebra and requires an exact canonical round trip.
pub(crate) fn canonical_parent_coinbase(
    serialized_coinbase: &[u8],
) -> Result<CanonicalCoinbase, MinerError> {
    let mut cursor = Cursor::new(serialized_coinbase);
    let transaction = Transaction::zcash_deserialize(&mut cursor)
        .map_err(|_| AuxPowError::InvalidParentCoinbase)?;
    let expected_length =
        u64::try_from(serialized_coinbase.len()).map_err(|_| AuxPowError::InvalidParentCoinbase)?;
    if cursor.position() != expected_length {
        return Err(AuxPowError::NonCanonicalParentCoinbase.into());
    }
    if !transaction.is_coinbase() {
        return Err(AuxPowError::ParentTransactionIsNotCoinbase.into());
    }
    let canonical = transaction
        .zcash_serialize_to_vec()
        .map_err(MinerError::CoinbaseSerialization)?;
    if canonical != serialized_coinbase {
        return Err(AuxPowError::NonCanonicalParentCoinbase.into());
    }

    let authorizing_data_digest = transaction
        .auth_digest()
        .ok_or(AuxPowError::UnsupportedParentCoinbaseVersion(
            transaction.version(),
        ))?
        .0;
    let miner_data = transaction
        .inputs()
        .first()
        .and_then(Input::miner_data)
        .ok_or(AuxPowError::MissingParentCoinbaseMinerData)?
        .clone();
    Ok(CanonicalCoinbase {
        transaction_id: transaction.hash().0,
        authorizing_data_digest,
        miner_data,
    })
}

#[cfg(test)]
mod tests {
    use wcash_zcash_aux::{validate_miner_data_commitment, AuxPowError};

    use super::*;

    #[test]
    fn nu63_coinbase_is_canonical_and_has_exact_miner_data_carrier() {
        let child_hash = [0x42; 32];
        let bytes = build_parent_coinbase(
            2_900_000,
            b"Wcash local AuxPoW",
            &[ParentOutput {
                value_zatoshis: 1,
                script_pubkey: vec![0x51],
            }],
            child_hash,
            7,
        )
        .expect("valid local parent coinbase");
        let summary =
            canonical_parent_coinbase(&bytes).expect("Zebra accepts its canonical serialization");

        validate_miner_data_commitment(&summary.miner_data, child_hash, &[], 0)
            .expect("commitment is exact and unambiguous");
    }

    #[test]
    fn parent_money_and_coinbase_bounds_are_independent_of_wcash_rules() {
        let too_large = ParentOutput {
            value_zatoshis: MAX_PARENT_MONEY + 1,
            script_pubkey: vec![0x51],
        };
        assert!(matches!(
            build_parent_coinbase(1, b"x", &[too_large], [0; 32], 0),
            Err(MinerError::InvalidParentAmount(_))
        ));
        assert!(matches!(
            build_parent_coinbase(u32::MAX, &[0; 95], &[], [0; 32], 0),
            Err(MinerError::CoinbaseDataTooLong)
        ));
        assert!(matches!(
            build_parent_coinbase(0, b"x", &[], [0; 32], 0),
            Err(MinerError::ZeroParentHeight)
        ));
    }

    #[test]
    fn exact_commitment_rejects_marker_in_an_extra_output() {
        let child_hash = [0x24; 32];
        let duplicate =
            wcash_zcash_aux::commitment_payload(child_hash, &[], 0, 1).expect("fixture commitment");
        let error = build_parent_coinbase(1, &duplicate, &[], child_hash, 1)
            .expect_err("Zebra parsing alone accepts the coinbase");
        assert!(matches!(
            error,
            MinerError::AuxPow(AuxPowError::DuplicateCommitmentMarker)
        ));
    }
}
