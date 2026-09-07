//! Authenticated Zcash coinbase adapter boundary.

#[cfg(any(feature = "zebra", test))]
use crate::AuxPowError;

/// One transparent output extracted from the exact serialized parent coinbase.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransparentOutput {
    value: u64,
    script_pubkey: Box<[u8]>,
}

impl TransparentOutput {
    /// Creates one parsed transparent output.
    pub fn new(value: u64, script_pubkey: impl Into<Box<[u8]>>) -> Self {
        Self {
            value,
            script_pubkey: script_pubkey.into(),
        }
    }

    /// Returns its zatoshi value.
    pub const fn value(&self) -> u64 {
        self.value
    }

    /// Returns its exact script bytes without a CompactSize prefix.
    pub fn script_pubkey(&self) -> &[u8] {
        &self.script_pubkey
    }
}

/// Consensus-relevant data authenticated by the crate's Zcash coinbase parser.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg(any(feature = "zebra", test))]
pub(crate) struct CoinbaseSummary {
    transaction_id: [u8; 32],
    transparent_outputs: Box<[TransparentOutput]>,
}

#[cfg(any(feature = "zebra", test))]
impl CoinbaseSummary {
    /// Creates a summary from a canonical parser's result.
    ///
    /// The transaction ID must be in Zcash raw serialized Merkle byte order,
    /// never conventional reversed display order.
    pub(crate) fn new(
        transaction_id: [u8; 32],
        transparent_outputs: impl Into<Box<[TransparentOutput]>>,
    ) -> Self {
        Self {
            transaction_id,
            transparent_outputs: transparent_outputs.into(),
        }
    }

    /// Returns the Zcash mined transaction ID in raw Merkle byte order.
    pub(crate) const fn transaction_id(&self) -> [u8; 32] {
        self.transaction_id
    }

    /// Returns every authenticated transparent output in wire order.
    pub(crate) fn transparent_outputs(&self) -> &[TransparentOutput] {
        &self.transparent_outputs
    }
}

/// Adapter for exact, canonical current-Zcash coinbase parsing and txid calculation.
///
/// Implementations are consensus-critical: they must parse the supplied bytes
/// themselves, consume all bytes, prove the transaction is a coinbase, compute
/// the correct mined txid for its exact transaction version, and return every
/// transparent output without filtering or reordering.
#[cfg(any(feature = "zebra", test))]
pub(crate) trait CoinbaseVerifier {
    /// Authenticates the exact serialized transaction and returns its summary.
    fn verify(&self, serialized_coinbase: &[u8]) -> Result<CoinbaseSummary, AuxPowError>;
}

/// Adapter backed by this source tree's pinned Zebra transaction implementation.
#[cfg(feature = "zebra")]
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ZebraCoinbaseVerifier;

#[cfg(feature = "zebra")]
impl CoinbaseVerifier for ZebraCoinbaseVerifier {
    fn verify(&self, serialized_coinbase: &[u8]) -> Result<CoinbaseSummary, AuxPowError> {
        use std::io::Cursor;

        use zebra_chain::{
            serialization::{ZcashDeserialize, ZcashSerialize},
            transaction::Transaction,
        };

        let mut cursor = Cursor::new(serialized_coinbase);
        let transaction = Transaction::zcash_deserialize(&mut cursor)
            .map_err(|_| AuxPowError::InvalidParentCoinbase)?;
        let expected_length = u64::try_from(serialized_coinbase.len())
            .map_err(|_| AuxPowError::InvalidParentCoinbase)?;
        if cursor.position() != expected_length {
            return Err(AuxPowError::NonCanonicalParentCoinbase);
        }
        if !transaction.is_coinbase() {
            return Err(AuxPowError::ParentTransactionIsNotCoinbase);
        }
        let canonical = transaction
            .zcash_serialize_to_vec()
            .map_err(|_| AuxPowError::InvalidParentCoinbase)?;
        if canonical != serialized_coinbase {
            return Err(AuxPowError::NonCanonicalParentCoinbase);
        }

        let outputs = transaction
            .outputs()
            .iter()
            .map(|output| {
                let value = u64::try_from(output.value.zatoshis())
                    .map_err(|_| AuxPowError::InvalidParentCoinbase)?;
                Ok(TransparentOutput::new(
                    value,
                    output.lock_script.as_raw_bytes().to_vec(),
                ))
            })
            .collect::<Result<Vec<_>, AuxPowError>>()?;

        Ok(CoinbaseSummary::new(transaction.hash().0, outputs))
    }
}

#[cfg(all(test, feature = "zebra"))]
mod tests {
    use hex::FromHex;

    use super::*;
    use crate::{sha256d_merkle_root, ParentHeader, PARENT_HEADER_BYTES};

    #[test]
    fn zebra_adapter_authenticates_real_zcash_genesis_coinbase() {
        let block = Vec::<u8>::from_hex(
            include_str!("../../zebra-test/src/vectors/block-main-0-000-000.txt").trim(),
        )
        .expect("upstream test vector is hexadecimal");
        assert_eq!(block[PARENT_HEADER_BYTES], 1, "genesis has one transaction");
        let header =
            ParentHeader::decode(&block[..PARENT_HEADER_BYTES]).expect("genesis header parses");
        let coinbase = &block[PARENT_HEADER_BYTES + 1..];
        let summary = ZebraCoinbaseVerifier
            .verify(coinbase)
            .expect("pinned Zebra parses its genesis coinbase");
        assert_eq!(
            sha256d_merkle_root(summary.transaction_id(), &[], 0)
                .expect("empty branch and index zero are valid"),
            header.merkle_root()
        );

        let mut trailing = coinbase.to_vec();
        trailing.push(0);
        assert_eq!(
            ZebraCoinbaseVerifier.verify(&trailing),
            Err(AuxPowError::NonCanonicalParentCoinbase)
        );
        assert_eq!(
            ZebraCoinbaseVerifier.verify(&[0; 8]),
            Err(AuxPowError::InvalidParentCoinbase)
        );
    }
}
