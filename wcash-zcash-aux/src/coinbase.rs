//! Authenticated Zcash coinbase adapter boundary.

#[cfg(any(feature = "zebra", test))]
use crate::AuxPowError;

/// Consensus-relevant data authenticated by the crate's Zcash coinbase parser.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg(any(feature = "zebra", test))]
pub(crate) struct CoinbaseSummary {
    transaction_id: [u8; 32],
    authorizing_data_digest: [u8; 32],
    miner_data: Box<[u8]>,
}

#[cfg(any(feature = "zebra", test))]
impl CoinbaseSummary {
    /// Creates a summary from a canonical parser's result.
    ///
    /// The transaction ID must be in Zcash raw serialized Merkle byte order,
    /// never conventional reversed display order.
    pub(crate) fn new(
        transaction_id: [u8; 32],
        authorizing_data_digest: [u8; 32],
        miner_data: impl Into<Box<[u8]>>,
    ) -> Self {
        Self {
            transaction_id,
            authorizing_data_digest,
            miner_data: miner_data.into(),
        }
    }

    /// Returns the Zcash mined transaction ID in raw Merkle byte order.
    pub(crate) const fn transaction_id(&self) -> [u8; 32] {
        self.transaction_id
    }

    /// Returns the ZIP-244 authorizing-data digest in raw Merkle byte order.
    pub(crate) const fn authorizing_data_digest(&self) -> [u8; 32] {
        self.authorizing_data_digest
    }

    /// Returns the exact bytes after the canonical coinbase-height prefix.
    pub(crate) fn miner_data(&self) -> &[u8] {
        &self.miner_data
    }
}

/// Adapter for exact, canonical current-Zcash coinbase parsing and digests.
///
/// Implementations are consensus-critical: they must parse the supplied bytes
/// themselves, consume all bytes, prove the transaction is a coinbase, compute
/// both ZIP-244 digests for its exact transaction version, and return the exact
/// miner-data suffix authenticated by the authorizing-data digest.
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

        if !matches!(transaction.version(), 5 | 6) {
            return Err(AuxPowError::UnsupportedParentCoinbaseVersion(
                transaction.version(),
            ));
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
            .and_then(|input| input.miner_data())
            .ok_or(AuxPowError::MissingParentCoinbaseMinerData)?
            .clone();

        Ok(CoinbaseSummary::new(
            transaction.hash().0,
            authorizing_data_digest,
            miner_data,
        ))
    }
}

#[cfg(all(test, feature = "zebra"))]
mod tests {
    use hex::FromHex;

    use super::*;
    use crate::PARENT_HEADER_BYTES;

    #[test]
    fn zebra_adapter_rejects_pre_zip244_zcash_genesis_coinbase() {
        let block = Vec::<u8>::from_hex(
            include_str!("../../zebra-test/src/vectors/block-main-0-000-000.txt").trim(),
        )
        .expect("upstream test vector is hexadecimal");
        assert_eq!(block[PARENT_HEADER_BYTES], 1, "genesis has one transaction");
        let coinbase = &block[PARENT_HEADER_BYTES + 1..];
        assert_eq!(
            ZebraCoinbaseVerifier.verify(coinbase),
            Err(AuxPowError::UnsupportedParentCoinbaseVersion(1))
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
