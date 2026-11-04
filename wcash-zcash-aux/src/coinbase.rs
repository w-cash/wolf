//! Authenticated Zcash coinbase adapter boundary.

#[cfg(any(feature = "zebra", test))]
use crate::AuxPowError;
#[cfg(feature = "zebra")]
use crate::ParentCoinbaseRules;

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
        ZebraCoinbaseVerifierWithRules(ParentCoinbaseRules::Legacy).verify(serialized_coinbase)
    }
}

/// The canonical parser with an explicitly selected parent-domain policy.
#[cfg(feature = "zebra")]
#[derive(Clone, Copy, Debug)]
pub(crate) struct ZebraCoinbaseVerifierWithRules(pub(crate) ParentCoinbaseRules);

#[cfg(feature = "zebra")]
impl CoinbaseVerifier for ZebraCoinbaseVerifierWithRules {
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
        let branch_id = transaction
            .embedded_consensus_branch_id()
            .ok_or(AuxPowError::InvalidParentCoinbase)?;
        self.0.check_branch(u32::from(branch_id))?;
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

    fn transparent_coinbase(version: u32, branch_id: u32) -> Vec<u8> {
        // Independent empty-shielded-bundle fixture following the v5/v6 wire
        // grammar. Build each format directly; v6 has an additional Ironwood
        // action count that cannot be retained in a v5 transaction.
        let group_id: u32 = match version {
            5 => 0x26a7_270a,
            6 => 0xd884_b698,
            _ => panic!("the fixture covers only v5 and v6"),
        };
        let height = 2_900_000u32.to_le_bytes();
        let mut script = vec![3];
        script.extend_from_slice(&height[..3]);
        script.extend_from_slice(b"parent-domain-fixture");

        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(0x8000_0000 | version).to_le_bytes());
        bytes.extend_from_slice(&group_id.to_le_bytes());
        bytes.extend_from_slice(&branch_id.to_le_bytes());
        bytes.extend_from_slice(&0u32.to_le_bytes()); // lock time
        bytes.extend_from_slice(&height); // expiry height
        bytes.push(1); // transparent input count
        bytes.extend_from_slice(&[0; 32]); // coinbase null outpoint
        bytes.extend_from_slice(&u32::MAX.to_le_bytes());
        bytes.push(
            u8::try_from(script.len()).expect("the fixture script fits one-byte CompactSize"),
        );
        bytes.extend_from_slice(&script);
        bytes.extend_from_slice(&u32::MAX.to_le_bytes()); // sequence
        bytes.push(1); // transparent output count
        bytes.extend_from_slice(&0u64.to_le_bytes());
        bytes.extend_from_slice(&[1, 0x51]); // one-byte OP_TRUE script
        bytes.extend_from_slice(&[0, 0, 0]); // Sapling spend/output and Orchard action counts
        if version == 6 {
            bytes.push(0); // Ironwood action count
        }
        bytes
    }

    #[test]
    fn explicit_legacy_rules_preserve_all_pinned_parser_domains() {
        // These are parser/coinbase tests, not full parent-chain-valid blocks
        // or solved Equihash vectors. The old AuxPoW adapter did not require
        // the embedded branch to be active on the external Zcash chain.
        for branch_id in [
            0x0000_0000u32,
            0x5ba8_1b19,
            0x76b8_09bb,
            0x2bb4_0e60,
            0xf5b9_230b,
            0xe9ff_75a6,
            0xc2d6_d0b4,
            0xc8e7_1055,
            0x4dec_4df0,
            0x5437_f330,
            0x37a5_165b,
            0xb3cf_d27e,
            0x54ba_2bfb,
            0xd9c6_a7ee,
            0xc3a6_678a,
        ] {
            for version in [5, 6] {
                let coinbase = transparent_coinbase(version, branch_id);
                let explicit = ZebraCoinbaseVerifierWithRules(ParentCoinbaseRules::Legacy)
                    .verify(&coinbase)
                    .expect("every frozen branch remains parseable for an empty-bundle coinbase");
                assert_eq!(ZebraCoinbaseVerifier.verify(&coinbase), Ok(explicit));
            }
        }
    }

    #[test]
    fn explicit_rules_preserve_canonical_parser_rejections() {
        let verifier = ZebraCoinbaseVerifierWithRules(ParentCoinbaseRules::Legacy);
        let mut coinbase = transparent_coinbase(6, 0x37a5_165b);
        coinbase.push(0);
        assert_eq!(
            verifier.verify(&coinbase),
            Err(AuxPowError::NonCanonicalParentCoinbase)
        );

        for branch_id in [0x7719_0ad9u32, 0xdead_beef, 0xffff_ffff] {
            for version in [5, 6] {
                let coinbase = transparent_coinbase(version, branch_id);
                assert!(
                    verifier.verify(&coinbase).is_err(),
                    "unknown domains cannot bypass canonical parsing or the selected policy"
                );
            }
        }
    }

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
