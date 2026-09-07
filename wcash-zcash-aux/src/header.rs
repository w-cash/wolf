//! Exact Zcash header parsing and independently targeted Equihash validation.

use equihash::is_valid_solution;

use crate::{merkle::sha256d, AuxPowError, Target, PARENT_HEADER_BYTES};

const HEADER_INPUT_BYTES: usize = 4 + 32 * 3 + 4 * 2;
const NONCE_BYTES: usize = 32;
const SOLUTION_BYTES: usize = 1_344;
const NONCE_START: usize = HEADER_INPUT_BYTES;
const SOLUTION_LENGTH_START: usize = NONCE_START + NONCE_BYTES;
const SOLUTION_START: usize = SOLUTION_LENGTH_START + 3;
const CANONICAL_SOLUTION_LENGTH: [u8; 3] = [0xfd, 0x40, 0x05];

/// One exact canonical-length Zcash Equihash `(200, 9)` parent header.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParentHeader {
    bytes: Box<[u8; PARENT_HEADER_BYTES]>,
    version: u32,
    advertised_n_bits: u32,
}

impl ParentHeader {
    /// Parses exactly one parent header.
    ///
    /// The parent `nBits` value is retained only as data. It is intentionally
    /// not decoded or used to authorize Wcash work.
    pub fn decode(bytes: &[u8]) -> Result<Self, AuxPowError> {
        if bytes.len() != PARENT_HEADER_BYTES {
            return Err(AuxPowError::InvalidHeaderLength {
                actual: bytes.len(),
                expected: PARENT_HEADER_BYTES,
            });
        }

        let version = u32::from_le_bytes(copy_array(&bytes[..4]));
        if version >> 31 != 0 {
            return Err(AuxPowError::HeaderVersionHighBit(version));
        }
        if version < 4 {
            return Err(AuxPowError::HeaderVersionTooLow(version));
        }
        if bytes[SOLUTION_LENGTH_START..SOLUTION_START] != CANONICAL_SOLUTION_LENGTH {
            return Err(AuxPowError::NonCanonicalSolutionLength);
        }

        let advertised_n_bits = u32::from_le_bytes(copy_array(&bytes[104..108]));
        let bytes: Box<[u8; PARENT_HEADER_BYTES]> = bytes
            .to_vec()
            .into_boxed_slice()
            .try_into()
            .map_err(|bytes: Box<[u8]>| AuxPowError::InvalidHeaderLength {
                actual: bytes.len(),
                expected: PARENT_HEADER_BYTES,
            })?;

        Ok(Self {
            bytes,
            version,
            advertised_n_bits,
        })
    }

    /// Returns the exact serialized header bytes.
    pub fn as_bytes(&self) -> &[u8; PARENT_HEADER_BYTES] {
        &self.bytes
    }

    /// Returns the signed-compatible Zcash header version.
    pub const fn version(&self) -> u32 {
        self.version
    }

    /// Returns parent `nBits` for diagnostics only.
    pub const fn advertised_n_bits(&self) -> u32 {
        self.advertised_n_bits
    }

    /// Returns the raw-byte-order parent transaction Merkle root.
    pub fn merkle_root(&self) -> [u8; 32] {
        copy_array(&self.bytes[36..68])
    }

    /// Returns the parent block hash in little-endian numeric order.
    pub fn block_hash(&self) -> ParentBlockHash {
        ParentBlockHash(sha256d(self.as_bytes()))
    }

    /// Checks the parent hash against an authenticated Wcash target.
    pub fn check_target(&self, required_target: Target) -> Result<ParentBlockHash, AuxPowError> {
        let block_hash = self.block_hash();
        if !required_target.is_met_by_le_hash(block_hash.into_le_bytes()) {
            return Err(AuxPowError::InsufficientParentWork {
                hash_le: block_hash.into_le_bytes(),
                target_le: required_target.to_le_bytes(),
            });
        }
        Ok(block_hash)
    }

    /// Validates target and Equihash using a crate-internal verifier.
    ///
    /// This hook is deliberately not public: external consensus callers must
    /// use the fixed `(200, 9)` verifier in [`Self::validate_work`].
    pub(crate) fn validate_work_with<V: EquihashVerifier>(
        &self,
        required_target: Target,
        verifier: &V,
    ) -> Result<ValidatedParentWork, AuxPowError> {
        let block_hash = self.check_target(required_target)?;
        verifier.verify(self)?;
        Ok(ValidatedParentWork {
            header: self.clone(),
            block_hash,
            required_target,
        })
    }

    /// Validates target and the built-in Equihash `(200, 9)` implementation.
    pub fn validate_work(
        &self,
        required_target: Target,
    ) -> Result<ValidatedParentWork, AuxPowError> {
        self.validate_work_with(required_target, &Equihash200_9)
    }

    pub(crate) fn equihash_input(&self) -> &[u8] {
        &self.bytes[..HEADER_INPUT_BYTES]
    }

    pub(crate) fn equihash_nonce(&self) -> &[u8] {
        &self.bytes[NONCE_START..SOLUTION_LENGTH_START]
    }

    pub(crate) fn equihash_solution(&self) -> &[u8] {
        &self.bytes[SOLUTION_START..SOLUTION_START + SOLUTION_BYTES]
    }
}

/// Crate-internal hook for an audited Equihash `(200, 9)` implementation.
///
/// Keeping this trait private to the crate prevents downstream consensus code
/// from constructing a validated-work token with a permissive implementation.
pub(crate) trait EquihashVerifier {
    /// Verifies the exact header input, nonce, and 1,344-byte solution.
    fn verify(&self, header: &ParentHeader) -> Result<(), AuxPowError>;
}

/// Built-in `equihash` crate verifier for the Zcash `(200, 9)` parameters.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Equihash200_9;

impl EquihashVerifier for Equihash200_9 {
    fn verify(&self, header: &ParentHeader) -> Result<(), AuxPowError> {
        is_valid_solution(
            200,
            9,
            header.equihash_input(),
            header.equihash_nonce(),
            header.equihash_solution(),
        )
        .map_err(|_| AuxPowError::InvalidEquihash)
    }
}

/// Raw SHA-256d parent header hash in little-endian numeric order.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ParentBlockHash([u8; 32]);

impl ParentBlockHash {
    /// Borrows the raw digest bytes.
    pub const fn as_le_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Returns the raw digest bytes.
    pub const fn into_le_bytes(self) -> [u8; 32] {
        self.0
    }
}

/// A parent header with Wcash target and Equihash checks completed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidatedParentWork {
    header: ParentHeader,
    block_hash: ParentBlockHash,
    required_target: Target,
}

impl ValidatedParentWork {
    /// Returns the checked parent header.
    pub const fn header(&self) -> &ParentHeader {
        &self.header
    }

    /// Returns the checked parent block hash.
    pub const fn block_hash(&self) -> ParentBlockHash {
        self.block_hash
    }

    /// Returns the authenticated Wcash target used for validation.
    pub const fn required_target(&self) -> Target {
        self.required_target
    }
}

fn copy_array<const N: usize>(bytes: &[u8]) -> [u8; N] {
    let mut output = [0; N];
    output.copy_from_slice(bytes);
    output
}

#[cfg(test)]
mod tests {
    use hex::FromHex;

    use super::*;

    fn genesis_header() -> ParentHeader {
        let block = Vec::<u8>::from_hex(
            include_str!("../../zebra-test/src/vectors/block-main-0-000-000.txt").trim(),
        )
        .expect("upstream test vector is hexadecimal");
        ParentHeader::decode(&block[..PARENT_HEADER_BYTES])
            .expect("upstream Zcash genesis header is canonical")
    }

    #[test]
    fn upstream_zcash_genesis_header_and_work_vector() {
        let header = genesis_header();
        let expected_display = <[u8; 32]>::from_hex(
            "00040fe8ec8471911baa1db1266ea15dd06b4a8a5c453883c000b031973dce08",
        )
        .expect("known hash is hexadecimal");
        let mut expected_raw = expected_display;
        expected_raw.reverse();

        assert_eq!(header.version(), 4);
        assert_eq!(header.block_hash().into_le_bytes(), expected_raw);
        let exact = Target::from_le_bytes(expected_raw).expect("genesis hash is nonzero");
        assert!(header.validate_work(exact).is_ok());
    }

    #[test]
    fn parser_rejects_ambiguous_header_encodings() {
        let valid = genesis_header().as_bytes().to_vec();
        assert!(matches!(
            ParentHeader::decode(&valid[..valid.len() - 1]),
            Err(AuxPowError::InvalidHeaderLength { .. })
        ));

        let mut trailing = valid.clone();
        trailing.push(0);
        assert!(matches!(
            ParentHeader::decode(&trailing),
            Err(AuxPowError::InvalidHeaderLength { .. })
        ));

        let mut low_version = valid.clone();
        low_version[..4].copy_from_slice(&3u32.to_le_bytes());
        assert_eq!(
            ParentHeader::decode(&low_version),
            Err(AuxPowError::HeaderVersionTooLow(3))
        );

        let mut high_bit = valid.clone();
        high_bit[..4].copy_from_slice(&0x8000_0004u32.to_le_bytes());
        assert_eq!(
            ParentHeader::decode(&high_bit),
            Err(AuxPowError::HeaderVersionHighBit(0x8000_0004))
        );

        let mut noncanonical_solution = valid;
        noncanonical_solution[SOLUTION_LENGTH_START..SOLUTION_START]
            .copy_from_slice(&[0xfe, 0x40, 0x05]);
        assert_eq!(
            ParentHeader::decode(&noncanonical_solution),
            Err(AuxPowError::NonCanonicalSolutionLength)
        );
    }

    #[test]
    fn wcash_target_does_not_use_parent_nbits() {
        let header = genesis_header();
        let mut changed = header.as_bytes().to_vec();
        changed[104..108].copy_from_slice(&0u32.to_le_bytes());
        let changed = ParentHeader::decode(&changed).expect("nBits is opaque to Wcash");
        assert_eq!(changed.advertised_n_bits(), 0);
        assert!(changed.check_target(Target::MAX).is_ok());
        assert_eq!(
            changed.validate_work(Target::MAX),
            Err(AuxPowError::InvalidEquihash)
        );
    }
}
