//! Canonical bounded proof framing and complete validation pipeline.

use crate::{
    codec::{encode_compact_size, Reader},
    merkle::auxiliary_tree_size,
    AuxPowError, ParentHeader, ValidatedCommitment, ValidatedParentWork,
    MAX_AUXILIARY_BRANCH_DEPTH, MAX_COINBASE_BYTES, MAX_PARENT_BRANCH_DEPTH, MAX_PROOF_BYTES,
    PARENT_HEADER_BYTES, PROOF_MAGIC, PROOF_VERSION,
};

#[cfg(any(feature = "zebra", test))]
use crate::{
    commitment::validate_commitment, merkle::sha256d_merkle_root, CoinbaseSummary,
    CoinbaseVerifier, Equihash200_9, EquihashVerifier, Target,
};

/// Structurally decoded Zcash-parent AuxPoW proof.
///
/// Construction certifies canonical framing and static bounds only. Use one of
/// the validation methods before crossing a consensus boundary.
/// All block IDs and Merkle branch nodes use raw serialized order, as specified
/// by the crate-level consensus byte-order contract.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuxPowProof {
    coinbase_bytes: Box<[u8]>,
    parent_merkle_branch: Box<[[u8; 32]]>,
    parent_coinbase_index: u32,
    auxiliary_merkle_branch: Box<[[u8; 32]]>,
    auxiliary_index: u32,
    parent_header: ParentHeader,
}

impl AuxPowProof {
    /// Constructs one bounded structural proof.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        coinbase_bytes: impl Into<Box<[u8]>>,
        parent_merkle_branch: impl Into<Box<[[u8; 32]]>>,
        parent_coinbase_index: u32,
        auxiliary_merkle_branch: impl Into<Box<[[u8; 32]]>>,
        auxiliary_index: u32,
        parent_header: ParentHeader,
    ) -> Result<Self, AuxPowError> {
        let proof = Self {
            coinbase_bytes: coinbase_bytes.into(),
            parent_merkle_branch: parent_merkle_branch.into(),
            parent_coinbase_index,
            auxiliary_merkle_branch: auxiliary_merkle_branch.into(),
            auxiliary_index,
            parent_header,
        };
        proof.check_bounds()?;
        if proof.encoded_len()? > MAX_PROOF_BYTES {
            return Err(AuxPowError::ProofTooLarge {
                actual: proof.encoded_len()?,
                max: MAX_PROOF_BYTES,
            });
        }
        Ok(proof)
    }

    /// Decodes exactly one canonical, bounded proof.
    ///
    /// Counts and byte lengths are capped before allocation. Unknown versions,
    /// non-minimal CompactSize values, and trailing data are rejected.
    pub fn decode(bytes: &[u8]) -> Result<Self, AuxPowError> {
        if bytes.len() > MAX_PROOF_BYTES {
            return Err(AuxPowError::ProofTooLarge {
                actual: bytes.len(),
                max: MAX_PROOF_BYTES,
            });
        }

        let mut reader = Reader::new(bytes);
        let magic = reader.read_array("proof magic")?;
        if magic != PROOF_MAGIC {
            return Err(AuxPowError::InvalidProofMagic(magic));
        }
        let version = reader.read_u8("proof version")?;
        if version != PROOF_VERSION {
            return Err(AuxPowError::UnsupportedProofVersion(version));
        }

        let coinbase_length = reader.read_compact_size()?;
        if coinbase_length > MAX_COINBASE_BYTES {
            return Err(AuxPowError::CoinbaseTooLarge {
                actual: coinbase_length,
                max: MAX_COINBASE_BYTES,
            });
        }
        let coinbase_bytes: Box<[u8]> = reader
            .read_slice(coinbase_length, "parent coinbase transaction")?
            .into();
        let parent_merkle_branch: Box<[[u8; 32]]> =
            read_branch(&mut reader, "parent", MAX_PARENT_BRANCH_DEPTH)?.into();
        let parent_coinbase_index = reader.read_u32_le("parent coinbase index")?;
        let auxiliary_merkle_branch: Box<[[u8; 32]]> =
            read_branch(&mut reader, "auxiliary", MAX_AUXILIARY_BRANCH_DEPTH)?.into();
        let auxiliary_index = reader.read_u32_le("auxiliary index")?;
        let parent_header =
            ParentHeader::decode(reader.read_slice(PARENT_HEADER_BYTES, "parent header")?)?;
        if reader.remaining() != 0 {
            return Err(AuxPowError::TrailingBytes(reader.remaining()));
        }

        Self::new(
            coinbase_bytes,
            parent_merkle_branch,
            parent_coinbase_index,
            auxiliary_merkle_branch,
            auxiliary_index,
            parent_header,
        )
    }

    /// Encodes this proof in the unique frozen byte format.
    pub fn encode(&self) -> Result<Vec<u8>, AuxPowError> {
        self.check_bounds()?;
        let length = self.encoded_len()?;
        if length > MAX_PROOF_BYTES {
            return Err(AuxPowError::ProofTooLarge {
                actual: length,
                max: MAX_PROOF_BYTES,
            });
        }

        let mut output = Vec::with_capacity(length);
        output.extend_from_slice(&PROOF_MAGIC);
        output.push(PROOF_VERSION);
        encode_compact_size(self.coinbase_bytes.len(), &mut output);
        output.extend_from_slice(&self.coinbase_bytes);
        write_branch(&self.parent_merkle_branch, &mut output);
        output.extend_from_slice(&self.parent_coinbase_index.to_le_bytes());
        write_branch(&self.auxiliary_merkle_branch, &mut output);
        output.extend_from_slice(&self.auxiliary_index.to_le_bytes());
        output.extend_from_slice(self.parent_header.as_bytes());
        debug_assert_eq!(output.len(), length);
        Ok(output)
    }

    /// Performs complete validation with the pinned Zebra coinbase adapter.
    #[cfg(feature = "zebra")]
    pub fn validate(
        &self,
        auxiliary_block_hash: [u8; 32],
        required_target: Target,
    ) -> Result<ValidatedAuxPow, AuxPowError> {
        self.validate_with_verifiers(
            auxiliary_block_hash,
            required_target,
            &crate::ZebraCoinbaseVerifier,
            &Equihash200_9,
        )
    }

    /// Test-only validation with an explicit coinbase adapter and the built-in
    /// Equihash verifier.
    #[cfg(test)]
    fn validate_with_coinbase<C: CoinbaseVerifier>(
        &self,
        auxiliary_block_hash: [u8; 32],
        required_target: Target,
        coinbase_verifier: &C,
    ) -> Result<ValidatedAuxPow, AuxPowError> {
        self.validate_with_verifiers(
            auxiliary_block_hash,
            required_target,
            coinbase_verifier,
            &Equihash200_9,
        )
    }

    /// Performs complete validation through crate-internal adapter boundaries.
    ///
    /// The Wcash target check is always performed by this crate before either
    /// adapter can authorize the proof. This method is private so the only
    /// production API capable of returning [`ValidatedAuxPow`] is
    /// [`Self::validate`], which selects the pinned Zebra parser and fixed
    /// Equihash `(200, 9)` verifier.
    #[cfg(any(feature = "zebra", test))]
    fn validate_with_verifiers<C, W>(
        &self,
        auxiliary_block_hash: [u8; 32],
        required_target: Target,
        coinbase_verifier: &C,
        work_verifier: &W,
    ) -> Result<ValidatedAuxPow, AuxPowError>
    where
        C: CoinbaseVerifier,
        W: EquihashVerifier,
    {
        self.check_bounds()?;

        // Cheap target rejection happens before parsing an attacker-controlled transaction.
        self.parent_header.check_target(required_target)?;
        let coinbase = coinbase_verifier.verify(&self.coinbase_bytes)?;
        self.validate_parent_merkle_path(&coinbase)?;
        let commitment = validate_commitment(
            coinbase.transparent_outputs(),
            auxiliary_block_hash,
            &self.auxiliary_merkle_branch,
            self.auxiliary_index,
        )?;
        let parent_work = self
            .parent_header
            .validate_work_with(required_target, work_verifier)?;

        Ok(ValidatedAuxPow {
            parent_work,
            coinbase_transaction_id: coinbase.transaction_id(),
            commitment,
        })
    }

    /// Returns the exact serialized parent coinbase bytes.
    pub fn coinbase_bytes(&self) -> &[u8] {
        &self.coinbase_bytes
    }

    /// Returns the parent transaction Merkle branch.
    pub fn parent_merkle_branch(&self) -> &[[u8; 32]] {
        &self.parent_merkle_branch
    }

    /// Returns the explicit parent coinbase index, which must be zero.
    pub const fn parent_coinbase_index(&self) -> u32 {
        self.parent_coinbase_index
    }

    /// Returns the auxiliary Merkle branch.
    pub fn auxiliary_merkle_branch(&self) -> &[[u8; 32]] {
        &self.auxiliary_merkle_branch
    }

    /// Returns the supplied auxiliary slot.
    pub const fn auxiliary_index(&self) -> u32 {
        self.auxiliary_index
    }

    /// Returns the structurally checked parent header.
    pub const fn parent_header(&self) -> &ParentHeader {
        &self.parent_header
    }

    #[cfg(any(feature = "zebra", test))]
    fn validate_parent_merkle_path(&self, coinbase: &CoinbaseSummary) -> Result<(), AuxPowError> {
        if self.parent_coinbase_index != 0 {
            return Err(AuxPowError::ParentCoinbaseIndexNotZero(
                self.parent_coinbase_index,
            ));
        }
        let root = sha256d_merkle_root(
            coinbase.transaction_id(),
            &self.parent_merkle_branch,
            self.parent_coinbase_index,
        )?;
        if root != self.parent_header.merkle_root() {
            return Err(AuxPowError::ParentMerkleRootMismatch);
        }
        Ok(())
    }

    fn check_bounds(&self) -> Result<(), AuxPowError> {
        if self.coinbase_bytes.len() > MAX_COINBASE_BYTES {
            return Err(AuxPowError::CoinbaseTooLarge {
                actual: self.coinbase_bytes.len(),
                max: MAX_COINBASE_BYTES,
            });
        }
        check_branch(
            "parent",
            self.parent_merkle_branch.len(),
            MAX_PARENT_BRANCH_DEPTH,
        )?;
        check_branch(
            "auxiliary",
            self.auxiliary_merkle_branch.len(),
            MAX_AUXILIARY_BRANCH_DEPTH,
        )?;
        if self.parent_coinbase_index != 0 {
            return Err(AuxPowError::ParentCoinbaseIndexNotZero(
                self.parent_coinbase_index,
            ));
        }
        let tree_size = auxiliary_tree_size(self.auxiliary_merkle_branch.len())?;
        if self.auxiliary_index >= tree_size {
            return Err(AuxPowError::MerkleIndexOutOfRange {
                tree: "auxiliary",
                index: self.auxiliary_index,
                tree_size: u64::from(tree_size),
            });
        }
        Ok(())
    }

    fn encoded_len(&self) -> Result<usize, AuxPowError> {
        let branch_bytes = self
            .parent_merkle_branch
            .len()
            .checked_add(self.auxiliary_merkle_branch.len())
            .and_then(|nodes| nodes.checked_mul(32))
            .ok_or(AuxPowError::EncodingLengthOverflow)?;
        PROOF_MAGIC
            .len()
            .checked_add(1)
            .and_then(|length| length.checked_add(compact_size_len(self.coinbase_bytes.len())))
            .and_then(|length| length.checked_add(self.coinbase_bytes.len()))
            .and_then(|length| {
                length.checked_add(compact_size_len(self.parent_merkle_branch.len()))
            })
            .and_then(|length| length.checked_add(branch_bytes))
            .and_then(|length| length.checked_add(4))
            .and_then(|length| {
                length.checked_add(compact_size_len(self.auxiliary_merkle_branch.len()))
            })
            .and_then(|length| length.checked_add(4))
            .and_then(|length| length.checked_add(PARENT_HEADER_BYTES))
            .ok_or(AuxPowError::EncodingLengthOverflow)
    }
}

/// A proof that completed every coinbase, commitment, Merkle, target, and
/// Equihash check.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidatedAuxPow {
    parent_work: ValidatedParentWork,
    coinbase_transaction_id: [u8; 32],
    commitment: ValidatedCommitment,
}

impl ValidatedAuxPow {
    /// Returns the validated parent work.
    pub const fn parent_work(&self) -> &ValidatedParentWork {
        &self.parent_work
    }

    /// Returns the authenticated parent coinbase transaction ID.
    pub const fn coinbase_transaction_id(&self) -> [u8; 32] {
        self.coinbase_transaction_id
    }

    /// Returns the validated merge-mining commitment.
    pub const fn commitment(&self) -> ValidatedCommitment {
        self.commitment
    }
}

fn read_branch(
    reader: &mut Reader<'_>,
    name: &'static str,
    maximum: usize,
) -> Result<Vec<[u8; 32]>, AuxPowError> {
    let length = reader.read_compact_size()?;
    check_branch(name, length, maximum)?;
    let mut branch = Vec::with_capacity(length);
    for _ in 0..length {
        branch.push(reader.read_array("Merkle branch node")?);
    }
    Ok(branch)
}

fn check_branch(name: &'static str, actual: usize, maximum: usize) -> Result<(), AuxPowError> {
    if actual > maximum {
        return Err(AuxPowError::BranchTooLong {
            branch: name,
            actual,
            max: maximum,
        });
    }
    Ok(())
}

fn write_branch(branch: &[[u8; 32]], output: &mut Vec<u8>) {
    encode_compact_size(branch.len(), output);
    for node in branch {
        output.extend_from_slice(node);
    }
}

const fn compact_size_len(value: usize) -> usize {
    if value <= 0xfc {
        1
    } else if value <= u16::MAX as usize {
        3
    } else if value <= u32::MAX as usize {
        5
    } else {
        9
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        commitment_script, expected_auxiliary_index, merkle::sha256d, CoinbaseSummary,
        TransparentOutput,
    };

    #[cfg(feature = "zebra")]
    use crate::auxiliary_leaf;

    use super::*;

    const AUX_HASH: [u8; 32] = [0x42; 32];
    const COINBASE: &[u8] = b"synthetic canonical coinbase fixture";
    const AUX_NONCE: u32 = 7;

    #[derive(Clone)]
    struct FixtureCoinbase {
        expected_bytes: Vec<u8>,
        summary: CoinbaseSummary,
    }

    impl CoinbaseVerifier for FixtureCoinbase {
        fn verify(&self, bytes: &[u8]) -> Result<CoinbaseSummary, AuxPowError> {
            if bytes != self.expected_bytes {
                return Err(AuxPowError::InvalidParentCoinbase);
            }
            Ok(self.summary.clone())
        }
    }

    struct AcceptFixtureEquihash;

    impl EquihashVerifier for AcceptFixtureEquihash {
        fn verify(&self, _header: &ParentHeader) -> Result<(), AuxPowError> {
            Ok(())
        }
    }

    fn header_with_merkle_root(root: [u8; 32]) -> ParentHeader {
        let mut bytes = vec![0; PARENT_HEADER_BYTES];
        bytes[..4].copy_from_slice(&4u32.to_le_bytes());
        bytes[36..68].copy_from_slice(&root);
        bytes[104..108].copy_from_slice(&0x1f07_ffffu32.to_le_bytes());
        bytes[140..143].copy_from_slice(&[0xfd, 0x40, 0x05]);
        ParentHeader::decode(&bytes).expect("fixture header has canonical framing")
    }

    fn valid_case() -> (AuxPowProof, FixtureCoinbase) {
        let auxiliary_branch = vec![[0x11; 32], [0x22; 32], [0x33; 32]];
        let auxiliary_index = expected_auxiliary_index(AUX_NONCE, auxiliary_branch.len())
            .expect("fixture depth is valid");
        let script = commitment_script(AUX_HASH, &auxiliary_branch, auxiliary_index, AUX_NONCE)
            .expect("fixture commitment is valid");
        let txid = sha256d(COINBASE);
        let parent_branch = vec![[0x55; 32]];
        let parent_root =
            sha256d_merkle_root(txid, &parent_branch, 0).expect("fixture parent branch is valid");
        let header = header_with_merkle_root(parent_root);
        let coinbase_verifier = FixtureCoinbase {
            expected_bytes: COINBASE.to_vec(),
            summary: CoinbaseSummary::new(
                txid,
                vec![
                    TransparentOutput::new(50, vec![0x51]),
                    TransparentOutput::new(0, script),
                ],
            ),
        };
        let proof = AuxPowProof::new(
            COINBASE.to_vec(),
            parent_branch,
            0,
            auxiliary_branch,
            auxiliary_index,
            header,
        )
        .expect("fixture proof is structurally valid");
        (proof, coinbase_verifier)
    }

    #[cfg(feature = "zebra")]
    fn ascending_bytes<const N: usize>(start: u8) -> [u8; N] {
        std::array::from_fn(|index| {
            start.wrapping_add(
                u8::try_from(index).expect("interoperability vector arrays are at most 32 bytes"),
            )
        })
    }

    /// Normative, non-palindromic pool interoperability vector.
    ///
    /// Every identifier and branch node below is written in raw serialized
    /// order. In particular, `AUXILIARY_BLOCK_ID` is deliberately not invariant
    /// under reversal, so this test fails if an implementation accidentally
    /// substitutes conventional display order.
    #[cfg(feature = "zebra")]
    #[test]
    fn raw_byte_order_interoperability_vector() {
        use sha2::{Digest, Sha256};

        const AUXILIARY_BLOCK_ID: [u8; 32] = [
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
            0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b,
            0x1c, 0x1d, 0x1e, 0x1f,
        ];
        const AUXILIARY_NONCE: u32 = 0x0d0c_0b0a;

        let auxiliary_branch = [ascending_bytes::<32>(0x20), ascending_bytes::<32>(0x40)];
        let auxiliary_index = expected_auxiliary_index(AUXILIARY_NONCE, auxiliary_branch.len())
            .expect("two levels fit the consensus auxiliary-tree bound");
        assert_eq!(auxiliary_index, 3);

        let leaf = auxiliary_leaf(AUXILIARY_BLOCK_ID);
        assert_eq!(
            hex::encode(leaf),
            "02738bd4cbffaecb1ac3265b1dccb2018cd2b4d64277a41509734167c0b28257"
        );
        let auxiliary_root = sha256d_merkle_root(leaf, &auxiliary_branch, auxiliary_index)
            .expect("the vector's auxiliary position is valid");
        assert_eq!(
            hex::encode(auxiliary_root),
            "9b48d0cb7ba1925f0ef43d4c401d50a6f129c8aa98a032db880df56285a84cc2"
        );

        let script = commitment_script(
            AUXILIARY_BLOCK_ID,
            &auxiliary_branch,
            auxiliary_index,
            AUXILIARY_NONCE,
        )
        .expect("the vector has a valid deterministic slot");
        assert_eq!(
            hex::encode(&script),
            "6a2cfabe6d6dc24ca88562f50d88db32a098aac829f1a6501d404c3df40e5f92a17bcbd0489b040000000a0b0c0d"
        );

        let mut display_order_id = AUXILIARY_BLOCK_ID;
        display_order_id.reverse();
        assert_ne!(
            auxiliary_leaf(display_order_id),
            leaf,
            "display-order hashes are not valid substitutes for raw block IDs"
        );

        // Canonical V1 Zcash coinbase at height 1 with the exact zero-value
        // commitment output. V1 keeps this independently reproducible without
        // importing a network-upgrade-specific transaction builder.
        let mut coinbase = Vec::new();
        coinbase.extend_from_slice(&1u32.to_le_bytes());
        coinbase.push(1);
        coinbase.extend_from_slice(&[0; 32]);
        coinbase.extend_from_slice(&u32::MAX.to_le_bytes());
        coinbase.extend_from_slice(&[2, 0x51, 0xab]);
        coinbase.extend_from_slice(&u32::MAX.to_le_bytes());
        coinbase.push(1);
        coinbase.extend_from_slice(&0u64.to_le_bytes());
        coinbase.push(46);
        coinbase.extend_from_slice(&script);
        coinbase.extend_from_slice(&0u32.to_le_bytes());
        assert_eq!(coinbase.len(), 108);
        assert_eq!(
            hex::encode(&coinbase),
            concat!(
                "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0251abffffffff",
                "0100000000000000002e6a2cfabe6d6dc24ca88562f50d88db32a098aac829f1a6501d404c3df40e5f92a17bcbd0489b",
                "040000000a0b0c0d00000000",
            )
        );

        let coinbase_summary = crate::ZebraCoinbaseVerifier
            .verify(&coinbase)
            .expect("the pinned Zebra parser accepts the canonical vector coinbase");
        assert_eq!(
            hex::encode(coinbase_summary.transaction_id()),
            "c3b6b1e87d678d15dcc4a33ea1483733c41395fa10b1661633a872b3822643a3"
        );

        let parent_branch = [ascending_bytes::<32>(0x60)];
        let parent_root = sha256d_merkle_root(coinbase_summary.transaction_id(), &parent_branch, 0)
            .expect("the vector coinbase is at parent index zero");
        assert_eq!(
            hex::encode(parent_root),
            "4bc36b65591db52f240d1030dd12e8131532e9c3d4389ffd2d0cc988025acf9c"
        );

        let mut header_bytes = vec![0; PARENT_HEADER_BYTES];
        header_bytes[..4].copy_from_slice(&4u32.to_le_bytes());
        header_bytes[4..36].copy_from_slice(&ascending_bytes::<32>(0x80));
        header_bytes[36..68].copy_from_slice(&parent_root);
        header_bytes[68..100].copy_from_slice(&ascending_bytes::<32>(0xa0));
        header_bytes[100..104].copy_from_slice(&0x0102_0304u32.to_le_bytes());
        header_bytes[104..108].copy_from_slice(&0x1f07_ffffu32.to_le_bytes());
        header_bytes[108..140].copy_from_slice(&ascending_bytes::<32>(0xc0));
        header_bytes[140..143].copy_from_slice(&[0xfd, 0x40, 0x05]);
        header_bytes[143..].fill(0xee);
        let parent_header =
            ParentHeader::decode(&header_bytes).expect("the vector parent header is canonical");

        let proof = AuxPowProof::new(
            coinbase,
            parent_branch,
            0,
            auxiliary_branch,
            auxiliary_index,
            parent_header,
        )
        .expect("the interoperability proof is structurally valid");
        let encoded = proof.encode().expect("the bounded proof encodes");
        assert_eq!(encoded.len(), 1_707);

        assert_eq!(
            hex::encode(&encoded),
            include_str!("../test-vectors/auxpow-v1-non-palindromic.hex").trim()
        );
        assert_eq!(
            hex::encode(Sha256::digest(&encoded)),
            "38d7a19f0627a850b76f77920e7422e54935001b5ce50bc3372e8193226f556b"
        );
        assert_eq!(
            AuxPowProof::decode(&encoded),
            Ok(proof.clone()),
            "the exact proof vector round-trips"
        );

        let validated = proof
            .validate_with_verifiers(
                AUXILIARY_BLOCK_ID,
                Target::MAX,
                &crate::ZebraCoinbaseVerifier,
                &AcceptFixtureEquihash,
            )
            .expect("all non-Equihash vector bindings validate");
        assert_eq!(
            validated.coinbase_transaction_id(),
            coinbase_summary.transaction_id()
        );
        assert_eq!(validated.commitment().auxiliary_root(), auxiliary_root);
    }

    #[test]
    fn proof_round_trips_and_all_checks_complete() {
        let (proof, coinbase) = valid_case();
        let bytes = proof.encode().expect("bounded proof encodes");
        assert_eq!(AuxPowProof::decode(&bytes), Ok(proof.clone()));
        let validated = proof
            .validate_with_verifiers(AUX_HASH, Target::MAX, &coinbase, &AcceptFixtureEquihash)
            .expect("all fixture bindings validate");
        assert_eq!(
            validated.coinbase_transaction_id(),
            coinbase.summary.transaction_id()
        );
        assert_eq!(validated.commitment().nonce(), AUX_NONCE);
    }

    #[test]
    fn builtin_equihash_never_accepts_fixture_placeholder_work() {
        let (proof, coinbase) = valid_case();
        assert_eq!(
            proof.validate_with_coinbase(AUX_HASH, Target::MAX, &coinbase),
            Err(AuxPowError::InvalidEquihash)
        );
    }

    #[test]
    fn magic_version_compactsize_truncation_and_trailing_bytes_fail() {
        let (proof, _) = valid_case();
        let bytes = proof.encode().expect("bounded proof encodes");

        let mut wrong_magic = bytes.clone();
        wrong_magic[0] ^= 1;
        assert!(matches!(
            AuxPowProof::decode(&wrong_magic),
            Err(AuxPowError::InvalidProofMagic(_))
        ));

        let mut unknown_version = bytes.clone();
        unknown_version[PROOF_MAGIC.len()] = 2;
        assert_eq!(
            AuxPowProof::decode(&unknown_version),
            Err(AuxPowError::UnsupportedProofVersion(2))
        );

        let mut noncanonical = bytes.clone();
        let length_offset = PROOF_MAGIC.len() + 1;
        noncanonical.splice(
            length_offset..length_offset + 1,
            [0xfd, COINBASE.len() as u8, 0],
        );
        assert_eq!(
            AuxPowProof::decode(&noncanonical),
            Err(AuxPowError::NonCanonicalCompactSize)
        );

        assert!(matches!(
            AuxPowProof::decode(&bytes[..bytes.len() - 1]),
            Err(AuxPowError::UnexpectedEnd { .. })
        ));

        let mut trailing = bytes;
        trailing.push(0);
        assert_eq!(
            AuxPowProof::decode(&trailing),
            Err(AuxPowError::TrailingBytes(1))
        );
    }

    #[test]
    fn counts_indexes_target_and_merkle_root_fail_closed() {
        let (proof, coinbase) = valid_case();

        assert_eq!(
            AuxPowProof::new(
                proof.coinbase_bytes.clone(),
                proof.parent_merkle_branch.clone(),
                1,
                proof.auxiliary_merkle_branch.clone(),
                proof.auxiliary_index,
                proof.parent_header.clone(),
            ),
            Err(AuxPowError::ParentCoinbaseIndexNotZero(1))
        );

        let too_long = vec![[0; 32]; MAX_PARENT_BRANCH_DEPTH + 1];
        assert!(matches!(
            AuxPowProof::new(
                COINBASE.to_vec(),
                too_long,
                0,
                Vec::<[u8; 32]>::new(),
                0,
                proof.parent_header.clone(),
            ),
            Err(AuxPowError::BranchTooLong {
                branch: "parent",
                ..
            })
        ));

        let mut wrong_parent_branch = proof.clone();
        wrong_parent_branch.parent_merkle_branch[0][0] ^= 1;
        assert_eq!(
            wrong_parent_branch.validate_with_verifiers(
                AUX_HASH,
                Target::MAX,
                &coinbase,
                &AcceptFixtureEquihash,
            ),
            Err(AuxPowError::ParentMerkleRootMismatch)
        );

        let hash = proof.parent_header.block_hash().into_le_bytes();
        let mut below_hash = hash;
        for byte in &mut below_hash {
            if *byte != 0 {
                *byte -= 1;
                break;
            }
            *byte = u8::MAX;
        }
        let below_hash = Target::from_le_bytes(below_hash).expect("fixture hash is above one");
        assert!(matches!(
            proof.validate_with_verifiers(AUX_HASH, below_hash, &coinbase, &AcceptFixtureEquihash,),
            Err(AuxPowError::InsufficientParentWork { .. })
        ));
    }

    #[test]
    fn declared_sizes_are_rejected_before_allocation() {
        let mut oversized = Vec::from(PROOF_MAGIC);
        oversized.push(PROOF_VERSION);
        oversized.push(0xfe);
        oversized.extend_from_slice(&((MAX_COINBASE_BYTES as u32) + 1).to_le_bytes());
        assert!(matches!(
            AuxPowProof::decode(&oversized),
            Err(AuxPowError::CoinbaseTooLarge { .. })
        ));

        let mut long_branch = Vec::from(PROOF_MAGIC);
        long_branch.push(PROOF_VERSION);
        long_branch.push(0);
        long_branch.push((MAX_PARENT_BRANCH_DEPTH + 1) as u8);
        assert!(matches!(
            AuxPowProof::decode(&long_branch),
            Err(AuxPowError::BranchTooLong {
                branch: "parent",
                ..
            })
        ));
    }
}
