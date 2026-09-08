//! Canonical bounded proof framing and complete validation pipeline.

use crate::{
    codec::{encode_compact_size, Reader},
    merkle::auxiliary_tree_size,
    AuxPowError, ParentHeader, ValidatedCommitment, ValidatedParentWork,
    MAX_AUTH_DATA_BRANCH_DEPTH, MAX_AUXILIARY_BRANCH_DEPTH, MAX_COINBASE_BYTES,
    MAX_PARENT_BRANCH_DEPTH, MAX_PROOF_BYTES, PARENT_HEADER_BYTES, PROOF_MAGIC, PROOF_VERSION,
};

#[cfg(any(feature = "zebra", test))]
use crate::{
    commitment::validate_miner_data_commitment,
    merkle::{auth_data_merkle_root, block_commitments_hash, sha256d_merkle_root},
    CoinbaseSummary, CoinbaseVerifier, Equihash200_9, EquihashVerifier, Target,
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
    auth_data_merkle_branch: Box<[[u8; 32]]>,
    auth_data_coinbase_index: u32,
    chain_history_root: [u8; 32],
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
        auth_data_merkle_branch: impl Into<Box<[[u8; 32]]>>,
        auth_data_coinbase_index: u32,
        chain_history_root: [u8; 32],
        auxiliary_merkle_branch: impl Into<Box<[[u8; 32]]>>,
        auxiliary_index: u32,
        parent_header: ParentHeader,
    ) -> Result<Self, AuxPowError> {
        let proof = Self {
            coinbase_bytes: coinbase_bytes.into(),
            parent_merkle_branch: parent_merkle_branch.into(),
            parent_coinbase_index,
            auth_data_merkle_branch: auth_data_merkle_branch.into(),
            auth_data_coinbase_index,
            chain_history_root,
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
        let auth_data_merkle_branch: Box<[[u8; 32]]> =
            read_branch(&mut reader, "auth-data", MAX_AUTH_DATA_BRANCH_DEPTH)?.into();
        let auth_data_coinbase_index = reader.read_u32_le("auth-data coinbase index")?;
        let chain_history_root = reader.read_array("chain-history root")?;
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
            auth_data_merkle_branch,
            auth_data_coinbase_index,
            chain_history_root,
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
        write_branch(&self.auth_data_merkle_branch, &mut output);
        output.extend_from_slice(&self.auth_data_coinbase_index.to_le_bytes());
        output.extend_from_slice(&self.chain_history_root);
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
        let auth_data_root = self.validate_parent_auth_data_path(&coinbase)?;
        let commitment = validate_miner_data_commitment(
            coinbase.miner_data(),
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
            coinbase_authorizing_data_digest: coinbase.authorizing_data_digest(),
            auth_data_root,
            chain_history_root: self.chain_history_root,
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

    /// Returns the parent authorizing-data Merkle branch.
    pub fn auth_data_merkle_branch(&self) -> &[[u8; 32]] {
        &self.auth_data_merkle_branch
    }

    /// Returns the authorizing-data index, which must be coinbase slot zero.
    pub const fn auth_data_coinbase_index(&self) -> u32 {
        self.auth_data_coinbase_index
    }

    /// Returns the raw parent chain-history root committed by the header.
    pub const fn chain_history_root(&self) -> [u8; 32] {
        self.chain_history_root
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

    #[cfg(any(feature = "zebra", test))]
    fn validate_parent_auth_data_path(
        &self,
        coinbase: &CoinbaseSummary,
    ) -> Result<[u8; 32], AuxPowError> {
        if self.auth_data_coinbase_index != 0 {
            return Err(AuxPowError::ParentAuthDataIndexNotZero(
                self.auth_data_coinbase_index,
            ));
        }
        if self.parent_merkle_branch.len() != self.auth_data_merkle_branch.len() {
            return Err(AuxPowError::ParentMerkleDepthMismatch {
                transaction: self.parent_merkle_branch.len(),
                auth_data: self.auth_data_merkle_branch.len(),
            });
        }

        let auth_data_root = auth_data_merkle_root(
            coinbase.authorizing_data_digest(),
            &self.auth_data_merkle_branch,
            self.auth_data_coinbase_index,
        )?;
        let expected = block_commitments_hash(self.chain_history_root, auth_data_root);
        if expected != self.parent_header.block_commitments_hash() {
            return Err(AuxPowError::ParentBlockCommitmentsMismatch);
        }
        Ok(auth_data_root)
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
            "auth-data",
            self.auth_data_merkle_branch.len(),
            MAX_AUTH_DATA_BRANCH_DEPTH,
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
        if self.auth_data_coinbase_index != 0 {
            return Err(AuxPowError::ParentAuthDataIndexNotZero(
                self.auth_data_coinbase_index,
            ));
        }
        if self.parent_merkle_branch.len() != self.auth_data_merkle_branch.len() {
            return Err(AuxPowError::ParentMerkleDepthMismatch {
                transaction: self.parent_merkle_branch.len(),
                auth_data: self.auth_data_merkle_branch.len(),
            });
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
            .checked_add(self.auth_data_merkle_branch.len())
            .and_then(|nodes| nodes.checked_add(self.auxiliary_merkle_branch.len()))
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
                length.checked_add(compact_size_len(self.auth_data_merkle_branch.len()))
            })
            .and_then(|length| length.checked_add(4))
            .and_then(|length| length.checked_add(32))
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
    coinbase_authorizing_data_digest: [u8; 32],
    auth_data_root: [u8; 32],
    chain_history_root: [u8; 32],
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

    /// Returns the authenticated coinbase ZIP-244 authorizing-data digest.
    pub const fn coinbase_authorizing_data_digest(&self) -> [u8; 32] {
        self.coinbase_authorizing_data_digest
    }

    /// Returns the authenticated ZIP-244 authorizing-data Merkle root.
    pub const fn auth_data_root(&self) -> [u8; 32] {
        self.auth_data_root
    }

    /// Returns the chain-history root used to authenticate `hashBlockCommitments`.
    pub const fn chain_history_root(&self) -> [u8; 32] {
        self.chain_history_root
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
        expected_auxiliary_index, merkle::sha256d, miner_data_commitment, CoinbaseSummary,
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

    fn header_with_roots(merkle_root: [u8; 32], block_commitments: [u8; 32]) -> ParentHeader {
        let mut bytes = vec![0; PARENT_HEADER_BYTES];
        bytes[..4].copy_from_slice(&4u32.to_le_bytes());
        bytes[36..68].copy_from_slice(&merkle_root);
        bytes[68..100].copy_from_slice(&block_commitments);
        bytes[104..108].copy_from_slice(&0x1f07_ffffu32.to_le_bytes());
        bytes[140..143].copy_from_slice(&[0xfd, 0x40, 0x05]);
        ParentHeader::decode(&bytes).expect("fixture header has canonical framing")
    }

    fn valid_case() -> (AuxPowProof, FixtureCoinbase) {
        let auxiliary_branch = vec![[0x11; 32], [0x22; 32], [0x33; 32]];
        let auxiliary_index = expected_auxiliary_index(AUX_NONCE, auxiliary_branch.len())
            .expect("fixture depth is valid");
        let payload =
            miner_data_commitment(AUX_HASH, &auxiliary_branch, auxiliary_index, AUX_NONCE)
                .expect("fixture commitment is valid");
        let mut miner_data = b"fixture-pool".to_vec();
        miner_data.extend_from_slice(&payload);
        let txid = sha256d(COINBASE);
        let parent_branch = vec![[0x55; 32]];
        let parent_root =
            sha256d_merkle_root(txid, &parent_branch, 0).expect("fixture parent branch is valid");
        let authorizing_data_digest = [0x66; 32];
        let auth_data_branch = vec![[0x77; 32]];
        let auth_data_root = auth_data_merkle_root(authorizing_data_digest, &auth_data_branch, 0)
            .expect("fixture auth-data branch is valid");
        let chain_history_root = [0x88; 32];
        let header = header_with_roots(
            parent_root,
            block_commitments_hash(chain_history_root, auth_data_root),
        );
        let coinbase_verifier = FixtureCoinbase {
            expected_bytes: COINBASE.to_vec(),
            summary: CoinbaseSummary::new(txid, authorizing_data_digest, miner_data),
        };
        let proof = AuxPowProof::new(
            COINBASE.to_vec(),
            parent_branch,
            0,
            auth_data_branch,
            0,
            chain_history_root,
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
        use zebra_chain::{
            amount::{Amount, NonNegative},
            block::Height,
            serialization::ZcashSerialize,
            transaction::{LockTime, Transaction},
            transparent::{Input, Output, Script},
        };

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
            "b38059dd1ca081e4a23cd86d4743aa48ce241675c5e89a1692976f4f509a8c82"
        );
        let auxiliary_root = sha256d_merkle_root(leaf, &auxiliary_branch, auxiliary_index)
            .expect("the vector's auxiliary position is valid");
        assert_eq!(
            hex::encode(auxiliary_root),
            "8d5950951d66307c42153ca11df77463bb828206cc18a1711a24b0e346781b92"
        );

        let payload = miner_data_commitment(
            AUXILIARY_BLOCK_ID,
            &auxiliary_branch,
            auxiliary_index,
            AUXILIARY_NONCE,
        )
        .expect("the vector has a valid deterministic slot");
        assert_eq!(
            hex::encode(payload),
            "fabe6d6d921b7846e3b0241a71a118cc068282bb6374f71da13c15427c30661d9550598d040000000a0b0c0d"
        );

        let mut display_order_id = AUXILIARY_BLOCK_ID;
        display_order_id.reverse();
        assert_ne!(
            auxiliary_leaf(display_order_id),
            leaf,
            "display-order hashes are not valid substitutes for raw block IDs"
        );

        let mut miner_data = b"wcash-vector/".to_vec();
        miner_data.extend_from_slice(&payload);
        let coinbase = Transaction::from_nu63_transparent_parts(
            vec![Input::Coinbase {
                height: Height(2_900_000),
                data: miner_data,
                sequence: u32::MAX,
            }],
            vec![Output::new(
                Amount::<NonNegative>::zero(),
                Script::new(&[0x51]),
            )],
            LockTime::unlocked(),
            Height(2_900_000),
        )
        .expect("NU6.3 transparent transaction parts are valid")
        .zcash_serialize_to_vec()
        .expect("the v6 vector coinbase serializes canonically");

        let coinbase_summary = crate::ZebraCoinbaseVerifier
            .verify(&coinbase)
            .expect("the pinned Zebra parser accepts the canonical vector coinbase");
        assert_eq!(
            hex::encode(coinbase_summary.transaction_id()),
            "4de1adc9e0576890004dc1f70468fb06877dd5d64fb2a99fdeeb1bacab672ff0"
        );
        assert_eq!(
            hex::encode(coinbase_summary.authorizing_data_digest()),
            "065d4020b28e88546e8059198691348d3ab5175285bbdd26009e87e9420eed15"
        );

        let parent_branch = [ascending_bytes::<32>(0x60)];
        let parent_root = sha256d_merkle_root(coinbase_summary.transaction_id(), &parent_branch, 0)
            .expect("the vector coinbase is at parent index zero");
        assert_eq!(
            hex::encode(parent_root),
            "99b2b282e8c6d72ea1caf8ca540cee4ea12cacc56958c578b4e7611cce93ff5e"
        );
        let auth_data_branch = [ascending_bytes::<32>(0x70)];
        let auth_data_root = auth_data_merkle_root(
            coinbase_summary.authorizing_data_digest(),
            &auth_data_branch,
            0,
        )
        .expect("the vector auth-data path is valid");
        assert_eq!(
            hex::encode(auth_data_root),
            "57d3d3462d84ed7d862dd4660b5b1ab90af5992eada5a28a5b4db93cb2795644"
        );
        let chain_history_root = ascending_bytes::<32>(0xa0);
        let parent_block_commitments = block_commitments_hash(chain_history_root, auth_data_root);
        assert_eq!(
            hex::encode(parent_block_commitments),
            "c8c145287afb11de88a57e1f5efe5026855684f906dbfb7c8bcd4ea96b86b165"
        );

        let mut header_bytes = vec![0; PARENT_HEADER_BYTES];
        header_bytes[..4].copy_from_slice(&4u32.to_le_bytes());
        header_bytes[4..36].copy_from_slice(&ascending_bytes::<32>(0x80));
        header_bytes[36..68].copy_from_slice(&parent_root);
        header_bytes[68..100].copy_from_slice(&parent_block_commitments);
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
            auth_data_branch,
            0,
            chain_history_root,
            auxiliary_branch,
            auxiliary_index,
            parent_header,
        )
        .expect("the interoperability proof is structurally valid");
        let encoded = proof.encode().expect("the bounded proof encodes");
        assert_eq!(encoded.len(), 1_806);
        assert_eq!(
            hex::encode(&encoded),
            include_str!("../test-vectors/auxpow-v2-non-palindromic.hex").trim()
        );
        assert_eq!(
            hex::encode(Sha256::digest(&encoded)),
            "d94e88abf717c604db33e42ea319f727bc072b86f7e7e0077d2ab5b7d40c18cf"
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

        // ZIP-244 deliberately leaves coinbase miner data outside the mined
        // transaction ID. Prove that v2 still rejects a byte-for-byte valid
        // alternative coinbase through the independently authenticated digest.
        let mut changed_authorizing_data = proof.clone();
        let pool_tag_offset = changed_authorizing_data
            .coinbase_bytes
            .windows(b"wcash-vector/".len())
            .position(|window| window == b"wcash-vector/")
            .expect("the vector contains its pool tag");
        changed_authorizing_data.coinbase_bytes[pool_tag_offset] ^= 1;
        let changed_summary = crate::ZebraCoinbaseVerifier
            .verify(changed_authorizing_data.coinbase_bytes())
            .expect("the length-preserving mutation remains a canonical v6 coinbase");
        assert_eq!(
            changed_summary.transaction_id(),
            coinbase_summary.transaction_id(),
            "authorizing data is not part of a ZIP-244 mined transaction ID"
        );
        assert_ne!(
            changed_summary.authorizing_data_digest(),
            coinbase_summary.authorizing_data_digest()
        );
        assert_eq!(
            changed_authorizing_data.validate_with_verifiers(
                AUXILIARY_BLOCK_ID,
                Target::MAX,
                &crate::ZebraCoinbaseVerifier,
                &AcceptFixtureEquihash,
            ),
            Err(AuxPowError::ParentBlockCommitmentsMismatch)
        );
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
    fn proof_is_bound_to_the_exact_auxiliary_block_id() {
        let (proof, coinbase) = valid_case();
        let mut different_network_block_id = AUX_HASH;
        different_network_block_id[0] ^= 1;

        assert_eq!(
            proof.validate_with_verifiers(
                different_network_block_id,
                Target::MAX,
                &coinbase,
                &AcceptFixtureEquihash,
            ),
            Err(AuxPowError::AuxiliaryRootMismatch)
        );
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
        unknown_version[PROOF_MAGIC.len()] = 3;
        assert_eq!(
            AuxPowProof::decode(&unknown_version),
            Err(AuxPowError::UnsupportedProofVersion(3))
        );

        let mut retired_v1 = bytes.clone();
        retired_v1[PROOF_MAGIC.len()] = 1;
        assert_eq!(
            AuxPowProof::decode(&retired_v1),
            Err(AuxPowError::UnsupportedProofVersion(1))
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
                proof.auth_data_merkle_branch.clone(),
                proof.auth_data_coinbase_index,
                proof.chain_history_root,
                proof.auxiliary_merkle_branch.clone(),
                proof.auxiliary_index,
                proof.parent_header.clone(),
            ),
            Err(AuxPowError::ParentCoinbaseIndexNotZero(1))
        );

        assert_eq!(
            AuxPowProof::new(
                proof.coinbase_bytes.clone(),
                proof.parent_merkle_branch.clone(),
                0,
                proof.auth_data_merkle_branch.clone(),
                1,
                proof.chain_history_root,
                proof.auxiliary_merkle_branch.clone(),
                proof.auxiliary_index,
                proof.parent_header.clone(),
            ),
            Err(AuxPowError::ParentAuthDataIndexNotZero(1))
        );

        assert!(matches!(
            AuxPowProof::new(
                proof.coinbase_bytes.clone(),
                proof.parent_merkle_branch.clone(),
                0,
                Vec::<[u8; 32]>::new(),
                0,
                proof.chain_history_root,
                proof.auxiliary_merkle_branch.clone(),
                proof.auxiliary_index,
                proof.parent_header.clone(),
            ),
            Err(AuxPowError::ParentMerkleDepthMismatch { .. })
        ));

        let too_long = vec![[0; 32]; MAX_PARENT_BRANCH_DEPTH + 1];
        assert!(matches!(
            AuxPowProof::new(
                COINBASE.to_vec(),
                too_long,
                0,
                Vec::<[u8; 32]>::new(),
                0,
                proof.chain_history_root,
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

        let mut wrong_auth_branch = proof.clone();
        wrong_auth_branch.auth_data_merkle_branch[0][0] ^= 1;
        assert_eq!(
            wrong_auth_branch.validate_with_verifiers(
                AUX_HASH,
                Target::MAX,
                &coinbase,
                &AcceptFixtureEquihash,
            ),
            Err(AuxPowError::ParentBlockCommitmentsMismatch)
        );

        let mut wrong_history_root = proof.clone();
        wrong_history_root.chain_history_root[0] ^= 1;
        assert_eq!(
            wrong_history_root.validate_with_verifiers(
                AUX_HASH,
                Target::MAX,
                &coinbase,
                &AcceptFixtureEquihash,
            ),
            Err(AuxPowError::ParentBlockCommitmentsMismatch)
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
