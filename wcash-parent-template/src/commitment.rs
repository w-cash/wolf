//! Canonical Zcash coinbase carriers for Wcash auxiliary commitments.

use sha2::{Digest, Sha256};

use crate::{
    expected_auxiliary_index,
    merkle::{auxiliary_tree_size, sha256d_merkle_root},
    AuxPowError, MAX_AUXILIARY_BRANCH_DEPTH, MERGED_MINING_MARKER, WCASH_AUXILIARY_CHAIN_ID,
};

/// Hash-domain tag used before every version-2 Wcash auxiliary block hash.
pub const AUXILIARY_LEAF_DOMAIN: &[u8] = b"Wcash/ZcashAuxPoW/leaf/v2\0";

const MINER_ROOT_START: usize = MERGED_MINING_MARKER.len();
const MINER_ROOT_END: usize = MINER_ROOT_START + 32;
const MINER_TREE_SIZE_END: usize = MINER_ROOT_END + 4;

/// Exact byte length of the version-2 coinbase miner-data commitment suffix.
pub const MINER_DATA_COMMITMENT_BYTES: usize = 44;

/// Domain-separates a Wcash auxiliary block ID before Merkle-tree insertion.
///
/// `auxiliary_block_hash` is the exact raw 32-byte Wcash block ID stored by the
/// node (`block::Hash.0`), not its reversed big-endian display encoding. The
/// returned leaf is also in raw SHA-256d/Merkle byte order.
pub fn auxiliary_leaf(auxiliary_block_hash: [u8; 32]) -> [u8; 32] {
    auxiliary_leaf_for_chain(WCASH_AUXILIARY_CHAIN_ID, auxiliary_block_hash)
}

/// Constructs the canonical version-2 coinbase miner-data commitment suffix.
///
/// The returned bytes must be appended directly to the coinbase input's inert
/// miner data. They are not wrapped in a script push opcode. A canonical v2
/// carrier is the final 44 bytes of that miner data, and the merged-mining
/// marker must occur nowhere else in the coinbase miner data.
pub fn miner_data_commitment(
    auxiliary_block_hash: [u8; 32],
    auxiliary_branch: &[[u8; 32]],
    auxiliary_index: u32,
    nonce: u32,
) -> Result<[u8; MINER_DATA_COMMITMENT_BYTES], AuxPowError> {
    check_auxiliary_position(auxiliary_branch, auxiliary_index, nonce)?;
    let tree_size = auxiliary_tree_size(auxiliary_branch.len())?;
    let mut root = sha256d_merkle_root(
        auxiliary_leaf(auxiliary_block_hash),
        auxiliary_branch,
        auxiliary_index,
    )?;
    // Namecoin-compatible carriers store the internal/raw root in reverse order.
    root.reverse();

    let mut commitment = [0; MINER_DATA_COMMITMENT_BYTES];
    commitment[..MINER_ROOT_START].copy_from_slice(&MERGED_MINING_MARKER);
    commitment[MINER_ROOT_START..MINER_ROOT_END].copy_from_slice(&root);
    commitment[MINER_ROOT_END..MINER_TREE_SIZE_END].copy_from_slice(&tree_size.to_le_bytes());
    commitment[MINER_TREE_SIZE_END..].copy_from_slice(&nonce.to_le_bytes());
    Ok(commitment)
}

/// Constructs the canonical version-2 coinbase miner-data commitment payload.
///
/// This allocation-friendly producer API returns exactly 44 bytes. It is
/// equivalent to [`miner_data_commitment`] and is intended for parent-template
/// builders that append the payload to existing pool-identification data.
pub fn commitment_payload(
    auxiliary_block_hash: [u8; 32],
    auxiliary_branch: &[[u8; 32]],
    auxiliary_index: u32,
    nonce: u32,
) -> Result<Vec<u8>, AuxPowError> {
    Ok(miner_data_commitment(
        auxiliary_block_hash,
        auxiliary_branch,
        auxiliary_index,
        nonce,
    )?
    .to_vec())
}

/// A commitment whose exact carrier, root, tree size, and slot were checked.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ValidatedCommitment {
    auxiliary_root: [u8; 32],
    tree_size: u32,
    nonce: u32,
    auxiliary_index: u32,
}

impl ValidatedCommitment {
    /// Returns the raw-byte-order auxiliary Merkle root.
    pub const fn auxiliary_root(&self) -> [u8; 32] {
        self.auxiliary_root
    }

    /// Returns the committed power-of-two tree size.
    pub const fn tree_size(&self) -> u32 {
        self.tree_size
    }

    /// Returns the merge-mining nonce.
    pub const fn nonce(&self) -> u32 {
        self.nonce
    }

    /// Returns the deterministic Wcash slot.
    pub const fn auxiliary_index(&self) -> u32 {
        self.auxiliary_index
    }
}

/// Validates the canonical version-2 commitment in authenticated coinbase data.
///
/// The commitment must be the exact final 44 bytes of `miner_data`, with no
/// script push wrapper. Exactly one merged-mining marker may occur across the
/// complete miner data. Rejecting alternate locations and duplicates prevents
/// parsers from authenticating different auxiliary roots from the same parent
/// coinbase.
pub fn validate_miner_data_commitment(
    miner_data: &[u8],
    auxiliary_block_hash: [u8; 32],
    auxiliary_branch: &[[u8; 32]],
    auxiliary_index: u32,
) -> Result<ValidatedCommitment, AuxPowError> {
    if auxiliary_branch.len() > MAX_AUXILIARY_BRANCH_DEPTH {
        return Err(AuxPowError::BranchTooLong {
            branch: "auxiliary",
            actual: auxiliary_branch.len(),
            max: MAX_AUXILIARY_BRANCH_DEPTH,
        });
    }

    match count_markers(miner_data) {
        0 => return Err(AuxPowError::MissingMinerDataCommitment),
        1 => {}
        _ => return Err(AuxPowError::DuplicateCommitmentMarker),
    }

    let carrier = miner_data
        .get(miner_data.len().saturating_sub(MINER_DATA_COMMITMENT_BYTES)..)
        .filter(|carrier| carrier.len() == MINER_DATA_COMMITMENT_BYTES)
        .ok_or(AuxPowError::CommitmentNotMinerDataSuffix)?;
    if carrier[..MINER_ROOT_START] != MERGED_MINING_MARKER {
        return Err(AuxPowError::CommitmentNotMinerDataSuffix);
    }

    validate_miner_data_carrier(
        carrier,
        auxiliary_block_hash,
        auxiliary_branch,
        auxiliary_index,
    )
}

fn validate_miner_data_carrier(
    carrier: &[u8],
    auxiliary_block_hash: [u8; 32],
    auxiliary_branch: &[[u8; 32]],
    auxiliary_index: u32,
) -> Result<ValidatedCommitment, AuxPowError> {
    debug_assert_eq!(carrier.len(), MINER_DATA_COMMITMENT_BYTES);
    let committed_tree_size =
        u32::from_le_bytes(copy_array(&carrier[MINER_ROOT_END..MINER_TREE_SIZE_END]));
    let nonce = u32::from_le_bytes(copy_array(&carrier[MINER_TREE_SIZE_END..]));
    check_auxiliary_position(auxiliary_branch, auxiliary_index, nonce)?;
    let expected_tree_size = auxiliary_tree_size(auxiliary_branch.len())?;
    if committed_tree_size != expected_tree_size {
        return Err(AuxPowError::AuxiliaryTreeSizeMismatch {
            committed: committed_tree_size,
            expected: expected_tree_size,
        });
    }

    let auxiliary_root = sha256d_merkle_root(
        auxiliary_leaf(auxiliary_block_hash),
        auxiliary_branch,
        auxiliary_index,
    )?;
    let mut committed_root = copy_array::<32>(&carrier[MINER_ROOT_START..MINER_ROOT_END]);
    committed_root.reverse();
    if committed_root != auxiliary_root {
        return Err(AuxPowError::AuxiliaryRootMismatch);
    }

    Ok(ValidatedCommitment {
        auxiliary_root,
        tree_size: expected_tree_size,
        nonce,
        auxiliary_index,
    })
}

fn count_markers(bytes: &[u8]) -> usize {
    bytes
        .windows(MERGED_MINING_MARKER.len())
        .filter(|window| *window == MERGED_MINING_MARKER)
        .count()
}

fn check_auxiliary_position(
    branch: &[[u8; 32]],
    index: u32,
    nonce: u32,
) -> Result<(), AuxPowError> {
    let tree_size = auxiliary_tree_size(branch.len())?;
    if index >= tree_size {
        return Err(AuxPowError::MerkleIndexOutOfRange {
            tree: "auxiliary",
            index,
            tree_size: u64::from(tree_size),
        });
    }
    let expected = expected_auxiliary_index(nonce, branch.len())?;
    if index != expected {
        return Err(AuxPowError::AuxiliaryIndexMismatch {
            actual: index,
            expected,
        });
    }
    Ok(())
}

fn auxiliary_leaf_for_chain(chain_id: u32, block_hash: [u8; 32]) -> [u8; 32] {
    let mut first = Sha256::new();
    first.update(AUXILIARY_LEAF_DOMAIN);
    first.update(chain_id.to_le_bytes());
    first.update(block_hash);
    Sha256::digest(first.finalize()).into()
}

fn copy_array<const N: usize>(bytes: &[u8]) -> [u8; N] {
    let mut output = [0; N];
    output.copy_from_slice(bytes);
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    const AUX_HASH: [u8; 32] = [0x42; 32];
    const NONCE: u32 = 7;

    #[test]
    fn leaf_is_domain_and_chain_separated() {
        assert_ne!(auxiliary_leaf(AUX_HASH), AUX_HASH);
        assert_ne!(
            auxiliary_leaf_for_chain(WCASH_AUXILIARY_CHAIN_ID, AUX_HASH),
            auxiliary_leaf_for_chain(WCASH_AUXILIARY_CHAIN_ID ^ 1, AUX_HASH)
        );
        assert_ne!(auxiliary_leaf(AUX_HASH), auxiliary_leaf([0x43; 32]));
    }

    #[test]
    fn exact_v2_miner_data_suffix_validates_and_is_unambiguous() {
        let branch = vec![[0x11; 32], [0x22; 32], [0x33; 32]];
        let index = expected_auxiliary_index(NONCE, branch.len()).expect("valid depth");
        let payload =
            miner_data_commitment(AUX_HASH, &branch, index, NONCE).expect("valid v2 commitment");
        assert_eq!(
            hex::encode(payload),
            "fabe6d6dcf52029358e363beb6a9da300e10e137cc5e6f1f9cb8a71bff3aac48d27c8fd30800000007000000"
        );
        assert_eq!(
            commitment_payload(AUX_HASH, &branch, index, NONCE),
            Ok(payload.to_vec())
        );

        let mut miner_data = b"/pool/".to_vec();
        miner_data.extend_from_slice(&payload);
        let validated = validate_miner_data_commitment(&miner_data, AUX_HASH, &branch, index)
            .expect("exact final suffix validates");
        assert_eq!(validated.nonce(), NONCE);
        assert_eq!(validated.tree_size(), 8);
        assert_eq!(validated.auxiliary_index(), index);

        let mut trailing = miner_data.clone();
        trailing.push(0);
        assert_eq!(
            validate_miner_data_commitment(&trailing, AUX_HASH, &branch, index),
            Err(AuxPowError::CommitmentNotMinerDataSuffix)
        );

        let mut duplicate = MERGED_MINING_MARKER.to_vec();
        duplicate.extend_from_slice(&miner_data);
        assert_eq!(
            validate_miner_data_commitment(&duplicate, AUX_HASH, &branch, index),
            Err(AuxPowError::DuplicateCommitmentMarker)
        );

        assert_eq!(
            validate_miner_data_commitment(b"/pool/no-commitment", AUX_HASH, &branch, index),
            Err(AuxPowError::MissingMinerDataCommitment)
        );
        assert_eq!(
            validate_miner_data_commitment(&miner_data, [0x43; 32], &branch, index),
            Err(AuxPowError::AuxiliaryRootMismatch)
        );

        let suffix_start = miner_data.len() - MINER_DATA_COMMITMENT_BYTES;
        let mut wrong_size = miner_data.clone();
        wrong_size[suffix_start + MINER_ROOT_END..suffix_start + MINER_TREE_SIZE_END]
            .copy_from_slice(&4u32.to_le_bytes());
        assert!(matches!(
            validate_miner_data_commitment(&wrong_size, AUX_HASH, &branch, index),
            Err(AuxPowError::AuxiliaryTreeSizeMismatch { .. })
        ));

        let wrong_index = (index + 1) % 8;
        assert!(matches!(
            validate_miner_data_commitment(&miner_data, AUX_HASH, &branch, wrong_index),
            Err(AuxPowError::AuxiliaryIndexMismatch { .. })
        ));

        let mut wrong_branch = branch.clone();
        wrong_branch[0][0] ^= 1;
        assert_eq!(
            validate_miner_data_commitment(&miner_data, AUX_HASH, &wrong_branch, index),
            Err(AuxPowError::AuxiliaryRootMismatch)
        );

        let oversized_branch = vec![[0; 32]; MAX_AUXILIARY_BRANCH_DEPTH + 1];
        assert!(matches!(
            validate_miner_data_commitment(&miner_data, AUX_HASH, &oversized_branch, index),
            Err(AuxPowError::BranchTooLong {
                branch: "auxiliary",
                ..
            })
        ));

        let mut marker_without_carrier = b"/pool/".to_vec();
        marker_without_carrier.extend_from_slice(&MERGED_MINING_MARKER);
        assert_eq!(
            validate_miner_data_commitment(&marker_without_carrier, AUX_HASH, &branch, index,),
            Err(AuxPowError::CommitmentNotMinerDataSuffix)
        );
    }
}
