//! SHA-256d Merkle paths and deterministic auxiliary-tree slots.

use sha2::{Digest, Sha256};

use crate::{
    AuxPowError, MAX_AUTH_DATA_BRANCH_DEPTH, MAX_PARENT_BRANCH_DEPTH, WCASH_AUXILIARY_CHAIN_ID,
};

const AUTH_DATA_HASH_PERSONALIZATION: &[u8; 16] = b"ZcashAuthDatHash";
const BLOCK_COMMITMENTS_PERSONALIZATION: &[u8; 16] = b"ZcashBlockCommit";

/// Reconstructs a raw-byte-order SHA-256d Merkle root.
///
/// `leaf`, every `branch` node, and the returned root are raw digest bytes in
/// serialized Merkle order. Callers must reverse conventional block-explorer
/// display hex before passing it here.
///
/// An even index hashes `current || sibling`; an odd index hashes
/// `sibling || current`. Index bits above the supplied path are rejected.
pub fn sha256d_merkle_root(
    mut leaf: [u8; 32],
    branch: &[[u8; 32]],
    mut index: u32,
) -> Result<[u8; 32], AuxPowError> {
    if branch.len() > MAX_PARENT_BRANCH_DEPTH {
        return Err(AuxPowError::BranchTooLong {
            branch: "Merkle",
            actual: branch.len(),
            max: MAX_PARENT_BRANCH_DEPTH,
        });
    }

    let original_index = index;
    for sibling in branch {
        leaf = if index & 1 == 0 {
            sha256d_pair(&leaf, sibling)
        } else {
            sha256d_pair(sibling, &leaf)
        };
        index >>= 1;
    }
    if index != 0 {
        let tree_size = 1u64 << branch.len();
        return Err(AuxPowError::MerkleIndexOutOfRange {
            tree: "Merkle",
            index: original_index,
            tree_size,
        });
    }
    Ok(leaf)
}

/// Reconstructs a ZIP-244 authorizing-data Merkle root.
///
/// `leaf`, every branch node, and the returned root use raw serialized byte
/// order. ZIP-244 pads the complete tree with zero leaves to a power of two;
/// those padding nodes, where applicable, must already be present in `branch`.
pub fn auth_data_merkle_root(
    mut leaf: [u8; 32],
    branch: &[[u8; 32]],
    mut index: u32,
) -> Result<[u8; 32], AuxPowError> {
    if branch.len() > MAX_AUTH_DATA_BRANCH_DEPTH {
        return Err(AuxPowError::BranchTooLong {
            branch: "auth-data",
            actual: branch.len(),
            max: MAX_AUTH_DATA_BRANCH_DEPTH,
        });
    }

    let original_index = index;
    for sibling in branch {
        leaf = if index & 1 == 0 {
            auth_data_hash(&leaf, sibling)
        } else {
            auth_data_hash(sibling, &leaf)
        };
        index >>= 1;
    }
    if index != 0 {
        let tree_size = 1u64 << branch.len();
        return Err(AuxPowError::MerkleIndexOutOfRange {
            tree: "auth-data",
            index: original_index,
            tree_size,
        });
    }

    Ok(leaf)
}

/// Computes the NU5-and-later Zcash `hashBlockCommitments` value.
///
/// Both inputs and the result are the raw 32-byte values serialized in their
/// respective Zcash structures.
pub fn block_commitments_hash(chain_history_root: [u8; 32], auth_data_root: [u8; 32]) -> [u8; 32] {
    blake2b_simd::Params::new()
        .hash_length(32)
        .personal(BLOCK_COMMITMENTS_PERSONALIZATION)
        .to_state()
        .update(&chain_history_root)
        .update(&auth_data_root)
        .update(&[0; 32])
        .finalize()
        .as_bytes()
        .try_into()
        .expect("the requested BLAKE2b digest length is 32 bytes")
}

/// Derives Wcash's slot using the established Namecoin wrapping-`u32` rule.
pub fn expected_auxiliary_index(nonce: u32, branch_depth: usize) -> Result<u32, AuxPowError> {
    let size = auxiliary_tree_size(branch_depth)?;
    let mut random = nonce;
    random = random.wrapping_mul(1_103_515_245).wrapping_add(12_345);
    random = random.wrapping_add(WCASH_AUXILIARY_CHAIN_ID);
    random = random.wrapping_mul(1_103_515_245).wrapping_add(12_345);
    Ok(random % size)
}

pub(crate) fn auxiliary_tree_size(branch_depth: usize) -> Result<u32, AuxPowError> {
    if branch_depth > crate::MAX_AUXILIARY_BRANCH_DEPTH || branch_depth >= u32::BITS as usize {
        return Err(AuxPowError::AuxiliaryTreeDepthTooLarge(branch_depth));
    }
    // Safe because the branch-depth checks keep the shift below 32.
    Ok(1u32 << branch_depth)
}

pub(crate) fn sha256d(bytes: &[u8]) -> [u8; 32] {
    let first = Sha256::digest(bytes);
    Sha256::digest(first).into()
}

fn sha256d_pair(left: &[u8; 32], right: &[u8; 32]) -> [u8; 32] {
    let mut bytes = [0; 64];
    bytes[..32].copy_from_slice(left);
    bytes[32..].copy_from_slice(right);
    sha256d(&bytes)
}

fn auth_data_hash(left: &[u8; 32], right: &[u8; 32]) -> [u8; 32] {
    blake2b_simd::Params::new()
        .hash_length(32)
        .personal(AUTH_DATA_HASH_PERSONALIZATION)
        .to_state()
        .update(left)
        .update(right)
        .finalize()
        .as_bytes()
        .try_into()
        .expect("the requested BLAKE2b digest length is 32 bytes")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slot_and_merkle_orientation_are_frozen() {
        assert_eq!(WCASH_AUXILIARY_CHAIN_ID, 0x5743_4153);
        assert_eq!(expected_auxiliary_index(7, 3), Ok(4));
        assert_eq!(expected_auxiliary_index(u32::MAX, 16), Ok(38_508));

        let leaf = [1; 32];
        let branch = [[2; 32]];
        assert_ne!(
            sha256d_merkle_root(leaf, &branch, 0),
            sha256d_merkle_root(leaf, &branch, 1)
        );
        assert!(matches!(
            sha256d_merkle_root(leaf, &branch, 2),
            Err(AuxPowError::MerkleIndexOutOfRange { .. })
        ));
    }

    #[test]
    fn maximum_parent_depth_accepts_every_u32_index() {
        let branch = vec![[3; 32]; 32];
        assert!(sha256d_merkle_root([4; 32], &branch, u32::MAX).is_ok());
        let too_long = vec![[3; 32]; 33];
        assert!(matches!(
            sha256d_merkle_root([4; 32], &too_long, 0),
            Err(AuxPowError::BranchTooLong { .. })
        ));
    }

    #[test]
    fn zip244_auth_tree_and_block_commitment_vectors_are_frozen() {
        let left = [0x11; 32];
        let right = [0x22; 32];
        let root = auth_data_merkle_root(left, &[right], 0).expect("one-level path is valid");
        assert_eq!(
            hex::encode(root),
            "9b3411bdd5e394f5b649cf55757a144f81b1007d55806f34d7bd19664bfd1d20"
        );

        let commitment = block_commitments_hash([0x33; 32], root);
        assert_eq!(
            hex::encode(commitment),
            "5ab11bd9e5a53b33550d66ee2b8f45070a6254091b50a1cc7487d32b5319cf31"
        );

        #[cfg(feature = "zebra")]
        {
            use zebra_chain::{
                block::{
                    merkle::AuthDataRoot, ChainHistoryBlockTxAuthCommitmentHash,
                    ChainHistoryMmrRootHash,
                },
                transaction::AuthDigest,
            };

            let zebra_auth_root: [u8; 32] = [AuthDigest(left), AuthDigest(right)]
                .into_iter()
                .collect::<AuthDataRoot>()
                .into();
            assert_eq!(root, zebra_auth_root);

            let zebra_commitment: [u8; 32] =
                ChainHistoryBlockTxAuthCommitmentHash::from_commitments(
                    &ChainHistoryMmrRootHash::from([0x33; 32]),
                    &AuthDataRoot::from(root),
                )
                .into();
            assert_eq!(commitment, zebra_commitment);
        }
    }
}
