//! SHA-256d Merkle paths and deterministic auxiliary-tree slots.

use sha2::{Digest, Sha256};

use crate::{AuxPowError, MAX_PARENT_BRANCH_DEPTH, WCASH_AUXILIARY_CHAIN_ID};

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
}
