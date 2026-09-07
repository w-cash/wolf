//! Exact zero-value transparent-output commitment carrier.

use sha2::{Digest, Sha256};

use crate::{
    coinbase::TransparentOutput,
    expected_auxiliary_index,
    merkle::{auxiliary_tree_size, sha256d_merkle_root},
    AuxPowError, MAX_AUXILIARY_BRANCH_DEPTH, MAX_TRANSPARENT_OUTPUTS, MAX_TRANSPARENT_SCRIPT_BYTES,
    MERGED_MINING_MARKER, WCASH_AUXILIARY_CHAIN_ID,
};

/// Hash-domain tag used before every Wcash auxiliary block hash.
pub const AUXILIARY_LEAF_DOMAIN: &[u8] = b"Wcash/ZcashAuxPoW/leaf/v1\0";

const OP_RETURN: u8 = 0x6a;
const PUSH_44: u8 = 44;
const SCRIPT_BYTES: usize = 46;
const ROOT_START: usize = 2 + MERGED_MINING_MARKER.len();
const ROOT_END: usize = ROOT_START + 32;
const TREE_SIZE_END: usize = ROOT_END + 4;

/// Domain-separates a Wcash auxiliary block ID before Merkle-tree insertion.
///
/// `auxiliary_block_hash` is the exact raw 32-byte Wcash block ID stored by the
/// node (`block::Hash.0`), not its reversed big-endian display encoding. The
/// returned leaf is also in raw SHA-256d/Merkle byte order.
pub fn auxiliary_leaf(auxiliary_block_hash: [u8; 32]) -> [u8; 32] {
    auxiliary_leaf_for_chain(WCASH_AUXILIARY_CHAIN_ID, auxiliary_block_hash)
}

/// Constructs the one canonical merge-mining commitment script for a pool.
///
/// The block ID and every node in `auxiliary_branch` use raw serialized order,
/// never conventional reversed display order.
pub fn commitment_script(
    auxiliary_block_hash: [u8; 32],
    auxiliary_branch: &[[u8; 32]],
    auxiliary_index: u32,
    nonce: u32,
) -> Result<Vec<u8>, AuxPowError> {
    check_auxiliary_position(auxiliary_branch, auxiliary_index, nonce)?;
    let tree_size = auxiliary_tree_size(auxiliary_branch.len())?;
    let mut root = sha256d_merkle_root(
        auxiliary_leaf(auxiliary_block_hash),
        auxiliary_branch,
        auxiliary_index,
    )?;
    // Namecoin-compatible carriers store the internal/raw root in reverse order.
    root.reverse();

    let mut script = Vec::with_capacity(SCRIPT_BYTES);
    script.extend_from_slice(&[OP_RETURN, PUSH_44]);
    script.extend_from_slice(&MERGED_MINING_MARKER);
    script.extend_from_slice(&root);
    script.extend_from_slice(&tree_size.to_le_bytes());
    script.extend_from_slice(&nonce.to_le_bytes());
    Ok(script)
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

/// Validates the exact Wcash carrier in authenticated transparent outputs.
///
/// Every marker occurrence is counted across every bounded script. Alternate
/// push opcodes, prefixes, suffixes, nonzero value, and duplicate markers all
/// fail closed.
pub fn validate_commitment(
    outputs: &[TransparentOutput],
    auxiliary_block_hash: [u8; 32],
    auxiliary_branch: &[[u8; 32]],
    auxiliary_index: u32,
) -> Result<ValidatedCommitment, AuxPowError> {
    if outputs.len() > MAX_TRANSPARENT_OUTPUTS {
        return Err(AuxPowError::TooManyTransparentOutputs {
            actual: outputs.len(),
            max: MAX_TRANSPARENT_OUTPUTS,
        });
    }
    if auxiliary_branch.len() > MAX_AUXILIARY_BRANCH_DEPTH {
        return Err(AuxPowError::BranchTooLong {
            branch: "auxiliary",
            actual: auxiliary_branch.len(),
            max: MAX_AUXILIARY_BRANCH_DEPTH,
        });
    }

    let mut carrier = None;
    for output in outputs {
        if output.script_pubkey().len() > MAX_TRANSPARENT_SCRIPT_BYTES {
            return Err(AuxPowError::TransparentScriptTooLarge {
                actual: output.script_pubkey().len(),
                max: MAX_TRANSPARENT_SCRIPT_BYTES,
            });
        }
        for window in output.script_pubkey().windows(MERGED_MINING_MARKER.len()) {
            if window == MERGED_MINING_MARKER && carrier.replace(output).is_some() {
                return Err(AuxPowError::DuplicateCommitmentMarker);
            }
        }
    }

    let carrier = carrier.ok_or(AuxPowError::MissingCommitmentOutput)?;
    let script = carrier.script_pubkey();
    if script.len() != SCRIPT_BYTES
        || script[0] != OP_RETURN
        || script[1] != PUSH_44
        || script[2..ROOT_START] != MERGED_MINING_MARKER
    {
        return Err(AuxPowError::CommitmentScriptMismatch);
    }
    if carrier.value() != 0 {
        return Err(AuxPowError::CommitmentOutputNotZero(carrier.value()));
    }

    let committed_tree_size = u32::from_le_bytes(copy_array(&script[ROOT_END..TREE_SIZE_END]));
    let nonce = u32::from_le_bytes(copy_array(&script[TREE_SIZE_END..SCRIPT_BYTES]));
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
    let mut committed_root = copy_array::<32>(&script[ROOT_START..ROOT_END]);
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

    fn valid_case() -> (Vec<[u8; 32]>, u32, Vec<TransparentOutput>) {
        let branch = vec![[0x11; 32], [0x22; 32], [0x33; 32]];
        let index = expected_auxiliary_index(NONCE, branch.len()).expect("valid depth");
        let script = commitment_script(AUX_HASH, &branch, index, NONCE)
            .expect("valid commitment parameters");
        let outputs = vec![
            TransparentOutput::new(50, vec![0x51]),
            TransparentOutput::new(0, script),
        ];
        (branch, index, outputs)
    }

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
    fn exact_zero_value_carrier_validates() {
        let (branch, index, outputs) = valid_case();
        assert_eq!(
            hex::encode(outputs[1].script_pubkey()),
            "6a2cfabe6d6dfd3ef87a4d51f460ac00d7ddc60430b5e9c210ed62e82d1984942ba8ad1de63e0800000007000000"
        );
        let commitment = validate_commitment(&outputs, AUX_HASH, &branch, index)
            .expect("exact carrier validates");
        assert_eq!(commitment.nonce(), NONCE);
        assert_eq!(commitment.tree_size(), 8);
        assert_eq!(commitment.auxiliary_index(), index);
    }

    #[test]
    fn alternate_and_duplicate_carriers_fail_closed() {
        let (branch, index, outputs) = valid_case();

        let mut prefixed = outputs.clone();
        let mut script = prefixed[1].script_pubkey().to_vec();
        script.insert(0, 0);
        prefixed[1] = TransparentOutput::new(0, script);
        assert_eq!(
            validate_commitment(&prefixed, AUX_HASH, &branch, index),
            Err(AuxPowError::CommitmentScriptMismatch)
        );

        let mut suffixed = outputs.clone();
        let mut script = suffixed[1].script_pubkey().to_vec();
        script.push(0);
        suffixed[1] = TransparentOutput::new(0, script);
        assert_eq!(
            validate_commitment(&suffixed, AUX_HASH, &branch, index),
            Err(AuxPowError::CommitmentScriptMismatch)
        );

        let mut alternate_push = outputs.clone();
        let mut script = alternate_push[1].script_pubkey().to_vec();
        script[1] = 0x4c;
        alternate_push[1] = TransparentOutput::new(0, script);
        assert_eq!(
            validate_commitment(&alternate_push, AUX_HASH, &branch, index),
            Err(AuxPowError::CommitmentScriptMismatch)
        );

        let mut duplicate = outputs.clone();
        duplicate.push(outputs[1].clone());
        assert_eq!(
            validate_commitment(&duplicate, AUX_HASH, &branch, index),
            Err(AuxPowError::DuplicateCommitmentMarker)
        );

        let mut double_marker = outputs;
        let mut script = double_marker[1].script_pubkey().to_vec();
        script.extend_from_slice(&MERGED_MINING_MARKER);
        double_marker[1] = TransparentOutput::new(0, script);
        assert_eq!(
            validate_commitment(&double_marker, AUX_HASH, &branch, index),
            Err(AuxPowError::DuplicateCommitmentMarker)
        );
    }

    #[test]
    fn every_commitment_binding_is_enforced() {
        let (branch, index, outputs) = valid_case();

        let mut nonzero = outputs.clone();
        nonzero[1] = TransparentOutput::new(1, nonzero[1].script_pubkey().to_vec());
        assert_eq!(
            validate_commitment(&nonzero, AUX_HASH, &branch, index),
            Err(AuxPowError::CommitmentOutputNotZero(1))
        );

        assert_eq!(
            validate_commitment(&outputs, [0x41; 32], &branch, index),
            Err(AuxPowError::AuxiliaryRootMismatch)
        );

        let mut wrong_size = outputs.clone();
        let mut script = wrong_size[1].script_pubkey().to_vec();
        script[ROOT_END..TREE_SIZE_END].copy_from_slice(&4u32.to_le_bytes());
        wrong_size[1] = TransparentOutput::new(0, script);
        assert!(matches!(
            validate_commitment(&wrong_size, AUX_HASH, &branch, index),
            Err(AuxPowError::AuxiliaryTreeSizeMismatch { .. })
        ));

        let wrong_index = (index + 1) % 8;
        assert!(matches!(
            validate_commitment(&outputs, AUX_HASH, &branch, wrong_index),
            Err(AuxPowError::AuxiliaryIndexMismatch { .. })
        ));

        let mut wrong_branch = branch;
        wrong_branch[0][0] ^= 1;
        assert_eq!(
            validate_commitment(&outputs, AUX_HASH, &wrong_branch, index),
            Err(AuxPowError::AuxiliaryRootMismatch)
        );
    }

    #[test]
    fn missing_and_oversized_carriers_fail_before_scanning_unbounded_data() {
        let (branch, index, _) = valid_case();
        assert_eq!(
            validate_commitment(
                &[TransparentOutput::new(0, vec![OP_RETURN, 0])],
                AUX_HASH,
                &branch,
                index,
            ),
            Err(AuxPowError::MissingCommitmentOutput)
        );

        let oversized_script = vec![0; MAX_TRANSPARENT_SCRIPT_BYTES + 1];
        assert!(matches!(
            validate_commitment(
                &[TransparentOutput::new(0, oversized_script)],
                AUX_HASH,
                &branch,
                index,
            ),
            Err(AuxPowError::TransparentScriptTooLarge { .. })
        ));

        let outputs =
            vec![TransparentOutput::new(0, Vec::<u8>::new()); MAX_TRANSPARENT_OUTPUTS + 1];
        assert!(matches!(
            validate_commitment(&outputs, AUX_HASH, &branch, index),
            Err(AuxPowError::TooManyTransparentOutputs { .. })
        ));
    }
}
