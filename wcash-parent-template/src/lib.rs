//! Bounded commitment helpers for the Wcash-maintained Zcash parent node.
//!
//! This crate constructs and checks the coinbase carrier and hash commitments.
//! It contains no transaction parser, Wcash consensus profile or proof validator.

#![forbid(unsafe_code)]

use sha2::{Digest, Sha256};

mod commitment;
mod error;
mod merkle;

pub use commitment::{
    auxiliary_leaf, commitment_payload, miner_data_commitment, validate_miner_data_commitment,
    ValidatedCommitment, AUXILIARY_LEAF_DOMAIN, MINER_DATA_COMMITMENT_BYTES,
};
pub use error::AuxPowError;
pub use merkle::{
    auth_data_merkle_root, block_commitments_hash, expected_auxiliary_index, sha256d_merkle_root,
};

/// Frozen Wcash auxiliary-chain identifier (`"WCAS"`).
pub const WCASH_AUXILIARY_CHAIN_ID: u32 = 0x5743_4153;
/// The unique merged-mining carrier marker.
pub const MERGED_MINING_MARKER: [u8; 4] = [0xfa, 0xbe, 0x6d, 0x6d];
/// Maximum parent transaction Merkle-path depth.
pub const MAX_PARENT_BRANCH_DEPTH: usize = 32;
/// Maximum parent authorizing-data Merkle-path depth.
pub const MAX_AUTH_DATA_BRANCH_DEPTH: usize = 32;
/// Maximum auxiliary-tree depth.
pub const MAX_AUXILIARY_BRANCH_DEPTH: usize = 16;
/// Domain separator for the parent payout-address attestation.
pub const PARENT_PAYOUT_COMMITMENT_DOMAIN: &[u8] = b"Wcash/Zcash parent payout address/v1\0";

/// Commits to the exact configured Zcash payout-address encoding.
pub fn parent_payout_address_commitment(encoded_address: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(PARENT_PAYOUT_COMMITMENT_DOMAIN);
    hasher.update(encoded_address.as_bytes());
    hasher.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parent_payout_commitment_matches_the_frozen_backend_contract() {
        assert_eq!(
            hex::encode(parent_payout_address_commitment("u1test-address")),
            "7216074cdd20dba22c98f155b7cad07883de0a79b62d6584170948014695e427"
        );
        assert_ne!(
            parent_payout_address_commitment("u1test-address"),
            parent_payout_address_commitment("u1test-address-2")
        );
    }
}
