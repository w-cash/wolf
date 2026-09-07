//! Consensus-defensive Zcash-parent AuxPoW primitives for Wcash.
//!
//! A proof binds one Wcash auxiliary block hash to an exact suffix in the miner
//! data of a canonical ZIP-244 Zcash coinbase transaction. The coinbase
//! authorizing-data digest is bound to the parent header's block-commitments
//! hash, while its transaction ID is independently bound to the transaction
//! Merkle root. The exact parent header is then authenticated by Zcash Equihash
//! `(200, 9)` work.
//!
//! The parent header's `nBits` field is never used as Wcash's difficulty
//! authority. Callers must supply the target derived from authenticated Wcash
//! chain state.
//!
//! # Consensus byte order
//!
//! Every 32-byte block ID, transaction ID, Merkle leaf, and Merkle branch node
//! passed to this crate is in **raw serialized order**: the exact byte order
//! stored in Zcash block headers and used as input to SHA-256d Merkle hashing.
//! For SHA-256d identifiers this is also their little-endian numeric order.
//! It is the reverse of the conventional big-endian hexadecimal display used
//! by Zebra, zcashd, and block explorers. Implementations must decode displayed
//! hashes and reverse all 32 bytes before using them here.

#![forbid(unsafe_code)]

use sha2::{Digest, Sha256};

mod codec;
mod coinbase;
mod commitment;
mod error;
mod header;
mod merkle;
mod proof;
mod target;

#[cfg(feature = "zebra")]
pub(crate) use coinbase::ZebraCoinbaseVerifier;
#[cfg(any(feature = "zebra", test))]
pub(crate) use coinbase::{CoinbaseSummary, CoinbaseVerifier};
pub use commitment::{
    auxiliary_leaf, commitment_payload, miner_data_commitment, validate_miner_data_commitment,
    ValidatedCommitment, AUXILIARY_LEAF_DOMAIN, MINER_DATA_COMMITMENT_BYTES,
};
pub use error::AuxPowError;
#[cfg(any(feature = "zebra", test))]
pub(crate) use header::{Equihash200_9, EquihashVerifier};
pub use header::{ParentBlockHash, ParentHeader, ValidatedParentWork};
pub use merkle::{
    auth_data_merkle_root, block_commitments_hash, expected_auxiliary_index, sha256d_merkle_root,
};
pub use proof::{AuxPowProof, ValidatedAuxPow};
pub use target::Target;

/// Current canonical proof-format version.
pub const PROOF_VERSION: u8 = 2;

/// Domain magic at the start of every encoded proof.
pub const PROOF_MAGIC: [u8; 4] = *b"WCAZ";

/// Frozen Wcash auxiliary-chain identifier (`"WCAS"`).
pub const WCASH_AUXILIARY_CHAIN_ID: u32 = 0x5743_4153;

/// Namecoin-compatible merged-mining marker.
pub const MERGED_MINING_MARKER: [u8; 4] = [0xfa, 0xbe, 0x6d, 0x6d];

/// Domain separator for the private parent-template payout-address attestation.
///
/// This commitment is an operator safety check, not part of Wcash consensus.
pub const PARENT_PAYOUT_COMMITMENT_DOMAIN: &[u8] = b"Wcash/Zcash parent payout address/v1\0";

/// Commits to the canonical encoded Zcash address configured on a parent
/// template node.
///
/// The merged-mining coordinator compares this value with the private GBT
/// extension returned by its loopback template node. This catches an
/// accidentally misconfigured parent reward recipient without sending the
/// plaintext address in an RPC request or writing it to coordinator logs.
pub fn parent_payout_address_commitment(encoded_address: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(PARENT_PAYOUT_COMMITMENT_DOMAIN);
    hasher.update(encoded_address.as_bytes());
    hasher.finalize().into()
}

/// Maximum encoded proof size accepted at a consensus boundary.
pub const MAX_PROOF_BYTES: usize = 256 * 1024;

/// Maximum serialized parent coinbase size accepted by this profile.
pub const MAX_COINBASE_BYTES: usize = 128 * 1024;

/// Maximum parent transaction Merkle-path depth.
pub const MAX_PARENT_BRANCH_DEPTH: usize = 32;

/// Maximum parent authorizing-data Merkle-path depth.
pub const MAX_AUTH_DATA_BRANCH_DEPTH: usize = 32;

/// Maximum auxiliary-tree depth.
pub const MAX_AUXILIARY_BRANCH_DEPTH: usize = 16;

/// Exact serialized size of a Zcash Equihash `(200, 9)` header.
pub const PARENT_HEADER_BYTES: usize = 4 + 32 * 3 + 4 * 2 + 32 + 3 + 1_344;

#[cfg(test)]
mod payout_tests {
    use super::parent_payout_address_commitment;

    #[test]
    fn parent_payout_commitment_is_domain_separated_and_stable() {
        let commitment = parent_payout_address_commitment("u1test-address");
        assert_eq!(
            hex::encode(commitment),
            "7216074cdd20dba22c98f155b7cad07883de0a79b62d6584170948014695e427"
        );
        assert_ne!(
            commitment,
            parent_payout_address_commitment("u1test-address-2")
        );
    }
}
