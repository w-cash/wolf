//! Consensus-defensive Zcash-parent AuxPoW primitives for Wcash.
//!
//! A proof binds one Wcash auxiliary block hash to an exact, zero-value
//! transparent output in a canonical Zcash coinbase transaction. The coinbase
//! transaction is then bound to an exact Zcash Equihash `(200, 9)` parent
//! header by a SHA-256d transaction Merkle path.
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

mod codec;
mod coinbase;
mod commitment;
mod error;
mod header;
mod merkle;
mod proof;
mod target;

pub use coinbase::TransparentOutput;
#[cfg(feature = "zebra")]
pub(crate) use coinbase::ZebraCoinbaseVerifier;
#[cfg(any(feature = "zebra", test))]
pub(crate) use coinbase::{CoinbaseSummary, CoinbaseVerifier};
pub use commitment::{auxiliary_leaf, commitment_script, validate_commitment, ValidatedCommitment};
pub use error::AuxPowError;
#[cfg(any(feature = "zebra", test))]
pub(crate) use header::{Equihash200_9, EquihashVerifier};
pub use header::{ParentBlockHash, ParentHeader, ValidatedParentWork};
pub use merkle::{expected_auxiliary_index, sha256d_merkle_root};
pub use proof::{AuxPowProof, ValidatedAuxPow};
pub use target::Target;

/// Current canonical proof-format version.
pub const PROOF_VERSION: u8 = 1;

/// Domain magic at the start of every encoded proof.
pub const PROOF_MAGIC: [u8; 4] = *b"WCAZ";

/// Frozen Wcash auxiliary-chain identifier (`"WCAS"`).
pub const WCASH_AUXILIARY_CHAIN_ID: u32 = 0x5743_4153;

/// Namecoin-compatible merged-mining marker.
pub const MERGED_MINING_MARKER: [u8; 4] = [0xfa, 0xbe, 0x6d, 0x6d];

/// Maximum encoded proof size accepted at a consensus boundary.
pub const MAX_PROOF_BYTES: usize = 256 * 1024;

/// Maximum serialized parent coinbase size accepted by this profile.
pub const MAX_COINBASE_BYTES: usize = 128 * 1024;

/// Maximum parent transaction Merkle-path depth.
pub const MAX_PARENT_BRANCH_DEPTH: usize = 32;

/// Maximum auxiliary-tree depth.
pub const MAX_AUXILIARY_BRANCH_DEPTH: usize = 16;

/// Exact serialized size of a Zcash Equihash `(200, 9)` header.
pub const PARENT_HEADER_BYTES: usize = 4 + 32 * 3 + 4 * 2 + 32 + 3 + 1_344;

/// Maximum transparent outputs accepted from the parent coinbase adapter.
pub const MAX_TRANSPARENT_OUTPUTS: usize = 4_096;

/// Maximum bytes scanned in any transparent output script.
pub const MAX_TRANSPARENT_SCRIPT_BYTES: usize = 10_000;
