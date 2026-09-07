//! Typed fail-closed parsing and validation errors.

use std::{error::Error, fmt};

/// A rejected Zcash-parent AuxPoW proof or component.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AuxPowError {
    /// Complete proof exceeds its outer bound.
    ProofTooLarge {
        /// Supplied byte length.
        actual: usize,
        /// Consensus maximum.
        max: usize,
    },
    /// Coinbase bytes exceed their dedicated bound.
    CoinbaseTooLarge {
        /// Supplied byte length.
        actual: usize,
        /// Consensus maximum.
        max: usize,
    },
    /// Proof magic is not the Wcash Zcash-AuxPoW domain magic.
    InvalidProofMagic([u8; 4]),
    /// Proof-format version is unknown.
    UnsupportedProofVersion(u8),
    /// A fixed or declared field is truncated.
    UnexpectedEnd {
        /// Field being decoded.
        field: &'static str,
        /// Bytes required for the field.
        needed: usize,
        /// Bytes remaining in the proof.
        remaining: usize,
    },
    /// CompactSize used a longer representation than necessary.
    NonCanonicalCompactSize,
    /// CompactSize cannot fit the current platform's address space.
    CompactSizeOverflow(u64),
    /// A Merkle branch exceeds its explicit cap.
    BranchTooLong {
        /// Branch domain.
        branch: &'static str,
        /// Supplied sibling count.
        actual: usize,
        /// Maximum sibling count.
        max: usize,
    },
    /// Bytes remain after the exact proof.
    TrailingBytes(usize),
    /// Checked encoded-length arithmetic overflowed.
    EncodingLengthOverflow,
    /// Parent header is not exactly the fixed Zcash length.
    InvalidHeaderLength {
        /// Supplied header bytes.
        actual: usize,
        /// Exact required header bytes.
        expected: usize,
    },
    /// Zcash rejects a header version below four.
    HeaderVersionTooLow(u32),
    /// Zcash interprets the high bit as an invalid signed version.
    HeaderVersionHighBit(u32),
    /// Equihash solution length was not canonical `fd4005`.
    NonCanonicalSolutionLength,
    /// The authenticated Wcash target was zero.
    ZeroTarget,
    /// Parent header hash exceeds the authenticated Wcash target.
    InsufficientParentWork {
        /// Parent hash in little-endian numeric order.
        hash_le: [u8; 32],
        /// Required target in little-endian numeric order.
        target_le: [u8; 32],
    },
    /// Equihash `(200, 9)` rejected the parent header.
    InvalidEquihash,
    /// The supplied coinbase adapter rejected the exact transaction bytes.
    InvalidParentCoinbase,
    /// Re-encoding the parsed Zcash coinbase did not reproduce the proof bytes.
    NonCanonicalParentCoinbase,
    /// The parent transaction is not structurally a coinbase transaction.
    ParentTransactionIsNotCoinbase,
    /// Proof v2 requires a ZIP-244 transaction with an authorizing-data digest.
    UnsupportedParentCoinbaseVersion(u32),
    /// A parsed coinbase did not expose its authenticated miner data.
    MissingParentCoinbaseMinerData,
    /// The explicit parent Merkle index is not the required coinbase slot zero.
    ParentCoinbaseIndexNotZero(u32),
    /// Coinbase transaction path does not reach the parent header root.
    ParentMerkleRootMismatch,
    /// The explicit authorizing-data Merkle index is not coinbase slot zero.
    ParentAuthDataIndexNotZero(u32),
    /// Transaction-ID and authorizing-data paths imply different tree depths.
    ParentMerkleDepthMismatch {
        /// Transaction-ID Merkle-path depth.
        transaction: usize,
        /// Authorizing-data Merkle-path depth.
        auth_data: usize,
    },
    /// Coinbase authorizing-data path is not bound by the parent header.
    ParentBlockCommitmentsMismatch,
    /// Auxiliary branch cannot form a bounded power-of-two tree.
    AuxiliaryTreeDepthTooLarge(usize),
    /// A Merkle index lies outside the tree implied by its branch depth.
    MerkleIndexOutOfRange {
        /// Tree domain.
        tree: &'static str,
        /// Supplied leaf index.
        index: u32,
        /// Leaf count implied by the branch depth.
        tree_size: u64,
    },
    /// Carrier tree size differs from auxiliary branch depth.
    AuxiliaryTreeSizeMismatch {
        /// Tree size encoded in the carrier.
        committed: u32,
        /// Tree size implied by branch depth.
        expected: u32,
    },
    /// Proof index differs from the deterministic Wcash slot.
    AuxiliaryIndexMismatch {
        /// Supplied auxiliary index.
        actual: u32,
        /// Index derived from chain ID and nonce.
        expected: u32,
    },
    /// No version-2 commitment occurs in the coinbase miner data.
    MissingMinerDataCommitment,
    /// The marker-bearing version-2 carrier is not the exact miner-data suffix.
    CommitmentNotMinerDataSuffix,
    /// More than one marker occurrence makes the carrier ambiguous.
    DuplicateCommitmentMarker,
    /// Committed auxiliary root differs from the proof.
    AuxiliaryRootMismatch,
}

impl fmt::Display for AuxPowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ProofTooLarge { actual, max } => {
                write!(f, "proof is {actual} bytes, maximum is {max}")
            }
            Self::CoinbaseTooLarge { actual, max } => {
                write!(f, "parent coinbase is {actual} bytes, maximum is {max}")
            }
            Self::InvalidProofMagic(magic) => write!(f, "invalid proof magic {magic:02x?}"),
            Self::UnsupportedProofVersion(version) => {
                write!(f, "unsupported Zcash AuxPoW proof version {version}")
            }
            Self::UnexpectedEnd {
                field,
                needed,
                remaining,
            } => write!(
                f,
                "truncated {field}: need {needed} bytes, only {remaining} remain"
            ),
            Self::NonCanonicalCompactSize => f.write_str("non-canonical CompactSize integer"),
            Self::CompactSizeOverflow(value) => {
                write!(f, "CompactSize value {value} does not fit usize")
            }
            Self::BranchTooLong {
                branch,
                actual,
                max,
            } => write!(f, "{branch} branch has {actual} siblings, maximum is {max}"),
            Self::TrailingBytes(count) => write!(f, "{count} trailing proof bytes"),
            Self::EncodingLengthOverflow => f.write_str("encoded proof length overflowed"),
            Self::InvalidHeaderLength { actual, expected } => {
                write!(f, "parent header is {actual} bytes, expected {expected}")
            }
            Self::HeaderVersionTooLow(version) => {
                write!(f, "Zcash header version {version} is below 4")
            }
            Self::HeaderVersionHighBit(version) => {
                write!(
                    f,
                    "Zcash header version 0x{version:08x} has its high bit set"
                )
            }
            Self::NonCanonicalSolutionLength => {
                f.write_str("Zcash Equihash solution length is not canonical fd4005")
            }
            Self::ZeroTarget => f.write_str("Wcash target must be nonzero"),
            Self::InsufficientParentWork { hash_le, target_le } => write!(
                f,
                "parent hash {hash_le:02x?} exceeds Wcash target {target_le:02x?}"
            ),
            Self::InvalidEquihash => f.write_str("invalid Zcash Equihash (200, 9) solution"),
            Self::InvalidParentCoinbase => f.write_str("invalid serialized Zcash coinbase"),
            Self::NonCanonicalParentCoinbase => {
                f.write_str("Zcash coinbase does not have one canonical byte encoding")
            }
            Self::ParentTransactionIsNotCoinbase => {
                f.write_str("parent transaction is not a coinbase")
            }
            Self::UnsupportedParentCoinbaseVersion(version) => write!(
                f,
                "Zcash AuxPoW v2 requires a version 5 or 6 parent coinbase, got version {version}"
            ),
            Self::MissingParentCoinbaseMinerData => {
                f.write_str("parent coinbase input has no authenticated miner data")
            }
            Self::ParentCoinbaseIndexNotZero(index) => {
                write!(f, "parent coinbase Merkle index is {index}, expected zero")
            }
            Self::ParentMerkleRootMismatch => {
                f.write_str("coinbase branch does not match the parent header Merkle root")
            }
            Self::ParentAuthDataIndexNotZero(index) => {
                write!(f, "parent coinbase auth-data index is {index}, expected zero")
            }
            Self::ParentMerkleDepthMismatch {
                transaction,
                auth_data,
            } => write!(
                f,
                "parent transaction Merkle depth {transaction} differs from auth-data depth {auth_data}"
            ),
            Self::ParentBlockCommitmentsMismatch => f.write_str(
                "coinbase auth-data branch and chain-history root do not match the parent block-commitments hash",
            ),
            Self::AuxiliaryTreeDepthTooLarge(depth) => {
                write!(f, "auxiliary branch depth {depth} is unsupported")
            }
            Self::MerkleIndexOutOfRange {
                tree,
                index,
                tree_size,
            } => write!(f, "{tree} index {index} lies outside tree size {tree_size}"),
            Self::AuxiliaryTreeSizeMismatch {
                committed,
                expected,
            } => write!(
                f,
                "commitment tree size is {committed}, expected {expected}"
            ),
            Self::AuxiliaryIndexMismatch { actual, expected } => {
                write!(
                    f,
                    "auxiliary index is {actual}, deterministic slot is {expected}"
                )
            }
            Self::MissingMinerDataCommitment => {
                f.write_str("Zcash coinbase miner-data commitment is missing")
            }
            Self::CommitmentNotMinerDataSuffix => {
                f.write_str("Zcash coinbase commitment is not the exact miner-data suffix")
            }
            Self::DuplicateCommitmentMarker => {
                f.write_str("merged-mining commitment marker is duplicated or ambiguous")
            }
            Self::AuxiliaryRootMismatch => {
                f.write_str("committed auxiliary root does not match the proof")
            }
        }
    }
}

impl Error for AuxPowError {}
