//! Canonical Zcash parent types isolated from Wcash transaction consensus.
//!
//! The modern parser is pinned independently of Wcash's historical parser. It
//! does not authorize a new parent domain in Wcash consensus: callers must
//! select the candidate Wcash block's explicit parent-proof policy first.

#![forbid(unsafe_code)]

use std::{fmt, io::Cursor};

pub use zec_parent_address as address;
pub use zec_parent_address::ZcashAddress;
pub use zec_parent_chain as chain;
pub use zec_parent_protocol::consensus::NetworkType;

mod payout;
pub use payout::recover_coinbase_payout;

/// The official Zcash NU7 consensus branch ID.
pub const NU7_BRANCH_ID: u32 = 0x7719_0ad9;
/// The existing Wcash parent-coinbase allocation bound.
pub const MAX_PARENT_COINBASE_BYTES: usize = 128 * 1024;

/// A bounded canonical-parent decoding failure, without private transaction data.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ParentError {
    /// The transaction exceeds the parent-coinbase size bound.
    CoinbaseTooLarge,
    /// Canonical transaction decoding or encoding failed.
    InvalidCoinbase,
    /// The encoding has trailing bytes or differs from its canonical round trip.
    NonCanonicalCoinbase,
    /// The transaction does not have exactly one null coinbase prevout.
    NotCoinbase,
    /// The transaction format is not v5 or v6.
    UnsupportedVersion(u32),
    /// The embedded branch is not the official NU7 domain.
    UnsupportedBranch(u32),
}

impl fmt::Display for ParentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CoinbaseTooLarge => formatter.write_str("parent coinbase exceeds its size bound"),
            Self::InvalidCoinbase => formatter.write_str("invalid canonical parent coinbase"),
            Self::NonCanonicalCoinbase => {
                formatter.write_str("non-canonical parent coinbase encoding")
            }
            Self::NotCoinbase => formatter.write_str("parent transaction is not a coinbase"),
            Self::UnsupportedVersion(version) => {
                write!(formatter, "unsupported parent coinbase version {version}")
            }
            Self::UnsupportedBranch(branch) => {
                write!(
                    formatter,
                    "unsupported parent coinbase branch {branch:#010x}"
                )
            }
        }
    }
}

impl std::error::Error for ParentError {}

/// Parses one bounded, exact, canonical NU7 v5/v6 coinbase.
///
/// The pinned Zebra parser retains its coinbase script/BIP34 and Sapling-spend
/// checks. Both transaction identifiers are computed from the exact embedded
/// NU7 domain. This performs structural parsing, not full parent-chain
/// consensus validation or proof-of-work verification.
pub fn canonical_nu7_coinbase(
    bytes: &[u8],
) -> Result<chain::transaction::Transaction, ParentError> {
    use chain::{
        serialization::{ZcashDeserialize, ZcashSerialize},
        transaction::Transaction,
    };

    if bytes.len() > MAX_PARENT_COINBASE_BYTES {
        return Err(ParentError::CoinbaseTooLarge);
    }
    let mut cursor = Cursor::new(bytes);
    let transaction =
        Transaction::zcash_deserialize(&mut cursor).map_err(|_| ParentError::InvalidCoinbase)?;
    if cursor.position() != u64::try_from(bytes.len()).map_err(|_| ParentError::InvalidCoinbase)? {
        return Err(ParentError::NonCanonicalCoinbase);
    }
    if !matches!(transaction.version(), 5 | 6) {
        return Err(ParentError::UnsupportedVersion(transaction.version()));
    }
    let branch = u32::from(transaction.consensus_branch_id());
    if branch != NU7_BRANCH_ID {
        return Err(ParentError::UnsupportedBranch(branch));
    }
    if !transaction.is_coinbase() {
        return Err(ParentError::NotCoinbase);
    }
    if transaction
        .zcash_serialize_to_vec()
        .map_err(|_| ParentError::InvalidCoinbase)?
        != bytes
    {
        return Err(ParentError::NonCanonicalCoinbase);
    }
    Ok(transaction)
}

#[cfg(test)]
mod tests;
