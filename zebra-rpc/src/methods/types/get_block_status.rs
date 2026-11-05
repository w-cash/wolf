//! Exact committed Zcash block membership used by durable mining outboxes.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// A committed block's location. Unknown includes blocks still being validated;
/// submission queues and remembered hashes never establish a side-chain commit.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum GetBlockStatusResponse {
    /// The exact block is in an atomic best-chain block-and-depth snapshot.
    BestChain {
        /// Canonical display-order block hash.
        hash: String,
        /// Height encoded in the committed coinbase.
        height: u32,
        /// One plus the depth from the same best-chain snapshot.
        confirmations: u32,
    },
    /// The exact block is committed to a retained non-finalized side chain.
    SideChain {
        /// Canonical display-order block hash.
        hash: String,
        /// Height encoded in the committed coinbase.
        height: u32,
    },
    /// No committed exact block was found; this is not an acceptance verdict.
    Unknown {},
}
