//! Exact predecessor state used to calculate a merged-mining parent reward.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use zebra_chain::block;

/// Versioned, exact-parent reward context. This is metadata, not proof of
/// independent consensus validation or permission to change the parent block.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WcashParentContext {
    /// Contract version; clients must require version 1.
    pub version: u32,
    /// Canonical display-order hash of the candidate's predecessor.
    #[serde(rename = "previousblockhash")]
    pub previous_block_hash: String,
    /// Candidate height, exactly one greater than the predecessor's height.
    pub height: u32,
    /// NSM balance after the predecessor, in integer zatoshis. The candidate's
    /// activation seed, if any, has not been added to this balance.
    #[serde(rename = "parentnsmvaluebalance")]
    pub parent_nsm_value_balance: u64,
}

impl WcashParentContext {
    pub(crate) fn new(
        previous_block_hash: block::Hash,
        height: block::Height,
        parent_nsm_value_balance: u64,
    ) -> Self {
        Self {
            version: 1,
            previous_block_hash: previous_block_hash.to_string(),
            height: height.0,
            parent_nsm_value_balance,
        }
    }

    pub(crate) fn matches_template(&self, previous_block_hash: block::Hash, height: u32) -> bool {
        self.version == 1
            && self.previous_block_hash == previous_block_hash.to_string()
            && self.height == height
    }
}
