//! Parameter types for the `getblocktemplate` RPC.

use derive_getters::Getters;
use schemars::JsonSchema;
use zebra_chain::block;

use crate::methods::{hex_data::HexData, types::long_poll::LongPollId};

/// Defines whether the RPC method should generate a block template or attempt to validate a block
/// proposal.
#[derive(
    Clone, Debug, Default, serde::Deserialize, serde::Serialize, PartialEq, Eq, JsonSchema,
)]
#[serde(rename_all = "lowercase")]
pub enum GetBlockTemplateRequestMode {
    /// Indicates a request for a block template.
    #[default]
    Template,

    /// Indicates a request to validate block data.
    Proposal,
}

/// Valid `capabilities` values that indicate client-side support.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum GetBlockTemplateCapability {
    /// Long Polling support.
    /// Currently ignored by zebra.
    LongPoll,

    /// Information for coinbase transaction, default template data with the `coinbasetxn` field.
    /// Currently ignored by zebra.
    CoinbaseTxn,

    /// Coinbase value, template response provides a `coinbasevalue` field and omits `coinbasetxn` field.
    /// Currently ignored by zebra.
    CoinbaseValue,

    /// Components of the coinbase transaction.
    /// Currently ignored by zebra.
    CoinbaseAux,

    /// Currently ignored by zcashd and zebra.
    Proposal,

    /// Currently ignored by zcashd and zebra.
    ServerList,

    /// Currently ignored by zcashd and zebra.
    WorkId,

    /// Unknown capability to fill in for mutations.
    // TODO: Fill out valid mutations capabilities.
    //       The set of possible capabilities is open-ended, so we need to keep UnknownCapability.
    #[serde(other)]
    UnknownCapability,
}

/// A Wcash auxiliary-block commitment requested in a Zcash block template.
///
/// The block hash uses the conventional display byte order used by Zebra's
/// JSON-RPC methods and block explorers. Template construction converts it to
/// the raw byte order used by the Wcash commitment algorithm.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize, Getters, JsonSchema,
)]
#[serde(deny_unknown_fields)]
pub struct WcashAuxRequest {
    /// The proof-independent Wcash block hash to commit to.
    #[serde(rename = "blockhash", with = "hex")]
    #[schemars(with = "String")]
    #[getter(copy)]
    pub(crate) block_hash: block::Hash,

    /// The auxiliary-tree nonce used to derive the Wcash tree position.
    #[getter(copy)]
    pub(crate) nonce: u32,
}

impl WcashAuxRequest {
    /// Creates a request for one Wcash auxiliary block.
    pub fn new(block_hash: block::Hash, nonce: u32) -> Self {
        Self { block_hash, nonce }
    }
}

/// Optional parameter `jsonrequestobject` for `getblocktemplate` RPC request.
///
/// The `data` field must be provided in `proposal` mode, and must be omitted in `template` mode.
/// All other fields are optional.
#[derive(
    Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize, Default, Getters, JsonSchema,
)]
pub struct GetBlockTemplateParameters {
    /// Defines whether the RPC method should generate a block template or attempt to
    /// validate block data, checking against all of the server's usual acceptance rules
    /// (excluding the check for a valid proof-of-work).
    #[serde(default)]
    pub(crate) mode: GetBlockTemplateRequestMode,

    /// Must be omitted when `getblocktemplate` RPC is called in "template" mode (or when `mode` is omitted).
    /// Must be provided when `getblocktemplate` RPC is called in "proposal" mode.
    ///
    /// Hex-encoded block data to be validated and checked against the server's usual acceptance rules
    /// (excluding the check for a valid proof-of-work).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) data: Option<HexData>,

    /// A list of client-side supported capability features
    #[serde(default)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) capabilities: Vec<GetBlockTemplateCapability>,

    /// An ID that delays the RPC response until the template changes.
    ///
    /// In Zebra, the ID represents the chain tip, max time, and mempool contents.
    #[serde(rename = "longpollid")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) long_poll_id: Option<LongPollId>,

    /// The workid for the block template.
    ///
    /// currently unused.
    #[serde(rename = "workid")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) _work_id: Option<String>,

    /// Optional Wcash auxiliary-block commitment for template mode.
    ///
    /// This private extension is rejected in proposal mode and together with
    /// `longpollid`. It does not alter ordinary Zcash template requests.
    #[serde(rename = "wcashaux")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[getter(copy)]
    pub(crate) wcash_aux: Option<WcashAuxRequest>,
}

impl GetBlockTemplateParameters {
    /// Creates standard `getblocktemplate` parameters without a Wcash
    /// auxiliary commitment.
    ///
    /// Keeping the existing constructor shape avoids changing ordinary Zebra
    /// RPC clients. Wcash integrations use [`Self::with_wcash_aux`].
    pub fn new(
        mode: GetBlockTemplateRequestMode,
        data: Option<HexData>,
        capabilities: Vec<GetBlockTemplateCapability>,
        long_poll_id: Option<LongPollId>,
        work_id: Option<String>,
    ) -> Self {
        Self {
            mode,
            data,
            capabilities,
            long_poll_id,
            _work_id: work_id,
            wcash_aux: None,
        }
    }

    /// Adds one Wcash auxiliary-block commitment request.
    pub fn with_wcash_aux(mut self, wcash_aux: WcashAuxRequest) -> Self {
        self.wcash_aux = Some(wcash_aux);
        self
    }

    /// Returns Some(data) with the block proposal hexdata if in `Proposal` mode and `data` is provided.
    pub fn block_proposal_data(&self) -> Option<HexData> {
        match self {
            Self { data: None, .. }
            | Self {
                mode: GetBlockTemplateRequestMode::Template,
                ..
            } => None,

            Self {
                mode: GetBlockTemplateRequestMode::Proposal,
                data,
                ..
            } => data.clone(),
        }
    }
}
