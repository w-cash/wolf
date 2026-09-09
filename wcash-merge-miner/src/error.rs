//! Error types for local job construction and share validation.

use std::io;

use thiserror::Error;

use zebra_chain::serialization::SerializationError;

/// A local merge-miner configuration, encoding, solver, or protocol error.
#[derive(Debug, Error)]
pub enum MinerError {
    /// The strict AuxPoW implementation rejected a component or proof.
    #[error(transparent)]
    AuxPow(#[from] wcash_zcash_aux::AuxPowError),

    /// Zebra could not serialize the constructed coinbase.
    #[error("could not serialize parent coinbase: {0}")]
    CoinbaseSerialization(#[source] io::Error),

    /// A Zebra header is not using the dedicated Wcash wire version and
    /// AuxPoW solution carrier.
    #[error("header is not a Wcash AuxPoW header (version 0x{version:08x})")]
    NotWcashHeader {
        /// The rejected header version.
        version: u32,
    },

    /// A Wcash header contains a zero, negative, or overflowing compact target.
    #[error("Wcash header contains an invalid compact difficulty target")]
    InvalidChildDifficulty,

    /// The header supplied for proof attachment is not the header committed by
    /// the prepared parent job.
    #[error("Wcash header ID changed after the parent job was prepared")]
    ChildHeaderMismatch,

    /// The verified proof could not be placed in Zebra's bounded Wcash carrier.
    #[error("could not encode the Wcash AuxPoW header witness: {0}")]
    WcashWitnessEncoding(#[source] SerializationError),

    /// A parent output amount is outside the Zcash monetary range.
    #[error("invalid parent output amount: {0}")]
    InvalidParentAmount(u64),

    /// The sum of supplied parent outputs is outside the Zcash monetary range.
    #[error("parent output total exceeds 21 million ZEC")]
    ParentOutputTotalTooLarge,

    /// A fixed-size hexadecimal field has the wrong length or malformed data.
    #[error("invalid {field}: {reason}")]
    InvalidHexField {
        /// Name of the rejected field.
        field: &'static str,
        /// Human-readable failure reason.
        reason: String,
    },

    /// A numeric command-line field was malformed.
    #[error("invalid {field}: {reason}")]
    InvalidNumericField {
        /// Name of the rejected field.
        field: &'static str,
        /// Human-readable failure reason.
        reason: String,
    },

    /// The parent block version is not valid for current Zcash headers.
    #[error("parent header version must be in 4..=0x7fffffff, got 0x{0:08x}")]
    InvalidParentVersion(u32),

    /// The local harness never creates the special height-zero Zcash coinbase.
    #[error("parent coinbase height must be greater than zero")]
    ZeroParentHeight,

    /// The coinbase input data is too large for the consensus coinbase script bound.
    #[error("parent coinbase input data is too long")]
    CoinbaseDataTooLong,

    /// A submitted nonce is not exactly 32 bytes.
    #[error("Equihash nonce is {0} bytes, expected 32")]
    InvalidNonceLength(usize),

    /// A submitted solution is not exactly the `(200, 9)` compressed size.
    #[error("Equihash solution is {0} bytes, expected 1344")]
    InvalidSolutionLength(usize),

    /// A submitted header time is not the exact frozen job time.
    #[error("submitted header time differs from the frozen native job")]
    SubmittedTimeMismatch,

    /// The bounded solver search ended without acceptable work.
    #[error("no target-valid Equihash solution found in {attempted} nonce runs")]
    SolverExhausted {
        /// Number of full Equihash solver runs attempted.
        attempted: u64,
    },

    /// The caller cancelled a bounded solver search.
    #[error("solver was cancelled after {attempted} nonce runs")]
    SolverCancelled {
        /// Number of full Equihash solver runs attempted before cancellation.
        attempted: u64,
    },

    /// The configured nonce interval overflowed `u64`.
    #[error("solver nonce range overflowed")]
    NonceRangeOverflow,

    /// A JSON-lines request exceeded the development server's frame cap.
    #[error("JSON request exceeds the {0}-byte local protocol limit")]
    RequestTooLarge(usize),

    /// A protocol request was not valid for this local server.
    #[error("invalid local protocol request: {0}")]
    InvalidRequest(String),

    /// A networking or stream error occurred.
    #[error(transparent)]
    Io(#[from] io::Error),

    /// JSON encoding or decoding failed.
    #[error(transparent)]
    Json(#[from] serde_json::Error),

    /// A parent-node RPC endpoint or quorum setting is unsafe or malformed.
    #[error("invalid parent RPC configuration: {0}")]
    RpcConfiguration(String),

    /// A parent-node RPC request failed before a valid JSON-RPC response arrived.
    #[error("parent RPC transport failed: {0}")]
    RpcTransport(#[source] reqwest::Error),

    /// A parent-node RPC endpoint returned a non-successful HTTP status.
    #[error("parent RPC returned HTTP status {0}")]
    RpcHttpStatus(reqwest::StatusCode),

    /// A parent-node RPC response exceeded the fixed wire-size limit.
    #[error("parent RPC response exceeds {0} bytes")]
    RpcResponseTooLarge(usize),

    /// A parent-node response violated JSON-RPC framing.
    #[error("invalid parent RPC response: {0}")]
    RpcProtocol(String),

    /// A parent node returned a well-formed JSON-RPC error object.
    #[error(
        "parent RPC endpoint {endpoint} returned error {code}: {message}",
        code = code.map_or_else(|| "unknown".to_string(), |code| code.to_string())
    )]
    RpcError {
        /// Credential-free endpoint label.
        endpoint: String,
        /// Numeric JSON-RPC error code, when supplied by the node.
        code: Option<i64>,
        /// Node-provided diagnostic message.
        message: String,
    },

    /// An exact candidate parent block failed proposal validation.
    #[error("parent proposal rejected by {endpoint}: {reason}")]
    ParentProposalRejected {
        /// Redacted endpoint label used for operational diagnosis.
        endpoint: String,
        /// Node-provided rejection reason.
        reason: String,
    },

    /// Parent nodes did not agree on the exact chain tip used by the job.
    #[error("parent tip mismatch: expected {expected}, {endpoint} returned {actual}")]
    ParentTipMismatch {
        /// Expected parent tip in conventional display order.
        expected: String,
        /// Redacted endpoint label.
        endpoint: String,
        /// Actual parent tip in conventional display order.
        actual: String,
    },

    /// The Wcash candidate became stale while its parent proposal was checked.
    #[error("Wcash tip mismatch: expected {expected}, {endpoint} returned {actual}")]
    ChildTipMismatch {
        /// Expected Wcash tip in conventional display order.
        expected: String,
        /// Redacted endpoint label.
        endpoint: String,
        /// Actual Wcash tip in conventional display order.
        actual: String,
    },

    /// An RPC endpoint is serving a chain with a different genesis block.
    #[error(
        "network identity mismatch: expected genesis {expected}, {endpoint} returned {actual}"
    )]
    NetworkIdentityMismatch {
        /// Operator-pinned genesis hash in conventional display order.
        expected: String,
        /// Redacted endpoint label.
        endpoint: String,
        /// Endpoint's height-zero block hash.
        actual: String,
    },

    /// A frozen native job exceeded its lifetime or was deactivated by a tip change.
    #[error("stale native merged-mining job: {0}")]
    StaleNativeJob(String),

    /// The live parent template failed a local invariant before reaching miners.
    #[error("invalid parent template: {0}")]
    InvalidParentTemplate(String),
}
