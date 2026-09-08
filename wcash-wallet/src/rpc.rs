//! Fail-closed access to Zebra's compact-block gRPC interface.

use std::{io::Cursor, net::IpAddr, time::Duration};

use serde::Serialize;
use thiserror::Error;
use tonic::{
    transport::{Channel, Endpoint},
    Code,
};
use zcash_client_backend::proto::service::{
    compact_tx_streamer_client::CompactTxStreamerClient, BlockId, ChainSpec, RawTransaction,
    TreeState, TxFilter,
};
use zcash_primitives::{
    block::BlockHash,
    transaction::{Transaction, TxVersion},
};
use zcash_protocol::consensus::BranchId;

use crate::{BlockRef, WalletNetwork};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const TCP_KEEPALIVE: Duration = Duration::from_secs(30);

type LightwalletdClient = CompactTxStreamerClient<Channel>;

/// A compact-block client that has proved it serves one exact Wcash genesis.
///
/// Network-sensitive methods are intentionally available only through this
/// wrapper, so callers cannot accidentally scan, sign, or broadcast through an
/// unattested Zcash or Wcash endpoint.
#[derive(Clone, Debug)]
pub struct AttestedWcashClient {
    inner: LightwalletdClient,
    network: WalletNetwork,
}

impl AttestedWcashClient {
    /// Connects to an endpoint and requires its height-zero block to match the
    /// frozen genesis for `network` before returning a usable client.
    pub async fn connect(endpoint: &str, network: WalletNetwork) -> Result<Self, WalletRpcError> {
        let mut inner = connect_unattested(endpoint).await?;
        attest_genesis(&mut inner, network).await?;
        Ok(Self { inner, network })
    }

    /// Returns the exact Wcash network attested by this connection.
    pub const fn network(&self) -> WalletNetwork {
        self.network
    }

    /// Returns the height and hash of this service's current best tip.
    pub async fn latest_block(&mut self) -> Result<BlockRef, WalletRpcError> {
        latest_block(&mut self.inner).await
    }

    /// Returns the canonical compact-block identifier at an exact height.
    pub async fn compact_block_ref(&mut self, height: u32) -> Result<BlockRef, WalletRpcError> {
        if height == 0 {
            return Ok(BlockRef {
                height,
                hash: self.network.genesis_hash(),
            });
        }
        compact_block_ref(&mut self.inner, height).await
    }

    /// Downloads and validates the commitment-tree state for an exact height.
    pub async fn tree_state(&mut self, height: u32) -> Result<TreeState, WalletRpcError> {
        let block = self.compact_block_ref(height).await?;
        let state = self
            .inner
            .get_tree_state(BlockId {
                height: 0,
                hash: block.hash.to_vec(),
            })
            .await?
            .into_inner();

        validate_tree_state(self.network, block, &state)?;
        Ok(state)
    }

    /// Queries this Wcash node for an exact transaction identifier.
    pub async fn transaction_status(
        &mut self,
        txid: zcash_protocol::TxId,
    ) -> Result<TransactionStatus, WalletRpcError> {
        transaction_status(&mut self.inner, txid).await
    }

    /// Submits exact signed transaction bytes idempotently.
    ///
    /// A transport failure is reported as ambiguous unless the node can prove
    /// that it knows the exact transaction identifier. The caller should retry
    /// the exact stored bytes instead of creating a replacement transfer.
    pub async fn broadcast_raw_transaction(
        &mut self,
        raw: Vec<u8>,
    ) -> Result<BroadcastResult, WalletRpcError> {
        broadcast_raw_transaction(&mut self.inner, raw).await
    }

    /// Gives the reviewed synchronizer access to the already-attested channel.
    pub(crate) fn inner_mut(&mut self) -> &mut LightwalletdClient {
        &mut self.inner
    }
}

/// Errors returned by the network boundary.
#[derive(Debug, Error)]
pub enum WalletRpcError {
    /// The endpoint is not a valid absolute HTTP(S) URI.
    #[error("invalid lightwalletd endpoint: {0}")]
    InvalidEndpoint(String),
    /// This experimental wallet only trusts a project-owned local Zebra.
    #[error("the experimental wallet accepts only loopback Zebra endpoints")]
    NonLoopbackEndpoint,
    /// Establishing the gRPC channel failed.
    #[error("could not connect to lightwalletd: {0}")]
    Connect(#[from] tonic::transport::Error),
    /// A gRPC request failed.
    #[error("lightwalletd request failed: {0}")]
    Status(#[from] tonic::Status),
    /// The server returned a malformed block identifier.
    #[error("lightwalletd returned a malformed {field} at height {height}")]
    MalformedBlockId {
        /// Name of the malformed field.
        field: &'static str,
        /// Block height associated with the malformed field.
        height: u64,
    },
    /// The server returned a block or tree state for the wrong height.
    #[error("lightwalletd returned the wrong {field} height: expected {expected}, got {actual}")]
    UnexpectedHeight {
        /// Name of the response field.
        field: &'static str,
        /// Height that was requested.
        expected: u64,
        /// Height returned by the service.
        actual: u64,
    },
    /// The endpoint serves a different chain.
    #[error("lightwalletd genesis mismatch: expected {expected}, got {actual}")]
    WrongGenesis {
        /// Expected internal-byte-order genesis identifier.
        expected: String,
        /// Actual internal-byte-order genesis identifier.
        actual: String,
    },
    /// A tree-state response used the wrong BIP70 network identifier.
    #[error("lightwalletd tree-state network mismatch: expected {expected}, got {actual}")]
    WrongNetworkName {
        /// Expected BIP70 network name.
        expected: String,
        /// Actual BIP70 network name.
        actual: String,
    },
    /// A tree-state response did not match the canonical block at its height.
    #[error("lightwalletd tree-state hash mismatch at height {height}: expected {expected}, got {actual}")]
    TreeStateHashMismatch {
        /// Requested block height.
        height: u32,
        /// Canonical display-order block hash.
        expected: String,
        /// Display-order block hash returned with the tree state.
        actual: String,
    },
    /// A tree-state response contained malformed hashes or commitment trees.
    #[error("malformed lightwalletd tree state at height {height}: {reason}")]
    MalformedTreeState {
        /// Requested block height.
        height: u32,
        /// Validation failure.
        reason: String,
    },
    /// A raw transaction is malformed or has trailing bytes.
    #[error("invalid signed Wcash transaction: {0}")]
    InvalidTransaction(String),
    /// A transaction was validly encoded but violates wallet privacy/domain invariants.
    #[error("signed transaction violates Wcash wallet policy: {0}")]
    TransactionPolicy(&'static str),
    /// The node rejected transaction submission.
    #[error("Wcash node rejected transaction ({code}): {message}")]
    Rejected {
        /// Node-defined error code.
        code: i32,
        /// Node-defined error message.
        message: String,
    },
    /// Submission may have reached the node, but acknowledgement was lost.
    #[error("broadcast outcome for {txid} is ambiguous: {reason}; retry these exact bytes")]
    AmbiguousBroadcast {
        /// Canonical display-order transaction identifier.
        txid: String,
        /// Transport and follow-up status details.
        reason: String,
    },
}

/// Status reported by Zebra for a transaction identifier.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum TransactionStatus {
    /// The transaction is not known by the selected Wcash node.
    Unknown,
    /// The transaction is currently in the Wcash mempool.
    Mempool,
    /// The transaction is mined in the Wcash best chain.
    Mined {
        /// Wcash block height containing the transaction.
        height: u64,
    },
    /// The transaction was mined, but only on a non-canonical fork.
    Orphaned,
}

/// Idempotent result of submitting signed transaction bytes.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct BroadcastResult {
    /// Canonical display-order transaction identifier.
    pub txid: String,
    /// Whether this invocation submitted the transaction or found it already known.
    pub disposition: BroadcastDisposition,
    /// Node status observed after submission.
    pub status: TransactionStatus,
}

/// Submission disposition for an idempotent broadcast.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BroadcastDisposition {
    /// This call submitted the transaction successfully.
    Submitted,
    /// The exact transaction identifier was already known to the node.
    AlreadyKnown,
}

async fn connect_unattested(endpoint: &str) -> Result<LightwalletdClient, WalletRpcError> {
    let endpoint = Endpoint::from_shared(endpoint.to_owned())
        .map_err(|error| WalletRpcError::InvalidEndpoint(error.to_string()))?;
    validate_endpoint(endpoint.uri().scheme_str(), endpoint.uri().host())?;
    let endpoint = endpoint
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(REQUEST_TIMEOUT)
        .tcp_keepalive(Some(TCP_KEEPALIVE));
    Ok(CompactTxStreamerClient::new(endpoint.connect().await?))
}

fn validate_endpoint(scheme: Option<&str>, host: Option<&str>) -> Result<(), WalletRpcError> {
    let scheme =
        scheme.ok_or_else(|| WalletRpcError::InvalidEndpoint("missing URI scheme".to_owned()))?;
    let host =
        host.ok_or_else(|| WalletRpcError::InvalidEndpoint("missing endpoint host".to_owned()))?;
    if !matches!(scheme, "http" | "https") {
        return Err(WalletRpcError::InvalidEndpoint(
            "endpoint scheme must be http or https".to_owned(),
        ));
    }
    if !is_loopback_host(host) {
        return Err(WalletRpcError::NonLoopbackEndpoint);
    }
    Ok(())
}

fn is_loopback_host(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<IpAddr>()
            .is_ok_and(|address| address.is_loopback())
}

fn validate_tree_state(
    network: WalletNetwork,
    block: BlockRef,
    state: &TreeState,
) -> Result<(), WalletRpcError> {
    if state.height != u64::from(block.height) {
        return Err(WalletRpcError::UnexpectedHeight {
            field: "tree state",
            expected: u64::from(block.height),
            actual: state.height,
        });
    }
    let expected_network = network.parameters().bip70_network_name();
    if state.network != expected_network {
        return Err(WalletRpcError::WrongNetworkName {
            expected: expected_network,
            actual: state.network.clone(),
        });
    }
    let expected_hash = BlockHash(block.hash).to_string();
    if state.hash != expected_hash {
        return Err(WalletRpcError::TreeStateHashMismatch {
            height: block.height,
            expected: expected_hash,
            actual: state.hash.clone(),
        });
    }
    state
        .to_chain_state()
        .map(|_| ())
        .map_err(|error| WalletRpcError::MalformedTreeState {
            height: block.height,
            reason: error.to_string(),
        })
}

async fn attest_genesis(
    client: &mut LightwalletdClient,
    network: WalletNetwork,
) -> Result<BlockRef, WalletRpcError> {
    let expected = network.genesis_hash();
    let block = client
        .get_block(BlockId {
            height: 0,
            hash: expected.to_vec(),
        })
        .await?
        .into_inner();
    if block.height != 0 {
        return Err(WalletRpcError::UnexpectedHeight {
            field: "genesis block",
            expected: 0,
            actual: block.height,
        });
    }
    let hash: [u8; 32] =
        block
            .hash
            .as_slice()
            .try_into()
            .map_err(|_| WalletRpcError::MalformedBlockId {
                field: "genesis hash",
                height: block.height,
            })?;
    if hash != expected {
        return Err(WalletRpcError::WrongGenesis {
            expected: hex::encode(expected),
            actual: hex::encode(hash),
        });
    }
    Ok(BlockRef { height: 0, hash })
}

async fn latest_block(client: &mut LightwalletdClient) -> Result<BlockRef, WalletRpcError> {
    let tip = client.get_latest_block(ChainSpec {}).await?.into_inner();
    let height = u32::try_from(tip.height).map_err(|_| WalletRpcError::MalformedBlockId {
        field: "tip height",
        height: tip.height,
    })?;
    let hash: [u8; 32] =
        tip.hash
            .as_slice()
            .try_into()
            .map_err(|_| WalletRpcError::MalformedBlockId {
                field: "tip hash",
                height: tip.height,
            })?;
    Ok(BlockRef { height, hash })
}

async fn compact_block_ref(
    client: &mut LightwalletdClient,
    height: u32,
) -> Result<BlockRef, WalletRpcError> {
    let block = client
        .get_block(BlockId {
            height: u64::from(height),
            hash: vec![],
        })
        .await?
        .into_inner();
    if block.height != u64::from(height) {
        return Err(WalletRpcError::UnexpectedHeight {
            field: "compact block",
            expected: u64::from(height),
            actual: block.height,
        });
    }
    let hash: [u8; 32] =
        block
            .hash
            .as_slice()
            .try_into()
            .map_err(|_| WalletRpcError::MalformedBlockId {
                field: "block hash",
                height: block.height,
            })?;
    Ok(BlockRef { height, hash })
}

/// Parses a raw transaction and enforces Wcash wallet output-pool policy.
pub fn inspect_signed_transaction(raw: &[u8]) -> Result<Transaction, WalletRpcError> {
    let mut reader = Cursor::new(raw);
    let transaction = Transaction::read(&mut reader, BranchId::WcashTestnetV1)
        .map_err(|error| WalletRpcError::InvalidTransaction(error.to_string()))?;
    if usize::try_from(reader.position()).ok() != Some(raw.len()) {
        return Err(WalletRpcError::InvalidTransaction(
            "trailing bytes after transaction".to_owned(),
        ));
    }
    if transaction.version() != TxVersion::V6 {
        return Err(WalletRpcError::TransactionPolicy(
            "only transaction version 6 is permitted",
        ));
    }
    if transaction.consensus_branch_id() != BranchId::WcashTestnetV1 {
        return Err(WalletRpcError::TransactionPolicy(
            "transaction does not use the Wcash Testnet V1 branch",
        ));
    }
    if transaction.transparent_bundle().is_some()
        || transaction.sapling_bundle().is_some()
        || transaction.orchard_bundle().is_some()
    {
        return Err(WalletRpcError::TransactionPolicy(
            "wallet transfers must contain only Ironwood components",
        ));
    }
    if transaction.ironwood_bundle().is_none() {
        return Err(WalletRpcError::TransactionPolicy(
            "wallet transfer is missing its Ironwood bundle",
        ));
    }
    Ok(transaction)
}

async fn transaction_status(
    client: &mut LightwalletdClient,
    txid: zcash_protocol::TxId,
) -> Result<TransactionStatus, WalletRpcError> {
    match client
        .get_transaction(TxFilter {
            block: None,
            index: 0,
            hash: txid.as_ref().to_vec(),
        })
        .await
    {
        Ok(response) => verified_status_response(response.into_inner(), txid),
        Err(status) if status.code() == Code::NotFound => Ok(TransactionStatus::Unknown),
        Err(status) => Err(status.into()),
    }
}

fn verified_status_response(
    response: RawTransaction,
    requested_txid: zcash_protocol::TxId,
) -> Result<TransactionStatus, WalletRpcError> {
    let returned = inspect_signed_transaction(&response.data)?;
    ensure_expected_txid(returned.txid(), requested_txid)?;
    match response.height {
        0 => Ok(TransactionStatus::Mempool),
        u64::MAX => Ok(TransactionStatus::Orphaned),
        height => Ok(TransactionStatus::Mined { height }),
    }
}

fn ensure_expected_txid(
    returned: zcash_protocol::TxId,
    requested: zcash_protocol::TxId,
) -> Result<(), WalletRpcError> {
    if returned == requested {
        Ok(())
    } else {
        Err(WalletRpcError::InvalidTransaction(
            "node returned transaction bytes for a different txid".to_owned(),
        ))
    }
}

async fn broadcast_raw_transaction(
    client: &mut LightwalletdClient,
    raw: Vec<u8>,
) -> Result<BroadcastResult, WalletRpcError> {
    let transaction = inspect_signed_transaction(&raw)?;
    let txid = transaction.txid();
    let before = transaction_status(client, txid).await?;
    if is_live_status(&before) {
        return Ok(BroadcastResult {
            txid: txid.to_string(),
            disposition: BroadcastDisposition::AlreadyKnown,
            status: before,
        });
    }

    let response = match client
        .send_transaction(RawTransaction {
            data: raw,
            height: 0,
        })
        .await
    {
        Ok(response) => response.into_inner(),
        Err(send_error) => {
            return match transaction_status(client, txid).await {
                Ok(status) if is_live_status(&status) => Ok(BroadcastResult {
                    txid: txid.to_string(),
                    disposition: BroadcastDisposition::AlreadyKnown,
                    status,
                }),
                Ok(TransactionStatus::Unknown | TransactionStatus::Orphaned) => {
                    Err(WalletRpcError::AmbiguousBroadcast {
                    txid: txid.to_string(),
                    reason: send_error.to_string(),
                    })
                }
                Ok(_) => unreachable!("all known transaction states were handled"),
                Err(status_error) => Err(WalletRpcError::AmbiguousBroadcast {
                    txid: txid.to_string(),
                    reason: format!(
                        "send failed ({send_error}); exact-txid status check failed ({status_error})"
                    ),
                }),
            };
        }
    };
    if response.error_code != 0 {
        let after = transaction_status(client, txid).await?;
        if is_live_status(&after) {
            return Ok(BroadcastResult {
                txid: txid.to_string(),
                disposition: BroadcastDisposition::AlreadyKnown,
                status: after,
            });
        }
        return Err(WalletRpcError::Rejected {
            code: response.error_code,
            message: response.error_message,
        });
    }

    let status = transaction_status(client, txid).await?;
    Ok(BroadcastResult {
        txid: txid.to_string(),
        disposition: BroadcastDisposition::Submitted,
        status,
    })
}

fn is_live_status(status: &TransactionStatus) -> bool {
    matches!(
        status,
        TransactionStatus::Mempool | TransactionStatus::Mined { .. }
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_policy_accepts_only_loopback_zebra() {
        assert!(validate_endpoint(Some("http"), Some("localhost")).is_ok());
        assert!(validate_endpoint(Some("http"), Some("127.0.0.1")).is_ok());
        assert!(validate_endpoint(Some("http"), Some("::1")).is_ok());
        assert!(matches!(
            validate_endpoint(Some("http"), Some("example.com")),
            Err(WalletRpcError::NonLoopbackEndpoint)
        ));
        assert!(matches!(
            validate_endpoint(Some("https"), Some("example.com")),
            Err(WalletRpcError::NonLoopbackEndpoint)
        ));
        assert!(validate_endpoint(Some("https"), Some("::1")).is_ok());
    }

    fn valid_tree_state(block: BlockRef) -> TreeState {
        TreeState {
            network: "test".to_owned(),
            height: u64::from(block.height),
            hash: BlockHash(block.hash).to_string(),
            time: 1,
            sapling_tree: String::new(),
            orchard_tree: String::new(),
            ironwood_tree: String::new(),
        }
    }

    #[test]
    fn tree_state_is_bound_to_height_network_hash_and_valid_roots() {
        let block = BlockRef {
            height: 7,
            hash: [42; 32],
        };
        let valid = valid_tree_state(block);
        assert!(validate_tree_state(WalletNetwork::Regtest, block, &valid).is_ok());

        let mut wrong_height = valid.clone();
        wrong_height.height += 1;
        assert!(matches!(
            validate_tree_state(WalletNetwork::Regtest, block, &wrong_height),
            Err(WalletRpcError::UnexpectedHeight { .. })
        ));

        let mut wrong_network = valid.clone();
        wrong_network.network = "main".to_owned();
        assert!(matches!(
            validate_tree_state(WalletNetwork::Regtest, block, &wrong_network),
            Err(WalletRpcError::WrongNetworkName { .. })
        ));

        let mut wrong_hash = valid.clone();
        wrong_hash.hash = "00".repeat(32);
        assert!(matches!(
            validate_tree_state(WalletNetwork::Regtest, block, &wrong_hash),
            Err(WalletRpcError::TreeStateHashMismatch { .. })
        ));

        let mut malformed_tree = valid;
        malformed_tree.ironwood_tree = "not-hex".to_owned();
        assert!(matches!(
            validate_tree_state(WalletNetwork::Regtest, block, &malformed_tree),
            Err(WalletRpcError::MalformedTreeState { .. })
        ));
    }

    #[test]
    fn malformed_or_foreign_raw_transaction_is_rejected() {
        assert!(matches!(
            inspect_signed_transaction(&[1, 2, 3]),
            Err(WalletRpcError::InvalidTransaction(_))
        ));
    }

    #[test]
    fn status_response_must_match_the_exact_requested_txid() {
        let first = zcash_protocol::TxId::from_bytes([1; 32]);
        let second = zcash_protocol::TxId::from_bytes([2; 32]);
        assert!(ensure_expected_txid(first, first).is_ok());
        assert!(matches!(
            ensure_expected_txid(first, second),
            Err(WalletRpcError::InvalidTransaction(_))
        ));
        assert!(matches!(
            verified_status_response(
                RawTransaction {
                    data: vec![1, 2, 3],
                    height: 0,
                },
                first,
            ),
            Err(WalletRpcError::InvalidTransaction(_))
        ));
    }

    #[test]
    fn orphaned_transactions_do_not_short_circuit_rebroadcast() {
        assert!(!is_live_status(&TransactionStatus::Unknown));
        assert!(!is_live_status(&TransactionStatus::Orphaned));
        assert!(is_live_status(&TransactionStatus::Mempool));
        assert!(is_live_status(&TransactionStatus::Mined { height: 7 }));
    }
}
