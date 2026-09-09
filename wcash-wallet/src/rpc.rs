//! Fail-closed access to Zebra's compact-block gRPC interface.

use std::{
    collections::{BTreeMap, HashSet},
    error::Error as StdError,
    future::Future,
    io::Cursor,
    mem,
    net::IpAddr,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use bytes::Bytes;
use futures_util::TryStreamExt;
use http_body_util::{BodyExt, Full};
use prost::Message;
use serde::Serialize;
use thiserror::Error;
use tonic::{
    body::Body,
    codegen::{http::Request, http::Response, Service},
    transport::{Channel, Endpoint},
    Code,
};
use zcash_client_backend::proto::service::{
    compact_tx_streamer_client::CompactTxStreamerClient, BlockId, ChainSpec, GetAddressUtxosArg,
    RawTransaction, TreeState, TxFilter,
};
use zcash_keys::encoding::AddressCodec;
use zcash_primitives::{
    block::BlockHash,
    transaction::{Transaction, TxVersion},
};
use zcash_protocol::{constants::MAX_BLOCK_BYTES, TxId};
use zcash_transparent::address::{Script, TransparentAddress};
use zebra_chain::primitives::{WcashAddress, WcashAddressKind};

use crate::{address::encode_wcash_transparent_receiver, BlockRef, WalletNetwork};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const TCP_KEEPALIVE: Duration = Duration::from_secs(30);

type LightwalletdClient = CompactTxStreamerClient<Channel>;
pub(crate) type WcashSyncClient = CompactTxStreamerClient<WcashNamespaceService>;

const GET_ADDRESS_UTXOS_PATH: &str = "/cash.z.wallet.sdk.rpc.CompactTxStreamer/GetAddressUtxos";
const GET_ADDRESS_UTXOS_STREAM_PATH: &str =
    "/cash.z.wallet.sdk.rpc.CompactTxStreamer/GetAddressUtxosStream";

type NamespaceServiceError = Box<dyn StdError + Send + Sync>;

/// A narrow transport adapter that changes only synchronizer-generated
/// transparent address strings from the Zcash namespace to the Wcash
/// namespace. Receiver payloads are unchanged.
#[derive(Clone, Debug)]
pub(crate) struct WcashNamespaceService {
    inner: Channel,
    network: WalletNetwork,
}

impl Service<Request<Body>> for WcashNamespaceService {
    type Response = Response<Body>;
    type Error = NamespaceServiceError;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Service::poll_ready(&mut self.inner, cx)
            .map_err(|error| Box::new(error) as NamespaceServiceError)
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        let replacement = self.inner.clone();
        let mut inner = mem::replace(&mut self.inner, replacement);
        let network = self.network;
        Box::pin(async move {
            let request = rewrite_sync_namespace_request(request, network).await?;
            Service::call(&mut inner, request)
                .await
                .map_err(|error| Box::new(error) as NamespaceServiceError)
        })
    }
}

/// Each matching P2PKH output occupies at least 34 bytes in its creator
/// transaction (eight value bytes, one CompactSize byte, and a 25-byte script).
/// This page size is therefore strictly larger than the number of matching
/// outputs that can fit in one consensus-valid block. An inclusive height
/// cursor can consequently never truncate a valid block when a full page is
/// resumed from its final height.
pub(crate) const TRANSPARENT_UTXO_PAGE_OUTPUTS: usize = 65_536;

const MIN_P2PKH_OUTPUT_ENCODED_BYTES: usize = 34;
const MAX_TRANSPARENT_UTXO_PAGE_BYTES: usize = 64 * 1024 * 1024;

fn transparent_utxo_page_is_block_safe(maximum_outputs: usize) -> bool {
    maximum_outputs > MAX_BLOCK_BYTES / MIN_P2PKH_OUTPUT_ENCODED_BYTES
}

/// One mined full transaction returned by the current-UTXO query.
pub(crate) struct MinedTransparentTransaction {
    pub(crate) transaction: Transaction,
    pub(crate) height: u32,
}

#[derive(Clone, Debug)]
struct TransparentUnspentOutput {
    output_index: usize,
    script: Vec<u8>,
    value_zat: u64,
    height: u32,
}

#[derive(Debug)]
pub(crate) struct TransparentUnspentCreator {
    txid: TxId,
    outputs: Vec<TransparentUnspentOutput>,
}

#[derive(Debug)]
pub(crate) struct TransparentUnspentPage {
    creators: Vec<TransparentUnspentCreator>,
    output_count: usize,
    first_height: Option<u32>,
    last_height: Option<u32>,
}

impl TransparentUnspentPage {
    pub(crate) fn into_complete_creators(
        self,
        repeated_boundary_height: Option<u32>,
    ) -> Vec<TransparentUnspentCreator> {
        self.creators
            .into_iter()
            .filter(|creator| {
                repeated_boundary_height.is_none_or(|height| {
                    creator
                        .outputs
                        .first()
                        .is_some_and(|output| output.height != height)
                })
            })
            .collect()
    }

    pub(crate) fn next_start_height(
        &self,
        current_start: u32,
        maximum_outputs: usize,
    ) -> Result<Option<u32>, WalletRpcError> {
        next_transparent_page_start(
            current_start,
            self.output_count,
            self.first_height,
            self.last_height,
            maximum_outputs,
        )
    }
}

/// A compact-block client that has proved it serves one exact Wcash genesis.
///
/// Network-sensitive methods are intentionally available only through this
/// wrapper, so callers cannot accidentally scan, sign, or broadcast through an
/// unattested Zcash or Wcash endpoint.
#[derive(Clone, Debug)]
pub struct AttestedWcashClient {
    inner: LightwalletdClient,
    channel: Channel,
    network: WalletNetwork,
}

impl AttestedWcashClient {
    /// Connects to an endpoint and requires its height-zero block to match the
    /// frozen genesis for `network` before returning a usable client.
    pub async fn connect(endpoint: &str, network: WalletNetwork) -> Result<Self, WalletRpcError> {
        let channel = connect_channel(endpoint).await?;
        let mut inner = CompactTxStreamerClient::new(channel.clone());
        attest_genesis(&mut inner, network).await?;
        Ok(Self {
            inner,
            channel,
            network,
        })
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
        transaction_status(&mut self.inner, txid, self.network).await
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
        broadcast_raw_transaction(&mut self.inner, raw, self.network).await
    }

    /// Creates a synchronizer client whose transparent UTXO requests use the
    /// Wcash textual namespace while retaining the attested channel.
    pub(crate) fn sync_client(&self) -> WcashSyncClient {
        CompactTxStreamerClient::new(WcashNamespaceService {
            inner: self.channel.clone(),
            network: self.network,
        })
    }

    /// Fetches one height-resumable page of current UTXOs for a Wcash receiver.
    ///
    /// Current UTXOs are used instead of a historical cursor so any wallet
    /// rewind is self-healing. The page bound is larger than the maximum number
    /// of P2PKH outputs in a valid block, so an inclusive final-height cursor
    /// cannot permanently omit outputs from a partially returned block.
    pub(crate) async fn transparent_unspent_page(
        &mut self,
        address: &str,
        start_height: u32,
        tip_height: u32,
        maximum_outputs: usize,
    ) -> Result<TransparentUnspentPage, WalletRpcError> {
        let receiver = decode_wcash_transparent_address(address, self.network)?;
        if start_height == 0
            || start_height > tip_height
            || maximum_outputs == 0
            || !transparent_utxo_page_is_block_safe(maximum_outputs)
        {
            return Err(WalletRpcError::InvalidTransparentHistoryRequest);
        }
        let request_limit = u32::try_from(maximum_outputs)
            .map_err(|_| WalletRpcError::InvalidTransparentHistoryRequest)?;

        let mut stream = self
            .inner
            .get_address_utxos_stream(GetAddressUtxosArg {
                addresses: vec![address.to_owned()],
                start_height: u64::from(start_height),
                max_entries: request_limit,
            })
            .await?
            .into_inner();

        let expected_script = Script::from(receiver.script()).0 .0;
        let mut creators = BTreeMap::<TxId, Vec<TransparentUnspentOutput>>::new();
        let mut outpoints = HashSet::new();
        let mut first_height = None;
        let mut previous_height = None;
        let mut encoded_bytes = 0usize;
        while let Some(utxo) = stream.try_next().await? {
            let (next_count, next_encoded_bytes) = checked_transparent_history_usage(
                outpoints.len(),
                encoded_bytes,
                utxo.encoded_len(),
                maximum_outputs,
                MAX_TRANSPARENT_UTXO_PAGE_BYTES,
            )?;
            if utxo.address != address || utxo.script != expected_script {
                return Err(WalletRpcError::TransparentUtxoMismatch(
                    "UTXO address or script does not match the requested receiver".to_owned(),
                ));
            }
            let txid = TxId::from_bytes(utxo.txid.as_slice().try_into().map_err(|_| {
                WalletRpcError::MalformedTransparentUtxo("transaction id must be 32 bytes")
            })?);
            let output_index = usize::try_from(utxo.index).map_err(|_| {
                WalletRpcError::MalformedTransparentUtxo("output index must be nonnegative")
            })?;
            let value_zat = u64::try_from(utxo.value_zat).map_err(|_| {
                WalletRpcError::MalformedTransparentUtxo("output value must be nonnegative")
            })?;
            let height = u32::try_from(utxo.height).map_err(|_| {
                WalletRpcError::UnexpectedTransparentTransactionHeight {
                    start: start_height,
                    end: tip_height,
                    actual: utxo.height,
                }
            })?;
            if !(start_height..=tip_height).contains(&height)
                || previous_height.is_some_and(|previous| height < previous)
            {
                return Err(WalletRpcError::UnexpectedTransparentTransactionHeight {
                    start: start_height,
                    end: tip_height,
                    actual: utxo.height,
                });
            }
            if !outpoints.insert((txid, output_index)) {
                return Err(WalletRpcError::DuplicateTransparentOutpoint {
                    txid: txid.to_string(),
                    output_index,
                });
            }
            first_height.get_or_insert(height);
            previous_height = Some(height);
            encoded_bytes = next_encoded_bytes;
            let creator_outputs = creators.entry(txid).or_default();
            if creator_outputs
                .first()
                .is_some_and(|output| output.height != height)
            {
                return Err(WalletRpcError::TransparentUtxoMismatch(format!(
                    "creator transaction {txid} was reported at multiple heights"
                )));
            }
            creator_outputs.push(TransparentUnspentOutput {
                output_index,
                script: utxo.script,
                value_zat,
                height,
            });
            debug_assert_eq!(outpoints.len(), next_count);
        }

        Ok(TransparentUnspentPage {
            creators: creators
                .into_iter()
                .map(|(txid, outputs)| TransparentUnspentCreator { txid, outputs })
                .collect(),
            output_count: outpoints.len(),
            first_height,
            last_height: previous_height,
        })
    }

    /// Fetches and verifies one full creator transaction from a previously
    /// validated current-UTXO page.
    pub(crate) async fn transparent_creator_transaction(
        &mut self,
        creator: TransparentUnspentCreator,
    ) -> Result<MinedTransparentTransaction, WalletRpcError> {
        let TransparentUnspentCreator { txid, outputs } = creator;
        let expected_height = outputs
            .first()
            .expect("a page creator always contains at least one output")
            .height;
        if outputs
            .iter()
            .any(|output| output.height != expected_height)
        {
            return Err(WalletRpcError::TransparentUtxoMismatch(format!(
                "creator transaction {txid} has inconsistent mined heights"
            )));
        }
        let raw = self
            .inner
            .get_transaction(TxFilter {
                block: None,
                index: 0,
                hash: txid.as_ref().to_vec(),
            })
            .await?
            .into_inner();
        if raw.height != u64::from(expected_height) {
            return Err(WalletRpcError::UnexpectedTransparentTransactionHeight {
                start: expected_height,
                end: expected_height,
                actual: raw.height,
            });
        }
        if raw.data.len() > MAX_BLOCK_BYTES {
            return Err(WalletRpcError::OversizedTransparentTransaction {
                txid: txid.to_string(),
                encoded_bytes: raw.data.len(),
            });
        }
        let transaction = parse_wcash_v6_transaction(&raw.data, self.network)?;
        ensure_expected_txid(transaction.txid(), txid)?;
        let receiver = outputs
            .first()
            .and_then(|output| transparent_receiver_from_script(&output.script))
            .ok_or_else(|| {
                WalletRpcError::TransparentUtxoMismatch(format!(
                    "creator transaction {txid} has a non-P2PKH output script"
                ))
            })?;
        validate_transparent_unspent_outputs(&transaction, &outputs, &receiver, txid)?;
        Ok(MinedTransparentTransaction {
            transaction,
            height: expected_height,
        })
    }
}

fn next_transparent_page_start(
    current_start: u32,
    output_count: usize,
    first_height: Option<u32>,
    last_height: Option<u32>,
    maximum_outputs: usize,
) -> Result<Option<u32>, WalletRpcError> {
    if output_count < maximum_outputs {
        return Ok(None);
    }
    let (first_height, last_height) = first_height
        .zip(last_height)
        .ok_or(WalletRpcError::InvalidTransparentHistoryRequest)?;
    if output_count != maximum_outputs
        || last_height <= current_start
        || first_height == last_height
    {
        return Err(WalletRpcError::TransparentUtxoPageCannotAdvance {
            height: last_height,
            maximum: maximum_outputs,
        });
    }
    Ok(Some(last_height))
}

fn checked_transparent_history_usage(
    current_count: usize,
    current_encoded_bytes: usize,
    next_transaction_bytes: usize,
    maximum_transactions: usize,
    maximum_encoded_bytes: usize,
) -> Result<(usize, usize), WalletRpcError> {
    let next_count = current_count
        .checked_add(1)
        .ok_or(WalletRpcError::TransparentUtxoLimit {
            maximum: maximum_transactions,
        })?;
    if next_count > maximum_transactions {
        return Err(WalletRpcError::TransparentUtxoLimit {
            maximum: maximum_transactions,
        });
    }
    let next_encoded_bytes = current_encoded_bytes
        .checked_add(next_transaction_bytes)
        .ok_or(WalletRpcError::TransparentUtxoByteLimit {
            maximum: maximum_encoded_bytes,
        })?;
    if next_encoded_bytes > maximum_encoded_bytes {
        return Err(WalletRpcError::TransparentUtxoByteLimit {
            maximum: maximum_encoded_bytes,
        });
    }
    Ok((next_count, next_encoded_bytes))
}

fn decode_wcash_transparent_address(
    encoded: &str,
    network: WalletNetwork,
) -> Result<TransparentAddress, WalletRpcError> {
    let address = WcashAddress::try_from_encoded(encoded)
        .map_err(|_| WalletRpcError::InvalidTransparentAddress)?;
    if address.network() != network.address_network() {
        return Err(WalletRpcError::InvalidTransparentAddress);
    }
    match address.kind() {
        WcashAddressKind::P2pkh(payload) => Ok(TransparentAddress::PublicKeyHash(*payload)),
        _ => Err(WalletRpcError::InvalidTransparentAddress),
    }
}

fn transparent_receiver_from_script(script: &[u8]) -> Option<TransparentAddress> {
    if script.len() == 25
        && script[0] == 0x76
        && script[1] == 0xa9
        && script[2] == 0x14
        && script[23] == 0x88
        && script[24] == 0xac
    {
        let payload: [u8; 20] = script[3..23].try_into().ok()?;
        Some(TransparentAddress::PublicKeyHash(payload))
    } else {
        None
    }
}

fn validate_transparent_unspent_outputs(
    transaction: &Transaction,
    outputs: &[TransparentUnspentOutput],
    receiver: &TransparentAddress,
    txid: TxId,
) -> Result<(), WalletRpcError> {
    let bundle = transaction.transparent_bundle().ok_or_else(|| {
        WalletRpcError::TransparentUtxoMismatch(format!(
            "creator transaction {txid} has no transparent bundle"
        ))
    })?;
    let matching_output_indexes = bundle
        .vout
        .iter()
        .enumerate()
        .filter_map(|(index, candidate)| {
            (candidate.recipient_address().as_ref() == Some(receiver)).then_some(index)
        })
        .collect::<Vec<_>>();
    let reported_output_indexes = outputs
        .iter()
        .map(|output| output.output_index)
        .collect::<Vec<_>>();
    if !exact_unique_index_sets_match(&matching_output_indexes, &reported_output_indexes) {
        return Err(WalletRpcError::TransparentUtxoMismatch(format!(
            "creator transaction {txid} has a payout-address output absent from the current UTXO set"
        )));
    }
    for output in outputs {
        let candidate = bundle.vout.get(output.output_index).ok_or_else(|| {
            WalletRpcError::TransparentUtxoMismatch(format!(
                "creator transaction {txid} has no output {}",
                output.output_index
            ))
        })?;
        if candidate.value().into_u64() != output.value_zat
            || candidate.script_pubkey().0 .0 != output.script
            || candidate.recipient_address().as_ref() != Some(receiver)
        {
            return Err(WalletRpcError::TransparentUtxoMismatch(format!(
                "creator transaction {txid} output {} differs from the UTXO record",
                output.output_index
            )));
        }
    }
    Ok(())
}

fn exact_unique_index_sets_match(expected: &[usize], actual: &[usize]) -> bool {
    if expected.len() != actual.len() {
        return false;
    }
    let mut expected = expected.to_vec();
    let mut actual = actual.to_vec();
    expected.sort_unstable();
    actual.sort_unstable();
    let has_duplicate = |indexes: &[usize]| indexes.windows(2).any(|pair| pair[0] == pair[1]);
    !has_duplicate(&expected) && !has_duplicate(&actual) && expected == actual
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
    /// A generated synchronizer request could not be translated into the Wcash
    /// transparent-address namespace.
    #[error("invalid transparent-address synchronization request: {0}")]
    InvalidNamespaceRequest(String),
    /// The transparent-history address is malformed, for another network, or
    /// is not the P2PKH form used for default coinbase payouts.
    #[error("invalid Wcash transparent coinbase address")]
    InvalidTransparentAddress,
    /// The current-UTXO query bound is invalid.
    #[error("transparent UTXO classification requires a nonzero tip and response limits")]
    InvalidTransparentHistoryRequest,
    /// A full transparent transaction response was outside the requested
    /// ordered height range.
    #[error("transparent transaction height {actual} is outside ordered range {start}..={end}")]
    UnexpectedTransparentTransactionHeight {
        /// Requested first block.
        start: u32,
        /// Requested final block.
        end: u32,
        /// Height returned by the service.
        actual: u64,
    },
    /// The node exceeded the wallet's explicit current-UTXO count bound.
    #[error("transparent UTXO set exceeds the {maximum}-output safety limit")]
    TransparentUtxoLimit {
        /// Maximum accepted number of unspent outputs.
        maximum: usize,
    },
    /// A full page ended without advancing beyond one inclusive block height.
    /// This is impossible for a valid block when the required page bound is
    /// used, and therefore indicates an inconsistent endpoint response.
    #[error(
        "transparent UTXO page cannot advance past height {height} at the {maximum}-output block-safe limit"
    )]
    TransparentUtxoPageCannotAdvance {
        /// Height at which pagination became unsafe.
        height: u32,
        /// Required block-safe page limit.
        maximum: usize,
    },
    /// The node exceeded the wallet's aggregate UTXO and full-transaction byte bound.
    #[error("transparent UTXO classification exceeds the {maximum}-byte safety limit")]
    TransparentUtxoByteLimit {
        /// Maximum accepted aggregate encoded bytes.
        maximum: usize,
    },
    /// A current-UTXO record contains an invalid primitive field.
    #[error("malformed transparent UTXO: {0}")]
    MalformedTransparentUtxo(&'static str),
    /// The node returned the same unspent outpoint more than once.
    #[error("transparent UTXO response duplicated {txid}:{output_index}")]
    DuplicateTransparentOutpoint {
        /// Creator transaction identifier.
        txid: String,
        /// Transparent output index.
        output_index: usize,
    },
    /// A creator transaction is larger than a consensus-valid block.
    #[error(
        "transparent creator transaction {txid} is {encoded_bytes} bytes, larger than a valid block"
    )]
    OversizedTransparentTransaction {
        /// Creator transaction identifier.
        txid: String,
        /// Returned transaction byte length.
        encoded_bytes: usize,
    },
    /// A full creator transaction did not match its current-UTXO records.
    #[error("transparent UTXO verification failed: {0}")]
    TransparentUtxoMismatch(String),
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

async fn connect_channel(endpoint: &str) -> Result<Channel, WalletRpcError> {
    let endpoint = Endpoint::from_shared(endpoint.to_owned())
        .map_err(|error| WalletRpcError::InvalidEndpoint(error.to_string()))?;
    validate_endpoint(endpoint.uri().scheme_str(), endpoint.uri().host())?;
    let endpoint = endpoint
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(REQUEST_TIMEOUT)
        .tcp_keepalive(Some(TCP_KEEPALIVE));
    Ok(endpoint.connect().await?)
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

async fn rewrite_sync_namespace_request(
    request: Request<Body>,
    network: WalletNetwork,
) -> Result<Request<Body>, NamespaceServiceError> {
    if !matches!(
        request.uri().path(),
        GET_ADDRESS_UTXOS_PATH | GET_ADDRESS_UTXOS_STREAM_PATH
    ) {
        return Ok(request);
    }

    let (mut parts, body) = request.into_parts();
    let frame = body
        .collect()
        .await
        .map_err(|error| Box::new(error) as NamespaceServiceError)?
        .to_bytes();
    let rewritten = rewrite_utxo_request_frame(&frame, network)
        .map_err(|error| Box::new(error) as NamespaceServiceError)?;
    parts.headers.remove("content-length");
    Ok(Request::from_parts(
        parts,
        Body::new(Full::new(Bytes::from(rewritten))),
    ))
}

fn rewrite_utxo_request_frame(
    frame: &[u8],
    network: WalletNetwork,
) -> Result<Vec<u8>, WalletRpcError> {
    if frame.len() < 5 || frame[0] != 0 {
        return Err(WalletRpcError::InvalidNamespaceRequest(
            "expected one uncompressed gRPC request frame".to_owned(),
        ));
    }
    let encoded_len = u32::from_be_bytes(
        frame[1..5]
            .try_into()
            .expect("a five-byte gRPC frame has a four-byte length"),
    );
    let encoded_len = usize::try_from(encoded_len).map_err(|_| {
        WalletRpcError::InvalidNamespaceRequest("request length is not representable".to_owned())
    })?;
    if frame.len() != encoded_len.saturating_add(5) {
        return Err(WalletRpcError::InvalidNamespaceRequest(
            "gRPC frame length mismatch or multiple request messages".to_owned(),
        ));
    }

    let mut request = GetAddressUtxosArg::decode(&frame[5..]).map_err(|error| {
        WalletRpcError::InvalidNamespaceRequest(format!("invalid UTXO request protobuf: {error}"))
    })?;
    let page_limit = u32::try_from(TRANSPARENT_UTXO_PAGE_OUTPUTS)
        .expect("the transparent UTXO page limit fits in u32");
    if request.max_entries == 0 || request.max_entries > page_limit {
        request.max_entries = page_limit;
    }
    let params = network.parameters();
    for encoded in &mut request.addresses {
        let receiver = TransparentAddress::decode(&params, encoded).map_err(|error| {
            WalletRpcError::InvalidNamespaceRequest(format!(
                "synchronizer emitted a non-canonical transparent address: {error}"
            ))
        })?;
        *encoded = encode_wcash_transparent_receiver(receiver, network);
    }

    let payload_len = u32::try_from(request.encoded_len()).map_err(|_| {
        WalletRpcError::InvalidNamespaceRequest("rewritten request is too large".to_owned())
    })?;
    let mut rewritten = Vec::with_capacity(request.encoded_len().saturating_add(5));
    rewritten.push(0);
    rewritten.extend_from_slice(&payload_len.to_be_bytes());
    request.encode(&mut rewritten).map_err(|error| {
        WalletRpcError::InvalidNamespaceRequest(format!("could not encode request: {error}"))
    })?;
    Ok(rewritten)
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

fn parse_wcash_v6_transaction(
    raw: &[u8],
    network: WalletNetwork,
) -> Result<Transaction, WalletRpcError> {
    let expected_branch_id = network.branch_id();
    let mut reader = Cursor::new(raw);
    let transaction = Transaction::read(&mut reader, expected_branch_id)
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
    if transaction.consensus_branch_id() != expected_branch_id {
        return Err(WalletRpcError::TransactionPolicy(
            "transaction does not use the selected Wcash network branch",
        ));
    }
    Ok(transaction)
}

/// Parses a signed V6 wallet transaction and enforces its Wcash pool policy.
///
/// Private transfers are Ironwood-only. Coinbase shielding additionally has
/// one or more transparent inputs and no transparent outputs. Sapling, legacy
/// Orchard, transparent change, and coinbase transactions are rejected.
pub fn inspect_signed_transaction(
    raw: &[u8],
    network: WalletNetwork,
) -> Result<Transaction, WalletRpcError> {
    let transaction = parse_wcash_v6_transaction(raw, network)?;
    if transaction.sapling_bundle().is_some() || transaction.orchard_bundle().is_some() {
        return Err(WalletRpcError::TransactionPolicy(
            "wallet transactions must not contain Sapling or legacy Orchard components",
        ));
    }
    if transaction.ironwood_bundle().is_none() {
        return Err(WalletRpcError::TransactionPolicy(
            "wallet transfer is missing its Ironwood bundle",
        ));
    }
    if let Some(transparent) = transaction.transparent_bundle() {
        if transparent.is_coinbase() || transparent.vin.is_empty() || !transparent.vout.is_empty() {
            return Err(WalletRpcError::TransactionPolicy(
                "coinbase shielding must have transparent inputs and no transparent outputs",
            ));
        }
    }
    Ok(transaction)
}

async fn transaction_status(
    client: &mut LightwalletdClient,
    txid: zcash_protocol::TxId,
    network: WalletNetwork,
) -> Result<TransactionStatus, WalletRpcError> {
    match client
        .get_transaction(TxFilter {
            block: None,
            index: 0,
            hash: txid.as_ref().to_vec(),
        })
        .await
    {
        Ok(response) => verified_status_response(response.into_inner(), txid, network),
        Err(status) if status.code() == Code::NotFound => Ok(TransactionStatus::Unknown),
        Err(status) => Err(status.into()),
    }
}

fn verified_status_response(
    response: RawTransaction,
    requested_txid: zcash_protocol::TxId,
    network: WalletNetwork,
) -> Result<TransactionStatus, WalletRpcError> {
    let returned = inspect_signed_transaction(&response.data, network)?;
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
    network: WalletNetwork,
) -> Result<BroadcastResult, WalletRpcError> {
    let transaction = inspect_signed_transaction(&raw, network)?;
    let txid = transaction.txid();
    let before = transaction_status(client, txid, network).await?;
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
            return match transaction_status(client, txid, network).await {
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
        return match transaction_status(client, txid, network).await {
            Ok(after) if is_live_status(&after) => Ok(BroadcastResult {
                txid: txid.to_string(),
                disposition: BroadcastDisposition::AlreadyKnown,
                status: after,
            }),
            Ok(TransactionStatus::Unknown | TransactionStatus::Orphaned) => {
                Err(WalletRpcError::Rejected {
                    code: response.error_code,
                    message: response.error_message,
                })
            }
            Ok(_) => unreachable!("all known transaction states were handled"),
            Err(status_error) => Err(WalletRpcError::AmbiguousBroadcast {
                txid: txid.to_string(),
                reason: format!(
                    "node returned rejection {} ({}) but the exact-txid status check failed ({status_error})",
                    response.error_code, response.error_message
                ),
            }),
        };
    }

    match transaction_status(client, txid, network).await {
        Ok(status) if is_live_status(&status) => Ok(BroadcastResult {
            txid: txid.to_string(),
            disposition: BroadcastDisposition::Submitted,
            status,
        }),
        Ok(TransactionStatus::Unknown | TransactionStatus::Orphaned) => {
            Err(WalletRpcError::AmbiguousBroadcast {
                txid: txid.to_string(),
                reason: "node accepted submission but did not report the exact transaction in its live mempool or best chain"
                    .to_owned(),
            })
        }
        Ok(_) => unreachable!("all known transaction states were handled"),
        Err(status_error) => Err(WalletRpcError::AmbiguousBroadcast {
            txid: txid.to_string(),
            reason: format!(
                "node accepted submission but the exact-txid status check failed ({status_error})"
            ),
        }),
    }
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
            inspect_signed_transaction(&[1, 2, 3], WalletNetwork::Regtest),
            Err(WalletRpcError::InvalidTransaction(_))
        ));
    }

    #[test]
    fn synchronizer_utxo_requests_preserve_receivers_in_the_wcash_namespace() {
        let network = WalletNetwork::Regtest;
        let receiver = TransparentAddress::PublicKeyHash([0x42; 20]);
        let zcash = receiver.encode(&network.parameters());
        let expected = encode_wcash_transparent_receiver(receiver, network);
        let request = GetAddressUtxosArg {
            addresses: vec![zcash],
            start_height: 7,
            max_entries: 11,
        };
        let mut frame = vec![0];
        frame.extend_from_slice(&u32::try_from(request.encoded_len()).unwrap().to_be_bytes());
        request.encode(&mut frame).unwrap();

        let rewritten = rewrite_utxo_request_frame(&frame, network).unwrap();
        let decoded = GetAddressUtxosArg::decode(&rewritten[5..]).unwrap();
        assert_eq!(decoded.addresses, vec![expected]);
        assert_eq!(decoded.start_height, 7);
        assert_eq!(decoded.max_entries, 11);

        let mut compressed = frame;
        compressed[0] = 1;
        assert!(matches!(
            rewrite_utxo_request_frame(&compressed, network),
            Err(WalletRpcError::InvalidNamespaceRequest(_))
        ));

        assert!(decode_wcash_transparent_address(
            &encode_wcash_transparent_receiver(receiver, network),
            network,
        )
        .is_ok());
        assert!(matches!(
            decode_wcash_transparent_address(
                &encode_wcash_transparent_receiver(receiver, WalletNetwork::Testnet),
                network,
            ),
            Err(WalletRpcError::InvalidTransparentAddress)
        ));
        assert!(matches!(
            decode_wcash_transparent_address(&receiver.encode(&network.parameters()), network),
            Err(WalletRpcError::InvalidTransparentAddress)
        ));
    }

    #[test]
    fn synchronizer_utxo_requests_are_capped_before_reaching_zebra() {
        let network = WalletNetwork::Regtest;
        let receiver = TransparentAddress::PublicKeyHash([0x24; 20]);
        let address = receiver.encode(&network.parameters());
        let page_limit = u32::try_from(TRANSPARENT_UTXO_PAGE_OUTPUTS).unwrap();

        for (requested, expected) in [
            (0, page_limit),
            (11, 11),
            (page_limit, page_limit),
            (page_limit + 1, page_limit),
        ] {
            let request = GetAddressUtxosArg {
                addresses: vec![address.clone()],
                start_height: 7,
                max_entries: requested,
            };
            let mut frame = vec![0];
            frame.extend_from_slice(&u32::try_from(request.encoded_len()).unwrap().to_be_bytes());
            request.encode(&mut frame).unwrap();

            let rewritten = rewrite_utxo_request_frame(&frame, network).unwrap();
            let decoded = GetAddressUtxosArg::decode(&rewritten[5..]).unwrap();
            assert_eq!(decoded.max_entries, expected);
        }
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
                WalletNetwork::Regtest,
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

    #[test]
    fn transparent_history_usage_enforces_count_and_encoded_byte_boundaries() {
        assert_eq!(
            checked_transparent_history_usage(1, 6, 4, 2, 10).unwrap(),
            (2, 10)
        );
        assert!(matches!(
            checked_transparent_history_usage(2, 6, 1, 2, 10),
            Err(WalletRpcError::TransparentUtxoLimit { maximum: 2 })
        ));
        assert!(matches!(
            checked_transparent_history_usage(1, 6, 5, 2, 10),
            Err(WalletRpcError::TransparentUtxoByteLimit { maximum: 10 })
        ));
        assert!(matches!(
            checked_transparent_history_usage(1, usize::MAX, 1, 2, usize::MAX),
            Err(WalletRpcError::TransparentUtxoByteLimit { .. })
        ));
    }

    #[test]
    fn transparent_utxo_pagination_repeats_a_full_page_boundary_height() {
        assert_eq!(
            next_transparent_page_start(10, 9, Some(10), Some(12), 10).unwrap(),
            None
        );
        assert_eq!(
            next_transparent_page_start(10, 10, Some(10), Some(12), 10).unwrap(),
            Some(12)
        );

        let creator = |marker, height| TransparentUnspentCreator {
            txid: TxId::from_bytes([marker; 32]),
            outputs: vec![TransparentUnspentOutput {
                output_index: 0,
                script: vec![marker],
                value_zat: u64::from(marker),
                height,
            }],
        };
        let complete = TransparentUnspentPage {
            creators: vec![creator(1, 10), creator(2, 12)],
            output_count: 10,
            first_height: Some(10),
            last_height: Some(12),
        }
        .into_complete_creators(Some(12));
        assert_eq!(complete.len(), 1);
        assert_eq!(complete[0].txid, TxId::from_bytes([1; 32]));
    }

    #[test]
    fn transparent_utxo_pagination_rejects_a_same_height_full_page() {
        assert!(matches!(
            next_transparent_page_start(10, 10, Some(10), Some(10), 10),
            Err(WalletRpcError::TransparentUtxoPageCannotAdvance {
                height: 10,
                maximum: 10
            })
        ));
        assert!(!transparent_utxo_page_is_block_safe(
            MAX_BLOCK_BYTES / MIN_P2PKH_OUTPUT_ENCODED_BYTES
        ));
        assert!(transparent_utxo_page_is_block_safe(
            TRANSPARENT_UTXO_PAGE_OUTPUTS
        ));
    }

    #[test]
    fn current_utxos_must_cover_every_output_to_the_payout_receiver() {
        assert!(exact_unique_index_sets_match(&[7, 2], &[2, 7]));
        assert!(!exact_unique_index_sets_match(&[2, 7], &[2]));
        assert!(!exact_unique_index_sets_match(&[2], &[2, 7]));
        assert!(!exact_unique_index_sets_match(&[2, 7], &[2, 2]));
    }

    #[test]
    fn full_creator_validation_rejects_an_unreported_spent_sibling() {
        use zcash_protocol::{consensus::BranchId, value::Zatoshis};
        use zcash_transparent::bundle::{Authorized, Bundle, OutPoint, TxIn, TxOut};

        let receiver = TransparentAddress::PublicKeyHash([0x55; 20]);
        let script = Script::from(receiver.script());
        let transaction = zcash_primitives::transaction::TransactionData::from_parts_v6(
            BranchId::WcashTestnetV1,
            0,
            zcash_protocol::consensus::BlockHeight::from_u32(0),
            Some(Bundle {
                vin: vec![TxIn::<Authorized>::from_parts(
                    OutPoint::NULL,
                    Script::default(),
                    u32::MAX,
                )],
                vout: vec![
                    TxOut::new(Zatoshis::const_from_u64(11), script.clone()),
                    TxOut::new(Zatoshis::const_from_u64(12), script.clone()),
                ],
                authorization: Authorized,
            }),
            None,
            None,
            None,
        )
        .freeze()
        .unwrap();
        let output = |output_index, value_zat| TransparentUnspentOutput {
            output_index,
            script: script.0 .0.clone(),
            value_zat,
            height: 7,
        };

        assert!(matches!(
            validate_transparent_unspent_outputs(
                &transaction,
                &[output(0, 11)],
                &receiver,
                transaction.txid(),
            ),
            Err(WalletRpcError::TransparentUtxoMismatch(_))
        ));
        assert!(validate_transparent_unspent_outputs(
            &transaction,
            &[output(1, 12), output(0, 11)],
            &receiver,
            transaction.txid(),
        )
        .is_ok());
    }
}
