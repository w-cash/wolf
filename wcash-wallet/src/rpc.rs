//! Fail-closed access to Zebra's compact-block gRPC interface.

use std::{
    collections::{BTreeMap, HashSet},
    io::Cursor,
    net::IpAddr,
    time::Duration,
};

use futures_util::{stream, StreamExt, TryStreamExt};
use prost::Message;
use serde::Serialize;
use thiserror::Error;
use tonic::{
    transport::{Channel, ClientTlsConfig, Endpoint},
    Code,
};
use zcash_client_backend::{
    data_api::{
        chain::{ChainState, CommitmentTreeRoot},
        IRONWOOD_SHARD_HEIGHT,
    },
    proto::{
        compact_formats::CompactBlock,
        service::{
            compact_tx_streamer_client::CompactTxStreamerClient, BlockId, BlockRange, ChainSpec,
            Empty, GetAddressUtxosArg, GetSubtreeRootsArg, LightdInfo, RawTransaction,
            ShieldedProtocol, TransparentAddressBlockFilter, TreeState, TxFilter,
        },
    },
};
use zcash_primitives::{
    block::BlockHash,
    merkle_tree::{write_commitment_tree, HashSer},
    transaction::{Transaction, TxVersion},
};
use zcash_protocol::{constants::MAX_BLOCK_BYTES, TxId};
use zcash_transparent::address::{Script, TransparentAddress};
use zebra_chain::{
    parameters::ConsensusBranchId,
    primitives::{WcashAddress, WcashAddressKind},
};

use crate::{cache::MAX_ATTESTED_COMPACT_BLOCKS_PER_RANGE, BlockRef, WalletNetwork};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const TCP_KEEPALIVE: Duration = Duration::from_secs(30);
pub(crate) const MAX_SUBTREE_ROOTS_PER_REQUEST: u32 = 1_024;
pub(crate) const MAX_TRANSPARENT_HISTORY_BLOCKS_PER_REQUEST: u32 = 16;
const MAX_SUBTREE_HASH_ATTESTATIONS_IN_FLIGHT: usize = 16;

type LightwalletdClient = CompactTxStreamerClient<Channel>;

/// An independently attested block and its legacy-free commitment-tree state.
pub(crate) struct AttestedChainState {
    pub(crate) block: BlockRef,
    pub(crate) state: ChainState,
    pub(crate) ironwood_tree_size: u32,
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

impl TransparentUnspentCreator {
    pub(crate) const fn txid(&self) -> TxId {
        self.txid
    }
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

/// A compact-block client that has proved it serves one exact Wcash network.
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
    /// Connects to an endpoint and requires its reported chain, active branch,
    /// and height-zero block to match `network` before returning a usable client.
    pub async fn connect(endpoint: &str, network: WalletNetwork) -> Result<Self, WalletRpcError> {
        let channel = connect_channel(endpoint).await?;
        let mut inner = CompactTxStreamerClient::new(channel.clone())
            .max_decoding_message_size(MAX_BLOCK_BYTES);
        attest_network(&mut inner, network).await?;
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

    /// Downloads an exact legacy-free tree state by its independently attested
    /// block identifier.
    pub(crate) async fn chain_state(
        &mut self,
        height: u32,
    ) -> Result<AttestedChainState, WalletRpcError> {
        let block = self.compact_block_ref(height).await?;
        let state = self
            .inner
            .get_tree_state(BlockId {
                height: 0,
                hash: block.hash.to_vec(),
            })
            .await?
            .into_inner();
        let state = validate_tree_state(self.network, block, &state)?;
        let ironwood_tree_size =
            u32::try_from(state.final_ironwood_tree().tree_size()).map_err(|_| {
                WalletRpcError::MalformedTreeState {
                    height,
                    reason: "Ironwood tree size exceeds the compact-block encoding".to_owned(),
                }
            })?;
        Ok(AttestedChainState {
            block,
            state,
            ironwood_tree_size,
        })
    }

    /// Streams one exact ascending compact-block range with explicit count and
    /// decoded-byte limits.
    pub(crate) async fn compact_block_range(
        &mut self,
        start: u32,
        end: u32,
    ) -> Result<Vec<CompactBlock>, WalletRpcError> {
        let mut validator = CompactRangeValidator::new(start, end)?;
        let mut stream = self
            .inner
            .get_block_range(BlockRange {
                start: Some(BlockId {
                    height: u64::from(start),
                    hash: Vec::new(),
                }),
                end: Some(BlockId {
                    height: u64::from(end - 1),
                    hash: Vec::new(),
                }),
                pool_types: Vec::new(),
            })
            .await?
            .into_inner();
        let mut blocks = Vec::with_capacity(validator.expected_count());
        loop {
            let next = tokio::time::timeout(REQUEST_TIMEOUT, stream.message())
                .await
                .map_err(|_| WalletRpcError::ResponseTimeout("compact block"))??;
            match next {
                Some(block) => {
                    validator.observe(&block)?;
                    blocks.push(block);
                }
                None => break,
            }
        }
        validator.finish()?;

        let expected_tip = blocks
            .last()
            .expect("the range validator rejects empty requested ranges");
        let expected_tip_hash: [u8; 32] =
            expected_tip.hash.as_slice().try_into().map_err(|_| {
                WalletRpcError::MalformedBlockId {
                    field: "compact range tip",
                    height: expected_tip.height,
                }
            })?;
        let canonical_tip = self.compact_block_ref(end - 1).await?;
        if canonical_tip.hash != expected_tip_hash {
            return Err(WalletRpcError::CompactRangeTipMismatch { height: end - 1 });
        }
        Ok(blocks)
    }

    /// Downloads an exact page of canonical Ironwood subtree roots.
    pub(crate) async fn ironwood_subtree_roots(
        &mut self,
        start_index: u32,
        count: u32,
        tip_height: u32,
    ) -> Result<Vec<CommitmentTreeRoot<orchard::tree::MerkleHashOrchard>>, WalletRpcError> {
        if count == 0 || count > MAX_SUBTREE_ROOTS_PER_REQUEST {
            return Err(WalletRpcError::InvalidSubtreeRequest);
        }
        let mut stream = self
            .inner
            .get_subtree_roots(GetSubtreeRootsArg {
                start_index,
                shielded_protocol: ShieldedProtocol::Ironwood as i32,
                max_entries: count,
            })
            .await?
            .into_inner();
        let mut roots = Vec::with_capacity(
            usize::try_from(count).expect("the subtree page limit fits in usize"),
        );
        let mut completing_blocks = Vec::with_capacity(
            usize::try_from(count).expect("the subtree page limit fits in usize"),
        );
        let mut previous_height = None;
        loop {
            let next = tokio::time::timeout(REQUEST_TIMEOUT, stream.message())
                .await
                .map_err(|_| WalletRpcError::ResponseTimeout("subtree root"))??;
            let Some(root) = next else { break };
            if roots.len() >= usize::try_from(count).expect("the subtree page limit fits in usize")
            {
                return Err(WalletRpcError::UnexpectedSubtreeCount {
                    expected: count,
                    actual: roots.len().saturating_add(1),
                });
            }
            let height = u32::try_from(root.completing_block_height).map_err(|_| {
                WalletRpcError::MalformedSubtreeRoot("completing height is out of range")
            })?;
            if height == 0
                || height > tip_height
                || previous_height.is_some_and(|previous| height <= previous)
            {
                return Err(WalletRpcError::MalformedSubtreeRoot(
                    "completing heights are not strictly ascending within the attested chain",
                ));
            }
            let root_hash = orchard::tree::MerkleHashOrchard::read(root.root_hash.as_slice())
                .map_err(|_| WalletRpcError::MalformedSubtreeRoot("invalid root hash"))?;
            if root.root_hash.len() != 32 || root.completing_block_hash.len() != 32 {
                return Err(WalletRpcError::MalformedSubtreeRoot(
                    "root and completing block hashes must be 32 bytes",
                ));
            }
            let mut completing_hash: [u8; 32] = root
                .completing_block_hash
                .as_slice()
                .try_into()
                .expect("length was checked above");
            completing_hash.reverse();
            completing_blocks.push((height, completing_hash));
            roots.push(CommitmentTreeRoot::from_parts(
                zcash_protocol::consensus::BlockHeight::from_u32(height),
                root_hash,
            ));
            previous_height = Some(height);
        }
        if roots.len() != usize::try_from(count).expect("the subtree page limit fits in usize") {
            return Err(WalletRpcError::UnexpectedSubtreeCount {
                expected: count,
                actual: roots.len(),
            });
        }

        let attested_client = self.clone();
        stream::iter(completing_blocks.into_iter().enumerate().map(
            |(offset, (height, expected_hash))| {
                let mut predecessor_client = attested_client.clone();
                let mut completing_client = attested_client.clone();
                async move {
                    let offset = u32::try_from(offset).map_err(|_| {
                        WalletRpcError::MalformedSubtreeRoot("subtree index is out of range")
                    })?;
                    let index = start_index.checked_add(offset).ok_or(
                        WalletRpcError::MalformedSubtreeRoot("subtree index overflow"),
                    )?;
                    let (predecessor, completing) = tokio::try_join!(
                        predecessor_client.chain_state(height - 1),
                        completing_client.chain_state(height),
                    )?;
                    if completing.block.hash != expected_hash {
                        return Err(WalletRpcError::MalformedSubtreeRoot(
                            "completing block hash is not canonical",
                        ));
                    }
                    validate_ironwood_subtree_completion(
                        index,
                        predecessor.ironwood_tree_size,
                        completing.ironwood_tree_size,
                    )
                }
            },
        ))
        .buffer_unordered(MAX_SUBTREE_HASH_ATTESTATIONS_IN_FLIGHT)
        .try_collect::<Vec<()>>()
        .await?;
        Ok(roots)
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
        loop {
            let next = tokio::time::timeout(REQUEST_TIMEOUT, stream.message())
                .await
                .map_err(|_| WalletRpcError::ResponseTimeout("transparent UTXO"))??;
            let Some(utxo) = next else { break };
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
        let txid = creator.txid;
        let raw = self
            .inner
            .get_transaction(TxFilter {
                block: None,
                index: 0,
                hash: txid.as_ref().to_vec(),
            })
            .await?
            .into_inner();
        let (transaction, expected_height) = validate_transparent_creator_transaction(
            self.network,
            &creator,
            &raw.data,
            raw.height,
        )?;
        Ok(MinedTransparentTransaction {
            transaction,
            height: expected_height,
        })
    }

    /// Revalidates a previously persisted creator against a fresh current-UTXO
    /// page without performing another transaction RPC.
    pub(crate) fn validate_stored_transparent_creator(
        &self,
        creator: &TransparentUnspentCreator,
        raw: &[u8],
        mined_height: u32,
    ) -> Result<bool, WalletRpcError> {
        validate_transparent_creator_transaction(
            self.network,
            creator,
            raw,
            u64::from(mined_height),
        )
        .map(|(transaction, _)| {
            transaction
                .transparent_bundle()
                .is_some_and(|bundle| bundle.is_coinbase())
        })
    }

    /// Fetches every mined transaction indexed for the Wcash payout receiver
    /// in one exact, inclusive block range.
    ///
    /// This deliberately uses the permissive Wcash V6 consensus parser rather
    /// than the locally-created transaction policy: transparent-only external
    /// spends are the records that let SQLite retire spent coinbase outputs.
    pub(crate) async fn transparent_transactions_range(
        &mut self,
        address: &str,
        start_height: u32,
        end_height: u32,
    ) -> Result<Vec<MinedTransparentTransaction>, WalletRpcError> {
        decode_wcash_transparent_address(address, self.network)?;
        let (maximum_transactions, maximum_bytes) =
            transparent_history_limits(start_height, end_height)?;
        let expected_predecessor = self.compact_block_ref(start_height - 1).await?;
        let expected_end = self.compact_block_ref(end_height).await?;
        let mut stream = self
            .inner
            .get_taddress_transactions(TransparentAddressBlockFilter {
                address: address.to_owned(),
                range: Some(BlockRange {
                    start: Some(BlockId {
                        height: u64::from(start_height),
                        hash: Vec::new(),
                    }),
                    end: Some(BlockId {
                        height: u64::from(end_height),
                        hash: Vec::new(),
                    }),
                    pool_types: Vec::new(),
                }),
            })
            .await?
            .into_inner();

        let mut transactions = Vec::new();
        let mut seen = HashSet::new();
        let mut previous_height = None;
        let mut encoded_bytes = 0usize;
        loop {
            let next = tokio::time::timeout(REQUEST_TIMEOUT, stream.message())
                .await
                .map_err(|_| WalletRpcError::ResponseTimeout("transparent history"))??;
            let Some(raw) = next else { break };
            if transactions.len() >= maximum_transactions {
                return Err(WalletRpcError::TransparentHistoryTransactionLimit {
                    maximum: maximum_transactions,
                });
            }
            let height = u32::try_from(raw.height).map_err(|_| {
                WalletRpcError::UnexpectedTransparentTransactionHeight {
                    start: start_height,
                    end: end_height,
                    actual: raw.height,
                }
            })?;
            if !(start_height..=end_height).contains(&height)
                || previous_height.is_some_and(|previous| height < previous)
            {
                return Err(WalletRpcError::UnexpectedTransparentTransactionHeight {
                    start: start_height,
                    end: end_height,
                    actual: raw.height,
                });
            }
            encoded_bytes = encoded_bytes.checked_add(raw.data.len()).ok_or(
                WalletRpcError::TransparentHistoryByteLimit {
                    maximum: maximum_bytes,
                },
            )?;
            if raw.data.len() > MAX_BLOCK_BYTES || encoded_bytes > maximum_bytes {
                return Err(WalletRpcError::TransparentHistoryByteLimit {
                    maximum: maximum_bytes,
                });
            }
            let transaction = parse_wcash_v6_transaction(&raw.data, self.network)?;
            if !seen.insert(transaction.txid()) {
                return Err(WalletRpcError::DuplicateTransparentTransaction(
                    transaction.txid().to_string(),
                ));
            }
            transactions.push(MinedTransparentTransaction {
                transaction,
                height,
            });
            previous_height = Some(height);
        }
        if self.compact_block_ref(start_height - 1).await? != expected_predecessor
            || self.compact_block_ref(end_height).await? != expected_end
        {
            return Err(WalletRpcError::TransparentHistoryChainChanged {
                start: start_height,
                end: end_height,
            });
        }
        Ok(transactions)
    }
}

fn validate_transparent_creator_transaction(
    network: WalletNetwork,
    creator: &TransparentUnspentCreator,
    raw: &[u8],
    actual_height: u64,
) -> Result<(Transaction, u32), WalletRpcError> {
    let txid = creator.txid;
    let expected_height = creator
        .outputs
        .first()
        .ok_or_else(|| {
            WalletRpcError::TransparentUtxoMismatch(format!(
                "creator transaction {txid} has no reported current outputs"
            ))
        })?
        .height;
    if creator
        .outputs
        .iter()
        .any(|output| output.height != expected_height)
    {
        return Err(WalletRpcError::TransparentUtxoMismatch(format!(
            "creator transaction {txid} has inconsistent mined heights"
        )));
    }
    if actual_height != u64::from(expected_height) {
        return Err(WalletRpcError::UnexpectedTransparentTransactionHeight {
            start: expected_height,
            end: expected_height,
            actual: actual_height,
        });
    }
    if raw.len() > MAX_BLOCK_BYTES {
        return Err(WalletRpcError::OversizedTransparentTransaction {
            txid: txid.to_string(),
            encoded_bytes: raw.len(),
        });
    }
    let transaction = parse_wcash_v6_transaction(raw, network)?;
    ensure_expected_txid(transaction.txid(), txid)?;
    let receiver = creator
        .outputs
        .first()
        .and_then(|output| transparent_receiver_from_script(&output.script))
        .ok_or_else(|| {
            WalletRpcError::TransparentUtxoMismatch(format!(
                "creator transaction {txid} has a non-P2PKH output script"
            ))
        })?;
    validate_transparent_unspent_outputs(&transaction, &creator.outputs, &receiver, txid)?;
    Ok((transaction, expected_height))
}

fn transparent_history_limits(
    start_height: u32,
    end_height: u32,
) -> Result<(usize, usize), WalletRpcError> {
    let block_count = end_height
        .checked_sub(start_height)
        .and_then(|difference| difference.checked_add(1))
        .filter(|count| start_height > 0 && *count <= MAX_TRANSPARENT_HISTORY_BLOCKS_PER_REQUEST)
        .ok_or(WalletRpcError::InvalidTransparentHistoryRequest)?;
    let maximum_bytes = usize::try_from(block_count)
        .expect("the transparent history range fits in usize")
        .checked_mul(MAX_BLOCK_BYTES)
        .ok_or(WalletRpcError::TransparentHistoryByteLimit {
            maximum: usize::MAX,
        })?;
    // Every matching transaction contains either a P2PKH output (at least 34
    // encoded bytes) or a larger transparent input spending one.
    let maximum_transactions = maximum_bytes
        .checked_div(MIN_P2PKH_OUTPUT_ENCODED_BYTES)
        .and_then(|count| count.checked_add(1))
        .ok_or(WalletRpcError::TransparentHistoryTransactionLimit {
            maximum: usize::MAX,
        })?;
    Ok((maximum_transactions, maximum_bytes))
}

fn validate_ironwood_subtree_completion(
    index: u32,
    predecessor_tree_size: u32,
    completing_tree_size: u32,
) -> Result<(), WalletRpcError> {
    let subtree_number =
        u64::from(index)
            .checked_add(1)
            .ok_or(WalletRpcError::MalformedSubtreeRoot(
                "subtree index overflow",
            ))?;
    let boundary = subtree_number
        .checked_shl(u32::from(IRONWOOD_SHARD_HEIGHT))
        .ok_or(WalletRpcError::MalformedSubtreeRoot(
            "subtree boundary overflow",
        ))?;
    let predecessor_tree_size = u64::from(predecessor_tree_size);
    let completing_tree_size = u64::from(completing_tree_size);
    let added_commitments = completing_tree_size
        .checked_sub(predecessor_tree_size)
        .ok_or(WalletRpcError::MalformedSubtreeRoot(
            "Ironwood tree size decreased",
        ))?;
    if predecessor_tree_size >= boundary || completing_tree_size < boundary {
        return Err(WalletRpcError::MalformedSubtreeRoot(
            "completing height does not cross the requested subtree boundary",
        ));
    }
    if added_commitments
        > u64::try_from(crate::cache::MAX_IRONWOOD_ACTIONS_PER_BLOCK)
            .expect("the per-block Ironwood action bound fits in u64")
    {
        return Err(WalletRpcError::MalformedSubtreeRoot(
            "completing block exceeds the Ironwood action limit",
        ));
    }
    Ok(())
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

#[derive(Debug)]
struct CompactRangeValidator {
    start: u32,
    end: u32,
    next_height: u32,
    count: usize,
    encoded_bytes: usize,
}

impl CompactRangeValidator {
    fn new(start: u32, end: u32) -> Result<Self, WalletRpcError> {
        let count = end
            .checked_sub(start)
            .ok_or(WalletRpcError::InvalidCompactRange)?;
        if start == 0 || count == 0 || count > MAX_ATTESTED_COMPACT_BLOCKS_PER_RANGE {
            return Err(WalletRpcError::InvalidCompactRange);
        }
        Ok(Self {
            start,
            end,
            next_height: start,
            count: 0,
            encoded_bytes: 0,
        })
    }

    fn expected_count(&self) -> usize {
        usize::try_from(self.end - self.start)
            .expect("the compact-block request limit fits in usize")
    }

    fn observe(&mut self, block: &CompactBlock) -> Result<(), WalletRpcError> {
        if self.count >= self.expected_count() {
            return Err(WalletRpcError::UnexpectedCompactBlockCount {
                expected: self.expected_count(),
                actual: self.count.saturating_add(1),
            });
        }
        if block.height != u64::from(self.next_height) {
            return Err(WalletRpcError::UnexpectedCompactBlockHeight {
                expected: self.next_height,
                actual: block.height,
            });
        }
        let block_bytes = block.encoded_len();
        if block_bytes > MAX_BLOCK_BYTES {
            return Err(WalletRpcError::CompactBlockByteLimit {
                maximum: MAX_BLOCK_BYTES,
            });
        }
        let aggregate_maximum = MAX_BLOCK_BYTES
            .checked_mul(
                usize::try_from(MAX_ATTESTED_COMPACT_BLOCKS_PER_RANGE)
                    .expect("the compact-block request limit fits in usize"),
            )
            .expect("the fixed compact-block aggregate bound fits in usize");
        self.encoded_bytes = self
            .encoded_bytes
            .checked_add(block_bytes)
            .filter(|total| *total <= aggregate_maximum)
            .ok_or(WalletRpcError::CompactBlockByteLimit {
                maximum: aggregate_maximum,
            })?;
        self.count += 1;
        self.next_height = self
            .next_height
            .checked_add(1)
            .ok_or(WalletRpcError::InvalidCompactRange)?;
        Ok(())
    }

    fn finish(self) -> Result<(), WalletRpcError> {
        let expected = self.expected_count();
        if self.count != expected {
            Err(WalletRpcError::UnexpectedCompactBlockCount {
                expected,
                actual: self.count,
            })
        } else {
            Ok(())
        }
    }
}

/// Errors returned by the network boundary.
#[derive(Debug, Error)]
pub enum WalletRpcError {
    /// The endpoint is not a valid absolute HTTP(S) URI.
    #[error("invalid lightwalletd endpoint: {0}")]
    InvalidEndpoint(String),
    /// Plaintext transport was requested for a host that is not a literal
    /// loopback IP address.
    #[error("plaintext lightwalletd endpoints require a literal loopback IP address")]
    InsecureEndpoint,
    /// Establishing the gRPC channel failed.
    #[error("could not connect to lightwalletd: {0}")]
    Connect(#[from] tonic::transport::Error),
    /// A gRPC request failed.
    #[error("lightwalletd request failed: {0}")]
    Status(#[from] tonic::Status),
    /// A streamed response stopped making progress within the request timeout.
    #[error("lightwalletd {0} stream timed out")]
    ResponseTimeout(&'static str),
    /// A compact-block range is empty, starts at genesis, or exceeds the owned boundary limit.
    #[error("invalid compact-block range")]
    InvalidCompactRange,
    /// The compact-block stream returned an unexpected number of messages.
    #[error("compact-block stream returned {actual} blocks; expected {expected}")]
    UnexpectedCompactBlockCount {
        /// Exact requested count.
        expected: usize,
        /// Count observed in the response.
        actual: usize,
    },
    /// A compact-block response was outside the exact requested height sequence.
    #[error("compact-block stream returned height {actual}; expected {expected}")]
    UnexpectedCompactBlockHeight {
        /// Exact next requested height.
        expected: u32,
        /// Height reported by the service.
        actual: u64,
    },
    /// A compact-block stream exceeded its decoded-byte resource limit.
    #[error("compact-block stream exceeds the {maximum}-byte safety limit")]
    CompactBlockByteLimit {
        /// Per-block or aggregate decoded-byte limit.
        maximum: usize,
    },
    /// The final streamed block was not canonical at its height.
    #[error("compact-block range tip at height {height} is not canonical")]
    CompactRangeTipMismatch {
        /// Final height in the requested range.
        height: u32,
    },
    /// An Ironwood subtree request is empty or exceeds the page limit.
    #[error("invalid Ironwood subtree-root request")]
    InvalidSubtreeRequest,
    /// A subtree-root stream returned an unexpected number of messages.
    #[error("subtree-root stream returned {actual} roots; expected {expected}")]
    UnexpectedSubtreeCount {
        /// Exact requested count.
        expected: u32,
        /// Count observed in the response.
        actual: usize,
    },
    /// A subtree-root response violates Wcash ordering, encoding, or chain binding.
    #[error("malformed Ironwood subtree root: {0}")]
    MalformedSubtreeRoot(&'static str),
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
    /// The service reported the wrong BIP70 chain identifier.
    #[error("lightwalletd chain mismatch: expected {expected}, got {actual}")]
    WrongChainName {
        /// Expected BIP70 network name.
        expected: String,
        /// Actual BIP70 network name.
        actual: String,
    },
    /// The service reported a branch ID that is not active at its tip.
    #[error("lightwalletd branch mismatch at height {height}: expected {expected}, got {actual}")]
    WrongConsensusBranchId {
        /// Height reported by the service.
        height: u64,
        /// Expected lowercase eight-digit branch identifier.
        expected: String,
        /// Actual branch identifier.
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
    /// An address-history response exceeded the number of transactions that
    /// can fit in its bounded block range.
    #[error("transparent history exceeds the {maximum}-transaction safety limit")]
    TransparentHistoryTransactionLimit {
        /// Maximum accepted transactions in this exact range.
        maximum: usize,
    },
    /// An address-history response exceeded the aggregate serialized size of
    /// the consensus-valid blocks in its requested range.
    #[error("transparent history exceeds the {maximum}-byte safety limit")]
    TransparentHistoryByteLimit {
        /// Maximum accepted serialized transaction bytes.
        maximum: usize,
    },
    /// The same transaction appeared twice in one exact address-history range.
    #[error("transparent history duplicated transaction {0}")]
    DuplicateTransparentTransaction(String),
    /// Either endpoint of an address-history range changed while it streamed.
    #[error("canonical chain changed while reading transparent history {start}..={end}")]
    TransparentHistoryChainChanged {
        /// First requested height.
        start: u32,
        /// Last requested height.
        end: u32,
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
    let endpoint = configured_endpoint(endpoint)?;
    let endpoint = endpoint
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(REQUEST_TIMEOUT)
        .tcp_keepalive(Some(TCP_KEEPALIVE));
    Ok(endpoint.connect().await?)
}

fn configured_endpoint(endpoint: &str) -> Result<Endpoint, WalletRpcError> {
    let mut endpoint = Endpoint::from_shared(endpoint.to_owned()).map_err(|_| {
        WalletRpcError::InvalidEndpoint("endpoint must be an absolute HTTP(S) URI".to_owned())
    })?;
    validate_endpoint(endpoint.uri())?;
    if endpoint.uri().scheme_str() == Some("https") {
        endpoint = endpoint.tls_config(ClientTlsConfig::new().with_webpki_roots())?;
    }
    Ok(endpoint)
}

fn validate_endpoint(uri: &tonic::codegen::http::Uri) -> Result<(), WalletRpcError> {
    let scheme = uri
        .scheme_str()
        .ok_or_else(|| WalletRpcError::InvalidEndpoint("missing URI scheme".to_owned()))?;
    let authority = uri
        .authority()
        .ok_or_else(|| WalletRpcError::InvalidEndpoint("missing endpoint host".to_owned()))?;
    if authority.as_str().contains('@') {
        return Err(WalletRpcError::InvalidEndpoint(
            "endpoint user information is not permitted".to_owned(),
        ));
    }
    let host = uri
        .host()
        .ok_or_else(|| WalletRpcError::InvalidEndpoint("missing endpoint host".to_owned()))?;
    if !matches!(scheme, "http" | "https") {
        return Err(WalletRpcError::InvalidEndpoint(
            "endpoint scheme must be http or https".to_owned(),
        ));
    }
    if host.eq_ignore_ascii_case("localhost") {
        return Err(WalletRpcError::InvalidEndpoint(
            "localhost names are not permitted; use a literal loopback IP".to_owned(),
        ));
    }
    if scheme == "http" && !is_literal_loopback(host) {
        return Err(WalletRpcError::InsecureEndpoint);
    }
    Ok(())
}

fn is_literal_loopback(host: &str) -> bool {
    let host = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host);
    host.parse::<IpAddr>()
        .is_ok_and(|address| address.is_loopback())
}

fn validate_tree_state(
    network: WalletNetwork,
    block: BlockRef,
    state: &TreeState,
) -> Result<ChainState, WalletRpcError> {
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
    let chain_state =
        state
            .to_chain_state()
            .map_err(|error| WalletRpcError::MalformedTreeState {
                height: block.height,
                reason: error.to_string(),
            })?;
    validate_canonical_tree_encoding(state, block.height)?;
    if chain_state.final_sapling_tree().tree_size() != 0
        || chain_state.final_orchard_tree().tree_size() != 0
    {
        return Err(WalletRpcError::MalformedTreeState {
            height: block.height,
            reason: "Wcash tree state contains a disabled legacy pool".to_owned(),
        });
    }
    Ok(chain_state)
}

fn validate_canonical_tree_encoding(state: &TreeState, height: u32) -> Result<(), WalletRpcError> {
    fn canonical_bytes(encoded: &str) -> Result<Option<Vec<u8>>, String> {
        if encoded.is_empty() {
            Ok(None)
        } else {
            hex::decode(encoded)
                .map(Some)
                .map_err(|error| format!("invalid tree hex: {error}"))
        }
    }

    let malformed = |error: std::io::Error| WalletRpcError::MalformedTreeState {
        height,
        reason: error.to_string(),
    };
    let sapling = state.sapling_tree().map_err(malformed)?;
    let orchard = state.orchard_tree().map_err(malformed)?;
    let ironwood = state.ironwood_tree().map_err(malformed)?;
    let mut sapling_canonical = Vec::new();
    let mut orchard_canonical = Vec::new();
    let mut ironwood_canonical = Vec::new();
    write_commitment_tree(&sapling, &mut sapling_canonical).map_err(malformed)?;
    write_commitment_tree(&orchard, &mut orchard_canonical).map_err(malformed)?;
    write_commitment_tree(&ironwood, &mut ironwood_canonical).map_err(malformed)?;
    for (name, encoded, canonical) in [
        ("Sapling", state.sapling_tree.as_str(), sapling_canonical),
        ("Orchard", state.orchard_tree.as_str(), orchard_canonical),
        ("Ironwood", state.ironwood_tree.as_str(), ironwood_canonical),
    ] {
        let decoded =
            canonical_bytes(encoded).map_err(|reason| WalletRpcError::MalformedTreeState {
                height,
                reason: format!("{name} {reason}"),
            })?;
        if decoded.as_deref().is_some_and(|bytes| bytes != canonical) {
            return Err(WalletRpcError::MalformedTreeState {
                height,
                reason: format!("{name} tree encoding is non-canonical or has trailing bytes"),
            });
        }
    }
    Ok(())
}

async fn attest_network(
    client: &mut LightwalletdClient,
    network: WalletNetwork,
) -> Result<BlockRef, WalletRpcError> {
    let info = client.get_lightd_info(Empty {}).await?.into_inner();
    validate_lightd_info(network, &info)?;
    attest_genesis(client, network).await
}

fn validate_lightd_info(network: WalletNetwork, info: &LightdInfo) -> Result<(), WalletRpcError> {
    let expected_chain = network.parameters().bip70_network_name();
    if info.chain_name != expected_chain {
        return Err(WalletRpcError::WrongChainName {
            expected: expected_chain,
            actual: info.chain_name.clone(),
        });
    }

    // Zebra serializes its no-active-upgrade sentinel as eight zeroes while
    // the best chain contains only genesis. Wcash activates its own branch at
    // height one, so every later tip must report that network-specific value.
    let expected_branch = if info.block_height == 0 {
        format!("{:08x}", u32::from(ConsensusBranchId::RPC_MISSING_ID))
    } else {
        network.branch_id_hex()
    };
    if info.consensus_branch_id != expected_branch {
        return Err(WalletRpcError::WrongConsensusBranchId {
            height: info.block_height,
            expected: expected_branch,
            actual: info.consensus_branch_id.clone(),
        });
    }

    Ok(())
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
    validate_genesis_block_id(network, block.height, &block.hash)
}

fn validate_genesis_block_id(
    network: WalletNetwork,
    height: u64,
    hash: &[u8],
) -> Result<BlockRef, WalletRpcError> {
    if height != 0 {
        return Err(WalletRpcError::UnexpectedHeight {
            field: "genesis block",
            expected: 0,
            actual: height,
        });
    }
    let hash: [u8; 32] = hash
        .try_into()
        .map_err(|_| WalletRpcError::MalformedBlockId {
            field: "genesis hash",
            height,
        })?;
    let expected = network.genesis_hash();
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
    fn endpoint_policy_requires_tls_except_for_literal_loopback_ips() {
        assert!(configured_endpoint("https://wallet-testnet.wcashexplorer.com").is_ok());
        assert!(configured_endpoint("https://203.0.113.8:443").is_ok());
        assert!(configured_endpoint("http://127.0.0.1:8234").is_ok());
        assert!(configured_endpoint("http://127.255.255.254:8234").is_ok());
        assert!(configured_endpoint("http://[::1]:8234").is_ok());

        for endpoint in [
            "http://example.com:8234",
            "http://192.168.1.10:8234",
            "http://10.0.0.10:8234",
        ] {
            assert!(matches!(
                configured_endpoint(endpoint),
                Err(WalletRpcError::InsecureEndpoint)
            ));
        }
        for endpoint in ["http://localhost:8234", "https://localhost:8234"] {
            assert!(matches!(
                configured_endpoint(endpoint),
                Err(WalletRpcError::InvalidEndpoint(_))
            ));
        }
        assert!(matches!(
            configured_endpoint("ftp://wallet-testnet.wcashexplorer.com"),
            Err(WalletRpcError::InvalidEndpoint(_))
        ));
    }

    #[test]
    fn endpoint_errors_do_not_echo_rejected_user_information() {
        let secret = "never-print-this-password";
        let error = configured_endpoint(&format!("https://wallet:{secret}@example.com"))
            .expect_err("endpoint user information must be rejected");
        assert!(!error.to_string().contains(secret));
    }

    fn lightd_info(height: u64, chain_name: &str, branch_id: &str) -> LightdInfo {
        LightdInfo {
            chain_name: chain_name.to_owned(),
            consensus_branch_id: branch_id.to_owned(),
            block_height: height,
            ..Default::default()
        }
    }

    #[test]
    fn lightd_info_is_bound_to_the_wcash_chain_and_active_branch() {
        let network = WalletNetwork::Testnet;
        let chain_name = network.parameters().bip70_network_name();
        let valid = lightd_info(1, &chain_name, &network.branch_id_hex());
        assert!(validate_lightd_info(network, &valid).is_ok());

        let wrong_chain = lightd_info(1, "main", &network.branch_id_hex());
        assert!(matches!(
            validate_lightd_info(network, &wrong_chain),
            Err(WalletRpcError::WrongChainName { .. })
        ));

        let wrong_branch = lightd_info(1, &chain_name, "deadbeef");
        assert!(matches!(
            validate_lightd_info(network, &wrong_branch),
            Err(WalletRpcError::WrongConsensusBranchId { .. })
        ));
    }

    #[test]
    fn genesis_tip_uses_only_the_no_active_upgrade_branch_sentinel() {
        let network = WalletNetwork::Testnet;
        let chain_name = network.parameters().bip70_network_name();
        let genesis = lightd_info(0, &chain_name, "00000000");
        assert!(validate_lightd_info(network, &genesis).is_ok());

        let premature = lightd_info(0, &chain_name, &network.branch_id_hex());
        assert!(matches!(
            validate_lightd_info(network, &premature),
            Err(WalletRpcError::WrongConsensusBranchId { .. })
        ));

        let stale = lightd_info(1, &chain_name, "00000000");
        assert!(matches!(
            validate_lightd_info(network, &stale),
            Err(WalletRpcError::WrongConsensusBranchId { .. })
        ));
    }

    #[test]
    fn genesis_attestation_requires_the_exact_internal_hash_bytes() {
        let network = WalletNetwork::Testnet;
        let internal = network.genesis_hash();
        assert_eq!(
            validate_genesis_block_id(network, 0, &internal).unwrap(),
            BlockRef {
                height: 0,
                hash: internal,
            }
        );

        let mut display_order = internal;
        display_order.reverse();
        assert!(matches!(
            validate_genesis_block_id(network, 0, &display_order),
            Err(WalletRpcError::WrongGenesis { .. })
        ));
        assert!(matches!(
            validate_genesis_block_id(network, 1, &internal),
            Err(WalletRpcError::UnexpectedHeight { .. })
        ));
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

        let mut canonical_empty = valid_tree_state(block);
        canonical_empty.ironwood_tree = "000000".to_owned();
        assert!(validate_tree_state(WalletNetwork::Regtest, block, &canonical_empty).is_ok());

        canonical_empty.ironwood_tree.push_str("00");
        assert!(matches!(
            validate_tree_state(WalletNetwork::Regtest, block, &canonical_empty),
            Err(WalletRpcError::MalformedTreeState { .. })
        ));
    }

    #[test]
    fn subtree_completion_must_cross_exactly_the_requested_boundary() {
        let shard_size = 1u32 << IRONWOOD_SHARD_HEIGHT;
        assert!(validate_ironwood_subtree_completion(0, shard_size - 1, shard_size).is_ok());
        assert!(
            validate_ironwood_subtree_completion(1, shard_size * 2 - 1, shard_size * 2,).is_ok()
        );

        for (index, predecessor, completing) in [
            (0, shard_size, shard_size + 1),
            (0, shard_size - 2, shard_size - 1),
            (1, shard_size - 1, shard_size),
            (0, shard_size, shard_size - 1),
        ] {
            assert!(matches!(
                validate_ironwood_subtree_completion(index, predecessor, completing),
                Err(WalletRpcError::MalformedSubtreeRoot(_))
            ));
        }

        let excessive = u32::try_from(crate::cache::MAX_IRONWOOD_ACTIONS_PER_BLOCK)
            .expect("action limit fits")
            + 1;
        assert!(matches!(
            validate_ironwood_subtree_completion(0, shard_size - 1, shard_size - 1 + excessive),
            Err(WalletRpcError::MalformedSubtreeRoot(_))
        ));
    }

    #[test]
    fn compact_range_validator_rejects_truncation_extras_and_wrong_heights() {
        for (start, end) in [(0, 1), (7, 7), (8, 7), (1, 18)] {
            assert!(matches!(
                CompactRangeValidator::new(start, end),
                Err(WalletRpcError::InvalidCompactRange)
            ));
        }

        let mut valid = CompactRangeValidator::new(7, 9).unwrap();
        valid
            .observe(&CompactBlock {
                height: 7,
                ..Default::default()
            })
            .unwrap();
        assert!(matches!(
            valid.finish(),
            Err(WalletRpcError::UnexpectedCompactBlockCount {
                expected: 2,
                actual: 1,
            })
        ));

        let mut wrong = CompactRangeValidator::new(7, 9).unwrap();
        assert!(matches!(
            wrong.observe(&CompactBlock {
                height: 8,
                ..Default::default()
            }),
            Err(WalletRpcError::UnexpectedCompactBlockHeight {
                expected: 7,
                actual: 8,
            })
        ));

        let mut extra = CompactRangeValidator::new(7, 9).unwrap();
        for height in [7, 8] {
            extra
                .observe(&CompactBlock {
                    height,
                    ..Default::default()
                })
                .unwrap();
        }
        assert!(matches!(
            extra.observe(&CompactBlock {
                height: 9,
                ..Default::default()
            }),
            Err(WalletRpcError::UnexpectedCompactBlockCount {
                expected: 2,
                actual: 3,
            })
        ));
    }

    #[test]
    fn compact_range_validator_enforces_decoded_resource_boundaries() {
        let mut validator = CompactRangeValidator::new(1, 2).unwrap();
        validator
            .observe(&CompactBlock {
                height: 1,
                ..Default::default()
            })
            .unwrap();
        validator.finish().unwrap();

        let mut oversized = CompactRangeValidator::new(1, 2).unwrap();
        assert!(matches!(
            oversized.observe(&CompactBlock {
                height: 1,
                header: vec![0; MAX_BLOCK_BYTES],
                ..Default::default()
            }),
            Err(WalletRpcError::CompactBlockByteLimit { .. })
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
    fn transparent_history_ranges_are_exact_and_resource_bounded() {
        let (maximum_transactions, maximum_bytes) =
            transparent_history_limits(7, 7 + MAX_TRANSPARENT_HISTORY_BLOCKS_PER_REQUEST - 1)
                .unwrap();
        assert_eq!(
            maximum_bytes,
            usize::try_from(MAX_TRANSPARENT_HISTORY_BLOCKS_PER_REQUEST).unwrap() * MAX_BLOCK_BYTES
        );
        assert_eq!(
            maximum_transactions,
            maximum_bytes / MIN_P2PKH_OUTPUT_ENCODED_BYTES + 1
        );
        for (start, end) in [
            (0, 1),
            (2, 1),
            (1, MAX_TRANSPARENT_HISTORY_BLOCKS_PER_REQUEST + 1),
            (u32::MAX, 1),
        ] {
            assert!(matches!(
                transparent_history_limits(start, end),
                Err(WalletRpcError::InvalidTransparentHistoryRequest)
            ));
        }
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

        let creator = TransparentUnspentCreator {
            txid: transaction.txid(),
            outputs: vec![output(1, 12), output(0, 11)],
        };
        let mut raw = Vec::new();
        transaction.write(&mut raw).unwrap();
        let (stored, height) =
            validate_transparent_creator_transaction(WalletNetwork::Testnet, &creator, &raw, 7)
                .unwrap();
        assert_eq!(stored.txid(), transaction.txid());
        assert_eq!(height, 7);
        assert!(stored
            .transparent_bundle()
            .is_some_and(|bundle| bundle.is_coinbase()));
        assert!(matches!(
            validate_transparent_creator_transaction(WalletNetwork::Testnet, &creator, &raw, 8,),
            Err(WalletRpcError::UnexpectedTransparentTransactionHeight { .. })
        ));
    }
}
