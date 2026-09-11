//! Ephemeral compact-block cache used by the reviewed wallet synchronizer.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use prost::Message;
use thiserror::Error;
use zcash_client_backend::{
    data_api::{
        chain::{error, BlockCache, BlockSource},
        scanning::ScanRange,
    },
    proto::compact_formats::{CompactBlock, CompactOrchardAction},
};
use zcash_note_encryption::{Domain, ShieldedOutput, COMPACT_NOTE_SIZE};
use zcash_protocol::{
    consensus::{BlockHeight, TxIndex},
    constants::MAX_BLOCK_BYTES,
};
use zebra_chain::orchard::shielded_data::AUTHORIZED_ACTION_SIZE;

/// Largest compact-block batch accepted by the cache.
///
/// This matches the public synchronizer limit. It bounds the data retained by
/// the wallet after gRPC decoding; the transport must separately bound bytes
/// while a streamed response is being decoded.
pub(crate) const MAX_COMPACT_BLOCKS_PER_BATCH: u32 = 10_000;

/// Maximum number of Ironwood actions, and therefore emitted compact
/// transactions, that can fit in a valid Wcash block.
///
/// Each full-wire Ironwood action occupies the same 884 bytes as an authorized
/// Orchard action. The byte reserved below for its vector length makes this a
/// conservative upper bound; other transaction fields only reduce the limit.
const MAX_IRONWOOD_ACTIONS_PER_BLOCK: usize =
    (MAX_BLOCK_BYTES - 1) / AUTHORIZED_ACTION_SIZE as usize;

/// Errors returned by the in-memory compact-block cache.
#[derive(Clone, Debug, Error)]
pub enum MemoryBlockCacheError {
    /// Another thread panicked while holding the cache lock.
    #[error("compact-block cache lock is poisoned")]
    Poisoned,
    /// A requested scan range begins at a missing height.
    #[error("compact-block cache is missing height {0}")]
    MissingHeight(u32),
    /// A compact block reports a height that does not fit the consensus type.
    #[error("compact block height {0} is out of range")]
    InvalidHeight(u64),
    /// A download batch was not strictly ascending and contiguous.
    #[error("compact-block batch is not contiguous: expected height {expected}, got {actual}")]
    NonContiguousBatch {
        /// Height required immediately after the preceding block.
        expected: u32,
        /// Height actually supplied by the service.
        actual: u32,
    },
    /// A compact block's identifier fields have the wrong length.
    #[error("compact block {height} must contain 32-byte hash and previous-hash fields")]
    InvalidHashLength {
        /// Height of the malformed compact block.
        height: u32,
    },
    /// A compact block claims itself as its own parent.
    #[error("compact block {height} has the same block and previous-block identifier")]
    SelfLinkedBlock {
        /// Height of the cyclic compact block.
        height: u32,
    },
    /// Two compact-block heights claim the same block identifier.
    #[error("compact blocks {first_height} and {second_height} have the same block identifier")]
    DuplicateBlockHash {
        /// First height at which the identifier was observed.
        first_height: u32,
        /// Later height that repeated the identifier.
        second_height: u32,
    },
    /// A download returned no blocks or more blocks than one supported batch.
    #[error("compact-block batch must contain between 1 and {maximum} blocks; got {actual}")]
    InvalidBatchCount {
        /// Maximum accepted number of compact blocks.
        maximum: u32,
        /// Number of compact blocks returned by the service.
        actual: usize,
    },
    /// A canonical compact block exceeds the consensus full-block byte limit.
    #[error("compact block {height} is {actual} bytes, exceeding the {maximum}-byte safety limit")]
    OversizedBlock {
        /// Height of the oversized block.
        height: u32,
        /// Maximum canonical encoded size accepted by the wallet.
        maximum: usize,
        /// Canonical protobuf-encoded size of the block.
        actual: usize,
    },
    /// Wcash's compact-block service unexpectedly supplied a block header.
    #[error("compact block {height} unexpectedly contains a full header")]
    UnexpectedHeader {
        /// Height of the block containing a header.
        height: u32,
    },
    /// A compact block omitted the tree-size metadata required for safe scanning.
    #[error("compact block {height} is missing commitment-tree metadata")]
    MissingChainMetadata {
        /// Height of the block missing metadata.
        height: u32,
    },
    /// A compact block contains data for a shielded pool disabled by Wcash.
    #[error("compact block {height} contains disabled Sapling or legacy Orchard data")]
    LegacyPoolData {
        /// Height of the block containing disabled pool data.
        height: u32,
    },
    /// A compact block contains transparent data omitted by Wcash Zebra.
    #[error("compact block {height} unexpectedly contains compact transparent data")]
    UnexpectedTransparentData {
        /// Height of the block containing unexpected transparent data.
        height: u32,
    },
    /// A compact transaction has no Ironwood action, so Wcash Zebra would omit it.
    #[error(
        "compact transaction index {transaction_index} in block {height} has no Ironwood action"
    )]
    MissingIronwoodAction {
        /// Height containing the unexpected compact transaction.
        height: u32,
        /// Full-block transaction index reported by the compact transaction.
        transaction_index: u64,
    },
    /// A compact block has more transactions than valid full-wire actions can support.
    #[error(
        "compact block {height} has {actual} transactions, exceeding the Wcash limit of {maximum}"
    )]
    TooManyCompactTransactions {
        /// Height containing too many compact transactions.
        height: u32,
        /// Maximum compact transactions possible in a valid Wcash block.
        maximum: usize,
        /// Compact transactions supplied by the service.
        actual: usize,
    },
    /// A compact transaction identifier is not exactly 32 bytes.
    #[error("compact transaction {transaction} in block {height} has an invalid identifier")]
    InvalidTransactionId {
        /// Height containing the malformed transaction.
        height: u32,
        /// Position in the compact transaction vector.
        transaction: usize,
    },
    /// A compact transaction index is not representable by the consensus type.
    #[error("compact transaction index {index} in block {height} is out of range")]
    InvalidTransactionIndex {
        /// Height containing the malformed transaction.
        height: u32,
        /// Index reported by the service.
        index: u64,
    },
    /// Compact transaction indices are not unique and strictly ascending.
    #[error("compact transaction indices in block {height} are not strictly ascending")]
    NonAscendingTransactionIndex {
        /// Height containing the malformed transaction order.
        height: u32,
    },
    /// A compact block repeats a transaction identifier.
    #[error("compact block {height} repeats a transaction identifier")]
    DuplicateTransactionId {
        /// Height containing the duplicate identifier.
        height: u32,
    },
    /// An Ironwood compact action has a malformed field encoding.
    #[error(
        "Ironwood action {action} in transaction index {transaction_index} at height {height} is malformed"
    )]
    InvalidIronwoodAction {
        /// Height containing the malformed action.
        height: u32,
        /// Full-block transaction index reported by the compact transaction.
        transaction_index: u64,
        /// Position in the transaction's Ironwood action vector.
        action: usize,
    },
    /// A compact block has more Ironwood actions than a valid full block can contain.
    #[error("compact block {height} has more than the Wcash limit of {maximum} Ironwood actions")]
    TooManyIronwoodActions {
        /// Height containing too many Ironwood actions.
        height: u32,
        /// Maximum full-wire actions possible in a valid Wcash block.
        maximum: usize,
    },
    /// An Ironwood tree-size transition contradicts the compact actions.
    #[error(
        "compact block {height} has invalid Ironwood tree size: expected {expected}, got {actual}"
    )]
    InvalidIronwoodTreeSize {
        /// Height containing the invalid tree-size transition.
        height: u32,
        /// Tree size computed from the available prior state and actions.
        expected: u32,
        /// Tree size reported by the service.
        actual: u32,
    },
    /// An Ironwood action count or tree-size transition overflowed its encoding.
    #[error("compact block {height} overflows the Ironwood tree-size encoding")]
    IronwoodTreeSizeOverflow {
        /// Height containing the overflowing count or transition.
        height: u32,
    },
    /// An adjacent compact block does not link to its predecessor.
    #[error("compact block {height} does not link to height {previous_height}")]
    BrokenLink {
        /// Height whose previous-hash relationship is invalid.
        height: u32,
        /// Height that should be its predecessor.
        previous_height: u32,
    },
    /// A server tried to replace a cached height with different data.
    #[error("conflicting compact block at height {height}")]
    ConflictingBlock {
        /// Height at which different block data was already cached.
        height: u32,
    },
}

/// A bounded-lifetime cache for one wallet synchronization run.
///
/// The synchronizer downloads blocks into this cache, scans them into the
/// persistent wallet database, and deletes them. Keeping it ephemeral avoids a
/// second mutable on-disk database while preserving librustzcash's tested
/// reorg, subtree, and witness handling.
#[derive(Clone, Debug, Default)]
pub struct MemoryBlockCache {
    blocks: Arc<Mutex<BTreeMap<u32, CompactBlock>>>,
}

impl MemoryBlockCache {
    /// Returns the number of cached compact blocks.
    pub fn len(&self) -> Result<usize, MemoryBlockCacheError> {
        self.blocks
            .lock()
            .map(|blocks| blocks.len())
            .map_err(|_| MemoryBlockCacheError::Poisoned)
    }

    /// Returns true if the cache contains no blocks.
    pub fn is_empty(&self) -> Result<bool, MemoryBlockCacheError> {
        self.len().map(|len| len == 0)
    }

    fn range_blocks(&self, range: &ScanRange) -> Result<Vec<CompactBlock>, MemoryBlockCacheError> {
        let blocks = self
            .blocks
            .lock()
            .map_err(|_| MemoryBlockCacheError::Poisoned)?;
        let start: u32 = range.block_range().start.into();
        let end: u32 = range.block_range().end.into();
        let mut result = Vec::with_capacity(end.saturating_sub(start) as usize);

        for height in start..end {
            match blocks.get(&height) {
                Some(block) => result.push(block.clone()),
                None if result.is_empty() => {
                    return Err(MemoryBlockCacheError::MissingHeight(height));
                }
                None => break,
            }
        }
        Ok(result)
    }

    fn validate_block(
        block: &CompactBlock,
        previous_ironwood_tree_size: Option<u32>,
    ) -> Result<u32, MemoryBlockCacheError> {
        let height = u32::try_from(block.height)
            .map_err(|_| MemoryBlockCacheError::InvalidHeight(block.height))?;
        if block.hash.len() != 32 || block.prev_hash.len() != 32 {
            return Err(MemoryBlockCacheError::InvalidHashLength { height });
        }
        if block.hash == block.prev_hash {
            return Err(MemoryBlockCacheError::SelfLinkedBlock { height });
        }

        // Zebra's Wcash CompactTxStreamer deliberately emits only hash, parent,
        // time, compact transactions, and tree sizes. Rejecting headers avoids
        // librustzcash selecting an unchecked header-derived hash in preference
        // to the identifier fields validated above.
        if !block.header.is_empty() {
            return Err(MemoryBlockCacheError::UnexpectedHeader { height });
        }

        let metadata = block
            .chain_metadata
            .as_ref()
            .ok_or(MemoryBlockCacheError::MissingChainMetadata { height })?;
        if metadata.sapling_commitment_tree_size != 0 || metadata.orchard_commitment_tree_size != 0
        {
            return Err(MemoryBlockCacheError::LegacyPoolData { height });
        }

        if block.vtx.len() > MAX_IRONWOOD_ACTIONS_PER_BLOCK {
            return Err(MemoryBlockCacheError::TooManyCompactTransactions {
                height,
                maximum: MAX_IRONWOOD_ACTIONS_PER_BLOCK,
                actual: block.vtx.len(),
            });
        }

        let mut previous_transaction_index = None;
        let mut transaction_ids = HashSet::with_capacity(block.vtx.len());
        let mut ironwood_action_count = 0usize;
        for (transaction, compact_tx) in block.vtx.iter().enumerate() {
            let txid: [u8; 32] = compact_tx.txid.as_slice().try_into().map_err(|_| {
                MemoryBlockCacheError::InvalidTransactionId {
                    height,
                    transaction,
                }
            })?;
            if !transaction_ids.insert(txid) {
                return Err(MemoryBlockCacheError::DuplicateTransactionId { height });
            }
            TxIndex::try_from(compact_tx.index).map_err(|_| {
                MemoryBlockCacheError::InvalidTransactionIndex {
                    height,
                    index: compact_tx.index,
                }
            })?;
            if previous_transaction_index.is_some_and(|previous| compact_tx.index <= previous) {
                return Err(MemoryBlockCacheError::NonAscendingTransactionIndex { height });
            }
            previous_transaction_index = Some(compact_tx.index);

            if !compact_tx.spends.is_empty()
                || !compact_tx.outputs.is_empty()
                || !compact_tx.actions.is_empty()
            {
                return Err(MemoryBlockCacheError::LegacyPoolData { height });
            }
            // Wcash Zebra reserves protobuf fields 7 and 8, while the pinned
            // upstream client decodes those tags as transparent inputs and
            // outputs. Reject them so a non-Wcash service cannot make the
            // generic scanner ingest data that Wcash Zebra never emitted.
            if !compact_tx.vin.is_empty() || !compact_tx.vout.is_empty() {
                return Err(MemoryBlockCacheError::UnexpectedTransparentData { height });
            }
            // GetBlockRange includes a transaction only when it has compact
            // data. Valid Wcash blocks have no Sapling or legacy Orchard data,
            // so every emitted transaction must contain an Ironwood action.
            // A transparent-only coinbase is omitted, not represented empty.
            if compact_tx.ironwood_actions.is_empty() {
                return Err(MemoryBlockCacheError::MissingIronwoodAction {
                    height,
                    transaction_index: compact_tx.index,
                });
            }

            ironwood_action_count = ironwood_action_count
                .checked_add(compact_tx.ironwood_actions.len())
                .ok_or(MemoryBlockCacheError::TooManyIronwoodActions {
                    height,
                    maximum: MAX_IRONWOOD_ACTIONS_PER_BLOCK,
                })?;
            if ironwood_action_count > MAX_IRONWOOD_ACTIONS_PER_BLOCK {
                return Err(MemoryBlockCacheError::TooManyIronwoodActions {
                    height,
                    maximum: MAX_IRONWOOD_ACTIONS_PER_BLOCK,
                });
            }
        }

        // Only walk the complete protobuf and parse cryptographic fields after
        // every repeated collection is known to be empty or bounded by what a
        // valid full Wcash block can contain.
        let encoded_bytes = block.encoded_len();
        if encoded_bytes > MAX_BLOCK_BYTES {
            return Err(MemoryBlockCacheError::OversizedBlock {
                height,
                maximum: MAX_BLOCK_BYTES,
                actual: encoded_bytes,
            });
        }
        for compact_tx in &block.vtx {
            for (action, compact_action) in compact_tx.ironwood_actions.iter().enumerate() {
                validate_ironwood_action(compact_action).map_err(|_| {
                    MemoryBlockCacheError::InvalidIronwoodAction {
                        height,
                        transaction_index: compact_tx.index,
                        action,
                    }
                })?;
            }
        }

        let ironwood_action_count = u32::try_from(ironwood_action_count)
            .map_err(|_| MemoryBlockCacheError::IronwoodTreeSizeOverflow { height })?;
        let prior_tree_size = previous_ironwood_tree_size.or((height == 1).then_some(0));
        if let Some(prior_tree_size) = prior_tree_size {
            let expected = prior_tree_size
                .checked_add(ironwood_action_count)
                .ok_or(MemoryBlockCacheError::IronwoodTreeSizeOverflow { height })?;
            if metadata.ironwood_commitment_tree_size != expected {
                return Err(MemoryBlockCacheError::InvalidIronwoodTreeSize {
                    height,
                    expected,
                    actual: metadata.ironwood_commitment_tree_size,
                });
            }
        } else if metadata.ironwood_commitment_tree_size < ironwood_action_count {
            return Err(MemoryBlockCacheError::InvalidIronwoodTreeSize {
                height,
                expected: ironwood_action_count,
                actual: metadata.ironwood_commitment_tree_size,
            });
        }

        Ok(height)
    }
}

fn validate_ironwood_action(
    action: &CompactOrchardAction,
) -> Result<(), zcash_client_backend::proto::CompactFormatError> {
    use orchard::note_encryption::{CompactAction, IronwoodDomain};

    let action = CompactAction::try_from(action)?;
    let ephemeral_key =
        <CompactAction as ShieldedOutput<IronwoodDomain, COMPACT_NOTE_SIZE>>::ephemeral_key(
            &action,
        );
    if <IronwoodDomain as Domain>::epk(&ephemeral_key).is_some() {
        Ok(())
    } else {
        Err(zcash_client_backend::proto::CompactFormatError::InvalidValue)
    }
}

impl BlockSource for MemoryBlockCache {
    type Error = MemoryBlockCacheError;

    fn with_blocks<F, WalletError>(
        &self,
        from_height: Option<BlockHeight>,
        limit: Option<usize>,
        mut with_block: F,
    ) -> Result<(), error::Error<WalletError, Self::Error>>
    where
        F: FnMut(CompactBlock) -> Result<(), error::Error<WalletError, Self::Error>>,
    {
        let blocks = self
            .blocks
            .lock()
            .map_err(|_| error::Error::BlockSource(MemoryBlockCacheError::Poisoned))?;
        let start = from_height.map(u32::from).unwrap_or(0);
        let mut next = start;

        for (read, (&height, block)) in blocks.range(start..).enumerate() {
            if limit.is_some_and(|limit| read >= limit) {
                break;
            }
            if height != next {
                if read == 0 {
                    return Err(error::Error::BlockSource(
                        MemoryBlockCacheError::MissingHeight(next),
                    ));
                }
                break;
            }
            with_block(block.clone())?;
            next = next.saturating_add(1);
        }

        Ok(())
    }
}

#[async_trait]
impl BlockCache for MemoryBlockCache {
    fn get_tip_height(
        &self,
        range: Option<&ScanRange>,
    ) -> Result<Option<BlockHeight>, Self::Error> {
        let blocks = self
            .blocks
            .lock()
            .map_err(|_| MemoryBlockCacheError::Poisoned)?;
        let height = match range {
            Some(range) => {
                let start: u32 = range.block_range().start.into();
                let end: u32 = range.block_range().end.into();
                blocks
                    .range(start..end)
                    .next_back()
                    .map(|(height, _)| *height)
            }
            None => blocks.last_key_value().map(|(height, _)| *height),
        };
        Ok(height.map(BlockHeight::from_u32))
    }

    async fn read(&self, range: &ScanRange) -> Result<Vec<CompactBlock>, Self::Error> {
        self.range_blocks(range)
    }

    async fn insert(&self, compact_blocks: Vec<CompactBlock>) -> Result<(), Self::Error> {
        let maximum_blocks = usize::try_from(MAX_COMPACT_BLOCKS_PER_BATCH)
            .expect("the compact-block batch limit fits in usize");
        // The upstream downloader only calls this method for a non-empty
        // ScanRange. An empty response is therefore a truncated response, not
        // a successful no-op.
        if compact_blocks.is_empty() || compact_blocks.len() > maximum_blocks {
            return Err(MemoryBlockCacheError::InvalidBatchCount {
                maximum: MAX_COMPACT_BLOCKS_PER_BATCH,
                actual: compact_blocks.len(),
            });
        }
        let mut blocks = self
            .blocks
            .lock()
            .map_err(|_| MemoryBlockCacheError::Poisoned)?;
        let mut block_hashes = blocks
            .iter()
            .map(|(&height, block)| {
                let hash: [u8; 32] = block
                    .hash
                    .as_slice()
                    .try_into()
                    .expect("cached blocks passed hash-length validation");
                (hash, height)
            })
            .collect::<HashMap<_, _>>();

        // Validate the complete batch before mutating the cache. This prevents a
        // truncated or equivocated response from leaving a partially updated view.
        let mut validated: Vec<(u32, CompactBlock)> = Vec::with_capacity(compact_blocks.len());
        for block in compact_blocks {
            let candidate_height = u32::try_from(block.height)
                .map_err(|_| MemoryBlockCacheError::InvalidHeight(block.height))?;
            let previous_tree_size = candidate_height
                .checked_sub(1)
                .and_then(|previous_height| {
                    validated
                        .last()
                        .filter(|(height, _)| *height == previous_height)
                        .map(|(_, block)| block)
                        .or_else(|| blocks.get(&previous_height))
                })
                .and_then(|previous| previous.chain_metadata.as_ref())
                .map(|metadata| metadata.ironwood_commitment_tree_size);
            let height = Self::validate_block(&block, previous_tree_size)?;
            let block_hash: [u8; 32] = block
                .hash
                .as_slice()
                .try_into()
                .expect("the candidate passed hash-length validation");
            if let Some(&first_height) = block_hashes.get(&block_hash) {
                if first_height != height {
                    return Err(MemoryBlockCacheError::DuplicateBlockHash {
                        first_height,
                        second_height: height,
                    });
                }
            } else {
                block_hashes.insert(block_hash, height);
            }
            if let Some((previous_height, previous)) = validated.last() {
                let expected = previous_height.checked_add(1).ok_or(
                    MemoryBlockCacheError::NonContiguousBatch {
                        expected: u32::MAX,
                        actual: height,
                    },
                )?;
                if height != expected {
                    return Err(MemoryBlockCacheError::NonContiguousBatch {
                        expected,
                        actual: height,
                    });
                }
                if block.prev_hash != previous.hash {
                    return Err(MemoryBlockCacheError::BrokenLink {
                        height,
                        previous_height: *previous_height,
                    });
                }
            }
            if let Some(existing) = blocks.get(&height) {
                if existing != &block {
                    return Err(MemoryBlockCacheError::ConflictingBlock { height });
                }
            }
            if let Some(previous_height) = height.checked_sub(1) {
                if let Some(previous) = blocks.get(&previous_height) {
                    if block.prev_hash != previous.hash {
                        return Err(MemoryBlockCacheError::BrokenLink {
                            height,
                            previous_height,
                        });
                    }
                }
            }
            if let Some(next_height) = height.checked_add(1) {
                if let Some(next) = blocks.get(&next_height) {
                    if next.prev_hash != block.hash {
                        return Err(MemoryBlockCacheError::BrokenLink {
                            height: next_height,
                            previous_height: height,
                        });
                    }
                    let ironwood_tree_size = block
                        .chain_metadata
                        .as_ref()
                        .ok_or(MemoryBlockCacheError::MissingChainMetadata { height })?
                        .ironwood_commitment_tree_size;
                    Self::validate_block(next, Some(ironwood_tree_size))?;
                }
            }
            validated.push((height, block));
        }

        for (height, block) in validated {
            blocks.entry(height).or_insert(block);
        }
        Ok(())
    }

    async fn delete(&self, range: ScanRange) -> Result<(), Self::Error> {
        let start: u32 = range.block_range().start.into();
        let end: u32 = range.block_range().end.into();
        let mut blocks = self
            .blocks
            .lock()
            .map_err(|_| MemoryBlockCacheError::Poisoned)?;
        blocks.retain(|height, _| !(*height >= start && *height < end));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use zcash_client_backend::data_api::scanning::ScanPriority;
    use zcash_client_backend::proto::compact_formats::{
        ChainMetadata, CompactOrchardAction, CompactSaplingOutput, CompactSaplingSpend, CompactTx,
        CompactTxIn, TxOut,
    };

    use super::*;

    fn block(height: u64) -> CompactBlock {
        let hash_byte = u8::try_from(height).unwrap_or(255);
        let previous_hash_byte = u8::try_from(height.saturating_sub(1)).unwrap_or(255);
        CompactBlock {
            height,
            hash: vec![hash_byte; 32],
            prev_hash: if height == 0 {
                vec![0; 32]
            } else {
                vec![previous_hash_byte; 32]
            },
            chain_metadata: Some(ChainMetadata::default()),
            ..Default::default()
        }
    }

    fn range(start: u32, end: u32) -> ScanRange {
        ScanRange::from_parts(
            BlockHeight::from_u32(start)..BlockHeight::from_u32(end),
            ScanPriority::Historic,
        )
    }

    fn compact_tx(index: u64, txid_byte: u8) -> CompactTx {
        CompactTx {
            index,
            txid: vec![txid_byte; 32],
            ..Default::default()
        }
    }

    fn valid_ironwood_action() -> CompactOrchardAction {
        CompactOrchardAction {
            nullifier: vec![0; 32],
            cmx: vec![0; 32],
            // A canonical non-identity Pallas point from the Orchard note
            // encryption test vectors. Ironwood uses the same key encoding.
            ephemeral_key: vec![
                0xad, 0xdb, 0x47, 0xb6, 0xac, 0x5d, 0xfc, 0x16, 0x55, 0x89, 0x23, 0xd3, 0xa8, 0xf3,
                0x76, 0x09, 0x5c, 0x69, 0x5c, 0x04, 0x7c, 0x4e, 0x32, 0x66, 0xae, 0x67, 0x69, 0x87,
                0xf7, 0xe3, 0x13, 0x81,
            ],
            ciphertext: vec![0; 52],
        }
    }

    fn ironwood_tx(index: u64, txid_byte: u8) -> CompactTx {
        let mut tx = compact_tx(index, txid_byte);
        tx.ironwood_actions.push(valid_ironwood_action());
        tx
    }

    fn ironwood_block(height: u64, action_count: usize, tree_size: u32) -> CompactBlock {
        let mut block = block(height);
        let mut tx = compact_tx(1, u8::try_from(height).unwrap_or(255));
        tx.ironwood_actions = vec![valid_ironwood_action(); action_count];
        block.vtx.push(tx);
        block
            .chain_metadata
            .as_mut()
            .unwrap()
            .ironwood_commitment_tree_size = tree_size;
        block
    }

    async fn rejection_after_valid_prefix(invalid: CompactBlock) -> MemoryBlockCacheError {
        assert_eq!(invalid.height, 21);
        let cache = MemoryBlockCache::default();
        let error = cache
            .insert(vec![block(20), invalid])
            .await
            .expect_err("the malformed second block must be rejected");
        assert!(cache.is_empty().unwrap(), "insertion must be atomic");
        error
    }

    #[tokio::test]
    async fn cache_returns_only_contiguous_ranges_and_deletes_scanned_blocks() {
        let cache = MemoryBlockCache::default();
        cache.insert(vec![block(10), block(11)]).await.unwrap();
        cache.insert(vec![block(13)]).await.unwrap();

        let first = cache.read(&range(10, 14)).await.unwrap();
        assert_eq!(
            first.iter().map(|block| block.height).collect::<Vec<_>>(),
            [10, 11]
        );
        assert!(matches!(
            cache.read(&range(12, 14)).await,
            Err(MemoryBlockCacheError::MissingHeight(12))
        ));

        cache.delete(range(10, 12)).await.unwrap();
        assert_eq!(cache.len().unwrap(), 1);
        assert_eq!(
            cache.get_tip_height(None).unwrap(),
            Some(BlockHeight::from_u32(13))
        );
    }

    #[tokio::test]
    async fn duplicate_height_is_idempotent_but_conflict_is_rejected() {
        let cache = MemoryBlockCache::default();
        let original = block(5);
        cache.insert(vec![original.clone()]).await.unwrap();
        cache.insert(vec![original]).await.unwrap();

        let mut conflict = block(5);
        conflict.time = 2;
        assert!(matches!(
            cache.insert(vec![conflict]).await,
            Err(MemoryBlockCacheError::ConflictingBlock { height: 5 })
        ));

        assert!(matches!(
            cache.insert(vec![block(u64::from(u32::MAX) + 1)]).await,
            Err(MemoryBlockCacheError::InvalidHeight(_))
        ));
    }

    #[tokio::test]
    async fn insertion_rejects_gap_and_broken_link_atomically() {
        let cache = MemoryBlockCache::default();
        assert!(matches!(
            cache.insert(vec![block(2), block(4)]).await,
            Err(MemoryBlockCacheError::NonContiguousBatch { .. })
        ));
        assert!(cache.is_empty().unwrap());

        let mut broken = block(3);
        broken.prev_hash = vec![99; 32];
        assert!(matches!(
            cache.insert(vec![block(2), broken]).await,
            Err(MemoryBlockCacheError::BrokenLink { .. })
        ));
        assert!(cache.is_empty().unwrap());
    }

    #[tokio::test]
    async fn insertion_rejects_cyclic_or_repeated_block_identifiers_atomically() {
        let cache = MemoryBlockCache::default();

        let mut self_linked = block(2);
        self_linked.prev_hash = self_linked.hash.clone();
        assert!(matches!(
            cache.insert(vec![self_linked]).await,
            Err(MemoryBlockCacheError::SelfLinkedBlock { height: 2 })
        ));
        assert!(cache.is_empty().unwrap());

        let first = block(10);
        cache.insert(vec![first.clone()]).await.unwrap();
        let mut repeated_across_batches = block(12);
        repeated_across_batches.hash = first.hash;
        assert!(matches!(
            cache.insert(vec![repeated_across_batches]).await,
            Err(MemoryBlockCacheError::DuplicateBlockHash {
                first_height: 10,
                second_height: 12,
            })
        ));
        assert_eq!(cache.len().unwrap(), 1);

        let fresh_cache = MemoryBlockCache::default();
        let first = block(20);
        let middle = block(21);
        let mut repeated_in_batch = block(22);
        repeated_in_batch.hash = first.hash.clone();
        assert!(matches!(
            fresh_cache
                .insert(vec![first, middle, repeated_in_batch])
                .await,
            Err(MemoryBlockCacheError::DuplicateBlockHash {
                first_height: 20,
                second_height: 22,
            })
        ));
        assert!(fresh_cache.is_empty().unwrap());
    }

    #[tokio::test]
    async fn insertion_rejects_a_block_that_conflicts_with_cached_successor() {
        let cache = MemoryBlockCache::default();
        cache.insert(vec![block(13)]).await.unwrap();

        let mut conflicting_predecessor = block(12);
        conflicting_predecessor.hash = vec![99; 32];
        assert!(matches!(
            cache.insert(vec![conflicting_predecessor]).await,
            Err(MemoryBlockCacheError::BrokenLink {
                height: 13,
                previous_height: 12,
            })
        ));
        assert_eq!(cache.len().unwrap(), 1);
    }

    #[tokio::test]
    async fn insertion_rejects_missing_or_unexpected_wcash_fields_atomically() {
        let mut invalid_hash = block(21);
        invalid_hash.hash.pop();
        assert!(matches!(
            rejection_after_valid_prefix(invalid_hash).await,
            MemoryBlockCacheError::InvalidHashLength { height: 21 }
        ));

        let mut header = block(21);
        header.header.push(1);
        assert!(matches!(
            rejection_after_valid_prefix(header).await,
            MemoryBlockCacheError::UnexpectedHeader { height: 21 }
        ));

        let mut missing_metadata = block(21);
        missing_metadata.chain_metadata = None;
        assert!(matches!(
            rejection_after_valid_prefix(missing_metadata).await,
            MemoryBlockCacheError::MissingChainMetadata { height: 21 }
        ));

        for metadata in [
            ChainMetadata {
                sapling_commitment_tree_size: 1,
                ..Default::default()
            },
            ChainMetadata {
                orchard_commitment_tree_size: 1,
                ..Default::default()
            },
        ] {
            let mut legacy_tree = block(21);
            legacy_tree.chain_metadata = Some(metadata);
            assert!(matches!(
                rejection_after_valid_prefix(legacy_tree).await,
                MemoryBlockCacheError::LegacyPoolData { height: 21 }
            ));
        }
    }

    #[tokio::test]
    async fn insertion_rejects_legacy_and_unexpected_transparent_content_atomically() {
        let mut sapling_spend = compact_tx(1, 1);
        sapling_spend.spends.push(CompactSaplingSpend::default());
        let mut sapling_output = compact_tx(1, 1);
        sapling_output.outputs.push(CompactSaplingOutput::default());
        let mut orchard_action = compact_tx(1, 1);
        orchard_action.actions.push(valid_ironwood_action());

        for legacy_tx in [sapling_spend, sapling_output, orchard_action] {
            let mut legacy_block = block(21);
            legacy_block.vtx.push(legacy_tx);
            assert!(matches!(
                rejection_after_valid_prefix(legacy_block).await,
                MemoryBlockCacheError::LegacyPoolData { height: 21 }
            ));
        }

        let mut transparent_input = block(21);
        let mut tx = compact_tx(1, 1);
        tx.vin.push(CompactTxIn::default());
        transparent_input.vtx.push(tx);
        assert!(matches!(
            rejection_after_valid_prefix(transparent_input).await,
            MemoryBlockCacheError::UnexpectedTransparentData { height: 21 }
        ));

        let mut transparent_output = block(21);
        let mut tx = compact_tx(1, 1);
        tx.vout.push(TxOut::default());
        transparent_output.vtx.push(tx);
        assert!(matches!(
            rejection_after_valid_prefix(transparent_output).await,
            MemoryBlockCacheError::UnexpectedTransparentData { height: 21 }
        ));
    }

    #[tokio::test]
    async fn insertion_validates_transaction_identifiers_and_indices_atomically() {
        let mut short_txid = block(21);
        let mut tx = compact_tx(1, 1);
        tx.txid.pop();
        short_txid.vtx.push(tx);
        assert!(matches!(
            rejection_after_valid_prefix(short_txid).await,
            MemoryBlockCacheError::InvalidTransactionId {
                height: 21,
                transaction: 0,
            }
        ));

        let mut oversized_index = block(21);
        oversized_index
            .vtx
            .push(compact_tx(u64::from(u16::MAX) + 1, 1));
        assert!(matches!(
            rejection_after_valid_prefix(oversized_index).await,
            MemoryBlockCacheError::InvalidTransactionIndex { height: 21, .. }
        ));

        let mut reversed_indices = block(21);
        reversed_indices.vtx = vec![ironwood_tx(9, 1), ironwood_tx(3, 2)];
        assert!(matches!(
            rejection_after_valid_prefix(reversed_indices).await,
            MemoryBlockCacheError::NonAscendingTransactionIndex { height: 21 }
        ));

        let mut duplicate_txid = block(21);
        duplicate_txid.vtx = vec![ironwood_tx(1, 1), ironwood_tx(9, 1)];
        assert!(matches!(
            rejection_after_valid_prefix(duplicate_txid).await,
            MemoryBlockCacheError::DuplicateTransactionId { height: 21 }
        ));

        let mut sparse_indices = block(21);
        sparse_indices.vtx = vec![ironwood_tx(1, 1), ironwood_tx(9, 2)];
        sparse_indices
            .chain_metadata
            .as_mut()
            .unwrap()
            .ironwood_commitment_tree_size = 2;
        MemoryBlockCache::default()
            .insert(vec![sparse_indices])
            .await
            .expect("filtered Wcash transactions may have sparse full-block indices");
    }

    #[tokio::test]
    async fn insertion_validates_ironwood_actions_and_tree_transitions_atomically() {
        assert_eq!(MAX_IRONWOOD_ACTIONS_PER_BLOCK, 2_262);

        let cache = MemoryBlockCache::default();
        cache
            .insert(vec![ironwood_block(1, 1, 1), ironwood_block(2, 2, 3)])
            .await
            .expect("valid Ironwood actions and exact tree deltas must be accepted");

        let mut missing_action = block(21);
        missing_action.vtx.push(compact_tx(1, 1));
        assert!(matches!(
            rejection_after_valid_prefix(missing_action).await,
            MemoryBlockCacheError::MissingIronwoodAction {
                height: 21,
                transaction_index: 1,
            }
        ));

        let mut too_many_transactions = block(21);
        too_many_transactions.vtx = vec![compact_tx(1, 1); MAX_IRONWOOD_ACTIONS_PER_BLOCK + 1];
        assert!(matches!(
            rejection_after_valid_prefix(too_many_transactions).await,
            MemoryBlockCacheError::TooManyCompactTransactions {
                height: 21,
                maximum: 2_262,
                actual: 2_263,
            }
        ));

        let too_many_actions = ironwood_block(
            21,
            MAX_IRONWOOD_ACTIONS_PER_BLOCK + 1,
            u32::try_from(MAX_IRONWOOD_ACTIONS_PER_BLOCK + 1).unwrap(),
        );
        assert!(matches!(
            rejection_after_valid_prefix(too_many_actions).await,
            MemoryBlockCacheError::TooManyIronwoodActions {
                height: 21,
                maximum: 2_262,
            }
        ));

        let mut short_action = ironwood_block(21, 1, 1);
        short_action.vtx[0].ironwood_actions[0].ciphertext.pop();
        assert!(matches!(
            rejection_after_valid_prefix(short_action).await,
            MemoryBlockCacheError::InvalidIronwoodAction {
                height: 21,
                transaction_index: 1,
                action: 0,
            }
        ));

        let mut invalid_encoding = ironwood_block(21, 1, 1);
        invalid_encoding.vtx[0].ironwood_actions[0].cmx = vec![u8::MAX; 32];
        assert!(matches!(
            rejection_after_valid_prefix(invalid_encoding).await,
            MemoryBlockCacheError::InvalidIronwoodAction { height: 21, .. }
        ));

        let mut identity_ephemeral_key = ironwood_block(21, 1, 1);
        identity_ephemeral_key.vtx[0].ironwood_actions[0].ephemeral_key = vec![0; 32];
        assert!(matches!(
            rejection_after_valid_prefix(identity_ephemeral_key).await,
            MemoryBlockCacheError::InvalidIronwoodAction { height: 21, .. }
        ));

        assert!(matches!(
            rejection_after_valid_prefix(ironwood_block(21, 1, 2)).await,
            MemoryBlockCacheError::InvalidIronwoodTreeSize {
                height: 21,
                expected: 1,
                actual: 2,
            }
        ));
    }

    #[tokio::test]
    async fn insertion_validates_a_cached_successor_tree_transition_atomically() {
        let cache = MemoryBlockCache::default();
        cache.insert(vec![ironwood_block(12, 1, 4)]).await.unwrap();

        let mut invalid_predecessor = block(11);
        invalid_predecessor
            .chain_metadata
            .as_mut()
            .unwrap()
            .ironwood_commitment_tree_size = 2;
        assert!(matches!(
            cache.insert(vec![invalid_predecessor]).await,
            Err(MemoryBlockCacheError::InvalidIronwoodTreeSize {
                height: 12,
                expected: 3,
                actual: 4,
            })
        ));
        assert_eq!(cache.len().unwrap(), 1);

        let mut valid_predecessor = block(11);
        valid_predecessor
            .chain_metadata
            .as_mut()
            .unwrap()
            .ironwood_commitment_tree_size = 3;
        cache.insert(vec![valid_predecessor]).await.unwrap();
        assert_eq!(cache.len().unwrap(), 2);
    }

    #[tokio::test]
    async fn insertion_enforces_block_and_batch_resource_bounds_atomically() {
        let cache = MemoryBlockCache::default();
        cache.insert(vec![block(30)]).await.unwrap();
        assert!(matches!(
            cache.insert(Vec::new()).await,
            Err(MemoryBlockCacheError::InvalidBatchCount { actual: 0, .. })
        ));
        assert_eq!(cache.len().unwrap(), 1);

        let too_many = vec![block(31); MAX_COMPACT_BLOCKS_PER_BATCH as usize + 1];
        assert!(matches!(
            cache.insert(too_many).await,
            Err(MemoryBlockCacheError::InvalidBatchCount { .. })
        ));
        assert_eq!(cache.len().unwrap(), 1);

        let mut oversized = ironwood_block(21, 1, 1);
        oversized.vtx[0].ironwood_actions[0].ciphertext = vec![0; MAX_BLOCK_BYTES];
        assert!(matches!(
            rejection_after_valid_prefix(oversized).await,
            MemoryBlockCacheError::OversizedBlock { height: 21, .. }
        ));
    }
}
