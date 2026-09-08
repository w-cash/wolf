//! Ephemeral compact-block cache used by the reviewed wallet synchronizer.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use thiserror::Error;
use zcash_client_backend::{
    data_api::{
        chain::{error, BlockCache, BlockSource},
        scanning::ScanRange,
    },
    proto::compact_formats::CompactBlock,
};
use zcash_protocol::consensus::BlockHeight;

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
        let mut blocks = self
            .blocks
            .lock()
            .map_err(|_| MemoryBlockCacheError::Poisoned)?;

        // Validate the complete batch before mutating the cache. This prevents a
        // truncated or equivocated response from leaving a partially updated view.
        let mut validated: Vec<(u32, CompactBlock)> = Vec::with_capacity(compact_blocks.len());
        for block in compact_blocks {
            let height = u32::try_from(block.height)
                .map_err(|_| MemoryBlockCacheError::InvalidHeight(block.height))?;
            if block.hash.len() != 32 || block.prev_hash.len() != 32 {
                return Err(MemoryBlockCacheError::InvalidHashLength { height });
            }
            if let Some((previous_height, previous)) = validated.last() {
                let expected = previous_height.saturating_add(1);
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
            ..Default::default()
        }
    }

    fn range(start: u32, end: u32) -> ScanRange {
        ScanRange::from_parts(
            BlockHeight::from_u32(start)..BlockHeight::from_u32(end),
            ScanPriority::Historic,
        )
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
}
