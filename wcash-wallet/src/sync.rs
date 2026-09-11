//! Wcash-owned compact synchronization boundary.

use std::{
    cmp,
    time::{Duration, Instant},
};

use rusqlite::params;
use thiserror::Error;
use zcash_client_backend::data_api::{
    chain::{error::Error as ChainError, scan_cached_blocks, BlockCache, CommitmentTreeRoot},
    scanning::{ScanPriority, ScanRange},
    WalletCommitmentTrees, WalletRead, WalletWrite, IRONWOOD_SHARD_HEIGHT,
};
use zcash_client_sqlite::error::SqliteClientError;
use zcash_protocol::consensus::BlockHeight;

use crate::{
    cache::MAX_ATTESTED_COMPACT_BLOCKS_PER_RANGE,
    identity::IRONWOOD_SYNC_STATE_TABLE,
    wallet::{WalletDatabase, WalletSyncCancellation},
    AttestedWcashClient, BlockRef, MemoryBlockCache, MemoryBlockCacheError, WalletRpcError,
};

const MAX_SESSION_RESTARTS: usize = 8;
const MAX_SYNC_SESSION_DURATION: Duration = Duration::from_secs(30 * 60);
const MAX_SUBTREE_REFRESH_DURATION: Duration = Duration::from_secs(2 * 60);
// One page maps to one wave of completing-block hash attestations in the RPC
// client. Keeping the durable unit small gives a healthy node ample time to
// commit at least one resume point inside the overall refresh deadline.
const MAX_PERSISTED_SUBTREE_ROOTS_PER_PAGE: u32 = 16;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct IronwoodPrefixAttestation {
    prefix_count: u32,
    tip: BlockRef,
}

#[derive(Debug, Eq, PartialEq)]
enum StoredIronwoodPrefixAttestation {
    Missing,
    Malformed,
    Valid(IronwoodPrefixAttestation),
}

#[derive(Debug, Error)]
pub(crate) enum WcashSyncError {
    #[error("wallet synchronization was cancelled")]
    Cancelled,
    #[error(transparent)]
    Rpc(#[from] WalletRpcError),
    #[error(transparent)]
    Cache(#[from] MemoryBlockCacheError),
    #[error("wallet database operation failed: {0}")]
    Database(String),
    #[error("compact-block scan failed: {0}")]
    Scan(String),
    #[error("the lightwalletd chain tip changed during synchronization")]
    StaleTip,
    #[error("compact-block scanner did not consume the exact requested range")]
    UnexpectedScanRange,
    #[error("wallet synchronization exceeded the session restart limit")]
    SessionRestartLimit,
    #[error("wallet synchronization exceeded the bounded session duration")]
    SessionDeadline,
    #[error("Ironwood subtree refresh exceeded its bounded duration")]
    SubtreeDeadline,
    #[error("the attested Ironwood tree size cannot fit in the reported chain height")]
    ImpossibleIronwoodTreeSize,
    #[error("wallet contains an Ironwood subtree root beyond the attested chain state")]
    StaleSubtreeRoot,
    #[error("stale unscanned Ironwood subtree roots were discarded")]
    SubtreeRestart,
    #[error("Ironwood subtree-root heights are not strictly ascending")]
    SubtreeOrder,
}

enum ScanOutcome {
    Complete,
    Restart,
}

pub(crate) async fn run(
    client: &mut AttestedWcashClient,
    params: &zebra_chain::parameters::Network,
    cache: &MemoryBlockCache,
    wallet: &mut WalletDatabase,
    batch_size: u32,
    cancellation: &WalletSyncCancellation,
) -> Result<(), WcashSyncError> {
    if batch_size == 0 || batch_size > MAX_ATTESTED_COMPACT_BLOCKS_PER_RANGE {
        return Err(WcashSyncError::Scan(format!(
            "batch size must be in 1..={MAX_ATTESTED_COMPACT_BLOCKS_PER_RANGE}"
        )));
    }

    let started_at = Instant::now();
    let mut session_restarts = 0usize;
    'session: loop {
        ensure_not_cancelled(cancellation)?;
        ensure_session_deadline(started_at)?;
        let tip = client.latest_block().await?;
        ensure_not_cancelled(cancellation)?;
        match reconcile_local_chain(client, cache, wallet, tip, cancellation).await {
            Ok(false) => {}
            Ok(true) | Err(WcashSyncError::StaleTip) => {
                bump_restart(&mut session_restarts)?;
                continue 'session;
            }
            Err(error) => return Err(error),
        }
        wallet
            .update_chain_tip(BlockHeight::from_u32(tip.height))
            .map_err(database_error)?;
        crate::wallet::ensure_no_legacy_pool_balances(wallet)
            .map_err(|error| WcashSyncError::Database(error.to_string()))?;

        // Validate any previously scanned unstable tail before trusting or
        // replacing subtree roots for this session.
        loop {
            ensure_not_cancelled(cancellation)?;
            ensure_session_deadline(started_at)?;
            let ranges = wallet.suggest_scan_ranges().map_err(database_error)?;
            let Some(range) = ranges
                .iter()
                .find(|range| range.priority() == ScanPriority::Verify)
            else {
                break;
            };
            let range = bounded_range(range, batch_size, tip.height)?;
            let outcome =
                match scan_range(client, params, cache, wallet, tip, &range, cancellation).await {
                    Ok(outcome) => outcome,
                    Err(WcashSyncError::StaleTip) => {
                        bump_restart(&mut session_restarts)?;
                        continue 'session;
                    }
                    Err(error) => return Err(error),
                };
            match outcome {
                ScanOutcome::Complete => session_restarts = 0,
                ScanOutcome::Restart => {
                    bump_restart(&mut session_restarts)?;
                    continue 'session;
                }
            }
        }

        ensure_not_cancelled(cancellation)?;
        match ensure_tip(client, tip).await {
            Ok(()) => {}
            Err(WcashSyncError::StaleTip) => {
                bump_restart(&mut session_restarts)?;
                continue 'session;
            }
            Err(error) => return Err(error),
        }
        let tip_state = client.chain_state(tip.height).await?;
        if tip_state.block != tip {
            bump_restart(&mut session_restarts)?;
            continue 'session;
        }
        let subtree_refresh = tokio::time::timeout(
            MAX_SUBTREE_REFRESH_DURATION,
            refresh_ironwood_subtrees(
                client,
                wallet,
                tip,
                tip_state.ironwood_tree_size,
                cancellation,
            ),
        )
        .await
        .map_err(|_| WcashSyncError::SubtreeDeadline)?;
        match subtree_refresh {
            Err(WcashSyncError::StaleTip) => {
                bump_restart(&mut session_restarts)?;
                continue 'session;
            }
            Err(WcashSyncError::SubtreeRestart) => {
                bump_restart(&mut session_restarts)?;
                continue 'session;
            }
            result => result?,
        }
        match ensure_tip(client, tip).await {
            Ok(()) => {}
            Err(WcashSyncError::StaleTip) => {
                bump_restart(&mut session_restarts)?;
                continue 'session;
            }
            Err(error) => return Err(error),
        }

        loop {
            ensure_not_cancelled(cancellation)?;
            ensure_session_deadline(started_at)?;
            let ranges = wallet.suggest_scan_ranges().map_err(database_error)?;
            let Some(range) = ranges
                .iter()
                .find(|range| range.priority() == ScanPriority::Verify)
                .or_else(|| ranges.first())
            else {
                break;
            };
            let range = bounded_range(range, batch_size, tip.height)?;
            let outcome =
                match scan_range(client, params, cache, wallet, tip, &range, cancellation).await {
                    Ok(outcome) => outcome,
                    Err(WcashSyncError::StaleTip) => {
                        bump_restart(&mut session_restarts)?;
                        continue 'session;
                    }
                    Err(error) => return Err(error),
                };
            match outcome {
                ScanOutcome::Complete => session_restarts = 0,
                ScanOutcome::Restart => {
                    bump_restart(&mut session_restarts)?;
                    continue 'session;
                }
            }
        }

        ensure_not_cancelled(cancellation)?;
        if client.latest_block().await? == tip {
            ensure_not_cancelled(cancellation)?;
            crate::wallet::ensure_no_legacy_pool_balances(wallet)
                .map_err(|error| WcashSyncError::Database(error.to_string()))?;
            return Ok(());
        }
        bump_restart(&mut session_restarts)?;
    }
}

/// Rewinds a stored tip that is no longer on the attested best chain.
///
/// A mismatch resets the wallet to the immutable, attested genesis state.
/// Reorganizations are rare, and a complete reset avoids retaining a stale
/// shielded-tree checkpoint or subtree root from an arbitrarily deep fork.
async fn reconcile_local_chain(
    client: &mut AttestedWcashClient,
    cache: &MemoryBlockCache,
    wallet: &mut WalletDatabase,
    session_tip: BlockRef,
    cancellation: &WalletSyncCancellation,
) -> Result<bool, WcashSyncError> {
    ensure_not_cancelled(cancellation)?;
    let Some((local_height, local_hash)) = wallet.get_max_height_hash().map_err(database_error)?
    else {
        return Ok(false);
    };
    let local_height: u32 = local_height.into();

    let canonical = if local_height <= session_tip.height {
        let canonical = client.compact_block_ref(local_height).await?;
        ensure_not_cancelled(cancellation)?;
        ensure_tip(client, session_tip).await?;
        Some(canonical)
    } else {
        None
    };
    if stored_chain_matches(local_height, local_hash.0, session_tip, canonical) {
        return Ok(false);
    }
    if local_height == 0 {
        return Err(WcashSyncError::Scan(
            "wallet genesis does not match the attested Wcash network".to_owned(),
        ));
    }

    reset_to_genesis(client, cache, wallet, session_tip, cancellation).await?;
    Ok(true)
}

async fn reset_to_genesis(
    client: &mut AttestedWcashClient,
    cache: &MemoryBlockCache,
    wallet: &mut WalletDatabase,
    session_tip: BlockRef,
    cancellation: &WalletSyncCancellation,
) -> Result<(), WcashSyncError> {
    ensure_not_cancelled(cancellation)?;
    let rewind_state = client.chain_state(0).await?;
    ensure_not_cancelled(cancellation)?;
    ensure_tip(client, session_tip).await?;
    discard_unscanned_ironwood_roots(wallet)?;
    wallet
        .truncate_to_chain_state(rewind_state.state)
        .map_err(database_error)?;
    cache.truncate(BlockHeight::from_u32(0)).await?;
    ensure_not_cancelled(cancellation)?;
    ensure_tip(client, session_tip).await?;
    Ok(())
}

fn discard_unscanned_ironwood_roots(wallet: &mut WalletDatabase) -> Result<(), WcashSyncError> {
    let discarded = wallet
        .transactionally_with_extension::<_, _, SqliteClientError>(|wallet, extension| {
            let truncated =
                wallet.with_ironwood_tree_mut(|tree| tree.truncate_to_checkpoint_depth(0))?;
            if truncated != Some(true) && wallet.get_ironwood_subtree_root(0)?.is_some() {
                // With no checkpoint there is no authenticated position at
                // which the root-only tree can be truncated. Leave both roots
                // and provenance untouched and fail closed.
                return Ok(false);
            }
            extension.execute(&format!("DELETE FROM {IRONWOOD_SYNC_STATE_TABLE}"), [])?;
            Ok(true)
        })
        .map_err(database_error)?;
    if discarded {
        Ok(())
    } else {
        Err(WcashSyncError::StaleSubtreeRoot)
    }
}

fn stored_chain_matches(
    local_height: u32,
    local_hash: [u8; 32],
    session_tip: BlockRef,
    canonical: Option<BlockRef>,
) -> bool {
    local_height <= session_tip.height
        && canonical.is_some_and(|block| block.height == local_height && block.hash == local_hash)
}

fn bounded_range(
    range: &ScanRange,
    batch_size: u32,
    tip_height: u32,
) -> Result<ScanRange, WcashSyncError> {
    let start = range.block_range().start;
    let start_height: u32 = start.into();
    let maximum_end = start_height
        .checked_add(batch_size)
        .map(BlockHeight::from_u32)
        .ok_or_else(|| WcashSyncError::Scan("scan range height overflow".to_owned()))?;
    let tip_end = tip_height
        .checked_add(1)
        .map(BlockHeight::from_u32)
        .ok_or_else(|| WcashSyncError::Scan("chain tip height overflow".to_owned()))?;
    let end = cmp::min(cmp::min(range.block_range().end, maximum_end), tip_end);
    if start == BlockHeight::from_u32(0) || start >= end {
        return Err(WcashSyncError::Scan(
            "wallet proposed an invalid compact-block range".to_owned(),
        ));
    }
    Ok(ScanRange::from_parts(start..end, range.priority()))
}

async fn scan_range(
    client: &mut AttestedWcashClient,
    params: &zebra_chain::parameters::Network,
    cache: &MemoryBlockCache,
    wallet: &mut WalletDatabase,
    session_tip: BlockRef,
    range: &ScanRange,
    cancellation: &WalletSyncCancellation,
) -> Result<ScanOutcome, WcashSyncError> {
    ensure_not_cancelled(cancellation)?;
    let start: u32 = range.block_range().start.into();
    let end: u32 = range.block_range().end.into();
    let predecessor_height = start
        .checked_sub(1)
        .ok_or_else(|| WcashSyncError::Scan("scan range starts before height one".to_owned()))?;
    let predecessor = client.chain_state(predecessor_height).await?;
    ensure_not_cancelled(cancellation)?;
    let blocks = client.compact_block_range(start, end).await?;
    ensure_not_cancelled(cancellation)?;
    ensure_tip(client, session_tip).await?;
    cache
        .insert_attested_range(
            blocks,
            start..end,
            predecessor.block,
            predecessor.ironwood_tree_size,
        )
        .await?;
    ensure_not_cancelled(cancellation)?;

    let scan_result = scan_cached_blocks(
        params,
        cache,
        wallet,
        range.block_range().start,
        &predecessor.state,
        range.len(),
    );
    cache.delete(range.clone()).await?;
    ensure_not_cancelled(cancellation)?;

    match scan_result {
        Ok(summary) if summary.scanned_range() == range.block_range().clone() => {
            ensure_tip(client, session_tip).await?;
            Ok(ScanOutcome::Complete)
        }
        Ok(_) => Err(WcashSyncError::UnexpectedScanRange),
        Err(ChainError::Scan(error)) if error.is_continuity_error() => {
            let rewind_height = error.at_height().saturating_sub(10);
            wallet
                .truncate_to_height(rewind_height)
                .map_err(database_error)?;
            cache.truncate(rewind_height).await?;
            Ok(ScanOutcome::Restart)
        }
        Err(error) => Err(WcashSyncError::Scan(error.to_string())),
    }
}

fn bump_restart(restarts: &mut usize) -> Result<(), WcashSyncError> {
    *restarts = restarts.saturating_add(1);
    if *restarts > MAX_SESSION_RESTARTS {
        Err(WcashSyncError::SessionRestartLimit)
    } else {
        Ok(())
    }
}

fn ensure_session_deadline(started_at: Instant) -> Result<(), WcashSyncError> {
    if started_at.elapsed() >= MAX_SYNC_SESSION_DURATION {
        Err(WcashSyncError::SessionDeadline)
    } else {
        Ok(())
    }
}

fn ensure_not_cancelled(cancellation: &WalletSyncCancellation) -> Result<(), WcashSyncError> {
    if cancellation.is_cancelled() {
        Err(WcashSyncError::Cancelled)
    } else {
        Ok(())
    }
}

async fn refresh_ironwood_subtrees(
    client: &mut AttestedWcashClient,
    wallet: &mut WalletDatabase,
    tip: BlockRef,
    ironwood_tree_size: u32,
    cancellation: &WalletSyncCancellation,
) -> Result<(), WcashSyncError> {
    ensure_not_cancelled(cancellation)?;
    validate_ironwood_tree_size(tip.height, ironwood_tree_size)?;

    let completed_count = ironwood_tree_size >> IRONWOOD_SHARD_HEIGHT;
    let maximum_safe_existing_count = match wallet.get_max_height_hash().map_err(database_error)? {
        Some((height, hash)) => {
            let local_height: u32 = height.into();
            let local_state = client.chain_state(local_height).await?;
            ensure_not_cancelled(cancellation)?;
            if local_state.block.hash != hash.0 {
                return Err(WcashSyncError::StaleTip);
            }
            ensure_tip(client, tip).await?;
            validate_ironwood_tree_size(local_height, local_state.ironwood_tree_size)?;
            local_state.ironwood_tree_size >> IRONWOOD_SHARD_HEIGHT
        }
        None => 0,
    };
    let existing_count = existing_ironwood_root_prefix(wallet, completed_count)?;

    // Roots at or below the locally scanned tree size are bound by the
    // canonical local block checked above. A larger cached prefix may only be
    // reused when its page-atomic provenance still names a canonical ancestor
    // whose attested tree contained the entire prefix.
    let unscanned_roots_are_cached = existing_count > maximum_safe_existing_count;
    let prior_attestation = read_ironwood_prefix_attestation(wallet)?;
    let expected_attestation = if unscanned_roots_are_cached {
        let Some(attestation) =
            matching_ironwood_prefix_attestation(prior_attestation, existing_count)
        else {
            discard_unscanned_ironwood_roots(wallet)?;
            return Err(WcashSyncError::SubtreeRestart);
        };
        if !ironwood_prefix_attestation_is_canonical(client, tip, attestation, cancellation).await?
        {
            discard_unscanned_ironwood_roots(wallet)?;
            return Err(WcashSyncError::SubtreeRestart);
        }
        Some(attestation)
    } else {
        None
    };

    let (boundary_start, boundary_page, mut previous_height) =
        if let Some(boundary_index) = existing_count.checked_sub(1) {
            let stored_root = wallet
                .get_ironwood_subtree_root(u64::from(boundary_index))
                .map_err(tree_error)?
                .ok_or(WcashSyncError::StaleSubtreeRoot)?;
            let mut boundary = client
                .ironwood_subtree_roots(boundary_index, 1, tip.height)
                .await?;
            ensure_not_cancelled(cancellation)?;
            let boundary = boundary.pop().ok_or(WcashSyncError::StaleSubtreeRoot)?;
            if boundary.root_hash() != &stored_root {
                if unscanned_roots_are_cached {
                    discard_unscanned_ironwood_roots(wallet)?;
                    return Err(WcashSyncError::SubtreeRestart);
                }
                return Err(WcashSyncError::StaleSubtreeRoot);
            }
            let boundary_height = u32::from(boundary.subtree_end_height());
            (boundary_index, vec![boundary], Some(boundary_height))
        } else {
            (0, Vec::new(), None)
        };

    // Refreshing the exact boundary root makes a resumed prefix prove both its
    // root and its completion height on the current chain. The root rewrite
    // and provenance rebind commit together, so a crash cannot separate them.
    ensure_tip(client, tip).await?;
    let mut attestation = bind_ironwood_prefix_attestation(
        wallet,
        expected_attestation,
        existing_count,
        tip,
        boundary_start,
        &boundary_page,
    )?;

    let mut start_index = existing_count;
    while start_index < completed_count {
        ensure_not_cancelled(cancellation)?;
        let count = cmp::min(
            completed_count - start_index,
            cmp::min(
                MAX_PERSISTED_SUBTREE_ROOTS_PER_PAGE,
                crate::rpc::MAX_SUBTREE_ROOTS_PER_REQUEST,
            ),
        );
        let page = client
            .ironwood_subtree_roots(start_index, count, tip.height)
            .await?;
        ensure_not_cancelled(cancellation)?;
        validate_subtree_heights(
            &mut previous_height,
            page.iter().map(|root| u32::from(root.subtree_end_height())),
        )?;
        attestation = persist_attested_ironwood_page(
            client,
            wallet,
            tip,
            attestation,
            start_index,
            &page,
            cancellation,
        )
        .await?;
        start_index = attestation.prefix_count;
    }

    ensure_tip(client, tip).await?;
    Ok(())
}

fn matching_ironwood_prefix_attestation(
    stored: StoredIronwoodPrefixAttestation,
    existing_count: u32,
) -> Option<IronwoodPrefixAttestation> {
    match stored {
        StoredIronwoodPrefixAttestation::Valid(attestation)
            if attestation.prefix_count == existing_count =>
        {
            Some(attestation)
        }
        StoredIronwoodPrefixAttestation::Missing
        | StoredIronwoodPrefixAttestation::Malformed
        | StoredIronwoodPrefixAttestation::Valid(_) => None,
    }
}

async fn ironwood_prefix_attestation_is_canonical(
    client: &mut AttestedWcashClient,
    current_tip: BlockRef,
    attestation: IronwoodPrefixAttestation,
    cancellation: &WalletSyncCancellation,
) -> Result<bool, WcashSyncError> {
    ensure_not_cancelled(cancellation)?;
    if attestation.tip.height > current_tip.height {
        return Ok(false);
    }
    let attested_state = client.chain_state(attestation.tip.height).await?;
    ensure_not_cancelled(cancellation)?;
    ensure_tip(client, current_tip).await?;
    validate_ironwood_tree_size(attestation.tip.height, attested_state.ironwood_tree_size)?;
    Ok(attested_state.block == attestation.tip
        && attestation.prefix_count <= attested_state.ironwood_tree_size >> IRONWOOD_SHARD_HEIGHT)
}

async fn persist_attested_ironwood_page(
    client: &mut AttestedWcashClient,
    wallet: &mut WalletDatabase,
    tip: BlockRef,
    expected_attestation: IronwoodPrefixAttestation,
    start_index: u32,
    page: &[CommitmentTreeRoot<orchard::tree::MerkleHashOrchard>],
    cancellation: &WalletSyncCancellation,
) -> Result<IronwoodPrefixAttestation, WcashSyncError> {
    // `ironwood_subtree_roots` has already enforced the requested count and
    // independently checked every completing block hash/tree-size transition.
    // Re-check the session tip immediately before the page's atomic SQLite
    // transaction so roots and their durable provenance cannot be separated.
    ensure_not_cancelled(cancellation)?;
    ensure_tip(client, tip).await?;
    ensure_not_cancelled(cancellation)?;
    commit_attested_ironwood_page(wallet, expected_attestation, start_index, page, tip)
}

fn commit_attested_ironwood_page(
    wallet: &mut WalletDatabase,
    expected_attestation: IronwoodPrefixAttestation,
    start_index: u32,
    page: &[CommitmentTreeRoot<orchard::tree::MerkleHashOrchard>],
    tip: BlockRef,
) -> Result<IronwoodPrefixAttestation, WcashSyncError> {
    let page_count = u32::try_from(page.len())
        .map_err(|_| WcashSyncError::Scan("subtree page count overflow".to_owned()))?;
    let new_prefix_count = start_index
        .checked_add(page_count)
        .ok_or_else(|| WcashSyncError::Scan("subtree index overflow".to_owned()))?;
    if page_count == 0 || expected_attestation.prefix_count != start_index {
        return Err(WcashSyncError::Scan(
            "subtree page does not extend the attested prefix".to_owned(),
        ));
    }

    wallet
        .transactionally_with_extension::<_, _, SqliteClientError>(|wallet, extension| {
            wallet.put_ironwood_subtree_roots(u64::from(start_index), page)?;
            let changed = extension.execute(
                &format!(
                    "UPDATE {IRONWOOD_SYNC_STATE_TABLE}
                 SET verified_prefix_count = ?1,
                     attested_tip_height = ?2,
                     attested_tip_hash = ?3
                 WHERE singleton = 1
                   AND verified_prefix_count = ?4
                   AND attested_tip_height = ?5
                   AND attested_tip_hash = ?6
                   AND ?1 > verified_prefix_count
                   AND ?2 >= attested_tip_height"
                ),
                params![
                    i64::from(new_prefix_count),
                    i64::from(tip.height),
                    tip.hash.as_slice(),
                    i64::from(expected_attestation.prefix_count),
                    i64::from(expected_attestation.tip.height),
                    expected_attestation.tip.hash.as_slice(),
                ],
            )?;
            if changed != 1 {
                return Err(SqliteClientError::CorruptedData(
                    "Ironwood subtree attestation did not match the expected prefix".to_owned(),
                ));
            }
            Ok(())
        })
        .map_err(database_error)?;

    Ok(IronwoodPrefixAttestation {
        prefix_count: new_prefix_count,
        tip,
    })
}

fn bind_ironwood_prefix_attestation(
    wallet: &mut WalletDatabase,
    expected_attestation: Option<IronwoodPrefixAttestation>,
    prefix_count: u32,
    tip: BlockRef,
    boundary_start: u32,
    boundary_page: &[CommitmentTreeRoot<orchard::tree::MerkleHashOrchard>],
) -> Result<IronwoodPrefixAttestation, WcashSyncError> {
    let boundary_count = u32::try_from(boundary_page.len())
        .map_err(|_| WcashSyncError::Scan("subtree boundary count overflow".to_owned()))?;
    let boundary_end = boundary_start
        .checked_add(boundary_count)
        .ok_or_else(|| WcashSyncError::Scan("subtree boundary overflow".to_owned()))?;
    if boundary_end != prefix_count
        || expected_attestation.is_some_and(|expected| expected.prefix_count != prefix_count)
    {
        return Err(WcashSyncError::Scan(
            "subtree boundary does not match the attested prefix".to_owned(),
        ));
    }

    wallet
        .transactionally_with_extension::<_, _, SqliteClientError>(|wallet, extension| {
            wallet.put_ironwood_subtree_roots(u64::from(boundary_start), boundary_page)?;
            let changed = if let Some(expected) = expected_attestation {
                extension.execute(
                    &format!(
                        "UPDATE {IRONWOOD_SYNC_STATE_TABLE}
                     SET attested_tip_height = ?1,
                         attested_tip_hash = ?2
                     WHERE singleton = 1
                       AND verified_prefix_count = ?3
                       AND attested_tip_height = ?4
                       AND attested_tip_hash = ?5
                       AND ?1 >= attested_tip_height"
                    ),
                    params![
                        i64::from(tip.height),
                        tip.hash.as_slice(),
                        i64::from(expected.prefix_count),
                        i64::from(expected.tip.height),
                        expected.tip.hash.as_slice(),
                    ],
                )?
            } else {
                extension.execute(
                    &format!(
                        "INSERT INTO {IRONWOOD_SYNC_STATE_TABLE} (
                         singleton,
                         verified_prefix_count,
                         attested_tip_height,
                         attested_tip_hash
                     ) VALUES (1, ?1, ?2, ?3)
                     ON CONFLICT(singleton) DO UPDATE SET
                         verified_prefix_count = excluded.verified_prefix_count,
                         attested_tip_height = excluded.attested_tip_height,
                         attested_tip_hash = excluded.attested_tip_hash"
                    ),
                    params![
                        i64::from(prefix_count),
                        i64::from(tip.height),
                        tip.hash.as_slice(),
                    ],
                )?
            };
            if changed != 1 {
                return Err(SqliteClientError::CorruptedData(
                    "Ironwood subtree attestation changed during boundary validation".to_owned(),
                ));
            }
            Ok(())
        })
        .map_err(database_error)?;

    Ok(IronwoodPrefixAttestation { prefix_count, tip })
}

fn read_ironwood_prefix_attestation(
    wallet: &mut WalletDatabase,
) -> Result<StoredIronwoodPrefixAttestation, WcashSyncError> {
    let raw = wallet
        .transactionally_with_extension::<_, _, SqliteClientError>(|_wallet, extension| {
            match extension.query_row(
                &format!(
                    "SELECT verified_prefix_count, attested_tip_height, attested_tip_hash
                     FROM {IRONWOOD_SYNC_STATE_TABLE}
                     WHERE singleton = 1"
                ),
                [],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                    ))
                },
            ) {
                Ok(row) => Ok(Some(row)),
                Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
                Err(error) => Err(SqliteClientError::from(error)),
            }
        })
        .map_err(database_error)?;

    let Some((prefix_count, tip_height, tip_hash)) = raw else {
        return Ok(StoredIronwoodPrefixAttestation::Missing);
    };
    Ok(decode_ironwood_prefix_attestation(
        prefix_count,
        tip_height,
        tip_hash,
    ))
}

fn decode_ironwood_prefix_attestation(
    prefix_count: i64,
    tip_height: i64,
    tip_hash: Vec<u8>,
) -> StoredIronwoodPrefixAttestation {
    let (Ok(prefix_count), Ok(tip_height), Ok(tip_hash)) = (
        u32::try_from(prefix_count),
        u32::try_from(tip_height),
        <[u8; 32]>::try_from(tip_hash),
    ) else {
        return StoredIronwoodPrefixAttestation::Malformed;
    };
    StoredIronwoodPrefixAttestation::Valid(IronwoodPrefixAttestation {
        prefix_count,
        tip: BlockRef {
            height: tip_height,
            hash: tip_hash,
        },
    })
}

fn validate_ironwood_tree_size(
    tip_height: u32,
    ironwood_tree_size: u32,
) -> Result<(), WcashSyncError> {
    let maximum_tree_size = u64::from(tip_height)
        .checked_mul(
            u64::try_from(crate::cache::MAX_IRONWOOD_ACTIONS_PER_BLOCK)
                .expect("the per-block Ironwood action bound fits in u64"),
        )
        .ok_or(WcashSyncError::ImpossibleIronwoodTreeSize)?;
    if u64::from(ironwood_tree_size) > maximum_tree_size {
        return Err(WcashSyncError::ImpossibleIronwoodTreeSize);
    }
    Ok(())
}

fn existing_ironwood_root_prefix(
    wallet: &mut WalletDatabase,
    completed_count: u32,
) -> Result<u32, WcashSyncError> {
    if wallet
        .get_ironwood_subtree_root(u64::from(completed_count))
        .map_err(tree_error)?
        .is_some()
    {
        discard_unscanned_ironwood_roots(wallet)?;
        return Err(WcashSyncError::SubtreeRestart);
    }

    // Wallet roots are inserted only as a contiguous prefix. Binary search
    // keeps the stable-chain fast path logarithmic even after many shards.
    let mut present = 0u32;
    let mut absent = completed_count;
    while present < absent {
        let midpoint = present + (absent - present) / 2;
        if wallet
            .get_ironwood_subtree_root(u64::from(midpoint))
            .map_err(tree_error)?
            .is_some()
        {
            present = midpoint + 1;
        } else {
            absent = midpoint;
        }
    }

    Ok(present)
}

fn validate_subtree_heights(
    previous_height: &mut Option<u32>,
    heights: impl IntoIterator<Item = u32>,
) -> Result<(), WcashSyncError> {
    for height in heights {
        if previous_height.is_some_and(|previous| height <= previous) {
            return Err(WcashSyncError::SubtreeOrder);
        }
        *previous_height = Some(height);
    }
    Ok(())
}

async fn ensure_tip(
    client: &mut AttestedWcashClient,
    expected: BlockRef,
) -> Result<(), WcashSyncError> {
    if client.latest_block().await? == expected {
        Ok(())
    } else {
        Err(WcashSyncError::StaleTip)
    }
}

fn database_error(error: impl std::fmt::Display) -> WcashSyncError {
    WcashSyncError::Database(error.to_string())
}

fn tree_error(error: impl std::fmt::Display) -> WcashSyncError {
    WcashSyncError::Database(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_ranges_are_nonempty_and_never_exceed_the_network_limit() {
        let range = ScanRange::from_parts(
            BlockHeight::from_u32(7)..BlockHeight::from_u32(100),
            ScanPriority::Historic,
        );
        assert_eq!(
            bounded_range(&range, MAX_ATTESTED_COMPACT_BLOCKS_PER_RANGE, 99)
                .unwrap()
                .block_range()
                .clone(),
            BlockHeight::from_u32(7)..BlockHeight::from_u32(23)
        );

        let empty = ScanRange::from_parts(
            BlockHeight::from_u32(7)..BlockHeight::from_u32(7),
            ScanPriority::Historic,
        );
        assert!(bounded_range(&empty, 1, 99).is_err());
        let genesis = ScanRange::from_parts(
            BlockHeight::from_u32(0)..BlockHeight::from_u32(1),
            ScanPriority::Historic,
        );
        assert!(bounded_range(&genesis, 1, 99).is_err());

        let past_tip = ScanRange::from_parts(
            BlockHeight::from_u32(11)..BlockHeight::from_u32(12),
            ScanPriority::Historic,
        );
        assert!(bounded_range(&past_tip, 1, 10).is_err());
    }

    #[test]
    fn subtree_order_is_strict_across_page_boundaries() {
        let mut previous = None;
        validate_subtree_heights(&mut previous, [7, 12]).unwrap();
        validate_subtree_heights(&mut previous, [18, 21]).unwrap();
        assert_eq!(previous, Some(21));
        assert!(matches!(
            validate_subtree_heights(&mut previous, [21]),
            Err(WcashSyncError::SubtreeOrder)
        ));
    }

    #[test]
    fn ironwood_tree_size_must_fit_the_attested_block_count() {
        let maximum = u32::try_from(crate::cache::MAX_IRONWOOD_ACTIONS_PER_BLOCK).unwrap();
        assert!(validate_ironwood_tree_size(0, 0).is_ok());
        assert!(validate_ironwood_tree_size(1, maximum).is_ok());
        assert!(matches!(
            validate_ironwood_tree_size(0, 1),
            Err(WcashSyncError::ImpossibleIronwoodTreeSize)
        ));
        assert!(matches!(
            validate_ironwood_tree_size(1, maximum + 1),
            Err(WcashSyncError::ImpossibleIronwoodTreeSize)
        ));
    }

    #[test]
    fn ironwood_prefix_attestation_decode_is_bounded() {
        let valid = IronwoodPrefixAttestation {
            prefix_count: u32::MAX,
            tip: BlockRef {
                height: u32::MAX,
                hash: [0x5a; 32],
            },
        };
        assert_eq!(
            decode_ironwood_prefix_attestation(
                i64::from(valid.prefix_count),
                i64::from(valid.tip.height),
                valid.tip.hash.to_vec(),
            ),
            StoredIronwoodPrefixAttestation::Valid(valid)
        );
        assert_eq!(
            decode_ironwood_prefix_attestation(-1, 0, vec![0; 32]),
            StoredIronwoodPrefixAttestation::Malformed
        );
        assert_eq!(
            decode_ironwood_prefix_attestation(i64::from(u32::MAX) + 1, 0, vec![0; 32]),
            StoredIronwoodPrefixAttestation::Malformed
        );
        assert_eq!(
            decode_ironwood_prefix_attestation(0, 0, vec![0; 31]),
            StoredIronwoodPrefixAttestation::Malformed
        );
        assert_eq!(
            matching_ironwood_prefix_attestation(
                StoredIronwoodPrefixAttestation::Valid(valid),
                valid.prefix_count - 1,
            ),
            None,
            "a crash-style root/provenance count mismatch must not bless the cached prefix",
        );
        assert_eq!(
            matching_ironwood_prefix_attestation(
                StoredIronwoodPrefixAttestation::Missing,
                valid.prefix_count,
            ),
            None
        );
        assert_eq!(
            matching_ironwood_prefix_attestation(
                StoredIronwoodPrefixAttestation::Malformed,
                valid.prefix_count,
            ),
            None
        );
    }

    #[test]
    fn ironwood_page_and_provenance_advance_atomically() {
        let directory = tempfile::tempdir().unwrap();
        let wallet_path = directory.path().join("wallet.sqlite");
        let mut wallet =
            crate::open_wallet_database(&wallet_path, crate::WalletNetwork::Regtest).unwrap();
        let tip = BlockRef {
            height: 10,
            hash: [0x2a; 32],
        };
        let initial = bind_ironwood_prefix_attestation(&mut wallet, None, 0, tip, 0, &[]).unwrap();

        let first_root = orchard::tree::MerkleHashOrchard::from_bytes(&[1; 32]).unwrap();
        let first_page = [CommitmentTreeRoot::from_parts(
            BlockHeight::from_u32(7),
            first_root,
        )];
        let advanced =
            commit_attested_ironwood_page(&mut wallet, initial, 0, &first_page, tip).unwrap();
        assert_eq!(advanced.prefix_count, 1);
        assert_eq!(
            read_ironwood_prefix_attestation(&mut wallet).unwrap(),
            StoredIronwoodPrefixAttestation::Valid(advanced)
        );
        assert_eq!(existing_ironwood_root_prefix(&mut wallet, 2).unwrap(), 1);

        // A provenance compare-and-swap failure must roll the root write back
        // with it, rather than leaving an unproven extension of the prefix.
        let wrong_expected = IronwoodPrefixAttestation {
            prefix_count: 1,
            tip: BlockRef {
                height: tip.height,
                hash: [0x99; 32],
            },
        };
        let second_root = orchard::tree::MerkleHashOrchard::from_bytes(&[2; 32]).unwrap();
        let second_page = [CommitmentTreeRoot::from_parts(
            BlockHeight::from_u32(8),
            second_root,
        )];
        assert!(matches!(
            commit_attested_ironwood_page(&mut wallet, wrong_expected, 1, &second_page, tip),
            Err(WcashSyncError::Database(_))
        ));
        assert_eq!(existing_ironwood_root_prefix(&mut wallet, 2).unwrap(), 1);
        assert_eq!(
            read_ironwood_prefix_attestation(&mut wallet).unwrap(),
            StoredIronwoodPrefixAttestation::Valid(advanced)
        );
    }

    #[test]
    fn stored_chain_match_rejects_shorter_and_replaced_tips() {
        let session_tip = BlockRef {
            height: 11,
            hash: [11; 32],
        };
        assert!(stored_chain_matches(
            10,
            [10; 32],
            session_tip,
            Some(BlockRef {
                height: 10,
                hash: [10; 32],
            }),
        ));
        assert!(!stored_chain_matches(12, [12; 32], session_tip, None));
        assert!(!stored_chain_matches(
            11,
            [0xaa; 32],
            session_tip,
            Some(session_tip),
        ));
        assert!(!stored_chain_matches(
            10,
            [0xaa; 32],
            session_tip,
            Some(BlockRef {
                height: 10,
                hash: [0xbb; 32],
            }),
        ));
    }
}
