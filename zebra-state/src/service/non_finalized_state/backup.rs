use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs::DirEntry,
    io::{self, ErrorKind, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use hex::ToHex;
use sha2::{Digest, Sha256};
use zebra_chain::{
    amount::Amount,
    block::{self, Block, Height},
    serialization::{ZcashDeserializeInto, ZcashSerialize},
};

use crate::{
    service::{
        finalized_state::{DiskDb, ReadDisk},
        foundation_validation::coverage,
    },
    Config, ContextuallyVerifiedBlock, IntoDisk, NonFinalizedState, SemanticallyVerifiedBlock,
    StateInitError, WatchReceiver, ZebraDb,
};

#[cfg(not(test))]
use crate::service::write::validate_and_commit_non_finalized;

/// The minimum duration that Zebra will wait between updates to the non-finalized state backup cache.
pub(crate) const MIN_DURATION_BETWEEN_BACKUP_UPDATES: Duration = Duration::from_secs(5);

/// Kept separate from legacy backup bytes, so old executables can still read the blocks.
const COVERAGE_DIRECTORY: &str = "foundation-validation";

/// Unpublished records are ignored on restart and retained for inspection.
const UNPUBLISHED_DIRECTORY: &str = "unpublished-backup-records";

/// Check legacy backup history before a writable database open or format migration starts.
/// This preflight never deletes directory entries, including malformed legacy files.
pub(crate) fn check_foundation_backup_coverage(
    config: &Config,
    network: &zebra_chain::parameters::Network,
    finalized: Option<&DiskDb>,
) -> Result<(), StateInitError> {
    let Some(path) = config
        .non_finalized_state_backup_dir(network)
        .filter(|path| path.exists())
    else {
        return Ok(());
    };
    for entry in read_backup_dir(&path) {
        let Some(hash) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<block::Hash>().ok())
        else {
            continue;
        };
        if finalized.is_some_and(|db| {
            db.cf_handle("height_by_hash")
                .is_some_and(|cf| db.zs_contains(&cf, &hash))
        }) {
            continue;
        }
        let Ok(bytes) = std::fs::read(entry.path()) else {
            continue;
        };
        let Ok(backup) = NonFinalizedBlockBackup::from_bytes(&bytes) else {
            continue;
        };
        if backup.block.coinbase_height().is_none() {
            continue;
        }
        check_backup_coverage(
            &path,
            network,
            &SemanticallyVerifiedBlock::from(backup.block),
            &bytes,
        )?;
    }
    Ok(())
}

/// Accepts an optional path to the non-finalized state backup directory and a handle to the database.
///
/// Looks for blocks above the finalized tip height in the backup directory (if a path was provided) and
/// attempts to commit them to the non-finalized state.
///
/// Returns the resulting non-finalized state.
pub(super) fn restore_backup(
    mut non_finalized_state: NonFinalizedState,
    backup_dir_path: &Path,
    finalized_state: &ZebraDb,
) -> Result<NonFinalizedState, StateInitError> {
    let mut store: BTreeMap<Height, Vec<SemanticallyVerifiedBlock>> = BTreeMap::new();

    for (block, bytes) in read_non_finalized_blocks_from_backup(backup_dir_path, finalized_state) {
        check_backup_coverage(
            backup_dir_path,
            &non_finalized_state.network,
            &block,
            &bytes,
        )?;
        store.entry(block.height).or_default().push(block);
    }

    for (height, blocks) in store {
        for block in blocks {
            #[cfg(test)]
            let commit_result = if non_finalized_state
                .any_chain_contains(&block.block.header.previous_block_hash)
            {
                non_finalized_state.commit_block(block, finalized_state)
            } else {
                non_finalized_state.commit_new_chain(block, finalized_state)
            };

            #[cfg(not(test))]
            let commit_result =
                validate_and_commit_non_finalized(finalized_state, &mut non_finalized_state, block);

            // Re-computes the block hash in case the hash from the filename is wrong.
            if let Err(commit_error) = commit_result {
                tracing::warn!(
                    ?commit_error,
                    ?height,
                    "failed to commit non-finalized block from backup directory"
                );
            }
        }
    }

    Ok(non_finalized_state)
}

/// Updates the non-finalized state backup cache by writing any blocks that are in the
/// non-finalized state but missing in the backup cache, and deleting any backup files
/// that are no longer present in the non-finalized state.
///
/// `backup_blocks` should be the current contents of the backup directory, obtained by
/// calling [`list_backup_dir_entries`] before the non-finalized state was updated.
///
/// This function performs blocking I/O and should be called from a blocking context,
/// or wrapped in [`tokio::task::spawn_blocking`].
/// Returns whether all requested writes and removals completed with the platform guarantee:
/// durable paired publication on Unix, or legacy single-file publication for noncovered records
/// elsewhere. Covered publication on unsupported platforms fails before mutation.
pub(super) fn update_non_finalized_state_backup(
    backup_dir_path: &Path,
    non_finalized_state: &NonFinalizedState,
    backup_blocks: HashMap<block::Hash, PathBuf>,
) -> bool {
    update_backup_with_cache(
        backup_dir_path,
        non_finalized_state,
        backup_blocks,
        &mut HashMap::new(),
        &mut HashMap::new(),
        |_| Ok(()),
    )
}

/// A process-local optimization: only records already compared with trusted memory and durably
/// published can enter this cache. Changed or replaced files must pass the full check again.
#[derive(Debug, PartialEq, Eq)]
struct BackupFileStamp {
    length: u64,
    modified: std::time::SystemTime,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
}

impl BackupFileStamp {
    fn read(path: &Path) -> io::Result<Self> {
        let metadata = std::fs::metadata(path)?;
        #[cfg(unix)]
        use std::os::unix::fs::MetadataExt;
        Ok(Self {
            length: metadata.len(),
            modified: metadata.modified()?,
            #[cfg(unix)]
            device: metadata.dev(),
            #[cfg(unix)]
            inode: metadata.ino(),
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
struct BackupRecordStamp {
    block: BackupFileStamp,
    marker: Option<BackupFileStamp>,
}

impl BackupRecordStamp {
    fn read(
        directory: &Path,
        network: &zebra_chain::parameters::Network,
        block: &ContextuallyVerifiedBlock,
    ) -> io::Result<Self> {
        let filename = block.hash.encode_hex::<String>();
        Ok(Self {
            block: BackupFileStamp::read(&directory.join(&filename))?,
            marker: coverage(network, block.height, block.hash)
                .map(|_| BackupFileStamp::read(&directory.join(COVERAGE_DIRECTORY).join(&filename)))
                .transpose()?,
        })
    }
}

fn update_backup_with_cache(
    backup_dir_path: &Path,
    non_finalized_state: &NonFinalizedState,
    mut backup_blocks: HashMap<block::Hash, PathBuf>,
    cache: &mut HashMap<block::Hash, BackupRecordStamp>,
    pending_removals: &mut HashMap<block::Hash, PathBuf>,
    mut removal_hook: impl FnMut(BackupStep) -> io::Result<()>,
) -> bool {
    // A failed unlink-directory-sync or marker removal must survive the next directory listing,
    // where the block itself might already be absent. Desired hashes cancel pending removal.
    backup_blocks.extend(std::mem::take(pending_removals));
    let mut complete = true;
    let mut visited = HashSet::new();
    for block in non_finalized_state
        .chain_iter()
        .flat_map(|chain| chain.blocks.values())
        .filter(|block| visited.insert(block.hash))
    {
        backup_blocks.remove(&block.hash);
        let stamp = BackupRecordStamp::read(backup_dir_path, &non_finalized_state.network, block);
        if stamp
            .as_ref()
            .is_ok_and(|stamp| cache.get(&block.hash) == Some(stamp))
        {
            continue;
        }
        cache.remove(&block.hash);
        // A filename alone cannot prove a previous attempt finished. Only trusted memory may
        // repair a missing record, and an exact, already-covered record needs no rewrite.
        if let Err(err) = write_backup_block(backup_dir_path, &non_finalized_state.network, block) {
            complete = false;
            tracing::warn!(?err, ?block.hash, "failed to publish non-finalized state backup; will retry");
        } else if let Ok(stamp) =
            BackupRecordStamp::read(backup_dir_path, &non_finalized_state.network, block)
        {
            cache.insert(block.hash, stamp);
        }
    }
    cache.retain(|hash, _| visited.contains(hash));

    // Remove any backup blocks that are not present in the non-finalized state
    for (hash, outdated_backup_block_path) in backup_blocks {
        if let Err(err) = remove_backup_block(&outdated_backup_block_path, &mut removal_hook) {
            complete = false;
            pending_removals.insert(hash, outdated_backup_block_path);
            tracing::warn!(
                ?err,
                "failed to durably remove non-finalized state backup; will retry"
            );
        }
    }
    complete
}

/// Updates the non-finalized state backup cache whenever the non-finalized state changes,
/// deleting any outdated backup files and writing any blocks that are in the non-finalized
/// state but missing in the backup cache.
pub(super) async fn run_backup_task(
    non_finalized_state_receiver: WatchReceiver<NonFinalizedState>,
    backup_dir_path: PathBuf,
) {
    run_backup_task_with_report(non_finalized_state_receiver, backup_dir_path, |_| {}).await;
}

async fn run_backup_task_with_report(
    mut non_finalized_state_receiver: WatchReceiver<NonFinalizedState>,
    backup_dir_path: PathBuf,
    mut report_update: impl FnMut(bool),
) {
    let mut retry_pending = false;
    let mut cache = HashMap::new();
    let mut pending_removals = HashMap::new();
    let err = loop {
        let rate_limit = tokio::time::sleep(MIN_DURATION_BETWEEN_BACKUP_UPDATES);
        if retry_pending {
            // A restored filesystem must be retried even if no new block changes the watch value.
            rate_limit.await;
            if let Err(err) = non_finalized_state_receiver.has_changed() {
                break err;
            }
            non_finalized_state_receiver.mark_as_seen();
        } else if let (Err(err), _) =
            tokio::join!(non_finalized_state_receiver.changed(), rate_limit)
        {
            break err;
        }

        let latest_non_finalized_state = non_finalized_state_receiver.cloned_watch_data();

        let backup_dir_path = backup_dir_path.clone();
        let (complete, updated_cache, updated_removals) = tokio::task::spawn_blocking(move || {
            let backup_blocks = list_backup_dir_entries(&backup_dir_path).collect();
            let complete = update_backup_with_cache(
                &backup_dir_path,
                &latest_non_finalized_state,
                backup_blocks,
                &mut cache,
                &mut pending_removals,
                |_| Ok(()),
            );
            (complete, cache, pending_removals)
        })
        .await
        .expect("failed to join blocking task when writing in backup task");
        retry_pending = !complete;
        cache = updated_cache;
        pending_removals = updated_removals;
        report_update(complete);
    };

    tracing::warn!(
        ?err,
        "got recv error waiting on non-finalized state change, is Zebra shutting down?"
    )
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct NonFinalizedBlockBackup {
    block: Arc<Block>,
    deferred_pool_balance_change: Amount,
}

impl From<&ContextuallyVerifiedBlock> for NonFinalizedBlockBackup {
    fn from(cv_block: &ContextuallyVerifiedBlock) -> Self {
        Self {
            block: cv_block.block.clone(),
            deferred_pool_balance_change: cv_block.chain_value_pool_change.deferred_amount(),
        }
    }
}

impl NonFinalizedBlockBackup {
    /// Encodes a [`NonFinalizedBlockBackup`] as a vector of bytes.
    fn as_bytes(&self) -> Vec<u8> {
        let block_bytes = self
            .block
            .zcash_serialize_to_vec()
            .expect("verified block header version should be valid");

        let deferred_pool_balance_change_bytes =
            self.deferred_pool_balance_change.as_bytes().to_vec();

        [deferred_pool_balance_change_bytes, block_bytes].concat()
    }

    /// Constructs a new [`NonFinalizedBlockBackup`] from a vector of bytes.
    #[allow(clippy::unwrap_in_result)]
    fn from_bytes(bytes: &[u8]) -> Result<Self, io::Error> {
        let (deferred_pool_balance_change_bytes, block_bytes) = bytes
            .split_at_checked(size_of::<Amount>())
            .ok_or(io::Error::new(
                ErrorKind::InvalidInput,
                "input is too short",
            ))?;

        Ok(Self {
            block: Arc::new(
                block_bytes
                    .zcash_deserialize_into()
                    .map_err(|err| io::Error::new(ErrorKind::InvalidData, err))?,
            ),
            deferred_pool_balance_change: Amount::from_bytes(
                deferred_pool_balance_change_bytes
                    .try_into()
                    .expect("slice from `split_at_checked()` should fit in [u8; 8]"),
            )
            .map_err(|err| io::Error::new(ErrorKind::InvalidData, err))?,
        })
    }
}

/// Writes a block to a file in the provided non-finalized state backup cache directory path.
fn write_backup_block(
    backup_dir_path: &Path,
    network: &zebra_chain::parameters::Network,
    block: &ContextuallyVerifiedBlock,
) -> io::Result<()> {
    write_backup_block_with_hook(backup_dir_path, network, block, |_| Ok(()))
}

/// Publication boundaries used by deterministic failure tests. Production always supplies a no-op.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BackupStep {
    ExistingBlockWithdrawn,
    WithdrawalDirectorySynced,
    BlockTemporaryCreated,
    BlockPartialWrite,
    BlockFlushed,
    BlockSynced,
    BeforeCoverageDirectoryCreation,
    CoverageDirectoryCreated,
    CoverageParentSynced,
    MarkerTemporaryCreated,
    MarkerPartialWrite,
    MarkerFlushed,
    MarkerSynced,
    MarkerPublished,
    MarkerDirectorySynced,
    BlockPublished,
    BlockDirectorySynced,
    BeforeBlockRemoval,
    BlockRemoved,
    RemovedBlockDirectorySynced,
    MarkerRemoved,
    RemovedMarkerDirectorySynced,
}

fn write_backup_block_with_hook(
    backup_dir_path: &Path,
    network: &zebra_chain::parameters::Network,
    block: &ContextuallyVerifiedBlock,
    mut hook: impl FnMut(BackupStep) -> io::Result<()>,
) -> io::Result<()> {
    if cfg!(not(unix)) {
        return write_noncovered_backup_without_directory_sync(backup_dir_path, network, block);
    }
    let backup_block_file_name: String = block.hash.encode_hex();
    let backup_block_file_path = backup_dir_path.join(&backup_block_file_name);
    let non_finalized_block_backup: NonFinalizedBlockBackup = block.into();
    let bytes = non_finalized_block_backup.as_bytes();
    let marker = backup_marker(network, block.height, block.hash, &bytes);
    let marker_dir = backup_dir_path.join(COVERAGE_DIRECTORY);
    let marker_path = marker_dir.join(&backup_block_file_name);

    match std::fs::read(&backup_block_file_path) {
        Ok(existing) => {
            let covered = marker.as_ref().is_none_or(|expected| {
                std::fs::read(&marker_path).is_ok_and(|actual| actual == *expected)
            });
            if existing == bytes && covered {
                // A preceding attempt might have failed after rename but before directory sync.
                if marker.is_some() {
                    sync_directory(&marker_dir)?;
                    hook(BackupStep::MarkerDirectorySynced)?;
                }
                sync_directory(backup_dir_path)?;
                hook(BackupStep::BlockDirectorySynced)?;
                return Ok(());
            }
            // Replacing the marker while different bytes are still published could poison a
            // previously usable record. Durably withdraw it first, preserving the bytes.
            let unpublished = backup_dir_path.join(UNPUBLISHED_DIRECTORY);
            std::fs::create_dir_all(&unpublished)?;
            let (_, withdrawn_path) = tempfile::Builder::new()
                .prefix("record-")
                .tempfile_in(&unpublished)?
                .keep()
                .map_err(|error| error.error)?;
            std::fs::rename(&backup_block_file_path, withdrawn_path)?;
            hook(BackupStep::ExistingBlockWithdrawn)?;
            sync_directory(&unpublished)?;
            sync_directory(backup_dir_path)?;
            hook(BackupStep::WithdrawalDirectorySynced)?;
        }
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }

    let mut staged_block = tempfile::Builder::new()
        .prefix(".unpublished-block-")
        .tempfile_in(backup_dir_path)?;
    hook(BackupStep::BlockTemporaryCreated)?;
    stage_backup_bytes(&mut staged_block, &bytes, false, &mut hook)?;

    if let Some(marker) = marker {
        hook(BackupStep::BeforeCoverageDirectoryCreation)?;
        std::fs::create_dir_all(&marker_dir)?;
        hook(BackupStep::CoverageDirectoryCreated)?;
        // The directory itself must survive before publishing a block which depends on it.
        sync_directory(backup_dir_path)?;
        hook(BackupStep::CoverageParentSynced)?;
        let mut staged_marker = tempfile::Builder::new()
            .prefix(".unpublished-marker-")
            .tempfile_in(&marker_dir)?;
        hook(BackupStep::MarkerTemporaryCreated)?;
        stage_backup_bytes(&mut staged_marker, &marker, true, &mut hook)?;
        staged_marker
            .persist(&marker_path)
            .map_err(|error| error.error)?;
        hook(BackupStep::MarkerPublished)?;
        sync_directory(&marker_dir)?;
        hook(BackupStep::MarkerDirectorySynced)?;
    }

    staged_block
        .persist(backup_block_file_path)
        .map_err(|error| error.error)?;
    hook(BackupStep::BlockPublished)?;
    sync_directory(backup_dir_path)?;
    hook(BackupStep::BlockDirectorySynced)
}

fn stage_backup_bytes(
    file: &mut tempfile::NamedTempFile,
    bytes: &[u8],
    marker: bool,
    hook: &mut impl FnMut(BackupStep) -> io::Result<()>,
) -> io::Result<()> {
    let (first, rest) = bytes.split_at(bytes.len() / 2);
    file.write_all(first)?;
    hook(if marker {
        BackupStep::MarkerPartialWrite
    } else {
        BackupStep::BlockPartialWrite
    })?;
    file.write_all(rest)?;
    file.flush()?;
    hook(if marker {
        BackupStep::MarkerFlushed
    } else {
        BackupStep::BlockFlushed
    })?;
    file.as_file().sync_all()?;
    hook(if marker {
        BackupStep::MarkerSynced
    } else {
        BackupStep::BlockSynced
    })
}

/// Do not remove coverage until the corresponding block unlink is durable.
fn remove_backup_block(
    block_path: &Path,
    mut hook: impl FnMut(BackupStep) -> io::Result<()>,
) -> io::Result<()> {
    if cfg!(not(unix)) {
        return remove_noncovered_backup_without_directory_sync(block_path);
    }
    let directory = block_path
        .parent()
        .ok_or_else(|| io::Error::other("backup block has no directory"))?;
    hook(BackupStep::BeforeBlockRemoval)?;
    match std::fs::remove_file(block_path) {
        Ok(()) => {}
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    hook(BackupStep::BlockRemoved)?;
    sync_directory(directory)?;
    hook(BackupStep::RemovedBlockDirectorySynced)?;
    if let Some(filename) = block_path.file_name() {
        let marker_dir = directory.join(COVERAGE_DIRECTORY);
        match std::fs::remove_file(marker_dir.join(filename)) {
            Ok(()) => {
                hook(BackupStep::MarkerRemoved)?;
                sync_directory(&marker_dir)?;
                hook(BackupStep::RemovedMarkerDirectorySynced)?;
            }
            Err(error) if error.kind() == ErrorKind::NotFound => {
                // A retry may follow a successful unlink whose directory sync failed.
                if marker_dir.try_exists()? {
                    sync_directory(&marker_dir)?;
                    hook(BackupStep::RemovedMarkerDirectorySynced)?;
                }
            }
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

/// Preserve the older single-file guarantee on non-Unix targets. Atomic rename is supported,
/// but this path claims no directory durability and must never publish a covered record.
fn write_noncovered_backup_without_directory_sync(
    directory: &Path,
    network: &zebra_chain::parameters::Network,
    block: &ContextuallyVerifiedBlock,
) -> io::Result<()> {
    if coverage(network, block.height, block.hash).is_some() {
        return Err(io::Error::new(
            ErrorKind::Unsupported,
            "covered non-finalized backups require durable directory publication support",
        ));
    }
    let path = directory.join(block.hash.encode_hex::<String>());
    let bytes = NonFinalizedBlockBackup::from(block).as_bytes();
    if std::fs::read(&path).is_ok_and(|actual| actual == bytes) {
        return Ok(());
    }
    let mut temporary = tempfile::Builder::new()
        .prefix(".unpublished-block-")
        .tempfile_in(directory)?;
    stage_backup_bytes(&mut temporary, &bytes, false, &mut |_| Ok(()))?;
    temporary.persist(path).map_err(|error| error.error)?;
    Ok(())
}

/// Never unlink a covered record when its paired directory durability cannot be provided.
fn remove_noncovered_backup_without_directory_sync(path: &Path) -> io::Result<()> {
    let directory = path
        .parent()
        .ok_or_else(|| io::Error::other("backup block has no directory"))?;
    let filename = path
        .file_name()
        .ok_or_else(|| io::Error::other("backup block has no filename"))?;
    if directory
        .join(COVERAGE_DIRECTORY)
        .join(filename)
        .try_exists()?
    {
        return Err(io::Error::new(
            ErrorKind::Unsupported,
            "covered non-finalized backups require durable directory publication support",
        ));
    }
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

#[cfg(unix)]
fn sync_directory(directory: &Path) -> io::Result<()> {
    std::fs::File::open(directory)?.sync_all()
}

#[cfg(not(unix))]
fn sync_directory(_directory: &Path) -> io::Result<()> {
    // std does not provide durable directory publication on these targets. Do not silently
    // publish a covered block while claiming that its sidecar is durable.
    Err(io::Error::new(
        ErrorKind::Unsupported,
        "durable non-finalized backup publication requires directory sync support",
    ))
}

fn backup_marker(
    network: &zebra_chain::parameters::Network,
    height: Height,
    hash: block::Hash,
    bytes: &[u8],
) -> Option<Vec<u8>> {
    let mut marker = coverage(network, height, hash)?;
    // Header hashes alone do not bind the serialized transactions or deferred pool balance.
    marker.extend_from_slice(&Sha256::digest(bytes));
    Some(marker)
}

/// Refuse uncovered history before constructing trusted non-finalized state from any backups.
fn check_backup_coverage(
    backup_dir_path: &Path,
    network: &zebra_chain::parameters::Network,
    block: &SemanticallyVerifiedBlock,
    bytes: &[u8],
) -> Result<(), StateInitError> {
    let Some(expected) = backup_marker(network, block.height, block.hash, bytes) else {
        return Ok(());
    };
    let filename = block.hash.encode_hex::<String>();
    let marker_path = backup_dir_path.join(COVERAGE_DIRECTORY).join(filename);
    let valid = std::fs::read(marker_path).is_ok_and(|actual| actual == expected);
    if !valid {
        return Err(StateInitError::UncoveredFoundationHistory {
            path: backup_dir_path.to_path_buf(),
            height: block.height,
            hash: block.hash,
        });
    }
    Ok(())
}

/// Reads blocks from the provided non-finalized state backup directory path.
///
/// Returns any blocks that are valid and not present in the finalized state.
fn read_non_finalized_blocks_from_backup<'a>(
    backup_dir_path: &Path,
    finalized_state: &'a ZebraDb,
) -> impl Iterator<Item = (SemanticallyVerifiedBlock, Vec<u8>)> + 'a {
    list_backup_dir_entries(backup_dir_path)
        // It's okay to leave the file here, the backup task will delete it as long as
        // the block is not added to the non-finalized state.
        .filter(|&(block_hash, _)| !finalized_state.contains_hash(block_hash))
        .filter_map(|(block_hash, file_path)| match std::fs::read(file_path) {
            Ok(block_bytes) => Some((block_hash, block_bytes)),
            Err(err) => {
                tracing::warn!(?err, "failed to open non-finalized state backup block file");
                None
            }
        })
        .filter_map(|(expected_block_hash, backup_block_file_contents)| {
            match NonFinalizedBlockBackup::from_bytes(&backup_block_file_contents) {
                Ok(NonFinalizedBlockBackup {
                    block,
                    deferred_pool_balance_change: _,
                }) if block.coinbase_height().is_some() => {
                    let block = SemanticallyVerifiedBlock::from(block);
                    if block.hash != expected_block_hash {
                        tracing::warn!(
                            block_hash = ?block.hash,
                            ?expected_block_hash,
                            "wrong block hash in file name"
                        );
                    }
                    Some((block, backup_block_file_contents))
                }
                Ok(block) => {
                    tracing::warn!(
                        ?block,
                        "invalid non-finalized backup block, missing coinbase height"
                    );
                    None
                }
                Err(err) => {
                    tracing::warn!(
                        ?err,
                        "failed to deserialize non-finalized backup data into block"
                    );
                    None
                }
            }
        })
}

/// Accepts a backup directory path, opens the directory, converts its entries
/// filenames to block hashes, and deletes any entries with invalid file names.
///
/// # Panics
///
/// If the provided path cannot be opened as a directory.
/// See [`read_backup_dir`] for more details.
pub(super) fn list_backup_dir_entries(
    backup_dir_path: &Path,
) -> impl Iterator<Item = (block::Hash, PathBuf)> {
    read_backup_dir(backup_dir_path).filter_map(process_backup_dir_entry)
}

/// Accepts a backup directory path and opens the directory.
///
/// Returns an iterator over all [`DirEntry`]s in the directory that are successfully read.
///
/// # Panics
///
/// If the provided path cannot be opened as a directory.
fn read_backup_dir(backup_dir_path: &Path) -> impl Iterator<Item = DirEntry> {
    std::fs::read_dir(backup_dir_path)
        .expect("failed to read non-finalized state backup directory")
        .filter_map(|entry| match entry {
            Ok(entry) => Some(entry),
            Err(io_err) => {
                tracing::warn!(
                    ?io_err,
                    "failed to read DirEntry in non-finalized state backup dir"
                );

                None
            }
        })
}

/// Accepts a [`DirEntry`] from the non-finalized state backup directory and
/// parses the filename into a block hash.
///
/// Returns the block hash and the file path if successful, or
/// returns None and deletes the file at the entry path otherwise.
fn process_backup_dir_entry(entry: DirEntry) -> Option<(block::Hash, PathBuf)> {
    if entry.file_name() == COVERAGE_DIRECTORY || entry.file_name() == UNPUBLISHED_DIRECTORY {
        return None;
    }
    let delete_file = || {
        if let Err(delete_error) = std::fs::remove_file(entry.path()) {
            tracing::warn!(?delete_error, "failed to delete backup block file");
        }
    };

    let block_file_name = match entry.file_name().into_string() {
        Ok(block_hash) => block_hash,
        Err(err) => {
            tracing::warn!(
                ?err,
                "failed to convert OsString to String, attempting to delete file"
            );

            delete_file();
            return None;
        }
    };

    let block_hash: block::Hash = match block_file_name.parse() {
        Ok(block_hash) => block_hash,
        Err(err) => {
            tracing::warn!(
                ?err,
                "failed to parse hex-encoded block hash from file name, attempting to delete file"
            );

            delete_file();
            return None;
        }
    };

    Some((block_hash, entry.path()))
}

#[cfg(test)]
mod tests {
    use zebra_chain::{
        parameters::{subsidy::wcash_foundation_activation_height, Network},
        transparent,
    };

    use crate::{
        arbitrary::Prepare,
        service::finalized_state::FinalizedState,
        tests::setup::{test_genesis, uses_wcash_consensus, wcash_fake_children},
        Config,
    };

    use super::*;

    fn policy_network() -> Network {
        let fixture = Network::new_wcash_regtest();
        if wcash_foundation_activation_height(&fixture).is_some() {
            fixture
        } else {
            Network::try_new_wcash_mainnet().unwrap()
        }
    }

    fn synthetic_block(network: &Network, height: Height) -> SemanticallyVerifiedBlock {
        let mut block = wcash_fake_children(network, 1).remove(0).as_ref().clone();
        let coinbase = block.transactions[0]
            .as_ref()
            .clone()
            .with_transparent_inputs(vec![transparent::Input::Coinbase {
                height,
                data: b"foundation backup guard".to_vec(),
                sequence: u32::MAX,
            }]);
        block.transactions[0] = Arc::new(coinbase);
        Arc::make_mut(&mut block.header).nonce.0 = [height.0.to_le_bytes()[0]; 32];
        SemanticallyVerifiedBlock::from(Arc::new(block))
    }

    fn write_legacy_backup(path: &Path, block: &SemanticallyVerifiedBlock) -> Vec<u8> {
        let bytes = NonFinalizedBlockBackup {
            block: block.block.clone(),
            deferred_pool_balance_change: Amount::zero(),
        }
        .as_bytes();
        std::fs::write(path.join(block.hash.encode_hex::<String>()), &bytes).unwrap();
        bytes
    }

    fn write_test_marker(
        path: &Path,
        network: &Network,
        block: &SemanticallyVerifiedBlock,
        bytes: &[u8],
    ) {
        let mut marker = coverage(network, block.height, block.hash).unwrap();
        marker.extend_from_slice(&Sha256::digest(bytes));
        let marker_dir = path.join(COVERAGE_DIRECTORY);
        std::fs::create_dir_all(&marker_dir).unwrap();
        std::fs::write(marker_dir.join(block.hash.encode_hex::<String>()), marker).unwrap();
    }

    /// Independently encode coverage with a missing or explicit allocation ratio.
    fn write_rule_marker_fixture(
        path: &Path,
        network: &Network,
        block: &SemanticallyVerifiedBlock,
        bytes: &[u8],
        ratio: Option<(u64, u64)>,
        activation: Height,
    ) -> Vec<u8> {
        let mut marker = if let Some((numerator, denominator)) = ratio {
            let mut marker = b"wcash-foundation-fund-validation-v2\0".to_vec();
            marker.extend_from_slice(&numerator.to_be_bytes());
            marker.extend_from_slice(&denominator.to_be_bytes());
            marker
        } else {
            b"wcash-foundation-fund-validation-v1\0".to_vec()
        };
        marker.extend_from_slice(&activation.0.to_be_bytes());
        marker.extend_from_slice(
            &zebra_chain::parameters::subsidy::wcash_foundation_expiry_height(network)
                .unwrap()
                .0
                .to_be_bytes(),
        );
        marker.extend_from_slice(&block.height.0.to_be_bytes());
        marker.extend_from_slice(&block.hash.0);
        marker.extend_from_slice(&Sha256::digest(bytes));
        let marker_dir = path.join(COVERAGE_DIRECTORY);
        std::fs::create_dir_all(&marker_dir).unwrap();
        std::fs::write(marker_dir.join(block.hash.encode_hex::<String>()), &marker).unwrap();
        marker
    }

    #[cfg(unix)]
    fn fixture_states() -> (
        Network,
        FinalizedState,
        NonFinalizedState,
        NonFinalizedState,
    ) {
        let network = Network::new_wcash_regtest();
        let mut finalized = FinalizedState::new_with_debug(
            &Config::ephemeral(),
            &network,
            true,
            #[cfg(feature = "elasticsearch")]
            false,
            false,
        )
        .unwrap();
        finalized
            .commit_finalized_direct(
                test_genesis(&network).into(),
                None,
                "backup publication test",
            )
            .unwrap();
        let children = wcash_fake_children(&network, 10);
        let mut state = NonFinalizedState::new(&network);
        state
            .commit_new_chain(children[0].clone().prepare(), &finalized.db)
            .unwrap();
        for child in children.iter().take(9).skip(1) {
            state
                .commit_block(child.clone().prepare(), &finalized.db)
                .unwrap();
        }
        let prefix = state.clone();
        state
            .commit_block(children[9].clone().prepare(), &finalized.db)
            .unwrap();
        (network, finalized, prefix, state)
    }

    #[cfg(unix)]
    fn has_fixture() -> bool {
        uses_wcash_consensus()
            && wcash_foundation_activation_height(&Network::new_wcash_regtest()).is_some()
    }

    #[test]
    #[cfg(unix)]
    fn foundation_non_unix_fallback_preserves_noncovered_behavior_and_refuses_covered_mutation() {
        let _init_guard = zebra_test::init();
        if !has_fixture() {
            return;
        }
        let (network, _finalized, prefix, state) = fixture_states();
        let prefix_chain = prefix.best_chain().unwrap();
        let ordinary = &prefix_chain.blocks[&Height(9)];
        let covered_chain = state.best_chain().unwrap();
        let covered = &covered_chain.blocks[&Height(10)];
        let dir = tempfile::tempdir().unwrap();
        write_noncovered_backup_without_directory_sync(dir.path(), &network, ordinary).unwrap();
        let ordinary_path = dir.path().join(ordinary.hash.encode_hex::<String>());
        assert_eq!(
            std::fs::read(&ordinary_path).unwrap(),
            NonFinalizedBlockBackup::from(ordinary).as_bytes()
        );
        remove_noncovered_backup_without_directory_sync(&ordinary_path).unwrap();
        assert!(!ordinary_path.exists());
        let error = write_noncovered_backup_without_directory_sync(dir.path(), &network, covered)
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Unsupported);
        let covered_path = dir.path().join(covered.hash.encode_hex::<String>());
        assert!(!covered_path.exists());
        write_backup_block(dir.path(), &network, covered).unwrap();
        let error = remove_noncovered_backup_without_directory_sync(&covered_path).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Unsupported);
        assert!(covered_path.is_file());
        assert!(dir
            .path()
            .join(COVERAGE_DIRECTORY)
            .join(covered.hash.encode_hex::<String>())
            .is_file());
    }

    #[test]
    #[cfg(unix)]
    fn foundation_backup_publication_interruptions_preserve_restartable_prefix_and_retry() {
        let _init_guard = zebra_test::init();
        if !has_fixture() {
            return;
        }
        let (network, finalized, prefix, state) = fixture_states();
        let chain = state.best_chain().unwrap();
        let block = &chain.blocks[&Height(10)];
        let steps = [
            BackupStep::BlockTemporaryCreated,
            BackupStep::BlockPartialWrite,
            BackupStep::BlockFlushed,
            BackupStep::BlockSynced,
            BackupStep::BeforeCoverageDirectoryCreation,
            BackupStep::CoverageDirectoryCreated,
            BackupStep::CoverageParentSynced,
            BackupStep::MarkerTemporaryCreated,
            BackupStep::MarkerPartialWrite,
            BackupStep::MarkerFlushed,
            BackupStep::MarkerSynced,
            BackupStep::MarkerPublished,
            BackupStep::MarkerDirectorySynced,
            BackupStep::BlockPublished,
            BackupStep::BlockDirectorySynced,
        ];
        for failed_step in steps {
            let dir = tempfile::tempdir().unwrap();
            assert!(update_non_finalized_state_backup(
                dir.path(),
                &prefix,
                HashMap::new()
            ));
            let mut reached = false;
            let result = write_backup_block_with_hook(dir.path(), &network, block, |step| {
                if step != failed_step {
                    return Ok(());
                }
                reached = true;
                // Model temporary files left behind by an abrupt exit, including partial bytes.
                if matches!(
                    step,
                    BackupStep::BlockPartialWrite | BackupStep::MarkerPartialWrite
                ) {
                    let temporary_directory = if step == BackupStep::BlockPartialWrite {
                        dir.path().to_path_buf()
                    } else {
                        dir.path().join(COVERAGE_DIRECTORY)
                    };
                    let temporary = std::fs::read_dir(&temporary_directory)
                        .unwrap()
                        .map(Result::unwrap)
                        .find(|entry| {
                            entry
                                .file_name()
                                .to_string_lossy()
                                .starts_with(".unpublished-")
                        })
                        .unwrap();
                    std::fs::copy(
                        temporary.path(),
                        temporary_directory.join(".interrupted-partial-record"),
                    )
                    .unwrap();
                }
                Err(io::Error::other("injected publication interruption"))
            });
            assert!(reached && result.is_err(), "{failed_step:?}");
            let restored =
                restore_backup(NonFinalizedState::new(&network), dir.path(), &finalized.db)
                    .unwrap();
            let published = matches!(
                failed_step,
                BackupStep::BlockPublished | BackupStep::BlockDirectorySynced
            );
            assert!(
                restored.eq_internal_state(if published { &state } else { &prefix }),
                "{failed_step:?}"
            );
            let mut retry_steps = Vec::new();
            write_backup_block_with_hook(dir.path(), &network, block, |step| {
                retry_steps.push(step);
                Ok(())
            })
            .unwrap();
            assert!(
                retry_steps.contains(&BackupStep::BlockDirectorySynced),
                "retry must finish directory sync after {failed_step:?}"
            );
            let restored =
                restore_backup(NonFinalizedState::new(&network), dir.path(), &finalized.db)
                    .unwrap();
            assert!(restored.eq_internal_state(&state), "{failed_step:?}");
        }
    }

    #[test]
    #[cfg(unix)]
    fn foundation_backup_directory_failure_and_existing_record_reuse() {
        use std::os::unix::fs::PermissionsExt;
        let _init_guard = zebra_test::init();
        if !has_fixture() {
            return;
        }
        let (network, finalized, prefix, state) = fixture_states();
        let chain = state.best_chain().unwrap();
        let block = &chain.blocks[&Height(10)];
        let dir = tempfile::tempdir().unwrap();
        assert!(update_non_finalized_state_backup(
            dir.path(),
            &prefix,
            HashMap::new()
        ));
        let marker_dir = dir.path().join(COVERAGE_DIRECTORY);
        // A file at the directory name deterministically fails directory creation on all users.
        std::fs::write(&marker_dir, b"blocked coverage directory").unwrap();
        assert!(write_backup_block(dir.path(), &network, block).is_err());
        assert!(!dir.path().join(block.hash.encode_hex::<String>()).exists());
        assert!(
            restore_backup(NonFinalizedState::new(&network), dir.path(), &finalized.db)
                .unwrap()
                .eq_internal_state(&prefix)
        );
        std::fs::remove_file(&marker_dir).unwrap();
        write_backup_block(dir.path(), &network, block).unwrap();
        std::fs::set_permissions(&marker_dir, std::fs::Permissions::from_mode(0o500)).unwrap();
        let mut reused = Vec::new();
        let result = write_backup_block_with_hook(dir.path(), &network, block, |step| {
            reused.push(step);
            Ok(())
        });
        std::fs::set_permissions(&marker_dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        result.unwrap();
        assert_eq!(
            reused,
            [
                BackupStep::MarkerDirectorySynced,
                BackupStep::BlockDirectorySynced
            ]
        );
        assert!(
            restore_backup(NonFinalizedState::new(&network), dir.path(), &finalized.db)
                .unwrap()
                .eq_internal_state(&state)
        );
    }

    #[test]
    #[cfg(unix)]
    fn foundation_backup_repairs_only_trusted_memory_and_preserves_withdrawn_bytes() {
        let _init_guard = zebra_test::init();
        if !has_fixture() {
            return;
        }
        let (network, finalized, prefix, state) = fixture_states();
        let chain = state.best_chain().unwrap();
        let block = &chain.blocks[&Height(10)];
        let semantic = SemanticallyVerifiedBlock::from(block.clone());
        let trusted_bytes = NonFinalizedBlockBackup::from(block).as_bytes();
        for altered in [false, true] {
            for failed_step in [
                BackupStep::ExistingBlockWithdrawn,
                BackupStep::WithdrawalDirectorySynced,
                BackupStep::MarkerPublished,
            ] {
                let dir = tempfile::tempdir().unwrap();
                assert!(update_non_finalized_state_backup(
                    dir.path(),
                    &prefix,
                    HashMap::new()
                ));
                let mut existing = write_legacy_backup(dir.path(), &semantic);
                if altered {
                    existing[0] ^= 1;
                    std::fs::write(
                        dir.path().join(block.hash.encode_hex::<String>()),
                        &existing,
                    )
                    .unwrap();
                    // A previously paired different record must be withdrawn before marker replacement.
                    write_test_marker(dir.path(), &network, &semantic, &existing);
                } else {
                    assert!(restore_backup(
                        NonFinalizedState::new(&network),
                        dir.path(),
                        &finalized.db
                    )
                    .is_err());
                }
                let mut reached = false;
                assert!(
                    write_backup_block_with_hook(dir.path(), &network, block, |step| {
                        if step == failed_step {
                            reached = true;
                            Err(io::Error::other("injected repair interruption"))
                        } else {
                            Ok(())
                        }
                    })
                    .is_err()
                );
                assert!(reached);
                assert!(restore_backup(
                    NonFinalizedState::new(&network),
                    dir.path(),
                    &finalized.db
                )
                .unwrap()
                .eq_internal_state(&prefix));
                let withdrawn = std::fs::read_dir(dir.path().join(UNPUBLISHED_DIRECTORY))
                    .unwrap()
                    .next()
                    .unwrap()
                    .unwrap();
                assert_eq!(std::fs::read(withdrawn.path()).unwrap(), existing);
                write_backup_block(dir.path(), &network, block).unwrap();
                assert_eq!(
                    std::fs::read(dir.path().join(block.hash.encode_hex::<String>())).unwrap(),
                    trusted_bytes
                );
                assert!(restore_backup(
                    NonFinalizedState::new(&network),
                    dir.path(),
                    &finalized.db
                )
                .unwrap()
                .eq_internal_state(&state));
            }
        }
    }

    #[test]
    #[cfg(unix)]
    fn foundation_backup_cleanup_interruptions_keep_coverage_until_block_unlink_is_durable() {
        let _init_guard = zebra_test::init();
        if !has_fixture() {
            return;
        }
        let (network, finalized, prefix, state) = fixture_states();
        let chain = state.best_chain().unwrap();
        let block = &chain.blocks[&Height(10)];
        for failed_step in [
            BackupStep::BeforeBlockRemoval,
            BackupStep::BlockRemoved,
            BackupStep::RemovedBlockDirectorySynced,
            BackupStep::MarkerRemoved,
            BackupStep::RemovedMarkerDirectorySynced,
        ] {
            let dir = tempfile::tempdir().unwrap();
            assert!(update_non_finalized_state_backup(
                dir.path(),
                &state,
                HashMap::new()
            ));
            let block_path = dir.path().join(block.hash.encode_hex::<String>());
            let marker_path = dir
                .path()
                .join(COVERAGE_DIRECTORY)
                .join(block.hash.encode_hex::<String>());
            assert!(remove_backup_block(&block_path, |step| {
                if step == failed_step {
                    Err(io::Error::new(
                        ErrorKind::PermissionDenied,
                        "injected unlink/sync failure",
                    ))
                } else {
                    Ok(())
                }
            })
            .is_err());
            if matches!(
                failed_step,
                BackupStep::BeforeBlockRemoval
                    | BackupStep::BlockRemoved
                    | BackupStep::RemovedBlockDirectorySynced
            ) {
                assert!(marker_path.is_file());
            }
            let restored =
                restore_backup(NonFinalizedState::new(&network), dir.path(), &finalized.db)
                    .unwrap();
            assert!(
                restored.eq_internal_state(if failed_step == BackupStep::BeforeBlockRemoval {
                    &state
                } else {
                    &prefix
                })
            );
            remove_backup_block(&block_path, |_| Ok(())).unwrap();
            assert!(!block_path.exists() && !marker_path.exists());
        }
    }

    #[test]
    #[cfg(unix)]
    fn foundation_backup_update_retains_failed_removals_and_cancels_when_desired_again() {
        let _init_guard = zebra_test::init();
        if !has_fixture() {
            return;
        }
        let (network, finalized, prefix, state) = fixture_states();
        let hash = state.best_chain().unwrap().blocks[&Height(10)].hash;
        for failed_step in [
            BackupStep::BlockRemoved,
            BackupStep::RemovedBlockDirectorySynced,
            BackupStep::MarkerRemoved,
        ] {
            for reappears in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let mut cache = HashMap::new();
                let mut pending = HashMap::new();
                assert!(update_backup_with_cache(
                    dir.path(),
                    &state,
                    HashMap::new(),
                    &mut cache,
                    &mut pending,
                    |_| Ok(())
                ));
                assert!(!update_backup_with_cache(
                    dir.path(),
                    &prefix,
                    list_backup_dir_entries(dir.path()).collect(),
                    &mut cache,
                    &mut pending,
                    |step| {
                        if step == failed_step {
                            Err(io::Error::other("injected cleanup failure"))
                        } else {
                            Ok(())
                        }
                    }
                ));
                assert_eq!(pending.len(), 1);
                assert!(!dir.path().join(hash.encode_hex::<String>()).exists());
                // Production's next directory listing cannot rediscover the removed block.
                let listed: HashMap<_, _> = list_backup_dir_entries(dir.path()).collect();
                assert!(!listed.contains_key(&hash));
                let desired = if reappears { &state } else { &prefix };
                assert!(update_backup_with_cache(
                    dir.path(),
                    desired,
                    listed,
                    &mut cache,
                    &mut pending,
                    |_| Ok(())
                ));
                assert!(pending.is_empty());
                assert_eq!(
                    dir.path()
                        .join(COVERAGE_DIRECTORY)
                        .join(hash.encode_hex::<String>())
                        .is_file(),
                    reappears
                );
                assert!(restore_backup(
                    NonFinalizedState::new(&network),
                    dir.path(),
                    &finalized.db
                )
                .unwrap()
                .eq_internal_state(desired));
            }
        }
    }

    #[test]
    #[cfg(unix)]
    fn foundation_backup_cache_rechecks_replaced_files_and_missing_markers() {
        let _init_guard = zebra_test::init();
        if !has_fixture() {
            return;
        }
        let (network, finalized, _prefix, state) = fixture_states();
        let chain = state.best_chain().unwrap();
        let block = &chain.blocks[&Height(10)];
        let dir = tempfile::tempdir().unwrap();
        let mut cache = HashMap::new();
        assert!(update_backup_with_cache(
            dir.path(),
            &state,
            HashMap::new(),
            &mut cache,
            &mut HashMap::new(),
            |_| Ok(()),
        ));
        assert_eq!(cache.len(), 10);
        let block_path = dir.path().join(block.hash.encode_hex::<String>());
        let marker_path = dir
            .path()
            .join(COVERAGE_DIRECTORY)
            .join(block.hash.encode_hex::<String>());
        std::fs::remove_file(&marker_path).unwrap();
        assert!(update_backup_with_cache(
            dir.path(),
            &state,
            list_backup_dir_entries(dir.path()).collect(),
            &mut cache,
            &mut HashMap::new(),
            |_| Ok(()),
        ));
        assert!(marker_path.is_file());
        let mut altered = NonFinalizedBlockBackup::from(block).as_bytes();
        altered[0] ^= 1;
        let replacement = dir.path().join("replacement");
        std::fs::write(&replacement, altered).unwrap();
        std::fs::rename(replacement, &block_path).unwrap();
        assert!(update_backup_with_cache(
            dir.path(),
            &state,
            list_backup_dir_entries(dir.path()).collect(),
            &mut cache,
            &mut HashMap::new(),
            |_| Ok(()),
        ));
        assert!(
            restore_backup(NonFinalizedState::new(&network), dir.path(), &finalized.db)
                .unwrap()
                .eq_internal_state(&state)
        );
        assert!(update_backup_with_cache(
            dir.path(),
            &NonFinalizedState::new(&network),
            list_backup_dir_entries(dir.path()).collect(),
            &mut cache,
            &mut HashMap::new(),
            |_| Ok(()),
        ));
        assert!(cache.is_empty());
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn foundation_backup_task_retries_without_another_tip_change() {
        let _init_guard = zebra_test::init();
        if !has_fixture() {
            return;
        }
        let (network, finalized, prefix, state) = fixture_states();
        let dir = tempfile::tempdir().unwrap();
        assert!(update_non_finalized_state_backup(
            dir.path(),
            &prefix,
            HashMap::new()
        ));
        let marker_dir = dir.path().join(COVERAGE_DIRECTORY);
        std::fs::write(&marker_dir, b"temporarily blocked").unwrap();
        let (sender, receiver) = tokio::sync::watch::channel(prefix.clone());
        let (reports, mut updates) = tokio::sync::mpsc::unbounded_channel();
        let task = tokio::spawn(run_backup_task_with_report(
            WatchReceiver::new(receiver),
            dir.path().to_path_buf(),
            move |complete| {
                let _ = reports.send(complete);
            },
        ));
        sender.send(state.clone()).unwrap();
        // The first pass must fail, and leave only a restartable covered prefix.
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(20), updates.recv())
                .await
                .unwrap(),
            Some(false)
        );
        assert!(
            restore_backup(NonFinalizedState::new(&network), dir.path(), &finalized.db)
                .unwrap()
                .eq_internal_state(&prefix)
        );
        std::fs::remove_file(&marker_dir).unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(20), updates.recv())
                .await
                .unwrap(),
            Some(true)
        );
        drop(sender);
        tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .unwrap()
            .unwrap();
        assert!(
            restore_backup(NonFinalizedState::new(&network), dir.path(), &finalized.db)
                .unwrap()
                .eq_internal_state(&state)
        );
    }

    #[test]
    fn foundation_backup_guard_refuses_legacy_and_old_appends() {
        let _init_guard = zebra_test::init();
        if !uses_wcash_consensus() {
            return;
        }
        let network = policy_network();
        let activation = wcash_foundation_activation_height(&network).unwrap();
        let finalized = FinalizedState::new_with_debug(
            &Config::ephemeral(),
            &network,
            true,
            #[cfg(feature = "elasticsearch")]
            false,
            false,
        )
        .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let before = synthetic_block(&network, activation.previous().unwrap());
        let before_bytes = write_legacy_backup(dir.path(), &before);
        check_backup_coverage(dir.path(), &network, &before, &before_bytes).unwrap();

        let first = synthetic_block(&network, activation);
        let first_bytes = write_legacy_backup(dir.path(), &first);
        let error = restore_backup(NonFinalizedState::new(&network), dir.path(), &finalized.db)
            .unwrap_err();
        assert!(
            matches!(error, StateInitError::UncoveredFoundationHistory { height, .. } if height == activation)
        );
        assert!(dir.path().join(first.hash.encode_hex::<String>()).is_file());

        write_test_marker(dir.path(), &network, &first, &first_bytes);
        check_backup_coverage(dir.path(), &network, &first, &first_bytes).unwrap();
        // A new executable's marker cannot bless the next block appended by an old executable.
        let old_append = synthetic_block(&network, activation.next().unwrap());
        write_legacy_backup(dir.path(), &old_append);
        let error = restore_backup(NonFinalizedState::new(&network), dir.path(), &finalized.db)
            .unwrap_err();
        assert!(
            matches!(error, StateInitError::UncoveredFoundationHistory { height, .. } if height == old_append.height)
        );

        // Even a header-preserving byte change cannot reuse an earlier coverage record.
        let mut changed = first_bytes.clone();
        changed[0] ^= 1;
        assert!(check_backup_coverage(dir.path(), &network, &first, &changed).is_err());
        let first_path = dir.path().join(first.hash.encode_hex::<String>());
        std::fs::write(&first_path, &changed).unwrap();
        assert!(
            restore_backup(NonFinalizedState::new(&network), dir.path(), &finalized.db).is_err()
        );
        assert_eq!(std::fs::read(&first_path).unwrap(), changed);
        std::fs::write(&first_path, &first_bytes).unwrap();
        let mut wrong_rule = std::fs::read(
            dir.path()
                .join(COVERAGE_DIRECTORY)
                .join(first.hash.encode_hex::<String>()),
        )
        .unwrap();
        wrong_rule[0] ^= 1;
        std::fs::write(
            dir.path()
                .join(COVERAGE_DIRECTORY)
                .join(first.hash.encode_hex::<String>()),
            wrong_rule,
        )
        .unwrap();
        assert!(check_backup_coverage(dir.path(), &network, &first, &first_bytes).is_err());

        let expired_height =
            zebra_chain::parameters::subsidy::wcash_foundation_expiry_height(&network).unwrap();
        let expired = synthetic_block(&network, expired_height);
        let expired_dir = tempfile::tempdir().unwrap();
        let bytes = write_legacy_backup(expired_dir.path(), &expired);
        assert!(check_backup_coverage(expired_dir.path(), &network, &expired, &bytes).is_err());
        let error = restore_backup(
            NonFinalizedState::new(&network),
            expired_dir.path(),
            &finalized.db,
        )
        .unwrap_err();
        assert!(
            matches!(error, StateInitError::UncoveredFoundationHistory { height, .. } if height == expired_height)
        );
        assert_eq!(
            std::fs::read(expired_dir.path().join(expired.hash.encode_hex::<String>())).unwrap(),
            bytes
        );
    }

    #[test]
    fn foundation_backup_rejects_incompatible_rule_coverage_on_restart() {
        let _init_guard = zebra_test::init();
        if !uses_wcash_consensus() {
            return;
        }
        let network = policy_network();
        let activation = wcash_foundation_activation_height(&network).unwrap();
        let expiry =
            zebra_chain::parameters::subsidy::wcash_foundation_expiry_height(&network).unwrap();
        let finalized = FinalizedState::new_with_debug(
            &Config::ephemeral(),
            &network,
            true,
            #[cfg(feature = "elasticsearch")]
            false,
            false,
        )
        .unwrap();

        for height in [activation, expiry] {
            for ratio in [None, Some((2, 10)), Some((3, 5))] {
                let dir = tempfile::tempdir().unwrap();
                let config = Config {
                    cache_dir: dir.path().to_path_buf(),
                    ephemeral: false,
                    should_backup_non_finalized_state: true,
                    delete_old_database: false,
                    ..Config::default()
                };
                let path = config.non_finalized_state_backup_dir(&network).unwrap();
                std::fs::create_dir_all(&path).unwrap();
                let block = synthetic_block(&network, height);
                let bytes = write_legacy_backup(&path, &block);
                let marker =
                    write_rule_marker_fixture(&path, &network, &block, &bytes, ratio, activation);
                assert!(matches!(
                    check_foundation_backup_coverage(&config, &network, None),
                    Err(StateInitError::UncoveredFoundationHistory { .. })
                ));
                assert!(matches!(
                    restore_backup(NonFinalizedState::new(&network), &path, &finalized.db),
                    Err(StateInitError::UncoveredFoundationHistory { .. })
                ));
                let filename = block.hash.encode_hex::<String>();
                assert_eq!(std::fs::read(path.join(&filename)).unwrap(), bytes);
                assert_eq!(
                    std::fs::read(path.join(COVERAGE_DIRECTORY).join(filename)).unwrap(),
                    marker
                );
                write_rule_marker_fixture(
                    &path,
                    &network,
                    &block,
                    &bytes,
                    Some((3, 10)),
                    activation,
                );
                check_foundation_backup_coverage(&config, &network, None).unwrap();
                check_backup_coverage(&path, &network, &block, &bytes).unwrap();
            }
        }

        // An obsolete sidecar below activation does not make unchanged legacy history unsafe.
        let dir = tempfile::tempdir().unwrap();
        let config = Config {
            cache_dir: dir.path().to_path_buf(),
            ephemeral: false,
            should_backup_non_finalized_state: true,
            delete_old_database: false,
            ..Config::default()
        };
        let path = config.non_finalized_state_backup_dir(&network).unwrap();
        std::fs::create_dir_all(&path).unwrap();
        let block = synthetic_block(&network, activation.previous().unwrap());
        let bytes = write_legacy_backup(&path, &block);
        write_rule_marker_fixture(&path, &network, &block, &bytes, None, activation);
        check_foundation_backup_coverage(&config, &network, None).unwrap();
        check_backup_coverage(&path, &network, &block, &bytes).unwrap();
    }

    #[test]
    fn foundation_backup_rejects_mismatched_activation_on_restart() {
        let _init_guard = zebra_test::init();
        if !uses_wcash_consensus() {
            return;
        }
        let network = Network::try_new_wcash_mainnet().unwrap();
        assert_eq!(
            wcash_foundation_activation_height(&network),
            Some(Height(33_000))
        );
        let incorrect_activation = Height(40_001);
        let expiry =
            zebra_chain::parameters::subsidy::wcash_foundation_expiry_height(&network).unwrap();
        let finalized = FinalizedState::new_with_debug(
            &Config::ephemeral(),
            &network,
            true,
            #[cfg(feature = "elasticsearch")]
            false,
            false,
        )
        .unwrap();

        for height in [incorrect_activation, expiry] {
            let dir = tempfile::tempdir().unwrap();
            let config = Config {
                cache_dir: dir.path().to_path_buf(),
                ephemeral: false,
                should_backup_non_finalized_state: true,
                delete_old_database: false,
                ..Config::default()
            };
            let path = config.non_finalized_state_backup_dir(&network).unwrap();
            std::fs::create_dir_all(&path).unwrap();
            let block = synthetic_block(&network, height);
            let bytes = write_legacy_backup(&path, &block);
            let marker = write_rule_marker_fixture(
                &path,
                &network,
                &block,
                &bytes,
                Some((3, 10)),
                incorrect_activation,
            );
            assert!(matches!(
                check_foundation_backup_coverage(&config, &network, None),
                Err(StateInitError::UncoveredFoundationHistory { .. })
            ));
            assert!(matches!(
                restore_backup(NonFinalizedState::new(&network), &path, &finalized.db),
                Err(StateInitError::UncoveredFoundationHistory { .. })
            ));
            let filename = block.hash.encode_hex::<String>();
            assert_eq!(std::fs::read(path.join(&filename)).unwrap(), bytes);
            assert_eq!(
                std::fs::read(path.join(COVERAGE_DIRECTORY).join(filename)).unwrap(),
                marker
            );
        }
    }

    #[test]
    fn foundation_backup_preflight_preserves_files_before_database_creation() {
        let _init_guard = zebra_test::init();
        if !uses_wcash_consensus() {
            return;
        }
        let network = policy_network();
        let activation = wcash_foundation_activation_height(&network).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let config = Config {
            cache_dir: dir.path().to_path_buf(),
            ephemeral: false,
            should_backup_non_finalized_state: true,
            delete_old_database: false,
            ..Config::default()
        };
        let path = config.non_finalized_state_backup_dir(&network).unwrap();
        std::fs::create_dir_all(&path).unwrap();
        let sentinel = path.join("preserve-malformed-legacy-file");
        std::fs::write(&sentinel, b"operator evidence").unwrap();
        let old = synthetic_block(&network, activation);
        write_legacy_backup(&path, &old);
        let result = FinalizedState::new_with_debug(
            &config,
            &network,
            true,
            #[cfg(feature = "elasticsearch")]
            false,
            false,
        );
        assert!(matches!(
            result,
            Err(StateInitError::UncoveredFoundationHistory { .. })
        ));
        assert!(
            sentinel.is_file(),
            "preflight must not delete even malformed legacy entries"
        );
        assert!(!config
            .db_path(
                crate::constants::STATE_DATABASE_KIND,
                crate::constants::state_database_format_version_in_code().major,
                &network
            )
            .exists());
    }

    #[test]
    fn foundation_fixture_backup_write_restore_and_cleanup() {
        let _init_guard = zebra_test::init();
        let network = Network::new_wcash_regtest();
        if !uses_wcash_consensus() || wcash_foundation_activation_height(&network).is_none() {
            return;
        }
        let mut finalized = FinalizedState::new_with_debug(
            &Config::ephemeral(),
            &network,
            true,
            #[cfg(feature = "elasticsearch")]
            false,
            false,
        )
        .unwrap();
        finalized
            .commit_finalized_direct(
                test_genesis(&network).into(),
                None,
                "foundation backup test",
            )
            .unwrap();
        let children = wcash_fake_children(&network, 21);
        let mut state = NonFinalizedState::new(&network);
        state
            .commit_new_chain(children[0].clone().prepare(), &finalized.db)
            .unwrap();
        for child in children.iter().skip(1) {
            state
                .commit_block(child.clone().prepare(), &finalized.db)
                .unwrap();
        }
        let dir = tempfile::tempdir().unwrap();
        update_non_finalized_state_backup(dir.path(), &state, HashMap::new());
        let restored =
            restore_backup(NonFinalizedState::new(&network), dir.path(), &finalized.db).unwrap();
        assert!(state.eq_internal_state(&restored));
        assert_eq!(
            std::fs::read_dir(dir.path().join(COVERAGE_DIRECTORY))
                .unwrap()
                .count(),
            12
        );

        let old_append = synthetic_block(&network, Height(22));
        write_legacy_backup(dir.path(), &old_append);
        assert!(
            restore_backup(NonFinalizedState::new(&network), dir.path(), &finalized.db).is_err()
        );

        let backup_blocks = list_backup_dir_entries(dir.path()).collect();
        update_non_finalized_state_backup(
            dir.path(),
            &NonFinalizedState::new(&network),
            backup_blocks,
        );
        assert_eq!(
            std::fs::read_dir(dir.path().join(COVERAGE_DIRECTORY))
                .unwrap()
                .count(),
            0
        );
    }
}
