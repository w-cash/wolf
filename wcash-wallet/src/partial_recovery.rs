//! Durable hints for bounded partial-transparent history recovery.
//!
//! A saved prefix is never evidence by itself. The caller must reattest its
//! canonical block hash and recover every required spend witness before using
//! the prefix to skip history. Save only after a complete history chunk has
//! been verified and stored under the wallet's exclusive operation lock.
//!
//! Separate witness receipts retain exact transaction digests and a canonical
//! prefix anchor across retries and successful scans. They become usable only
//! after fresh anchor attestation and full transaction/input validation. Checked
//! candidate heights merely avoid duplicate queries and never prove a spend.

use rusqlite::{params, OptionalExtension};
use sha2::{Digest, Sha256};
use zcash_client_sqlite::ExtensionTransaction;
use zcash_protocol::{constants::MAX_BLOCK_BYTES, TxId};

use crate::{
    identity::{
        PARTIAL_TRANSPARENT_CHECKED_CANDIDATE_TABLE, PARTIAL_TRANSPARENT_RECOVERY_TABLE,
        PARTIAL_TRANSPARENT_WITNESS_GENERATION_TABLE, PARTIAL_TRANSPARENT_WITNESS_TABLE,
    },
    BlockRef, WalletDatabase, WalletServiceError, MAX_COINBASE_ADDRESS_INDEX,
};

// Match the reconciliation's per-receiver tracked-output bound. A transaction
// can witness several outputs, so distinct witness transactions cannot exceed it.
const MAX_CACHED_WITNESSES: usize = 2_000_000;
const MAX_WITNESS_PAGE: usize = 256;

/// Metadata for an exact mined transaction previously verified by recovery.
///
/// This is usable only after the caller freshly reattests the generation's
/// canonical anchor, then parses the raw bytes and observes its exact inputs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CachedTransparentWitness {
    pub(crate) txid: TxId,
    pub(crate) height: u32,
    pub(crate) raw_digest: [u8; 32],
}

struct WitnessGeneration {
    digest: [u8; 32],
    anchor: BlockRef,
    count: usize,
    checked_count: usize,
}

/// Loads bounded metadata for this exact receiver-output generation.
///
/// Async recovery should use [`load_witnesses_page`] with deadline checks
/// between pages. No transaction bytes are allocated by this metadata lookup.
#[cfg(test)]
pub(crate) fn load_witnesses(
    wallet: &mut WalletDatabase,
    child_index: u32,
    digest: [u8; 32],
) -> Result<Option<(BlockRef, Vec<CachedTransparentWitness>)>, WalletServiceError> {
    let mut all = Vec::new();
    let mut after = None;
    loop {
        let Some((anchor, page)) =
            load_witnesses_page(wallet, child_index, digest, after, MAX_WITNESS_PAGE)?
        else {
            return Ok(None);
        };
        if all
            .len()
            .checked_add(page.len())
            .is_none_or(|count| count > MAX_CACHED_WITNESSES)
        {
            return Err(malformed_witnesses());
        }
        let complete = page.len() < MAX_WITNESS_PAGE;
        after = page.last().map(|witness| witness.txid);
        all.extend(page);
        if complete {
            return Ok(Some((anchor, all)));
        }
    }
}

/// Loads at most 256 receipt metadata rows, ordered by raw transaction ID.
///
/// A short page ends the generation. The caller holds the exclusive wallet
/// operation lock and checks cancellation/deadline between calls. The anchor
/// must be reattested before any receipt is used as mined evidence.
pub(crate) fn load_witnesses_page(
    wallet: &mut WalletDatabase,
    child_index: u32,
    digest: [u8; 32],
    after_txid: Option<TxId>,
    maximum: usize,
) -> Result<Option<(BlockRef, Vec<CachedTransparentWitness>)>, WalletServiceError> {
    validate_child_index(child_index)?;
    if maximum == 0 || maximum > MAX_WITNESS_PAGE {
        return Err(malformed_witnesses());
    }
    wallet.transactionally_with_extension(|_wallet, extension| {
        let Some(generation) = read_witness_generation(extension, child_index)? else {
            let orphan: bool = extension.query_row(
                &format!("SELECT EXISTS (SELECT 1 FROM {PARTIAL_TRANSPARENT_WITNESS_TABLE} WHERE child_index = ?1)
                    OR EXISTS (SELECT 1 FROM {PARTIAL_TRANSPARENT_CHECKED_CANDIDATE_TABLE} WHERE child_index = ?1)"),
                [child_index], |row| row.get(0),
            )?;
            return if orphan { Err(malformed_witnesses()) } else { Ok(None) };
        };
        if generation.digest != digest {
            delete_witness_generation(extension, child_index)?;
            return Ok(None);
        }
        if after_txid.is_none() {
            let (actual_count, malformed_rows): (i64, i64) = extension.query_row(
                &format!("SELECT COUNT(*), COALESCE(SUM(CASE WHEN
                    typeof(txid) != 'blob' OR length(txid) != 32 OR
                    typeof(raw_digest) != 'blob' OR length(raw_digest) != 32 OR
                    typeof(mined_height) != 'integer' OR mined_height < 1 OR mined_height > ?2
                    THEN 1 ELSE 0 END), 0)
                    FROM {PARTIAL_TRANSPARENT_WITNESS_TABLE} WHERE child_index = ?1"),
                params![child_index, generation.anchor.height], |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            if usize::try_from(actual_count).ok() != Some(generation.count) || malformed_rows != 0 {
                return Err(malformed_witnesses());
            }
            let (checked_count, malformed_heights): (i64, i64) = extension.query_row(
                &format!("SELECT COUNT(*), COALESCE(SUM(CASE WHEN
                    typeof(mined_height) != 'integer' OR mined_height < 1 OR mined_height > ?2
                    THEN 1 ELSE 0 END), 0)
                    FROM {PARTIAL_TRANSPARENT_CHECKED_CANDIDATE_TABLE} WHERE child_index = ?1"),
                params![child_index, generation.anchor.height], |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            if usize::try_from(checked_count).ok() != Some(generation.checked_count)
                || malformed_heights != 0
            {
                return Err(malformed_witnesses());
            }
        }
        let query = format!(
            "SELECT txid, mined_height, raw_digest FROM {PARTIAL_TRANSPARENT_WITNESS_TABLE}
             WHERE child_index = ?1 AND txid > ?2
             ORDER BY txid LIMIT 1"
        );
        let mut after = after_txid.map(|txid| txid.as_ref().to_vec()).unwrap_or_default();
        let mut receipts = Vec::new();
        for _ in 0..maximum {
            let stored = extension.query_row(&query, params![child_index, &after], |row| {
                Ok((read_blob32(row, 0)?, row.get::<_, i64>(1)?, read_blob32(row, 2)?))
            }).optional()?;
            let Some((txid, height, raw_digest)) = stored else { break; };
            let receipt = CachedTransparentWitness {
                txid: TxId::from_bytes(txid),
                height: u32::try_from(height).map_err(|_| malformed_witnesses())?,
                raw_digest,
            };
            validate_witness_height(&receipt, generation.anchor)?;
            after = txid.to_vec();
            receipts.push(receipt);
        }
        if receipts.len() > generation.count {
            return Err(malformed_witnesses());
        }
        Ok(Some((generation.anchor, receipts)))
    })
}

/// Loads one bounded raw transaction and checks its receipt's exact digest.
///
/// A failed sync can rewind the backend's mined height to NULL. The caller's
/// freshly canonical anchor permits restoring that verified receipt height;
/// a different non-NULL mined height is rejected.
pub(crate) fn load_witness_transaction(
    wallet: &mut WalletDatabase,
    receipt: &CachedTransparentWitness,
) -> Result<Vec<u8>, WalletServiceError> {
    wallet.transactionally_with_extension(|_wallet, extension| {
        read_witness_transaction(extension, receipt)
    })
}

/// Atomically extends a generation with an already imported, verified batch.
///
/// Before extending an existing matching generation, the caller MUST freshly
/// reattest its old canonical anchor. The new anchor must cover every receipt.
/// Successful reconciliation retains these receipts; clearing scan progress
/// with [`clear`] does not discard verified evidence.
pub(crate) fn store_witnesses(
    wallet: &mut WalletDatabase,
    child_index: u32,
    digest: [u8; 32],
    anchor: BlockRef,
    witnesses: &[CachedTransparentWitness],
    checked_heights: &[u32],
) -> Result<(), WalletServiceError> {
    validate_child_index(child_index)?;
    if anchor.height == 0
        || witnesses.len() > MAX_CACHED_WITNESSES
        || checked_heights.len() > MAX_CACHED_WITNESSES
    {
        return Err(malformed_witnesses());
    }
    wallet.transactionally_with_extension(|_wallet, extension| {
        let previous = read_witness_generation(extension, child_index)?;
        let (mut count, mut checked_count) = match previous {
            Some(previous) if previous.digest == digest => {
                if anchor.height < previous.anchor.height
                    || (anchor.height == previous.anchor.height && anchor != previous.anchor)
                {
                    return Err(malformed_witnesses());
                }
                (previous.count, previous.checked_count)
            }
            _ => {
                delete_witness_generation(extension, child_index)?;
                (0, 0)
            }
        };
        extension.execute(
            &format!("INSERT INTO {PARTIAL_TRANSPARENT_WITNESS_GENERATION_TABLE}
                (child_index, reconciliation_digest, anchor_height, canonical_block_hash, witness_count, checked_count)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                ON CONFLICT(child_index) DO UPDATE SET
                    reconciliation_digest = excluded.reconciliation_digest,
                    anchor_height = excluded.anchor_height,
                    canonical_block_hash = excluded.canonical_block_hash"),
            params![child_index, &digest[..], anchor.height, &anchor.hash[..], count, checked_count],
        )?;
        for receipt in witnesses {
            validate_witness_height(receipt, anchor)?;
            // The raw bytes remain in the upstream transactions table; neither
            // this cache nor a spend-link row can substitute for exact bytes.
            read_witness_transaction(extension, receipt)?;
            let existing = extension.query_row(
                &format!("SELECT mined_height, raw_digest FROM {PARTIAL_TRANSPARENT_WITNESS_TABLE}
                    WHERE child_index = ?1 AND txid = ?2"),
                params![child_index, receipt.txid.as_ref()],
                |row| Ok((row.get::<_, i64>(0)?, read_blob32(row, 1)?)),
            ).optional()?;
            if let Some((height, raw_digest)) = existing {
                if height != i64::from(receipt.height) || raw_digest != receipt.raw_digest {
                    return Err(malformed_witnesses());
                }
                continue;
            }
            count = count.checked_add(1).filter(|count| *count <= MAX_CACHED_WITNESSES)
                .ok_or_else(malformed_witnesses)?;
            extension.execute(
                &format!("INSERT INTO {PARTIAL_TRANSPARENT_WITNESS_TABLE}
                    (child_index, txid, mined_height, raw_digest) VALUES (?1, ?2, ?3, ?4)"),
                params![child_index, receipt.txid.as_ref(), receipt.height, &receipt.raw_digest[..]],
            )?;
        }
        for height in checked_heights {
            if *height == 0 || *height > anchor.height {
                return Err(malformed_witnesses());
            }
            let inserted = extension.execute(
                &format!("INSERT INTO {PARTIAL_TRANSPARENT_CHECKED_CANDIDATE_TABLE}
                    (child_index, mined_height) VALUES (?1, ?2) ON CONFLICT DO NOTHING"),
                params![child_index, height],
            )?;
            checked_count = checked_count.checked_add(inserted)
                .filter(|count| *count <= MAX_CACHED_WITNESSES).ok_or_else(malformed_witnesses)?;
        }
        extension.execute(
            &format!("UPDATE {PARTIAL_TRANSPARENT_WITNESS_GENERATION_TABLE}
                SET witness_count = ?2, checked_count = ?3 WHERE child_index = ?1"),
            params![child_index, count, checked_count],
        )?;
        Ok(())
    })
}

/// Whether a canonical batch already checked this candidate height.
///
/// This is only a query-avoidance hint, never a witness for an omitted output.
/// The caller must freshly reattest this generation's anchor before using it.
pub(crate) fn candidate_was_checked(
    wallet: &mut WalletDatabase,
    child_index: u32,
    digest: [u8; 32],
    height: u32,
) -> Result<bool, WalletServiceError> {
    validate_child_index(child_index)?;
    if height == 0 {
        return Err(malformed_witnesses());
    }
    wallet.transactionally_with_extension(|_wallet, extension| {
        let Some(generation) = read_witness_generation(extension, child_index)? else {
            return Ok(false);
        };
        if generation.digest != digest {
            delete_witness_generation(extension, child_index)?;
            return Ok(false);
        }
        if height > generation.anchor.height {
            return Ok(false);
        }
        let checked: bool = extension.query_row(
            &format!(
                "SELECT EXISTS (SELECT 1 FROM {PARTIAL_TRANSPARENT_CHECKED_CANDIDATE_TABLE}
                WHERE child_index = ?1 AND mined_height = ?2)"
            ),
            params![child_index, height],
            |row| row.get(0),
        )?;
        Ok(checked)
    })
}

/// Discards stale verified evidence without altering the separate scan cursor.
pub(crate) fn clear_witnesses(
    wallet: &mut WalletDatabase,
    child_index: u32,
) -> Result<(), WalletServiceError> {
    validate_child_index(child_index)?;
    wallet.transactionally_with_extension(|_wallet, extension| {
        delete_witness_generation(extension, child_index)
    })
}

fn read_witness_generation(
    extension: &ExtensionTransaction<'_>,
    child_index: u32,
) -> Result<Option<WitnessGeneration>, WalletServiceError> {
    let stored = extension.query_row(
        &format!("SELECT reconciliation_digest, anchor_height, canonical_block_hash, witness_count, checked_count
            FROM {PARTIAL_TRANSPARENT_WITNESS_GENERATION_TABLE} WHERE child_index = ?1"),
        [child_index],
        |row| Ok((read_blob32(row, 0)?, row.get::<_, i64>(1)?,
            read_blob32(row, 2)?, row.get::<_, i64>(3)?, row.get::<_, i64>(4)?)),
    ).optional()?;
    stored
        .map(|(digest, height, hash, count, checked_count)| {
            let height = u32::try_from(height)
                .ok()
                .filter(|height| *height > 0)
                .ok_or_else(malformed_witnesses)?;
            let count = usize::try_from(count)
                .ok()
                .filter(|count| *count <= MAX_CACHED_WITNESSES)
                .ok_or_else(malformed_witnesses)?;
            let checked_count = usize::try_from(checked_count)
                .ok()
                .filter(|count| *count <= MAX_CACHED_WITNESSES)
                .ok_or_else(malformed_witnesses)?;
            Ok(WitnessGeneration {
                digest,
                anchor: BlockRef { height, hash },
                count,
                checked_count,
            })
        })
        .transpose()
}

fn read_witness_transaction(
    extension: &ExtensionTransaction<'_>,
    receipt: &CachedTransparentWitness,
) -> Result<Vec<u8>, WalletServiceError> {
    if receipt.height == 0 {
        return Err(malformed_witnesses());
    }
    let metadata = extension
        .query_row(
            "SELECT length(raw), typeof(raw), mined_height FROM transactions WHERE txid = ?1",
            [receipt.txid.as_ref()],
            |row| {
                Ok((
                    row.get::<_, Option<i64>>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                ))
            },
        )
        .optional()?
        .ok_or_else(malformed_witnesses)?;
    let (length, kind, height) = metadata;
    if kind != "blob"
        || length.is_none_or(|length| {
            length <= 0 || length > i64::try_from(MAX_BLOCK_BYTES).expect("block size fits i64")
        })
        || height.is_some_and(|height| height != i64::from(receipt.height))
    {
        return Err(malformed_witnesses());
    }
    let raw: Vec<u8> = extension.query_row(
        "SELECT raw FROM transactions WHERE txid = ?1",
        [receipt.txid.as_ref()],
        |row| row.get(0),
    )?;
    let digest: [u8; 32] = Sha256::digest(&raw).into();
    if digest != receipt.raw_digest {
        return Err(malformed_witnesses());
    }
    Ok(raw)
}

fn delete_witness_generation(
    extension: &ExtensionTransaction<'_>,
    child_index: u32,
) -> Result<(), WalletServiceError> {
    // Explicitly delete children too, even if a connection disabled SQLite FKs.
    extension.execute(
        &format!("DELETE FROM {PARTIAL_TRANSPARENT_WITNESS_TABLE} WHERE child_index = ?1"),
        [child_index],
    )?;
    extension.execute(
        &format!(
            "DELETE FROM {PARTIAL_TRANSPARENT_CHECKED_CANDIDATE_TABLE} WHERE child_index = ?1"
        ),
        [child_index],
    )?;
    extension.execute(
        &format!(
            "DELETE FROM {PARTIAL_TRANSPARENT_WITNESS_GENERATION_TABLE} WHERE child_index = ?1"
        ),
        [child_index],
    )?;
    Ok(())
}

fn validate_witness_height(
    receipt: &CachedTransparentWitness,
    anchor: BlockRef,
) -> Result<(), WalletServiceError> {
    if receipt.height == 0 || receipt.height > anchor.height {
        return Err(malformed_witnesses());
    }
    Ok(())
}

fn read_blob32(row: &rusqlite::Row<'_>, index: usize) -> Result<[u8; 32], rusqlite::Error> {
    row.get_ref(index)?
        .as_blob()
        .map_err(|_| rusqlite::Error::InvalidQuery)?
        .try_into()
        .map_err(|_| rusqlite::Error::InvalidQuery)
}

fn malformed_witnesses() -> WalletServiceError {
    WalletServiceError::Database(
        "partial transparent witness cache is malformed or inconsistent".to_owned(),
    )
}

/// Loads a structurally valid prefix hint for this exact reconciliation set.
///
/// A changed digest discards the old cursor. Malformed metadata fails closed
/// rather than allowing a corrupt cursor to skip a history range.
pub(crate) fn load(
    wallet: &mut WalletDatabase,
    child_index: u32,
    digest: [u8; 32],
) -> Result<Option<BlockRef>, WalletServiceError> {
    validate_child_index(child_index)?;
    wallet.transactionally_with_extension(|_wallet, extension| {
        let stored = extension
            .query_row(
                &format!(
                    "SELECT reconciliation_digest, scanned_through, canonical_block_hash
                     FROM {PARTIAL_TRANSPARENT_RECOVERY_TABLE} WHERE child_index = ?1"
                ),
                [child_index],
                |row| {
                    Ok((
                        row.get::<_, Vec<u8>>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                    ))
                },
            )
            .optional()?;
        let Some((stored_digest, height, hash)) = stored else {
            return Ok(None);
        };
        let stored_digest: [u8; 32] = stored_digest.try_into().map_err(|_| malformed_progress())?;
        let height = u32::try_from(height)
            .ok()
            .filter(|height| *height > 0)
            .ok_or_else(malformed_progress)?;
        let hash = hash.try_into().map_err(|_| malformed_progress())?;
        if stored_digest != digest {
            extension.execute(
                &format!("DELETE FROM {PARTIAL_TRANSPARENT_RECOVERY_TABLE} WHERE child_index = ?1"),
                [child_index],
            )?;
            return Ok(None);
        }
        Ok(Some(BlockRef { height, hash }))
    })
}

/// Replaces one receiver's prefix after the caller verifies and stores a chunk.
pub(crate) fn store(
    wallet: &mut WalletDatabase,
    child_index: u32,
    digest: [u8; 32],
    scanned_through: BlockRef,
) -> Result<(), WalletServiceError> {
    validate_child_index(child_index)?;
    if scanned_through.height == 0 {
        return Err(malformed_progress());
    }
    wallet.transactionally_with_extension(|_wallet, extension| {
        extension.execute(
            &format!(
                "INSERT INTO {PARTIAL_TRANSPARENT_RECOVERY_TABLE}
                    (child_index, reconciliation_digest, scanned_through, canonical_block_hash)
                 VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(child_index) DO UPDATE SET
                    reconciliation_digest = excluded.reconciliation_digest,
                    scanned_through = excluded.scanned_through,
                    canonical_block_hash = excluded.canonical_block_hash"
            ),
            params![
                child_index,
                &digest[..],
                scanned_through.height,
                &scanned_through.hash[..],
            ],
        )?;
        Ok(())
    })
}

/// Discards a prefix whose canonical hash no longer matches the pinned chain.
pub(crate) fn clear(
    wallet: &mut WalletDatabase,
    child_index: u32,
) -> Result<(), WalletServiceError> {
    validate_child_index(child_index)?;
    wallet.transactionally_with_extension(|_wallet, extension| {
        extension.execute(
            &format!("DELETE FROM {PARTIAL_TRANSPARENT_RECOVERY_TABLE} WHERE child_index = ?1"),
            [child_index],
        )?;
        Ok(())
    })
}

fn validate_child_index(child_index: u32) -> Result<(), WalletServiceError> {
    if child_index > MAX_COINBASE_ADDRESS_INDEX {
        return Err(WalletServiceError::InvalidRequest(
            "partial recovery receiver index is outside the managed range".to_owned(),
        ));
    }
    Ok(())
}

fn malformed_progress() -> WalletServiceError {
    WalletServiceError::Database("partial transparent recovery progress is malformed".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{open_wallet_database, WalletNetwork};

    // These short bytes test persistence only; the RPC validator independently
    // parses complete Wcash transactions before any receipt becomes evidence.
    fn stored_witness(path: &std::path::Path, id: u32, height: u32) -> CachedTransparentWitness {
        let mut txid = [0; 32];
        txid[..4].copy_from_slice(&id.to_be_bytes());
        let raw = id.to_le_bytes().to_vec();
        let receipt = CachedTransparentWitness {
            txid: TxId::from_bytes(txid),
            height,
            raw_digest: Sha256::digest(&raw).into(),
        };
        rusqlite::Connection::open(path)
            .unwrap()
            .execute(
                "INSERT INTO transactions (txid, raw, mined_height, min_observed_height)
             VALUES (?1, ?2, ?3, 0)",
                params![receipt.txid.as_ref(), raw, height],
            )
            .unwrap();
        receipt
    }

    fn witness_anchor(height: u32) -> BlockRef {
        BlockRef {
            height,
            hash: [0x55; 32],
        }
    }

    #[test]
    fn witness_cache_survives_reopen_extends_atomically_and_keeps_scan_cursor_separate() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("wallet.sqlite");
        let mut wallet = open_wallet_database(&path, WalletNetwork::Regtest).unwrap();
        let first = stored_witness(&path, 1, 10);
        let second = stored_witness(&path, 2, 20);
        store_witnesses(
            &mut wallet,
            0,
            [1; 32],
            witness_anchor(20),
            std::slice::from_ref(&first),
            &[10],
        )
        .unwrap();
        store_witnesses(
            &mut wallet,
            1,
            [2; 32],
            witness_anchor(20),
            std::slice::from_ref(&second),
            &[20],
        )
        .unwrap();
        drop(wallet);
        let mut wallet = open_wallet_database(&path, WalletNetwork::Regtest).unwrap();
        store_witnesses(
            &mut wallet,
            0,
            [1; 32],
            witness_anchor(25),
            std::slice::from_ref(&second),
            &[20, 20],
        )
        .unwrap();
        assert_eq!(
            load_witnesses(&mut wallet, 0, [1; 32]).unwrap(),
            Some((witness_anchor(25), vec![first.clone(), second.clone()]))
        );
        assert_eq!(
            load_witnesses(&mut wallet, 1, [2; 32]).unwrap(),
            Some((witness_anchor(20), vec![second]))
        );
        assert!(candidate_was_checked(&mut wallet, 0, [1; 32], 10).unwrap());
        assert!(!candidate_was_checked(&mut wallet, 0, [1; 32], 11).unwrap());
        assert!(!candidate_was_checked(&mut wallet, 0, [1; 32], 26).unwrap());
        store(&mut wallet, 0, [1; 32], witness_anchor(16)).unwrap();
        clear(&mut wallet, 0).unwrap();
        assert!(load_witnesses(&mut wallet, 0, [1; 32]).unwrap().is_some());
        assert_eq!(
            load_witness_transaction(&mut wallet, &first).unwrap(),
            1_u32.to_le_bytes()
        );
        // An upstream rewind may un-mine a still-canonical cached spender.
        rusqlite::Connection::open(&path)
            .unwrap()
            .execute(
                "UPDATE transactions SET mined_height = NULL WHERE txid = ?1",
                [first.txid.as_ref()],
            )
            .unwrap();
        assert!(load_witness_transaction(&mut wallet, &first).is_ok());
        assert_eq!(load_witnesses(&mut wallet, 0, [3; 32]).unwrap(), None);
        assert_eq!(load_witnesses(&mut wallet, 0, [1; 32]).unwrap(), None);
        assert!(!candidate_was_checked(&mut wallet, 0, [1; 32], 10).unwrap());
        assert!(load_witnesses(&mut wallet, 1, [2; 32]).unwrap().is_some());
        clear_witnesses(&mut wallet, 1).unwrap();
        clear_witnesses(&mut wallet, 1).unwrap();
        assert_eq!(load_witnesses(&mut wallet, 1, [2; 32]).unwrap(), None);
    }

    #[test]
    fn witness_cache_rejects_inconsistent_bytes_heights_and_rolls_back_whole_batch() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("wallet.sqlite");
        let mut wallet = open_wallet_database(&path, WalletNetwork::Regtest).unwrap();
        let first = stored_witness(&path, 1, 10);
        let second = stored_witness(&path, 2, 20);
        store_witnesses(
            &mut wallet,
            0,
            [1; 32],
            witness_anchor(10),
            std::slice::from_ref(&first),
            &[10],
        )
        .unwrap();
        let mut wrong = second.clone();
        wrong.raw_digest = [0xff; 32];
        // Even a digest replacement must not delete the original generation
        // when a later member of its replacement batch fails validation.
        assert!(store_witnesses(
            &mut wallet,
            0,
            [2; 32],
            witness_anchor(30),
            &[second.clone(), wrong],
            &[20]
        )
        .is_err());
        assert_eq!(
            load_witnesses(&mut wallet, 0, [1; 32]).unwrap(),
            Some((witness_anchor(10), vec![first.clone()]))
        );
        assert!(!candidate_was_checked(&mut wallet, 0, [1; 32], 20).unwrap());
        assert!(store_witnesses(
            &mut wallet,
            0,
            [1; 32],
            witness_anchor(30),
            std::slice::from_ref(&second),
            &[31]
        )
        .is_err());
        assert_eq!(
            load_witnesses(&mut wallet, 0, [1; 32]).unwrap(),
            Some((witness_anchor(10), vec![first.clone()]))
        );
        assert!(store_witnesses(&mut wallet, 0, [1; 32], witness_anchor(9), &[], &[]).is_err());
        assert!(store_witnesses(
            &mut wallet,
            0,
            [1; 32],
            BlockRef {
                hash: [0x99; 32],
                ..witness_anchor(10)
            },
            &[],
            &[]
        )
        .is_err());
        assert!(store_witnesses(
            &mut wallet,
            0,
            [1; 32],
            witness_anchor(19),
            std::slice::from_ref(&second),
            &[]
        )
        .is_err());
        for assignment in [
            "raw = NULL",
            "raw = X'00'",
            "raw = 'text'",
            "raw = zeroblob(2000001)",
            "mined_height = 11",
        ] {
            let connection = rusqlite::Connection::open(&path).unwrap();
            connection
                .execute(
                    &format!("UPDATE transactions SET {assignment} WHERE txid = ?1"),
                    [first.txid.as_ref()],
                )
                .unwrap();
            assert!(
                load_witness_transaction(&mut wallet, &first).is_err(),
                "{assignment}"
            );
            connection
                .execute(
                    "UPDATE transactions SET raw = ?2, mined_height = 10 WHERE txid = ?1",
                    params![first.txid.as_ref(), 1_u32.to_le_bytes().as_slice()],
                )
                .unwrap();
        }
        let absent = CachedTransparentWitness {
            txid: TxId::from_bytes([0x88; 32]),
            ..first.clone()
        };
        assert!(load_witness_transaction(&mut wallet, &absent).is_err());
        assert!(store_witnesses(&mut wallet, 10, [1; 32], witness_anchor(20), &[], &[]).is_err());
        assert!(store_witnesses(&mut wallet, 0, [1; 32], witness_anchor(0), &[], &[]).is_err());
        assert!(candidate_was_checked(&mut wallet, 0, [1; 32], 0).is_err());
        assert!(clear_witnesses(&mut wallet, 10).is_err());
        // Store accepts a changed output set only by replacing both caches.
        store_witnesses(
            &mut wallet,
            0,
            [2; 32],
            witness_anchor(20),
            std::slice::from_ref(&second),
            &[20],
        )
        .unwrap();
        assert_eq!(
            load_witnesses(&mut wallet, 0, [2; 32]).unwrap(),
            Some((witness_anchor(20), vec![second]))
        );
        assert!(!candidate_was_checked(&mut wallet, 0, [2; 32], 10).unwrap());
    }

    #[test]
    fn witness_cache_metadata_paging_and_corruption_fail_closed() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("wallet.sqlite");
        let mut wallet = open_wallet_database(&path, WalletNetwork::Regtest).unwrap();
        let receipts = (0..257)
            .map(|id| stored_witness(&path, id, 10))
            .collect::<Vec<_>>();
        store_witnesses(
            &mut wallet,
            0,
            [1; 32],
            witness_anchor(10),
            &receipts,
            &[10],
        )
        .unwrap();
        let (anchor, first) = load_witnesses_page(&mut wallet, 0, [1; 32], None, 256)
            .unwrap()
            .unwrap();
        assert_eq!(anchor, witness_anchor(10));
        assert_eq!(first, receipts[..256]);
        assert_eq!(
            load_witnesses_page(&mut wallet, 0, [1; 32], Some(first[255].txid), 256)
                .unwrap()
                .unwrap()
                .1,
            receipts[256..]
        );
        assert!(load_witnesses_page(&mut wallet, 0, [1; 32], None, 0).is_err());
        assert!(load_witnesses_page(&mut wallet, 0, [1; 32], None, 257).is_err());
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection
            .pragma_update(None, "ignore_check_constraints", true)
            .unwrap();
        for assignment in [
            "reconciliation_digest = X'00'",
            "reconciliation_digest = '01234567890123456789012345678901'",
            "anchor_height = 0",
            "anchor_height = 4294967296",
            "canonical_block_hash = X'00'",
            "witness_count = -1",
            "witness_count = 2000001",
            "witness_count = 256",
            "checked_count = 0",
            "checked_count = 2000001",
        ] {
            connection
                .execute(
                    &format!(
                        "UPDATE {PARTIAL_TRANSPARENT_WITNESS_GENERATION_TABLE} SET {assignment}"
                    ),
                    [],
                )
                .unwrap();
            assert!(
                load_witnesses(&mut wallet, 0, [1; 32]).is_err(),
                "{assignment}"
            );
            connection.execute(&format!("UPDATE {PARTIAL_TRANSPARENT_WITNESS_GENERATION_TABLE} SET reconciliation_digest=?1, anchor_height=10, canonical_block_hash=?2, witness_count=257, checked_count=1"),params![&[1_u8;32][..],&[0x55_u8;32][..]]).unwrap();
        }
        for assignment in [
            "raw_digest = X'00'",
            "raw_digest = '01234567890123456789012345678901'",
            "mined_height = 0",
            "mined_height = 11",
            "txid = X'00'",
            "txid = X''",
        ] {
            connection
                .execute(
                    &format!(
                        "UPDATE {PARTIAL_TRANSPARENT_WITNESS_TABLE} SET {assignment} WHERE txid=?1"
                    ),
                    [receipts[0].txid.as_ref()],
                )
                .unwrap();
            assert!(
                load_witnesses(&mut wallet, 0, [1; 32]).is_err(),
                "{assignment}"
            );
            connection.execute(&format!("DELETE FROM {PARTIAL_TRANSPARENT_WITNESS_TABLE} WHERE txid=?1 OR txid=X'00' OR txid=X''"),[receipts[0].txid.as_ref()]).unwrap();
            connection
                .execute(
                    &format!("INSERT INTO {PARTIAL_TRANSPARENT_WITNESS_TABLE} VALUES (0,?1,10,?2)"),
                    params![receipts[0].txid.as_ref(), &receipts[0].raw_digest[..]],
                )
                .unwrap();
        }
    }

    #[test]
    fn witness_cache_conflict_and_count_limits_leave_previous_generation_unchanged() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("wallet.sqlite");
        let mut wallet = open_wallet_database(&path, WalletNetwork::Regtest).unwrap();
        let first = stored_witness(&path, 1, 10);
        let second = stored_witness(&path, 2, 20);
        store_witnesses(
            &mut wallet,
            0,
            [1; 32],
            witness_anchor(20),
            std::slice::from_ref(&first),
            &[10],
        )
        .unwrap();
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection
            .execute(
                "UPDATE transactions SET mined_height=11 WHERE txid=?1",
                [first.txid.as_ref()],
            )
            .unwrap();
        let conflicting = CachedTransparentWitness {
            height: 11,
            ..first.clone()
        };
        assert!(store_witnesses(
            &mut wallet,
            0,
            [1; 32],
            witness_anchor(30),
            &[conflicting],
            &[11]
        )
        .is_err());
        connection
            .execute(
                "UPDATE transactions SET mined_height=10 WHERE txid=?1",
                [first.txid.as_ref()],
            )
            .unwrap();
        assert_eq!(
            load_witnesses(&mut wallet, 0, [1; 32]).unwrap(),
            Some((witness_anchor(20), vec![first.clone()]))
        );
        // Exercise the count guard without allocating millions of fixtures.
        for (field, receipts, checked) in [
            ("witness_count", vec![second], vec![]),
            ("checked_count", vec![], vec![20]),
        ] {
            connection
                .execute(
                    &format!(
                        "UPDATE {PARTIAL_TRANSPARENT_WITNESS_GENERATION_TABLE} SET {field}=2000000"
                    ),
                    [],
                )
                .unwrap();
            assert!(store_witnesses(
                &mut wallet,
                0,
                [1; 32],
                witness_anchor(30),
                &receipts,
                &checked
            )
            .is_err());
            assert_eq!(connection.query_row(&format!("SELECT anchor_height FROM {PARTIAL_TRANSPARENT_WITNESS_GENERATION_TABLE}"),[],|row|row.get::<_,i64>(0)).unwrap(),20);
            connection
                .execute(
                    &format!("UPDATE {PARTIAL_TRANSPARENT_WITNESS_GENERATION_TABLE} SET {field}=1"),
                    [],
                )
                .unwrap();
            assert_eq!(
                load_witnesses(&mut wallet, 0, [1; 32]).unwrap(),
                Some((witness_anchor(20), vec![first.clone()]))
            );
            assert!(!candidate_was_checked(&mut wallet, 0, [1; 32], 20).unwrap());
        }
    }

    #[test]
    fn witness_cache_schema_migration_preserves_incomplete_recovery_and_existing_cursor() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("wallet.sqlite");
        let mut wallet = open_wallet_database(&path, WalletNetwork::Regtest).unwrap();
        store(&mut wallet, 0, [1; 32], witness_anchor(10)).unwrap();
        drop(wallet);
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection.execute_batch(&format!("DROP TABLE {PARTIAL_TRANSPARENT_WITNESS_TABLE}; DROP TABLE {PARTIAL_TRANSPARENT_CHECKED_CANDIDATE_TABLE}; DROP TABLE {PARTIAL_TRANSPARENT_WITNESS_GENERATION_TABLE};")).unwrap();
        connection.execute("UPDATE ext_wcash_transparent_sync_state SET recovery_in_progress=1,recovery_session=?1",[&[0x11_u8;32][..]]).unwrap();
        drop(connection);
        let mut wallet = open_wallet_database(&path, WalletNetwork::Regtest).unwrap();
        assert_eq!(
            load(&mut wallet, 0, [1; 32]).unwrap(),
            Some(witness_anchor(10))
        );
        assert_eq!(load_witnesses(&mut wallet, 0, [1; 32]).unwrap(), None);
        store_witnesses(&mut wallet, 0, [1; 32], witness_anchor(10), &[], &[10]).unwrap();
        assert!(candidate_was_checked(&mut wallet, 0, [1; 32], 10).unwrap());
        let connection = rusqlite::Connection::open(&path).unwrap();
        assert_eq!(
            connection
                .query_row(
                    "SELECT recovery_in_progress FROM ext_wcash_transparent_sync_state",
                    [],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            1
        );
    }

    #[test]
    fn partial_recovery_progress_survives_reopen_and_is_bound_to_receiver_and_digest() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("wallet.sqlite");
        let network = WalletNetwork::Regtest;
        let first = BlockRef {
            height: 10,
            hash: [0x10; 32],
        };
        let second = BlockRef {
            height: 26,
            hash: [0x26; 32],
        };
        let mut wallet = open_wallet_database(&path, network).unwrap();
        assert_eq!(load(&mut wallet, 0, [1; 32]).unwrap(), None);
        store(&mut wallet, 0, [1; 32], first).unwrap();
        store(&mut wallet, 1, [2; 32], second).unwrap();
        drop(wallet);

        let mut wallet = open_wallet_database(&path, network).unwrap();
        assert_eq!(load(&mut wallet, 0, [1; 32]).unwrap(), Some(first));
        assert_eq!(load(&mut wallet, 1, [2; 32]).unwrap(), Some(second));
        store(&mut wallet, 0, [1; 32], second).unwrap();
        assert_eq!(load(&mut wallet, 0, [1; 32]).unwrap(), Some(second));
        assert_eq!(load(&mut wallet, 0, [3; 32]).unwrap(), None);
        // Digest changes discard the old row, so switching back cannot revive it.
        assert_eq!(load(&mut wallet, 0, [1; 32]).unwrap(), None);
        assert_eq!(load(&mut wallet, 1, [2; 32]).unwrap(), Some(second));
        clear(&mut wallet, 1).unwrap();
        clear(&mut wallet, 1).unwrap();
        assert_eq!(load(&mut wallet, 1, [2; 32]).unwrap(), None);
    }

    #[test]
    fn partial_recovery_progress_refuses_invalid_input_and_corrupt_persisted_metadata() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("wallet.sqlite");
        let mut wallet = open_wallet_database(&path, WalletNetwork::Regtest).unwrap();
        let progress = BlockRef {
            height: 10,
            hash: [0x10; 32],
        };
        assert!(store(&mut wallet, 10, [1; 32], progress).is_err());
        assert!(load(&mut wallet, 10, [1; 32]).is_err());
        assert!(clear(&mut wallet, 10).is_err());
        assert!(store(
            &mut wallet,
            0,
            [1; 32],
            BlockRef {
                height: 0,
                ..progress
            }
        )
        .is_err());

        for assignment in [
            "reconciliation_digest = X'00'",
            "reconciliation_digest = '01234567890123456789012345678901'",
            "scanned_through = -1",
            "scanned_through = 0",
            "scanned_through = 4294967296",
            "canonical_block_hash = X'00'",
            "canonical_block_hash = '01234567890123456789012345678901'",
        ] {
            store(&mut wallet, 0, [1; 32], progress).unwrap();
            let connection = rusqlite::Connection::open(&path).unwrap();
            connection
                .pragma_update(None, "ignore_check_constraints", true)
                .unwrap();
            connection
                .execute(
                    &format!("UPDATE {PARTIAL_TRANSPARENT_RECOVERY_TABLE} SET {assignment}"),
                    [],
                )
                .unwrap();
            drop(connection);
            assert!(load(&mut wallet, 0, [1; 32]).is_err(), "{assignment}");
            assert!(load(&mut wallet, 0, [2; 32]).is_err(), "{assignment}");
        }
    }

    #[test]
    fn partial_recovery_schema_is_added_to_existing_identified_wallet_without_reset() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("wallet.sqlite");
        let network = WalletNetwork::Regtest;
        drop(open_wallet_database(&path, network).unwrap());
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection
            .execute(
                &format!("DROP TABLE {PARTIAL_TRANSPARENT_RECOVERY_TABLE}"),
                [],
            )
            .unwrap();
        connection
            .execute("UPDATE ext_wcash_transparent_sync_state SET recovery_in_progress = 1, recovery_session = ?1", [&[0x55_u8; 32][..]])
            .unwrap();
        drop(connection);

        let mut wallet = open_wallet_database(&path, network).unwrap();
        assert_eq!(load(&mut wallet, 0, [1; 32]).unwrap(), None);
        store(
            &mut wallet,
            0,
            [1; 32],
            BlockRef {
                height: 16,
                hash: [0x16; 32],
            },
        )
        .unwrap();
        let connection = rusqlite::Connection::open(&path).unwrap();
        let in_progress: u32 = connection
            .query_row(
                "SELECT recovery_in_progress FROM ext_wcash_transparent_sync_state",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(in_progress, 1);
        assert!(connection
            .execute(
                &format!(
                    "INSERT INTO {PARTIAL_TRANSPARENT_RECOVERY_TABLE} VALUES (10, ?1, 16, ?2)"
                ),
                params![&[1_u8; 32][..], &[2_u8; 32][..]],
            )
            .is_err());
    }
}
