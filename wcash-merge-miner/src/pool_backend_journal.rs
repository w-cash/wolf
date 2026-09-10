//! Durable, append-only journal for backend protocol-v1 events.
//!
//! The first line is a versioned header that binds this file to one backend
//! installation, one journal sequence namespace, and one exact pair of chains.
//! Every later line contains a complete [`BackendEvent`] and a SHA-256 link to
//! the preceding record. The first event links to the canonical header digest,
//! so records cannot be moved between otherwise valid journals unnoticed.
//!
//! This initial implementation retains every validated event in memory to make
//! replay and bounded page reads straightforward. To keep that design bounded,
//! a journal is limited to [`MAX_JOURNAL_EVENTS`] events and
//! [`MAX_JOURNAL_BYTES`] bytes. A future indexed implementation may raise those
//! limits without changing the on-disk format.

use std::{
    fs::{self, File, OpenOptions},
    io::{self, BufRead, BufReader, Seek, SeekFrom, Write},
    path::{Component, Path, PathBuf},
    sync::{Mutex, MutexGuard},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;
use wcash_pool_protocol::{
    BackendEvent, BackendMessage, CanonicalUuid, Hex32, ProtocolError, BACKEND_PROTOCOL_VERSION,
    MAX_BACKEND_PAYLOAD_BYTES, MAX_EVENT_PAGE_ITEMS,
};

/// On-disk format implemented by this module.
pub const JOURNAL_FORMAT_VERSION: u16 = 1;

/// Maximum number of events retained and replayed by this foundation.
pub const MAX_JOURNAL_EVENTS: usize = 1_000_000;

/// Maximum total size of a journal, including its header and newlines.
pub const MAX_JOURNAL_BYTES: u64 = 1024 * 1024 * 1024;

/// Maximum size of one header or event line, including its newline.
pub const MAX_JOURNAL_RECORD_BYTES: usize = MAX_BACKEND_PAYLOAD_BYTES + 4 * 1024;

const PRIVATE_FILE_MODE: u32 = 0o600;
const HEADER_DIGEST_DOMAIN: &[u8] = b"wcash-pool/backend-journal-header/v1\0";
const EVENT_DIGEST_DOMAIN: &[u8] = b"wcash-pool/backend-journal-event/v1\0";

/// An error that prevents safe creation, replay, or use of the backend journal.
#[derive(Debug, Error)]
pub enum PoolBackendJournalError {
    /// The requested path cannot identify a journal file.
    #[error("invalid pool backend journal path: {0}")]
    InvalidPath(
        /// Human-readable reason the path is unsafe or unusable.
        String,
    ),

    /// Creation was requested for a path that already exists.
    #[error("pool backend journal {path} already exists", path = .path.display())]
    AlreadyExists {
        /// Path at which creation found an existing filesystem entry.
        path: PathBuf,
    },

    /// A filesystem operation failed.
    #[error(
        "failed to {operation} pool backend journal {path}: {source}",
        path = .path.display()
    )]
    Io {
        /// Filesystem operation that failed.
        operation: &'static str,
        /// Journal or parent path involved in the operation.
        path: PathBuf,
        /// Underlying operating-system error.
        #[source]
        source: io::Error,
    },

    /// Another process holds the journal's exclusive advisory lock.
    #[error(
        "pool backend journal {path} is already locked or cannot be locked: {source}",
        path = .path.display()
    )]
    Lock {
        /// Journal path whose exclusive lock could not be acquired.
        path: PathBuf,
        /// Underlying locking error.
        #[source]
        source: io::Error,
    },

    /// The journal or its parent resolves through a symbolic link where links
    /// are not accepted.
    #[error("pool backend journal {path} must not be a symbolic link", path = .path.display())]
    SymbolicLink {
        /// Path that resolves to a disallowed symbolic link.
        path: PathBuf,
    },

    /// The journal path does not identify a regular file.
    #[error("pool backend journal {path} is not a regular file", path = .path.display())]
    NotRegularFile {
        /// Path that does not identify a regular file.
        path: PathBuf,
    },

    /// The parent path is not a real directory.
    #[error(
        "pool backend journal parent {path} must be a non-symbolic-link directory",
        path = .path.display()
    )]
    UnsafeParentType {
        /// Parent path that is not an acceptable real directory.
        path: PathBuf,
    },

    /// Group or other users can modify the journal's parent directory.
    #[error(
        "pool backend journal parent {path} must not be writable by group or other users",
        path = .path.display()
    )]
    UnsafeParentMode {
        /// Parent directory whose permissions allow unsafe modification.
        path: PathBuf,
    },

    /// The parent is not owned by the backend's effective Unix user.
    #[cfg(unix)]
    #[error(
        "pool backend journal parent {path} is owned by UID {owner_uid}, not effective UID {effective_uid}",
        path = .path.display()
    )]
    UnsafeParentOwner {
        /// Parent directory with the unexpected owner.
        path: PathBuf,
        /// UID recorded in the directory metadata.
        owner_uid: u32,
        /// Effective UID required by the journal process.
        effective_uid: u32,
    },

    /// The journal does not have exactly owner read/write permissions.
    #[error(
        "pool backend journal {path} must have mode 0600 (found {mode:04o})",
        path = .path.display()
    )]
    UnsafeFileMode {
        /// Journal path with unsafe permissions.
        path: PathBuf,
        /// Permission bits found on the journal file.
        mode: u32,
    },

    /// The journal is not owned by the backend's effective Unix user.
    #[cfg(unix)]
    #[error(
        "pool backend journal {path} is owned by UID {owner_uid}, not effective UID {effective_uid}",
        path = .path.display()
    )]
    UnsafeFileOwner {
        /// Journal path with the unexpected owner.
        path: PathBuf,
        /// UID recorded in the file metadata.
        owner_uid: u32,
        /// Effective UID required by the journal process.
        effective_uid: u32,
    },

    /// A second directory entry names the same inode.
    #[error(
        "pool backend journal {path} must have exactly one hard link (found {links})",
        path = .path.display()
    )]
    HardLinked {
        /// Journal path whose inode has multiple directory entries.
        path: PathBuf,
        /// Hard-link count recorded in the file metadata.
        links: u64,
    },

    /// The live pathname no longer names the locked inode or expected length.
    #[error(
        "pool backend journal path {path} no longer refers to the locked journal; restart before writing",
        path = .path.display()
    )]
    PathChanged {
        /// Journal path that no longer names the locked file state.
        path: PathBuf,
    },

    /// A caller supplied an invalid journal identity or network parameter.
    #[error("invalid pool backend journal configuration: {0}")]
    InvalidConfiguration(
        /// Description of the invalid configuration invariant.
        &'static str,
    ),

    /// An existing journal contains no versioned header.
    #[error("existing pool backend journal {path} is empty", path = .path.display())]
    EmptyJournal {
        /// Existing journal path that contains no header.
        path: PathBuf,
    },

    /// A crash interrupted the header itself, so there is no journal to recover.
    #[error(
        "pool backend journal {path} has an unterminated header and cannot be recovered",
        path = .path.display()
    )]
    UnterminatedHeader {
        /// Journal path containing the interrupted header.
        path: PathBuf,
    },

    /// The complete header does not match the strict v1 schema.
    #[error(
        "pool backend journal {path} has an invalid versioned header: {source}",
        path = .path.display()
    )]
    MalformedHeader {
        /// Journal path containing the malformed header.
        path: PathBuf,
        /// Strict JSON decoding error for the header.
        #[source]
        source: serde_json::Error,
    },

    /// The header uses an unsupported journal or backend protocol version.
    #[error("pool backend journal header has unsupported {field} version {actual}")]
    UnsupportedVersion {
        /// Header version field that is unsupported.
        field: &'static str,
        /// Unsupported version value read from disk.
        actual: u16,
    },

    /// The durable header is for a different backend installation or network.
    #[error("pool backend journal header does not match expected {field}")]
    HeaderMismatch {
        /// Header field that differs from the expected identity or network.
        field: &'static str,
    },

    /// A complete event record is not strict schema-valid JSON.
    #[error(
        "pool backend journal {path} has a malformed complete record at line {line}: {source}",
        path = .path.display()
    )]
    MalformedRecord {
        /// Journal path containing the malformed record.
        path: PathBuf,
        /// One-based line number of the malformed record.
        line: u64,
        /// Strict JSON decoding error for the record.
        #[source]
        source: serde_json::Error,
    },

    /// A line exceeds the defensive per-record allocation bound.
    #[error("pool backend journal line {line} exceeds the {maximum}-byte record limit")]
    RecordTooLarge {
        /// One-based line number of the oversized record.
        line: u64,
        /// Maximum permitted encoded record size in bytes.
        maximum: usize,
    },

    /// The whole journal exceeds its defensive byte bound.
    #[error("pool backend journal exceeds the {maximum}-byte safety limit")]
    JournalTooLarge {
        /// Maximum permitted journal size in bytes.
        maximum: u64,
    },

    /// The in-memory foundation reached its explicit event-count bound.
    #[error("pool backend journal reached the {maximum}-event safety limit")]
    EventCapacity {
        /// Maximum number of retained journal events.
        maximum: usize,
    },

    /// A record or length computation overflowed.
    #[error("pool backend journal length or sequence overflowed")]
    Overflow,

    /// Memory could not be reserved before replay or append.
    #[error("pool backend journal could not reserve bounded replay memory: {0}")]
    Allocation(
        /// Allocation failure reported while reserving bounded storage.
        #[source]
        std::collections::TryReserveError,
    ),

    /// A full protocol event failed its own pinned v1 validation.
    #[error("pool backend journal event {event_seq} is invalid: {source}")]
    InvalidEvent {
        /// Sequence number of the invalid event.
        event_seq: u64,
        /// Protocol validation error returned for the event.
        #[source]
        source: ProtocolError,
    },

    /// The outer record and embedded event disagree about their sequence.
    #[error(
        "pool backend journal record sequence {record_seq} does not match embedded event sequence {event_seq}"
    )]
    EventSequenceMismatch {
        /// Sequence number stored in the journal record envelope.
        record_seq: u64,
        /// Sequence number stored in the embedded backend event.
        event_seq: u64,
    },

    /// An append or replay would create a gap, duplicate, or zero sequence.
    #[error("pool backend journal expected event sequence {expected}, found {actual}")]
    NonContiguousSequence {
        /// Exact next sequence number required by the journal.
        expected: u64,
        /// Sequence number supplied by the record or caller.
        actual: u64,
    },

    /// An event's previous digest does not link to the prior durable record.
    #[error("pool backend journal digest chain is broken at event {event_seq}")]
    PreviousDigestMismatch {
        /// Sequence number at which the digest link is broken.
        event_seq: u64,
    },

    /// An event's recorded digest does not authenticate its canonical content.
    #[error("pool backend journal event {event_seq} has an invalid digest")]
    DigestMismatch {
        /// Sequence number whose persisted digest is invalid.
        event_seq: u64,
    },

    /// Serialization of a known schema unexpectedly failed.
    #[error("failed to serialize canonical {context}: {source}")]
    Serialization {
        /// Schema context being serialized.
        context: &'static str,
        /// JSON serialization error.
        #[source]
        source: serde_json::Error,
    },

    /// One otherwise-valid event cannot fit in the protocol's 64 KiB page.
    #[error("pool backend journal event {event_seq} cannot fit in a {maximum}-byte protocol page")]
    EventTooLargeForPage {
        /// Sequence number of the event that cannot fit in one page.
        event_seq: u64,
        /// Maximum permitted protocol page size in bytes.
        maximum: usize,
    },

    /// `read_events` received a limit outside the protocol range.
    #[error("event page limit must be in 1..={maximum}, found {actual}")]
    InvalidPageLimit {
        /// Page limit supplied by the caller.
        actual: u16,
        /// Maximum page-item limit allowed by protocol v1.
        maximum: u16,
    },

    /// `read_events` received a cursor beyond the durable watermark.
    #[error("event cursor {after} is beyond current durable sequence {current}")]
    CursorBeyondEnd {
        /// Cursor supplied by the caller.
        after: u64,
        /// Current durable event sequence.
        current: u64,
    },

    /// A prior write, sync, or live-path verification failed. Restarting and
    /// replaying is required before any state can be trusted again.
    #[error(
        "pool backend journal is poisoned after a write, sync, or path failure; restart to recover"
    )]
    Poisoned,

    /// The journal mutex was poisoned by an internal panic.
    #[error("pool backend journal mutex is poisoned; restart before continuing")]
    MutexPoisoned,
}

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum HeaderRecordKind {
    PoolBackendJournalHeader,
}

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum EventRecordKind {
    PoolBackendJournalEvent,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct JournalHeader {
    record: HeaderRecordKind,
    format_version: u16,
    backend_protocol_version: u16,
    backend_instance: CanonicalUuid,
    journal_stream: CanonicalUuid,
    wcash_genesis: Hex32,
    zcash_genesis: Hex32,
    wcash_payout_commitment: Hex32,
    zcash_payout_commitment: Hex32,
    chain_id: u32,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PersistedEventRecord {
    record: EventRecordKind,
    event_seq: u64,
    event: BackendEvent,
    previous_digest: Hex32,
    digest: Hex32,
}

#[derive(Serialize)]
struct CanonicalUnsignedEventRecord<'a> {
    record: EventRecordKind,
    event_seq: u64,
    event: &'a BackendEvent,
    previous_digest: &'a Hex32,
}

struct JournalState {
    file: File,
    bytes_written: u64,
    last_digest: Hex32,
    events: Vec<BackendEvent>,
    poisoned: bool,
}

/// A bounded page read directly from the authoritative durable event sequence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JournalEventPage {
    /// Exclusive cursor supplied to [`PoolBackendJournal::read_events`].
    pub after_event_seq: u64,
    /// Last returned event sequence, or `after_event_seq` for an empty page.
    pub next_event_seq: u64,
    /// True when this page reaches the current durable journal end.
    pub complete: bool,
    /// Strictly contiguous protocol-v1 events.
    pub events: Vec<BackendEvent>,
}

/// Locked, durable protocol-v1 backend journal.
pub struct PoolBackendJournal {
    path: PathBuf,
    header: JournalHeader,
    state: Mutex<JournalState>,
}

impl PoolBackendJournal {
    /// Creates a brand-new journal without replacing any existing filesystem
    /// object. A fresh random stream UUID becomes the durable sequence
    /// namespace, so deleting and recreating a journal cannot silently reuse
    /// an earlier event namespace.
    #[allow(clippy::too_many_arguments)]
    pub fn create_new(
        path: impl AsRef<Path>,
        backend_instance: CanonicalUuid,
        wcash_genesis: Hex32,
        zcash_genesis: Hex32,
        wcash_payout_commitment: Hex32,
        zcash_payout_commitment: Hex32,
        chain_id: u32,
    ) -> Result<Self, PoolBackendJournalError> {
        let journal_stream = fresh_journal_stream(backend_instance)?;
        Self::create_new_with_stream(
            path,
            backend_instance,
            journal_stream,
            wcash_genesis,
            zcash_genesis,
            wcash_payout_commitment,
            zcash_payout_commitment,
            chain_id,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn create_new_with_stream(
        path: impl AsRef<Path>,
        backend_instance: CanonicalUuid,
        journal_stream: CanonicalUuid,
        wcash_genesis: Hex32,
        zcash_genesis: Hex32,
        wcash_payout_commitment: Hex32,
        zcash_payout_commitment: Hex32,
        chain_id: u32,
    ) -> Result<Self, PoolBackendJournalError> {
        validate_identity_and_network(
            backend_instance,
            Some(journal_stream),
            &wcash_genesis,
            &zcash_genesis,
            &wcash_payout_commitment,
            &zcash_payout_commitment,
            chain_id,
        )?;

        let header = JournalHeader {
            record: HeaderRecordKind::PoolBackendJournalHeader,
            format_version: JOURNAL_FORMAT_VERSION,
            backend_protocol_version: BACKEND_PROTOCOL_VERSION,
            backend_instance,
            journal_stream,
            wcash_genesis,
            zcash_genesis,
            wcash_payout_commitment,
            zcash_payout_commitment,
            chain_id,
        };
        let last_digest = digest_header(&header)?;
        let mut encoded_header = canonical_json(&header, "journal header")?;
        encoded_header.push(b'\n');
        check_record_size(encoded_header.len(), 1)?;

        let path = checked_journal_path(path.as_ref())?;
        match fs::symlink_metadata(&path) {
            Ok(_) => return Err(PoolBackendJournalError::AlreadyExists { path }),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(source) => return Err(journal_io("inspect", &path, source)),
        }

        let mut options = OpenOptions::new();
        options.read(true).append(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(PRIVATE_FILE_MODE);
        }
        let mut file = match options.open(&path) {
            Ok(file) => file,
            Err(source) if source.kind() == io::ErrorKind::AlreadyExists => {
                return Err(PoolBackendJournalError::AlreadyExists { path });
            }
            Err(source) => return Err(journal_io("create", &path, source)),
        };

        fs2::FileExt::try_lock_exclusive(&file).map_err(|source| {
            PoolBackendJournalError::Lock {
                path: path.clone(),
                source,
            }
        })?;

        #[cfg(unix)]
        file.set_permissions(std::os::unix::fs::PermissionsExt::from_mode(
            PRIVATE_FILE_MODE,
        ))
        .map_err(|source| journal_io("set permissions on", &path, source))?;

        verify_locked_journal_path(&file, &path, 0)?;
        file.write_all(&encoded_header)
            .map_err(|source| journal_io("write header to", &path, source))?;
        file.sync_all()
            .map_err(|source| journal_io("fsync", &path, source))?;
        let bytes_written =
            u64::try_from(encoded_header.len()).map_err(|_| PoolBackendJournalError::Overflow)?;
        verify_locked_journal_path(&file, &path, bytes_written)?;
        sync_parent_directory(&path)?;
        file.seek(SeekFrom::End(0))
            .map_err(|source| journal_io("seek", &path, source))?;

        Ok(Self {
            path,
            header,
            state: Mutex::new(JournalState {
                file,
                bytes_written,
                last_digest,
                events: Vec::new(),
                poisoned: false,
            }),
        })
    }

    /// Opens and replays an existing journal after checking its backend and
    /// chain identity. The stream UUID is recovered exclusively from the
    /// validated durable header.
    #[allow(clippy::too_many_arguments)]
    pub fn open_existing(
        path: impl AsRef<Path>,
        expected_backend_instance: CanonicalUuid,
        expected_wcash_genesis: Hex32,
        expected_zcash_genesis: Hex32,
        expected_wcash_payout_commitment: Hex32,
        expected_zcash_payout_commitment: Hex32,
        expected_chain_id: u32,
    ) -> Result<Self, PoolBackendJournalError> {
        validate_identity_and_network(
            expected_backend_instance,
            None,
            &expected_wcash_genesis,
            &expected_zcash_genesis,
            &expected_wcash_payout_commitment,
            &expected_zcash_payout_commitment,
            expected_chain_id,
        )?;

        let path = checked_journal_path(path.as_ref())?;
        preflight_existing_journal(&path)?;

        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(&path)
            .map_err(|source| journal_io("open", &path, source))?;
        fs2::FileExt::try_lock_exclusive(&file).map_err(|source| {
            PoolBackendJournalError::Lock {
                path: path.clone(),
                source,
            }
        })?;

        let initial_length = file
            .metadata()
            .map_err(|source| journal_io("inspect", &path, source))?
            .len();
        if initial_length == 0 {
            return Err(PoolBackendJournalError::EmptyJournal { path });
        }
        if initial_length > MAX_JOURNAL_BYTES {
            return Err(PoolBackendJournalError::JournalTooLarge {
                maximum: MAX_JOURNAL_BYTES,
            });
        }
        verify_locked_journal_path(&file, &path, initial_length)?;

        let mut replay_file = file
            .try_clone()
            .map_err(|source| journal_io("clone descriptor for replay of", &path, source))?;
        replay_file
            .seek(SeekFrom::Start(0))
            .map_err(|source| journal_io("seek", &path, source))?;
        let mut reader = BufReader::new(replay_file);

        let header_line = read_bounded_line(&mut reader, &path, 1)?
            .ok_or_else(|| PoolBackendJournalError::EmptyJournal { path: path.clone() })?;
        if !header_line.terminated {
            return Err(PoolBackendJournalError::UnterminatedHeader { path });
        }
        let header: JournalHeader =
            serde_json::from_slice(&header_line.bytes).map_err(|source| {
                PoolBackendJournalError::MalformedHeader {
                    path: path.clone(),
                    source,
                }
            })?;
        validate_header(&header)?;
        validate_expected_header(
            &header,
            expected_backend_instance,
            &expected_wcash_genesis,
            &expected_zcash_genesis,
            &expected_wcash_payout_commitment,
            &expected_zcash_payout_commitment,
            expected_chain_id,
        )?;

        let mut complete_bytes = header_line.consumed;
        let mut previous_digest = digest_header(&header)?;
        let mut events = Vec::new();
        let mut line_number = 2u64;
        let mut repaired_or_recovered_tail = false;

        while let Some(line) = read_bounded_line(&mut reader, &path, line_number)? {
            if !line.terminated {
                match serde_json::from_slice::<PersistedEventRecord>(&line.bytes) {
                    Ok(record) => {
                        // A crash can occur after the complete record body was
                        // written but before its newline or acknowledgement.
                        // Preserve that authenticated event and repair only
                        // the record delimiter. A client retry will then
                        // observe the original durable sequence idempotently.
                        validate_replayed_record(&record, &previous_digest, events.len())?;
                        ensure_event_fits_page(&record.event)?;
                        let repaired_consumed = line
                            .consumed
                            .checked_add(1)
                            .ok_or(PoolBackendJournalError::Overflow)?;
                        checked_append_limits(events.len(), complete_bytes, repaired_consumed)?;
                        verify_locked_journal_path(&file, &path, initial_length)?;
                        file.write_all(b"\n")
                            .map_err(|source| journal_io("repair delimiter in", &path, source))?;
                        file.sync_all()
                            .map_err(|source| journal_io("fsync repaired", &path, source))?;
                        complete_bytes = complete_bytes
                            .checked_add(repaired_consumed)
                            .ok_or(PoolBackendJournalError::Overflow)?;
                        verify_locked_journal_path(&file, &path, complete_bytes)?;
                        previous_digest = record.digest;
                        events
                            .try_reserve(1)
                            .map_err(PoolBackendJournalError::Allocation)?;
                        events.push(record.event);
                    }
                    Err(source) if source.classify() == serde_json::error::Category::Eof => {
                        // Only an incomplete JSON value is an unambiguous torn
                        // append. Never discard a complete, authenticated event
                        // merely because its trailing newline was lost.
                        verify_locked_journal_path(&file, &path, initial_length)?;
                        file.set_len(complete_bytes).map_err(|source| {
                            journal_io("truncate torn tail from", &path, source)
                        })?;
                        file.sync_all()
                            .map_err(|source| journal_io("fsync recovered", &path, source))?;
                        verify_locked_journal_path(&file, &path, complete_bytes)?;
                    }
                    Err(source) => {
                        return Err(PoolBackendJournalError::MalformedRecord {
                            path,
                            line: line_number,
                            source,
                        });
                    }
                }
                repaired_or_recovered_tail = true;
                break;
            }

            let record: PersistedEventRecord =
                serde_json::from_slice(&line.bytes).map_err(|source| {
                    PoolBackendJournalError::MalformedRecord {
                        path: path.clone(),
                        line: line_number,
                        source,
                    }
                })?;
            validate_replayed_record(&record, &previous_digest, events.len())?;
            ensure_event_fits_page(&record.event)?;
            checked_append_limits(events.len(), complete_bytes, line.consumed)?;
            events
                .try_reserve(1)
                .map_err(PoolBackendJournalError::Allocation)?;
            complete_bytes = complete_bytes
                .checked_add(line.consumed)
                .ok_or(PoolBackendJournalError::Overflow)?;
            previous_digest = record.digest;
            events.push(record.event);
            line_number = line_number
                .checked_add(1)
                .ok_or(PoolBackendJournalError::Overflow)?;
        }

        if !repaired_or_recovered_tail {
            verify_locked_journal_path(&file, &path, initial_length)?;
            if complete_bytes != initial_length {
                return Err(PoolBackendJournalError::PathChanged { path });
            }
        }
        file.seek(SeekFrom::End(0))
            .map_err(|source| journal_io("seek", &path, source))?;

        Ok(Self {
            path,
            header,
            state: Mutex::new(JournalState {
                file,
                bytes_written: complete_bytes,
                last_digest: previous_digest,
                events,
                poisoned: false,
            }),
        })
    }

    /// Returns the stable journal sequence namespace recovered from the header.
    pub const fn journal_stream(&self) -> CanonicalUuid {
        self.header.journal_stream
    }

    /// Returns the stable backend installation identity bound into the header.
    pub const fn backend_instance(&self) -> CanonicalUuid {
        self.header.backend_instance
    }

    /// Returns the pinned Wcash genesis bytes.
    pub fn wcash_genesis(&self) -> &Hex32 {
        &self.header.wcash_genesis
    }

    /// Returns the pinned Zcash genesis bytes.
    pub fn zcash_genesis(&self) -> &Hex32 {
        &self.header.zcash_genesis
    }

    /// Returns the configured Wcash child block-reward recipient commitment.
    pub fn wcash_payout_commitment(&self) -> &Hex32 {
        &self.header.wcash_payout_commitment
    }

    /// Returns the configured Zcash parent block-reward recipient commitment.
    pub fn zcash_payout_commitment(&self) -> &Hex32 {
        &self.header.zcash_payout_commitment
    }

    /// Returns the nonzero Wcash AuxPoW chain ID.
    pub const fn chain_id(&self) -> u32 {
        self.header.chain_id
    }

    /// Returns the latest event sequence known to be durable.
    pub fn current_event_seq(&self) -> Result<u64, PoolBackendJournalError> {
        let state = self.lock_state()?;
        ensure_usable(&state)?;
        Ok(state.events.last().map_or(0, BackendEvent::event_seq))
    }

    /// Appends exactly the next validated event and fsyncs it before changing
    /// in-memory state or returning success.
    pub fn append_event(&self, event: BackendEvent) -> Result<(), PoolBackendJournalError> {
        event
            .validate()
            .map_err(|source| PoolBackendJournalError::InvalidEvent {
                event_seq: event.event_seq(),
                source,
            })?;
        ensure_event_fits_page(&event)?;

        let mut state = self.lock_state()?;
        ensure_usable(&state)?;
        let current = state.events.last().map_or(0, BackendEvent::event_seq);
        let expected = current
            .checked_add(1)
            .ok_or(PoolBackendJournalError::Overflow)?;
        if event.event_seq() != expected {
            return Err(PoolBackendJournalError::NonContiguousSequence {
                expected,
                actual: event.event_seq(),
            });
        }

        state
            .events
            .try_reserve(1)
            .map_err(PoolBackendJournalError::Allocation)?;
        let previous_digest = state.last_digest.clone();
        let digest = digest_event(&event, &previous_digest)?;
        let record = PersistedEventRecord {
            record: EventRecordKind::PoolBackendJournalEvent,
            event_seq: event.event_seq(),
            event,
            previous_digest,
            digest,
        };
        let mut encoded = canonical_json(&record, "journal event record")?;
        encoded.push(b'\n');
        check_record_size(encoded.len(), expected.saturating_add(1))?;
        let record_bytes =
            u64::try_from(encoded.len()).map_err(|_| PoolBackendJournalError::Overflow)?;
        let new_length =
            checked_append_limits(state.events.len(), state.bytes_written, record_bytes)?;

        if let Err(error) = verify_locked_journal_path(&state.file, &self.path, state.bytes_written)
        {
            state.poisoned = true;
            return Err(error);
        }
        if let Err(source) = state.file.write_all(&encoded) {
            state.poisoned = true;
            return Err(journal_io("append to", &self.path, source));
        }
        if let Err(source) = state.file.sync_all() {
            state.poisoned = true;
            return Err(journal_io("fsync", &self.path, source));
        }
        if let Err(error) = verify_locked_journal_path(&state.file, &self.path, new_length) {
            state.poisoned = true;
            return Err(error);
        }

        // Capacity was reserved before the write; all state changes happen only
        // after both the append and fsync have succeeded.
        state.bytes_written = new_length;
        state.last_digest = record.digest;
        state.events.push(record.event);
        Ok(())
    }

    /// Reads a bounded, contiguous page after an exclusive sequence cursor.
    ///
    /// In addition to the protocol item limit, this method conservatively sizes
    /// the exact JSON `events_page` payload with a maximum-width request ID and
    /// next cursor, stopping before the 64 KiB backend payload limit.
    pub fn read_events(
        &self,
        after_event_seq: u64,
        limit: u16,
    ) -> Result<JournalEventPage, PoolBackendJournalError> {
        if !(1..=MAX_EVENT_PAGE_ITEMS).contains(&limit) {
            return Err(PoolBackendJournalError::InvalidPageLimit {
                actual: limit,
                maximum: MAX_EVENT_PAGE_ITEMS,
            });
        }

        let state = self.lock_state()?;
        ensure_usable(&state)?;
        let current = state.events.last().map_or(0, BackendEvent::event_seq);
        if after_event_seq > current {
            return Err(PoolBackendJournalError::CursorBeyondEnd {
                after: after_event_seq,
                current,
            });
        }

        let start =
            usize::try_from(after_event_seq).map_err(|_| PoolBackendJournalError::Overflow)?;
        let available = &state.events[start..];
        let take = available.len().min(usize::from(limit));
        let mut events = Vec::new();
        events
            .try_reserve(take)
            .map_err(PoolBackendJournalError::Allocation)?;
        let mut payload_size = empty_events_page_payload_size(after_event_seq)?;

        for event in &available[..take] {
            let event_size = canonical_json(event, "backend event")?.len();
            let separator = usize::from(!events.is_empty());
            let candidate_size = payload_size
                .checked_add(separator)
                .and_then(|size| size.checked_add(event_size))
                .ok_or(PoolBackendJournalError::Overflow)?;
            if candidate_size > MAX_BACKEND_PAYLOAD_BYTES {
                break;
            }
            events.push(event.clone());
            payload_size = candidate_size;
        }

        if events.is_empty() && !available.is_empty() {
            return Err(PoolBackendJournalError::EventTooLargeForPage {
                event_seq: available[0].event_seq(),
                maximum: MAX_BACKEND_PAYLOAD_BYTES,
            });
        }
        let next_event_seq = events
            .last()
            .map_or(after_event_seq, BackendEvent::event_seq);
        Ok(JournalEventPage {
            after_event_seq,
            next_event_seq,
            complete: next_event_seq == current,
            events,
        })
    }

    fn lock_state(&self) -> Result<MutexGuard<'_, JournalState>, PoolBackendJournalError> {
        self.state
            .lock()
            .map_err(|_| PoolBackendJournalError::MutexPoisoned)
    }
}

fn fresh_journal_stream(
    backend_instance: CanonicalUuid,
) -> Result<CanonicalUuid, PoolBackendJournalError> {
    for _ in 0..8 {
        let candidate = CanonicalUuid::new(Uuid::new_v4());
        if candidate != backend_instance {
            return Ok(candidate);
        }
    }
    Err(PoolBackendJournalError::InvalidConfiguration(
        "could not create a unique random journal stream identity",
    ))
}

struct BoundedLine {
    bytes: Vec<u8>,
    terminated: bool,
    consumed: u64,
}

fn read_bounded_line<R: BufRead>(
    reader: &mut R,
    path: &Path,
    line_number: u64,
) -> Result<Option<BoundedLine>, PoolBackendJournalError> {
    let mut line = Vec::new();
    loop {
        let (chunk_len, terminated) = {
            let available = reader
                .fill_buf()
                .map_err(|source| journal_io("read", path, source))?;
            if available.is_empty() {
                if line.is_empty() {
                    return Ok(None);
                }
                let consumed =
                    u64::try_from(line.len()).map_err(|_| PoolBackendJournalError::Overflow)?;
                return Ok(Some(BoundedLine {
                    bytes: line,
                    terminated: false,
                    consumed,
                }));
            }
            match available.iter().position(|byte| *byte == b'\n') {
                Some(position) => (position, true),
                None => (available.len(), false),
            }
        };

        let resulting_size = line
            .len()
            .checked_add(chunk_len)
            .and_then(|size| size.checked_add(usize::from(terminated)))
            .ok_or(PoolBackendJournalError::Overflow)?;
        if resulting_size > MAX_JOURNAL_RECORD_BYTES {
            return Err(PoolBackendJournalError::RecordTooLarge {
                line: line_number,
                maximum: MAX_JOURNAL_RECORD_BYTES,
            });
        }
        let available = reader
            .fill_buf()
            .map_err(|source| journal_io("read", path, source))?;
        line.extend_from_slice(&available[..chunk_len]);
        reader.consume(chunk_len + usize::from(terminated));
        if terminated {
            let consumed =
                u64::try_from(resulting_size).map_err(|_| PoolBackendJournalError::Overflow)?;
            return Ok(Some(BoundedLine {
                bytes: line,
                terminated: true,
                consumed,
            }));
        }
    }
}

fn validate_replayed_record(
    record: &PersistedEventRecord,
    expected_previous_digest: &Hex32,
    prior_event_count: usize,
) -> Result<(), PoolBackendJournalError> {
    let expected = u64::try_from(prior_event_count)
        .map_err(|_| PoolBackendJournalError::Overflow)?
        .checked_add(1)
        .ok_or(PoolBackendJournalError::Overflow)?;
    if record.event_seq != expected {
        return Err(PoolBackendJournalError::NonContiguousSequence {
            expected,
            actual: record.event_seq,
        });
    }
    if record.event.event_seq() != record.event_seq {
        return Err(PoolBackendJournalError::EventSequenceMismatch {
            record_seq: record.event_seq,
            event_seq: record.event.event_seq(),
        });
    }
    record
        .event
        .validate()
        .map_err(|source| PoolBackendJournalError::InvalidEvent {
            event_seq: record.event_seq,
            source,
        })?;
    if &record.previous_digest != expected_previous_digest {
        return Err(PoolBackendJournalError::PreviousDigestMismatch {
            event_seq: record.event_seq,
        });
    }
    if record.digest != digest_event(&record.event, &record.previous_digest)? {
        return Err(PoolBackendJournalError::DigestMismatch {
            event_seq: record.event_seq,
        });
    }
    Ok(())
}

fn validate_header(header: &JournalHeader) -> Result<(), PoolBackendJournalError> {
    if header.format_version != JOURNAL_FORMAT_VERSION {
        return Err(PoolBackendJournalError::UnsupportedVersion {
            field: "journal format",
            actual: header.format_version,
        });
    }
    if header.backend_protocol_version != BACKEND_PROTOCOL_VERSION {
        return Err(PoolBackendJournalError::UnsupportedVersion {
            field: "backend protocol",
            actual: header.backend_protocol_version,
        });
    }
    validate_identity_and_network(
        header.backend_instance,
        Some(header.journal_stream),
        &header.wcash_genesis,
        &header.zcash_genesis,
        &header.wcash_payout_commitment,
        &header.zcash_payout_commitment,
        header.chain_id,
    )
}

fn validate_identity_and_network(
    backend_instance: CanonicalUuid,
    journal_stream: Option<CanonicalUuid>,
    wcash_genesis: &Hex32,
    zcash_genesis: &Hex32,
    wcash_payout_commitment: &Hex32,
    zcash_payout_commitment: &Hex32,
    chain_id: u32,
) -> Result<(), PoolBackendJournalError> {
    if backend_instance.is_nil() {
        return Err(PoolBackendJournalError::InvalidConfiguration(
            "backend_instance must be non-nil",
        ));
    }
    if let Some(journal_stream) = journal_stream {
        if journal_stream.is_nil() {
            return Err(PoolBackendJournalError::InvalidConfiguration(
                "journal_stream must be non-nil",
            ));
        }
        if journal_stream == backend_instance {
            return Err(PoolBackendJournalError::InvalidConfiguration(
                "backend_instance and journal_stream must be distinct",
            ));
        }
    }
    if wcash_genesis.is_zero() {
        return Err(PoolBackendJournalError::InvalidConfiguration(
            "wcash_genesis must be nonzero",
        ));
    }
    if zcash_genesis.is_zero() {
        return Err(PoolBackendJournalError::InvalidConfiguration(
            "zcash_genesis must be nonzero",
        ));
    }
    if wcash_payout_commitment.is_zero() {
        return Err(PoolBackendJournalError::InvalidConfiguration(
            "wcash_payout_commitment must be nonzero",
        ));
    }
    if zcash_payout_commitment.is_zero() {
        return Err(PoolBackendJournalError::InvalidConfiguration(
            "zcash_payout_commitment must be nonzero",
        ));
    }
    if chain_id == 0 {
        return Err(PoolBackendJournalError::InvalidConfiguration(
            "chain_id must be nonzero",
        ));
    }
    Ok(())
}

fn validate_expected_header(
    header: &JournalHeader,
    backend_instance: CanonicalUuid,
    wcash_genesis: &Hex32,
    zcash_genesis: &Hex32,
    wcash_payout_commitment: &Hex32,
    zcash_payout_commitment: &Hex32,
    chain_id: u32,
) -> Result<(), PoolBackendJournalError> {
    if header.backend_instance != backend_instance {
        return Err(PoolBackendJournalError::HeaderMismatch {
            field: "backend_instance",
        });
    }
    if &header.wcash_genesis != wcash_genesis {
        return Err(PoolBackendJournalError::HeaderMismatch {
            field: "wcash_genesis",
        });
    }
    if &header.zcash_genesis != zcash_genesis {
        return Err(PoolBackendJournalError::HeaderMismatch {
            field: "zcash_genesis",
        });
    }
    if &header.wcash_payout_commitment != wcash_payout_commitment {
        return Err(PoolBackendJournalError::HeaderMismatch {
            field: "wcash_payout_commitment",
        });
    }
    if &header.zcash_payout_commitment != zcash_payout_commitment {
        return Err(PoolBackendJournalError::HeaderMismatch {
            field: "zcash_payout_commitment",
        });
    }
    if header.chain_id != chain_id {
        return Err(PoolBackendJournalError::HeaderMismatch { field: "chain_id" });
    }
    Ok(())
}

fn digest_header(header: &JournalHeader) -> Result<Hex32, PoolBackendJournalError> {
    digest_canonical(HEADER_DIGEST_DOMAIN, header, "journal header")
}

fn digest_event(
    event: &BackendEvent,
    previous_digest: &Hex32,
) -> Result<Hex32, PoolBackendJournalError> {
    let unsigned = CanonicalUnsignedEventRecord {
        record: EventRecordKind::PoolBackendJournalEvent,
        event_seq: event.event_seq(),
        event,
        previous_digest,
    };
    digest_canonical(EVENT_DIGEST_DOMAIN, &unsigned, "unsigned event record")
}

fn digest_canonical<T: Serialize>(
    domain: &[u8],
    value: &T,
    context: &'static str,
) -> Result<Hex32, PoolBackendJournalError> {
    let canonical = canonical_json(value, context)?;
    let length = u64::try_from(canonical.len()).map_err(|_| PoolBackendJournalError::Overflow)?;
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update(length.to_be_bytes());
    hasher.update(canonical);
    Ok(Hex32::new(hasher.finalize().into()))
}

fn canonical_json<T: Serialize>(
    value: &T,
    context: &'static str,
) -> Result<Vec<u8>, PoolBackendJournalError> {
    // Every serialized value is a fixed-order struct or a pinned protocol
    // struct/enum containing no maps. Compact serde_json is consequently a
    // deterministic canonical encoding for this exact format version.
    serde_json::to_vec(value)
        .map_err(|source| PoolBackendJournalError::Serialization { context, source })
}

fn ensure_event_fits_page(event: &BackendEvent) -> Result<(), PoolBackendJournalError> {
    let after_event_seq =
        event
            .event_seq()
            .checked_sub(1)
            .ok_or(PoolBackendJournalError::NonContiguousSequence {
                expected: 1,
                actual: 0,
            })?;
    let base = empty_events_page_payload_size(after_event_seq)?;
    let event_size = canonical_json(event, "backend event")?.len();
    let total = base
        .checked_add(event_size)
        .ok_or(PoolBackendJournalError::Overflow)?;
    if total > MAX_BACKEND_PAYLOAD_BYTES {
        return Err(PoolBackendJournalError::EventTooLargeForPage {
            event_seq: event.event_seq(),
            maximum: MAX_BACKEND_PAYLOAD_BYTES,
        });
    }
    Ok(())
}

fn empty_events_page_payload_size(after_event_seq: u64) -> Result<usize, PoolBackendJournalError> {
    let envelope = BackendMessage::EventsPage {
        version: BACKEND_PROTOCOL_VERSION,
        id: u64::MAX,
        after_event_seq,
        next_event_seq: u64::MAX,
        complete: false,
        events: Vec::new(),
    };
    canonical_json(&envelope, "empty events-page envelope").map(|bytes| bytes.len())
}

fn check_record_size(
    encoded_bytes: usize,
    line_number: u64,
) -> Result<(), PoolBackendJournalError> {
    if encoded_bytes > MAX_JOURNAL_RECORD_BYTES {
        return Err(PoolBackendJournalError::RecordTooLarge {
            line: line_number,
            maximum: MAX_JOURNAL_RECORD_BYTES,
        });
    }
    Ok(())
}

fn checked_append_limits(
    current_event_count: usize,
    current_bytes: u64,
    record_bytes: u64,
) -> Result<u64, PoolBackendJournalError> {
    if current_event_count >= MAX_JOURNAL_EVENTS {
        return Err(PoolBackendJournalError::EventCapacity {
            maximum: MAX_JOURNAL_EVENTS,
        });
    }
    let new_length = current_bytes
        .checked_add(record_bytes)
        .ok_or(PoolBackendJournalError::Overflow)?;
    if new_length > MAX_JOURNAL_BYTES {
        return Err(PoolBackendJournalError::JournalTooLarge {
            maximum: MAX_JOURNAL_BYTES,
        });
    }
    Ok(new_length)
}

fn ensure_usable(state: &JournalState) -> Result<(), PoolBackendJournalError> {
    if state.poisoned {
        Err(PoolBackendJournalError::Poisoned)
    } else {
        Ok(())
    }
}

fn checked_journal_path(path: &Path) -> Result<PathBuf, PoolBackendJournalError> {
    if path.as_os_str().is_empty() {
        return Err(PoolBackendJournalError::InvalidPath(
            "path must not be empty".to_string(),
        ));
    }
    if !path.is_absolute() {
        return Err(PoolBackendJournalError::InvalidPath(format!(
            "{} must be absolute",
            path.display()
        )));
    }
    if path.file_name().is_none() {
        return Err(PoolBackendJournalError::InvalidPath(format!(
            "{} has no journal file name",
            path.display()
        )));
    }
    if path
        .components()
        .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
    {
        return Err(PoolBackendJournalError::InvalidPath(format!(
            "{} must not contain `.` or `..` components",
            path.display()
        )));
    }
    let normalized: PathBuf = path.components().collect();
    if normalized.as_os_str() != path.as_os_str() {
        return Err(PoolBackendJournalError::InvalidPath(format!(
            "{} must be lexically canonical without redundant separators",
            path.display()
        )));
    }
    let parent = path.parent().ok_or_else(|| {
        PoolBackendJournalError::InvalidPath(format!("{} has no journal parent", path.display()))
    })?;
    validate_parent_directory(parent)?;
    Ok(path.to_path_buf())
}

fn parent_directory(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

fn validate_parent_directory(parent: &Path) -> Result<(), PoolBackendJournalError> {
    let metadata = fs::symlink_metadata(parent)
        .map_err(|source| journal_io("inspect parent of", parent, source))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(PoolBackendJournalError::UnsafeParentType {
            path: parent.to_path_buf(),
        });
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        let canonical_parent = fs::canonicalize(parent)
            .map_err(|source| journal_io("canonicalize", parent, source))?;
        if canonical_parent.as_os_str() != parent.as_os_str() {
            return Err(PoolBackendJournalError::UnsafeParentType {
                path: parent.to_path_buf(),
            });
        }
        validate_parent_owner(parent, metadata.uid())?;
        if metadata.permissions().mode() & 0o022 != 0 {
            return Err(PoolBackendJournalError::UnsafeParentMode {
                path: parent.to_path_buf(),
            });
        }
    }
    Ok(())
}

fn preflight_existing_journal(path: &Path) -> Result<(), PoolBackendJournalError> {
    let metadata =
        fs::symlink_metadata(path).map_err(|source| journal_io("inspect", path, source))?;
    if metadata.file_type().is_symlink() {
        return Err(PoolBackendJournalError::SymbolicLink {
            path: path.to_path_buf(),
        });
    }
    if !metadata.is_file() {
        return Err(PoolBackendJournalError::NotRegularFile {
            path: path.to_path_buf(),
        });
    }
    validate_private_metadata(&metadata, path)
}

fn verify_locked_journal_path(
    file: &File,
    path: &Path,
    expected_length: u64,
) -> Result<(), PoolBackendJournalError> {
    validate_parent_directory(parent_directory(path))?;
    let locked = file
        .metadata()
        .map_err(|source| journal_io("inspect locked descriptor for", path, source))?;
    if !locked.is_file() {
        return Err(PoolBackendJournalError::NotRegularFile {
            path: path.to_path_buf(),
        });
    }
    validate_private_metadata(&locked, path)?;

    let current = fs::symlink_metadata(path).map_err(|_| PoolBackendJournalError::PathChanged {
        path: path.to_path_buf(),
    })?;
    if current.file_type().is_symlink() || !current.is_file() {
        return Err(PoolBackendJournalError::PathChanged {
            path: path.to_path_buf(),
        });
    }
    validate_private_metadata(&current, path)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if locked.dev() != current.dev() || locked.ino() != current.ino() {
            return Err(PoolBackendJournalError::PathChanged {
                path: path.to_path_buf(),
            });
        }
    }
    if locked.len() != expected_length || current.len() != expected_length {
        return Err(PoolBackendJournalError::PathChanged {
            path: path.to_path_buf(),
        });
    }
    Ok(())
}

fn validate_private_metadata(
    metadata: &fs::Metadata,
    path: &Path,
) -> Result<(), PoolBackendJournalError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        validate_file_owner(path, metadata.uid())?;
        let mode = metadata.permissions().mode() & 0o7777;
        if mode != PRIVATE_FILE_MODE {
            return Err(PoolBackendJournalError::UnsafeFileMode {
                path: path.to_path_buf(),
                mode,
            });
        }
        let links = metadata.nlink();
        if links != 1 {
            return Err(PoolBackendJournalError::HardLinked {
                path: path.to_path_buf(),
                links,
            });
        }
    }
    Ok(())
}

#[cfg(unix)]
fn validate_parent_owner(path: &Path, owner_uid: u32) -> Result<(), PoolBackendJournalError> {
    let effective_uid = nix::unistd::geteuid().as_raw();
    if owner_uid != effective_uid {
        return Err(PoolBackendJournalError::UnsafeParentOwner {
            path: path.to_path_buf(),
            owner_uid,
            effective_uid,
        });
    }
    Ok(())
}

#[cfg(unix)]
fn validate_file_owner(path: &Path, owner_uid: u32) -> Result<(), PoolBackendJournalError> {
    let effective_uid = nix::unistd::geteuid().as_raw();
    if owner_uid != effective_uid {
        return Err(PoolBackendJournalError::UnsafeFileOwner {
            path: path.to_path_buf(),
            owner_uid,
            effective_uid,
        });
    }
    Ok(())
}

#[cfg(unix)]
fn sync_parent_directory(path: &Path) -> Result<(), PoolBackendJournalError> {
    let parent = parent_directory(path);
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| journal_io("fsync parent of", path, source))
}

#[cfg(not(unix))]
fn sync_parent_directory(_path: &Path) -> Result<(), PoolBackendJournalError> {
    Ok(())
}

fn journal_io(operation: &'static str, path: &Path, source: io::Error) -> PoolBackendJournalError {
    PoolBackendJournalError::Io {
        operation,
        path: path.to_path_buf(),
        source,
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use serde_json::{json, Value};
    use tempfile::TempDir;

    use super::*;

    const TEST_CHAIN_ID: u32 = 0x5743_4153;

    #[derive(Clone)]
    struct TestConfig {
        backend_instance: CanonicalUuid,
        journal_stream: CanonicalUuid,
        wcash_genesis: Hex32,
        zcash_genesis: Hex32,
        wcash_payout_commitment: Hex32,
        zcash_payout_commitment: Hex32,
        chain_id: u32,
    }

    fn uuid(last: u8) -> CanonicalUuid {
        serde_json::from_value(Value::String(format!(
            "00000000-0000-4000-8000-{last:012x}"
        )))
        .expect("canonical non-nil test UUID")
    }

    fn nil_uuid() -> CanonicalUuid {
        serde_json::from_value(Value::String(
            "00000000-0000-0000-0000-000000000000".to_string(),
        ))
        .expect("canonical nil UUID")
    }

    fn config() -> TestConfig {
        TestConfig {
            backend_instance: uuid(1),
            journal_stream: uuid(2),
            wcash_genesis: Hex32::new([0x11; 32]),
            zcash_genesis: Hex32::new([0x22; 32]),
            wcash_payout_commitment: Hex32::new([0x33; 32]),
            zcash_payout_commitment: Hex32::new([0x44; 32]),
            chain_id: TEST_CHAIN_ID,
        }
    }

    fn private_temp_dir() -> TempDir {
        let canonical_temporary_root =
            fs::canonicalize(std::env::temp_dir()).expect("canonical temporary root");
        let directory = tempfile::Builder::new()
            .tempdir_in(canonical_temporary_root)
            .expect("temporary directory");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700))
                .expect("make temporary directory private");
        }
        directory
    }

    fn create(path: &Path, config: &TestConfig) -> PoolBackendJournal {
        PoolBackendJournal::create_new_with_stream(
            path,
            config.backend_instance,
            config.journal_stream,
            config.wcash_genesis.clone(),
            config.zcash_genesis.clone(),
            config.wcash_payout_commitment.clone(),
            config.zcash_payout_commitment.clone(),
            config.chain_id,
        )
        .expect("create test journal")
    }

    fn open(path: &Path, config: &TestConfig) -> PoolBackendJournal {
        PoolBackendJournal::open_existing(
            path,
            config.backend_instance,
            config.wcash_genesis.clone(),
            config.zcash_genesis.clone(),
            config.wcash_payout_commitment.clone(),
            config.zcash_payout_commitment.clone(),
            config.chain_id,
        )
        .expect("open test journal")
    }

    fn event(event_seq: u64, byte: u8) -> BackendEvent {
        BackendEvent::GenerationClosed {
            event_seq,
            job_id: Hex32::new([byte; 32]),
        }
    }

    fn append_raw(path: &Path, bytes: &[u8]) {
        let mut file = OpenOptions::new()
            .append(true)
            .open(path)
            .expect("open raw journal fixture");
        file.write_all(bytes).expect("append raw fixture bytes");
        file.sync_all().expect("sync raw fixture bytes");
    }

    fn write_private_new(path: &Path, bytes: &[u8]) {
        let mut options = OpenOptions::new();
        options.create_new(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(PRIVATE_FILE_MODE);
        }
        let mut file = options.open(path).expect("create private fixture");
        file.write_all(bytes).expect("write private fixture");
        file.sync_all().expect("sync private fixture");
        #[cfg(unix)]
        file.set_permissions(std::os::unix::fs::PermissionsExt::from_mode(
            PRIVATE_FILE_MODE,
        ))
        .expect("set exact fixture mode");
    }

    fn rewrite_record(path: &Path, record_index: usize, mutate: impl FnOnce(&mut Value)) {
        let bytes = fs::read(path).expect("read journal fixture");
        let mut lines: Vec<Vec<u8>> = bytes
            .split_inclusive(|byte| *byte == b'\n')
            .map(ToOwned::to_owned)
            .collect();
        let line = lines.get_mut(record_index).expect("fixture record exists");
        let json_bytes = line.strip_suffix(b"\n").unwrap_or(line);
        let mut value: Value = serde_json::from_slice(json_bytes).expect("fixture JSON");
        mutate(&mut value);
        *line = serde_json::to_vec(&value).expect("serialize changed fixture");
        line.push(b'\n');
        let rewritten: Vec<u8> = lines.into_iter().flatten().collect();
        fs::write(path, rewritten).expect("rewrite journal fixture");
        #[cfg(unix)]
        fs::set_permissions(
            path,
            std::os::unix::fs::PermissionsExt::from_mode(PRIVATE_FILE_MODE),
        )
        .expect("retain private fixture mode");
    }

    #[test]
    fn create_and_stable_reopen_bind_identity_and_network() {
        let directory = private_temp_dir();
        let path = directory.path().join("backend.jsonl");
        let config = config();
        let journal = create(&path, &config);
        assert_eq!(journal.current_event_seq().expect("watermark"), 0);
        assert_eq!(journal.journal_stream(), config.journal_stream);
        assert_eq!(journal.backend_instance(), config.backend_instance);
        assert_eq!(journal.wcash_genesis(), &config.wcash_genesis);
        assert_eq!(journal.zcash_genesis(), &config.zcash_genesis);
        assert_eq!(
            journal.wcash_payout_commitment(),
            &config.wcash_payout_commitment
        );
        assert_eq!(
            journal.zcash_payout_commitment(),
            &config.zcash_payout_commitment
        );
        assert_eq!(journal.chain_id(), config.chain_id);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&path)
                    .expect("journal metadata")
                    .permissions()
                    .mode()
                    & 0o7777,
                PRIVATE_FILE_MODE
            );
        }
        let initial_bytes = fs::read(&path).expect("initial journal bytes");
        drop(journal);

        let reopened = open(&path, &config);
        assert_eq!(reopened.journal_stream(), config.journal_stream);
        assert_eq!(reopened.current_event_seq().expect("watermark"), 0);
        assert_eq!(
            fs::read(&path).expect("reopened journal bytes"),
            initial_bytes
        );
    }

    #[test]
    fn duplicate_create_and_invalid_fresh_identity_are_rejected() {
        let directory = private_temp_dir();
        let path = directory.path().join("backend.jsonl");
        let config = config();
        let _journal = create(&path, &config);
        assert!(matches!(
            PoolBackendJournal::create_new(
                &path,
                config.backend_instance,
                config.wcash_genesis.clone(),
                config.zcash_genesis.clone(),
                config.wcash_payout_commitment.clone(),
                config.zcash_payout_commitment.clone(),
                config.chain_id,
            ),
            Err(PoolBackendJournalError::AlreadyExists { .. })
        ));

        let nil_path = directory.path().join("nil.jsonl");
        assert!(matches!(
            PoolBackendJournal::create_new(
                &nil_path,
                nil_uuid(),
                config.wcash_genesis.clone(),
                config.zcash_genesis.clone(),
                config.wcash_payout_commitment.clone(),
                config.zcash_payout_commitment.clone(),
                config.chain_id,
            ),
            Err(PoolBackendJournalError::InvalidConfiguration(_))
        ));
        assert!(!nil_path.exists());

        let fresh_path = directory.path().join("fresh.jsonl");
        let fresh = PoolBackendJournal::create_new(
            &fresh_path,
            config.backend_instance,
            config.wcash_genesis.clone(),
            config.zcash_genesis.clone(),
            config.wcash_payout_commitment.clone(),
            config.zcash_payout_commitment.clone(),
            config.chain_id,
        )
        .expect("generate a fresh journal stream");
        let first_stream = fresh.journal_stream();
        assert_ne!(first_stream, config.backend_instance);
        assert_eq!(first_stream.get().get_version_num(), 4);
        drop(fresh);
        fs::remove_file(&fresh_path).expect("remove first journal generation");
        let recreated = PoolBackendJournal::create_new(
            &fresh_path,
            config.backend_instance,
            config.wcash_genesis.clone(),
            config.zcash_genesis.clone(),
            config.wcash_payout_commitment.clone(),
            config.zcash_payout_commitment.clone(),
            config.chain_id,
        )
        .expect("recreate journal with a new sequence namespace");
        assert_ne!(recreated.journal_stream(), first_stream);
    }

    #[test]
    fn paths_must_be_absolute_lexically_canonical_and_real() {
        let directory = private_temp_dir();
        let config = config();
        assert!(matches!(
            PoolBackendJournal::create_new(
                "relative.jsonl",
                config.backend_instance,
                config.wcash_genesis.clone(),
                config.zcash_genesis.clone(),
                config.wcash_payout_commitment.clone(),
                config.zcash_payout_commitment.clone(),
                config.chain_id,
            ),
            Err(PoolBackendJournalError::InvalidPath(_))
        ));

        let dotted = directory.path().join(".").join("dotted.jsonl");
        assert!(matches!(
            PoolBackendJournal::create_new(
                &dotted,
                config.backend_instance,
                config.wcash_genesis.clone(),
                config.zcash_genesis.clone(),
                config.wcash_payout_commitment.clone(),
                config.zcash_payout_commitment.clone(),
                config.chain_id,
            ),
            Err(PoolBackendJournalError::InvalidPath(_))
        ));
        assert!(!directory.path().join("dotted.jsonl").exists());

        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;

            let real_parent = directory.path().join("real-parent");
            fs::create_dir(&real_parent).expect("create real parent");
            let alias = directory.path().join("parent-alias");
            symlink(&real_parent, &alias).expect("create parent symlink");
            let aliased_path = alias.join("backend.jsonl");
            assert!(matches!(
                PoolBackendJournal::create_new(
                    &aliased_path,
                    config.backend_instance,
                    config.wcash_genesis.clone(),
                    config.zcash_genesis.clone(),
                    config.wcash_payout_commitment.clone(),
                    config.zcash_payout_commitment.clone(),
                    config.chain_id,
                ),
                Err(PoolBackendJournalError::UnsafeParentType { .. })
            ));

            let wrong_uid = nix::unistd::geteuid().as_raw().wrapping_add(1);
            assert!(matches!(
                validate_parent_owner(directory.path(), wrong_uid),
                Err(PoolBackendJournalError::UnsafeParentOwner { .. })
            ));
            assert!(matches!(
                validate_file_owner(&directory.path().join("file"), wrong_uid),
                Err(PoolBackendJournalError::UnsafeFileOwner { .. })
            ));
        }
    }

    #[test]
    fn reopen_rejects_identity_and_each_network_mismatch() {
        let directory = private_temp_dir();
        let path = directory.path().join("backend.jsonl");
        let config = config();
        drop(create(&path, &config));

        let attempts = [
            PoolBackendJournal::open_existing(
                &path,
                uuid(9),
                config.wcash_genesis.clone(),
                config.zcash_genesis.clone(),
                config.wcash_payout_commitment.clone(),
                config.zcash_payout_commitment.clone(),
                config.chain_id,
            ),
            PoolBackendJournal::open_existing(
                &path,
                config.backend_instance,
                Hex32::new([0x91; 32]),
                config.zcash_genesis.clone(),
                config.wcash_payout_commitment.clone(),
                config.zcash_payout_commitment.clone(),
                config.chain_id,
            ),
            PoolBackendJournal::open_existing(
                &path,
                config.backend_instance,
                config.wcash_genesis.clone(),
                Hex32::new([0x92; 32]),
                config.wcash_payout_commitment.clone(),
                config.zcash_payout_commitment.clone(),
                config.chain_id,
            ),
            PoolBackendJournal::open_existing(
                &path,
                config.backend_instance,
                config.wcash_genesis.clone(),
                config.zcash_genesis.clone(),
                Hex32::new([0x93; 32]),
                config.zcash_payout_commitment.clone(),
                config.chain_id,
            ),
            PoolBackendJournal::open_existing(
                &path,
                config.backend_instance,
                config.wcash_genesis.clone(),
                config.zcash_genesis.clone(),
                config.wcash_payout_commitment.clone(),
                Hex32::new([0x94; 32]),
                config.chain_id,
            ),
            PoolBackendJournal::open_existing(
                &path,
                config.backend_instance,
                config.wcash_genesis.clone(),
                config.zcash_genesis.clone(),
                config.wcash_payout_commitment.clone(),
                config.zcash_payout_commitment.clone(),
                config.chain_id + 1,
            ),
        ];
        for attempt in attempts {
            assert!(matches!(
                attempt,
                Err(PoolBackendJournalError::HeaderMismatch { .. })
            ));
        }
        assert_eq!(open(&path, &config).current_event_seq().unwrap(), 0);
    }

    #[test]
    fn append_replay_and_page_reads_are_contiguous() {
        let directory = private_temp_dir();
        let path = directory.path().join("backend.jsonl");
        let config = config();
        let journal = create(&path, &config);
        for sequence in 1..=3 {
            journal
                .append_event(event(sequence, sequence as u8))
                .expect("append contiguous event");
        }
        assert_eq!(journal.current_event_seq().expect("watermark"), 3);

        let first = journal.read_events(0, 2).expect("first page");
        assert_eq!(first.after_event_seq, 0);
        assert_eq!(first.next_event_seq, 2);
        assert!(!first.complete);
        assert_eq!(first.events, vec![event(1, 1), event(2, 2)]);
        let second = journal.read_events(2, 2).expect("second page");
        assert_eq!(second.next_event_seq, 3);
        assert!(second.complete);
        assert_eq!(second.events, vec![event(3, 3)]);
        let empty = journal.read_events(3, 1).expect("empty final page");
        assert!(empty.complete);
        assert_eq!(empty.next_event_seq, 3);
        assert!(empty.events.is_empty());
        drop(journal);

        let reopened = open(&path, &config);
        assert_eq!(reopened.current_event_seq().expect("replayed watermark"), 3);
        assert_eq!(
            reopened.read_events(0, 3).expect("replayed events").events,
            vec![event(1, 1), event(2, 2), event(3, 3)]
        );
    }

    #[test]
    fn append_rejects_invalid_or_non_next_sequence_without_poisoning() {
        let directory = private_temp_dir();
        let path = directory.path().join("backend.jsonl");
        let config = config();
        let journal = create(&path, &config);
        assert!(matches!(
            journal.append_event(event(2, 2)),
            Err(PoolBackendJournalError::NonContiguousSequence {
                expected: 1,
                actual: 2
            })
        ));
        assert!(matches!(
            journal.append_event(event(0, 1)),
            Err(PoolBackendJournalError::InvalidEvent { .. })
        ));
        journal
            .append_event(event(1, 1))
            .expect("validation errors do not poison journal");
        assert_eq!(journal.current_event_seq().unwrap(), 1);
    }

    #[test]
    fn malformed_complete_record_fails_without_truncation() {
        let directory = private_temp_dir();
        let path = directory.path().join("backend.jsonl");
        let config = config();
        drop(create(&path, &config));
        append_raw(&path, b"{not-json}\nunterminated-tail");
        let corrupt_length = fs::metadata(&path).expect("metadata").len();
        assert!(matches!(
            PoolBackendJournal::open_existing(
                &path,
                config.backend_instance,
                config.wcash_genesis.clone(),
                config.zcash_genesis.clone(),
                config.wcash_payout_commitment.clone(),
                config.zcash_payout_commitment.clone(),
                config.chain_id,
            ),
            Err(PoolBackendJournalError::MalformedRecord { line: 2, .. })
        ));
        assert_eq!(
            fs::metadata(&path).expect("metadata after failure").len(),
            corrupt_length,
            "complete corruption must never be truncated as a torn tail"
        );
    }

    #[test]
    fn exactly_one_unterminated_tail_is_truncated_and_replay_continues() {
        let directory = private_temp_dir();
        let path = directory.path().join("backend.jsonl");
        let config = config();
        let journal = create(&path, &config);
        journal.append_event(event(1, 1)).expect("append event");
        drop(journal);
        let complete_length = fs::metadata(&path).expect("complete metadata").len();
        append_raw(&path, br#"{"record":"pool_backend_journal_event""#);

        let recovered = open(&path, &config);
        assert_eq!(recovered.current_event_seq().unwrap(), 1);
        assert_eq!(
            fs::metadata(&path).expect("recovered metadata").len(),
            complete_length
        );
        recovered
            .append_event(event(2, 2))
            .expect("append after recovery");
        drop(recovered);
        assert_eq!(open(&path, &config).current_event_seq().unwrap(), 2);
    }

    #[test]
    fn complete_authenticated_record_without_newline_is_preserved_and_repaired() {
        let directory = private_temp_dir();
        let path = directory.path().join("backend.jsonl");
        let config = config();
        let journal = create(&path, &config);
        journal.append_event(event(1, 1)).expect("append event");
        drop(journal);

        let mut bytes = fs::read(&path).expect("read complete journal");
        assert_eq!(bytes.pop(), Some(b'\n'));
        fs::write(&path, &bytes).expect("remove only the final delimiter");
        #[cfg(unix)]
        fs::set_permissions(
            &path,
            std::os::unix::fs::PermissionsExt::from_mode(PRIVATE_FILE_MODE),
        )
        .expect("retain private fixture mode");

        let repaired = open(&path, &config);
        assert_eq!(repaired.current_event_seq().unwrap(), 1);
        drop(repaired);
        let repaired_bytes = fs::read(&path).expect("read repaired journal");
        assert_eq!(repaired_bytes.last(), Some(&b'\n'));
        assert_eq!(repaired_bytes.len(), bytes.len() + 1);
    }

    #[test]
    fn every_incomplete_final_record_prefix_recovers_without_sequence_reuse() {
        let source_directory = private_temp_dir();
        let source_path = source_directory.path().join("source.jsonl");
        let config = config();
        let journal = create(&source_path, &config);
        journal.append_event(event(1, 1)).expect("append event");
        drop(journal);
        let source = fs::read(&source_path).expect("read source journal");
        let header_end = source
            .iter()
            .position(|byte| *byte == b'\n')
            .expect("header delimiter")
            + 1;
        let header = &source[..header_end];
        let record = source[header_end..]
            .strip_suffix(b"\n")
            .expect("event delimiter");

        let crash_directory = private_temp_dir();
        for prefix_length in 1..=record.len() {
            let path = crash_directory
                .path()
                .join(format!("crash-{prefix_length}.jsonl"));
            let mut fixture = Vec::with_capacity(header.len() + prefix_length);
            fixture.extend_from_slice(header);
            fixture.extend_from_slice(&record[..prefix_length]);
            write_private_new(&path, &fixture);

            let recovered = open(&path, &config);
            let expected = u64::from(prefix_length == record.len());
            assert_eq!(
                recovered.current_event_seq().unwrap(),
                expected,
                "unexpected recovery result for prefix length {prefix_length}"
            );
            drop(recovered);
            let recovered_bytes = fs::read(&path).expect("read recovered prefix");
            if expected == 0 {
                assert_eq!(recovered_bytes, header);
            } else {
                assert_eq!(recovered_bytes, source);
            }
        }
    }

    #[test]
    fn digest_corruption_and_replay_gap_fail_closed() {
        let directory = private_temp_dir();
        let path = directory.path().join("digest.jsonl");
        let config = config();
        let journal = create(&path, &config);
        journal.append_event(event(1, 1)).expect("append event");
        drop(journal);
        rewrite_record(&path, 1, |record| {
            record["digest"] = Value::String("00".repeat(32));
        });
        assert!(matches!(
            PoolBackendJournal::open_existing(
                &path,
                config.backend_instance,
                config.wcash_genesis.clone(),
                config.zcash_genesis.clone(),
                config.wcash_payout_commitment.clone(),
                config.zcash_payout_commitment.clone(),
                config.chain_id,
            ),
            Err(PoolBackendJournalError::DigestMismatch { event_seq: 1 })
        ));

        let gap_path = directory.path().join("gap.jsonl");
        let gap_journal = create(&gap_path, &config);
        let previous_digest = gap_journal
            .state
            .lock()
            .expect("journal state")
            .last_digest
            .clone();
        drop(gap_journal);
        let gap_event = event(3, 3);
        let gap_record = PersistedEventRecord {
            record: EventRecordKind::PoolBackendJournalEvent,
            event_seq: 3,
            digest: digest_event(&gap_event, &previous_digest).expect("event digest"),
            event: gap_event,
            previous_digest,
        };
        let mut bytes = serde_json::to_vec(&gap_record).expect("gap record");
        bytes.push(b'\n');
        append_raw(&gap_path, &bytes);
        assert!(matches!(
            PoolBackendJournal::open_existing(
                &gap_path,
                config.backend_instance,
                config.wcash_genesis.clone(),
                config.zcash_genesis.clone(),
                config.wcash_payout_commitment.clone(),
                config.zcash_payout_commitment.clone(),
                config.chain_id,
            ),
            Err(PoolBackendJournalError::NonContiguousSequence {
                expected: 1,
                actual: 3
            })
        ));
    }

    #[test]
    fn strict_header_and_event_records_deny_unknown_fields() {
        let directory = private_temp_dir();
        let config = config();
        let header_path = directory.path().join("header.jsonl");
        drop(create(&header_path, &config));
        rewrite_record(&header_path, 0, |header| {
            header["unexpected"] = json!(true);
        });
        assert!(matches!(
            PoolBackendJournal::open_existing(
                &header_path,
                config.backend_instance,
                config.wcash_genesis.clone(),
                config.zcash_genesis.clone(),
                config.wcash_payout_commitment.clone(),
                config.zcash_payout_commitment.clone(),
                config.chain_id,
            ),
            Err(PoolBackendJournalError::MalformedHeader { .. })
        ));

        let event_path = directory.path().join("event.jsonl");
        let journal = create(&event_path, &config);
        journal.append_event(event(1, 1)).expect("append event");
        drop(journal);
        rewrite_record(&event_path, 1, |record| {
            record["unexpected"] = json!(true);
        });
        assert!(matches!(
            PoolBackendJournal::open_existing(
                &event_path,
                config.backend_instance,
                config.wcash_genesis.clone(),
                config.zcash_genesis.clone(),
                config.wcash_payout_commitment.clone(),
                config.zcash_payout_commitment.clone(),
                config.chain_id,
            ),
            Err(PoolBackendJournalError::MalformedRecord { .. })
        ));
    }

    #[test]
    fn empty_legacy_v2_and_unterminated_header_are_refused() {
        let directory = private_temp_dir();
        let config = config();
        for (name, bytes) in [
            ("empty.jsonl", b"".as_slice()),
            (
                "legacy.jsonl",
                br#"{"version":2,"record":"job_activated","job_id":"00"}"#.as_slice(),
            ),
            (
                "header-tail.jsonl",
                br#"{"record":"pool_backend_journal_header""#.as_slice(),
            ),
        ] {
            let path = directory.path().join(name);
            write_private_new(&path, bytes);
            assert!(PoolBackendJournal::open_existing(
                &path,
                config.backend_instance,
                config.wcash_genesis.clone(),
                config.zcash_genesis.clone(),
                config.wcash_payout_commitment.clone(),
                config.zcash_payout_commitment.clone(),
                config.chain_id,
            )
            .is_err());
        }
        assert_eq!(
            fs::metadata(directory.path().join("header-tail.jsonl"))
                .expect("unterminated header metadata")
                .len(),
            br#"{"record":"pool_backend_journal_header""#.len() as u64,
            "an incomplete header must not be truncated into an accepted empty journal"
        );
    }

    #[test]
    fn lock_mode_symlink_hardlink_and_parent_checks_fail_closed() {
        let directory = private_temp_dir();
        let config = config();
        let path = directory.path().join("backend.jsonl");
        let journal = create(&path, &config);
        assert!(matches!(
            PoolBackendJournal::open_existing(
                &path,
                config.backend_instance,
                config.wcash_genesis.clone(),
                config.zcash_genesis.clone(),
                config.wcash_payout_commitment.clone(),
                config.zcash_payout_commitment.clone(),
                config.chain_id,
            ),
            Err(PoolBackendJournalError::Lock { .. })
        ));
        drop(journal);

        #[cfg(unix)]
        {
            use std::os::unix::fs::{symlink, PermissionsExt};

            fs::set_permissions(&path, fs::Permissions::from_mode(0o640))
                .expect("make journal mode unsafe");
            assert!(matches!(
                PoolBackendJournal::open_existing(
                    &path,
                    config.backend_instance,
                    config.wcash_genesis.clone(),
                    config.zcash_genesis.clone(),
                    config.wcash_payout_commitment.clone(),
                    config.zcash_payout_commitment.clone(),
                    config.chain_id,
                ),
                Err(PoolBackendJournalError::UnsafeFileMode { .. })
            ));
            fs::set_permissions(&path, fs::Permissions::from_mode(PRIVATE_FILE_MODE))
                .expect("restore private mode");

            let hardlink = directory.path().join("backend-hardlink.jsonl");
            fs::hard_link(&path, &hardlink).expect("create hard link");
            assert!(matches!(
                PoolBackendJournal::open_existing(
                    &path,
                    config.backend_instance,
                    config.wcash_genesis.clone(),
                    config.zcash_genesis.clone(),
                    config.wcash_payout_commitment.clone(),
                    config.zcash_payout_commitment.clone(),
                    config.chain_id,
                ),
                Err(PoolBackendJournalError::HardLinked { .. })
            ));
            fs::remove_file(&hardlink).expect("remove hard link fixture");

            let symlink_path = directory.path().join("backend-link.jsonl");
            symlink(&path, &symlink_path).expect("create symbolic link");
            assert!(matches!(
                PoolBackendJournal::open_existing(
                    &symlink_path,
                    config.backend_instance,
                    config.wcash_genesis.clone(),
                    config.zcash_genesis.clone(),
                    config.wcash_payout_commitment.clone(),
                    config.zcash_payout_commitment.clone(),
                    config.chain_id,
                ),
                Err(PoolBackendJournalError::SymbolicLink { .. })
            ));

            let unsafe_parent = directory.path().join("unsafe-parent");
            fs::create_dir(&unsafe_parent).expect("create unsafe parent");
            fs::set_permissions(&unsafe_parent, fs::Permissions::from_mode(0o770))
                .expect("make parent group-writable");
            assert!(matches!(
                PoolBackendJournal::create_new(
                    unsafe_parent.join("backend.jsonl"),
                    config.backend_instance,
                    config.wcash_genesis.clone(),
                    config.zcash_genesis.clone(),
                    config.wcash_payout_commitment.clone(),
                    config.zcash_payout_commitment.clone(),
                    config.chain_id,
                ),
                Err(PoolBackendJournalError::UnsafeParentMode { .. })
            ));
        }
    }

    #[test]
    fn live_path_replacement_poisoning_prevents_future_acknowledgements() {
        let directory = private_temp_dir();
        let config = config();
        let path = directory.path().join("backend.jsonl");
        let displaced = directory.path().join("displaced.jsonl");
        let journal = create(&path, &config);
        let original_length = fs::metadata(&path).expect("metadata").len();
        fs::rename(&path, &displaced).expect("displace locked inode");
        write_private_new(&path, b"");

        assert!(matches!(
            journal.append_event(event(1, 1)),
            Err(PoolBackendJournalError::PathChanged { .. })
        ));
        assert!(matches!(
            journal.append_event(event(1, 1)),
            Err(PoolBackendJournalError::Poisoned)
        ));
        assert!(matches!(
            journal.current_event_seq(),
            Err(PoolBackendJournalError::Poisoned)
        ));
        assert_eq!(
            fs::metadata(&displaced).expect("displaced metadata").len(),
            original_length,
            "path verification must fail before appending to a displaced inode"
        );
        assert_eq!(fs::metadata(&path).expect("replacement metadata").len(), 0);
    }

    #[test]
    fn page_and_storage_caps_return_errors_without_poisoning() {
        let directory = private_temp_dir();
        let config = config();
        let path = directory.path().join("backend.jsonl");
        let journal = create(&path, &config);
        assert!(matches!(
            journal.read_events(0, 0),
            Err(PoolBackendJournalError::InvalidPageLimit { .. })
        ));
        assert!(matches!(
            journal.read_events(0, MAX_EVENT_PAGE_ITEMS + 1),
            Err(PoolBackendJournalError::InvalidPageLimit { .. })
        ));
        assert!(matches!(
            journal.read_events(1, 1),
            Err(PoolBackendJournalError::CursorBeyondEnd {
                after: 1,
                current: 0
            })
        ));
        assert!(matches!(
            checked_append_limits(MAX_JOURNAL_EVENTS, 0, 1),
            Err(PoolBackendJournalError::EventCapacity { .. })
        ));
        assert!(matches!(
            checked_append_limits(0, MAX_JOURNAL_BYTES, 1),
            Err(PoolBackendJournalError::JournalTooLarge { .. })
        ));
        assert!(matches!(
            check_record_size(MAX_JOURNAL_RECORD_BYTES + 1, 2),
            Err(PoolBackendJournalError::RecordTooLarge { .. })
        ));
        journal
            .append_event(event(1, 1))
            .expect("preflight cap errors do not poison the journal");
    }
}
