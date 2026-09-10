//! Durable identity for one installed pool backend.
//!
//! Initialization and opening are deliberately separate operations. Losing an
//! identity file must stop the backend instead of silently assigning a new
//! backend instance to an existing installation.

use std::{
    fmt,
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Component, Path, PathBuf},
};

use rand_core::{OsRng, RngCore};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use wcash_pool_protocol::CanonicalUuid;

const IDENTITY_FILE_VERSION: u8 = 1;
const MAX_IDENTITY_FILE_BYTES: usize = 512;

/// An error while initializing or opening a durable backend identity.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum PoolBackendIdentityError {
    /// The caller supplied an empty path.
    #[error("backend identity path is empty")]
    EmptyPath,

    /// An existing identity was requested, but no file exists at the path.
    #[error("backend identity {path} does not exist; initialize a new installation explicitly")]
    Missing {
        /// The requested identity path.
        path: PathBuf,
    },

    /// Initialization would replace an existing identity.
    #[error("backend identity {path} already exists; refusing to replace it")]
    AlreadyExists {
        /// The identity path that already exists.
        path: PathBuf,
    },

    /// The identity file could not be locked for this process.
    #[error("backend identity {path} is already locked or cannot be exclusively locked: {source}")]
    Lock {
        /// The identity path being locked.
        path: PathBuf,
        /// The operating-system locking error.
        #[source]
        source: io::Error,
    },

    /// The path, filesystem metadata, or persisted record is unsafe or invalid.
    #[error("invalid backend identity {path}: {reason}")]
    Invalid {
        /// The rejected identity path.
        path: PathBuf,
        /// The reason the identity was rejected.
        reason: String,
    },

    /// A filesystem operation failed.
    #[error("could not {operation} backend identity {path}: {source}")]
    Io {
        /// A short description of the failed operation.
        operation: &'static str,
        /// The path involved in the operation.
        path: PathBuf,
        /// The underlying I/O error.
        #[source]
        source: io::Error,
    },
}

/// A stable backend installation identity together with its exclusive file lock.
///
/// The exclusive lock is held until this value is dropped. This type is not
/// cloneable so one returned value has one unambiguous lock lifetime.
pub struct PoolBackendIdentity {
    backend_instance: CanonicalUuid,
    _lock_file: File,
}

impl fmt::Debug for PoolBackendIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PoolBackendIdentity")
            .field("backend_instance", &self.backend_instance)
            .finish_non_exhaustive()
    }
}

impl PoolBackendIdentity {
    /// Atomically initializes a new durable identity without replacing any path.
    ///
    /// The new file is written with mode `0600` on Unix, synchronized, and then
    /// followed by a parent-directory synchronization before this method
    /// succeeds. If any initialization step fails, callers must inspect or
    /// remove the failed path explicitly rather than retrying as an open-or-create
    /// operation.
    pub fn initialize(path: impl AsRef<Path>) -> Result<Self, PoolBackendIdentityError> {
        let path = checked_path(path.as_ref())?;
        validate_parent_directory(path)?;
        reject_initialize_target(path)?;

        let backend_instance = generate_backend_instance(path)?;
        let record = PersistedBackendIdentity {
            version: IDENTITY_FILE_VERSION,
            backend_instance,
        };
        let encoded = encode_canonical_record(path, &record)?;

        let mut options = OpenOptions::new();
        options.read(true).write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;

            options.mode(0o600);
        }

        let mut file = options.open(path).map_err(|source| {
            if source.kind() == io::ErrorKind::AlreadyExists {
                PoolBackendIdentityError::AlreadyExists {
                    path: path.to_path_buf(),
                }
            } else {
                io_error("create", path, source)
            }
        })?;

        lock_exclusive(&file, path)?;
        validate_opened_file(&file, path)?;
        verify_path_names_opened_file(&file, path)?;
        validate_parent_directory(path)?;

        file.write_all(&encoded)
            .map_err(|source| io_error("write", path, source))?;
        file.sync_all()
            .map_err(|source| io_error("synchronize", path, source))?;

        validate_opened_file(&file, path)?;
        verify_path_names_opened_file(&file, path)?;
        validate_parent_directory(path)?;
        sync_parent_directory(path)?;
        verify_path_names_opened_file(&file, path)?;

        Ok(Self {
            backend_instance,
            _lock_file: file,
        })
    }

    /// Opens and locks an already initialized durable identity.
    ///
    /// A missing file is always an error. This method never creates or rewrites
    /// an identity file.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, PoolBackendIdentityError> {
        let path = checked_path(path.as_ref())?;
        validate_open_target(path)?;
        validate_parent_directory(path)?;

        let mut file = OpenOptions::new().read(true).open(path).map_err(|source| {
            if source.kind() == io::ErrorKind::NotFound {
                PoolBackendIdentityError::Missing {
                    path: path.to_path_buf(),
                }
            } else {
                io_error("open", path, source)
            }
        })?;

        lock_exclusive(&file, path)?;
        validate_opened_file(&file, path)?;
        verify_path_names_opened_file(&file, path)?;
        validate_parent_directory(path)?;

        let length = file
            .metadata()
            .map_err(|source| io_error("inspect", path, source))?
            .len();
        let maximum_length = u64::try_from(MAX_IDENTITY_FILE_BYTES)
            .expect("the small identity-file byte limit always fits in u64");
        if length > maximum_length {
            return Err(invalid(
                path,
                format!(
                    "file is {length} bytes, exceeding the {MAX_IDENTITY_FILE_BYTES}-byte limit"
                ),
            ));
        }

        let mut encoded = Vec::new();
        (&mut file)
            .take(maximum_length + 1)
            .read_to_end(&mut encoded)
            .map_err(|source| io_error("read", path, source))?;
        if encoded.len() > MAX_IDENTITY_FILE_BYTES {
            return Err(invalid(
                path,
                format!("file grew beyond the {MAX_IDENTITY_FILE_BYTES}-byte limit while reading"),
            ));
        }

        verify_path_names_opened_file(&file, path)?;
        validate_opened_file(&file, path)?;
        validate_parent_directory(path)?;
        let record = decode_canonical_record(path, &encoded)?;

        Ok(Self {
            backend_instance: record.backend_instance,
            _lock_file: file,
        })
    }

    /// Returns the exact protocol UUID identifying this backend installation.
    pub const fn id(&self) -> CanonicalUuid {
        self.backend_instance
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PersistedBackendIdentity {
    version: u8,
    backend_instance: CanonicalUuid,
}

fn checked_path(path: &Path) -> Result<&Path, PoolBackendIdentityError> {
    if path.as_os_str().is_empty() {
        return Err(PoolBackendIdentityError::EmptyPath);
    }
    if !path.is_absolute() {
        return Err(invalid(path, "path must be absolute"));
    }
    if path.file_name().is_none() {
        return Err(invalid(path, "path must name an identity file"));
    }
    if path
        .components()
        .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
    {
        return Err(invalid(
            path,
            "path must not contain `.` or `..` components",
        ));
    }
    let normalized: PathBuf = path.components().collect();
    if normalized.as_os_str() != path.as_os_str() {
        return Err(invalid(
            path,
            "path must be lexically canonical without redundant separators or components",
        ));
    }
    Ok(path)
}

fn generate_backend_instance(path: &Path) -> Result<CanonicalUuid, PoolBackendIdentityError> {
    let mut bytes = [0u8; 16];
    OsRng
        .try_fill_bytes(&mut bytes)
        .map_err(|error| invalid(path, format!("operating-system randomness failed: {error}")))?;

    // RFC 4122 UUIDv4: version 4 in the high nibble of byte 6 and the `10`
    // variant in the high bits of byte 8.
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    let encoded = format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7],
        bytes[8],
        bytes[9],
        bytes[10],
        bytes[11],
        bytes[12],
        bytes[13],
        bytes[14],
        bytes[15],
    );
    serde_json::from_value(serde_json::Value::String(encoded)).map_err(|error| {
        invalid(
            path,
            format!("generated UUID could not be represented canonically: {error}"),
        )
    })
}

fn encode_canonical_record(
    path: &Path,
    record: &PersistedBackendIdentity,
) -> Result<Vec<u8>, PoolBackendIdentityError> {
    let mut encoded = serde_json::to_vec(record)
        .map_err(|error| invalid(path, format!("record could not be encoded: {error}")))?;
    encoded.push(b'\n');
    Ok(encoded)
}

fn decode_canonical_record(
    path: &Path,
    encoded: &[u8],
) -> Result<PersistedBackendIdentity, PoolBackendIdentityError> {
    let record: PersistedBackendIdentity = serde_json::from_slice(encoded)
        .map_err(|error| invalid(path, format!("malformed versioned JSON: {error}")))?;
    if record.version != IDENTITY_FILE_VERSION {
        return Err(invalid(
            path,
            format!(
                "unsupported file version {}; expected {IDENTITY_FILE_VERSION}",
                record.version
            ),
        ));
    }
    validate_backend_instance(path, record.backend_instance)?;
    if encode_canonical_record(path, &record)? != encoded {
        return Err(invalid(
            path,
            "JSON is not in the canonical compact field order with one trailing newline",
        ));
    }
    Ok(record)
}

fn validate_backend_instance(
    path: &Path,
    backend_instance: CanonicalUuid,
) -> Result<(), PoolBackendIdentityError> {
    if backend_instance.is_nil() {
        return Err(invalid(path, "backend_instance must not be the nil UUID"));
    }
    let uuid = backend_instance.get();
    let bytes = uuid.as_bytes();
    if bytes[6] >> 4 != 4 || bytes[8] & 0xc0 != 0x80 {
        return Err(invalid(
            path,
            "backend_instance must be an RFC 4122 variant UUIDv4",
        ));
    }
    Ok(())
}

fn reject_initialize_target(path: &Path) -> Result<(), PoolBackendIdentityError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            Err(invalid(path, "path must not be a symbolic link"))
        }
        Ok(metadata) if !metadata.is_file() => Err(invalid(path, "path is not a regular file")),
        Ok(_) => Err(PoolBackendIdentityError::AlreadyExists {
            path: path.to_path_buf(),
        }),
        Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(io_error("inspect", path, source)),
    }
}

fn validate_open_target(path: &Path) -> Result<(), PoolBackendIdentityError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            Err(invalid(path, "path must not be a symbolic link"))
        }
        Ok(metadata) if !metadata.is_file() => Err(invalid(path, "path is not a regular file")),
        Ok(_) => Ok(()),
        Err(source) if source.kind() == io::ErrorKind::NotFound => {
            Err(PoolBackendIdentityError::Missing {
                path: path.to_path_buf(),
            })
        }
        Err(source) => Err(io_error("inspect", path, source)),
    }
}

fn lock_exclusive(file: &File, path: &Path) -> Result<(), PoolBackendIdentityError> {
    fs2::FileExt::try_lock_exclusive(file).map_err(|source| PoolBackendIdentityError::Lock {
        path: path.to_path_buf(),
        source,
    })
}

fn validate_opened_file(file: &File, path: &Path) -> Result<(), PoolBackendIdentityError> {
    let metadata = file
        .metadata()
        .map_err(|source| io_error("inspect", path, source))?;
    if !metadata.is_file() {
        return Err(invalid(path, "opened object is not a regular file"));
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        validate_unix_owner(path, "file", metadata.uid())?;
        let mode = metadata.permissions().mode();
        if mode & 0o077 != 0 {
            return Err(invalid(
                path,
                format!(
                    "file mode {:04o} permits group or other access; expected 0600 or stricter",
                    mode & 0o7777
                ),
            ));
        }
        if metadata.nlink() != 1 {
            return Err(invalid(
                path,
                format!(
                    "file has {} hard links; exactly one link is required",
                    metadata.nlink()
                ),
            ));
        }
    }

    Ok(())
}

fn validate_parent_directory(path: &Path) -> Result<(), PoolBackendIdentityError> {
    let parent = parent_directory(path);
    let metadata = fs::symlink_metadata(parent)
        .map_err(|source| io_error("inspect parent directory of", path, source))?;
    if metadata.file_type().is_symlink() {
        return Err(invalid(
            path,
            format!(
                "parent directory {} must not be a symbolic link",
                parent.display()
            ),
        ));
    }
    if !metadata.is_dir() {
        return Err(invalid(
            path,
            format!("parent path {} is not a directory", parent.display()),
        ));
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        let canonical_parent = fs::canonicalize(parent)
            .map_err(|source| io_error("canonicalize parent directory of", path, source))?;
        if canonical_parent.as_os_str() != parent.as_os_str() {
            return Err(invalid(
                path,
                format!(
                    "parent directory {} contains a symbolic link or is not canonical",
                    parent.display()
                ),
            ));
        }

        validate_unix_owner(path, "parent directory", metadata.uid())?;
        let mode = metadata.permissions().mode();
        if mode & 0o7777 != 0o700 {
            return Err(invalid(
                path,
                format!(
                    "parent directory {} has mode {:04o}; expected exactly 0700",
                    parent.display(),
                    mode & 0o7777
                ),
            ));
        }
    }

    Ok(())
}

#[cfg(unix)]
fn validate_unix_owner(
    path: &Path,
    object: &str,
    owner_uid: u32,
) -> Result<(), PoolBackendIdentityError> {
    let effective_uid = nix::unistd::geteuid().as_raw();
    if owner_uid != effective_uid {
        return Err(invalid(
            path,
            format!(
                "{object} is owned by UID {owner_uid}, but the current effective UID is {effective_uid}"
            ),
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn verify_path_names_opened_file(file: &File, path: &Path) -> Result<(), PoolBackendIdentityError> {
    use std::os::unix::fs::MetadataExt;

    let opened = file
        .metadata()
        .map_err(|source| io_error("inspect opened file for", path, source))?;
    let named = fs::symlink_metadata(path)
        .map_err(|source| io_error("inspect current path for", path, source))?;
    if named.file_type().is_symlink() || !named.is_file() {
        return Err(invalid(
            path,
            "path no longer names a regular non-symbolic-link file",
        ));
    }
    if opened.dev() != named.dev() || opened.ino() != named.ino() {
        return Err(invalid(path, "path was replaced while it was being opened"));
    }
    Ok(())
}

#[cfg(not(unix))]
fn verify_path_names_opened_file(
    _file: &File,
    path: &Path,
) -> Result<(), PoolBackendIdentityError> {
    let named = fs::symlink_metadata(path)
        .map_err(|source| io_error("inspect current path for", path, source))?;
    if named.file_type().is_symlink() || !named.is_file() {
        return Err(invalid(
            path,
            "path no longer names a regular non-symbolic-link file",
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn sync_parent_directory(path: &Path) -> Result<(), PoolBackendIdentityError> {
    let parent = parent_directory(path);
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| io_error("synchronize parent directory of", path, source))
}

#[cfg(not(unix))]
fn sync_parent_directory(_path: &Path) -> Result<(), PoolBackendIdentityError> {
    Ok(())
}

fn parent_directory(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

fn invalid(path: &Path, reason: impl Into<String>) -> PoolBackendIdentityError {
    PoolBackendIdentityError::Invalid {
        path: path.to_path_buf(),
        reason: reason.into(),
    }
}

fn io_error(operation: &'static str, path: &Path, source: io::Error) -> PoolBackendIdentityError {
    PoolBackendIdentityError::Io {
        operation,
        path: path.to_path_buf(),
        source,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn private_temp_dir() -> tempfile::TempDir {
        let directory = tempfile::tempdir().expect("temporary directory");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700))
                .expect("test directory is private");
        }
        directory
    }

    fn write_private(path: &Path, bytes: &[u8]) {
        fs::write(path, bytes).expect("write identity fixture");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            fs::set_permissions(path, fs::Permissions::from_mode(0o600))
                .expect("identity fixture is private");
        }
    }

    fn path_in(directory: &tempfile::TempDir, name: &str) -> PathBuf {
        fs::canonicalize(directory.path())
            .expect("temporary directory has a canonical absolute path")
            .join(name)
    }

    #[test]
    fn initialize_and_open_preserve_one_uuid_v4() {
        let directory = private_temp_dir();
        let path = path_in(&directory, "backend-identity.json");

        let initialized = PoolBackendIdentity::initialize(&path).expect("initialize identity");
        let expected = initialized.id();
        assert!(!expected.is_nil());
        let uuid = expected.get();
        assert_eq!(uuid.as_bytes()[6] >> 4, 4);
        assert_eq!(uuid.as_bytes()[8] & 0xc0, 0x80);

        let persisted = fs::read(&path).expect("read persisted identity");
        assert_eq!(
            persisted,
            encode_canonical_record(
                &path,
                &PersistedBackendIdentity {
                    version: IDENTITY_FILE_VERSION,
                    backend_instance: expected,
                }
            )
            .expect("encode expected identity")
        );
        assert!(matches!(
            PoolBackendIdentity::open(&path),
            Err(PoolBackendIdentityError::Lock { .. })
        ));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            assert_eq!(
                fs::metadata(&path)
                    .expect("identity metadata")
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }

        drop(initialized);
        let opened = PoolBackendIdentity::open(&path).expect("open initialized identity");
        assert_eq!(opened.id(), expected);
        drop(opened);
        assert_eq!(
            PoolBackendIdentity::open(&path)
                .expect("reopen initialized identity")
                .id(),
            expected
        );
    }

    #[test]
    fn opening_missing_identity_never_initializes_it() {
        let directory = private_temp_dir();
        let path = path_in(&directory, "missing.json");

        assert!(matches!(
            PoolBackendIdentity::open(&path),
            Err(PoolBackendIdentityError::Missing { .. })
        ));
        assert!(!path.exists());
    }

    #[test]
    fn duplicate_initialization_is_rejected_without_rotation() {
        let directory = private_temp_dir();
        let path = path_in(&directory, "backend-identity.json");
        let first = PoolBackendIdentity::initialize(&path).expect("initialize identity");
        let expected = first.id();

        assert!(matches!(
            PoolBackendIdentity::initialize(&path),
            Err(PoolBackendIdentityError::AlreadyExists { .. })
        ));
        drop(first);
        assert_eq!(
            PoolBackendIdentity::open(&path)
                .expect("original identity remains")
                .id(),
            expected
        );
    }

    #[test]
    fn malformed_noncanonical_and_unknown_json_are_rejected() {
        let directory = private_temp_dir();
        let path = path_in(&directory, "backend-identity.json");

        write_private(&path, b"not JSON\n");
        assert!(PoolBackendIdentity::open(&path).is_err());

        write_private(
            &path,
            br#"{"version":1,"backend_instance":"123e4567-e89b-42d3-a456-426614174000","unknown":true}
"#,
        );
        assert!(PoolBackendIdentity::open(&path).is_err());

        write_private(
            &path,
            br#"{ "version": 1, "backend_instance": "123e4567-e89b-42d3-a456-426614174000" }
"#,
        );
        assert!(PoolBackendIdentity::open(&path).is_err());
    }

    #[test]
    fn unsupported_nil_and_non_v4_identities_are_rejected() {
        let directory = private_temp_dir();
        let path = path_in(&directory, "backend-identity.json");

        write_private(
            &path,
            br#"{"version":2,"backend_instance":"123e4567-e89b-42d3-a456-426614174000"}
"#,
        );
        assert!(PoolBackendIdentity::open(&path).is_err());

        write_private(
            &path,
            br#"{"version":1,"backend_instance":"00000000-0000-0000-0000-000000000000"}
"#,
        );
        assert!(PoolBackendIdentity::open(&path).is_err());

        write_private(
            &path,
            br#"{"version":1,"backend_instance":"123e4567-e89b-12d3-a456-426614174000"}
"#,
        );
        assert!(PoolBackendIdentity::open(&path).is_err());
    }

    #[test]
    fn identity_path_must_be_absolute_and_lexically_canonical() {
        assert!(PoolBackendIdentity::open("relative-identity.json").is_err());

        let directory = private_temp_dir();
        let canonical_directory =
            fs::canonicalize(directory.path()).expect("temporary directory has a canonical path");
        let dotted = canonical_directory.join(".").join("dotted-identity.json");
        assert!(PoolBackendIdentity::initialize(&dotted).is_err());
        assert!(!canonical_directory.join("dotted-identity.json").exists());
    }

    #[cfg(unix)]
    #[test]
    fn insecure_file_and_parent_modes_are_rejected() {
        use std::os::unix::fs::PermissionsExt;

        let directory = private_temp_dir();
        let path = path_in(&directory, "backend-identity.json");
        drop(PoolBackendIdentity::initialize(&path).expect("initialize identity"));
        fs::set_permissions(&path, fs::Permissions::from_mode(0o640))
            .expect("make identity group-readable");
        assert!(PoolBackendIdentity::open(&path).is_err());

        let unsafe_directory = private_temp_dir();
        fs::set_permissions(unsafe_directory.path(), fs::Permissions::from_mode(0o770))
            .expect("make test parent group-writable");
        let unsafe_path = path_in(&unsafe_directory, "identity.json");
        assert!(PoolBackendIdentity::initialize(&unsafe_path).is_err());
        assert!(!unsafe_path.exists());

        let exposed_directory = private_temp_dir();
        fs::set_permissions(exposed_directory.path(), fs::Permissions::from_mode(0o750))
            .expect("make test parent group-accessible");
        let exposed_path = path_in(&exposed_directory, "identity.json");
        assert!(PoolBackendIdentity::initialize(&exposed_path).is_err());
        assert!(!exposed_path.exists());
    }

    #[cfg(unix)]
    #[test]
    fn foreign_unix_ownership_is_rejected() {
        let directory = private_temp_dir();
        let path = path_in(&directory, "identity.json");
        let effective_uid = nix::unistd::geteuid().as_raw();
        let foreign_uid = effective_uid.checked_add(1).unwrap_or(0);

        assert!(validate_unix_owner(&path, "test file", foreign_uid).is_err());
        assert!(validate_unix_owner(&path, "test parent", effective_uid).is_ok());

        if effective_uid == 0 {
            drop(PoolBackendIdentity::initialize(&path).expect("initialize root-owned identity"));
            std::os::unix::fs::chown(&path, Some(foreign_uid), None)
                .expect("make identity foreign-owned");
            assert!(PoolBackendIdentity::open(&path).is_err());
            std::os::unix::fs::chown(&path, Some(effective_uid), None)
                .expect("restore identity ownership for cleanup");

            let foreign_directory = private_temp_dir();
            let foreign_path = path_in(&foreign_directory, "identity.json");
            std::os::unix::fs::chown(foreign_directory.path(), Some(foreign_uid), None)
                .expect("make parent foreign-owned");
            assert!(PoolBackendIdentity::initialize(&foreign_path).is_err());
            std::os::unix::fs::chown(foreign_directory.path(), Some(effective_uid), None)
                .expect("restore parent ownership for cleanup");
        }
    }

    #[cfg(unix)]
    #[test]
    fn symlinked_parent_chain_is_rejected() {
        use std::os::unix::{fs::symlink, fs::PermissionsExt};

        let directory = private_temp_dir();
        let canonical_directory =
            fs::canonicalize(directory.path()).expect("temporary directory has a canonical path");
        let real_parent = canonical_directory.join("real-parent");
        let nested = real_parent.join("nested");
        fs::create_dir(&real_parent).expect("create real parent");
        fs::create_dir(&nested).expect("create nested state directory");
        fs::set_permissions(&real_parent, fs::Permissions::from_mode(0o700))
            .expect("make real parent private");
        fs::set_permissions(&nested, fs::Permissions::from_mode(0o700))
            .expect("make nested state directory private");

        let alias = canonical_directory.join("parent-alias");
        symlink(&real_parent, &alias).expect("create parent-chain symlink");
        let identity_path = alias.join("nested").join("identity.json");
        assert!(PoolBackendIdentity::initialize(&identity_path).is_err());
        assert!(!nested.join("identity.json").exists());
    }

    #[cfg(unix)]
    #[test]
    fn symbolic_links_and_hard_links_are_rejected() {
        use std::os::unix::fs::symlink;

        let directory = private_temp_dir();
        let target = path_in(&directory, "target.json");
        drop(PoolBackendIdentity::initialize(&target).expect("initialize target"));

        let symbolic = path_in(&directory, "symbolic.json");
        symlink(&target, &symbolic).expect("create symbolic link");
        assert!(PoolBackendIdentity::open(&symbolic).is_err());

        let hard = path_in(&directory, "hard.json");
        fs::hard_link(&target, &hard).expect("create hard link");
        assert!(PoolBackendIdentity::open(&target).is_err());
        assert!(PoolBackendIdentity::open(&hard).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn path_replacement_is_detected_against_the_locked_inode() {
        use std::os::unix::fs::PermissionsExt;

        let directory = private_temp_dir();
        let path = path_in(&directory, "backend-identity.json");
        let displaced = path_in(&directory, "displaced.json");
        let identity = PoolBackendIdentity::initialize(&path).expect("initialize identity");

        fs::rename(&path, &displaced).expect("displace locked identity");
        write_private(
            &path,
            br#"{"version":1,"backend_instance":"123e4567-e89b-42d3-a456-426614174000"}
"#,
        );
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
            .expect("replacement identity is private");

        assert!(verify_path_names_opened_file(&identity._lock_file, &path).is_err());
    }
}
