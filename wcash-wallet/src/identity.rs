//! Persistent Wcash wallet database identity.

use std::path::Path;

use rusqlite::{params, Connection, OpenFlags, TransactionBehavior};
use thiserror::Error;

use crate::{keys::WALLET_SEED_KDF_VERSION, WalletNetwork};

const IDENTITY_TABLE: &str = "ext_wcash_wallet_identity";
const IDENTITY_FORMAT_VERSION: i64 = 1;

/// Errors returned before a SQLite database is opened as a Wcash wallet.
#[derive(Debug, Error)]
pub enum WalletDatabaseIdentityError {
    /// The database has wallet data but predates persistent chain identity.
    #[error(
        "wallet database has no Wcash chain identity and may belong to the pre-launch v4 profile; automatic migration is unsafe because Testnet v5 changed the genesis-bound key derivation. Move the database aside, initialize a new database from the seed, and rescan, or use a separately reviewed migration tool"
    )]
    PrelaunchIdentityMissing,
    /// The identity record cannot be interpreted unambiguously.
    #[error("wallet database identity is malformed: {0}")]
    Malformed(String),
    /// One persisted identity field differs from the selected chain.
    #[error(
        "wallet database identity mismatch for {field}: expected {expected}, found {actual}; refusing to open data from a different Wcash chain or wallet derivation"
    )]
    Mismatch {
        /// Identity field that failed comparison.
        field: &'static str,
        /// Value required by this binary and selected network.
        expected: String,
        /// Value stored in SQLite.
        actual: String,
    },
    /// SQLite could not read or atomically persist the identity.
    #[error("could not verify wallet database identity: {0}")]
    Sqlite(#[from] rusqlite::Error),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct WalletDatabaseIdentity {
    format_version: i64,
    network_id: String,
    genesis_hash: Vec<u8>,
    seed_kdf_version: i64,
    transaction_branch_id: i64,
}

impl WalletDatabaseIdentity {
    fn expected(network: WalletNetwork) -> Self {
        Self {
            format_version: IDENTITY_FORMAT_VERSION,
            network_id: network.database_identity_name().to_owned(),
            genesis_hash: network.genesis_hash().to_vec(),
            seed_kdf_version: i64::from(WALLET_SEED_KDF_VERSION),
            transaction_branch_id: i64::from(u32::from(network.branch_id())),
        }
    }
}

/// Opens an existing SQLite file without creating or modifying its schema.
pub(crate) fn open_wallet_connection(path: &Path) -> Result<Connection, rusqlite::Error> {
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
        | OpenFlags::SQLITE_OPEN_NO_MUTEX
        | OpenFlags::SQLITE_OPEN_NOFOLLOW;
    Connection::open_with_flags(path, flags)
}

/// Verifies the persisted chain identity, or initializes it in an empty file.
pub(crate) fn verify_or_initialize_identity(
    connection: &mut Connection,
    network: WalletNetwork,
) -> Result<(), WalletDatabaseIdentityError> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let identity_exists = schema_object_exists(&transaction, IDENTITY_TABLE)?;

    if !identity_exists {
        if schema_has_any_objects(&transaction)? {
            return Err(WalletDatabaseIdentityError::PrelaunchIdentityMissing);
        }

        create_identity(&transaction, network)?;
    }

    let actual = read_identity(&transaction)?;
    verify_identity(network, actual)?;
    transaction.commit()?;
    Ok(())
}

fn schema_object_exists(connection: &Connection, name: &str) -> Result<bool, rusqlite::Error> {
    connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name = ?1)",
        [name],
        |row| row.get(0),
    )
}

fn schema_has_any_objects(connection: &Connection) -> Result<bool, rusqlite::Error> {
    connection.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_schema)", [], |row| {
        row.get(0)
    })
}

fn create_identity(connection: &Connection, network: WalletNetwork) -> Result<(), rusqlite::Error> {
    connection.execute_batch(
        "CREATE TABLE ext_wcash_wallet_identity (
            singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
            format_version INTEGER NOT NULL,
            network_id TEXT NOT NULL,
            genesis_hash BLOB NOT NULL CHECK (length(genesis_hash) = 32),
            seed_kdf_version INTEGER NOT NULL,
            transaction_branch_id INTEGER NOT NULL
        ) WITHOUT ROWID;",
    )?;

    let identity = WalletDatabaseIdentity::expected(network);
    connection.execute(
        "INSERT INTO ext_wcash_wallet_identity (
            singleton,
            format_version,
            network_id,
            genesis_hash,
            seed_kdf_version,
            transaction_branch_id
        ) VALUES (1, ?1, ?2, ?3, ?4, ?5)",
        params![
            identity.format_version,
            identity.network_id,
            identity.genesis_hash,
            identity.seed_kdf_version,
            identity.transaction_branch_id,
        ],
    )?;
    Ok(())
}

fn read_identity(
    connection: &Connection,
) -> Result<WalletDatabaseIdentity, WalletDatabaseIdentityError> {
    let object_type: String = connection
        .query_row(
            "SELECT type FROM sqlite_schema WHERE name = ?1",
            [IDENTITY_TABLE],
            |row| row.get(0),
        )
        .map_err(|error| malformed_identity(format!("identity object cannot be read: {error}")))?;
    if object_type != "table" {
        return Err(malformed_identity(format!(
            "{IDENTITY_TABLE} is a {object_type}, not a table"
        )));
    }

    let mut statement = connection
        .prepare(
            "SELECT
                singleton,
                format_version,
                network_id,
                genesis_hash,
                seed_kdf_version,
                transaction_branch_id
             FROM ext_wcash_wallet_identity",
        )
        .map_err(|error| malformed_identity(format!("identity table cannot be read: {error}")))?;
    let mut rows = statement
        .query([])
        .map_err(|error| malformed_identity(format!("identity row cannot be read: {error}")))?;
    let row = rows
        .next()
        .map_err(|error| malformed_identity(format!("identity row cannot be read: {error}")))?
        .ok_or_else(|| malformed_identity("identity table is empty"))?;

    let singleton = read_integer(row, 0, "singleton")?;
    let network_id = row
        .get_ref(2)
        .map_err(|error| malformed_identity(format!("invalid network ID: {error}")))?
        .as_str()
        .map_err(|error| malformed_identity(format!("invalid network ID: {error}")))?;
    if network_id.len() > 64
        || !network_id.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_')
        })
    {
        return Err(malformed_identity(
            "network ID must be at most 64 lowercase ASCII letters, digits, hyphens, or underscores",
        ));
    }
    let genesis_hash = row
        .get_ref(3)
        .map_err(|error| malformed_identity(format!("invalid genesis hash: {error}")))?
        .as_blob()
        .map_err(|error| malformed_identity(format!("invalid genesis hash: {error}")))?;
    if genesis_hash.len() != 32 {
        return Err(malformed_identity(format!(
            "genesis hash must contain 32 bytes, found {}",
            genesis_hash.len()
        )));
    }
    let identity = WalletDatabaseIdentity {
        format_version: read_integer(row, 1, "format version")?,
        network_id: network_id.to_owned(),
        genesis_hash: genesis_hash.to_vec(),
        seed_kdf_version: read_integer(row, 4, "seed KDF version")?,
        transaction_branch_id: read_integer(row, 5, "transaction branch ID")?,
    };

    if singleton != 1 {
        return Err(malformed_identity(format!(
            "singleton key must be 1, found {singleton}"
        )));
    }
    if rows
        .next()
        .map_err(|error| malformed_identity(format!("identity row cannot be read: {error}")))?
        .is_some()
    {
        return Err(malformed_identity(
            "identity table contains more than one row",
        ));
    }
    Ok(identity)
}

fn read_integer(
    row: &rusqlite::Row<'_>,
    index: usize,
    field: &str,
) -> Result<i64, WalletDatabaseIdentityError> {
    row.get_ref(index)
        .map_err(|error| malformed_identity(format!("invalid {field}: {error}")))?
        .as_i64()
        .map_err(|error| malformed_identity(format!("invalid {field}: {error}")))
}

fn verify_identity(
    network: WalletNetwork,
    actual: WalletDatabaseIdentity,
) -> Result<(), WalletDatabaseIdentityError> {
    let expected = WalletDatabaseIdentity::expected(network);
    require_equal(
        "identity format version",
        expected.format_version,
        actual.format_version,
    )?;
    require_equal("network", expected.network_id, actual.network_id)?;
    require_equal(
        "genesis hash (internal byte order)",
        hex::encode(expected.genesis_hash),
        hex::encode(actual.genesis_hash),
    )?;
    require_equal(
        "seed KDF version",
        expected.seed_kdf_version,
        actual.seed_kdf_version,
    )?;
    require_equal(
        "transaction branch ID",
        format!("0x{:08x}", expected.transaction_branch_id),
        format!("0x{:08x}", actual.transaction_branch_id),
    )?;
    Ok(())
}

fn require_equal<T>(
    field: &'static str,
    expected: T,
    actual: T,
) -> Result<(), WalletDatabaseIdentityError>
where
    T: Eq + ToString,
{
    if expected == actual {
        Ok(())
    } else {
        Err(WalletDatabaseIdentityError::Mismatch {
            field,
            expected: expected.to_string(),
            actual: actual.to_string(),
        })
    }
}

fn malformed_identity(message: impl Into<String>) -> WalletDatabaseIdentityError {
    WalletDatabaseIdentityError::Malformed(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{open_wallet_database, WalletServiceError};
    use rand_core::OsRng;
    use zcash_client_sqlite::{util::SystemClock, wallet::init::init_wallet_db, WalletDb};

    fn identity_table_exists(connection: &Connection) -> bool {
        schema_object_exists(connection, IDENTITY_TABLE).unwrap()
    }

    fn open_error(path: &Path, network: WalletNetwork) -> WalletServiceError {
        match open_wallet_database(path, network) {
            Ok(_) => panic!("database unexpectedly passed its identity check"),
            Err(error) => error,
        }
    }

    #[cfg(unix)]
    fn make_private(path: &Path) {
        use std::os::unix::fs::PermissionsExt;

        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }

    #[cfg(not(unix))]
    fn make_private(_path: &Path) {}

    #[test]
    fn expected_identity_binds_every_consensus_and_derivation_domain() {
        let testnet = WalletDatabaseIdentity::expected(WalletNetwork::Testnet);
        let regtest = WalletDatabaseIdentity::expected(WalletNetwork::Regtest);

        assert_eq!(testnet.format_version, 1);
        assert_eq!(testnet.network_id, "wcash-testnet");
        assert_eq!(testnet.genesis_hash, WalletNetwork::Testnet.genesis_hash());
        assert_eq!(testnet.seed_kdf_version, 1);
        assert_eq!(testnet.transaction_branch_id, 0xb3cf_d27e);

        assert_ne!(testnet.network_id, regtest.network_id);
        assert_ne!(testnet.genesis_hash, regtest.genesis_hash);
        assert_ne!(testnet.transaction_branch_id, regtest.transaction_branch_id);
    }

    #[test]
    fn new_database_persists_exact_identity_and_reopens() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("wallet.sqlite");
        let network = WalletNetwork::Testnet;

        drop(open_wallet_database(&path, network).unwrap());

        let connection = Connection::open(&path).unwrap();
        let actual = read_identity(&connection).unwrap();
        assert_eq!(actual, WalletDatabaseIdentity::expected(network));
        drop(connection);

        drop(open_wallet_database(&path, network).unwrap());
    }

    #[test]
    fn empty_existing_database_can_be_bound_safely() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("wallet.sqlite");
        drop(Connection::open(&path).unwrap());
        make_private(&path);

        drop(open_wallet_database(&path, WalletNetwork::Regtest).unwrap());

        let connection = Connection::open(path).unwrap();
        assert_eq!(
            read_identity(&connection).unwrap(),
            WalletDatabaseIdentity::expected(WalletNetwork::Regtest)
        );
    }

    #[test]
    fn prelaunch_v4_database_requires_reset_without_modification() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("wallet-v4.sqlite");

        // This is the exact unguarded initialization path used before Wcash
        // owned a database identity record.
        let mut legacy = WalletDb::for_path(
            &path,
            WalletNetwork::Testnet.parameters(),
            SystemClock,
            OsRng,
        )
        .unwrap();
        init_wallet_db(&mut legacy, None).unwrap();
        drop(legacy);
        make_private(&path);

        let connection = Connection::open(&path).unwrap();
        let schema_objects_before = connection
            .query_row("SELECT COUNT(*) FROM sqlite_schema", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap();
        assert!(schema_objects_before > 0);
        assert!(!identity_table_exists(&connection));
        drop(connection);

        let error = open_error(&path, WalletNetwork::Testnet);
        assert!(matches!(
            error,
            WalletServiceError::Identity(WalletDatabaseIdentityError::PrelaunchIdentityMissing)
        ));
        assert!(error.to_string().contains("pre-launch v4"));
        assert!(error.to_string().contains("rescan"));

        let connection = Connection::open(path).unwrap();
        assert!(!identity_table_exists(&connection));
        assert_eq!(
            connection
                .query_row("SELECT COUNT(*) FROM sqlite_schema", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            schema_objects_before
        );
    }

    #[test]
    fn database_cannot_be_reopened_for_another_wcash_network() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("wallet.sqlite");
        drop(open_wallet_database(&path, WalletNetwork::Testnet).unwrap());

        let error = open_error(&path, WalletNetwork::Regtest);
        assert!(matches!(
            error,
            WalletServiceError::Identity(WalletDatabaseIdentityError::Mismatch {
                field: "network",
                ..
            })
        ));
    }

    #[test]
    fn every_persisted_identity_field_is_verified() {
        let cases = [
            (
                "format_version = 2",
                "identity format version",
            ),
            ("network_id = 'zcash-testnet'", "network"),
            (
                "genesis_hash = X'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'",
                "genesis hash (internal byte order)",
            ),
            ("seed_kdf_version = 2", "seed KDF version"),
            ("transaction_branch_id = 1", "transaction branch ID"),
        ];

        for (assignment, expected_field) in cases {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("wallet.sqlite");
            drop(open_wallet_database(&path, WalletNetwork::Testnet).unwrap());

            let connection = Connection::open(&path).unwrap();
            connection
                .execute(
                    &format!("UPDATE ext_wcash_wallet_identity SET {assignment}"),
                    [],
                )
                .unwrap();
            drop(connection);

            let error = open_error(&path, WalletNetwork::Testnet);
            assert!(matches!(
                error,
                WalletServiceError::Identity(WalletDatabaseIdentityError::Mismatch {
                    field,
                    ..
                }) if field == expected_field
            ));
        }
    }

    #[test]
    fn malformed_identity_is_never_treated_as_a_new_database() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("wallet.sqlite");
        drop(open_wallet_database(&path, WalletNetwork::Testnet).unwrap());

        let connection = Connection::open(&path).unwrap();
        connection
            .execute("DELETE FROM ext_wcash_wallet_identity", [])
            .unwrap();
        drop(connection);

        let error = open_error(&path, WalletNetwork::Testnet);
        assert!(matches!(
            error,
            WalletServiceError::Identity(WalletDatabaseIdentityError::Malformed(_))
        ));
    }
}
