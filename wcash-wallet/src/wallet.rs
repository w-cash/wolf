//! SQLite-backed Wcash wallet operations.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    convert::Infallible,
    fs::{self, File, OpenOptions},
    future::Future,
    num::NonZeroU32,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use fs2::FileExt;
use orchard::keys::Scope as OrchardScope;
use rand_core::{OsRng, RngCore};
use rusqlite::{params, OptionalExtension};
use secrecy::SecretVec;
use serde::Serialize;
use thiserror::Error;
use zcash_client_backend::{
    data_api::wallet::{
        create_proposed_transactions, decrypt_and_store_transaction,
        input_selection::{GreedyInputSelector, SpendPolicy},
        propose_shielding_coinbase, propose_transfer, unlock_proposal_inputs, ConfirmationsPolicy,
        LockRequest, SpendingKeys,
    },
    data_api::{
        Account, AccountBirthday, NullifierQuery, OutputStatusFilter, TransactionDataRequest,
        TransactionStatusFilter, TransactionsInvolvingAddress, WalletRead, WalletWrite,
    },
    decrypt_transaction,
    fees::{standard::SingleOutputChangeStrategy, DustOutputPolicy, StandardFeeRule},
    wallet::{LockOwner, Note as WalletNote, OvkPolicy},
    zip321::{Payment, TransactionRequest},
    TransferType,
};
use zcash_client_sqlite::{
    util::SystemClock, wallet::init::init_wallet_db, AccountUuid, ExtensionTransaction, WalletDb,
};
use zcash_primitives::transaction::{Transaction, TxVersion};
use zcash_proofs::prover::LocalTxProver;
use zcash_protocol::{
    consensus::{BlockHeight, COINBASE_MATURITY_BLOCKS},
    memo::MemoBytes,
    value::Zatoshis,
    PoolType, ShieldedPool,
};
use zcash_transparent::bundle::OutPoint;
use zebra_chain::{block::Height, parameters::Network};

use crate::{
    address::default_transparent_receiver,
    decode_recipient, derive_wallet_seed, derive_wallet_spending_key, encode_orchard_receiver,
    encode_transparent_coinbase_receiver,
    identity::{
        open_wallet_connection, open_wallet_connection_read_only, verify_existing_identity,
        verify_or_initialize_identity, ExistingWalletIdentityError, TRANSPARENT_SYNC_STATE_TABLE,
    },
    inspect_signed_transaction,
    rpc::{MAX_TRANSPARENT_HISTORY_BLOCKS_PER_REQUEST, TRANSPARENT_UTXO_PAGE_OUTPUTS},
    sync, AttestedWcashClient, BlockRef, MemoryBlockCache, WalletAddressError,
    WalletDatabaseIdentityError, WalletKeyError, WalletNetwork, WalletRpcError,
};

/// Maximum compact blocks requested in one synchronization batch.
pub const MAX_SYNC_BATCH_SIZE: u32 = crate::cache::MAX_ATTESTED_COMPACT_BLOCKS_PER_RANGE;
/// Maximum recipients in one experimental shielded transfer.
pub const MAX_TRANSFER_RECIPIENTS: usize = 100;
/// Maximum transaction expiry interval accepted by the wallet.
pub const MAX_EXPIRY_DELTA: u32 = 100;
/// Maximum note-lock lifetime accepted by the wallet.
pub const MAX_LOCK_FOR_BLOCKS: u32 = 1_000;
/// Maximum transparent coinbase inputs swept by one shielding transaction.
pub const MAX_COINBASE_SHIELDING_INPUTS: usize = 100;
/// Maximum locally-created transactions returned by one recovery page.
pub const MAX_PENDING_TRANSACTION_PAGE_SIZE: usize = 25;
/// Consensus maturity required before a transparent coinbase output can be
/// shielded.
pub const COINBASE_SHIELDING_MATURITY: u32 = COINBASE_MATURITY_BLOCKS;
/// The deterministic coinbase receiver is recoverable from height one even if
/// it was published before this SQLite wallet was initialized.
pub const TRANSPARENT_COINBASE_RECOVERY_START_HEIGHT: u32 = 1;
/// Maximum wall-clock duration of one restartable transparent recovery pass.
pub const MAX_TRANSPARENT_COINBASE_RECOVERY_DURATION: Duration = Duration::from_secs(5 * 60);

/// Cooperative cancellation handle for one wallet synchronization.
///
/// Cancellation is sticky and is observed between bounded network, scan, and
/// recovery batches. It does not interrupt a request or SQLite transaction
/// that is already in progress. Dropping a cancelled synchronization releases
/// the wallet operation lock before the future returns.
#[derive(Clone, Debug, Default)]
pub struct WalletSyncCancellation {
    cancelled: Arc<AtomicBool>,
}

impl WalletSyncCancellation {
    /// Creates a synchronization handle in the active state.
    pub fn new() -> Self {
        Self::default()
    }

    /// Requests cancellation of every synchronization using this handle.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    /// Returns whether cancellation has been requested.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

/// Concrete SQLite wallet database used by this crate.
pub type WalletDatabase = WalletDb<rusqlite::Connection, Network, SystemClock, OsRng>;

/// Errors returned by persistent Wcash wallet operations.
#[derive(Debug, Error)]
pub enum WalletServiceError {
    /// The requested wallet database does not exist.
    #[error("wallet database does not exist")]
    WalletDatabaseMissing,
    /// The existing SQLite database has no Wcash wallet account.
    #[error("wallet database is empty")]
    WalletDatabaseEmpty,
    /// The existing SQLite database belongs to another application or Wcash network.
    #[error("wallet database belongs to a different application or Wcash network")]
    ForeignWalletDatabase,
    /// The existing Wcash database cannot be interpreted safely.
    #[error("wallet database is corrupt or incompatible")]
    CorruptWalletDatabase,
    /// A local wallet path could not be inspected or secured.
    #[error("wallet file security check failed: {0}")]
    Io(#[from] std::io::Error),
    /// The wallet path is a symbolic link, which this tool refuses to follow.
    #[error("refusing to open a wallet database through a symbolic link")]
    SymlinkWalletPath,
    /// The wallet path does not name a regular file.
    #[error("wallet database path is not a regular file")]
    NonRegularWalletPath,
    /// Another local account can replace entries in the wallet's parent directory.
    #[error("wallet database parent directory must not be group- or world-writable")]
    InsecureWalletParent,
    /// The wallet file or its parent is not owned by the process effective UID.
    #[error("wallet database {object} must be owned by the process effective UID")]
    InsecureWalletOwnership {
        /// The object whose owner failed validation.
        object: &'static str,
    },
    /// An ancestor can be replaced by an untrusted local account.
    #[error("wallet database path has an unsafe ancestor: {0}")]
    InsecureWalletAncestor(PathBuf),
    /// An existing wallet file is readable or writable by another local account.
    #[error("existing wallet database permissions must be private; set its mode to 0600")]
    InsecureWalletPermissions,
    /// A hard-linked database has another mutable name outside the checked path.
    #[error("wallet database must have exactly one hard link")]
    HardLinkedWalletPath,
    /// The wallet pathname changed while its file was being opened.
    #[error("wallet database path changed while it was being opened")]
    WalletPathChanged,
    /// Opening the SQLite file failed.
    #[error("could not open wallet database: {0}")]
    Sqlite(#[from] rusqlite::Error),
    /// Applying the reviewed librustzcash schema failed.
    #[error("wallet database migration failed: {0}")]
    Migration(String),
    /// The database is not bound to the selected Wcash chain and derivation.
    #[error(transparent)]
    Identity(#[from] WalletDatabaseIdentityError),
    /// A wallet database operation failed.
    #[error("wallet database operation failed: {0}")]
    Database(String),
    /// The compact-block synchronizer failed.
    #[error("wallet synchronization failed: {0}")]
    Synchronization(String),
    /// The caller cooperatively cancelled wallet synchronization.
    #[error("wallet synchronization was cancelled")]
    SynchronizationCancelled,
    /// The requested account birthday cannot be represented on this chain.
    #[error("invalid wallet birthday: {0}")]
    InvalidBirthday(String),
    /// Decoding the birthday tree state failed.
    #[error("invalid birthday tree state: {0}")]
    BirthdayTree(String),
    /// The master seed did not match the only stored viewing account.
    #[error("stdin seed does not control the stored Wcash wallet account")]
    WrongSeed,
    /// Pool operation requires exactly one wallet account.
    #[error("wallet must contain exactly one account; found {0}")]
    UnexpectedAccountCount(usize),
    /// Address derivation or recipient parsing failed.
    #[error(transparent)]
    Address(#[from] WalletAddressError),
    /// Wcash seed derivation failed.
    #[error(transparent)]
    Key(#[from] WalletKeyError),
    /// The connected node failed chain/RPC validation.
    #[error(transparent)]
    Rpc(#[from] WalletRpcError),
    /// The attested client and requested wallet network differ.
    #[error("wallet network does not match the attested Zebra network")]
    ClientNetworkMismatch,
    /// The wallet has not yet scanned to the node's current tip.
    #[error("wallet must be fully synchronized before signing")]
    NotSynchronized,
    /// The live chain changed during synchronization or after transaction input selection.
    #[error("wallet chain state is stale; synchronize and rebuild the transaction")]
    StaleChain,
    /// One restartable current-UTXO recovery pass exceeded its wall-clock limit.
    #[error("transparent coinbase recovery exceeded its bounded session duration")]
    TransparentRecoveryDeadline,
    /// Another process is synchronizing or signing with this wallet.
    #[error("wallet is busy with another synchronization or signing operation")]
    WalletBusy,
    /// Compact scanning has not been followed by a complete, canonical
    /// transparent recovery pass for the stored tip.
    #[error("transparent recovery is incomplete; synchronize the wallet before reading balances or signing")]
    TransparentRecoveryIncomplete,
    /// A transparent-recovery session tried to publish completion after its
    /// durable ownership token had changed.
    #[error("transparent recovery session lost ownership of its completion marker")]
    TransparentRecoveryOwnershipLost,
    /// SQLite requested a transparent-history shape outside Wcash's single
    /// fixed payout receiver and mined-spend recovery policy.
    #[error("wallet requested unsupported transparent history: {0}")]
    UnexpectedTransparentHistoryRequest(&'static str),
    /// Signing succeeded and SQLite contains one or more transactions, but a
    /// subsequent retrieval or policy check failed.
    #[error(
        "persisted signed transactions {txids:?} require review: {reason}; recover exact bytes with list-pending or export and do not build a replacement"
    )]
    PersistedTransactionsRequireReview {
        /// Canonical display-order identifiers returned by the signer.
        txids: Vec<String>,
        /// Retrieval, serialization, policy, or canonical-chain failure.
        reason: String,
    },
    /// Refuses to create a replacement while an earlier locally signed
    /// transaction can still be mined.
    #[error(
        "unexpired signed transaction {txid} must settle, be rebroadcast, or reach expiry height {expiry_height} before signing another transaction"
    )]
    ActivePendingTransaction {
        /// Canonical display-order identifier of the transaction to resolve.
        txid: String,
        /// Signed expiry height; zero means the transaction does not expire.
        expiry_height: u32,
    },
    /// The transfer uses a confirmation policy below the public safety floor.
    #[error("confirmation policy below 100 is allowed only by an explicit Regtest-only override")]
    UnsafeConfirmations,
    /// The transfer request is malformed or outside the monetary range.
    #[error("invalid transfer request: {0}")]
    InvalidRequest(String),
    /// Input selection or fee calculation failed.
    #[error("could not construct transfer proposal: {0}")]
    Proposal(String),
    /// The proposal would use a pool other than Ironwood.
    #[error("transfer proposal is not Ironwood-only")]
    NonIronwoodProposal,
    /// The database contains value in a shielded pool disabled by Wcash.
    #[error("wallet database contains disabled Sapling or legacy Orchard value")]
    LegacyPoolState,
    /// Coinbase shielding found no mature, fully classified coinbase output.
    #[error(
        "no mature transparent coinbase output is available; coinbase requires 100 blocks and full transaction classification"
    )]
    NoMatureCoinbase,
    /// A coinbase shielding proposal violated its transparent-input and
    /// Ironwood-output invariants.
    #[error("coinbase shielding proposal violates wallet policy")]
    InvalidCoinbaseShieldingProposal,
    /// Stored viewing authority does not match the supplied spending authority.
    #[error("stored account or deterministic Ironwood change receiver does not match stdin seed")]
    AuthorityMismatch,
    /// Transaction proving or signing failed.
    #[error("could not prove and sign transfer: {0}")]
    Signing(String),
    /// The wallet failed to persist or return the newly signed transaction.
    #[error("signed transaction was not stored by the wallet")]
    MissingSignedTransaction,
    /// A numeric height calculation overflowed.
    #[error("height calculation overflow")]
    HeightOverflow,
}

/// Result of idempotently initializing one Wcash wallet account.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct InitializedWallet {
    /// Opaque database account identifier.
    pub account_id: String,
    /// First block the wallet will scan.
    pub birthday_height: u32,
    /// Canonical Wcash Unified Address for private receipts.
    pub address: String,
    /// Default Wcash P2PKH receiver for transparent coinbase payouts.
    pub transparent_coinbase_address: String,
    /// Whether this invocation created the account.
    pub created: bool,
}

/// Seedless metadata read from one existing Wcash wallet account.
///
/// This type contains public wallet identifiers and receiving addresses only;
/// it never contains spending authority or viewing keys.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct WalletInfo {
    /// Opaque database account identifier.
    pub account_id: String,
    /// First block the wallet scans for account funds.
    pub birthday_height: u32,
    /// Canonical Wcash Unified Address for private receipts.
    pub address: String,
    /// Default Wcash P2PKH receiver for transparent coinbase payouts.
    pub transparent_coinbase_address: String,
}

/// Wallet synchronization and balance summary.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct WalletBalanceSummary {
    /// Wcash node tip known to the wallet database.
    pub chain_tip_height: u32,
    /// Highest contiguous height fully scanned by the wallet.
    pub fully_scanned_height: u32,
    /// True only when scanning reached the known chain tip.
    pub synchronized: bool,
    /// Per-account balances.
    pub accounts: Vec<AccountBalanceSummary>,
}

/// Pool-separated balances for one Wcash account.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct AccountBalanceSummary {
    /// Opaque database account identifier.
    pub account_id: String,
    /// Total Ironwood value in zatoshis.
    pub ironwood_total_zat: u64,
    /// Immediately spendable Ironwood value in zatoshis.
    pub ironwood_spendable_zat: u64,
    /// Ironwood value locked by in-flight proposals.
    pub ironwood_locked_zat: u64,
    /// Ironwood change awaiting confirmations.
    pub ironwood_pending_change_zat: u64,
    /// Ironwood value awaiting scan/witness/confirmation spendability.
    pub ironwood_pending_spendability_zat: u64,
    /// Total legacy Sapling value, expected to stay zero for pool operation.
    pub sapling_total_zat: u64,
    /// Total legacy Orchard value, expected to stay zero for pool operation.
    pub orchard_total_zat: u64,
    /// Total transparent value.
    pub transparent_total_zat: u64,
    /// Total mature and immature transparent coinbase value.
    pub transparent_coinbase_total_zat: u64,
    /// Mature transparent coinbase value available for shielding.
    pub transparent_coinbase_spendable_zat: u64,
    /// Immature or otherwise pending transparent coinbase value.
    pub transparent_coinbase_pending_zat: u64,
    /// Transparent non-coinbase value. Pool wallets expect this to remain zero.
    pub transparent_regular_total_zat: u64,
}

/// One external shielded recipient supplied to transaction construction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransferRecipient {
    /// Canonical Wcash Unified Address.
    pub address: String,
    /// Amount in zatoshis.
    pub amount_zat: u64,
    /// Raw memo bytes, at most 512 bytes.
    pub memo: Vec<u8>,
}

/// Fully signed transaction bytes returned only to the caller.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SignedTransaction {
    /// Canonical display-order transaction identifier.
    pub txid: String,
    /// Exact signed transaction serialization, hex encoded.
    pub raw_transaction_hex: String,
    /// Wcash transaction consensus branch identifier.
    pub branch_id: String,
    /// Target height committed by the proposal.
    pub target_height: u32,
    /// Transaction expiry height.
    pub expiry_height: u32,
    /// Exact ZIP 317 fee in zatoshis.
    pub fee_zat: u64,
    /// True after comparing the deterministic internal change receiver from
    /// the stored UFVK against the receiver derived from the stdin seed.
    pub internal_change_receiver_verified: bool,
}

/// Signed transaction bytes recovered from the local wallet database.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct StoredSignedTransaction {
    /// Canonical display-order transaction identifier.
    pub txid: String,
    /// Exact signed transaction serialization, hex encoded.
    pub raw_transaction_hex: String,
    /// Wcash transaction consensus branch identifier.
    pub branch_id: String,
    /// Transaction expiry height encoded in the signed transaction.
    pub expiry_height: u32,
}

/// One bounded page of locally-created, not-currently-mined transactions.
///
/// Exact signed bytes are included so a caller that crashed before receiving
/// the original signing result can recover and rebroadcast the same
/// transaction rather than creating a conflicting replacement.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PendingSignedTransactionPage {
    /// Canonical wallet tip used to classify active rows. This is `None` for
    /// the unfiltered recovery-history API.
    pub exact_tip: Option<BlockRef>,
    /// Signed transactions in SQLite row order.
    pub transactions: Vec<StoredSignedTransaction>,
    /// Opaque row cursor for the next page, or `None` when this page is final.
    pub next_after_row_id: Option<u64>,
}

struct StoredTransparentCreator {
    raw: Vec<u8>,
    mined_height: u32,
    tx_index: Option<i64>,
}

struct TransparentRecoveryContext<'a> {
    parameters: &'a zebra_chain::parameters::Network,
    coinbase_address: &'a str,
    coinbase_receiver: zcash_transparent::address::TransparentAddress,
    expected_tip: BlockRef,
    deadline: tokio::time::Instant,
    cancellation: &'a WalletSyncCancellation,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TransferOutputRole {
    Payment,
    InternalChange,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BoundIronwoodOutput {
    receiver: [u8; 43],
    value_zat: u64,
    memo: MemoBytes,
    role: TransferOutputRole,
    pool: ShieldedPool,
}

const WALLET_OPERATION_LOCK_SUFFIX: &str = ".wcash-operation.lock";
const TRANSPARENT_RECOVERY_SESSION_BYTES: usize = 32;

#[derive(Clone, Copy)]
enum WalletOperationLockMode {
    Shared,
    Exclusive,
}

struct WalletOperationLock {
    _file: File,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct TransparentRecoverySession([u8; TRANSPARENT_RECOVERY_SESSION_BYTES]);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct TransparentSyncState {
    active_session: Option<TransparentRecoverySession>,
    completed_tip: Option<BlockRef>,
}

/// Opens and migrates a wallet database without loading spend authority.
pub fn open_wallet_database(
    path: impl AsRef<Path>,
    network: WalletNetwork,
) -> Result<WalletDatabase, WalletServiceError> {
    let path = path.as_ref();
    let mut wallet = open_secured_wallet(path, network)?;
    init_wallet_db(&mut wallet, None)
        .map_err(|error| WalletServiceError::Migration(error.to_string()))?;
    Ok(wallet)
}

/// Inspects one existing Wcash wallet without a seed or network request.
///
/// The database is opened read-only after taking the wallet's shared operation
/// lock. This function never creates a database, runs migrations, or modifies
/// wallet state. It fails closed for empty, foreign-network, malformed, and
/// incompatible databases.
pub fn inspect_wallet(
    path: impl AsRef<Path>,
    network: WalletNetwork,
) -> Result<WalletInfo, WalletServiceError> {
    let path = path.as_ref();
    require_existing_wallet_file(path)?;
    let _operation_lock = acquire_wallet_operation_lock(path, WalletOperationLockMode::Shared)?;
    let wallet = open_existing_wallet_read_only(path, network)?;
    let account_ids = wallet
        .get_account_ids()
        .map_err(|_| WalletServiceError::CorruptWalletDatabase)?;
    let account_id = match account_ids.as_slice() {
        [] => return Err(WalletServiceError::WalletDatabaseEmpty),
        [account_id] => *account_id,
        _ => {
            return Err(WalletServiceError::UnexpectedAccountCount(
                account_ids.len(),
            ))
        }
    };
    let account = wallet
        .get_account(account_id)
        .map_err(|_| WalletServiceError::CorruptWalletDatabase)?
        .ok_or(WalletServiceError::CorruptWalletDatabase)?;
    let ufvk = account
        .ufvk()
        .ok_or(WalletServiceError::CorruptWalletDatabase)?;
    let address = encode_orchard_receiver(ufvk, network)
        .map_err(|_| WalletServiceError::CorruptWalletDatabase)?;
    let transparent_coinbase_address = encode_transparent_coinbase_receiver(ufvk, network)
        .map_err(|_| WalletServiceError::CorruptWalletDatabase)?;

    Ok(WalletInfo {
        account_id: account_id.expose_uuid().to_string(),
        birthday_height: account.birthday_height().into(),
        address,
        transparent_coinbase_address,
    })
}

fn open_wallet_database_with_seed(
    path: impl AsRef<Path>,
    network: WalletNetwork,
    master_seed: &SecretVec<u8>,
) -> Result<WalletDatabase, WalletServiceError> {
    let path = path.as_ref();
    let mut wallet = open_secured_wallet(path, network)?;
    let migration_seed = derive_wallet_seed(master_seed, network)?;
    init_wallet_db(&mut wallet, Some(migration_seed))
        .map_err(|error| WalletServiceError::Migration(error.to_string()))?;
    Ok(wallet)
}

fn open_existing_wallet_read_only(
    path: &Path,
    network: WalletNetwork,
) -> Result<WalletDatabase, WalletServiceError> {
    let prepared = prepare_existing_wallet_path(path)?;
    let connection = open_wallet_connection_read_only(&prepared.canonical_path)
        .map_err(classify_read_only_open_error)?;
    prepared.verify_current_path()?;
    verify_existing_identity(&connection, network).map_err(|error| match error {
        ExistingWalletIdentityError::Empty => WalletServiceError::WalletDatabaseEmpty,
        ExistingWalletIdentityError::Foreign => WalletServiceError::ForeignWalletDatabase,
        ExistingWalletIdentityError::Corrupt => WalletServiceError::CorruptWalletDatabase,
    })?;
    rusqlite::vtab::array::load_module(&connection)
        .map_err(|_| WalletServiceError::CorruptWalletDatabase)?;
    prepared.verify_current_path()?;
    Ok(WalletDb::from_connection(
        connection,
        network.parameters(),
        SystemClock,
        OsRng,
    ))
}

fn open_secured_wallet(
    path: &Path,
    network: WalletNetwork,
) -> Result<WalletDatabase, WalletServiceError> {
    let prepared = prepare_wallet_path(path)?;
    let mut connection = open_wallet_connection(&prepared.canonical_path)?;
    prepared.verify_current_path()?;
    verify_or_initialize_identity(&mut connection, network)?;
    prepared.verify_current_path()?;
    rusqlite::vtab::array::load_module(&connection)?;
    Ok(WalletDb::from_connection(
        connection,
        network.parameters(),
        SystemClock,
        OsRng,
    ))
}

fn acquire_wallet_operation_lock(
    wallet_path: &Path,
    mode: WalletOperationLockMode,
) -> Result<WalletOperationLock, WalletServiceError> {
    let lock_path = wallet_operation_lock_path(wallet_path)?;
    let prepared = prepare_wallet_path(&lock_path)?;
    let lock_result = match mode {
        WalletOperationLockMode::Shared => FileExt::try_lock_shared(&prepared.opened_file),
        WalletOperationLockMode::Exclusive => FileExt::try_lock_exclusive(&prepared.opened_file),
    };
    if let Err(error) = lock_result {
        return if error.kind() == std::io::ErrorKind::WouldBlock {
            Err(WalletServiceError::WalletBusy)
        } else {
            Err(WalletServiceError::Io(error))
        };
    }
    prepared.verify_current_path()?;
    Ok(WalletOperationLock {
        _file: prepared.opened_file,
    })
}

fn wallet_operation_lock_path(wallet_path: &Path) -> Result<PathBuf, WalletServiceError> {
    let file_name = wallet_path.file_name().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "wallet path must name a database file",
        )
    })?;
    let mut lock_name = file_name.to_os_string();
    lock_name.push(WALLET_OPERATION_LOCK_SUFFIX);
    Ok(wallet_path.with_file_name(lock_name))
}

fn begin_transparent_recovery(
    wallet: &mut WalletDatabase,
) -> Result<TransparentRecoverySession, WalletServiceError> {
    let mut session = TransparentRecoverySession([0; TRANSPARENT_RECOVERY_SESSION_BYTES]);
    OsRng.fill_bytes(&mut session.0);
    wallet.transactionally_with_extension(|_wallet, extension| {
        // Validate the old row before replacing its token. Holding the
        // exclusive operation lock means an active session cannot own it, so
        // a valid in-progress row is durable crash residue and is safe to
        // reclaim.
        read_transparent_sync_state(extension)?;
        let updated = extension.execute(
            "UPDATE ext_wcash_transparent_sync_state
             SET recovery_in_progress = 1,
                 recovery_session = ?1
             WHERE singleton = 1",
            params![&session.0[..]],
        )?;
        if updated != 1 {
            return Err(WalletServiceError::Database(
                "transparent sync state singleton is missing".to_owned(),
            ));
        }
        Ok(session)
    })
}

fn complete_transparent_recovery(
    wallet: &mut WalletDatabase,
    session: TransparentRecoverySession,
    expected_tip: BlockRef,
) -> Result<(), WalletServiceError> {
    wallet.transactionally_with_extension(|wallet, extension| {
        let current_tip =
            wallet
                .get_max_height_hash()
                .map_err(database_error)?
                .map(|(height, hash)| BlockRef {
                    height: height.into(),
                    hash: hash.0,
                });
        if !transparent_tip_matches_wallet(expected_tip, current_tip) {
            return Err(WalletServiceError::StaleChain);
        }
        let updated = extension.execute(
            "UPDATE ext_wcash_transparent_sync_state
             SET recovery_in_progress = 0,
                 recovery_session = NULL,
                 completed_height = ?1,
                 completed_hash = ?2
             WHERE singleton = 1
               AND recovery_in_progress = 1
               AND recovery_session = ?3",
            params![expected_tip.height, &expected_tip.hash[..], &session.0[..]],
        )?;
        if updated != 1 {
            return Err(WalletServiceError::TransparentRecoveryOwnershipLost);
        }
        Ok(())
    })
}

fn require_transparent_recovery_complete(
    wallet: &mut WalletDatabase,
) -> Result<(), WalletServiceError> {
    wallet
        .transactionally_with_extension(|wallet, extension| {
            require_transparent_recovery_complete_in_transaction(wallet, extension)
        })
        .map(|_| ())
}

fn require_transparent_recovery_complete_in_transaction<W: WalletRead>(
    wallet: &W,
    extension: &ExtensionTransaction<'_>,
) -> Result<BlockRef, WalletServiceError> {
    let state = read_transparent_sync_state(extension)?;
    let current_tip =
        wallet
            .get_max_height_hash()
            .map_err(database_error)?
            .map(|(height, hash)| BlockRef {
                height: height.into(),
                hash: hash.0,
            });
    match (state.active_session, state.completed_tip) {
        (None, Some(completed)) if transparent_tip_matches_wallet(completed, current_tip) => {
            Ok(completed)
        }
        _ => Err(WalletServiceError::TransparentRecoveryIncomplete),
    }
}

fn exact_synchronized_recovery_tip_in_transaction<W: WalletRead>(
    wallet: &W,
    extension: &ExtensionTransaction<'_>,
) -> Result<BlockRef, WalletServiceError> {
    let completed = require_transparent_recovery_complete_in_transaction(wallet, extension)?;
    let chain_tip_height = wallet
        .chain_height()
        .map_err(database_error)?
        .ok_or(WalletServiceError::NotSynchronized)?;
    let fully_scanned = wallet
        .block_fully_scanned()
        .map_err(database_error)?
        .ok_or(WalletServiceError::NotSynchronized)?;
    let fully_scanned_tip = BlockRef {
        height: fully_scanned.block_height().into(),
        hash: fully_scanned.block_hash().0,
    };
    if u32::from(chain_tip_height) != completed.height || fully_scanned_tip != completed {
        return Err(WalletServiceError::NotSynchronized);
    }
    Ok(completed)
}

fn read_transparent_sync_state(
    extension: &ExtensionTransaction<'_>,
) -> Result<TransparentSyncState, WalletServiceError> {
    let object_type = extension.query_row(
        "SELECT type FROM sqlite_schema WHERE name = ?1",
        [TRANSPARENT_SYNC_STATE_TABLE],
        |row| row.get::<_, String>(0),
    )?;
    if object_type != "table" {
        return Err(WalletServiceError::Database(format!(
            "{TRANSPARENT_SYNC_STATE_TABLE} is not a table"
        )));
    }
    let (in_progress, session, completed_height, completed_hash) = extension.query_row(
        "SELECT recovery_in_progress, recovery_session, completed_height, completed_hash
         FROM ext_wcash_transparent_sync_state
         WHERE singleton = 1",
        [],
        |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Option<Vec<u8>>>(1)?,
                row.get::<_, Option<i64>>(2)?,
                row.get::<_, Option<Vec<u8>>>(3)?,
            ))
        },
    )?;
    let active_session = match (in_progress, session) {
        (0, None) => None,
        (1, Some(session)) => Some(TransparentRecoverySession(session.try_into().map_err(
            |_| {
                WalletServiceError::Database(
                    "transparent recovery session token is malformed".to_owned(),
                )
            },
        )?)),
        _ => {
            return Err(WalletServiceError::Database(
                "transparent recovery state is malformed".to_owned(),
            ))
        }
    };
    let completed_tip = match (completed_height, completed_hash) {
        (None, None) => None,
        (Some(height), Some(hash)) => Some(BlockRef {
            height: u32::try_from(height).map_err(|_| {
                WalletServiceError::Database(
                    "transparent recovery completion height is malformed".to_owned(),
                )
            })?,
            hash: hash.try_into().map_err(|_| {
                WalletServiceError::Database(
                    "transparent recovery completion hash is malformed".to_owned(),
                )
            })?,
        }),
        _ => {
            return Err(WalletServiceError::Database(
                "transparent recovery completion tip is malformed".to_owned(),
            ))
        }
    };
    Ok(TransparentSyncState {
        active_session,
        completed_tip,
    })
}

fn transparent_tip_matches_wallet(expected: BlockRef, wallet_tip: Option<BlockRef>) -> bool {
    wallet_tip == Some(expected) || (expected.height == 0 && wallet_tip.is_none())
}

struct PreparedWalletPath {
    canonical_path: PathBuf,
    opened_file: File,
}

impl PreparedWalletPath {
    fn verify_current_path(&self) -> Result<(), WalletServiceError> {
        require_private_parent_and_ancestors(&self.canonical_path)?;
        verify_named_file_matches(&self.canonical_path, &self.opened_file)
    }
}

fn prepare_wallet_path(path: &Path) -> Result<PreparedWalletPath, WalletServiceError> {
    reject_symlink(path)?;
    let canonical_path = canonical_wallet_path(path)?;
    require_private_parent_and_ancestors(&canonical_path)?;

    let opened_file = match private_open_options(true).open(&canonical_path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            require_private_regular_file(&canonical_path)?;
            private_open_options(false).open(&canonical_path)?
        }
        Err(error) => return Err(error.into()),
    };

    require_private_opened_file(&opened_file)?;
    require_private_parent_and_ancestors(&canonical_path)?;
    verify_named_file_matches(&canonical_path, &opened_file)?;

    Ok(PreparedWalletPath {
        canonical_path,
        opened_file,
    })
}

fn require_existing_wallet_file(path: &Path) -> Result<(), WalletServiceError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            Err(WalletServiceError::SymlinkWalletPath)
        }
        Ok(metadata) if !metadata.is_file() => Err(WalletServiceError::NonRegularWalletPath),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            Err(WalletServiceError::WalletDatabaseMissing)
        }
        Err(error) => Err(error.into()),
    }
}

fn prepare_existing_wallet_path(path: &Path) -> Result<PreparedWalletPath, WalletServiceError> {
    require_existing_wallet_file(path)?;
    let canonical_path = canonical_wallet_path(path)?;
    require_private_parent_and_ancestors(&canonical_path)?;
    require_private_regular_file(&canonical_path)?;

    let opened_file = private_read_only_open_options()
        .open(&canonical_path)
        .map_err(classify_existing_file_open_error)?;
    require_private_opened_file(&opened_file)?;
    require_private_parent_and_ancestors(&canonical_path)?;
    verify_named_file_matches(&canonical_path, &opened_file)?;

    Ok(PreparedWalletPath {
        canonical_path,
        opened_file,
    })
}

fn classify_read_only_open_error(error: rusqlite::Error) -> WalletServiceError {
    match &error {
        rusqlite::Error::SqliteFailure(code, _) if code.code == rusqlite::ErrorCode::CannotOpen => {
            WalletServiceError::WalletDatabaseMissing
        }
        _ => WalletServiceError::CorruptWalletDatabase,
    }
}

fn classify_existing_file_open_error(error: std::io::Error) -> WalletServiceError {
    if error.kind() == std::io::ErrorKind::NotFound {
        WalletServiceError::WalletDatabaseMissing
    } else {
        WalletServiceError::Io(error)
    }
}

fn private_read_only_open_options() -> OpenOptions {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options.custom_flags(nix::libc::O_CLOEXEC | nix::libc::O_NOFOLLOW);
    }
    options
}

fn private_open_options(create_new: bool) -> OpenOptions {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create_new(create_new);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options
            .mode(0o600)
            .custom_flags(nix::libc::O_CLOEXEC | nix::libc::O_NOFOLLOW);
    }
    options
}

fn canonical_wallet_path(path: &Path) -> Result<PathBuf, std::io::Error> {
    let file_name = path.file_name().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "wallet path must name a database file",
        )
    })?;
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    Ok(fs::canonicalize(parent)?.join(file_name))
}

fn reject_symlink(path: &Path) -> Result<(), WalletServiceError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            Err(WalletServiceError::SymlinkWalletPath)
        }
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

#[cfg(unix)]
fn require_private_parent_and_ancestors(path: &Path) -> Result<(), WalletServiceError> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let parent = path
        .parent()
        .expect("canonical wallet paths always have a parent directory");
    let effective_uid = nix::unistd::geteuid().as_raw();
    let parent_metadata = fs::symlink_metadata(parent)?;
    if parent_metadata.file_type().is_symlink() || !parent_metadata.is_dir() {
        return Err(WalletServiceError::NonRegularWalletPath);
    }
    require_effective_uid(parent_metadata.uid(), effective_uid, "parent directory")?;
    if parent_metadata.permissions().mode() & 0o022 != 0 {
        return Err(WalletServiceError::InsecureWalletParent);
    }

    let mut protected_child_uid = parent_metadata.uid();
    let mut ancestor = parent.parent();
    while let Some(path) = ancestor {
        let metadata = fs::symlink_metadata(path)?;
        let owner_uid = metadata.uid();
        let mode = metadata.permissions().mode();
        if metadata.file_type().is_symlink()
            || !metadata.is_dir()
            || (owner_uid != effective_uid && owner_uid != 0)
            || (mode & 0o022 != 0
                && (mode & 0o1000 == 0
                    || (protected_child_uid != effective_uid && protected_child_uid != 0)))
        {
            return Err(WalletServiceError::InsecureWalletAncestor(
                path.to_path_buf(),
            ));
        }

        protected_child_uid = owner_uid;
        ancestor = path.parent();
    }
    Ok(())
}

#[cfg(not(unix))]
fn require_private_parent_and_ancestors(_path: &Path) -> Result<(), WalletServiceError> {
    Ok(())
}

fn require_private_regular_file(path: &Path) -> Result<(), WalletServiceError> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() {
        return Err(WalletServiceError::SymlinkWalletPath);
    }
    if !metadata.is_file() {
        return Err(WalletServiceError::NonRegularWalletPath);
    }

    #[cfg(unix)]
    {
        validate_private_unix_file_metadata(&metadata)?;
    }

    Ok(())
}

fn require_private_opened_file(file: &File) -> Result<(), WalletServiceError> {
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        return Err(WalletServiceError::NonRegularWalletPath);
    }

    #[cfg(unix)]
    {
        validate_private_unix_file_metadata(&metadata)?;
    }

    Ok(())
}

#[cfg(unix)]
fn validate_private_unix_file_metadata(metadata: &fs::Metadata) -> Result<(), WalletServiceError> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    require_effective_uid(metadata.uid(), nix::unistd::geteuid().as_raw(), "file")?;
    if metadata.permissions().mode() & 0o077 != 0 {
        return Err(WalletServiceError::InsecureWalletPermissions);
    }
    if metadata.nlink() != 1 {
        return Err(WalletServiceError::HardLinkedWalletPath);
    }
    Ok(())
}

#[cfg(unix)]
fn require_effective_uid(
    owner_uid: u32,
    effective_uid: u32,
    object: &'static str,
) -> Result<(), WalletServiceError> {
    if owner_uid != effective_uid {
        return Err(WalletServiceError::InsecureWalletOwnership { object });
    }
    Ok(())
}

fn verify_named_file_matches(path: &Path, file: &File) -> Result<(), WalletServiceError> {
    let named = fs::symlink_metadata(path)?;
    if named.file_type().is_symlink() {
        return Err(WalletServiceError::SymlinkWalletPath);
    }
    if !named.is_file() {
        return Err(WalletServiceError::NonRegularWalletPath);
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;

        let opened = file.metadata()?;
        validate_private_unix_file_metadata(&named)?;
        if opened.dev() != named.dev() || opened.ino() != named.ino() {
            return Err(WalletServiceError::WalletPathChanged);
        }
    }

    Ok(())
}

/// Initializes one seed-derived account after attesting the Wcash chain.
///
/// The seed is used only in memory for migrations, account derivation, and
/// ownership validation. SQLite receives the UFVK and seed fingerprint but
/// never the master or derived seed bytes.
pub async fn initialize_wallet(
    client: &mut AttestedWcashClient,
    path: impl AsRef<Path>,
    network: WalletNetwork,
    master_seed: &SecretVec<u8>,
    requested_birthday: Option<u32>,
) -> Result<InitializedWallet, WalletServiceError> {
    ensure_client_network(client, network)?;
    let path = path.as_ref();
    let _operation_lock = acquire_wallet_operation_lock(path, WalletOperationLockMode::Exclusive)?;
    let mut wallet = open_wallet_database_with_seed(path, network, master_seed)?;
    let account_ids = wallet.get_account_ids().map_err(database_error)?;

    if !account_ids.is_empty() {
        let account_id = only_account(&account_ids)?;
        verify_seed_authority(&wallet, account_id, master_seed, network)?;
        let account = wallet
            .get_account(account_id)
            .map_err(database_error)?
            .ok_or(WalletServiceError::AuthorityMismatch)?;
        let ufvk = account
            .ufvk()
            .ok_or(WalletServiceError::AuthorityMismatch)?;
        return Ok(InitializedWallet {
            account_id: account_id.expose_uuid().to_string(),
            birthday_height: account.birthday_height().into(),
            address: encode_orchard_receiver(ufvk, network)?,
            transparent_coinbase_address: encode_transparent_coinbase_receiver(ufvk, network)?,
            created: false,
        });
    }

    let tip = client.latest_block().await?;
    let maximum_birthday = tip
        .height
        .checked_add(1)
        .ok_or(WalletServiceError::HeightOverflow)?;
    let birthday_height = requested_birthday.unwrap_or_else(|| default_wallet_birthday(tip.height));
    if birthday_height == 0 || birthday_height > maximum_birthday {
        return Err(WalletServiceError::InvalidBirthday(format!(
            "height {birthday_height} is outside 1..={maximum_birthday}"
        )));
    }
    let prior_height = birthday_height
        .checked_sub(1)
        .ok_or(WalletServiceError::HeightOverflow)?;
    let birthday_state = client.tree_state(prior_height).await?;
    let recover_until =
        (birthday_height <= tip.height).then_some(BlockHeight::from_u32(tip.height));
    let birthday = AccountBirthday::from_treestate(birthday_state, recover_until)
        .map_err(|error| WalletServiceError::BirthdayTree(error.to_string()))?;

    let account_seed = derive_wallet_seed(master_seed, network)?;
    let (account_id, usk) = wallet
        .create_account(
            "Wcash local wallet",
            &account_seed,
            &birthday,
            Some("stdin-seed-v1"),
        )
        .map_err(database_error)?;
    let ufvk = usk.to_unified_full_viewing_key();
    let address = encode_orchard_receiver(&ufvk, network)?;
    let transparent_coinbase_address = encode_transparent_coinbase_receiver(&ufvk, network)?;
    Ok(InitializedWallet {
        account_id: account_id.expose_uuid().to_string(),
        birthday_height,
        address,
        transparent_coinbase_address,
        created: true,
    })
}

/// Synchronizes compact Ironwood blocks and classifies current coinbase UTXOs.
///
/// The reviewed synchronizer refreshes the current transparent UTXO set. Wcash
/// then fetches and verifies each current UTXO's full creator transaction for
/// the default coinbase receiver, because the lightwalletd UTXO message does
/// not carry the transaction index needed to classify an output as coinbase.
/// Repeating this current-set pass after every sync repairs classification after
/// any upstream rewind without a separate historical cursor.
///
/// The current-set query starts at height one rather than the shielded scanning
/// birthday. The fixed transparent receiver can be derived and published before
/// SQLite initialization, so this guarantees recovery of every still-unspent
/// coinbase controlled by the seed without replaying spent history.
///
/// A creator transaction with both spent and unspent outputs to this same
/// receiver is rejected rather than risking resurrection of the spent sibling.
/// The supported mining path therefore uses one payout-address output in each
/// coinbase transaction; a nonstandard split creator requires manual recovery.
pub async fn synchronize_wallet(
    client: &mut AttestedWcashClient,
    path: impl AsRef<Path>,
    network: WalletNetwork,
    batch_size: u32,
) -> Result<WalletBalanceSummary, WalletServiceError> {
    let cancellation = WalletSyncCancellation::new();
    synchronize_wallet_cancellable(client, path, network, batch_size, &cancellation).await
}

/// Synchronizes the wallet while permitting cooperative foreground cancellation.
///
/// This is the cancellable counterpart to [`synchronize_wallet`]. The handle
/// is checked between bounded compact-block, subtree-root, transparent-UTXO,
/// creator-transaction, and transparent-history batches. Cancelling leaves
/// every completed SQLite transaction durable, marks the wallet as requiring
/// a resumed synchronization, and releases the exclusive operation lock before
/// returning [`WalletServiceError::SynchronizationCancelled`].
pub async fn synchronize_wallet_cancellable(
    client: &mut AttestedWcashClient,
    path: impl AsRef<Path>,
    network: WalletNetwork,
    batch_size: u32,
    cancellation: &WalletSyncCancellation,
) -> Result<WalletBalanceSummary, WalletServiceError> {
    if batch_size == 0 || batch_size > MAX_SYNC_BATCH_SIZE {
        return Err(WalletServiceError::InvalidRequest(format!(
            "sync batch size must be in 1..={MAX_SYNC_BATCH_SIZE}"
        )));
    }
    ensure_client_network(client, network)?;
    let path = path.as_ref();
    let _operation_lock = acquire_wallet_operation_lock(path, WalletOperationLockMode::Exclusive)?;
    ensure_sync_not_cancelled(cancellation)?;
    let mut wallet = open_wallet_database(path, network)?;
    let account_ids = wallet.get_account_ids().map_err(database_error)?;
    let account_id = only_account(&account_ids)?;
    let account = wallet
        .get_account(account_id)
        .map_err(database_error)?
        .ok_or(WalletServiceError::AuthorityMismatch)?;
    let account_ufvk = account
        .ufvk()
        .ok_or(WalletServiceError::AuthorityMismatch)?;
    let coinbase_receiver = default_transparent_receiver(account_ufvk)?;
    let coinbase_address = encode_transparent_coinbase_receiver(account_ufvk, network)?;
    let recovery_session = begin_transparent_recovery(&mut wallet)?;
    let cache = MemoryBlockCache::default();
    let parameters = network.parameters();
    match sync::run(
        client,
        &parameters,
        &cache,
        &mut wallet,
        batch_size,
        cancellation,
    )
    .await
    {
        Ok(()) => {}
        Err(sync::WcashSyncError::Cancelled) => {
            return Err(WalletServiceError::SynchronizationCancelled)
        }
        Err(error) => return Err(WalletServiceError::Synchronization(error.to_string())),
    }
    ensure_sync_not_cancelled(cancellation)?;

    let synced = wallet_balance_summary(&wallet, public_confirmation_policy())?;
    if !synced.synchronized {
        return Err(WalletServiceError::NotSynchronized);
    }
    let tip_height = synced.chain_tip_height;
    let expected_tip = BlockRef {
        height: tip_height,
        hash: if tip_height == 0 {
            network.genesis_hash()
        } else {
            wallet
                .get_block_hash(BlockHeight::from_u32(tip_height))
                .map_err(database_error)?
                .ok_or(WalletServiceError::NotSynchronized)?
                .0
        },
    };
    if client.latest_block().await? != expected_tip {
        return Err(WalletServiceError::StaleChain);
    }
    if transparent_coinbase_recovery_start(tip_height).is_some() {
        let recovery = TransparentRecoveryContext {
            parameters: &parameters,
            coinbase_address: &coinbase_address,
            coinbase_receiver,
            expected_tip,
            deadline: tokio::time::Instant::now() + MAX_TRANSPARENT_COINBASE_RECOVERY_DURATION,
            cancellation,
        };
        if let Err(error) = recover_transparent_coinbase(client, &mut wallet, &recovery).await {
            // Transparent recovery can commit verified transactions and
            // request-frontier progress incrementally. If the complete pass
            // does not finish, rewind one compact height so no later command
            // can mistake this database for a fully synchronized wallet.
            wallet
                .truncate_to_height(BlockHeight::from_u32(tip_height.saturating_sub(1)))
                .map_err(database_error)?;
            return Err(error);
        }
    }
    let recovered = wallet_balance_summary(&wallet, public_confirmation_policy())?;
    if !recovered.synchronized || recovered.chain_tip_height != expected_tip.height {
        return Err(WalletServiceError::NotSynchronized);
    }
    ensure_sync_not_cancelled(cancellation)?;
    complete_transparent_recovery(&mut wallet, recovery_session, expected_tip)?;
    Ok(recovered)
}

pub(crate) fn ensure_sync_not_cancelled(
    cancellation: &WalletSyncCancellation,
) -> Result<(), WalletServiceError> {
    if cancellation.is_cancelled() {
        Err(WalletServiceError::SynchronizationCancelled)
    } else {
        Ok(())
    }
}

async fn recover_transparent_coinbase(
    client: &mut AttestedWcashClient,
    wallet: &mut WalletDatabase,
    recovery: &TransparentRecoveryContext<'_>,
) -> Result<(), WalletServiceError> {
    let mut page_start = TRANSPARENT_COINBASE_RECOVERY_START_HEIGHT;
    loop {
        require_exact_recovery_tip(client, recovery).await?;
        let page = await_transparent_recovery_rpc(
            recovery.deadline,
            recovery.cancellation,
            client.transparent_unspent_page(
                recovery.coinbase_address,
                page_start,
                recovery.expected_tip.height,
                TRANSPARENT_UTXO_PAGE_OUTPUTS,
            ),
        )
        .await?;
        require_exact_recovery_tip(client, recovery).await?;
        let next_start = page.next_start_height(page_start, TRANSPARENT_UTXO_PAGE_OUTPUTS)?;
        let creators = page.into_complete_creators(next_start);
        let stored_creators =
            fully_classified_stored_creators(client, wallet, &creators, recovery)?;
        for creator in creators {
            ensure_sync_not_cancelled(recovery.cancellation)?;
            if stored_creators.contains(&creator.txid()) {
                continue;
            }
            require_exact_recovery_tip(client, recovery).await?;
            let transaction = await_transparent_recovery_rpc(
                recovery.deadline,
                recovery.cancellation,
                client.transparent_creator_transaction(creator),
            )
            .await?;
            require_exact_recovery_tip(client, recovery).await?;
            decrypt_and_store_transaction(
                recovery.parameters,
                wallet,
                &transaction.transaction,
                Some(BlockHeight::from_u32(transaction.height)),
            )
            .map_err(database_error)?;
            ensure_transparent_recovery_deadline(recovery.deadline)?;
        }
        match next_start {
            Some(next_start) => page_start = next_start,
            None => break,
        }
    }

    recover_transparent_spends(client, wallet, recovery).await?;
    require_exact_recovery_tip(client, recovery).await
}

fn fully_classified_stored_creators(
    client: &AttestedWcashClient,
    wallet: &mut WalletDatabase,
    creators: &[crate::rpc::TransparentUnspentCreator],
    recovery: &TransparentRecoveryContext<'_>,
) -> Result<HashSet<zcash_protocol::TxId>, WalletServiceError> {
    wallet.transactionally_with_extension(|_wallet, extension| {
        let mut fully_classified = HashSet::with_capacity(creators.len());
        for creator in creators {
            ensure_sync_not_cancelled(recovery.cancellation)?;
            let stored = extension
                .query_row(
                    "SELECT raw, mined_height, tx_index
                     FROM transactions
                     WHERE txid = ?1
                       AND raw IS NOT NULL
                       AND mined_height IS NOT NULL",
                    params![creator.txid().as_ref()],
                    |row| {
                        let mined_height = row.get::<_, i64>(1)?;
                        Ok(StoredTransparentCreator {
                            raw: row.get(0)?,
                            mined_height: u32::try_from(mined_height).map_err(|_| {
                                rusqlite::Error::IntegralValueOutOfRange(1, mined_height)
                            })?,
                            tx_index: row.get(2)?,
                        })
                    },
                )
                .optional()?;
            let Some(stored) = stored else { continue };
            let is_fully_classified = match client.validate_stored_transparent_creator(
                creator,
                &stored.raw,
                stored.mined_height,
            ) {
                Ok(true) if stored.tx_index == Some(0) => true,
                Ok(true) if stored.tx_index.is_none() => false,
                Ok(true) => {
                    return Err(WalletServiceError::Database(
                        "stored coinbase creator has a nonzero transaction index".to_owned(),
                    ))
                }
                Ok(false) if stored.tx_index.is_none_or(|index| index > 0) => true,
                Ok(false) => {
                    return Err(WalletServiceError::Database(
                        "stored non-coinbase creator has an invalid transaction index".to_owned(),
                    ))
                }
                Err(_) => false,
            };
            ensure_transparent_recovery_deadline(recovery.deadline)?;
            if is_fully_classified {
                fully_classified.insert(creator.txid());
            }
        }
        Ok(fully_classified)
    })
}

async fn recover_transparent_spends(
    client: &mut AttestedWcashClient,
    wallet: &mut WalletDatabase,
    recovery: &TransparentRecoveryContext<'_>,
) -> Result<(), WalletServiceError> {
    let maximum_end = recovery.expected_tip.height.checked_add(1).ok_or(
        WalletServiceError::UnexpectedTransparentHistoryRequest("chain-tip height overflow"),
    )?;

    loop {
        ensure_sync_not_cancelled(recovery.cancellation)?;
        ensure_transparent_recovery_deadline(recovery.deadline)?;
        let groups = group_transparent_history_requests(
            wallet.transaction_data_requests().map_err(database_error)?,
            recovery.coinbase_receiver,
            maximum_end,
        )?;
        let Some(((start, end), requests)) = groups.into_iter().next() else {
            return Ok(());
        };

        let final_height = end - 1;
        let expected_end = await_transparent_recovery_rpc(
            recovery.deadline,
            recovery.cancellation,
            client.compact_block_ref(final_height),
        )
        .await?;
        let mut cursor = start;
        while cursor < end {
            ensure_sync_not_cancelled(recovery.cancellation)?;
            let chunk_end = cursor
                .checked_add(MAX_TRANSPARENT_HISTORY_BLOCKS_PER_REQUEST)
                .map_or(end, |candidate| candidate.min(end));
            let transactions = await_transparent_recovery_rpc(
                recovery.deadline,
                recovery.cancellation,
                client.transparent_transactions_range(
                    recovery.coinbase_address,
                    cursor,
                    chunk_end - 1,
                ),
            )
            .await?;
            for transaction in transactions {
                ensure_sync_not_cancelled(recovery.cancellation)?;
                decrypt_and_store_transaction(
                    recovery.parameters,
                    wallet,
                    &transaction.transaction,
                    Some(BlockHeight::from_u32(transaction.height)),
                )
                .map_err(database_error)?;
                ensure_transparent_recovery_deadline(recovery.deadline)?;
            }
            cursor = chunk_end;
        }
        if await_transparent_recovery_rpc(
            recovery.deadline,
            recovery.cancellation,
            client.compact_block_ref(final_height),
        )
        .await?
            != expected_end
        {
            return Err(WalletServiceError::StaleChain);
        }
        for request in requests {
            ensure_sync_not_cancelled(recovery.cancellation)?;
            wallet
                .notify_address_checked(request, BlockHeight::from_u32(final_height))
                .map_err(database_error)?;
        }
        // Recompute after each durable notification because decrypting this
        // range can discover spends, add creators, or retire other requests.
    }
}

fn group_transparent_history_requests(
    requests: Vec<TransactionDataRequest>,
    expected_receiver: zcash_transparent::address::TransparentAddress,
    maximum_end: u32,
) -> Result<BTreeMap<(u32, u32), Vec<TransactionsInvolvingAddress>>, WalletServiceError> {
    let mut groups = BTreeMap::<(u32, u32), Vec<TransactionsInvolvingAddress>>::new();
    for request in requests {
        let TransactionDataRequest::TransactionsInvolvingAddress(request) = request else {
            continue;
        };
        if request.tx_status_filter() != &TransactionStatusFilter::Mined
            || request.output_status_filter() != &OutputStatusFilter::All
        {
            // librustzcash schedules `All/Unspent` probes for reserved ZIP 320
            // ephemeral addresses even though Wcash never exposes or funds
            // them. They are outside this fixed coinbase-recovery boundary. A
            // mined/all-output request for another funded receiver is rejected
            // below rather than silently ignored.
            if request.address() == expected_receiver {
                return Err(WalletServiceError::UnexpectedTransparentHistoryRequest(
                    "coinbase receiver request is not mined all-output spend discovery",
                ));
            }
            continue;
        }
        if request.address() != expected_receiver {
            return Err(WalletServiceError::UnexpectedTransparentHistoryRequest(
                "receiver is not the fixed coinbase address",
            ));
        }
        let original_start: u32 = request.block_range_start().into();
        let start = original_start.max(TRANSPARENT_COINBASE_RECOVERY_START_HEIGHT);
        let end: u32 = request
            .block_range_end()
            .ok_or(WalletServiceError::UnexpectedTransparentHistoryRequest(
                "request has no finite end height",
            ))?
            .into();
        if start >= end || end > maximum_end {
            return Err(WalletServiceError::UnexpectedTransparentHistoryRequest(
                "request range is outside the synchronized tip",
            ));
        }
        groups.entry((start, end)).or_default().push(request);
    }
    Ok(groups)
}

async fn require_exact_recovery_tip(
    client: &mut AttestedWcashClient,
    recovery: &TransparentRecoveryContext<'_>,
) -> Result<(), WalletServiceError> {
    if await_transparent_recovery_rpc(
        recovery.deadline,
        recovery.cancellation,
        client.latest_block(),
    )
    .await?
        == recovery.expected_tip
    {
        Ok(())
    } else {
        Err(WalletServiceError::StaleChain)
    }
}

async fn await_transparent_recovery_rpc<T>(
    deadline: tokio::time::Instant,
    cancellation: &WalletSyncCancellation,
    operation: impl Future<Output = Result<T, WalletRpcError>>,
) -> Result<T, WalletServiceError> {
    ensure_sync_not_cancelled(cancellation)?;
    let result = tokio::time::timeout_at(deadline, operation)
        .await
        .map_err(|_| WalletServiceError::TransparentRecoveryDeadline)?
        .map_err(WalletServiceError::Rpc)?;
    ensure_sync_not_cancelled(cancellation)?;
    Ok(result)
}

fn ensure_transparent_recovery_deadline(
    deadline: tokio::time::Instant,
) -> Result<(), WalletServiceError> {
    if tokio::time::Instant::now() >= deadline {
        Err(WalletServiceError::TransparentRecoveryDeadline)
    } else {
        Ok(())
    }
}

fn default_wallet_birthday(tip_height: u32) -> u32 {
    tip_height.saturating_sub(99).max(1)
}

fn transparent_coinbase_recovery_start(tip_height: u32) -> Option<u32> {
    (TRANSPARENT_COINBASE_RECOVERY_START_HEIGHT <= tip_height)
        .then_some(TRANSPARENT_COINBASE_RECOVERY_START_HEIGHT)
}

/// Returns current SQLite wallet balances without a network request or migration.
///
/// Balance readers share the cooperative operation lock and open the existing
/// database read-only. Schema creation and migration remain exclusive wallet
/// operations, so a read can never write while another reader holds the lock.
pub fn wallet_balance(
    path: impl AsRef<Path>,
    network: WalletNetwork,
) -> Result<WalletBalanceSummary, WalletServiceError> {
    wallet_balance_with_confirmations(path, network, COINBASE_SHIELDING_MATURITY, false)
}

/// Returns current SQLite wallet balances under an explicit confirmation policy.
///
/// A confirmation count below the public floor requires both `Regtest` and the
/// explicit unsafe Regtest override. Policy validation precedes wallet access.
pub fn wallet_balance_with_confirmations(
    path: impl AsRef<Path>,
    network: WalletNetwork,
    confirmations: u32,
    allow_unsafe_regtest_confirmations: bool,
) -> Result<WalletBalanceSummary, WalletServiceError> {
    let confirmations =
        validate_confirmation_policy(network, confirmations, allow_unsafe_regtest_confirmations)?;
    let path = path.as_ref();
    require_existing_wallet_file(path)?;
    let _operation_lock = acquire_wallet_operation_lock(path, WalletOperationLockMode::Shared)?;
    let mut wallet = open_existing_wallet_read_only(path, network)?;
    require_transparent_recovery_complete(&mut wallet)?;
    wallet_balance_summary(
        &wallet,
        ConfirmationsPolicy::new_symmetrical(confirmations, false),
    )
}

/// Recovers exact signed bytes previously persisted by transaction creation.
pub fn stored_signed_transaction(
    path: impl AsRef<Path>,
    network: WalletNetwork,
    txid: zcash_protocol::TxId,
) -> Result<StoredSignedTransaction, WalletServiceError> {
    let wallet = open_wallet_database(path, network)?;
    let transaction = wallet
        .get_transaction(txid)
        .map_err(database_error)?
        .ok_or(WalletServiceError::MissingSignedTransaction)?;
    let mut raw = Vec::new();
    transaction
        .write(&mut raw)
        .map_err(|error| WalletServiceError::Signing(error.to_string()))?;
    let inspected = inspect_signed_transaction(&raw, network)?;
    if inspected.txid() != txid {
        return Err(WalletServiceError::MissingSignedTransaction);
    }
    Ok(StoredSignedTransaction {
        txid: txid.to_string(),
        raw_transaction_hex: hex::encode(raw),
        branch_id: network.branch_id_hex(),
        expiry_height: inspected.expiry_height().into(),
    })
}

/// Lists a bounded page of exact signed bytes created by this wallet that are
/// not currently recorded as mined.
///
/// This is the crash-recovery entry point when signing committed to SQLite but
/// the process exited before it printed a transaction identifier. The cursor is
/// an opaque local database row identifier and has no consensus meaning.
pub fn pending_signed_transactions(
    path: impl AsRef<Path>,
    network: WalletNetwork,
    after_row_id: Option<u64>,
    limit: usize,
) -> Result<PendingSignedTransactionPage, WalletServiceError> {
    pending_signed_transactions_inner(
        path,
        network,
        PendingTransactionQuery::All,
        after_row_id,
        limit,
    )
}

/// Lists a bounded page of locally-created, unmined signed transactions that
/// remain valid after the wallet's internally attested synchronized tip.
///
/// Expired rows remain in SQLite for history and reorganization recovery. A
/// later call after a rewind can therefore expose them again. Pass the exact
/// tip returned by the preceding page when continuing pagination; a same-height
/// reorganization then fails closed instead of changing the result set.
pub fn active_pending_signed_transactions(
    path: impl AsRef<Path>,
    network: WalletNetwork,
    expected_tip: Option<BlockRef>,
    after_row_id: Option<u64>,
    limit: usize,
) -> Result<PendingSignedTransactionPage, WalletServiceError> {
    if after_row_id.is_some() && expected_tip.is_none() {
        return Err(WalletServiceError::InvalidRequest(
            "active pending continuation requires the preceding page's exact tip".to_owned(),
        ));
    }
    pending_signed_transactions_inner(
        path,
        network,
        PendingTransactionQuery::Active { expected_tip },
        after_row_id,
        limit,
    )
}

#[derive(Clone, Copy)]
enum PendingTransactionQuery {
    All,
    Active {
        expected_tip: Option<BlockRef>,
    },
    #[cfg(test)]
    ActiveAtAttestedTip(BlockRef),
}

fn pending_signed_transactions_inner(
    path: impl AsRef<Path>,
    network: WalletNetwork,
    query: PendingTransactionQuery,
    after_row_id: Option<u64>,
    limit: usize,
) -> Result<PendingSignedTransactionPage, WalletServiceError> {
    if limit == 0 || limit > MAX_PENDING_TRANSACTION_PAGE_SIZE {
        return Err(WalletServiceError::InvalidRequest(format!(
            "pending transaction page size must be in 1..={MAX_PENDING_TRANSACTION_PAGE_SIZE}"
        )));
    }
    let after_row_id = i64::try_from(after_row_id.unwrap_or(0)).map_err(|_| {
        WalletServiceError::InvalidRequest("pending transaction cursor is too large".to_owned())
    })?;
    let path = path.as_ref();
    require_existing_wallet_file(path)?;
    let _operation_lock = acquire_wallet_operation_lock(path, WalletOperationLockMode::Shared)?;
    let mut wallet = open_existing_wallet_read_only(path, network)?;
    pending_signed_transaction_page(&mut wallet, network, query, after_row_id, limit)
}

fn pending_signed_transaction_page(
    wallet: &mut WalletDatabase,
    network: WalletNetwork,
    query: PendingTransactionQuery,
    after_row_id: i64,
    limit: usize,
) -> Result<PendingSignedTransactionPage, WalletServiceError> {
    pending_signed_transaction_page_with_inspector(
        wallet,
        network,
        query,
        after_row_id,
        limit,
        |raw| {
            let inspected = inspect_signed_transaction(raw, network)?;
            Ok((inspected.txid(), inspected.expiry_height().into()))
        },
    )
}

fn pending_signed_transaction_page_with_inspector<F>(
    wallet: &mut WalletDatabase,
    network: WalletNetwork,
    query: PendingTransactionQuery,
    after_row_id: i64,
    limit: usize,
    mut inspect: F,
) -> Result<PendingSignedTransactionPage, WalletServiceError>
where
    F: FnMut(&[u8]) -> Result<(zcash_protocol::TxId, u32), WalletServiceError>,
{
    wallet.transactionally_with_extension(|wallet, extension| {
        let exact_tip = match query {
            PendingTransactionQuery::All => None,
            PendingTransactionQuery::Active { expected_tip } => {
                let actual_tip = exact_synchronized_recovery_tip_in_transaction(wallet, extension)?;
                verify_expected_pending_tip(expected_tip, actual_tip)?;
                Some(actual_tip)
            }
            #[cfg(test)]
            PendingTransactionQuery::ActiveAtAttestedTip(exact_tip) => Some(exact_tip),
        };
        let mut transactions = Vec::with_capacity(limit);
        let mut cursor = after_row_id;
        loop {
            let metadata = extension
                .query_row(
                    "SELECT id_tx, txid, expiry_height, length(raw)
                     FROM transactions
                     WHERE id_tx > ?1
                       AND created IS NOT NULL
                       AND mined_height IS NULL
                       AND raw IS NOT NULL
                     ORDER BY id_tx
                     LIMIT 1",
                    [cursor],
                    |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, Vec<u8>>(1)?,
                            row.get::<_, Option<i64>>(2)?,
                            row.get::<_, i64>(3)?,
                        ))
                    },
                )
                .optional()?;
            let Some((row_id, txid_bytes, expiry_height, raw_len)) = metadata else {
                return Ok(PendingSignedTransactionPage {
                    exact_tip,
                    transactions,
                    next_after_row_id: None,
                });
            };
            if row_id <= cursor || raw_len < 0 {
                return Err(WalletServiceError::Database(
                    "invalid pending transaction row metadata".to_owned(),
                ));
            }
            let raw_len = usize::try_from(raw_len).map_err(|_| {
                WalletServiceError::Database(
                    "pending transaction length is not representable".to_owned(),
                )
            })?;
            if raw_len > zcash_protocol::constants::MAX_BLOCK_BYTES {
                return Err(WalletServiceError::Database(format!(
                    "pending transaction row {row_id} exceeds the consensus block-size bound"
                )));
            }
            let txid = zcash_protocol::TxId::from_bytes(txid_bytes.try_into().map_err(|_| {
                WalletServiceError::Database(format!(
                    "pending transaction row {row_id} has an invalid transaction identifier"
                ))
            })?);
            let expiry_height = expiry_height
                .map(|height| {
                    u32::try_from(height).map_err(|_| {
                        WalletServiceError::Database(format!(
                            "pending transaction row {row_id} has an invalid expiry height"
                        ))
                    })
                })
                .transpose()?;
            let raw = extension.query_row(
                "SELECT raw FROM transactions WHERE id_tx = ?1",
                [row_id],
                |row| row.get::<_, Vec<u8>>(0),
            )?;
            if raw.len() != raw_len {
                return Err(WalletServiceError::Database(format!(
                    "pending transaction row {row_id} changed during recovery"
                )));
            }
            let (inspected_txid, inspected_expiry_height) = inspect(&raw)?;
            if inspected_txid != txid
                || expiry_height.is_some_and(|stored| stored != inspected_expiry_height)
                || (exact_tip.is_some() && expiry_height.is_none())
            {
                return Err(WalletServiceError::Database(format!(
                    "persisted transaction {txid} has mismatched recovery metadata"
                )));
            }
            let is_active = exact_tip.is_none_or(|tip| {
                inspected_expiry_height == 0 || inspected_expiry_height > tip.height
            });
            if is_active && transactions.len() == limit {
                let next_after_row_id = u64::try_from(cursor).map_err(|_| {
                    WalletServiceError::Database(
                        "pending transaction cursor is not representable".to_owned(),
                    )
                })?;
                return Ok(PendingSignedTransactionPage {
                    exact_tip,
                    transactions,
                    next_after_row_id: Some(next_after_row_id),
                });
            }
            cursor = row_id;
            if is_active {
                transactions.push(StoredSignedTransaction {
                    txid: txid.to_string(),
                    raw_transaction_hex: hex::encode(raw),
                    branch_id: network.branch_id_hex(),
                    expiry_height: inspected_expiry_height,
                });
            }
        }
    })
}

fn verify_expected_pending_tip(
    expected_tip: Option<BlockRef>,
    actual_tip: BlockRef,
) -> Result<(), WalletServiceError> {
    if expected_tip.is_none_or(|expected| expected == actual_tip) {
        Ok(())
    } else {
        Err(WalletServiceError::StaleChain)
    }
}

fn ensure_no_active_pending_transactions(
    wallet: &mut WalletDatabase,
    network: WalletNetwork,
) -> Result<(), WalletServiceError> {
    let page = pending_signed_transaction_page(
        wallet,
        network,
        PendingTransactionQuery::Active { expected_tip: None },
        0,
        1,
    )?;
    match page.transactions.first() {
        Some(transaction) => Err(WalletServiceError::ActivePendingTransaction {
            txid: transaction.txid.clone(),
            expiry_height: transaction.expiry_height,
        }),
        None => Ok(()),
    }
}

fn wallet_balance_summary(
    wallet: &WalletDatabase,
    confirmations: ConfirmationsPolicy,
) -> Result<WalletBalanceSummary, WalletServiceError> {
    let summary = wallet
        .get_wallet_summary(confirmations)
        .map_err(database_error)?
        .ok_or(WalletServiceError::NotSynchronized)?;
    let mut accounts = Vec::with_capacity(summary.account_balances().len());
    for (account_id, balance) in summary.account_balances() {
        reject_legacy_balance_values(
            balance.sapling_balance().total().into_u64(),
            balance.orchard_balance().total().into_u64(),
        )?;
        let ironwood = balance.ironwood_balance();
        let transparent_regular = balance.unshielded_regular_balance();
        let transparent_coinbase = balance.unshielded_coinbase_balance();
        let transparent_total = (transparent_regular.total() + transparent_coinbase.total())
            .ok_or_else(|| {
                WalletServiceError::Database("transparent balance overflow".to_owned())
            })?;
        accounts.push(AccountBalanceSummary {
            account_id: account_id.expose_uuid().to_string(),
            ironwood_total_zat: ironwood.total().into_u64(),
            ironwood_spendable_zat: ironwood.spendable_value().into_u64(),
            ironwood_locked_zat: ironwood.locked_value().into_u64(),
            ironwood_pending_change_zat: ironwood.change_pending_confirmation().into_u64(),
            ironwood_pending_spendability_zat: ironwood.value_pending_spendability().into_u64(),
            sapling_total_zat: balance.sapling_balance().total().into_u64(),
            orchard_total_zat: balance.orchard_balance().total().into_u64(),
            transparent_total_zat: transparent_total.into_u64(),
            transparent_coinbase_total_zat: transparent_coinbase.total().into_u64(),
            transparent_coinbase_spendable_zat: transparent_coinbase.spendable_value().into_u64(),
            transparent_coinbase_pending_zat: transparent_coinbase
                .value_pending_spendability()
                .into_u64(),
            transparent_regular_total_zat: transparent_regular.total().into_u64(),
        });
    }
    accounts.sort_by(|left, right| left.account_id.cmp(&right.account_id));
    Ok(WalletBalanceSummary {
        chain_tip_height: summary.chain_tip_height().into(),
        fully_scanned_height: summary.fully_scanned_height().into(),
        synchronized: summary.is_synced(),
        accounts,
    })
}

pub(crate) fn ensure_no_legacy_pool_balances(
    wallet: &WalletDatabase,
) -> Result<(), WalletServiceError> {
    let summary = wallet
        .get_wallet_summary(public_confirmation_policy())
        .map_err(database_error)?;
    let Some(summary) = summary else {
        // A newly initialized database has no balance view until its first
        // compact scan. The same guard runs again before synchronization can
        // return successfully.
        return Ok(());
    };
    for balance in summary.account_balances().values() {
        reject_legacy_balance_values(
            balance.sapling_balance().total().into_u64(),
            balance.orchard_balance().total().into_u64(),
        )?;
    }
    Ok(())
}

fn reject_legacy_balance_values(
    sapling_total_zat: u64,
    orchard_total_zat: u64,
) -> Result<(), WalletServiceError> {
    if sapling_total_zat == 0 && orchard_total_zat == 0 {
        Ok(())
    } else {
        Err(WalletServiceError::LegacyPoolState)
    }
}

/// Creates, proves, signs, persists, and returns one Ironwood-only Wcash transaction.
///
/// Values below 100 confirmations are accepted only for `Regtest` when
/// `allow_unsafe_regtest_confirmations` is true. This is a one-shot experimental
/// transfer operation, not a durable or idempotent pool settlement API.
#[allow(clippy::too_many_arguments)]
pub async fn create_signed_transfer(
    client: &mut AttestedWcashClient,
    path: impl AsRef<Path>,
    network: WalletNetwork,
    master_seed: &SecretVec<u8>,
    recipients: Vec<TransferRecipient>,
    confirmations: u32,
    allow_unsafe_regtest_confirmations: bool,
    expiry_delta: u32,
    lock_for_blocks: u32,
) -> Result<SignedTransaction, WalletServiceError> {
    ensure_client_network(client, network)?;
    let confirmation_count =
        validate_confirmation_policy(network, confirmations, allow_unsafe_regtest_confirmations)?;
    if recipients.is_empty() || recipients.len() > MAX_TRANSFER_RECIPIENTS {
        return Err(WalletServiceError::InvalidRequest(format!(
            "recipient count must be in 1..={MAX_TRANSFER_RECIPIENTS}"
        )));
    }
    if expiry_delta == 0
        || expiry_delta > MAX_EXPIRY_DELTA
        || lock_for_blocks < expiry_delta
        || lock_for_blocks > MAX_LOCK_FOR_BLOCKS
    {
        return Err(WalletServiceError::InvalidRequest(
            format!(
                "expiry must be in 1..={MAX_EXPIRY_DELTA}, and lock lifetime must cover expiry without exceeding {MAX_LOCK_FOR_BLOCKS}"
            ),
        ));
    }

    let path = path.as_ref();
    let _operation_lock = acquire_wallet_operation_lock(path, WalletOperationLockMode::Exclusive)?;
    let mut wallet = open_wallet_database_with_seed(path, network, master_seed)?;
    require_transparent_recovery_complete(&mut wallet)?;
    ensure_no_active_pending_transactions(&mut wallet, network)?;
    ensure_no_legacy_pool_balances(&wallet)?;
    let account_ids = wallet.get_account_ids().map_err(database_error)?;
    let account_id = only_account(&account_ids)?;
    let confirmations_policy = ConfirmationsPolicy::new_symmetrical(confirmation_count, false);
    let summary = wallet
        .get_wallet_summary(confirmations_policy)
        .map_err(database_error)?
        .ok_or(WalletServiceError::NotSynchronized)?;
    if !summary.is_synced() {
        return Err(WalletServiceError::NotSynchronized);
    }

    let prepared_payments = recipients
        .iter()
        .map(|recipient| {
            let address = decode_recipient(&recipient.address, network)?;
            let receiver = address
                .orchard()
                .copied()
                .ok_or(WalletServiceError::NonIronwoodProposal)?;
            let amount = Zatoshis::from_u64(recipient.amount_zat)
                .map_err(|error| WalletServiceError::InvalidRequest(error.to_string()))?;
            if amount == Zatoshis::ZERO {
                return Err(WalletServiceError::InvalidRequest(
                    "zero-valued transfer".to_owned(),
                ));
            }
            let memo = if recipient.memo.is_empty() {
                None
            } else {
                Some(
                    MemoBytes::from_bytes(&recipient.memo)
                        .map_err(|error| WalletServiceError::InvalidRequest(error.to_string()))?,
                )
            };
            let expected_memo = memo.clone().unwrap_or_else(MemoBytes::empty);
            let payment = Payment::new(
                address.to_zcash_address(network.address_network()),
                Some(amount),
                memo,
                None,
                None,
                vec![],
            )
            .map_err(|error| WalletServiceError::InvalidRequest(error.to_string()))?;
            Ok((
                payment,
                BoundIronwoodOutput {
                    receiver: receiver.to_raw_address_bytes(),
                    value_zat: amount.into_u64(),
                    memo: expected_memo,
                    role: TransferOutputRole::Payment,
                    pool: ShieldedPool::Ironwood,
                },
            ))
        })
        .collect::<Result<Vec<_>, WalletServiceError>>()?;
    let payments = prepared_payments
        .iter()
        .map(|(payment, _)| payment.clone())
        .collect();
    let expected_payment_outputs = prepared_payments
        .into_iter()
        .map(|(_, output)| output)
        .collect::<Vec<_>>();
    let request = TransactionRequest::new(payments)
        .map_err(|error| WalletServiceError::InvalidRequest(error.to_string()))?;
    let expected_request = request.clone();

    let fee_rule = StandardFeeRule::Zip317;
    let change_strategy = SingleOutputChangeStrategy::<WalletDatabase>::new(
        fee_rule,
        None,
        ShieldedPool::Ironwood,
        DustOutputPolicy::default(),
    );
    let selector = GreedyInputSelector::new();
    let spend_policy = SpendPolicy::shielded_pools([ShieldedPool::Ironwood]);
    let lock_owner = LockOwner::new(random_lock_owner());
    let parameters = network.parameters();
    let proposal =
        propose_transfer::<_, _, _, _, zcash_client_sqlite::wallet::commitment_tree::Error>(
            &mut wallet,
            &parameters,
            account_id,
            &selector,
            &change_strategy,
            request,
            confirmations_policy,
            &spend_policy,
            Some(LockRequest::new(lock_owner, lock_for_blocks)),
            Some(TxVersion::V6),
        )
        .map_err(|error| WalletServiceError::Proposal(format!("{error:?}")))?;

    if proposal.steps().len() != 1
        || proposal.steps().first().transaction_request() != &expected_request
        || proposal.steps().first().payment_pools().len() != recipients.len()
        || proposal.input_count_in_pool(PoolType::IRONWOOD) == 0
        || proposal.input_count_in_pool(PoolType::SAPLING) != 0
        || proposal.input_count_in_pool(PoolType::ORCHARD) != 0
        || proposal.input_count_in_pool(PoolType::Transparent) != 0
        || proposal
            .steps()
            .iter()
            .flat_map(|step| step.payment_pools().values())
            .any(|pool| *pool != PoolType::IRONWOOD)
        || proposal
            .steps()
            .iter()
            .flat_map(|step| step.balance().proposed_change())
            .any(|change| change.output_pool() != PoolType::IRONWOOD)
    {
        let _ = unlock_proposal_inputs(&mut wallet, &proposal, lock_owner);
        return Err(WalletServiceError::NonIronwoodProposal);
    }

    let target_height = BlockHeight::from(proposal.min_target_height());
    let target_height_u32: u32 = target_height.into();
    let anchor_height = match proposal.steps().first().anchor_height() {
        Some(height) => height,
        None => {
            let _ = unlock_proposal_inputs(&mut wallet, &proposal, lock_owner);
            return Err(WalletServiceError::NonIronwoodProposal);
        }
    };
    let current_heights = match wallet.get_target_and_anchor_heights(confirmation_count) {
        Ok(Some(heights)) => heights,
        Ok(None) => {
            let _ = unlock_proposal_inputs(&mut wallet, &proposal, lock_owner);
            return Err(WalletServiceError::StaleChain);
        }
        Err(error) => {
            let _ = unlock_proposal_inputs(&mut wallet, &proposal, lock_owner);
            return Err(database_error(error));
        }
    };
    if BlockHeight::from(current_heights.0) != target_height || current_heights.1 != anchor_height {
        let _ = unlock_proposal_inputs(&mut wallet, &proposal, lock_owner);
        return Err(WalletServiceError::StaleChain);
    }
    let expected_chain = match expected_chain_refs(&wallet, network, target_height, anchor_height) {
        Ok(expected) => expected,
        Err(error) => {
            let _ = unlock_proposal_inputs(&mut wallet, &proposal, lock_owner);
            return Err(error);
        }
    };
    if let Err(error) = revalidate_exact_chain_tip(client, expected_chain).await {
        let _ = unlock_proposal_inputs(&mut wallet, &proposal, lock_owner);
        return Err(error);
    }

    let usk = match derive_wallet_spending_key(master_seed, network, 0) {
        Ok(usk) => usk,
        Err(error) => {
            let _ = unlock_proposal_inputs(&mut wallet, &proposal, lock_owner);
            return Err(error.into());
        }
    };
    if let Err(error) = verify_seed_authority(&wallet, account_id, master_seed, network) {
        let _ = unlock_proposal_inputs(&mut wallet, &proposal, lock_owner);
        return Err(error);
    }
    if let Err(error) = verify_internal_change_receiver(&wallet, account_id, &usk) {
        let _ = unlock_proposal_inputs(&mut wallet, &proposal, lock_owner);
        return Err(error);
    }
    let stored_ufvk = match wallet.get_account(account_id).map_err(database_error) {
        Ok(Some(account)) => match account.ufvk() {
            Some(ufvk) => ufvk.clone(),
            None => {
                let _ = unlock_proposal_inputs(&mut wallet, &proposal, lock_owner);
                return Err(WalletServiceError::AuthorityMismatch);
            }
        },
        Ok(None) => {
            let _ = unlock_proposal_inputs(&mut wallet, &proposal, lock_owner);
            return Err(WalletServiceError::AuthorityMismatch);
        }
        Err(error) => {
            let _ = unlock_proposal_inputs(&mut wallet, &proposal, lock_owner);
            return Err(error);
        }
    };
    let internal_change_receiver = match stored_ufvk.orchard() {
        Some(fvk) => fvk.address_at(0u32, OrchardScope::Internal),
        None => {
            let _ = unlock_proposal_inputs(&mut wallet, &proposal, lock_owner);
            return Err(WalletServiceError::AuthorityMismatch);
        }
    };
    let orchard_fvk = stored_ufvk
        .orchard()
        .expect("the internal receiver check requires an Orchard viewing key");
    let expected_nullifiers = match proposal.steps().first().shielded_inputs().map(|inputs| {
        inputs
            .notes()
            .iter()
            .map(|received| match received.note() {
                WalletNote::Orchard {
                    note,
                    pool: orchard::ValuePool::Ironwood,
                } => Some(note.nullifier(orchard_fvk).to_bytes()),
                _ => None,
            })
            .collect::<Option<Vec<_>>>()
    }) {
        Some(Some(nullifiers)) if !nullifiers.is_empty() => nullifiers,
        _ => {
            let _ = unlock_proposal_inputs(&mut wallet, &proposal, lock_owner);
            return Err(WalletServiceError::NonIronwoodProposal);
        }
    };
    let known_wallet_nullifiers = match wallet.get_ironwood_nullifiers(NullifierQuery::All) {
        Ok(nullifiers) => nullifiers
            .into_iter()
            .filter_map(|(known_account, nullifier)| {
                (known_account == account_id).then_some(nullifier.to_bytes())
            })
            .collect::<Vec<_>>(),
        Err(error) => {
            let _ = unlock_proposal_inputs(&mut wallet, &proposal, lock_owner);
            return Err(database_error(error));
        }
    };
    let expected_ironwood_actions = match proposal.steps().first().ironwood_action_count(
        proposal.steps().first().ironwood_bundle_padding(),
        orchard::bundle::BundleVersion::ironwood_v3(),
    ) {
        Ok(count) => count,
        Err(error) => {
            let _ = unlock_proposal_inputs(&mut wallet, &proposal, lock_owner);
            return Err(WalletServiceError::Proposal(error.to_owned()));
        }
    };
    let expected_change_outputs = proposal
        .steps()
        .first()
        .balance()
        .proposed_change()
        .iter()
        .map(|change| BoundIronwoodOutput {
            receiver: internal_change_receiver.to_raw_address_bytes(),
            value_zat: change.value().into_u64(),
            memo: change.memo().cloned().unwrap_or_else(MemoBytes::empty),
            role: TransferOutputRole::InternalChange,
            pool: ShieldedPool::Ironwood,
        })
        .collect::<Vec<_>>();

    let expiry_height_u32 = match target_height_u32.checked_add(expiry_delta) {
        Some(height) if height <= u32::from(Height::MAX_EXPIRY_HEIGHT) => height,
        None => {
            let _ = unlock_proposal_inputs(&mut wallet, &proposal, lock_owner);
            return Err(WalletServiceError::HeightOverflow);
        }
        Some(_) => {
            let _ = unlock_proposal_inputs(&mut wallet, &proposal, lock_owner);
            return Err(WalletServiceError::InvalidRequest(format!(
                "transaction expiry height must not exceed {}",
                u32::from(Height::MAX_EXPIRY_HEIGHT)
            )));
        }
    };
    let prover = LocalTxProver::bundled();
    let created = match create_proposed_transactions::<_, _, Infallible, _, Infallible, _>(
        &mut wallet,
        &parameters,
        &prover,
        &prover,
        &SpendingKeys::from_unified_spending_key(usk),
        OvkPolicy::Sender,
        &proposal,
        Some(BlockHeight::from_u32(expiry_height_u32)),
    ) {
        Ok(created) => created,
        Err(error) => {
            let _ = unlock_proposal_inputs(&mut wallet, &proposal, lock_owner);
            return Err(WalletServiceError::Signing(format!("{error:?}")));
        }
    };
    if created.is_empty() {
        return Err(WalletServiceError::MissingSignedTransaction);
    }
    if created.len() != 1 {
        return Err(persisted_transactions_require_review(
            &created,
            "the signer returned an unexpected number of transactions",
        ));
    }
    let txid = created[0];
    let transaction = match wallet.get_transaction(txid) {
        Ok(Some(transaction)) => transaction,
        Ok(None) => {
            return Err(persisted_transactions_require_review(
                &created,
                "the signed transaction could not be read back from SQLite",
            ));
        }
        Err(error) => {
            return Err(persisted_transactions_require_review(
                &created,
                format!("SQLite readback failed: {error:?}"),
            ));
        }
    };
    let mut raw = Vec::new();
    if let Err(error) = transaction.write(&mut raw) {
        return Err(persisted_transactions_require_review(
            &created,
            format!("signed transaction serialization failed: {error}"),
        ));
    }
    let inspected = inspect_signed_transaction(&raw, network).map_err(|error| {
        persisted_transactions_require_review(
            &created,
            format!("signed transaction policy inspection failed: {error}"),
        )
    })?;
    let fee_zat = proposal.steps().first().balance().fee_required().into_u64();
    let actual_fee = inspected
        .fee_paid(|_| Ok::<_, zcash_protocol::value::BalanceError>(None))
        .ok()
        .flatten()
        .map(Zatoshis::into_u64);
    let serialized_outputs_match = signed_transfer_outputs_match(
        &inspected,
        &parameters,
        account_id,
        &stored_ufvk,
        target_height_u32,
        &expected_payment_outputs,
        &expected_change_outputs,
        &expected_nullifiers,
        &known_wallet_nullifiers,
        expected_ironwood_actions,
    );
    if inspected.txid() != txid
        || inspected.transparent_bundle().is_some()
        || u32::from(inspected.expiry_height()) != expiry_height_u32
        || actual_fee != Some(fee_zat)
        || !serialized_outputs_match
    {
        return Err(persisted_transactions_require_review(
            &created,
            "signed transfer identifier, expiry, inputs, fee, recipients, or change differs from the checked proposal",
        ));
    }
    drop(wallet);
    if let Err(error) =
        revalidate_canonical_ancestors(client, expected_chain, expiry_height_u32).await
    {
        return Err(persisted_transactions_require_review(
            &created,
            format!("final canonical-chain validation failed: {error}"),
        ));
    }
    Ok(SignedTransaction {
        txid: txid.to_string(),
        raw_transaction_hex: hex::encode(raw),
        branch_id: network.branch_id_hex(),
        target_height: target_height_u32,
        expiry_height: expiry_height_u32,
        fee_zat,
        internal_change_receiver_verified: true,
    })
}

/// Shields only mature, fully classified transparent coinbase outputs into the
/// wallet's own private Ironwood receiver.
///
/// This operation deliberately exposes no maturity override. The reviewed
/// `propose_shielding_coinbase` API selects coinbase outputs exclusively and
/// applies the 100-block consensus maturity rule. It creates one private
/// payment for the selected input value minus the ZIP 317 fee, with no
/// transparent or shielded change.
#[allow(clippy::too_many_arguments)]
pub async fn create_signed_coinbase_shielding(
    client: &mut AttestedWcashClient,
    path: impl AsRef<Path>,
    network: WalletNetwork,
    master_seed: &SecretVec<u8>,
    maximum_inputs: usize,
    expiry_delta: u32,
    lock_for_blocks: u32,
) -> Result<SignedTransaction, WalletServiceError> {
    ensure_client_network(client, network)?;
    if maximum_inputs == 0 || maximum_inputs > MAX_COINBASE_SHIELDING_INPUTS {
        return Err(WalletServiceError::InvalidRequest(format!(
            "coinbase input limit must be in 1..={MAX_COINBASE_SHIELDING_INPUTS}"
        )));
    }
    if expiry_delta == 0
        || expiry_delta > MAX_EXPIRY_DELTA
        || lock_for_blocks < expiry_delta
        || lock_for_blocks > MAX_LOCK_FOR_BLOCKS
    {
        return Err(WalletServiceError::InvalidRequest(format!(
            "expiry must be in 1..={MAX_EXPIRY_DELTA}, and lock lifetime must cover expiry without exceeding {MAX_LOCK_FOR_BLOCKS}"
        )));
    }

    let path = path.as_ref();
    let _operation_lock = acquire_wallet_operation_lock(path, WalletOperationLockMode::Exclusive)?;
    let mut wallet = open_wallet_database_with_seed(path, network, master_seed)?;
    require_transparent_recovery_complete(&mut wallet)?;
    ensure_no_active_pending_transactions(&mut wallet, network)?;
    ensure_no_legacy_pool_balances(&wallet)?;
    let account_ids = wallet.get_account_ids().map_err(database_error)?;
    let account_id = only_account(&account_ids)?;
    let summary = wallet
        .get_wallet_summary(public_confirmation_policy())
        .map_err(database_error)?
        .ok_or(WalletServiceError::NotSynchronized)?;
    if !summary.is_synced() {
        return Err(WalletServiceError::NotSynchronized);
    }
    let coinbase_balance = summary
        .account_balances()
        .get(&account_id)
        .ok_or(WalletServiceError::AuthorityMismatch)?
        .unshielded_coinbase_balance();
    if coinbase_balance.spendable_value() == Zatoshis::ZERO {
        return Err(WalletServiceError::NoMatureCoinbase);
    }

    let account = wallet
        .get_account(account_id)
        .map_err(database_error)?
        .ok_or(WalletServiceError::AuthorityMismatch)?;
    let stored_ufvk = account
        .ufvk()
        .ok_or(WalletServiceError::AuthorityMismatch)?
        .clone();
    let source = default_transparent_receiver(&stored_ufvk)?;
    let private_destination =
        decode_recipient(&encode_orchard_receiver(&stored_ufvk, network)?, network)?;
    let destination_receiver = *private_destination
        .orchard()
        .ok_or(WalletServiceError::AuthorityMismatch)?;
    let destination = private_destination.to_zcash_address(network.address_network());

    let usk = derive_wallet_spending_key(master_seed, network, 0)?;
    verify_seed_authority(&wallet, account_id, master_seed, network)?;
    verify_internal_change_receiver(&wallet, account_id, &usk)?;
    if default_transparent_receiver(&usk.to_unified_full_viewing_key())? != source {
        return Err(WalletServiceError::AuthorityMismatch);
    }

    let selector = GreedyInputSelector::new();
    let fee_rule = StandardFeeRule::Zip317;
    let lock_owner = LockOwner::new(random_lock_owner());
    let parameters = network.parameters();
    let proposal = propose_shielding_coinbase::<
        _,
        _,
        _,
        _,
        zcash_client_sqlite::wallet::commitment_tree::Error,
    >(
        &mut wallet,
        &parameters,
        &selector,
        &fee_rule,
        Zatoshis::ZERO,
        &[source],
        destination.clone(),
        None,
        Some(maximum_inputs),
        Some(LockRequest::new(lock_owner, lock_for_blocks)),
    )
    .map_err(|error| WalletServiceError::Proposal(format!("{error:?}")))?
    .with_proposed_version(Some(TxVersion::V6));

    let target_height = BlockHeight::from(proposal.min_target_height());
    let target_height_u32: u32 = target_height.into();
    let step = proposal.steps().first();
    let payment = step.transaction_request().payments().values().next();
    let input_total = step
        .transparent_inputs()
        .iter()
        .try_fold(0u64, |total, input| {
            total.checked_add(input.value().into_u64())
        });
    let payment_total = payment
        .and_then(|payment| payment.amount())
        .map(Zatoshis::into_u64);
    let fee_zat = step.balance().fee_required().into_u64();
    let values_balance = input_total
        .and_then(|total| {
            payment_total
                .and_then(|payment| payment.checked_add(fee_zat).map(|spent| (total, spent)))
        })
        .is_some_and(|(total, spent)| total == spent);
    let inputs_are_mature = step.transparent_inputs().iter().all(|input| {
        input
            .mined_height()
            .is_some_and(|height| coinbase_is_mature(u32::from(height), target_height_u32))
    });
    let valid_proposal = proposal.steps().len() == 1
        && proposal.proposed_version() == Some(TxVersion::V6)
        // This reviewed API intentionally uses the explicit-payment proposal
        // shape (`is_shielding == false`), rather than legacy all-in-change
        // shielding. The transparent-only input and Ironwood-only payment
        // invariants below define the operation.
        && !step.is_shielding()
        && !step.transparent_inputs().is_empty()
        && step.transparent_inputs().len() <= maximum_inputs
        && step
            .transparent_inputs()
            .iter()
            .all(|input| input.recipient_address() == &source)
        && inputs_are_mature
        && proposal.input_count_in_pool(PoolType::Transparent) == step.transparent_inputs().len()
        && proposal.input_count_in_pool(PoolType::SAPLING) == 0
        && proposal.input_count_in_pool(PoolType::ORCHARD) == 0
        && proposal.input_count_in_pool(PoolType::IRONWOOD) == 0
        && step.transaction_request().payments().len() == 1
        && payment.is_some_and(|payment| payment.recipient_address() == &destination)
        && step.payment_pools().len() == 1
        && step
            .payment_pools()
            .values()
            .all(|pool| *pool == PoolType::IRONWOOD)
        && step.balance().proposed_change().is_empty()
        && step.prior_step_inputs().is_empty()
        && values_balance;
    if !valid_proposal {
        let _ = unlock_proposal_inputs(&mut wallet, &proposal, lock_owner);
        return Err(WalletServiceError::InvalidCoinbaseShieldingProposal);
    }

    let known_wallet_nullifiers = match wallet.get_ironwood_nullifiers(NullifierQuery::All) {
        Ok(nullifiers) => nullifiers
            .into_iter()
            .filter_map(|(known_account, nullifier)| {
                (known_account == account_id).then_some(nullifier.to_bytes())
            })
            .collect::<Vec<_>>(),
        Err(error) => {
            let _ = unlock_proposal_inputs(&mut wallet, &proposal, lock_owner);
            return Err(database_error(error));
        }
    };
    let expected_ironwood_actions = match step.ironwood_action_count(
        step.ironwood_bundle_padding(),
        orchard::bundle::BundleVersion::ironwood_v3(),
    ) {
        Ok(count) => count,
        Err(error) => {
            let _ = unlock_proposal_inputs(&mut wallet, &proposal, lock_owner);
            return Err(WalletServiceError::Proposal(error.to_owned()));
        }
    };

    let expected_outpoints = step
        .transparent_inputs()
        .iter()
        .map(|input| input.outpoint().clone())
        .collect::<Vec<_>>();
    let anchor_height = match step.anchor_height() {
        Some(height) => height,
        None => {
            let _ = unlock_proposal_inputs(&mut wallet, &proposal, lock_owner);
            return Err(WalletServiceError::InvalidCoinbaseShieldingProposal);
        }
    };
    let current_heights =
        match wallet.get_target_and_anchor_heights(proposal.confirmations_policy().trusted()) {
            Ok(Some(heights)) => heights,
            Ok(None) => {
                let _ = unlock_proposal_inputs(&mut wallet, &proposal, lock_owner);
                return Err(WalletServiceError::StaleChain);
            }
            Err(error) => {
                let _ = unlock_proposal_inputs(&mut wallet, &proposal, lock_owner);
                return Err(database_error(error));
            }
        };
    if BlockHeight::from(current_heights.0) != target_height || current_heights.1 != anchor_height {
        let _ = unlock_proposal_inputs(&mut wallet, &proposal, lock_owner);
        return Err(WalletServiceError::StaleChain);
    }
    let expected_chain = match expected_chain_refs(&wallet, network, target_height, anchor_height) {
        Ok(expected) => expected,
        Err(error) => {
            let _ = unlock_proposal_inputs(&mut wallet, &proposal, lock_owner);
            return Err(error);
        }
    };
    if let Err(error) = revalidate_exact_chain_tip(client, expected_chain).await {
        let _ = unlock_proposal_inputs(&mut wallet, &proposal, lock_owner);
        return Err(error);
    }

    let expiry_height_u32 = match target_height_u32.checked_add(expiry_delta) {
        Some(height) if height <= u32::from(Height::MAX_EXPIRY_HEIGHT) => height,
        None => {
            let _ = unlock_proposal_inputs(&mut wallet, &proposal, lock_owner);
            return Err(WalletServiceError::HeightOverflow);
        }
        Some(_) => {
            let _ = unlock_proposal_inputs(&mut wallet, &proposal, lock_owner);
            return Err(WalletServiceError::InvalidRequest(format!(
                "transaction expiry height must not exceed {}",
                u32::from(Height::MAX_EXPIRY_HEIGHT)
            )));
        }
    };
    let prover = LocalTxProver::bundled();
    let created = match create_proposed_transactions::<_, _, Infallible, _, Infallible, _>(
        &mut wallet,
        &parameters,
        &prover,
        &prover,
        &SpendingKeys::from_unified_spending_key(usk),
        OvkPolicy::Sender,
        &proposal,
        Some(BlockHeight::from_u32(expiry_height_u32)),
    ) {
        Ok(created) => created,
        Err(error) => {
            let _ = unlock_proposal_inputs(&mut wallet, &proposal, lock_owner);
            return Err(WalletServiceError::Signing(format!("{error:?}")));
        }
    };
    if created.is_empty() {
        return Err(WalletServiceError::MissingSignedTransaction);
    }
    if created.len() != 1 {
        return Err(persisted_transactions_require_review(
            &created,
            "the signer returned an unexpected number of transactions",
        ));
    }
    let txid = created[0];
    let transaction = match wallet.get_transaction(txid) {
        Ok(Some(transaction)) => transaction,
        Ok(None) => {
            return Err(persisted_transactions_require_review(
                &created,
                "the signed transaction could not be read back from SQLite",
            ));
        }
        Err(error) => {
            return Err(persisted_transactions_require_review(
                &created,
                format!("SQLite readback failed: {error:?}"),
            ));
        }
    };
    let mut raw = Vec::new();
    if let Err(error) = transaction.write(&mut raw) {
        return Err(persisted_transactions_require_review(
            &created,
            format!("signed transaction serialization failed: {error}"),
        ));
    }
    let inspected = inspect_signed_transaction(&raw, network).map_err(|error| {
        persisted_transactions_require_review(
            &created,
            format!("signed transaction policy inspection failed: {error}"),
        )
    })?;
    let exact_inputs = inspected.transparent_bundle().is_some_and(|bundle| {
        let actual_outpoints = bundle
            .vin
            .iter()
            .map(|input| input.prevout().clone())
            .collect::<Vec<_>>();
        bundle.vout.is_empty() && unique_outpoint_sets_match(&expected_outpoints, &actual_outpoints)
    });
    let serialized_policy_matches =
        input_total
            .zip(payment_total)
            .is_some_and(|(input_total, payment_total)| {
                signed_coinbase_shielding_matches(
                    &inspected,
                    &parameters,
                    account_id,
                    &stored_ufvk,
                    destination_receiver,
                    target_height_u32,
                    expiry_height_u32,
                    input_total,
                    payment_total,
                    fee_zat,
                    &known_wallet_nullifiers,
                    expected_ironwood_actions,
                )
            });
    if inspected.txid() != txid || !exact_inputs || !serialized_policy_matches {
        return Err(persisted_transactions_require_review(
            &created,
            "signed transaction inputs, expiry, value, or private destination differ from the checked proposal",
        ));
    }
    drop(wallet);
    if let Err(error) =
        revalidate_canonical_ancestors(client, expected_chain, expiry_height_u32).await
    {
        return Err(persisted_transactions_require_review(
            &created,
            format!("final canonical-chain validation failed: {error}"),
        ));
    }
    Ok(SignedTransaction {
        txid: txid.to_string(),
        raw_transaction_hex: hex::encode(raw),
        branch_id: network.branch_id_hex(),
        target_height: target_height_u32,
        expiry_height: expiry_height_u32,
        fee_zat,
        internal_change_receiver_verified: true,
    })
}

fn random_lock_owner() -> [u8; 32] {
    let mut rng = OsRng;
    loop {
        let mut owner = [0; 32];
        rng.fill_bytes(&mut owner);
        if owner != [0; 32] {
            return owner;
        }
    }
}

fn persisted_transactions_require_review<'a>(
    txids: impl IntoIterator<Item = &'a zcash_protocol::TxId>,
    reason: impl Into<String>,
) -> WalletServiceError {
    WalletServiceError::PersistedTransactionsRequireReview {
        txids: txids.into_iter().map(ToString::to_string).collect(),
        reason: reason.into(),
    }
}

#[allow(clippy::too_many_arguments)]
fn signed_transfer_outputs_match(
    transaction: &Transaction,
    parameters: &Network,
    account_id: AccountUuid,
    stored_ufvk: &zcash_keys::keys::UnifiedFullViewingKey,
    target_height: u32,
    expected_payments: &[BoundIronwoodOutput],
    expected_change: &[BoundIronwoodOutput],
    expected_nullifiers: &[[u8; 32]],
    known_wallet_nullifiers: &[[u8; 32]],
    expected_action_count: usize,
) -> bool {
    let Some(bundle) = transaction.ironwood_bundle() else {
        return false;
    };
    let actual_nullifiers = bundle
        .actions()
        .iter()
        .map(|action| action.nullifier().to_bytes())
        .collect::<Vec<_>>();
    if !ironwood_input_shape_matches(
        expected_nullifiers,
        known_wallet_nullifiers,
        &actual_nullifiers,
        expected_action_count,
    ) {
        return false;
    }

    let ufvks = HashMap::from([(account_id, stored_ufvk.clone())]);
    let decrypted = decrypt_transaction(
        parameters,
        None,
        target_height.checked_sub(1).map(BlockHeight::from_u32),
        transaction,
        &ufvks,
    );
    if !decrypted.sapling_outputs().is_empty() || !decrypted.orchard_outputs().is_empty() {
        return false;
    }

    let mut actual = Vec::with_capacity(decrypted.ironwood_outputs().len());
    for output in decrypted.ironwood_outputs() {
        if output.account() != &account_id
            || output.value_pool() != ShieldedPool::Ironwood
            || output.note().1 != orchard::ValuePool::Ironwood
        {
            return false;
        }
        let role = match output.transfer_type() {
            TransferType::Incoming | TransferType::Outgoing => TransferOutputRole::Payment,
            TransferType::AccountInternal => TransferOutputRole::InternalChange,
            TransferType::WalletInternal => return false,
        };
        actual.push(BoundIronwoodOutput {
            receiver: output.note().0.recipient().to_raw_address_bytes(),
            value_zat: output.note().0.value().inner(),
            memo: output.memo().clone(),
            role,
            pool: output.value_pool(),
        });
    }

    let mut expected = Vec::with_capacity(expected_payments.len() + expected_change.len());
    expected.extend_from_slice(expected_payments);
    expected.extend_from_slice(expected_change);
    exact_bound_output_multisets_match(&expected, &actual)
}

fn ironwood_input_shape_matches(
    expected_real_nullifiers: &[[u8; 32]],
    known_wallet_nullifiers: &[[u8; 32]],
    actual_action_nullifiers: &[[u8; 32]],
    expected_action_count: usize,
) -> bool {
    let unique_count = |values: &[[u8; 32]]| {
        let mut values = values.to_vec();
        values.sort_unstable();
        values.dedup();
        values.len()
    };
    let mut expected_wallet_intersection = expected_real_nullifiers.to_vec();
    expected_wallet_intersection.sort_unstable();
    let mut actual_wallet_intersection = actual_action_nullifiers
        .iter()
        .filter(|nullifier| known_wallet_nullifiers.contains(nullifier))
        .copied()
        .collect::<Vec<_>>();
    actual_wallet_intersection.sort_unstable();

    actual_action_nullifiers.len() == expected_action_count
        && unique_count(expected_real_nullifiers) == expected_real_nullifiers.len()
        && unique_count(known_wallet_nullifiers) == known_wallet_nullifiers.len()
        && unique_count(actual_action_nullifiers) == actual_action_nullifiers.len()
        && actual_wallet_intersection == expected_wallet_intersection
}

fn exact_bound_output_multisets_match(
    expected: &[BoundIronwoodOutput],
    actual: &[BoundIronwoodOutput],
) -> bool {
    if expected.len() != actual.len() {
        return false;
    }
    let mut unmatched = expected.to_vec();
    for output in actual {
        let Some(index) = unmatched.iter().position(|expected| expected == output) else {
            return false;
        };
        unmatched.swap_remove(index);
    }
    unmatched.is_empty()
}

#[allow(clippy::too_many_arguments)]
fn signed_coinbase_shielding_matches(
    transaction: &Transaction,
    parameters: &Network,
    account_id: AccountUuid,
    stored_ufvk: &zcash_keys::keys::UnifiedFullViewingKey,
    destination_receiver: orchard::Address,
    target_height: u32,
    expiry_height: u32,
    input_total_zat: u64,
    payment_total_zat: u64,
    fee_zat: u64,
    known_wallet_nullifiers: &[[u8; 32]],
    expected_action_count: usize,
) -> bool {
    let Some(bundle) = transaction.ironwood_bundle() else {
        return false;
    };
    let actual_nullifiers = bundle
        .actions()
        .iter()
        .map(|action| action.nullifier().to_bytes())
        .collect::<Vec<_>>();
    if !ironwood_input_shape_matches(
        &[],
        known_wallet_nullifiers,
        &actual_nullifiers,
        expected_action_count,
    ) {
        return false;
    }

    let expected_payment = input_total_zat.checked_sub(fee_zat);
    let expected_balance = i64::try_from(payment_total_zat)
        .ok()
        .and_then(i64::checked_neg);
    let actual_balance = Some(i64::from_le_bytes(bundle.value_balance().to_i64_le_bytes()));
    if !shielding_amounts_match(
        u32::from(transaction.expiry_height()),
        actual_balance,
        expiry_height,
        input_total_zat,
        payment_total_zat,
        fee_zat,
    ) || expected_payment != Some(payment_total_zat)
        || actual_balance != expected_balance
    {
        return false;
    }

    let ufvks = HashMap::from([(account_id, stored_ufvk.clone())]);
    let decrypted = decrypt_transaction(
        parameters,
        None,
        target_height.checked_sub(1).map(BlockHeight::from_u32),
        transaction,
        &ufvks,
    );
    decrypted.sapling_outputs().is_empty()
        && decrypted.orchard_outputs().is_empty()
        && matches!(
            decrypted.ironwood_outputs(),
            [output]
                if output.account() == &account_id
                    && output.value_pool() == ShieldedPool::Ironwood
                    && output.note().1 == orchard::ValuePool::Ironwood
                    && output.note().0.recipient() == destination_receiver
                    && output.note().0.value().inner() == payment_total_zat
                    && output.transfer_type() == TransferType::Incoming
        )
}

fn shielding_amounts_match(
    actual_expiry_height: u32,
    actual_ironwood_balance: Option<i64>,
    expected_expiry_height: u32,
    input_total_zat: u64,
    payment_total_zat: u64,
    fee_zat: u64,
) -> bool {
    let expected_balance = i64::try_from(payment_total_zat)
        .ok()
        .and_then(i64::checked_neg);
    actual_expiry_height == expected_expiry_height
        && input_total_zat.checked_sub(fee_zat) == Some(payment_total_zat)
        && actual_ironwood_balance == expected_balance
}

fn coinbase_is_mature(mined_height: u32, spend_height: u32) -> bool {
    spend_height
        .checked_sub(mined_height)
        .is_some_and(|confirmations| confirmations >= COINBASE_SHIELDING_MATURITY)
}

fn unique_outpoint_sets_match(expected: &[OutPoint], actual: &[OutPoint]) -> bool {
    if expected.len() != actual.len() {
        return false;
    }
    let mut expected = expected.to_vec();
    let mut actual = actual.to_vec();
    expected.sort_unstable();
    actual.sort_unstable();
    let has_duplicate =
        |outpoints: &[OutPoint]| outpoints.windows(2).any(|pair| pair[0] == pair[1]);
    !has_duplicate(&expected) && !has_duplicate(&actual) && expected == actual
}

fn validate_confirmation_policy(
    network: WalletNetwork,
    confirmations: u32,
    allow_unsafe_regtest_confirmations: bool,
) -> Result<NonZeroU32, WalletServiceError> {
    let confirmations =
        NonZeroU32::new(confirmations).ok_or(WalletServiceError::UnsafeConfirmations)?;
    if confirmations.get() < COINBASE_SHIELDING_MATURITY
        && !(network == WalletNetwork::Regtest && allow_unsafe_regtest_confirmations)
    {
        return Err(WalletServiceError::UnsafeConfirmations);
    }
    Ok(confirmations)
}

fn public_confirmation_policy() -> ConfirmationsPolicy {
    ConfirmationsPolicy::new_symmetrical(
        NonZeroU32::new(COINBASE_SHIELDING_MATURITY)
            .expect("the consensus coinbase maturity is nonzero"),
        false,
    )
}

fn only_account(account_ids: &[AccountUuid]) -> Result<AccountUuid, WalletServiceError> {
    if let [account_id] = account_ids {
        Ok(*account_id)
    } else {
        Err(WalletServiceError::UnexpectedAccountCount(
            account_ids.len(),
        ))
    }
}

fn verify_seed_authority(
    wallet: &WalletDatabase,
    account_id: AccountUuid,
    master_seed: &SecretVec<u8>,
    network: WalletNetwork,
) -> Result<(), WalletServiceError> {
    let derived_seed = derive_wallet_seed(master_seed, network)?;
    if wallet
        .validate_seed(account_id, &derived_seed)
        .map_err(database_error)?
    {
        Ok(())
    } else {
        Err(WalletServiceError::WrongSeed)
    }
}

fn verify_internal_change_receiver(
    wallet: &WalletDatabase,
    account_id: AccountUuid,
    usk: &zcash_keys::keys::UnifiedSpendingKey,
) -> Result<(), WalletServiceError> {
    let account = wallet
        .get_account(account_id)
        .map_err(database_error)?
        .ok_or(WalletServiceError::AuthorityMismatch)?;
    let stored_ufvk = account
        .ufvk()
        .ok_or(WalletServiceError::AuthorityMismatch)?;
    let derived_ufvk = usk.to_unified_full_viewing_key();
    let stored_orchard = stored_ufvk
        .orchard()
        .ok_or(WalletServiceError::AuthorityMismatch)?;
    let derived_orchard = derived_ufvk
        .orchard()
        .ok_or(WalletServiceError::AuthorityMismatch)?;
    let stored_change = stored_orchard.address_at(0u32, OrchardScope::Internal);
    let derived_change = derived_orchard.address_at(0u32, OrchardScope::Internal);
    if stored_orchard != derived_orchard
        || stored_change != derived_change
        || stored_change.diversifier() != derived_change.diversifier()
        || stored_change.to_raw_address_bytes() != derived_change.to_raw_address_bytes()
    {
        return Err(WalletServiceError::AuthorityMismatch);
    }
    Ok(())
}

fn expected_chain_refs(
    wallet: &WalletDatabase,
    network: WalletNetwork,
    target_height: BlockHeight,
    anchor_height: BlockHeight,
) -> Result<(BlockRef, BlockRef), WalletServiceError> {
    let target_height_u32: u32 = target_height.into();
    let expected_tip = target_height_u32
        .checked_sub(1)
        .map(BlockHeight::from_u32)
        .ok_or(WalletServiceError::HeightOverflow)?;
    let expected_tip_u32: u32 = expected_tip.into();
    let anchor_height_u32: u32 = anchor_height.into();
    Ok((
        expected_chain_ref(wallet, network, expected_tip_u32)?,
        expected_chain_ref(wallet, network, anchor_height_u32)?,
    ))
}

fn expected_chain_ref(
    wallet: &WalletDatabase,
    network: WalletNetwork,
    height: u32,
) -> Result<BlockRef, WalletServiceError> {
    if height == 0 {
        return Ok(BlockRef {
            height,
            hash: network.genesis_hash(),
        });
    }

    let stored_hash = wallet
        .get_block_hash(BlockHeight::from_u32(height))
        .map_err(database_error)?
        .ok_or(WalletServiceError::StaleChain)?;
    Ok(BlockRef {
        height,
        hash: stored_hash.0,
    })
}

async fn revalidate_exact_chain_tip(
    client: &mut AttestedWcashClient,
    expected: (BlockRef, BlockRef),
) -> Result<(), WalletServiceError> {
    let (expected_tip, expected_anchor) = expected;
    let live_tip = client.latest_block().await?;
    if live_tip != expected_tip {
        return Err(WalletServiceError::StaleChain);
    }

    let live_anchor = client.compact_block_ref(expected_anchor.height).await?;
    if live_anchor != expected_anchor {
        return Err(WalletServiceError::StaleChain);
    }
    Ok(())
}

async fn revalidate_canonical_ancestors(
    client: &mut AttestedWcashClient,
    expected: (BlockRef, BlockRef),
    expiry_height: u32,
) -> Result<(), WalletServiceError> {
    let (expected_tip, expected_anchor) = expected;
    let live_tip = client.latest_block().await?;
    if live_tip.height < expected_tip.height || live_tip.height >= expiry_height {
        return Err(WalletServiceError::StaleChain);
    }
    let live_expected_tip = if live_tip.height == expected_tip.height {
        live_tip
    } else {
        client.compact_block_ref(expected_tip.height).await?
    };
    let live_anchor = client.compact_block_ref(expected_anchor.height).await?;
    if !post_proof_chain_is_acceptable(
        live_tip,
        live_expected_tip,
        expected_tip,
        live_anchor,
        expected_anchor,
        expiry_height,
    ) {
        return Err(WalletServiceError::StaleChain);
    }
    Ok(())
}

fn post_proof_chain_is_acceptable(
    live_tip: BlockRef,
    live_expected_tip: BlockRef,
    expected_tip: BlockRef,
    live_anchor: BlockRef,
    expected_anchor: BlockRef,
    expiry_height: u32,
) -> bool {
    live_tip.height >= expected_tip.height
        && live_tip.height < expiry_height
        && live_expected_tip == expected_tip
        && live_anchor == expected_anchor
}

fn ensure_client_network(
    client: &AttestedWcashClient,
    network: WalletNetwork,
) -> Result<(), WalletServiceError> {
    if client.network() == network {
        Ok(())
    } else {
        Err(WalletServiceError::ClientNetworkMismatch)
    }
}

fn database_error(error: impl std::fmt::Debug) -> WalletServiceError {
    WalletServiceError::Database(format!("{error:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use zcash_client_backend::data_api::chain::ChainState;
    use zcash_primitives::block::BlockHash;

    fn create_wallet_accounts(
        path: &Path,
        network: WalletNetwork,
        account_count: usize,
    ) -> Vec<WalletInfo> {
        let master_seed = SecretVec::new(vec![0x35; 32]);
        let mut wallet = open_wallet_database_with_seed(path, network, &master_seed).unwrap();
        let account_seed = derive_wallet_seed(&master_seed, network).unwrap();
        let birthday = AccountBirthday::from_parts(
            ChainState::empty(BlockHeight::from_u32(0), BlockHash(network.genesis_hash())),
            None,
        );
        let mut accounts = Vec::with_capacity(account_count);
        for index in 0..account_count {
            let (account_id, usk) = wallet
                .create_account(
                    &format!("Wcash inspection test account {index}"),
                    &account_seed,
                    &birthday,
                    Some("inspection-test-seed"),
                )
                .unwrap();
            let ufvk = usk.to_unified_full_viewing_key();
            accounts.push(WalletInfo {
                account_id: account_id.expose_uuid().to_string(),
                birthday_height: 1,
                address: encode_orchard_receiver(&ufvk, network).unwrap(),
                transparent_coinbase_address: encode_transparent_coinbase_receiver(&ufvk, network)
                    .unwrap(),
            });
        }
        accounts
    }

    fn block_ref(height: u32, byte: u8) -> BlockRef {
        BlockRef {
            height,
            hash: [byte; 32],
        }
    }

    #[test]
    fn seedless_inspection_returns_public_metadata_without_mutating_the_database() {
        let directory = tempfile::tempdir().unwrap();
        let wallet_path = directory.path().join("wallet.sqlite");
        let expected = create_wallet_accounts(&wallet_path, WalletNetwork::Testnet, 1)
            .pop()
            .unwrap();
        let database_before = fs::read(&wallet_path).unwrap();

        let actual = inspect_wallet(&wallet_path, WalletNetwork::Testnet).unwrap();

        assert_eq!(actual, expected);
        assert_eq!(fs::read(&wallet_path).unwrap(), database_before);
        assert!(actual.address.starts_with("wutest1"));
        assert!(actual.transparent_coinbase_address.starts_with("WT"));
    }

    #[test]
    fn seedless_inspection_does_not_create_a_missing_database() {
        let directory = tempfile::tempdir().unwrap();
        let wallet_path = directory.path().join("missing.sqlite");
        let lock_path = wallet_operation_lock_path(&wallet_path).unwrap();

        assert!(matches!(
            inspect_wallet(&wallet_path, WalletNetwork::Testnet),
            Err(WalletServiceError::WalletDatabaseMissing)
        ));
        assert!(!wallet_path.exists());
        assert!(!lock_path.exists());
    }

    #[test]
    fn seedless_inspection_distinguishes_empty_wallet_files_and_accounts() {
        let directory = tempfile::tempdir().unwrap();
        let empty_path = directory.path().join("empty.sqlite");
        drop(File::create(&empty_path).unwrap());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            fs::set_permissions(&empty_path, fs::Permissions::from_mode(0o600)).unwrap();
        }
        let empty_before = fs::read(&empty_path).unwrap();
        assert!(matches!(
            inspect_wallet(&empty_path, WalletNetwork::Testnet),
            Err(WalletServiceError::WalletDatabaseEmpty)
        ));
        assert_eq!(fs::read(&empty_path).unwrap(), empty_before);

        let no_account_path = directory.path().join("no-account.sqlite");
        drop(open_wallet_database(&no_account_path, WalletNetwork::Testnet).unwrap());
        let no_account_before = fs::read(&no_account_path).unwrap();
        assert!(matches!(
            inspect_wallet(&no_account_path, WalletNetwork::Testnet),
            Err(WalletServiceError::WalletDatabaseEmpty)
        ));
        assert_eq!(fs::read(&no_account_path).unwrap(), no_account_before);
    }

    #[test]
    fn seedless_inspection_rejects_foreign_databases_without_modification() {
        let directory = tempfile::tempdir().unwrap();
        let foreign_path = directory.path().join("foreign.sqlite");
        let connection = rusqlite::Connection::open(&foreign_path).unwrap();
        connection
            .execute_batch("CREATE TABLE unrelated_public_data (value INTEGER NOT NULL);")
            .unwrap();
        drop(connection);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            fs::set_permissions(&foreign_path, fs::Permissions::from_mode(0o600)).unwrap();
        }
        let foreign_before = fs::read(&foreign_path).unwrap();

        assert!(matches!(
            inspect_wallet(&foreign_path, WalletNetwork::Testnet),
            Err(WalletServiceError::ForeignWalletDatabase)
        ));
        assert_eq!(fs::read(&foreign_path).unwrap(), foreign_before);

        let other_network_path = directory.path().join("other-network.sqlite");
        create_wallet_accounts(&other_network_path, WalletNetwork::Testnet, 1);
        assert!(matches!(
            inspect_wallet(&other_network_path, WalletNetwork::Regtest),
            Err(WalletServiceError::ForeignWalletDatabase)
        ));
    }

    #[test]
    fn seedless_inspection_rejects_corrupt_and_malformed_databases_generically() {
        let directory = tempfile::tempdir().unwrap();
        let corrupt_path = directory.path().join("corrupt.sqlite");
        fs::write(&corrupt_path, b"not a SQLite database").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            fs::set_permissions(&corrupt_path, fs::Permissions::from_mode(0o600)).unwrap();
        }
        let corrupt_error = inspect_wallet(&corrupt_path, WalletNetwork::Testnet).unwrap_err();
        assert!(matches!(
            corrupt_error,
            WalletServiceError::CorruptWalletDatabase
        ));
        assert_eq!(
            corrupt_error.to_string(),
            "wallet database is corrupt or incompatible"
        );

        let malformed_path = directory.path().join("malformed.sqlite");
        create_wallet_accounts(&malformed_path, WalletNetwork::Testnet, 1);
        let connection = rusqlite::Connection::open(&malformed_path).unwrap();
        connection
            .execute("DELETE FROM ext_wcash_wallet_identity", [])
            .unwrap();
        drop(connection);
        assert!(matches!(
            inspect_wallet(&malformed_path, WalletNetwork::Testnet),
            Err(WalletServiceError::CorruptWalletDatabase)
        ));
    }

    #[test]
    fn seedless_inspection_requires_exactly_one_account_and_a_shared_lock() {
        let directory = tempfile::tempdir().unwrap();
        let wallet_path = directory.path().join("wallet.sqlite");
        create_wallet_accounts(&wallet_path, WalletNetwork::Testnet, 2);
        assert!(matches!(
            inspect_wallet(&wallet_path, WalletNetwork::Testnet),
            Err(WalletServiceError::UnexpectedAccountCount(2))
        ));

        let one_account_path = directory.path().join("one-account.sqlite");
        create_wallet_accounts(&one_account_path, WalletNetwork::Testnet, 1);
        let exclusive =
            acquire_wallet_operation_lock(&one_account_path, WalletOperationLockMode::Exclusive)
                .unwrap();
        assert!(matches!(
            inspect_wallet(&one_account_path, WalletNetwork::Testnet),
            Err(WalletServiceError::WalletBusy)
        ));
        drop(exclusive);
        assert!(inspect_wallet(&one_account_path, WalletNetwork::Testnet).is_ok());
    }

    #[cfg(unix)]
    #[test]
    fn seedless_inspection_accepts_a_read_only_wallet_file() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let wallet_path = directory.path().join("wallet.sqlite");
        create_wallet_accounts(&wallet_path, WalletNetwork::Testnet, 1);
        fs::set_permissions(&wallet_path, fs::Permissions::from_mode(0o400)).unwrap();

        assert!(inspect_wallet(&wallet_path, WalletNetwork::Testnet).is_ok());
        assert_eq!(
            fs::metadata(&wallet_path).unwrap().permissions().mode() & 0o777,
            0o400
        );
    }

    #[test]
    fn confirmation_floor_is_public_by_default_and_regtest_override_is_explicit() {
        const LOCAL_CONFIRMATIONS: u32 = 1;
        const ZERO_CONFIRMATIONS: u32 = 0;
        let below_public_confirmations = COINBASE_SHIELDING_MATURITY - LOCAL_CONFIRMATIONS;

        assert!(validate_confirmation_policy(
            WalletNetwork::Testnet,
            COINBASE_SHIELDING_MATURITY,
            false
        )
        .is_ok());
        assert!(matches!(
            validate_confirmation_policy(WalletNetwork::Testnet, below_public_confirmations, true),
            Err(WalletServiceError::UnsafeConfirmations)
        ));
        assert!(matches!(
            validate_confirmation_policy(WalletNetwork::Regtest, LOCAL_CONFIRMATIONS, false),
            Err(WalletServiceError::UnsafeConfirmations)
        ));
        assert!(matches!(
            validate_confirmation_policy(WalletNetwork::Regtest, ZERO_CONFIRMATIONS, true),
            Err(WalletServiceError::UnsafeConfirmations)
        ));
        assert_eq!(
            validate_confirmation_policy(WalletNetwork::Regtest, LOCAL_CONFIRMATIONS, true)
                .unwrap()
                .get(),
            LOCAL_CONFIRMATIONS
        );
    }

    #[test]
    fn balance_policy_is_validated_before_wallet_access() {
        const LOCAL_CONFIRMATIONS: u32 = 1;
        let directory = tempfile::tempdir().unwrap();
        let missing_wallet = directory.path().join("missing.sqlite");

        assert!(matches!(
            wallet_balance_with_confirmations(
                &missing_wallet,
                WalletNetwork::Testnet,
                LOCAL_CONFIRMATIONS,
                true,
            ),
            Err(WalletServiceError::UnsafeConfirmations)
        ));
        assert!(matches!(
            wallet_balance_with_confirmations(
                &missing_wallet,
                WalletNetwork::Regtest,
                LOCAL_CONFIRMATIONS,
                false,
            ),
            Err(WalletServiceError::UnsafeConfirmations)
        ));
        assert!(matches!(
            wallet_balance_with_confirmations(
                &missing_wallet,
                WalletNetwork::Regtest,
                LOCAL_CONFIRMATIONS,
                true,
            ),
            Err(WalletServiceError::WalletDatabaseMissing)
        ));
        assert!(matches!(
            wallet_balance(&missing_wallet, WalletNetwork::Regtest),
            Err(WalletServiceError::WalletDatabaseMissing)
        ));
    }

    #[test]
    fn transparent_coinbase_maturity_boundary_is_exactly_one_hundred_blocks() {
        assert!(!coinbase_is_mature(10, 109));
        assert!(coinbase_is_mature(10, 110));
        assert!(!coinbase_is_mature(110, 10));
        assert_eq!(COINBASE_SHIELDING_MATURITY, 100);
        assert_eq!(TRANSPARENT_COINBASE_RECOVERY_START_HEIGHT, 1);
    }

    #[test]
    fn transparent_coinbase_recovery_precedes_a_late_default_birthday() {
        let tip_height = 250;
        let shielded_birthday = default_wallet_birthday(tip_height);
        assert_eq!(shielded_birthday, 151);
        assert_eq!(transparent_coinbase_recovery_start(tip_height), Some(1));
        assert!(TRANSPARENT_COINBASE_RECOVERY_START_HEIGHT < shielded_birthday);
        assert!(coinbase_is_mature(
            TRANSPARENT_COINBASE_RECOVERY_START_HEIGHT,
            tip_height + 1,
        ));
        assert_eq!(transparent_coinbase_recovery_start(0), None);
    }

    #[test]
    fn transparent_coinbase_recovery_has_a_hard_session_deadline() {
        assert_eq!(
            MAX_TRANSPARENT_COINBASE_RECOVERY_DURATION,
            Duration::from_secs(5 * 60)
        );
        let expired = tokio::time::Instant::now() - Duration::from_secs(1);
        assert!(matches!(
            ensure_transparent_recovery_deadline(expired),
            Err(WalletServiceError::TransparentRecoveryDeadline)
        ));
    }

    #[test]
    fn transparent_history_groups_preserve_durable_request_boundaries() {
        let receiver = zcash_transparent::address::TransparentAddress::PublicKeyHash([1; 20]);
        let unused_ephemeral =
            zcash_transparent::address::TransparentAddress::PublicKeyHash([2; 20]);
        let spend_request = || {
            TransactionDataRequest::transactions_involving_address(
                receiver,
                BlockHeight::from_u32(7),
                Some(BlockHeight::from_u32(18)),
                None,
                TransactionStatusFilter::Mined,
                OutputStatusFilter::All,
            )
        };
        let ephemeral_probe = TransactionDataRequest::transactions_involving_address(
            unused_ephemeral,
            BlockHeight::from_u32(0),
            None,
            None,
            TransactionStatusFilter::All,
            OutputStatusFilter::Unspent,
        );

        let groups = group_transparent_history_requests(
            vec![spend_request(), ephemeral_probe, spend_request()],
            receiver,
            20,
        )
        .unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups.get(&(7, 18)).map(Vec::len), Some(2));

        let foreign_spend_request = TransactionDataRequest::transactions_involving_address(
            unused_ephemeral,
            BlockHeight::from_u32(7),
            Some(BlockHeight::from_u32(18)),
            None,
            TransactionStatusFilter::Mined,
            OutputStatusFilter::All,
        );
        assert!(matches!(
            group_transparent_history_requests(vec![foreign_spend_request], receiver, 20),
            Err(WalletServiceError::UnexpectedTransparentHistoryRequest(_))
        ));

        let malformed_coinbase_request = TransactionDataRequest::transactions_involving_address(
            receiver,
            BlockHeight::from_u32(7),
            None,
            None,
            TransactionStatusFilter::All,
            OutputStatusFilter::Unspent,
        );
        assert!(matches!(
            group_transparent_history_requests(vec![malformed_coinbase_request], receiver, 20),
            Err(WalletServiceError::UnexpectedTransparentHistoryRequest(_))
        ));
    }

    #[test]
    fn signed_shielding_amounts_bind_expiry_fee_and_ironwood_value() {
        assert!(shielding_amounts_match(
            140,
            Some(-624_990_000),
            140,
            625_000_000,
            624_990_000,
            10_000,
        ));
        assert!(!shielding_amounts_match(
            141,
            Some(-624_990_000),
            140,
            625_000_000,
            624_990_000,
            10_000,
        ));
        assert!(!shielding_amounts_match(
            140,
            Some(-624_980_000),
            140,
            625_000_000,
            624_990_000,
            10_000,
        ));
        assert!(!shielding_amounts_match(
            140,
            Some(-624_990_000),
            140,
            625_000_001,
            624_990_000,
            10_000,
        ));
    }

    #[test]
    fn signed_transfer_outputs_bind_recipient_value_memo_role_and_pool() {
        let payment = BoundIronwoodOutput {
            receiver: [1; 43],
            value_zat: 60_000,
            memo: MemoBytes::from_bytes(b"payout-17").unwrap(),
            role: TransferOutputRole::Payment,
            pool: ShieldedPool::Ironwood,
        };
        let change = BoundIronwoodOutput {
            receiver: [2; 43],
            value_zat: 30_000,
            memo: MemoBytes::empty(),
            role: TransferOutputRole::InternalChange,
            pool: ShieldedPool::Ironwood,
        };
        let expected = vec![payment.clone(), change.clone()];

        assert!(exact_bound_output_multisets_match(
            &expected,
            &[change.clone(), payment.clone()]
        ));

        for mutated in [
            BoundIronwoodOutput {
                receiver: [3; 43],
                ..payment.clone()
            },
            BoundIronwoodOutput {
                value_zat: payment.value_zat + 1,
                ..payment.clone()
            },
            BoundIronwoodOutput {
                memo: MemoBytes::from_bytes(b"wrong memo").unwrap(),
                ..payment.clone()
            },
            BoundIronwoodOutput {
                role: TransferOutputRole::InternalChange,
                ..payment.clone()
            },
            BoundIronwoodOutput {
                pool: ShieldedPool::Orchard,
                ..payment.clone()
            },
        ] {
            assert!(!exact_bound_output_multisets_match(
                &expected,
                &[mutated, change.clone()]
            ));
        }
        assert!(!exact_bound_output_multisets_match(
            &expected,
            std::slice::from_ref(&payment)
        ));
        assert!(!exact_bound_output_multisets_match(
            &expected,
            &[payment.clone(), change.clone(), change.clone()]
        ));
        assert!(!exact_bound_output_multisets_match(
            &expected,
            &[payment.clone(), payment]
        ));
    }

    #[test]
    fn signed_transfer_contains_every_selected_nullifier_and_exact_action_shape() {
        let first = [1; 32];
        let second = [2; 32];
        let dummy = [3; 32];
        let other_wallet_note = [4; 32];
        assert!(ironwood_input_shape_matches(
            &[first, second],
            &[first, second, other_wallet_note],
            &[dummy, second, first],
            3,
        ));
        assert!(!ironwood_input_shape_matches(
            &[first, second],
            &[first, second, other_wallet_note],
            &[dummy, first],
            2,
        ));
        assert!(!ironwood_input_shape_matches(
            &[first, second],
            &[first, second, other_wallet_note],
            &[other_wallet_note, second, first],
            3,
        ));
        assert!(!ironwood_input_shape_matches(
            &[first, second],
            &[first, second, other_wallet_note],
            &[dummy, second, first],
            2,
        ));
        assert!(!ironwood_input_shape_matches(
            &[first, second],
            &[first, second, other_wallet_note],
            &[first, second, second],
            3,
        ));
        assert!(!ironwood_input_shape_matches(
            &[first, first],
            &[first, second, other_wallet_note],
            &[dummy, second, first],
            3,
        ));
        assert!(!ironwood_input_shape_matches(
            &[first, second],
            &[first, second, second],
            &[dummy, second, first],
            3,
        ));
    }

    #[test]
    fn signed_coinbase_shielding_rejects_any_known_wallet_note_input() {
        let known_wallet_note = [1; 32];
        let first_dummy = [2; 32];
        let second_dummy = [3; 32];

        assert!(ironwood_input_shape_matches(
            &[],
            &[known_wallet_note],
            &[first_dummy, second_dummy],
            2,
        ));
        assert!(!ironwood_input_shape_matches(
            &[],
            &[known_wallet_note],
            &[known_wallet_note, first_dummy],
            2,
        ));
        assert!(!ironwood_input_shape_matches(
            &[],
            &[known_wallet_note],
            &[first_dummy, second_dummy],
            3,
        ));
        assert!(!ironwood_input_shape_matches(
            &[],
            &[known_wallet_note],
            &[first_dummy, first_dummy],
            2,
        ));
    }

    #[test]
    fn signed_coinbase_inputs_must_match_the_exact_unique_proposal_set() {
        let first = OutPoint::new([1; 32], 0);
        let second = OutPoint::new([2; 32], 1);
        assert!(unique_outpoint_sets_match(
            &[first.clone(), second.clone()],
            &[second.clone(), first.clone()],
        ));
        assert!(!unique_outpoint_sets_match(
            &[first.clone(), second],
            &[first.clone(), first.clone()],
        ));
        assert!(!unique_outpoint_sets_match(
            &[first.clone(), first.clone()],
            &[first.clone(), first],
        ));
    }

    #[test]
    fn post_proof_policy_accepts_advanced_canonical_tip_only_before_expiry() {
        let expected_tip = block_ref(10, 10);
        let expected_anchor = block_ref(7, 7);
        assert!(post_proof_chain_is_acceptable(
            block_ref(12, 12),
            expected_tip,
            expected_tip,
            expected_anchor,
            expected_anchor,
            20,
        ));
        assert!(!post_proof_chain_is_acceptable(
            block_ref(9, 9),
            expected_tip,
            expected_tip,
            expected_anchor,
            expected_anchor,
            20,
        ));
        assert!(!post_proof_chain_is_acceptable(
            block_ref(20, 20),
            expected_tip,
            expected_tip,
            expected_anchor,
            expected_anchor,
            20,
        ));
    }

    #[test]
    fn post_proof_policy_rejects_tip_or_anchor_reorg() {
        let expected_tip = block_ref(10, 10);
        let expected_anchor = block_ref(7, 7);
        assert!(!post_proof_chain_is_acceptable(
            block_ref(12, 12),
            block_ref(10, 99),
            expected_tip,
            expected_anchor,
            expected_anchor,
            20,
        ));
        assert!(!post_proof_chain_is_acceptable(
            block_ref(12, 12),
            expected_tip,
            expected_tip,
            block_ref(7, 99),
            expected_anchor,
            20,
        ));
    }

    #[test]
    fn expected_chain_refs_use_only_the_frozen_genesis_without_stored_block_metadata() {
        let directory = tempfile::tempdir().unwrap();
        let wallet_path = directory.path().join("wallet.sqlite");
        let network = WalletNetwork::Regtest;
        let wallet = open_wallet_database(&wallet_path, network).unwrap();

        assert_eq!(
            expected_chain_ref(&wallet, network, 0).unwrap(),
            BlockRef {
                height: 0,
                hash: network.genesis_hash(),
            }
        );
        assert!(matches!(
            expected_chain_ref(&wallet, network, 1),
            Err(WalletServiceError::StaleChain)
        ));
        drop(wallet);

        let stored_hash = [0x5a; 32];
        let connection = rusqlite::Connection::open(&wallet_path).unwrap();
        connection
            .execute(
                "INSERT INTO blocks (height, hash, time, sapling_tree)
                 VALUES (1, ?1, 0, X'')",
                rusqlite::params![stored_hash],
            )
            .unwrap();
        drop(connection);

        let wallet = open_wallet_database(&wallet_path, network).unwrap();
        assert_eq!(
            expected_chain_ref(&wallet, network, 1).unwrap(),
            BlockRef {
                height: 1,
                hash: stored_hash,
            }
        );
    }

    #[test]
    fn transparent_recovery_marker_is_crash_resumable_tokenized_and_tip_bound() {
        let directory = tempfile::tempdir().unwrap();
        let wallet_path = directory.path().join("wallet.sqlite");
        let network = WalletNetwork::Regtest;
        let operation_lock =
            acquire_wallet_operation_lock(&wallet_path, WalletOperationLockMode::Exclusive)
                .unwrap();
        let mut wallet = open_wallet_database(&wallet_path, network).unwrap();

        assert!(matches!(
            require_transparent_recovery_complete(&mut wallet),
            Err(WalletServiceError::TransparentRecoveryIncomplete)
        ));
        drop(wallet);

        let completed_hash = [0x5a; 32];
        let connection = rusqlite::Connection::open(&wallet_path).unwrap();
        connection
            .execute(
                "INSERT INTO blocks (height, hash, time, sapling_tree)
                 VALUES (1, ?1, 0, X'')",
                rusqlite::params![completed_hash],
            )
            .unwrap();
        drop(connection);

        let mut wallet = open_wallet_database(&wallet_path, network).unwrap();
        let abandoned = begin_transparent_recovery(&mut wallet).unwrap();
        assert!(matches!(
            require_transparent_recovery_complete(&mut wallet),
            Err(WalletServiceError::TransparentRecoveryIncomplete)
        ));

        // Reclaiming while the exclusive operation lock is held replaces a
        // crash residue, and the abandoned owner can no longer publish.
        let replacement = begin_transparent_recovery(&mut wallet).unwrap();
        assert_ne!(abandoned, replacement);
        let completed_tip = BlockRef {
            height: 1,
            hash: completed_hash,
        };
        assert!(matches!(
            complete_transparent_recovery(&mut wallet, abandoned, completed_tip),
            Err(WalletServiceError::TransparentRecoveryOwnershipLost)
        ));
        assert!(matches!(
            require_transparent_recovery_complete(&mut wallet),
            Err(WalletServiceError::TransparentRecoveryIncomplete)
        ));
        assert!(matches!(
            complete_transparent_recovery(
                &mut wallet,
                replacement,
                BlockRef {
                    height: 1,
                    hash: [0x7c; 32],
                },
            ),
            Err(WalletServiceError::StaleChain)
        ));
        assert!(matches!(
            require_transparent_recovery_complete(&mut wallet),
            Err(WalletServiceError::TransparentRecoveryIncomplete)
        ));
        complete_transparent_recovery(&mut wallet, replacement, completed_tip).unwrap();
        require_transparent_recovery_complete(&mut wallet).unwrap();
        drop(wallet);
        drop(operation_lock);

        let connection = rusqlite::Connection::open(&wallet_path).unwrap();
        connection
            .execute(
                "UPDATE blocks SET hash = ?1 WHERE height = 1",
                rusqlite::params![[0x6bu8; 32]],
            )
            .unwrap();
        drop(connection);
        assert!(matches!(
            wallet_balance(&wallet_path, network),
            Err(WalletServiceError::TransparentRecoveryIncomplete)
        ));
    }

    #[test]
    fn wallet_operation_lock_serializes_mutations_and_allows_shared_reads() {
        let directory = tempfile::tempdir().unwrap();
        let wallet_path = directory.path().join("wallet.sqlite");

        let exclusive =
            acquire_wallet_operation_lock(&wallet_path, WalletOperationLockMode::Exclusive)
                .unwrap();
        assert!(matches!(
            acquire_wallet_operation_lock(&wallet_path, WalletOperationLockMode::Shared),
            Err(WalletServiceError::WalletBusy)
        ));
        assert!(matches!(
            acquire_wallet_operation_lock(&wallet_path, WalletOperationLockMode::Exclusive),
            Err(WalletServiceError::WalletBusy)
        ));
        drop(exclusive);

        let first_shared =
            acquire_wallet_operation_lock(&wallet_path, WalletOperationLockMode::Shared).unwrap();
        let second_shared =
            acquire_wallet_operation_lock(&wallet_path, WalletOperationLockMode::Shared).unwrap();
        assert!(matches!(
            acquire_wallet_operation_lock(&wallet_path, WalletOperationLockMode::Exclusive),
            Err(WalletServiceError::WalletBusy)
        ));
        drop((first_shared, second_shared));

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let lock_path = wallet_operation_lock_path(&wallet_path).unwrap();
            assert_eq!(
                fs::metadata(lock_path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }

    #[test]
    fn wallet_balance_never_migrates_under_a_shared_read_lock() {
        let directory = tempfile::tempdir().unwrap();
        let wallet_path = directory.path().join("wallet.sqlite");
        create_wallet_accounts(&wallet_path, WalletNetwork::Regtest, 1);

        let connection = rusqlite::Connection::open(&wallet_path).unwrap();
        connection
            .execute("DROP TABLE ext_wcash_ironwood_sync", [])
            .unwrap();
        drop(connection);

        let reader_path = wallet_path.clone();
        let (reader_ready_tx, reader_ready_rx) = std::sync::mpsc::channel();
        let (reader_release_tx, reader_release_rx) = std::sync::mpsc::channel();
        let reader = std::thread::spawn(move || {
            let operation_lock =
                acquire_wallet_operation_lock(&reader_path, WalletOperationLockMode::Shared)
                    .unwrap();
            reader_ready_tx.send(()).unwrap();
            reader_release_rx.recv().unwrap();
            drop(operation_lock);
        });
        reader_ready_rx.recv().unwrap();

        assert!(matches!(
            wallet_balance(&wallet_path, WalletNetwork::Regtest),
            Err(WalletServiceError::TransparentRecoveryIncomplete)
        ));

        let connection = rusqlite::Connection::open(&wallet_path).unwrap();
        let ironwood_table_exists: bool = connection
            .query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM sqlite_schema
                    WHERE type = 'table' AND name = 'ext_wcash_ironwood_sync'
                )",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(!ironwood_table_exists);
        drop(connection);

        assert!(matches!(
            acquire_wallet_operation_lock(&wallet_path, WalletOperationLockMode::Exclusive),
            Err(WalletServiceError::WalletBusy)
        ));
        reader_release_tx.send(()).unwrap();
        reader.join().unwrap();

        let migration =
            acquire_wallet_operation_lock(&wallet_path, WalletOperationLockMode::Exclusive)
                .unwrap();
        drop(open_wallet_database(&wallet_path, WalletNetwork::Regtest).unwrap());
        drop(migration);

        let connection = rusqlite::Connection::open(&wallet_path).unwrap();
        let ironwood_table_exists: bool = connection
            .query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM sqlite_schema
                    WHERE type = 'table' AND name = 'ext_wcash_ironwood_sync'
                )",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(ironwood_table_exists);
    }

    #[test]
    fn wallet_balance_queries_a_completed_recovery_marker_without_modifying_it() {
        let directory = tempfile::tempdir().unwrap();
        let wallet_path = directory.path().join("wallet.sqlite");
        let network = WalletNetwork::Regtest;
        create_wallet_accounts(&wallet_path, network, 1);

        let operation_lock =
            acquire_wallet_operation_lock(&wallet_path, WalletOperationLockMode::Exclusive)
                .unwrap();
        let mut wallet = open_wallet_database(&wallet_path, network).unwrap();
        wallet.update_chain_tip(BlockHeight::from_u32(0)).unwrap();
        let recovery = begin_transparent_recovery(&mut wallet).unwrap();
        complete_transparent_recovery(
            &mut wallet,
            recovery,
            BlockRef {
                height: 0,
                hash: network.genesis_hash(),
            },
        )
        .unwrap();
        drop(wallet);
        drop(operation_lock);

        let database_before = fs::read(&wallet_path).unwrap();
        assert!(matches!(
            wallet_balance(&wallet_path, network),
            Err(WalletServiceError::NotSynchronized)
        ));
        assert_eq!(fs::read(&wallet_path).unwrap(), database_before);
    }

    #[tokio::test]
    async fn cancelled_sync_releases_the_operation_lock_before_network_or_database_io() {
        let directory = tempfile::tempdir().unwrap();
        let wallet_path = directory.path().join("wallet.sqlite");
        let network = WalletNetwork::Regtest;
        let mut client = AttestedWcashClient::disconnected_for_test(network);
        let cancellation = WalletSyncCancellation::new();
        let cancellation_request = cancellation.clone();
        std::thread::spawn(move || cancellation_request.cancel())
            .join()
            .unwrap();

        let error = synchronize_wallet_cancellable(
            &mut client,
            &wallet_path,
            network,
            MAX_SYNC_BATCH_SIZE,
            &cancellation,
        )
        .await
        .unwrap_err();
        assert!(matches!(
            &error,
            WalletServiceError::SynchronizationCancelled
        ));
        assert_eq!(error.to_string(), "wallet synchronization was cancelled");
        assert!(cancellation.is_cancelled());
        assert!(!wallet_path.exists());

        let replacement =
            acquire_wallet_operation_lock(&wallet_path, WalletOperationLockMode::Exclusive)
                .unwrap();
        drop(replacement);
    }

    #[test]
    fn post_signing_error_exposes_every_recovery_txid() {
        let error = WalletServiceError::PersistedTransactionsRequireReview {
            txids: vec!["11".repeat(32), "22".repeat(32)],
            reason: "test reorg".to_owned(),
        };
        assert!(error.to_string().contains(&"11".repeat(32)));
        assert!(error.to_string().contains(&"22".repeat(32)));
        assert!(error.to_string().contains("list-pending"));
    }

    fn fake_pending_raw(txid: [u8; 32], expiry_height: u32) -> Vec<u8> {
        let mut raw = Vec::with_capacity(36);
        raw.extend_from_slice(&txid);
        raw.extend_from_slice(&expiry_height.to_le_bytes());
        raw
    }

    fn inspect_fake_pending(raw: &[u8]) -> Result<(zcash_protocol::TxId, u32), WalletServiceError> {
        let txid: [u8; 32] = raw
            .get(..32)
            .ok_or_else(|| {
                WalletServiceError::Database("fake pending transaction is truncated".to_owned())
            })?
            .try_into()
            .expect("slice length checked");
        let expiry_height: [u8; 4] = raw
            .get(32..36)
            .ok_or_else(|| {
                WalletServiceError::Database("fake pending transaction is truncated".to_owned())
            })?
            .try_into()
            .expect("slice length checked");
        Ok((
            zcash_protocol::TxId::from_bytes(txid),
            u32::from_le_bytes(expiry_height),
        ))
    }

    fn set_synchronized_test_tip(
        wallet_path: &Path,
        network: WalletNetwork,
        tip_height: u32,
        hash_byte: u8,
    ) -> BlockRef {
        let mut connection = rusqlite::Connection::open(wallet_path).unwrap();
        let transaction = connection.transaction().unwrap();
        transaction.execute("DELETE FROM blocks", []).unwrap();
        transaction.execute("DELETE FROM scan_queue", []).unwrap();
        for height in 1..=tip_height {
            let hash = if height == tip_height {
                vec![hash_byte; 32]
            } else {
                vec![(height & 0xff) as u8; 32]
            };
            transaction
                .execute(
                    "INSERT INTO blocks
                     (height, hash, time, sapling_tree,
                      sapling_commitment_tree_size, orchard_commitment_tree_size,
                      sapling_output_count, orchard_action_count,
                      ironwood_commitment_tree_size, ironwood_action_count)
                     VALUES (?1, ?2, 0, X'', 0, 0, 0, 0, 0, 0)",
                    rusqlite::params![height, hash],
                )
                .unwrap();
        }
        transaction
            .execute(
                "INSERT INTO scan_queue (block_range_start, block_range_end, priority)
                 VALUES (1, ?1, 10)",
                [tip_height + 1],
            )
            .unwrap();
        transaction.commit().unwrap();
        drop(connection);

        let exact_tip = block_ref(tip_height, hash_byte);
        let operation_lock =
            acquire_wallet_operation_lock(wallet_path, WalletOperationLockMode::Exclusive).unwrap();
        let mut wallet = open_wallet_database(wallet_path, network).unwrap();
        let recovery = begin_transparent_recovery(&mut wallet).unwrap();
        complete_transparent_recovery(&mut wallet, recovery, exact_tip).unwrap();
        drop(wallet);
        drop(operation_lock);
        exact_tip
    }

    fn parser_valid_ironwood_raw(
        network: WalletNetwork,
        expiry_height: u32,
        seed: u64,
    ) -> (zcash_protocol::TxId, Vec<u8>) {
        let bundle = zebra_chain::transaction::arbitrary::fake_bundle_for_branch(
            network.branch_id(),
            orchard::ValuePool::Ironwood,
            1,
            seed,
        )
        .expect("Ironwood is active for every Wcash wallet network");
        let transaction = zcash_primitives::transaction::TransactionData::<
            zcash_primitives::transaction::Authorized,
        >::from_parts_v6(
            network.branch_id(),
            0,
            BlockHeight::from_u32(expiry_height),
            None,
            None,
            None,
            Some(bundle),
        )
        .freeze()
        .unwrap();
        let txid = transaction.txid();
        let mut raw = Vec::new();
        transaction.write(&mut raw).unwrap();
        (txid, raw)
    }

    #[test]
    fn active_pending_public_api_attests_the_exact_wallet_tip() {
        let directory = tempfile::tempdir().unwrap();
        let wallet_path = directory.path().join("wallet.sqlite");
        let network = WalletNetwork::Regtest;
        create_wallet_accounts(&wallet_path, network, 1);
        let exact_tip = set_synchronized_test_tip(&wallet_path, network, 100, 0xaa);

        let page = active_pending_signed_transactions(&wallet_path, network, None, None, 1)
            .expect("the synchronized exact tip is internally attested");
        assert_eq!(page.exact_tip, Some(exact_tip));
        assert!(page.transactions.is_empty());
        assert_eq!(page.next_after_row_id, None);

        assert!(matches!(
            active_pending_signed_transactions(&wallet_path, network, None, Some(1), 1),
            Err(WalletServiceError::InvalidRequest(_))
        ));

        assert!(matches!(
            active_pending_signed_transactions(
                &wallet_path,
                network,
                Some(block_ref(exact_tip.height, 0xbb)),
                None,
                1,
            ),
            Err(WalletServiceError::StaleChain)
        ));
        assert!(matches!(
            active_pending_signed_transactions(
                &wallet_path,
                network,
                Some(block_ref(exact_tip.height + 1, 0xaa)),
                None,
                1,
            ),
            Err(WalletServiceError::StaleChain)
        ));
    }

    #[test]
    fn active_pending_public_api_never_filters_on_mismatched_sqlite_expiry() {
        let directory = tempfile::tempdir().unwrap();
        let wallet_path = directory.path().join("wallet.sqlite");
        let network = WalletNetwork::Regtest;
        create_wallet_accounts(&wallet_path, network, 1);
        set_synchronized_test_tip(&wallet_path, network, 100, 0xaa);
        let (txid, raw) = parser_valid_ironwood_raw(network, 101, 7);

        let connection = rusqlite::Connection::open(&wallet_path).unwrap();
        connection
            .execute(
                "INSERT INTO transactions
                 (txid, created, expiry_height, raw, target_height, min_observed_height)
                 VALUES (?1, '2026-09-12T00:00:00Z', 100, ?2, 60, 1)",
                rusqlite::params![txid.as_ref(), raw],
            )
            .unwrap();
        drop(connection);

        assert!(matches!(
            active_pending_signed_transactions(&wallet_path, network, None, None, 1),
            Err(WalletServiceError::Database(_))
        ));
    }

    #[test]
    fn signing_guard_rejects_a_parser_valid_active_transaction_under_the_writer_lock() {
        let directory = tempfile::tempdir().unwrap();
        let wallet_path = directory.path().join("wallet.sqlite");
        let network = WalletNetwork::Regtest;
        create_wallet_accounts(&wallet_path, network, 1);
        set_synchronized_test_tip(&wallet_path, network, 100, 0xaa);
        let (expired_txid, expired_raw) = parser_valid_ironwood_raw(network, 100, 8);
        let (active_txid, active_raw) = parser_valid_ironwood_raw(network, 101, 9);

        let connection = rusqlite::Connection::open(&wallet_path).unwrap();
        for (txid, expiry_height, raw) in [
            (expired_txid, 100_u32, expired_raw),
            (active_txid, 101_u32, active_raw),
        ] {
            connection
                .execute(
                    "INSERT INTO transactions
                     (txid, created, expiry_height, raw, target_height, min_observed_height)
                     VALUES (?1, '2026-09-12T00:00:00Z', ?2, ?3, 60, 1)",
                    rusqlite::params![txid.as_ref(), expiry_height, raw],
                )
                .unwrap();
        }
        drop(connection);

        let _writer =
            acquire_wallet_operation_lock(&wallet_path, WalletOperationLockMode::Exclusive)
                .unwrap();
        let mut wallet = open_wallet_database(&wallet_path, network).unwrap();
        let error = ensure_no_active_pending_transactions(&mut wallet, network).unwrap_err();
        assert!(matches!(
            error,
            WalletServiceError::ActivePendingTransaction {
                txid,
                expiry_height: 101,
            } if txid == active_txid.to_string()
        ));
    }

    #[test]
    fn pending_transactions_survive_restart_and_page_without_replacement() {
        let directory = tempfile::tempdir().unwrap();
        let wallet_path = directory.path().join("wallet.sqlite");
        drop(open_wallet_database(&wallet_path, WalletNetwork::Regtest).unwrap());

        let connection = rusqlite::Connection::open(&wallet_path).unwrap();
        for marker in [1u8, 2, 3] {
            connection
                .execute(
                    "INSERT INTO transactions
                     (txid, created, expiry_height, raw, target_height, min_observed_height)
                     VALUES (?1, '2026-09-09T00:00:00Z', 20, ?2, 10, 1)",
                    rusqlite::params![vec![marker; 32], fake_pending_raw([marker; 32], 20)],
                )
                .unwrap();
        }
        connection
            .execute(
                "INSERT INTO transactions
                 (txid, created, mined_height, expiry_height, raw, target_height, min_observed_height)
                 VALUES (?1, '2026-09-09T00:00:00Z', 11, 20, ?2, 10, 1)",
                rusqlite::params![vec![9u8; 32], fake_pending_raw([9; 32], 20)],
            )
            .unwrap();
        drop(connection);

        let mut wallet = open_wallet_database(&wallet_path, WalletNetwork::Regtest).unwrap();
        let first = pending_signed_transaction_page_with_inspector(
            &mut wallet,
            WalletNetwork::Regtest,
            PendingTransactionQuery::All,
            0,
            2,
            inspect_fake_pending,
        )
        .unwrap();
        assert_eq!(first.exact_tip, None);
        assert_eq!(first.transactions.len(), 2);
        assert_eq!(
            first.transactions[0].txid,
            zcash_protocol::TxId::from_bytes([1; 32]).to_string()
        );
        assert_eq!(
            first.transactions[0].raw_transaction_hex,
            hex::encode(fake_pending_raw([1; 32], 20))
        );
        let next = first
            .next_after_row_id
            .expect("a third unmined transaction remains");
        drop(wallet);

        let mut reopened = open_wallet_database(&wallet_path, WalletNetwork::Regtest).unwrap();
        let second = pending_signed_transaction_page_with_inspector(
            &mut reopened,
            WalletNetwork::Regtest,
            PendingTransactionQuery::All,
            i64::try_from(next).expect("test cursor fits"),
            2,
            inspect_fake_pending,
        )
        .unwrap();
        assert_eq!(second.transactions.len(), 1);
        assert_eq!(
            second.transactions[0].txid,
            zcash_protocol::TxId::from_bytes([3; 32]).to_string()
        );
        assert_eq!(second.next_after_row_id, None);
    }

    #[test]
    fn active_pending_transactions_skip_terminal_history_and_reappear_after_a_reorg() {
        const EXACT_TIP: u32 = 100;
        const EXPIRED_ROWS: u32 = 2_501;

        let directory = tempfile::tempdir().unwrap();
        let wallet_path = directory.path().join("wallet.sqlite");
        drop(open_wallet_database(&wallet_path, WalletNetwork::Regtest).unwrap());

        let mut connection = rusqlite::Connection::open(&wallet_path).unwrap();
        let transaction = connection.transaction().unwrap();
        for index in 0..EXPIRED_ROWS {
            let mut txid = [0_u8; 32];
            txid[..4].copy_from_slice(&index.to_le_bytes());
            transaction
                .execute(
                    "INSERT INTO transactions
                     (txid, created, expiry_height, raw, target_height, min_observed_height)
                     VALUES (?1, '2026-09-12T00:00:00Z', ?2, ?3, 60, 1)",
                    rusqlite::params![txid, EXACT_TIP, fake_pending_raw(txid, EXACT_TIP)],
                )
                .unwrap();
        }
        for (txid, expiry_height) in [
            ([0xf0_u8; 32], 0_u32),
            ([0xf1_u8; 32], EXACT_TIP + 1),
            ([0xf2_u8; 32], EXACT_TIP + 2),
        ] {
            transaction
                .execute(
                    "INSERT INTO transactions
                     (txid, created, expiry_height, raw, target_height, min_observed_height)
                     VALUES (?1, '2026-09-12T00:00:00Z', ?2, ?3, 60, 1)",
                    rusqlite::params![txid, expiry_height, fake_pending_raw(txid, expiry_height)],
                )
                .unwrap();
        }
        transaction
            .execute(
                "INSERT INTO transactions
                 (txid, created, mined_height, expiry_height, raw, target_height, min_observed_height)
                 VALUES (?1, '2026-09-12T00:00:00Z', 80, ?2, ?3, 60, 1)",
                rusqlite::params![
                    vec![0xf3_u8; 32],
                    EXACT_TIP + 1,
                    fake_pending_raw([0xf3; 32], EXACT_TIP + 1)
                ],
            )
            .unwrap();
        transaction.commit().unwrap();
        drop(connection);

        let exact_tip = block_ref(EXACT_TIP, 0xaa);
        let mut wallet = open_wallet_database(&wallet_path, WalletNetwork::Regtest).unwrap();
        let first = pending_signed_transaction_page_with_inspector(
            &mut wallet,
            WalletNetwork::Regtest,
            PendingTransactionQuery::ActiveAtAttestedTip(exact_tip),
            0,
            2,
            inspect_fake_pending,
        )
        .unwrap();
        assert_eq!(first.exact_tip, Some(exact_tip));
        assert_eq!(
            first
                .transactions
                .iter()
                .map(|transaction| transaction.txid.clone())
                .collect::<Vec<_>>(),
            vec![
                zcash_protocol::TxId::from_bytes([0xf0; 32]).to_string(),
                zcash_protocol::TxId::from_bytes([0xf1; 32]).to_string(),
            ]
        );
        let second = pending_signed_transaction_page_with_inspector(
            &mut wallet,
            WalletNetwork::Regtest,
            PendingTransactionQuery::ActiveAtAttestedTip(exact_tip),
            i64::try_from(first.next_after_row_id.unwrap()).unwrap(),
            2,
            inspect_fake_pending,
        )
        .unwrap();
        assert_eq!(second.transactions.len(), 1);
        assert_eq!(second.transactions[0].expiry_height, EXACT_TIP + 2);
        assert_eq!(second.next_after_row_id, None);

        let after_reorg = pending_signed_transaction_page_with_inspector(
            &mut wallet,
            WalletNetwork::Regtest,
            PendingTransactionQuery::ActiveAtAttestedTip(block_ref(EXACT_TIP - 1, 0xbb)),
            0,
            1,
            inspect_fake_pending,
        )
        .unwrap();
        assert_eq!(after_reorg.transactions.len(), 1);
        assert_eq!(after_reorg.transactions[0].expiry_height, EXACT_TIP);
    }

    #[test]
    fn active_pending_transactions_reject_untrusted_expiry_metadata() {
        let directory = tempfile::tempdir().unwrap();
        let wallet_path = directory.path().join("wallet.sqlite");
        drop(open_wallet_database(&wallet_path, WalletNetwork::Regtest).unwrap());

        let connection = rusqlite::Connection::open(&wallet_path).unwrap();
        connection
            .execute(
                "INSERT INTO transactions
                 (txid, created, expiry_height, raw, target_height, min_observed_height)
                 VALUES (?1, '2026-09-12T00:00:00Z', 100, ?2, 60, 1)",
                rusqlite::params![vec![0x41_u8; 32], fake_pending_raw([0x41; 32], 101)],
            )
            .unwrap();
        drop(connection);

        let mut wallet = open_wallet_database(&wallet_path, WalletNetwork::Regtest).unwrap();
        let error = pending_signed_transaction_page_with_inspector(
            &mut wallet,
            WalletNetwork::Regtest,
            PendingTransactionQuery::ActiveAtAttestedTip(block_ref(100, 0xaa)),
            0,
            1,
            inspect_fake_pending,
        )
        .unwrap_err();
        assert!(matches!(error, WalletServiceError::Database(_)));

        drop(wallet);
        let connection = rusqlite::Connection::open(&wallet_path).unwrap();
        connection
            .execute("UPDATE transactions SET expiry_height = NULL", [])
            .unwrap();
        drop(connection);
        let mut wallet = open_wallet_database(&wallet_path, WalletNetwork::Regtest).unwrap();
        let error = pending_signed_transaction_page_with_inspector(
            &mut wallet,
            WalletNetwork::Regtest,
            PendingTransactionQuery::ActiveAtAttestedTip(block_ref(100, 0xaa)),
            0,
            1,
            inspect_fake_pending,
        )
        .unwrap_err();
        assert!(matches!(error, WalletServiceError::Database(_)));
    }

    #[test]
    fn pending_transactions_reject_negative_expiry_metadata() {
        let directory = tempfile::tempdir().unwrap();
        let wallet_path = directory.path().join("wallet.sqlite");
        drop(open_wallet_database(&wallet_path, WalletNetwork::Regtest).unwrap());

        let connection = rusqlite::Connection::open(&wallet_path).unwrap();
        connection
            .execute(
                "INSERT INTO transactions
                 (txid, created, expiry_height, raw, target_height, min_observed_height)
                 VALUES (?1, '2026-09-12T00:00:00Z', -1, ?2, 60, 1)",
                rusqlite::params![vec![0x42_u8; 32], fake_pending_raw([0x42; 32], 0)],
            )
            .unwrap();
        drop(connection);

        let mut wallet = open_wallet_database(&wallet_path, WalletNetwork::Regtest).unwrap();
        let error = pending_signed_transaction_page_with_inspector(
            &mut wallet,
            WalletNetwork::Regtest,
            PendingTransactionQuery::ActiveAtAttestedTip(block_ref(100, 0xaa)),
            0,
            1,
            inspect_fake_pending,
        )
        .unwrap_err();
        assert!(matches!(error, WalletServiceError::Database(_)));
    }

    #[test]
    fn active_pending_pagination_binds_height_and_hash() {
        let actual = block_ref(100, 0xaa);
        assert!(verify_expected_pending_tip(None, actual).is_ok());
        assert!(verify_expected_pending_tip(Some(actual), actual).is_ok());
        assert!(matches!(
            verify_expected_pending_tip(Some(block_ref(101, 0xaa)), actual),
            Err(WalletServiceError::StaleChain)
        ));
        assert!(matches!(
            verify_expected_pending_tip(Some(block_ref(100, 0xbb)), actual),
            Err(WalletServiceError::StaleChain)
        ));
    }

    #[test]
    fn pending_transaction_readers_obey_the_wallet_operation_lock() {
        let directory = tempfile::tempdir().unwrap();
        let wallet_path = directory.path().join("wallet.sqlite");
        drop(open_wallet_database(&wallet_path, WalletNetwork::Regtest).unwrap());
        let _writer =
            acquire_wallet_operation_lock(&wallet_path, WalletOperationLockMode::Exclusive)
                .unwrap();

        assert!(matches!(
            pending_signed_transactions(&wallet_path, WalletNetwork::Regtest, None, 1),
            Err(WalletServiceError::WalletBusy)
        ));
    }

    #[test]
    fn disabled_pool_balances_fail_closed() {
        assert!(reject_legacy_balance_values(0, 0).is_ok());
        for (sapling, orchard) in [(1, 0), (0, 1), (1, 1)] {
            assert!(matches!(
                reject_legacy_balance_values(sapling, orchard),
                Err(WalletServiceError::LegacyPoolState)
            ));
        }
    }

    #[cfg(unix)]
    #[test]
    fn wallet_database_is_private_and_symlinks_are_rejected() {
        use std::os::unix::fs::{symlink, PermissionsExt};

        let directory = tempfile::tempdir().unwrap();
        let wallet_path = directory.path().join("wallet.sqlite");
        drop(open_wallet_database(&wallet_path, WalletNetwork::Regtest).unwrap());
        assert_eq!(
            fs::metadata(&wallet_path).unwrap().permissions().mode() & 0o777,
            0o600
        );

        let symlink_path = directory.path().join("wallet-link.sqlite");
        symlink(&wallet_path, &symlink_path).unwrap();
        assert!(matches!(
            open_wallet_database(&symlink_path, WalletNetwork::Regtest),
            Err(WalletServiceError::SymlinkWalletPath)
        ));
    }

    #[cfg(unix)]
    #[test]
    fn insecure_existing_wallet_permissions_are_rejected_without_rewriting_them() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let wallet_path = directory.path().join("wallet.sqlite");
        drop(
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&wallet_path)
                .unwrap(),
        );
        fs::set_permissions(&wallet_path, fs::Permissions::from_mode(0o640)).unwrap();

        assert!(matches!(
            open_wallet_database(&wallet_path, WalletNetwork::Regtest),
            Err(WalletServiceError::InsecureWalletPermissions)
        ));
        assert_eq!(
            fs::metadata(wallet_path).unwrap().permissions().mode() & 0o777,
            0o640
        );
    }

    #[cfg(unix)]
    #[test]
    fn writable_wallet_parent_is_rejected_before_file_creation() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let shared_parent = directory.path().join("shared");
        fs::create_dir(&shared_parent).unwrap();
        fs::set_permissions(&shared_parent, fs::Permissions::from_mode(0o770)).unwrap();
        let wallet_path = shared_parent.join("wallet.sqlite");

        assert!(matches!(
            open_wallet_database(&wallet_path, WalletNetwork::Regtest),
            Err(WalletServiceError::InsecureWalletParent)
        ));
        assert!(!wallet_path.exists());
    }

    #[cfg(unix)]
    #[test]
    fn owner_must_match_the_process_effective_uid() {
        let effective_uid = nix::unistd::geteuid().as_raw();
        let different_uid = effective_uid.wrapping_add(1);

        for object in ["file", "parent directory"] {
            assert!(matches!(
                require_effective_uid(different_uid, effective_uid, object),
                Err(WalletServiceError::InsecureWalletOwnership {
                    object: rejected_object,
                }) if rejected_object == object
            ));
        }
    }

    #[cfg(unix)]
    #[test]
    fn hard_linked_wallet_database_is_rejected() {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        let directory = tempfile::tempdir().unwrap();
        let original_path = directory.path().join("original.sqlite");
        let wallet_path = directory.path().join("wallet.sqlite");
        drop(
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&original_path)
                .unwrap(),
        );
        fs::set_permissions(&original_path, fs::Permissions::from_mode(0o600)).unwrap();
        fs::hard_link(&original_path, &wallet_path).unwrap();

        assert_eq!(fs::metadata(&wallet_path).unwrap().nlink(), 2);
        assert!(matches!(
            open_wallet_database(&wallet_path, WalletNetwork::Regtest),
            Err(WalletServiceError::HardLinkedWalletPath)
        ));
    }

    #[cfg(unix)]
    #[test]
    fn writable_ancestor_above_private_parent_is_rejected() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let unsafe_ancestor = directory.path().join("shared");
        let private_parent = unsafe_ancestor.join("wallet");
        fs::create_dir(&unsafe_ancestor).unwrap();
        fs::set_permissions(&unsafe_ancestor, fs::Permissions::from_mode(0o770)).unwrap();
        fs::create_dir(&private_parent).unwrap();
        fs::set_permissions(&private_parent, fs::Permissions::from_mode(0o700)).unwrap();
        let wallet_path = private_parent.join("wallet.sqlite");
        let canonical_unsafe_ancestor = fs::canonicalize(&unsafe_ancestor).unwrap();

        assert!(matches!(
            open_wallet_database(&wallet_path, WalletNetwork::Regtest),
            Err(WalletServiceError::InsecureWalletAncestor(path))
                if path == canonical_unsafe_ancestor
        ));
        assert!(!wallet_path.exists());
    }

    #[cfg(unix)]
    #[test]
    fn pathname_replacement_is_detected_against_the_opened_inode() {
        let directory = tempfile::tempdir().unwrap();
        let wallet_path = directory.path().join("wallet.sqlite");
        let moved_path = directory.path().join("moved.sqlite");
        let opened = private_open_options(true).open(&wallet_path).unwrap();
        fs::rename(&wallet_path, &moved_path).unwrap();
        drop(private_open_options(true).open(&wallet_path).unwrap());

        assert!(matches!(
            verify_named_file_matches(&wallet_path, &opened),
            Err(WalletServiceError::WalletPathChanged)
        ));
    }
}
