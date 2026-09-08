//! SQLite-backed Wcash wallet operations.

use std::{convert::Infallible, fs, num::NonZeroU32, path::Path};

use orchard::keys::Scope as OrchardScope;
use rand_core::{OsRng, RngCore};
use secrecy::SecretVec;
use serde::Serialize;
use thiserror::Error;
use zcash_client_backend::{
    data_api::wallet::{
        create_proposed_transactions,
        input_selection::{GreedyInputSelector, SpendPolicy},
        propose_transfer, unlock_proposal_inputs, ConfirmationsPolicy, LockRequest, SpendingKeys,
    },
    data_api::{Account, AccountBirthday, WalletRead, WalletWrite},
    fees::{standard::SingleOutputChangeStrategy, DustOutputPolicy, StandardFeeRule},
    sync,
    wallet::{LockOwner, OvkPolicy},
    zip321::{Payment, TransactionRequest},
};
use zcash_client_sqlite::{util::SystemClock, wallet::init::init_wallet_db, AccountUuid, WalletDb};
use zcash_primitives::transaction::TxVersion;
use zcash_proofs::prover::LocalTxProver;
use zcash_protocol::{
    consensus::BlockHeight, memo::MemoBytes, value::Zatoshis, PoolType, ShieldedPool,
};
use zebra_chain::{block::Height, parameters::Network};

use crate::{
    decode_recipient, derive_wallet_seed, derive_wallet_spending_key, encode_orchard_receiver,
    inspect_signed_transaction, AttestedWcashClient, BlockRef, MemoryBlockCache,
    WalletAddressError, WalletKeyError, WalletNetwork, WalletRpcError,
};

/// Maximum compact blocks requested in one synchronization batch.
pub const MAX_SYNC_BATCH_SIZE: u32 = 10_000;
/// Maximum recipients in one experimental shielded transfer.
pub const MAX_TRANSFER_RECIPIENTS: usize = 100;
/// Maximum transaction expiry interval accepted by the wallet.
pub const MAX_EXPIRY_DELTA: u32 = 100;
/// Maximum note-lock lifetime accepted by the wallet.
pub const MAX_LOCK_FOR_BLOCKS: u32 = 1_000;

/// Concrete SQLite wallet database used by this crate.
pub type WalletDatabase = WalletDb<rusqlite::Connection, Network, SystemClock, OsRng>;

/// Errors returned by persistent Wcash wallet operations.
#[derive(Debug, Error)]
pub enum WalletServiceError {
    /// A local wallet path could not be inspected or secured.
    #[error("wallet file security check failed: {0}")]
    Io(#[from] std::io::Error),
    /// The wallet path is a symbolic link, which this tool refuses to follow.
    #[error("refusing to open a wallet database through a symbolic link")]
    SymlinkWalletPath,
    /// Opening the SQLite file failed.
    #[error("could not open wallet database: {0}")]
    Sqlite(#[from] rusqlite::Error),
    /// Applying the reviewed librustzcash schema failed.
    #[error("wallet database migration failed: {0}")]
    Migration(String),
    /// A wallet database operation failed.
    #[error("wallet database operation failed: {0}")]
    Database(String),
    /// The compact-block synchronizer failed.
    #[error("wallet synchronization failed: {0}")]
    Synchronization(String),
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
    /// The live chain changed after the transaction proposal was selected.
    #[error("proposal tip or anchor is stale; synchronize and rebuild the transfer")]
    StaleChain,
    /// Signing succeeded and SQLite contains the transaction, but the final
    /// canonical-chain check failed.
    #[error(
        "signed transaction {txid} requires review after final chain validation failed: {reason}; recover its exact bytes with the export command"
    )]
    StaleAfterSigning {
        /// Canonical display-order identifier of the persisted transaction.
        txid: String,
        /// Final validation error.
        reason: String,
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
    /// Whether this invocation created the account.
    pub created: bool,
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
    /// Total transparent value, expected to stay zero for pool operation.
    pub transparent_total_zat: u64,
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

fn open_secured_wallet(
    path: &Path,
    network: WalletNetwork,
) -> Result<WalletDatabase, WalletServiceError> {
    reject_symlink(path)?;
    let wallet = WalletDb::for_path(path, network.parameters(), SystemClock, OsRng)?;
    reject_symlink(path)?;
    secure_wallet_permissions(path)?;
    Ok(wallet)
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
fn secure_wallet_permissions(path: &Path) -> Result<(), WalletServiceError> {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    Ok(())
}

#[cfg(not(unix))]
fn secure_wallet_permissions(_path: &Path) -> Result<(), WalletServiceError> {
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
            created: false,
        });
    }

    let tip = client.latest_block().await?;
    let maximum_birthday = tip
        .height
        .checked_add(1)
        .ok_or(WalletServiceError::HeightOverflow)?;
    let birthday_height =
        requested_birthday.unwrap_or_else(|| tip.height.saturating_sub(99).max(1));
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
    let address = encode_orchard_receiver(&usk.to_unified_full_viewing_key(), network)?;
    Ok(InitializedWallet {
        account_id: account_id.expose_uuid().to_string(),
        birthday_height,
        address,
        created: true,
    })
}

/// Synchronizes compact Ironwood blocks into the persistent SQLite wallet.
pub async fn synchronize_wallet(
    client: &mut AttestedWcashClient,
    path: impl AsRef<Path>,
    network: WalletNetwork,
    batch_size: u32,
) -> Result<WalletBalanceSummary, WalletServiceError> {
    if batch_size == 0 || batch_size > MAX_SYNC_BATCH_SIZE {
        return Err(WalletServiceError::InvalidRequest(format!(
            "sync batch size must be in 1..={MAX_SYNC_BATCH_SIZE}"
        )));
    }
    ensure_client_network(client, network)?;
    let mut wallet = open_wallet_database(path, network)?;
    let account_ids = wallet.get_account_ids().map_err(database_error)?;
    only_account(&account_ids)?;
    let cache = MemoryBlockCache::default();
    let parameters = network.parameters();
    sync::run(
        client.inner_mut(),
        &parameters,
        &cache,
        &mut wallet,
        batch_size,
    )
    .await
    .map_err(|error| WalletServiceError::Synchronization(error.to_string()))?;
    wallet_balance_summary(&wallet, public_confirmation_policy())
}

/// Returns current SQLite wallet balances without making a network request.
pub fn wallet_balance(
    path: impl AsRef<Path>,
    network: WalletNetwork,
) -> Result<WalletBalanceSummary, WalletServiceError> {
    let wallet = open_wallet_database(path, network)?;
    wallet_balance_summary(&wallet, public_confirmation_policy())
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
    let inspected = inspect_signed_transaction(&raw)?;
    if inspected.txid() != txid {
        return Err(WalletServiceError::MissingSignedTransaction);
    }
    Ok(StoredSignedTransaction {
        txid: txid.to_string(),
        raw_transaction_hex: hex::encode(raw),
        branch_id: "b3cfd27e".to_owned(),
        expiry_height: inspected.expiry_height().into(),
    })
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
        let ironwood = balance.ironwood_balance();
        let transparent_total = (balance.unshielded_regular_balance().total()
            + balance.unshielded_coinbase_balance().total())
        .ok_or_else(|| WalletServiceError::Database("transparent balance overflow".to_owned()))?;
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

    let mut wallet = open_wallet_database_with_seed(path, network, master_seed)?;
    let account_ids = wallet.get_account_ids().map_err(database_error)?;
    let account_id = only_account(&account_ids)?;
    let confirmations_policy = ConfirmationsPolicy::new_symmetrical(confirmation_count);
    let summary = wallet
        .get_wallet_summary(confirmations_policy)
        .map_err(database_error)?
        .ok_or(WalletServiceError::NotSynchronized)?;
    if !summary.is_synced() {
        return Err(WalletServiceError::NotSynchronized);
    }

    let payments = recipients
        .iter()
        .map(|recipient| {
            let address = decode_recipient(&recipient.address, network)?;
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
            Payment::new(
                address.to_zcash_address(network.address_network()),
                Some(amount),
                memo,
                None,
                None,
                vec![],
            )
            .map_err(|error| WalletServiceError::InvalidRequest(error.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?;
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
    let expected_chain = match expected_chain_refs(&wallet, target_height, anchor_height) {
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
    if created.len() != 1 {
        return Err(WalletServiceError::MissingSignedTransaction);
    }
    let txid = created[0];
    let transaction = wallet
        .get_transaction(txid)
        .map_err(database_error)?
        .ok_or(WalletServiceError::MissingSignedTransaction)?;
    let mut raw = Vec::new();
    transaction
        .write(&mut raw)
        .map_err(|error| WalletServiceError::Signing(error.to_string()))?;
    let inspected = inspect_signed_transaction(&raw)?;
    if inspected.txid() != txid {
        return Err(WalletServiceError::MissingSignedTransaction);
    }
    let fee_zat = proposal.steps().first().balance().fee_required().into_u64();
    drop(wallet);
    if let Err(error) =
        revalidate_canonical_ancestors(client, expected_chain, expiry_height_u32).await
    {
        return Err(WalletServiceError::StaleAfterSigning {
            txid: txid.to_string(),
            reason: error.to_string(),
        });
    }
    Ok(SignedTransaction {
        txid: txid.to_string(),
        raw_transaction_hex: hex::encode(raw),
        branch_id: "b3cfd27e".to_owned(),
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

fn validate_confirmation_policy(
    network: WalletNetwork,
    confirmations: u32,
    allow_unsafe_regtest_confirmations: bool,
) -> Result<NonZeroU32, WalletServiceError> {
    let confirmations =
        NonZeroU32::new(confirmations).ok_or(WalletServiceError::UnsafeConfirmations)?;
    if confirmations.get() < 100
        && !(network == WalletNetwork::Regtest && allow_unsafe_regtest_confirmations)
    {
        return Err(WalletServiceError::UnsafeConfirmations);
    }
    Ok(confirmations)
}

fn public_confirmation_policy() -> ConfirmationsPolicy {
    ConfirmationsPolicy::new_symmetrical(NonZeroU32::new(100).expect("100 is nonzero"))
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
    target_height: BlockHeight,
    anchor_height: BlockHeight,
) -> Result<(BlockRef, BlockRef), WalletServiceError> {
    let target_height_u32: u32 = target_height.into();
    let expected_tip = target_height_u32
        .checked_sub(1)
        .map(BlockHeight::from_u32)
        .ok_or(WalletServiceError::HeightOverflow)?;
    let expected_tip_u32: u32 = expected_tip.into();
    let stored_tip_hash = wallet
        .get_block_hash(expected_tip)
        .map_err(database_error)?
        .ok_or(WalletServiceError::StaleChain)?;
    let anchor_height_u32: u32 = anchor_height.into();
    let stored_anchor_hash = wallet
        .get_block_hash(anchor_height)
        .map_err(database_error)?
        .ok_or(WalletServiceError::StaleChain)?;
    Ok((
        BlockRef {
            height: expected_tip_u32,
            hash: stored_tip_hash.0,
        },
        BlockRef {
            height: anchor_height_u32,
            hash: stored_anchor_hash.0,
        },
    ))
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

    fn block_ref(height: u32, byte: u8) -> BlockRef {
        BlockRef {
            height,
            hash: [byte; 32],
        }
    }

    #[test]
    fn confirmation_floor_is_public_by_default_and_regtest_override_is_explicit() {
        assert!(validate_confirmation_policy(WalletNetwork::Testnet, 100, false).is_ok());
        assert!(matches!(
            validate_confirmation_policy(WalletNetwork::Testnet, 99, true),
            Err(WalletServiceError::UnsafeConfirmations)
        ));
        assert!(matches!(
            validate_confirmation_policy(WalletNetwork::Regtest, 1, false),
            Err(WalletServiceError::UnsafeConfirmations)
        ));
        assert_eq!(
            validate_confirmation_policy(WalletNetwork::Regtest, 1, true)
                .unwrap()
                .get(),
            1
        );
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
    fn post_signing_error_exposes_recovery_txid() {
        let error = WalletServiceError::StaleAfterSigning {
            txid: "11".repeat(32),
            reason: "test reorg".to_owned(),
        };
        assert!(error.to_string().contains(&"11".repeat(32)));
        assert!(error.to_string().contains("export"));
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
}
