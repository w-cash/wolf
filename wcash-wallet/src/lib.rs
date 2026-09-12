//! Experimental local wallet for Wcash Testnet and Regtest.
//!
//! This crate deliberately builds on the audited `librustzcash` wallet stack.
//! It adds Wcash chain and address separation at the boundary, while leaving
//! Ironwood scanning, witness maintenance, note locking, and transaction
//! construction to `zcash_client_backend` and `zcash_client_sqlite`.

mod address;
mod cache;
mod identity;
mod keys;
mod network;
mod rpc;
mod sync;
mod wallet;

pub use address::{
    decode_recipient, encode_orchard_receiver, encode_transparent_coinbase_receiver,
    validate_wcash_address, ValidatedWcashAddress, WalletAddressError, WcashReceiverKind,
};
pub use cache::{MemoryBlockCache, MemoryBlockCacheError};
pub use identity::WalletDatabaseIdentityError;
pub use keys::{derive_wallet_seed, derive_wallet_spending_key, WalletKeyError};
pub use network::WalletNetwork;
/// A chain position bound to its canonical internal-byte-order block hash.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BlockRef {
    /// Block height.
    pub height: u32,
    /// Internal-byte-order block hash, matching compact-block protobufs.
    pub hash: [u8; 32],
}
pub use rpc::{
    inspect_signed_transaction, AttestedWcashClient, BroadcastDisposition, BroadcastResult,
    TransactionStatus, WalletRpcError,
};
pub use wallet::{
    broadcast_signed_payout_batch, create_idempotent_payout_batch,
    create_signed_coinbase_shielding, create_signed_transfer, initialize_wallet,
    inspect_signed_payout_batch, inspect_wallet, open_wallet_database, payout_wallet_identity,
    pending_signed_transactions, recover_signed_payout_batch, stored_signed_transaction,
    synchronize_wallet, synchronize_wallet_cancellable, wallet_balance, AccountBalanceSummary,
    InitializedWallet, PayoutBatchInspectionRequest, PayoutBatchLookup, PayoutBatchOutput,
    PayoutBatchRequest, PayoutBroadcastOutcome, PayoutBroadcastResult, PayoutFundSource,
    PayoutWalletIdentity, PendingSignedTransactionPage, SignedPayoutBatch, SignedTransaction,
    StoredSignedTransaction, TransferRecipient, WalletBalanceSummary, WalletDatabase, WalletInfo,
    WalletServiceError, WalletSyncCancellation, COINBASE_SHIELDING_MATURITY,
    MAX_COINBASE_SHIELDING_INPUTS, MAX_EXPIRY_DELTA, MAX_LOCK_FOR_BLOCKS,
    MAX_PENDING_TRANSACTION_PAGE_SIZE, MAX_SYNC_BATCH_SIZE, MAX_TRANSFER_RECIPIENTS,
    PAYOUT_BATCH_FORMAT_VERSION, TRANSPARENT_COINBASE_RECOVERY_START_HEIGHT,
};
