//! Command-line interface for the experimental Wcash wallet.

use std::{
    fs::{self, File, OpenOptions},
    io::{self, IsTerminal, Read, Write},
    path::{Path, PathBuf},
};

use clap::{Parser, Subcommand, ValueEnum};
use secrecy::SecretVec;
use serde::{de::DeserializeOwned, Serialize};
use thiserror::Error;
use wcash_wallet::{
    broadcast_signed_payout_batch, create_idempotent_payout_batch,
    create_signed_coinbase_shielding, create_signed_transfer, derive_wallet_spending_key,
    encode_orchard_receiver, encode_transparent_coinbase_receiver, initialize_wallet,
    inspect_signed_payout_batch, payout_wallet_identity, payout_wallet_observation,
    pending_signed_transactions, recover_signed_payout_batch, stored_signed_transaction,
    synchronize_wallet, validate_wcash_address, wallet_balance, AttestedWcashClient,
    PayoutBatchInspectionRequest, PayoutBatchLookup, PayoutBatchRequest, TransferRecipient,
    WalletAddressError, WalletKeyError, WalletNetwork, WalletRpcError, WalletServiceError,
};
use zcash_protocol::TxId;
use zeroize::Zeroizing;

const MAX_SEED_HEX_INPUT: u64 = 506;
const MAX_RAW_TRANSACTION_HEX_INPUT: u64 = 4_000_002;
const MAX_WCASH_ADDRESS_INPUT: u64 = 1_024;
const MAX_PAYOUT_REQUEST_JSON_INPUT: u64 = 512 * 1_024;
const MAX_PAYOUT_INSPECTION_JSON_INPUT: u64 = 4_500_000;
const MIN_PAYOUT_SEED_BYTES: usize = 32;
const MAX_PAYOUT_SEED_BYTES: usize = 252;
const PAYOUT_SIGN_FRAME_MAGIC: &[u8; 16] = b"WCASHPAYSIGNV1\0\0";
const PAYOUT_SIGN_FRAME_HEADER_BYTES: usize = PAYOUT_SIGN_FRAME_MAGIC.len() + 2 + 4;

#[derive(Debug, Error)]
enum CliError {
    #[error("--db is required for this command")]
    MissingDatabase,
    #[error("--lightwalletd is required for this command")]
    MissingEndpoint,
    #[error("{0} must be piped through stdin; terminal input is refused")]
    TerminalInput(&'static str),
    #[error("could not read {field} from stdin: {source}")]
    Stdin {
        field: &'static str,
        source: io::Error,
    },
    #[error("{field} exceeds the maximum encoded length")]
    InputTooLong { field: &'static str },
    #[error("{field} must be exactly one hexadecimal value")]
    InvalidHexInput { field: &'static str },
    #[error("{field} must be exactly one non-empty value without whitespace")]
    InvalidSingleLineInput { field: &'static str },
    #[error("{field} is not valid hexadecimal: {source}")]
    Hex {
        field: &'static str,
        source: hex::FromHexError,
    },
    #[error("transaction identifier must be exactly 64 hexadecimal digits in display order")]
    InvalidTxId,
    #[error("unsafe private IVK output path: {0}")]
    UnsafePrivateOutput(String),
    #[error("could not {operation} private IVK output {path}: {source}")]
    PrivateOutputIo {
        operation: &'static str,
        path: PathBuf,
        source: io::Error,
    },
    #[error("unsafe private seed input: {0}")]
    UnsafePrivateInput(String),
    #[error("payout signing requires exactly one private seed source")]
    MissingPayoutSeedSource,
    #[error("invalid private payout-sign input frame")]
    InvalidPayoutSignFrame,
    #[error("could not {operation} private seed input {path}: {source}")]
    PrivateInputIo {
        operation: &'static str,
        path: PathBuf,
        source: io::Error,
    },
    #[error(transparent)]
    Address(#[from] WalletAddressError),
    #[error(transparent)]
    Key(#[from] WalletKeyError),
    #[error(transparent)]
    Rpc(#[from] WalletRpcError),
    #[error(transparent)]
    Wallet(#[from] WalletServiceError),
    #[error("could not encode JSON output: {0}")]
    Json(#[from] serde_json::Error),
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum CliNetwork {
    Testnet,
    Regtest,
}

impl From<CliNetwork> for WalletNetwork {
    fn from(value: CliNetwork) -> Self {
        match value {
            CliNetwork::Testnet => Self::Testnet,
            CliNetwork::Regtest => Self::Regtest,
        }
    }
}

/// Experimental one-shot Wcash wallet for an attested Wcash compact-block service.
#[derive(Debug, Parser)]
#[command(name = "wcash-wallet", version, about)]
struct Cli {
    /// Wcash chain identity. Mainnet is intentionally unsupported.
    #[arg(long, value_enum)]
    network: CliNetwork,
    /// Persistent SQLite wallet path, required by wallet database commands.
    #[arg(long, global = true)]
    db: Option<PathBuf>,
    /// HTTPS compact-block endpoint, or plaintext literal loopback for local development.
    #[arg(long, global = true)]
    lightwalletd: Option<String>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Derive the canonical Wcash Unified Address from a hex seed on stdin.
    DeriveAddress {
        /// ZIP 32 account index.
        #[arg(long, default_value_t = 0)]
        account: u32,
    },
    /// Derive collector addresses and exclusively write its read-only Ironwood IVK.
    DeriveCollector {
        /// ZIP 32 account index.
        #[arg(long, default_value_t = 0)]
        account: u32,
        /// New absolute owner-private file for the raw IVK (never printed).
        #[arg(long)]
        ivk_file: PathBuf,
    },
    /// Canonically classify one Wcash address read from standard input.
    ValidateAddress,
    /// Create or verify the single wallet account using a hex seed on stdin.
    Init {
        /// First Wcash block to scan; defaults to a bounded recent birthday.
        #[arg(long)]
        birthday: Option<u32>,
    },
    /// Download, validate, and scan compact blocks into SQLite.
    Sync {
        /// Maximum compact blocks per synchronizer batch.
        #[arg(long, default_value_t = 16, value_parser = clap::value_parser!(u32).range(1..=16))]
        batch_size: u32,
    },
    /// Print the locally stored pool-separated wallet balance.
    Balance,
    /// Shield mature transparent coinbase outputs into this wallet's Ironwood receiver.
    ShieldCoinbase {
        /// Maximum mature coinbase UTXOs to sweep, highest value first.
        #[arg(long, default_value_t = 100, value_parser = clap::value_parser!(u64).range(1..=100))]
        max_inputs: u64,
        /// Number of blocks after the proposal target at which the transaction expires.
        #[arg(long, default_value_t = 40, value_parser = clap::value_parser!(u32).range(1..=100))]
        expiry_delta: u32,
        /// Number of blocks for which selected coinbase outputs remain locked.
        #[arg(long, default_value_t = 100, value_parser = clap::value_parser!(u32).range(1..=1_000))]
        lock_for_blocks: u32,
    },
    /// Create and persist one Ironwood-only V6 transfer using a hex seed on stdin.
    Transfer {
        /// Canonical Wcash Unified Address.
        #[arg(long)]
        recipient: String,
        /// Recipient value in zatoshis (100,000,000 zatoshis = 1 TWC).
        #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
        amount_zat: u64,
        /// Optional raw memo bytes as hexadecimal (maximum 512 bytes).
        #[arg(long)]
        memo_hex: Option<String>,
        /// Required confirmations; public Testnet requires at least 100.
        #[arg(long, default_value_t = 100, value_parser = clap::value_parser!(u32).range(1..))]
        confirmations: u32,
        /// Permit fewer than 100 confirmations on isolated Regtest only.
        #[arg(long)]
        unsafe_regtest_confirmations: bool,
        /// Number of blocks after the proposal target at which the transaction expires.
        #[arg(long, default_value_t = 40, value_parser = clap::value_parser!(u32).range(1..=100))]
        expiry_delta: u32,
        /// Number of blocks for which selected notes remain locked.
        #[arg(long, default_value_t = 100, value_parser = clap::value_parser!(u32).range(1..=1_000))]
        lock_for_blocks: u32,
    },
    /// Recover exact signed bytes previously persisted by transaction identifier.
    Export {
        /// Canonical display-order transaction identifier.
        #[arg(long)]
        txid: String,
    },
    /// List exact bytes for locally-created transactions not currently recorded as mined.
    ListPending {
        /// Opaque cursor returned by the previous page.
        #[arg(long)]
        after_row_id: Option<u64>,
        /// Maximum rows to return in this page.
        #[arg(long, default_value_t = 25, value_parser = clap::value_parser!(u64).range(1..=25))]
        limit: u64,
    },
    /// Broadcast exact signed transaction hex read from stdin.
    Broadcast,
    /// Query the exact transaction identifier from the attested Wcash node.
    Status {
        /// Canonical display-order transaction identifier.
        #[arg(long)]
        txid: String,
    },
    /// Print the seedless identity required by the native Testnet payout protocol.
    PayoutIdentity,
    /// Print a short-lived, tip-attested Testnet collector observation.
    PayoutObserve,
    /// Atomically sign or recover one exact Testnet payout request read from stdin.
    PayoutSign {
        /// Absolute owner-private seed credential file; contents are never printed or logged.
        #[arg(
            long,
            value_name = "PATH",
            required_unless_present = "seed_stdin",
            conflicts_with = "seed_stdin"
        )]
        seed_file: Option<PathBuf>,
        /// Read a bounded binary frame containing the raw seed and payout JSON from stdin.
        #[arg(
            long,
            required_unless_present = "seed_file",
            conflicts_with = "seed_file"
        )]
        seed_stdin: bool,
    },
    /// Recover an exact signed payout using a batch lookup read from stdin.
    PayoutRecover,
    /// Verify caller-held payout bytes against the durable batch read from stdin.
    PayoutInspect,
    /// Broadcast exact caller-held bytes only after matching the durable batch.
    PayoutBroadcast,
}

#[derive(Serialize)]
struct DerivedAddress {
    network: WalletNetwork,
    account: u32,
    address: String,
    transparent_coinbase_address: String,
}

#[derive(Serialize)]
struct DerivedCollector {
    network: WalletNetwork,
    account: u32,
    address: String,
    transparent_coinbase_address: String,
    ivk_file_written: bool,
}

#[derive(Serialize)]
struct StatusOutput {
    txid: String,
    status: wcash_wallet::TransactionStatus,
}

#[tokio::main]
async fn main() {
    match run(Cli::parse()).await {
        Ok(()) => {}
        Err(error) => {
            let output = CliFailure {
                protocol_version: 1,
                code: classify_cli_failure(&error),
                error: error.to_string(),
            };
            if let Ok(encoded) = serde_json::to_string(&output) {
                eprintln!("{encoded}");
            }
            std::process::exit(1);
        }
    }
}

#[derive(Serialize)]
struct CliFailure {
    protocol_version: u32,
    code: CliFailureCode,
    error: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum CliFailureCode {
    Rejected,
    Unavailable,
    IdempotencyConflict,
    Ambiguous,
}

fn classify_cli_failure(error: &CliError) -> CliFailureCode {
    match error {
        CliError::Wallet(WalletServiceError::PayoutBatchConflict { .. }) => {
            CliFailureCode::IdempotencyConflict
        }
        CliError::Wallet(
            WalletServiceError::PersistedPayoutRequiresRecovery { .. }
            | WalletServiceError::IncompletePayoutBatch { .. }
            | WalletServiceError::PersistedTransactionsRequireReview { .. },
        ) => CliFailureCode::Ambiguous,
        CliError::Rpc(_)
        | CliError::Wallet(
            WalletServiceError::Rpc(_)
            | WalletServiceError::Sqlite(_)
            | WalletServiceError::Database(_)
            | WalletServiceError::InvalidSystemClock
            | WalletServiceError::NotSynchronized
            | WalletServiceError::StaleChain
            | WalletServiceError::Synchronization(_)
            | WalletServiceError::SynchronizationCancelled
            | WalletServiceError::TransparentRecoveryIncomplete
            | WalletServiceError::WalletBusy,
        ) => CliFailureCode::Unavailable,
        _ => CliFailureCode::Rejected,
    }
}

async fn run(cli: Cli) -> Result<(), CliError> {
    let network = WalletNetwork::from(cli.network);
    match cli.command {
        Command::DeriveAddress { account } => {
            let seed = read_seed()?;
            let spending_key = derive_wallet_spending_key(&seed, network, account)?;
            let viewing_key = spending_key.to_unified_full_viewing_key();
            print_json(&DerivedAddress {
                network,
                account,
                address: encode_orchard_receiver(&viewing_key, network)?,
                transparent_coinbase_address: encode_transparent_coinbase_receiver(
                    &viewing_key,
                    network,
                )?,
            })
        }
        Command::DeriveCollector { account, ivk_file } => {
            let seed = read_seed()?;
            let spending_key = derive_wallet_spending_key(&seed, network, account)?;
            let viewing_key = spending_key.to_unified_full_viewing_key();
            let orchard = viewing_key.orchard().ok_or_else(|| {
                WalletAddressError::Derivation(
                    "unified full viewing key has no Ironwood component".to_owned(),
                )
            })?;
            let address = encode_orchard_receiver(&viewing_key, network)?;
            let transparent_coinbase_address =
                encode_transparent_coinbase_receiver(&viewing_key, network)?;
            write_private_ivk_file(&ivk_file, orchard.to_ivk(zip32::Scope::External).to_bytes())?;
            print_json(&DerivedCollector {
                network,
                account,
                address,
                transparent_coinbase_address,
                ivk_file_written: true,
            })
        }
        Command::ValidateAddress => {
            let address = read_address()?;
            print_json(&validate_wcash_address(&address, network)?)
        }
        Command::Init { birthday } => {
            let mut client = connect_required(&cli.lightwalletd, network).await?;
            let seed = read_seed()?;
            let result = initialize_wallet(
                &mut client,
                required_database(&cli.db)?,
                network,
                &seed,
                birthday,
            )
            .await?;
            print_json(&result)
        }
        Command::Sync { batch_size } => {
            let mut client = connect_required(&cli.lightwalletd, network).await?;
            let result = synchronize_wallet(
                &mut client,
                required_database(&cli.db)?,
                network,
                batch_size,
            )
            .await?;
            print_json(&result)
        }
        Command::Balance => print_json(&wallet_balance(required_database(&cli.db)?, network)?),
        Command::ShieldCoinbase {
            max_inputs,
            expiry_delta,
            lock_for_blocks,
        } => {
            let mut client = connect_required(&cli.lightwalletd, network).await?;
            let seed = read_seed()?;
            let result = create_signed_coinbase_shielding(
                &mut client,
                required_database(&cli.db)?,
                network,
                &seed,
                usize::try_from(max_inputs).map_err(|_| {
                    WalletServiceError::InvalidRequest(
                        "coinbase input limit is not representable".to_owned(),
                    )
                })?,
                expiry_delta,
                lock_for_blocks,
            )
            .await?;
            print_json(&result)
        }
        Command::Transfer {
            recipient,
            amount_zat,
            memo_hex,
            confirmations,
            unsafe_regtest_confirmations,
            expiry_delta,
            lock_for_blocks,
        } => {
            let memo = memo_hex
                .map(|memo| decode_hex_value("memo", &memo))
                .transpose()?;
            let mut client = connect_required(&cli.lightwalletd, network).await?;
            let seed = read_seed()?;
            let result = create_signed_transfer(
                &mut client,
                required_database(&cli.db)?,
                network,
                &seed,
                vec![TransferRecipient {
                    address: recipient,
                    amount_zat,
                    memo: memo.unwrap_or_default(),
                }],
                confirmations,
                unsafe_regtest_confirmations,
                expiry_delta,
                lock_for_blocks,
            )
            .await?;
            print_json(&result)
        }
        Command::Export { txid } => print_json(&stored_signed_transaction(
            required_database(&cli.db)?,
            network,
            parse_txid(&txid)?,
        )?),
        Command::ListPending {
            after_row_id,
            limit,
        } => print_json(&pending_signed_transactions(
            required_database(&cli.db)?,
            network,
            after_row_id,
            usize::try_from(limit).map_err(|_| {
                WalletServiceError::InvalidRequest(
                    "pending transaction page size is not representable".to_owned(),
                )
            })?,
        )?),
        Command::Broadcast => {
            let raw = read_hex_stdin("signed transaction", MAX_RAW_TRANSACTION_HEX_INPUT)?;
            let mut client = connect_required(&cli.lightwalletd, network).await?;
            print_json(&client.broadcast_raw_transaction(raw).await?)
        }
        Command::Status { txid } => {
            let txid = parse_txid(&txid)?;
            let mut client = connect_required(&cli.lightwalletd, network).await?;
            let status = client.transaction_status(txid).await?;
            print_json(&StatusOutput {
                txid: txid.to_string(),
                status,
            })
        }
        Command::PayoutIdentity => print_json(&payout_wallet_identity(
            required_database(&cli.db)?,
            network,
        )?),
        Command::PayoutObserve => {
            let mut client = connect_required(&cli.lightwalletd, network).await?;
            print_json(
                &payout_wallet_observation(&mut client, required_database(&cli.db)?, network)
                    .await?,
            )
        }
        Command::PayoutSign {
            seed_file,
            seed_stdin,
        } => {
            let (request, seed) = if seed_stdin {
                read_payout_sign_frame()?
            } else {
                let request = read_json_stdin("payout request", MAX_PAYOUT_REQUEST_JSON_INPUT)?;
                let path = seed_file
                    .as_deref()
                    .ok_or(CliError::MissingPayoutSeedSource)?;
                (request, read_seed_file(path)?)
            };
            let mut client = connect_required(&cli.lightwalletd, network).await?;
            print_json(
                &create_idempotent_payout_batch(
                    &mut client,
                    required_database(&cli.db)?,
                    network,
                    &seed,
                    request,
                )
                .await?,
            )
        }
        Command::PayoutRecover => {
            let lookup: PayoutBatchLookup =
                read_json_stdin("payout lookup", MAX_PAYOUT_REQUEST_JSON_INPUT)?;
            print_json(&recover_signed_payout_batch(
                required_database(&cli.db)?,
                network,
                &lookup,
            )?)
        }
        Command::PayoutInspect => {
            let request: PayoutBatchInspectionRequest =
                read_json_stdin("payout inspection", MAX_PAYOUT_INSPECTION_JSON_INPUT)?;
            print_json(&inspect_signed_payout_batch(
                required_database(&cli.db)?,
                network,
                &request,
            )?)
        }
        Command::PayoutBroadcast => {
            let request: PayoutBatchInspectionRequest =
                read_json_stdin("payout broadcast", MAX_PAYOUT_INSPECTION_JSON_INPUT)?;
            let mut client = connect_required(&cli.lightwalletd, network).await?;
            print_json(
                &broadcast_signed_payout_batch(
                    &mut client,
                    required_database(&cli.db)?,
                    network,
                    &request,
                )
                .await?,
            )
        }
    }
}

fn required_database(path: &Option<PathBuf>) -> Result<&Path, CliError> {
    path.as_deref().ok_or(CliError::MissingDatabase)
}

async fn connect_required(
    endpoint: &Option<String>,
    network: WalletNetwork,
) -> Result<AttestedWcashClient, CliError> {
    let endpoint = endpoint.as_deref().ok_or(CliError::MissingEndpoint)?;
    Ok(AttestedWcashClient::connect(endpoint, network).await?)
}

fn read_seed() -> Result<SecretVec<u8>, CliError> {
    let stdin = io::stdin();
    if stdin.is_terminal() {
        return Err(CliError::TerminalInput("wallet seed"));
    }
    let mut encoded = Zeroizing::new(String::new());
    stdin
        .take(MAX_SEED_HEX_INPUT.saturating_add(1))
        .read_to_string(&mut encoded)
        .map_err(|source| CliError::Stdin {
            field: "wallet seed",
            source,
        })?;
    if u64::try_from(encoded.len()).unwrap_or(u64::MAX) > MAX_SEED_HEX_INPUT {
        return Err(CliError::InputTooLong {
            field: "wallet seed",
        });
    }
    parse_seed_input(&encoded)
}

fn parse_seed_input(encoded: &str) -> Result<SecretVec<u8>, CliError> {
    let encoded = encoded.trim();
    if encoded.is_empty() || encoded.chars().any(char::is_whitespace) {
        return Err(CliError::InvalidHexInput {
            field: "wallet seed",
        });
    }
    decode_hex_value("wallet seed", encoded).map(SecretVec::new)
}

#[cfg(unix)]
fn read_seed_file(path: &Path) -> Result<SecretVec<u8>, CliError> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};

    if !path.is_absolute()
        || path.file_name().is_none()
        || path.components().any(|component| {
            matches!(
                component,
                std::path::Component::CurDir | std::path::Component::ParentDir
            )
        })
        || PathBuf::from_iter(path.components()) != path
    {
        return Err(CliError::UnsafePrivateInput(
            "path must be absolute and lexically canonical".to_owned(),
        ));
    }
    let named =
        fs::symlink_metadata(path).map_err(|source| private_input_io("inspect", path, source))?;
    let mut options = OpenOptions::new();
    options
        .read(true)
        .custom_flags(nix::libc::O_NOFOLLOW | nix::libc::O_CLOEXEC);
    let mut file = options
        .open(path)
        .map_err(|source| private_input_io("open", path, source))?;
    let opened = file
        .metadata()
        .map_err(|source| private_input_io("inspect open", path, source))?;
    if named.file_type().is_symlink()
        || !named.is_file()
        || !opened.is_file()
        || opened.uid() != nix::unistd::geteuid().as_raw()
        || opened.permissions().mode() & 0o077 != 0
        || opened.nlink() != 1
        || opened.dev() != named.dev()
        || opened.ino() != named.ino()
    {
        return Err(CliError::UnsafePrivateInput(
            "seed credential must be one owner-private regular file with exactly one hard link"
                .to_owned(),
        ));
    }
    let mut encoded = Zeroizing::new(String::new());
    (&mut file)
        .take(MAX_SEED_HEX_INPUT.saturating_add(1))
        .read_to_string(&mut encoded)
        .map_err(|source| private_input_io("read", path, source))?;
    if u64::try_from(encoded.len()).unwrap_or(u64::MAX) > MAX_SEED_HEX_INPUT {
        return Err(CliError::InputTooLong {
            field: "wallet seed",
        });
    }
    let after = file
        .metadata()
        .map_err(|source| private_input_io("reinspect open", path, source))?;
    let renamed = fs::symlink_metadata(path)
        .map_err(|source| private_input_io("reinspect named", path, source))?;
    if after.dev() != opened.dev()
        || after.ino() != opened.ino()
        || renamed.dev() != opened.dev()
        || renamed.ino() != opened.ino()
    {
        return Err(CliError::UnsafePrivateInput(
            "seed credential pathname changed while it was read".to_owned(),
        ));
    }
    parse_seed_input(&encoded)
}

#[cfg(not(unix))]
fn read_seed_file(_path: &Path) -> Result<SecretVec<u8>, CliError> {
    Err(CliError::UnsafePrivateInput(
        "private seed credentials are currently supported only on Unix".to_owned(),
    ))
}

fn private_input_io(operation: &'static str, path: &Path, source: io::Error) -> CliError {
    CliError::PrivateInputIo {
        operation,
        path: path.to_path_buf(),
        source,
    }
}

fn read_json_stdin<T: DeserializeOwned>(
    field: &'static str,
    maximum_bytes: u64,
) -> Result<T, CliError> {
    let stdin = io::stdin();
    if stdin.is_terminal() {
        return Err(CliError::TerminalInput(field));
    }
    let mut encoded = String::new();
    stdin
        .take(maximum_bytes.saturating_add(1))
        .read_to_string(&mut encoded)
        .map_err(|source| CliError::Stdin { field, source })?;
    if u64::try_from(encoded.len()).unwrap_or(u64::MAX) > maximum_bytes {
        return Err(CliError::InputTooLong { field });
    }
    parse_json_input(field, &encoded, maximum_bytes)
}

/// Reads the private pool signer protocol without placing seed bytes in JSON,
/// command-line arguments, environment variables, or persistent files.
///
/// The frame is `magic[16] || seed_len_be[2] || json_len_be[4] || seed || json`.
/// Standard input must close after the exact frame so truncation and trailing
/// content both fail closed.
fn read_payout_sign_frame() -> Result<(PayoutBatchRequest, SecretVec<u8>), CliError> {
    let stdin = io::stdin();
    if stdin.is_terminal() {
        return Err(CliError::TerminalInput("private payout-sign frame"));
    }
    parse_payout_sign_frame(stdin.lock())
}

fn parse_payout_sign_frame(
    reader: impl Read,
) -> Result<(PayoutBatchRequest, SecretVec<u8>), CliError> {
    let maximum = PAYOUT_SIGN_FRAME_HEADER_BYTES
        .saturating_add(MAX_PAYOUT_SEED_BYTES)
        .saturating_add(MAX_PAYOUT_REQUEST_JSON_INPUT as usize);
    let mut frame = Zeroizing::new(Vec::new());
    reader
        .take(u64::try_from(maximum).unwrap_or(u64::MAX).saturating_add(1))
        .read_to_end(&mut frame)
        .map_err(|source| CliError::Stdin {
            field: "private payout-sign frame",
            source,
        })?;
    if frame.len() > maximum || frame.len() < PAYOUT_SIGN_FRAME_HEADER_BYTES {
        return Err(CliError::InvalidPayoutSignFrame);
    }
    if frame.get(..PAYOUT_SIGN_FRAME_MAGIC.len()) != Some(PAYOUT_SIGN_FRAME_MAGIC) {
        return Err(CliError::InvalidPayoutSignFrame);
    }

    let seed_length = u16::from_be_bytes([
        frame[PAYOUT_SIGN_FRAME_MAGIC.len()],
        frame[PAYOUT_SIGN_FRAME_MAGIC.len() + 1],
    ]) as usize;
    let json_offset = PAYOUT_SIGN_FRAME_MAGIC.len() + 2;
    let json_length = u32::from_be_bytes([
        frame[json_offset],
        frame[json_offset + 1],
        frame[json_offset + 2],
        frame[json_offset + 3],
    ]) as usize;
    if !(MIN_PAYOUT_SEED_BYTES..=MAX_PAYOUT_SEED_BYTES).contains(&seed_length)
        || json_length == 0
        || json_length > MAX_PAYOUT_REQUEST_JSON_INPUT as usize
    {
        return Err(CliError::InvalidPayoutSignFrame);
    }
    let seed_start = PAYOUT_SIGN_FRAME_HEADER_BYTES;
    let json_start = seed_start
        .checked_add(seed_length)
        .ok_or(CliError::InvalidPayoutSignFrame)?;
    let frame_end = json_start
        .checked_add(json_length)
        .ok_or(CliError::InvalidPayoutSignFrame)?;
    if frame_end != frame.len() {
        return Err(CliError::InvalidPayoutSignFrame);
    }

    let request = serde_json::from_slice(&frame[json_start..frame_end])?;
    let seed = SecretVec::new(frame[seed_start..json_start].to_vec());
    Ok((request, seed))
}

fn parse_json_input<T: DeserializeOwned>(
    field: &'static str,
    encoded: &str,
    maximum_bytes: u64,
) -> Result<T, CliError> {
    if u64::try_from(encoded.len()).unwrap_or(u64::MAX) > maximum_bytes {
        return Err(CliError::InputTooLong { field });
    }
    Ok(serde_json::from_str(encoded)?)
}

fn read_address() -> Result<String, CliError> {
    let stdin = io::stdin();
    if stdin.is_terminal() {
        return Err(CliError::TerminalInput("Wcash address"));
    }
    let mut encoded = String::new();
    stdin
        .take(MAX_WCASH_ADDRESS_INPUT.saturating_add(1))
        .read_to_string(&mut encoded)
        .map_err(|source| CliError::Stdin {
            field: "Wcash address",
            source,
        })?;
    parse_address_input(encoded)
}

fn parse_address_input(encoded: String) -> Result<String, CliError> {
    if u64::try_from(encoded.len()).unwrap_or(u64::MAX) > MAX_WCASH_ADDRESS_INPUT {
        return Err(CliError::InputTooLong {
            field: "Wcash address",
        });
    }
    let encoded = encoded.trim();
    if encoded.is_empty() || encoded.chars().any(char::is_whitespace) {
        return Err(CliError::InvalidSingleLineInput {
            field: "Wcash address",
        });
    }
    Ok(encoded.to_owned())
}

#[cfg(unix)]
fn write_private_ivk_file(path: &Path, ivk: [u8; 64]) -> Result<(), CliError> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};

    let ivk = Zeroizing::new(ivk);
    if !path.is_absolute()
        || path.file_name().is_none()
        || path.components().any(|component| {
            matches!(
                component,
                std::path::Component::CurDir | std::path::Component::ParentDir
            )
        })
        || PathBuf::from_iter(path.components()) != path
    {
        return Err(CliError::UnsafePrivateOutput(
            "path must be absolute and lexically canonical".to_owned(),
        ));
    }
    let parent = path.parent().ok_or_else(|| {
        CliError::UnsafePrivateOutput("path must have a parent directory".to_owned())
    })?;
    let parent_metadata = fs::symlink_metadata(parent)
        .map_err(|source| private_output_io("inspect parent of", path, source))?;
    let canonical_parent = fs::canonicalize(parent)
        .map_err(|source| private_output_io("canonicalize parent of", path, source))?;
    if !parent_metadata.is_dir()
        || parent_metadata.file_type().is_symlink()
        || canonical_parent != parent
        || parent_metadata.uid() != nix::unistd::geteuid().as_raw()
        || parent_metadata.permissions().mode() & 0o022 != 0
    {
        return Err(CliError::UnsafePrivateOutput(
            "parent must be an owner-controlled canonical directory without group/other write access"
                .to_owned(),
        ));
    }
    match fs::symlink_metadata(path) {
        Ok(_) => {
            return Err(CliError::UnsafePrivateOutput(
                "output already exists; replacement is forbidden".to_owned(),
            ))
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(source) => return Err(private_output_io("inspect", path, source)),
    }

    let mut options = OpenOptions::new();
    options.write(true).create_new(true).mode(0o600);
    let mut file = options
        .open(path)
        .map_err(|source| private_output_io("create", path, source))?;
    validate_private_ivk_output(&file, path)?;
    let mut encoded = Zeroizing::new(hex::encode(ivk.as_slice()));
    encoded.push('\n');
    file.write_all(encoded.as_bytes())
        .map_err(|source| private_output_io("write", path, source))?;
    file.sync_all()
        .map_err(|source| private_output_io("synchronize", path, source))?;
    validate_private_ivk_output(&file, path)?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| private_output_io("synchronize parent of", path, source))?;
    validate_private_ivk_output(&file, path)
}

#[cfg(unix)]
fn validate_private_ivk_output(file: &File, path: &Path) -> Result<(), CliError> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let opened = file
        .metadata()
        .map_err(|source| private_output_io("inspect open", path, source))?;
    let named = fs::symlink_metadata(path)
        .map_err(|source| private_output_io("inspect named", path, source))?;
    if !opened.is_file()
        || named.file_type().is_symlink()
        || !named.is_file()
        || opened.uid() != nix::unistd::geteuid().as_raw()
        || opened.permissions().mode() & 0o077 != 0
        || opened.nlink() != 1
        || opened.dev() != named.dev()
        || opened.ino() != named.ino()
    {
        return Err(CliError::UnsafePrivateOutput(
            "output must remain one owner-private regular file with exactly one hard link"
                .to_owned(),
        ));
    }
    Ok(())
}

#[cfg(not(unix))]
fn write_private_ivk_file(_path: &Path, ivk: [u8; 64]) -> Result<(), CliError> {
    drop(Zeroizing::new(ivk));
    Err(CliError::UnsafePrivateOutput(
        "private IVK file creation is currently supported only on Unix".to_owned(),
    ))
}

fn private_output_io(operation: &'static str, path: &Path, source: io::Error) -> CliError {
    CliError::PrivateOutputIo {
        operation,
        path: path.to_path_buf(),
        source,
    }
}

fn read_hex_stdin(field: &'static str, maximum_bytes: u64) -> Result<Vec<u8>, CliError> {
    let stdin = io::stdin();
    if stdin.is_terminal() {
        return Err(CliError::TerminalInput(field));
    }
    let mut encoded = String::new();
    stdin
        .take(maximum_bytes.saturating_add(1))
        .read_to_string(&mut encoded)
        .map_err(|source| CliError::Stdin { field, source })?;
    if u64::try_from(encoded.len()).unwrap_or(u64::MAX) > maximum_bytes {
        return Err(CliError::InputTooLong { field });
    }
    let encoded = encoded.trim();
    if encoded.is_empty() || encoded.chars().any(char::is_whitespace) {
        return Err(CliError::InvalidHexInput { field });
    }
    decode_hex_value(field, encoded)
}

fn decode_hex_value(field: &'static str, encoded: &str) -> Result<Vec<u8>, CliError> {
    hex::decode(encoded).map_err(|source| CliError::Hex { field, source })
}

fn parse_txid(encoded: &str) -> Result<TxId, CliError> {
    TxId::from_hex(encoded).ok_or(CliError::InvalidTxId)
}

fn print_json(value: &impl Serialize) -> Result<(), CliError> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[test]
    fn txid_parser_uses_canonical_display_order() {
        let txid = TxId::from_bytes([7; 32]);
        assert_eq!(parse_txid(&txid.to_string()).unwrap(), txid);
        assert!(matches!(parse_txid("07"), Err(CliError::InvalidTxId)));
    }

    #[test]
    fn command_line_requires_explicit_network() {
        assert!(Cli::try_parse_from(["wcash-wallet", "balance"]).is_err());
        assert!(Cli::try_parse_from([
            "wcash-wallet",
            "--network",
            "regtest",
            "--db",
            "wallet.sqlite",
            "balance",
        ])
        .is_ok());
        assert!(
            Cli::try_parse_from(["wcash-wallet", "--network", "testnet", "validate-address",])
                .is_ok()
        );
        assert!(Cli::try_parse_from([
            "wcash-wallet",
            "--network",
            "testnet",
            "derive-collector",
            "--ivk-file",
            "/run/credentials/wcash-collector.ivk",
        ])
        .is_ok());
        assert!(Cli::try_parse_from([
            "wcash-wallet",
            "--network",
            "testnet",
            "--db",
            "/var/lib/wcash/wallet.sqlite",
            "--lightwalletd",
            "http://127.0.0.1:38234",
            "payout-observe",
        ])
        .is_ok());
    }

    #[test]
    fn transient_observation_failures_are_machine_classified_as_unavailable() {
        for error in [
            WalletServiceError::NotSynchronized,
            WalletServiceError::StaleChain,
            WalletServiceError::TransparentRecoveryIncomplete,
            WalletServiceError::WalletBusy,
            WalletServiceError::InvalidSystemClock,
        ] {
            assert_eq!(
                classify_cli_failure(&CliError::Wallet(error)),
                CliFailureCode::Unavailable
            );
        }
    }

    #[derive(Debug, Deserialize, Eq, PartialEq)]
    #[serde(deny_unknown_fields)]
    struct BoundedInput {
        value: u32,
    }

    #[test]
    fn payout_json_parser_is_bounded_and_rejects_unknown_fields() {
        assert_eq!(
            parse_json_input::<BoundedInput>("payout request", r#"{"value":7}"#, 32).unwrap(),
            BoundedInput { value: 7 }
        );
        assert!(matches!(
            parse_json_input::<BoundedInput>("payout request", r#"{"value":7}"#, 4),
            Err(CliError::InputTooLong { .. })
        ));
        assert!(matches!(
            parse_json_input::<BoundedInput>("payout request", r#"{"value":7,"ignored":true}"#, 64,),
            Err(CliError::Json(_))
        ));
    }

    fn payout_request_json() -> String {
        serde_json::json!({
            "batch_id": "10000000-0000-4000-8000-000000000001",
            "request_commitment": "11".repeat(32),
            "identity": {
                "protocol_version": 2,
                "network": "testnet",
                "genesis_hash": "22".repeat(32),
                "branch_id": "b3cfd27e",
                "account_id": "20000000-0000-4000-8000-000000000002",
                "collector_payout_commitment": "33".repeat(32),
                "fund_source": "ironwood",
                "synchronized": true
            },
            "outputs": [{
                "allocation_id": "30000000-0000-4000-8000-000000000003",
                "canonical_address": "wutest1privatefixture",
                "receiver_kind": "ironwood",
                "amount_zat": 50_000,
                "memo_hex": ""
            }],
            "confirmations": 100,
            "max_fee_zat": 10_000
        })
        .to_string()
    }

    fn payout_sign_frame(seed: &[u8], json: &[u8]) -> Vec<u8> {
        let mut frame = Vec::new();
        frame.extend_from_slice(PAYOUT_SIGN_FRAME_MAGIC);
        frame.extend_from_slice(&(seed.len() as u16).to_be_bytes());
        frame.extend_from_slice(&(json.len() as u32).to_be_bytes());
        frame.extend_from_slice(seed);
        frame.extend_from_slice(json);
        frame
    }

    #[test]
    fn payout_sign_cli_requires_exactly_one_private_seed_source() {
        let common = [
            "wcash-wallet",
            "--network",
            "testnet",
            "--db",
            "/var/lib/wcash/wallet.sqlite",
            "--lightwalletd",
            "http://127.0.0.1:38234",
            "payout-sign",
        ];
        let mut framed = common.to_vec();
        framed.push("--seed-stdin");
        assert!(Cli::try_parse_from(framed).is_ok());

        let mut protected = common.to_vec();
        protected.extend(["--seed-file", "/run/credentials/wcash.seed"]);
        assert!(Cli::try_parse_from(protected).is_ok());

        assert!(Cli::try_parse_from(common).is_err());
        let mut both = common.to_vec();
        both.extend(["--seed-stdin", "--seed-file", "/run/credentials/wcash.seed"]);
        assert!(Cli::try_parse_from(both).is_err());
    }

    #[test]
    fn payout_sign_frame_keeps_seed_out_of_json_and_rejects_malleation() {
        use secrecy::ExposeSecret;

        let seed = [0x5a; 32];
        let json = payout_request_json();
        assert!(!json.as_bytes().windows(seed.len()).any(|part| part == seed));
        let frame = payout_sign_frame(&seed, json.as_bytes());
        let (request, parsed_seed) = parse_payout_sign_frame(frame.as_slice()).unwrap();
        assert_eq!(parsed_seed.expose_secret(), &seed);
        assert_eq!(request.batch_id, "10000000-0000-4000-8000-000000000001");

        let mut trailing = frame.clone();
        trailing.push(0);
        assert!(matches!(
            parse_payout_sign_frame(trailing.as_slice()),
            Err(CliError::InvalidPayoutSignFrame)
        ));
        assert!(matches!(
            parse_payout_sign_frame(&frame[..frame.len() - 1]),
            Err(CliError::InvalidPayoutSignFrame)
        ));

        let mut bad_magic = frame;
        bad_magic[0] ^= 0xff;
        assert!(matches!(
            parse_payout_sign_frame(bad_magic.as_slice()),
            Err(CliError::InvalidPayoutSignFrame)
        ));
    }

    #[test]
    fn payout_sign_frame_rejects_unsafe_seed_lengths_before_json_decode() {
        let json = payout_request_json();
        for seed in [vec![0x41; 31], vec![0x42; 253]] {
            assert!(matches!(
                parse_payout_sign_frame(payout_sign_frame(&seed, json.as_bytes()).as_slice()),
                Err(CliError::InvalidPayoutSignFrame)
            ));
        }
    }

    #[cfg(unix)]
    #[test]
    fn payout_seed_file_requires_one_owner_private_regular_inode() {
        use std::os::unix::fs::{symlink, PermissionsExt};

        let directory = tempfile::tempdir().unwrap();
        let seed_path = directory.path().join("seed");
        fs::write(&seed_path, format!("{}\n", "35".repeat(32))).unwrap();
        fs::set_permissions(&seed_path, fs::Permissions::from_mode(0o400)).unwrap();
        assert_eq!(
            secrecy::ExposeSecret::expose_secret(&read_seed_file(&seed_path).unwrap()),
            &vec![0x35; 32]
        );

        fs::set_permissions(&seed_path, fs::Permissions::from_mode(0o640)).unwrap();
        assert!(matches!(
            read_seed_file(&seed_path),
            Err(CliError::UnsafePrivateInput(_))
        ));
        fs::set_permissions(&seed_path, fs::Permissions::from_mode(0o600)).unwrap();
        let link_path = directory.path().join("seed-link");
        symlink(&seed_path, &link_path).unwrap();
        assert!(matches!(
            read_seed_file(&link_path),
            Err(CliError::UnsafePrivateInput(_)) | Err(CliError::PrivateInputIo { .. })
        ));
        let hardlink_path = directory.path().join("seed-hardlink");
        fs::hard_link(&seed_path, &hardlink_path).unwrap();
        assert!(matches!(
            read_seed_file(&seed_path),
            Err(CliError::UnsafePrivateInput(_))
        ));
    }

    #[test]
    fn address_input_is_bounded_and_exactly_one_value() {
        assert_eq!(
            parse_address_input("wutest1fixture\n".to_owned()).unwrap(),
            "wutest1fixture"
        );
        assert!(matches!(
            parse_address_input("a".repeat(MAX_WCASH_ADDRESS_INPUT as usize + 1)),
            Err(CliError::InputTooLong {
                field: "Wcash address"
            })
        ));
        assert!(matches!(
            parse_address_input("first second".to_owned()),
            Err(CliError::InvalidSingleLineInput {
                field: "Wcash address"
            })
        ));
    }

    #[cfg(unix)]
    #[test]
    fn collector_ivk_is_exclusively_written_to_an_owner_private_file() {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        let temporary_root = fs::canonicalize(std::env::temp_dir()).unwrap();
        let directory = tempfile::Builder::new().tempdir_in(temporary_root).unwrap();
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let path = fs::canonicalize(directory.path())
            .unwrap()
            .join("collector.ivk");
        let spending_key =
            derive_wallet_spending_key(&SecretVec::new(vec![0x42; 32]), WalletNetwork::Testnet, 0)
                .unwrap();
        let viewing_key = spending_key.to_unified_full_viewing_key();
        let expected = viewing_key
            .orchard()
            .expect("derived Wcash keys always have an Ironwood component")
            .to_ivk(zip32::Scope::External)
            .to_bytes();

        write_private_ivk_file(&path, expected).unwrap();
        let metadata = fs::symlink_metadata(&path).unwrap();
        assert!(metadata.is_file());
        assert_eq!(metadata.uid(), nix::unistd::geteuid().as_raw());
        assert_eq!(metadata.nlink(), 1);
        assert_eq!(metadata.permissions().mode() & 0o077, 0);
        assert_eq!(
            hex::decode(fs::read_to_string(&path).unwrap().trim()).unwrap(),
            expected
        );
        assert!(matches!(
            write_private_ivk_file(&path, [0x24; 64]),
            Err(CliError::UnsafePrivateOutput(_))
        ));
    }

    #[test]
    fn transfer_cli_caps_resource_arguments() {
        assert!(Cli::try_parse_from([
            "wcash-wallet",
            "--network",
            "regtest",
            "transfer",
            "--recipient",
            "wutest1invalid",
            "--amount-zat",
            "1",
            "--expiry-delta",
            "101",
        ])
        .is_err());

        assert!(Cli::try_parse_from([
            "wcash-wallet",
            "--network",
            "regtest",
            "shield-coinbase",
            "--max-inputs",
            "101",
        ])
        .is_err());

        assert!(Cli::try_parse_from([
            "wcash-wallet",
            "--network",
            "regtest",
            "sync",
            "--batch-size",
            "17",
        ])
        .is_err());
    }
}
