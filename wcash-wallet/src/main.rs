//! Command-line interface for the experimental Wcash wallet.

use std::{
    fs::{self, File, OpenOptions},
    io::{self, IsTerminal, Read, Write},
    path::{Path, PathBuf},
};

use clap::{Parser, Subcommand, ValueEnum};
use secrecy::SecretVec;
use serde::Serialize;
use thiserror::Error;
use wcash_wallet::{
    create_signed_coinbase_shielding, create_signed_transfer, derive_wallet_spending_key,
    encode_orchard_receiver, encode_transparent_coinbase_receiver, initialize_wallet,
    pending_signed_transactions, stored_signed_transaction, synchronize_wallet,
    validate_wcash_address, wallet_balance, AttestedWcashClient, TransferRecipient,
    WalletAddressError, WalletKeyError, WalletNetwork, WalletRpcError, WalletServiceError,
};
use zcash_protocol::TxId;
use zeroize::Zeroizing;

const MAX_SEED_HEX_INPUT: u64 = 506;
const MAX_RAW_TRANSACTION_HEX_INPUT: u64 = 4_000_002;
const MAX_WCASH_ADDRESS_INPUT: u64 = 1_024;

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
            let output = serde_json::json!({ "error": error.to_string() });
            eprintln!("{output}");
            std::process::exit(1);
        }
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
    let encoded = encoded.trim();
    if encoded.is_empty() || encoded.chars().any(char::is_whitespace) {
        return Err(CliError::InvalidHexInput {
            field: "wallet seed",
        });
    }
    decode_hex_value("wallet seed", encoded).map(SecretVec::new)
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
