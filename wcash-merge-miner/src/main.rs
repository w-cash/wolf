//! Command-line entry point for the local Wcash merge-mining harness.

use std::{env, error::Error, net::SocketAddr, time::SystemTime};

use serde_json::json;
use wcash_merge_miner::{protocol::serve_loopback, JobConfig, MinerError, PreparedJob};
use wcash_zcash_aux::Target;

const USAGE: &str = "\
Local Wcash/Zcash AuxPoW harness (development only)\n\n\
Usage:\n\
  wcash-merge-miner job  <child-hash-le> [target-le]\n\
  wcash-merge-miner mine <child-hash-le> [target-le] [max-runs] [start-nonce]\n\
  wcash-merge-miner serve <child-hash-le> [target-le] [127.0.0.1:port]\n\n\
All hashes and targets are exactly 32 bytes encoded in little-endian numeric/raw\n\
consensus order. The default target is ff..ff. The default server address is\n\
127.0.0.1:28237. The synthetic parent template has one coinbase transaction.\n";

fn main() -> Result<(), Box<dyn Error>> {
    let mut arguments = env::args().skip(1);
    let Some(command) = arguments.next() else {
        print!("{USAGE}");
        return Ok(());
    };
    if command == "help" || command == "--help" || command == "-h" {
        print!("{USAGE}");
        return Ok(());
    }

    let child = parse_hash(
        arguments
            .next()
            .ok_or_else(|| MinerError::InvalidRequest("missing child-hash-le".to_string()))?,
        "child-hash-le",
    )?;
    let target = match arguments.next() {
        Some(encoded) => Target::from_le_bytes(parse_hash(encoded, "target-le")?)?,
        None => Target::MAX,
    };
    let timestamp = u32::try_from(
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)?
            .as_secs(),
    )
    .map_err(|_| MinerError::InvalidRequest("current time exceeds u32".to_string()))?;
    let config = JobConfig {
        timestamp,
        ..JobConfig::default()
    };
    let job = PreparedJob::new(child, target, config)?;

    match command.as_str() {
        "job" => {
            ensure_no_more(arguments)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "job_id": job.job_id(),
                    "algorithm": "Equihash(200,9)",
                    "child_block_hash_le": hex::encode(job.child_block_hash()),
                    "target_le": hex::encode(job.required_target().to_le_bytes()),
                    "parent_header_input": hex::encode(job.parent_header_input()),
                    "parent_coinbase": hex::encode(job.coinbase_bytes()),
                    "parent_coinbase_txid_le": hex::encode(job.coinbase_transaction_id()),
                }))?
            );
        }
        "mine" => {
            let max_runs = parse_optional_u64(arguments.next(), 16, "max-runs")?;
            let start_nonce = parse_optional_u64(arguments.next(), 0, "start-nonce")?;
            ensure_no_more(arguments)?;
            let solved = job.solve(start_nonce, max_runs)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "job_id": job.job_id(),
                    "parent_block_hash_le": hex::encode(solved.parent_block_hash_le()),
                    "nonce_le": hex::encode(solved.nonce()),
                    "solution": hex::encode(solved.solution()),
                    "auxpow_proof": hex::encode(solved.encoded_proof()),
                }))?
            );
        }
        "serve" => {
            let bind: SocketAddr = arguments
                .next()
                .unwrap_or_else(|| "127.0.0.1:28237".to_string())
                .parse()?;
            ensure_no_more(arguments)?;
            eprintln!("serving local job {} on {bind}", job.job_id());
            serve_loopback(bind, job)?;
        }
        _ => return Err(MinerError::InvalidRequest(format!("unknown command {command:?}")).into()),
    }

    Ok(())
}

fn parse_hash(encoded: String, field: &'static str) -> Result<[u8; 32], MinerError> {
    let bytes = hex::decode(encoded).map_err(|error| MinerError::InvalidHexField {
        field,
        reason: error.to_string(),
    })?;
    bytes
        .try_into()
        .map_err(|bytes: Vec<u8>| MinerError::InvalidHexField {
            field,
            reason: format!("decoded to {} bytes, expected 32", bytes.len()),
        })
}

fn parse_optional_u64(
    value: Option<String>,
    default: u64,
    field: &'static str,
) -> Result<u64, MinerError> {
    value.map_or(Ok(default), |value| {
        value
            .parse()
            .map_err(|error| MinerError::InvalidNumericField {
                field,
                reason: format!("expected an unsigned integer: {error}"),
            })
    })
}

fn ensure_no_more(mut arguments: impl Iterator<Item = String>) -> Result<(), MinerError> {
    match arguments.next() {
        Some(argument) => Err(MinerError::InvalidRequest(format!(
            "unexpected extra argument {argument:?}"
        ))),
        None => Ok(()),
    }
}
