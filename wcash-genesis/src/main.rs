//! Offline Wcash genesis-anchor inspection and local-regtest derivation tool.

use std::{env, error::Error, fmt::Write as _, process};

use wcash_genesis::{BitcoinHeader, RegtestAnchorOverride};

fn main() {
    if let Err(error) = run() {
        eprintln!("error: {error}");
        process::exit(2);
    }
}

fn run() -> Result<(), Box<dyn Error>> {
    let mut arguments = env::args().skip(1);
    let command = arguments
        .next()
        .unwrap_or_else(|| "show-regtest".to_owned());

    match command.as_str() {
        "show-testnet" => {
            reject_extra_arguments(arguments)?;
            print_anchor(wcash_genesis::TESTNET_ANCHOR);
        }
        "show-regtest" => {
            reject_extra_arguments(arguments)?;
            print_anchor(wcash_genesis::REGTEST_ANCHOR);
        }
        "derive-regtest" => {
            let height = arguments
                .next()
                .ok_or("derive-regtest requires HEIGHT and HEADER_HEX")?
                .parse::<u32>()?;
            let header = arguments
                .next()
                .ok_or("derive-regtest requires HEIGHT and HEADER_HEX")?
                .parse::<BitcoinHeader>()?;
            reject_extra_arguments(arguments)?;

            let anchor = RegtestAnchorOverride::from_header(height, header)?.anchor();
            print_anchor(anchor);
        }
        "status" => {
            reject_extra_arguments(arguments)?;
            println!("project: Wcash (WCASH)");
            println!(
                "mainnet: disabled; designated Bitcoin anchor height {}",
                wcash_genesis::DESIGNATED_MAINNET_BITCOIN_HEIGHT
            );
            println!(
                "testnet: enabled for mining interoperability; Bitcoin block {} anchor (verified at Bitcoin height {})",
                wcash_genesis::PUBLIC_TESTNET_BITCOIN_HEIGHT,
                wcash_genesis::PUBLIC_TESTNET_VERIFICATION_HEIGHT,
            );
            println!(
                "regtest: enabled; frozen Bitcoin block {} anchor",
                wcash_genesis::LOCAL_REGTEST_BITCOIN_HEIGHT
            );
        }
        _ => return Err(usage().into()),
    }

    Ok(())
}

fn reject_extra_arguments(mut arguments: impl Iterator<Item = String>) -> Result<(), &'static str> {
    if arguments.next().is_some() {
        Err("unexpected extra argument")
    } else {
        Ok(())
    }
}

fn print_anchor(anchor: wcash_genesis::BitcoinAnchor) {
    println!("network: {}", anchor.wcash_network());
    println!("bitcoin_height: {}", anchor.bitcoin_height());
    println!("bitcoin_hash: {}", anchor.bitcoin_block_hash());
    println!("statement: {}", anchor.genesis_statement());
    println!("encoding: {}", encode_hex(anchor.encode()));
    println!("commitment: {}", encode_hex(anchor.commitment()));
}

fn encode_hex(bytes: impl IntoIterator<Item = u8>) -> String {
    bytes.into_iter().fold(String::new(), |mut output, byte| {
        write!(&mut output, "{byte:02x}")
            .expect("writing hexadecimal bytes to a String cannot fail");
        output
    })
}

fn usage() -> &'static str {
    "usage: wcash-genesis [show-testnet | show-regtest | status | derive-regtest HEIGHT HEADER_HEX]"
}
