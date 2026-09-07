# Wcash

Wcash is an experimental privacy-focused auxiliary proof-of-work chain written
in Rust. It is based on the Zcash Foundation's Zebra v6.3.0 codebase and its
Ironwood/NU6.3 implementation. Wcash borrows Equihash `(200, 9)` work from a
Zcash parent block while maintaining its own blocks, difficulty target,
transactions, and shielded value pool.

> **Development status:** this repository is regtest/pre-testnet software. It is
> not production ready, no public Wcash network is active, and it must not be
> used with funds of real value.

## Current consensus snapshot

| Property | Current rule |
| --- | --- |
| Parent proof of work | Zcash Equihash `(200, 9)` AuxPoW only |
| Target block spacing | 75 seconds |
| Initial subsidy | 10 WCASH at heights 1 through 1,680,000 |
| Halving | Every 1,680,000 blocks; first halved block is 1,680,001 |
| Scheduled issuance | Exactly 33,599,999.78160000 WCASH |
| Development allocation | None: no founders reward, funding stream, lockbox, or developer tax |
| Coinbase destination | Ironwood only after genesis |
| Public networks | Disabled until a reviewed Bitcoin anchor is frozen |

Every post-genesis coinbase must create its reward in Ironwood. Transparent,
Sapling, and Orchard coinbase outputs are rejected. Unlike the standard Zcash
coinbase recovery convention, Wcash also rejects Ironwood reward outputs that
can be recovered with the all-zero outgoing viewing key. The recipient and note
contents are therefore not published by that convention.

Aggregate issuance remains auditable. Subsidy is deterministic, transaction
fees are checked, and the public Ironwood value balance commits to the net value
entering the pool. This hides the reward recipient, not the protocol-defined
gross reward amount; parent-pool metadata can also create off-chain correlation.

## Run the isolated local network

Install Rust 1.91 or newer plus the native build dependencies required by
Zebra, then run:

```sh
cargo build --release -p zebrad --features internal-miner --bin zebrad
cargo build --release -p wcash-merge-miner --bin wcash-merge-miner
./target/release/zebrad -c wcash-local.toml start
```

The supplied configuration binds P2P and RPC services to loopback, uses an
ephemeral state database, enables the synthetic-parent internal miner, and
disables RPC cookie authentication only on that loopback listener. Its miner
address is a public test fixture; replace it with an address for which you
control the keys before testing spendability.

The merge-mining harness can exercise real Equihash solving and proof
validation, but it is not yet an end-to-end production pool adapter:

```sh
cargo run --release -p wcash-merge-miner -- --help
```

See:

- [Consensus and monetary policy](docs/wcash-consensus.md)
- [Zcash merged-mining design](docs/wcash-merged-mining.md)
- [Local regtest guide](docs/wcash-local.md)

## Release blockers

The designated Bitcoin mainnet anchor height, `965,954`, is intentionally
treated as not yet mined/frozen. The mainnet and public-testnet anchor constants
remain unset, so public activation fails closed. A public release also requires
an independently reviewed consensus specification and implementation, stable
public network/address domains, adversarial testing, and a production pool
adapter that starts from live Zcash `getblocktemplate` data and correctly
submits qualifying work to both chains.

## Upstream attribution and license

Wcash is a fork of [Zebra](https://github.com/ZcashFoundation/zebra), the Zcash
Foundation's independent Rust implementation of the Zcash protocol. Wcash is
not represented as an official Zcash Foundation release or endorsement. The
upstream source history and copyright notices are retained.

This repository remains distributed under the MIT and Apache License 2.0 terms
used by Zebra. Some inherited crates are MIT-only. See
[LICENSE-MIT](LICENSE-MIT), [LICENSE-APACHE](LICENSE-APACHE), and the individual
crate manifests for details.
