# Wcash

Wcash is an experimental privacy-focused auxiliary proof-of-work chain written
in Rust. It is based on the Zcash Foundation's Zebra `main` history through
[`d1fd7adfd`](https://github.com/ZcashFoundation/zebra/commit/d1fd7adfd366bddfb8f38ce70b417a5e3408799b)
(188 commits after v6.3.0), including its latest Ironwood/NU6.3 fixes. Wcash
borrows Equihash `(200, 9)` work from a Zcash parent block while maintaining its
own blocks, difficulty target, transactions, and shielded value pool.

> **Development status:** this repository enables a public Wcash Testnet for
> engineering and mining interoperability, plus an isolated local regtest. It
> is not production ready, no Wcash mainnet is active, and it must not be used
> with funds of real value.

The current public Testnet is deliberately mining-only. Consensus rejects every
non-coinbase transaction until Wcash has a chain-specific NU6.3 signature and
branch-ID domain wired through the cryptographic dependencies. Mined rewards
cannot be transferred. A testnet reset and consensus upgrade are required
before any value-bearing transaction test.

## Current consensus snapshot

| Property | Current rule |
| --- | --- |
| Parent proof of work | Zcash Equihash `(200, 9)` AuxPoW only |
| Target block spacing | 75 seconds |
| Initial subsidy | 6.25 WCASH at heights 1 through 1,680,000 |
| Halving | Every 1,680,000 blocks (about four years); first halved block is 1,680,001 |
| Monetary cap | 21,000,000 WCASH; exact scheduled issuance is 20,999,999.81520000 WCASH after zatoshi truncation |
| Development allocation | None: no founders reward, funding stream, lockbox, or developer tax |
| Coinbase destination | Ironwood only after genesis |
| Public networks | Mining-only engineering Testnet enabled; mainnet disabled |
| Testnet transfers | Disabled by consensus; coinbase transactions only |

Every post-genesis coinbase must create its reward in Ironwood. Transparent,
Sapling, and Orchard coinbase outputs are rejected. Unlike the standard Zcash
coinbase recovery convention, Wcash also rejects Ironwood reward outputs that
can be recovered with the all-zero outgoing viewing key. The recipient and note
contents are therefore not published by that convention.

Aggregate issuance remains auditable. Subsidy is deterministic, transaction
fees are checked, and the public Ironwood value balance commits to the net value
entering the pool. This hides the reward recipient, not the protocol-defined
gross reward amount; parent-pool metadata can also create off-chain correlation.

Payment addresses use Wcash-specific namespaces: Unified `wu...`, Sapling
`ws...`, TEX `wtex...`, and transparent `W...`, with distinct testnet and
regtest variants. Wcash mining requires a Wcash Unified Address and rejects an
inherited Zcash address. The node implements payment-address codecs, not a
wallet or Wcash-specific spending/viewing-key formats; those remain release
work. See the [consensus snapshot](docs/wcash-consensus.md#payment-address-domains)
for the complete prefix table.

## Run the isolated local network

Install Rust 1.91 or newer plus the native build dependencies required by
Zebra, then run:

```sh
cargo build --locked --release -p zebrad --bin zebrad \
  --features wcash-consensus,internal-miner
cargo build --locked --release -p wcash-merge-miner --bin wcash-merge-miner
./target/release/zebrad -c wcash-local.toml start
```

The supplied configuration binds P2P and RPC services to loopback, uses an
ephemeral state database, enables the synthetic-parent internal miner, and
disables RPC cookie authentication only on that loopback listener. Its miner
address is a public test fixture; replace it with an address for which you
control the keys before testing spendability.

This local chain freezes Bitcoin mainnet block 965,910, hash
`00000000000000000000bbbdb28d2ff098642c6fde0a5fd84a707c92d146b146`,
which was the Blockstream tip snapshot selected for regtest genesis. It replaces
the earlier local genesis and uses the `Wcash/regtest/v4` P2P identity.

The native merge-mining coordinator can create a private Wcash candidate,
request a commitment-aware Zcash template, obtain independent proposal
validation, expose a ZIP-301 backend for Equihash ASIC interoperability testing,
and durably submit winners to both nodes. It is intentionally loopback-only and
is not a public pool edge:

```sh
cargo run --release -p wcash-merge-miner -- --help
```

See:

- [Consensus and monetary policy](docs/wcash-consensus.md)
- [Zcash merged-mining design](docs/wcash-merged-mining.md)
- [Local regtest guide](docs/wcash-local.md)
- [Public Testnet identity and operator boundary](docs/wcash-testnet.md)

## Release blockers

The mainnet Bitcoin anchor remains deliberately unset, so mainnet activation
fails closed. The enabled Testnet is an engineering network, not evidence that
a community pool or value-bearing mainnet is ready. Promotion requires an
independently reviewed consensus specification and implementation, adversarial
multi-node soak and reorg testing, Wcash wallet/key support, project-controlled
seed infrastructure, physical ASIC interoperability, and an operated TLS,
variable-difficulty, accounting, payout, and monitoring layer in front of the
included loopback ZIP-301 service.

## Upstream attribution and license

Wcash is a fork of [Zebra](https://github.com/ZcashFoundation/zebra), the Zcash
Foundation's independent Rust implementation of the Zcash protocol. Wcash is
not represented as an official Zcash Foundation release or endorsement. The
upstream source history and copyright notices are retained.

This repository remains distributed under the MIT and Apache License 2.0 terms
used by Zebra. Some inherited crates are MIT-only. See
[LICENSE-MIT](LICENSE-MIT), [LICENSE-APACHE](LICENSE-APACHE), and the individual
crate manifests for details.
