# Wcash

Wcash is an experimental privacy-focused auxiliary proof-of-work chain written
in Rust. It is based on the Zcash Foundation's Zebra `main` history through
[`99a1bed5c`](https://github.com/ZcashFoundation/zebra/commit/99a1bed5c0b3f0fab76879229ebbb65dfa4b67db)
(211 commits after v6.3.0), including the NU6.3 changes and the upstream fixes
that restore coinbase-script-length, expiry-height, and fallible Sprout
aggregate value-balance validation across the parser/verifier boundary. Wcash
borrows Equihash `(200, 9)` work from a Zcash parent block while maintaining its
own blocks, difficulty target, transactions, and shielded value pool.

> **Development status:** this repository contains a frozen Wcash engineering
> Testnet profile plus an isolated local regtest. No public Wcash Testnet node,
> seed, or community pool is deployed. The software is not production ready,
> Wcash mainnet remains disabled, and it must not be used with funds of real
> value.

The built-in Testnet profile has a Wcash-specific NU6.3 transaction domain.
Every post-genesis transaction must use version 6 and embed the exact Wcash
Testnet v1 branch ID; inherited Zcash branch IDs and legacy V1-V5 transactions
are rejected. This removes the earlier consensus-level mining-only gate, but it
does not by itself make the engineering profile or its wallet production ready.

## Current consensus snapshot

| Property | Current rule |
| --- | --- |
| Parent proof of work | Zcash Equihash `(200, 9)` AuxPoW only |
| Target block spacing | 75 seconds |
| Monetary precision | 8 decimal places, identical to Zcash; 1 WCASH = 100,000,000 zatoshi |
| Initial subsidy | 6.25 WCASH at heights 1 through 1,680,000 |
| Halving | Every 1,680,000 blocks (about four years); first halved block is 1,680,001 |
| Monetary cap | 21,000,000 WCASH; exact scheduled issuance is 20,999,999.81520000 WCASH after zatoshi truncation |
| Development allocation | None: no founders reward, funding stream, lockbox, or developer tax |
| Coinbase destination | Ironwood only after genesis |
| Public networks | Engineering Testnet profile implemented but not deployed; mainnet disabled |
| Testnet transfers | Wcash-domain V6 only; inherited Zcash domains and V1-V5 rejected |

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
inherited Zcash address. The node implements payment-address codecs, and the
workspace contains an experimental one-shot wallet for controlled Testnet and
regtest transfers. It derives Wcash addresses, scans a local SQLite wallet from
an attested loopback node, signs Wcash-domain V6 Ironwood transfers, and
broadcasts the exact signed bytes. It is not a pool payout or durable settlement
system. Its controlled local Regtest gate has mined three private coinbases,
scanned and spent one WCASH under the Wcash V6 domain, included the exact
transaction in the mempool and block template, mined it through AuxPoW, and
rescanned the recipient and private change. That gate deliberately used the
explicit Regtest-only one-confirmation override; the public Testnet wallet policy
remains 100 confirmations. The complete prefix table is in the
[consensus snapshot](docs/wcash-consensus.md#payment-address-domains).

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
- [Testnet profile and operator boundary](docs/wcash-testnet.md)

## Release blockers

The mainnet Bitcoin anchor remains deliberately unset, so mainnet activation
fails closed. The controlled private-coinbase spend lifecycle has passed in
isolated Regtest, but no public Testnet is deployed and that local result is not
evidence that a community pool or value-bearing mainnet is ready. Promotion
still requires an independently reviewed consensus specification and
implementation, public-network operation under the 100-confirmation wallet
policy, adversarial multi-node soak and reorg testing, project-controlled seed
infrastructure, physical ASIC interoperability, and a separately reviewed TLS,
variable-difficulty, accounting, payout, settlement, and monitoring layer in
front of the included loopback ZIP-301 service.

## Upstream attribution and license

Wcash is a fork of [Zebra](https://github.com/ZcashFoundation/zebra), the Zcash
Foundation's independent Rust implementation of the Zcash protocol. Wcash is
not represented as an official Zcash Foundation release or endorsement. The
upstream source history and copyright notices are retained.

This repository remains distributed under the MIT and Apache License 2.0 terms
used by Zebra. Some inherited crates are MIT-only. See
[LICENSE-MIT](LICENSE-MIT), [LICENSE-APACHE](LICENSE-APACHE), and the individual
crate manifests for details.
