# Wcash

Wcash is an experimental privacy-focused auxiliary proof-of-work chain written
in Rust. It is based on the Zcash Foundation's Zebra `main` history through
[`301d4d5a5`](https://github.com/ZcashFoundation/zebra/commit/301d4d5a5a2287fdcecab90e24df7ca15692e592),
including the NU6.3 changes and the upstream fixes that restore
coinbase-script-length, expiry-height, fallible Sprout aggregate value-balance,
and non-coinbase null-prevout validation across the parser/verifier boundary. Wcash
borrows Equihash `(200, 9)` work from a Zcash parent block while maintaining its
own blocks, difficulty target, transactions, and shielded value pool.

> **Development status:** this repository contains a frozen Wcash engineering
> Testnet profile plus an isolated local regtest. No public Wcash Testnet node,
> seed, or community pool is deployed. The software is not production ready,
> Wcash mainnet remains disabled, and it must not be used with funds of real
> value.

The built-in Testnet and regtest profiles have separate Wcash-specific NU6.3
transaction domains. Every post-genesis transaction must use version 6 and
embed the exact branch ID selected by its Wcash network; inherited Zcash branch
IDs, cross-network Wcash branch IDs, and legacy V1-V5 transactions are rejected.
This removes the earlier consensus-level mining-only gate, but it does not by
itself make either engineering profile or its wallet production ready.

## Current consensus snapshot

| Property | Current rule |
| --- | --- |
| Parent proof of work | Zcash Equihash `(200, 9)` AuxPoW only |
| Target block spacing | 75 seconds |
| Monetary precision | 8 decimal places, identical to Zcash; 1 WEC = 100,000,000 zatoshi |
| Initial subsidy | 6.25 WEC at heights 1 through 1,680,000 |
| Halving | Every 1,680,000 blocks (about four years); first halved block is 1,680,001 |
| Monetary cap | 21,000,000 WEC; exact scheduled issuance is 20,999,999.81520000 WEC after zatoshi truncation |
| Development allocation | None: no founders reward, funding stream, lockbox, or developer tax |
| Coinbase destination | Explicit transparent Wcash address is the normal mode; private Ironwood payout is optional |
| Active value pools | Transparent and Ironwood only; Sprout, Sapling, and legacy Orchard are rejected |
| Public networks | Engineering Testnet profile implemented but not deployed; mainnet disabled |
| Testnet transfers | Wcash-domain V6 only; inherited Zcash domains and V1-V5 rejected |

Pool-facing examples pay each post-genesis coinbase to an explicit Wcash
transparent address, matching the operational model expected by established
mining software. The node never invents an unsafe hard-coded payout address;
the operator must supply one, and the address-type selector defaults to
transparent. An operator can instead supply a Wcash Unified Address with an
Orchard receiver to create a private Ironwood reward. Private Ironwood rewards
remain unrecoverable with Zcash's conventional all-zero outgoing viewing key.
Inherited Zcash addresses are rejected in both modes.

Aggregate issuance remains auditable in either mode. Subsidy and transaction
fees are deterministic consensus inputs; transparent outputs are public, while
the Ironwood value balance publicly commits to value entering the private pool.
Private mode hides the reward recipient, not the protocol-defined gross reward
amount, and parent-pool metadata can still create off-chain correlation.
Transparent coinbase outputs mature after 100 blocks and must be shielded on
their first spend, matching the inherited Zcash public-network policy.

Payment addresses use Wcash-specific namespaces: Unified `wu...`, transparent
`W...`, and transparent-source-only TEX `wtex...`, with distinct testnet and
regtest variants. The former `ws...` Sapling namespace is permanently reserved
and rejected. Wcash mining accepts a Wcash transparent address for the
normal pool-integration path or a Wcash Unified Address with an Orchard receiver
for an optional private Ironwood reward. It rejects inherited Zcash addresses.
The node implements payment-address codecs, and the workspace contains an
experimental one-shot wallet for controlled Testnet and regtest operations. It
derives a private Wcash Unified Address and a transparent P2PKH coinbase address
from the same seed, scans both histories into local SQLite, and can build a
bounded transaction that shields only mature, fully classified coinbase outputs
into its own Ironwood receiver. It also signs Wcash-domain V6 Ironwood transfers
and broadcasts exact signed bytes. It is a controlled, single-writer tool, not a
pool payout or durable settlement system. Its current full local Regtest gate
has mined three private coinbases, scanned and spent one WEC under the Wcash
V6 domain, included the exact transaction in the mempool and block template,
mined it through AuxPoW, and rescanned the recipient and private change. A
separate phase mined 101 transparent coinbases through the same AuxPoW path,
proved the 100-block maturity boundary and pre-maturity rejection, recovered all
current UTXOs from a deliberately late wallet birthday, shielded one mature
coinbase with its exact signed bytes in the mempool and template, and mined the
shielding transaction at height 101. The run conserved the complete
631.25-WEC supply across transparent and Ironwood pools and rotated far beyond
the coordinator's 16-candidate cache bound. Only the private-transfer phase used
the explicit Regtest-only one-confirmation override; transparent coinbase
maturity was not shortened. The public Testnet wallet policy remains 100
confirmations. The complete prefix table is in the
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
the earlier local genesis and uses the `Wcash/regtest/v5` P2P identity.

The native merge-mining coordinator can create a transparent or private Wcash candidate,
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
fails closed. The controlled private-transfer and transparent-coinbase shielding
lifecycles have passed in isolated Regtest, but no public Testnet is deployed
and those local results are not evidence that a community pool or value-bearing
mainnet is ready. Promotion still requires an independently reviewed consensus
specification and implementation, public-network operation under the
100-confirmation wallet policy, adversarial multi-node soak and reorg testing,
project-controlled seed infrastructure, physical ASIC interoperability, and a
separately reviewed TLS, variable-difficulty, accounting, payout, settlement,
and monitoring layer in front of the included loopback ZIP-301 service.

## Upstream attribution and license

Wcash is a fork of [Zebra](https://github.com/ZcashFoundation/zebra), the Zcash
Foundation's independent Rust implementation of the Zcash protocol. Wcash is
not represented as an official Zcash Foundation release or endorsement. The
upstream source history and copyright notices are retained.

This repository remains distributed under the MIT and Apache License 2.0 terms
used by Zebra. Some inherited crates are MIT-only. See
[LICENSE-MIT](LICENSE-MIT), [LICENSE-APACHE](LICENSE-APACHE), and the individual
crate manifests for details.
