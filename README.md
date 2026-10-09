# Wcash

Wolf is the Rust node and protocol workspace for **Wcash (WEC)**, an independent
Zcash-derived chain using Zcash Equihash `(200, 9)` auxiliary proof of work
(AuxPoW). Wcash has its own genesis, transactions, difficulty and monetary policy.
It is not ZEC, and mining at an unchanged Zcash pool does not automatically
provide Wcash support.

This repository is a fork of [Zebra](https://github.com/ZcashFoundation/zebra).
Many crate names and the executable, `zebrad`, retain their upstream names.
**Build with `wcash-consensus` to select the Wcash node profile.** The default
Zcash build and a Wcash build are not interchangeable.

> **Implementation and release status:** Mainnet, Testnet and local Regtest
> profiles are implemented in this source tree. Mainnet has a frozen genesis and
> its own emission implementation; it is not disabled. A source checkout, an
> inherited Zebra version number or a successful local test does not establish
> that a deployed service or wallet release is audited or ready for unrestricted
> use. Verify the source, build profile and network of every artifact.
> Testnet and Regtest coins are valueless `TWC`.
>
> The legacy unversioned `CHANGELOG.md` section is a historical pre-Mainnet
> snapshot, not the current protocol definition. Use the
> [protocol direction](docs/wcash-direction.md), [consensus reference](docs/wcash-consensus.md),
> source and release fragments for current behavior. This revision also keeps
> inherited Zebra publication and deployment workflows disabled in `w-cash/wolf`
> until a Wcash-specific artifact pipeline is implemented and reviewed.

## Start here

| I want to… | Read |
| --- | --- |
| Understand Wcash and find ecosystem services | [w.cash](https://w.cash/) and the [documentation index](docs/README.md) |
| Understand the protocol's intended direction | [Protocol direction](docs/wcash-direction.md) |
| Check official release and deployment evidence | [Release and deployment status](docs/wcash-release-status.md) |
| Build and run a node | [Node operator guide](docs/wcash-node.md) |
| Check network identity, supply and privacy rules | [Consensus reference](docs/wcash-consensus.md) |
| Query a node from software or an AI agent | [Agent and integration guide](docs/wcash-agents.md) |
| Contribute changes with a coding agent | [AGENTS.md](AGENTS.md) and [CONTRIBUTING.md](CONTRIBUTING.md) |
| Integrate merged mining | [Mining contract](docs/wcash-merged-mining.md) and [pool compatibility](docs/wcash-pool-compatibility.md) |
| Exercise a disposable local network | [Local Regtest guide](docs/wcash-local.md) |
| Use the included experimental wallet | [Wallet guide](wcash-wallet/README.md) and [payout boundary](docs/wcash-wallet-payout.md) |
| Report a suspected vulnerability privately | [Security policy](SECURITY.md) |

For a consumer wallet, start at [wcashwallet.com](https://wcashwallet.com/) and
check the platform's actual release and signing status. This workspace's
`wcash-wallet` is an experimental operator/developer tool, not the desktop or
mobile application's installation package.

## Mainnet at a glance

This table describes implemented **Mainnet** rules. Testnet and Regtest have
different identities and subsidy schedules; see the
[network reference](docs/wcash-consensus.md).

| Property | Mainnet rule |
| --- | --- |
| Ticker and precision | `WEC`; 1 WEC = 100,000,000 integer atomic units (zatoshi) |
| Target block spacing | 75 seconds; observed intervals vary |
| Proof of work | Zcash Equihash `(200, 9)` with AuxPoW v2 |
| Genesis | `5bae12c8662a577b04ce1591af1a137c128f0cb51018a5f1622d861d1bb6fc48` |
| Transaction domain | Wcash V6, Mainnet branch ID `0xd9c6a7ee` |
| Emission | 40,000-block linear slow start, then Monero-style integer smooth decay with a permanent 0.375-WEC tail |
| Halvings and maximum supply | No Mainnet halvings and no fixed economic supply cap |
| Protocol allocation | No founders reward, funding stream, lockbox or developer tax |
| Active value pools | Transparent and Ironwood; Sprout, Sapling and legacy Orchard components are rejected |
| Coinbase maturity | 100 blocks; transparent coinbase outputs must first be shielded |

The exact recurrence is in
[`wcash_mainnet.rs`](zebra-chain/src/parameters/network/subsidy/wcash_mainnet.rs).
Its curve scale is not a supply cap, and its post-ramp counter excludes
slow-start issuance. Do not substitute a floating-point approximation or a
testing network's halving schedule.

The same mining work is checked against each chain's own target. A valid Wcash
block need not also be a Zcash block. Merged mining does not imply that all
Zcash hashrate participates in Wcash.

## Build the node

Use the pinned [Rust toolchain](rust-toolchain.toml), committed lockfile and
native prerequisites in the [node guide](docs/wcash-node.md). At the repository
root:

```sh
cargo build --locked --release -p zebrad --bin zebrad \
  --features wcash-consensus
```

The executable is `target/release/zebrad`. Keep Wcash and default Zcash builds
in separate target directories when building both profiles. Do not infer the
consensus profile from the executable name alone.

For isolated synthetic-parent Regtest mining, the separate
[`wcash-local.toml`](wcash-local.toml) fixture also requires `internal-miner`:

```sh
cargo build --locked --release -p zebrad --bin zebrad \
  --features wcash-consensus,internal-miner
./target/release/zebrad -c wcash-local.toml start
```

That fixture uses loopback listeners, ephemeral state, a public test payout
address and disabled RPC cookie authentication for local testing. It is not a
Mainnet configuration. Never reuse its test addresses, authentication bypass or
ephemeral-state settings for funds or a public service.

## Repository boundaries

| Component | Purpose |
| --- | --- |
| `zebrad` and `zebra-*` crates | Node runtime, chain validation, networking, state and RPC |
| `wcash-genesis` | Frozen network identities and external-chain anchors |
| `wcash-zcash-aux` | AuxPoW serialization, commitments and vectors |
| `wcash-merge-miner` | Template validation, mining coordination and durable backend tooling |
| `wcash-wallet` | Experimental local wallet, inspection and controlled payout tooling |

The public [explorer](https://wcashexplorer.com/),
[wallet applications](https://wcashwallet.com/) and
[pool service](https://pool.zecwec.com/) are separate deployments. Their REST
routes, account features, payout policies and availability are not the node's
JSON-RPC contract. A working service URL does not identify its deployed commit.

The [website specification](https://w.cash/whitepaper) is an additional reading
aid. For reproducible integrations, record the Wolf commit, build features,
genesis and transaction branch, and check implementation and tests. The
[protocol direction](docs/wcash-direction.md) records the intended design; the
[documentation index](docs/README.md) maps each topic to its repository source.

## Review and security

Mainnet implementation does not substitute for release review. Consensus,
wallet recovery, key handling, public endpoints, pool accounting and reorg-safe
settlement need their own validation and responsible operators. Local Regtest
results establish only the behavior actually exercised by that run.

Keep node RPC, mining capabilities, cookie files and spending material private.
Do not publish suspected security flaws in a documentation PR; use
[SECURITY.md](SECURITY.md). Non-sensitive improvements follow
[CONTRIBUTING.md](CONTRIBUTING.md).

## Upstream attribution and license

Wcash retains Zebra's upstream history and copyright notices. It is not an
official Zcash Foundation release or endorsement. Inherited Zcash documentation
is useful for shared internals, but its network parameters and release artifacts
are not Wcash instructions.

This repository retains Zebra's MIT and Apache License 2.0 terms. Some inherited
crates are MIT-only. See [LICENSE-MIT](LICENSE-MIT),
[LICENSE-APACHE](LICENSE-APACHE), and individual crate manifests.
