# Wcash Testnet profile

The source tree contains a frozen Wcash engineering and mining-interoperability
Testnet profile so independently built nodes can agree on the same height-zero
block. No public Wcash Testnet node, DNS seed, or community pool is deployed.
This is not a production-readiness claim: Wcash mainnet remains disabled and
testnet coins must have no monetary value.

> **Transaction-domain test boundary:** Wcash Testnet accepts only post-genesis V6
> transactions carrying its chain-specific branch ID `0xb3cfd27e`. Zcash
> domains and V1-V5 are rejected. The controlled spend lifecycle has passed on
> isolated Regtest, but no coin has monetary value and that local test does not
> make the wallet, a public Testnet, a mining pool, or payout/settlement system
> production ready.

## Frozen identity

The built-in `WcashTestnet` network commits to Bitcoin mainnet block 965,900:

```text
Bitcoin block:       965900
Bitcoin block hash:  0000000000000000000056b59ff5f4af3ca8b47837f2eac5d83a271c3e6b9851
Anchor audit height: 966011 (112 confirmations counting the anchor)
Wcash genesis hash:  78b292284bc7b03c6a16b62e29a3ab2015c40d6414cbc27ddcee878225600f10
P2P magic:           8ee56b56
P2P port:            38233
Suggested RPC port:  38232 (loopback only)
```

The exact Bitcoin header, proof of work, confirmation audit height, complete
Wcash genesis bytes, and derived hash are frozen test vectors in the source.
Consensus never fetches or replaces an anchor at runtime. Testnet, regtest, and
Zcash use different genesis hashes, P2P magic, ports, and peer-cache paths.
Wcash configuration also rejects inherited Zcash DNS seeds. Any local state
created by the earlier mining-only prototype must be deleted and resynchronized;
post-genesis blocks from that prototype are not compatible with the frozen
Wcash Testnet v1 transaction domain.

## Anchor verification record

The immutable runtime input is this exact 80-byte Bitcoin wire header:

```text
00203220700b5cbd51c3511db177feb5891754ee7e3ec5f4849a010000000000
000000000444905fd34cdb083f512a1957ec2c7ffedf3a7ff5098d1f20a5aed9
c8484b46c5719e6a5e35021706442381
```

Its decoded fields are:

| Field | Frozen value |
| --- | --- |
| Version | `0x20322000` (`540155904`) |
| Previous block | `000000000000000000019a84f4c53e7eee541789b5fe77b11d51c351bd5c0b70` |
| Merkle root | `464b48c8d9aea5201f8d09f57f3adffe7f2cec57192a513f08db4cd35f904404` |
| Timestamp | `1788768709` (`2026-09-07 08:11:49 UTC`) |
| Compact target | `0x1702355e` (`386020702`) |
| Nonce | `0x81234406` (`2166572038`) |

At the release audit snapshot, both [Blockstream's height
lookup](https://blockstream.info/api/block-height/965900) and [mempool.space's
height lookup](https://mempool.space/api/block-height/965900) mapped height
965,900 to the frozen hash. Their [Blockstream block
record](https://blockstream.info/api/block/0000000000000000000056b59ff5f4af3ca8b47837f2eac5d83a271c3e6b9851)
and [mempool.space block
record](https://mempool.space/api/block/0000000000000000000056b59ff5f4af3ca8b47837f2eac5d83a271c3e6b9851)
agreed on the decoded fields above. Both height-965,901 lookups returned
[the same next hash on
Blockstream](https://blockstream.info/api/block-height/965901) and [on
mempool.space](https://mempool.space/api/block-height/965901):
`0000000000000000000044700b8a0a6bebac628571573e05c3b71ad146ad1014`.
That block's `previousblockhash` is the anchor. Blockchain.com's [independent
raw record](https://blockchain.info/rawblock/0000000000000000000056b59ff5f4af3ca8b47837f2eac5d83a271c3e6b9851?format=json)
agreed on hash, height, version, predecessor, Merkle root, timestamp, bits, and
nonce, reported `main_chain: true`, and listed the same next block.

The confirmation snapshot was taken at Bitcoin best-chain height 966,011,
hash `0000000000000000000193744718da7f525889a7ed87d2c2a86ea309a657d7b9`.
[Blockstream](https://blockstream.info/api/block-height/966011) and
[mempool.space](https://mempool.space/api/block-height/966011) independently
returned that audit-tip hash. Thus the anchor had 112 confirmations counting
the anchor block. Later Bitcoin heights are expected and do not mutate this
record.

A local double-SHA256 of the 80 bytes, displayed in conventional reversed byte
order, produces the frozen hash. Decoding `nBits` produces target:

```text
00000000000000000002355e0000000000000000000000000000000000000000
```

The displayed hash, interpreted as an unsigned integer, is less than or equal
to that target (by a factor of six under integer division), so the header's
isolated Bitcoin proof of work is valid. These explorer observations are an
external audit snapshot only. Nodes do not trust explorers: runtime consensus
uses only the immutable header and derived values compiled into this release.

Testnet activates NU6.3 at height 1, uses a 75-second target spacing, starts at
the compact proof-of-work limit `0x2007ffff` (`2^251 - 1`), and enables normal
damped retargeting from launch. A block more than 450 seconds after its
predecessor may use the testnet minimum difficulty. The boundary is strict:
exactly 450 seconds is not enough.

## Start a node

Build the feature-isolated Wcash binary from the reviewed lockfile:

```sh
cargo build --locked --release -p zebrad --bin zebrad \
  --no-default-features --features wcash-consensus
./target/release/zebrad -c wcash-testnet.toml start
```

The checked-in `wcash-testnet.toml` is a safe first-seed baseline. It exposes
P2P on port 38233, keeps RPC on loopback port 38232 with cookie authentication,
uses persistent network-isolated state, and never enables
`debug_force_finished_sync`. No project-operated DNS seed is published yet.
Until one exists, an operator must either accept inbound Wcash peers or add
explicit trusted Wcash endpoints to `initial_testnet_peers`. Never add a Zcash
seed or copy Zcash peer-cache data.

Zebra's normal test-network policy permits `getblocktemplate` and
`createauxblock` while a node is isolated or still syncing. A real pool must
independently require healthy Wcash peers and confirm that its child node is on
the current best tip before publishing work. The peerless configuration below
exists only to test deterministic bootstrap; mining on its templates outside
the smoke test can build a stale private fork. The Wcash template path uses the
normal mempool snapshot and can include valid Wcash-domain V6 transactions.

Treat the RPC cookie, node binary, Wcash payout receiver, and host as one
payout-critical boundary. The private Wcash coinbase deliberately prevents the
coordinator from publicly recovering its recipient, so the pool must trust its
loopback child node to construct the requested reward correctly.

## Reproduce the local mining and spend E2E

This script is a mandatory local release check. It is intentionally not run by
GitHub Actions because it performs real proof-of-work solving; hosted CI uses
fixed Equihash and AuxPoW vectors and exercises the non-solving validation paths.
It uses ephemeral, loopback-only profiles with no peers, no cookie
authentication, and no `debug_force_finished_sync` override. It explicitly
enables the mempool at genesis and exposes loopback lightwalletd gRPC so the
controlled spend path is deterministic without public peers; both settings are
test-only and must not be copied into pool operations.

The first phase boots the frozen Testnet profile, asserts its identity and zero
initial supply, serves a real ZIP-301 job, solves Equihash `(200, 9)`, and
requires consensus acceptance by the Wcash Testnet-profile node and two isolated
Zcash Regtest parent nodes. The second phase boots a separate Wcash Regtest node
against the same parent validators and exercises the controlled wallet lifecycle.
Bounded template retries cover asynchronous RPC, state, mempool, and
proposal-service startup.

```sh
cargo build --locked --release -p zebrad --bin zebrad --no-default-features
cp target/release/zebrad target/release/zcash-zebrad

cargo build --locked --release -p zebrad --bin zebrad \
  --no-default-features --features wcash-consensus
cp target/release/zebrad target/release/wcash-zebrad

cargo build --locked --release -p wcash-merge-miner --bin wcash-merge-miner
cargo build --locked --release -p wcash-wallet --bin wcash-wallet
scripts/wcash-testnet-e2e.sh
```

The passing run proved that the frozen Testnet profile boots cleanly and that a
proposal-validated ZIP-301 job survives the parent coinbase and block-commitment
paths into Wcash and both Zcash validators. In its controlled Regtest phase, the
script mined and scanned three private 6.25-WCASH coinbases, signed a one-WCASH
V6 transfer under branch ID `0xb3cfd27e`, observed the exact transaction bytes
and fee in the mempool and `getblocktemplate`, checked duplicate-broadcast
behavior, and required both standard Zcash Regtest nodes to reject those Wcash
bytes. It then mined the transfer in a fourth real AuxPoW block, rescanned the
recipient and private change, and verified that all 25 WCASH remained in
Ironwood while the other pools remained zero.

The Regtest transfer deliberately uses the explicit unsafe one-confirmation
test override. The public Testnet wallet policy remains 100 confirmations, and
this local run is not evidence of a deployed public network or production wallet.

## Controlled-key spend gate: local result

The spendability gate uses fresh ephemeral state and deterministic test seeds
supplied outside command-line arguments and logs. The experimental wallet is
intentionally limited to a one-shot Ironwood transfer against an attested
loopback Wcash node. It provides Wcash address derivation, SQLite scanning and
balance reporting, Wcash-domain V6 signing, recovery by exported transaction ID,
exact-byte broadcast, and transaction-status inspection. It is not a batch
payout, pool accounting, durable settlement, or idempotent-request system.

The passing automated run established this controlled local path:

1. It derived separate miner and recipient addresses in the Wcash Regtest
   namespace, mined three real AuxPoW child blocks to the controlled miner, and
   scanned all three private 6.25-WCASH coinbases.
2. It constructed a balanced one-WCASH V6 Ironwood transfer under branch ID
   `0xb3cfd27e`, including verified private change, fee, and expiry height. It
   used the explicit Regtest-only unsafe one-confirmation override; the public
   Testnet wallet policy remains 100 confirmations.
3. It submitted the exact signed bytes, observed the transaction in the Wcash
   mempool, checked duplicate-broadcast behavior, and matched its exact bytes and
   fee in `getblocktemplate`.
4. Both standard Zcash Regtest nodes rejected the exact Wcash transaction and
   did not admit it to their mempools. Consensus and cross-crate integration
   tests separately enforce exact two-way Wcash/Zcash branch-domain rejection
   and reject Wcash post-genesis V1-V5 transactions.
5. It mined and accepted the transfer in a fourth real AuxPoW child block,
   observed its mined status, and independently rescanned the sender and
   recipient wallets.
6. The recipient held exactly one WCASH, the sender retained 24 WCASH, and the
   chain reported exactly 25 WCASH entirely in Ironwood, with transparent,
   Sapling, and Orchard balances remaining zero.

This local pass goes beyond transaction serialization and wallet unit tests. It
does not supply Internet-facing pool security, miner settlement,
crash-idempotent payout batches, or reorg-safe accounting. Those require a
separate durable service that freezes settlement across reorgs and cannot select
or pay the same input twice.

## What is not ready

The controlled private-coinbase spend lifecycle has passed only in isolated
Regtest. No public Wcash Testnet is deployed, the one-shot wallet remains
experimental, and public Testnet behavior under its 100-confirmation wallet
policy has not been exercised by this local test. Project-operated seed
infrastructure and physical ASIC firmware certification are also missing. The
included pool backend intentionally binds only loopback and uses one fixed share
target. It does not provide Internet-facing TLS, source-IP abuse controls,
variable difficulty, multi-generation late-share grace, balances, payouts,
settlement, monitoring, backups, or incident automation. Wcash mainnet remains
disabled.

The independent Zcash parent coinbase follows standard Zcash rules and its
shielded recipient is publicly recoverable; only the Wcash child reward uses
the stronger private-coinbase policy. Do not advertise hidden Zcash payouts or
pay miners directly from raw share counts. A separately reviewed accounting
and settlement service must consume authenticated journal snapshots before a
community-facing pool can be operated.
