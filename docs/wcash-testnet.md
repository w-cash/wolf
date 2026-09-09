# Wcash Testnet profile

The source tree contains a frozen Wcash engineering and mining-interoperability
Testnet profile so independently built nodes can agree on the same height-zero
block. No public Wcash Testnet node, DNS seed, or community pool is deployed.
This is not a production-readiness claim: Wcash mainnet remains disabled and
testnet coins must have no monetary value.

> **Transaction-domain test boundary:** Wcash Testnet accepts only post-genesis V6
> transactions carrying its chain-specific branch ID `0xb3cfd27e`. Zcash
> domains, the distinct Wcash Regtest domain `0xc3a6678a`, and V1-V5 are
> rejected. The controlled private-transfer and
> transparent-coinbase shielding lifecycles have passed on isolated Regtest, but
> no coin has monetary value and those local tests do not
> make the wallet, a public Testnet, a mining pool, or payout/settlement system
> production ready.

## Frozen identity

The built-in `WcashTestnet` network commits to Bitcoin mainnet block 965,900:

```text
Bitcoin block:       965900
Bitcoin block hash:  0000000000000000000056b59ff5f4af3ca8b47837f2eac5d83a271c3e6b9851
Anchor audit height: 966011 (112 confirmations counting the anchor)
Wcash genesis hash:  d95a9f2f1daf07d48fb3c863ad7334ec630a4a7077da98c8f7e65f8c0e277cf1
P2P identity:        Wcash/testnet/v4
P2P magic:           2f229b8c
P2P port:            38233
Suggested RPC port:  38232 (loopback only)
```

The exact Bitcoin header, proof of work, confirmation audit height, complete
Wcash genesis bytes, and derived hash are frozen test vectors in the source.
Consensus never fetches or replaces an anchor at runtime. Testnet, regtest, and
Zcash use different genesis hashes, P2P magic, ports, and peer-cache paths.
Wcash configuration also rejects inherited Zcash DNS seeds. The corrected
Testnet identity stores state under `wcashtestnet-v4` and peers under its
matching cache namespace, so earlier profile data is not loaded. Operators must
start from the new empty paths and must not copy or rename older Wcash state
into them.

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

Build the feature-isolated node binaries from the reviewed lockfile:

```sh
scripts/build-wcash-testnet-binaries.sh
./target/release/wcash-zebrad -c wcash-testnet.toml start
```

The build script uses separate Cargo target directories for the Zcash and
Wcash consensus profiles and refuses to publish them if their executable bytes
are identical. This prevents Cargo's shared top-level `zebrad` artifact from
being copied under the wrong profile name after a cached feature build.

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

Treat the RPC cookie, node binary, Wcash payout address, and host as one
payout-critical boundary. The coordinator verifies an exact transparent child
recipient and value from the serialized candidate. If an operator explicitly
selects a private Unified Address, its Ironwood recipient is not publicly
recoverable; the coordinator verifies the private-only shape and value, while
the pool trusts the loopback child node to construct the requested recipient.

## Reproduce the local mining and spend E2E

This script is a mandatory local release check. It is intentionally not run by
GitHub Actions because it performs real proof-of-work solving. The checked-in
hosted workflow uses fixed Equihash and AuxPoW vectors and exercises every
non-solving validation path; its result is a separate required release signal,
not a substitute for the local solver run.
It uses ephemeral, loopback-only profiles with no peers and no cookie
authentication. The first Wcash Testnet-profile phase does not use a
`debug_force_finished_sync` override; the isolated Regtest wallet and parent
profiles do use that explicit test-only shortcut. The script also enables the
mempool at genesis and exposes loopback lightwalletd gRPC so the controlled
spend path is deterministic without public peers. None of those Regtest settings
may be copied into pool operations.

The first phase boots the frozen Testnet profile, asserts its identity and zero
initial supply, uses explicit transparent Wcash and Zcash test-only payout
fixtures, serves a real ZIP-301 job, solves Equihash `(200, 9)`, and requires
consensus acceptance by the Wcash Testnet-profile node and two isolated Zcash
Regtest parent nodes. The coordinator and Wcash AuxPoW verifier independently
check the real Equihash solution. The isolated Zcash Regtest profiles disable
their native proof-of-work check, so their role in this gate is exact parent
block structure, target, proposal, and chain acceptance rather than a second
Equihash verification. The second phase boots a separate Wcash Regtest node,
explicitly selects private Ironwood payout, and exercises the controlled wallet
lifecycle against the same parent validators. The third phase boots a fresh
Wcash Regtest node, mines 101 transparent coinbases, and exercises the inherited
100-block maturity and mandatory-shielding path without reducing confirmations.
Bounded template retries cover asynchronous RPC, state, mempool, and
proposal-service startup.

```sh
scripts/build-wcash-testnet-binaries.sh
scripts/wcash-testnet-e2e.sh
```

The passing run proved that the frozen Testnet profile boots cleanly and that a
proposal-validated ZIP-301 job survives the parent coinbase and block-commitment
paths into Wcash and both Zcash validators. In its controlled Regtest phase, the
script mined and scanned three private 6.25-WEC coinbases, signed a one-WEC
V6 transfer under the Regtest branch ID `0xc3a6678a`, observed the exact
transaction bytes and fee in the mempool and `getblocktemplate`, checked duplicate-broadcast
behavior, and required both standard Zcash Regtest nodes to reject those Wcash
bytes. It then mined the transfer in a fourth real AuxPoW block, rescanned the
recipient and private change, and verified that all 25 WEC remained in
Ironwood while the other pools remained zero.

The transparent phase reported all 99 rewards pending at tip 99 and rejected
premature shielding. At tip 100, only the height-1 reward was spendable by a
height-101 transaction. A second wallet initialized with birthday 100 recovered
all 100 current coinbase UTXOs, proving that transparent recovery does not depend
on the shielded birthday. The primary wallet persisted and broadcast the exact
V6 transaction that shielded one mature coinbase, matched it in the mempool and
block template, and mined it through AuxPoW at height 101. Final wallet and node
checks conserved exactly 631.25 WEC across the transparent and Ironwood pools.
Processing 101 distinct child jobs also exercised confirmed-candidate release
beyond the coordinator's 16-entry active cache.

Only the private Regtest transfer deliberately uses the explicit unsafe
one-confirmation test override. Transparent coinbase maturity remains 100
blocks. The public Testnet wallet policy remains 100 confirmations, and these
local runs are not evidence of a deployed public network or production wallet.

## Controlled-key spend gate: local result

The spendability gate uses fresh ephemeral state and deterministic test seeds
supplied outside command-line arguments and logs. The experimental wallet is
intentionally limited to one-shot operations against an attested loopback Wcash
node. `derive-address` reports a private Unified Address and its related
transparent P2PKH coinbase address. Synchronization classifies transparent
coinbase history, balance output separates mature and pending coinbase value,
and `shield-coinbase` can build and persist one bounded V6 sweep of mature
coinbase inputs into the wallet's Ironwood receiver. The wallet also provides
one-shot Ironwood transfers, recovery by exported transaction ID, bounded and
paginated `list-pending` recovery with exact signed bytes, exact-byte broadcast,
and transaction-status inspection. It requires one exclusive writer per wallet
database. After any ambiguous post-sign outcome, an operator must enumerate all
pending pages, inspect status, and rebroadcast the persisted bytes; constructing
a replacement before resolving those records is unsafe. The wallet is not an
automatic shielding, batch payout, pool accounting, durable settlement, or
idempotent-request system.

The passing automated run established this controlled local path:

1. It derived separate miner and recipient addresses in the Wcash Regtest
   namespace, mined three real AuxPoW child blocks to the controlled miner, and
   scanned all three private 6.25-WEC coinbases.
2. It constructed a balanced one-WEC V6 Ironwood transfer under the Regtest
   branch ID `0xc3a6678a`, including verified private change, fee, and expiry
   height. It used the explicit Regtest-only unsafe one-confirmation override; the public
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
6. The recipient held exactly one WEC, the sender retained 24 WEC, and the
   chain reported exactly 25 WEC entirely in Ironwood, with transparent,
   Sapling, and Orchard balances remaining zero.
7. A fresh wallet and chain mined 101 transparent coinbases through AuxPoW,
   rotating well beyond the coordinator's 16-entry active-candidate cache.
8. At tip 99, all 99 rewards remained pending and `shield-coinbase` failed. At
   tip 100, only the height-1 reward was mature for the height-101 transaction.
9. A wallet initialized with birthday 100 recovered every one of the 100 current
   transparent coinbase UTXOs through the separate current-UTXO scan.
10. The primary wallet persisted and broadcast the exact shielding transaction,
    the node independently decoded its one transparent input, zero transparent
    outputs, and two-action Ironwood bundle, and the template contained those
    same bytes and fee.
11. AuxPoW block 101 mined the transaction. The wallet and chain independently
    reported 631.25 WEC total across transparent and Ironwood, with zero value
    in Sapling and Orchard.

This local pass goes beyond transaction serialization and wallet unit tests. It
does not supply Internet-facing pool security, miner settlement,
crash-idempotent payout batches, or reorg-safe accounting. Those require a
separate durable service that freezes settlement across reorgs and cannot select
or pay the same input twice.

## What is not ready

The controlled private-transfer and transparent-coinbase shielding lifecycles
have passed only in isolated Regtest. No public Wcash Testnet is deployed, the
single-writer one-shot wallet remains experimental, and public Testnet behavior
under its 100-confirmation wallet policy has not been exercised by these local
tests. Public-network reorg behavior, project-operated seed
infrastructure and physical ASIC firmware certification are also missing. The
included pool backend intentionally binds only loopback and uses one fixed share
target. It does not provide Internet-facing TLS, source-IP abuse controls,
variable difficulty, multi-generation late-share grace, balances, payouts,
settlement, monitoring, backups, or incident automation. Wcash mainnet remains
disabled.

The independent Zcash parent coinbase follows standard Zcash rules. The normal
pool examples use public transparent payouts on both chains; a private Wcash
child reward is available only when the operator explicitly supplies the
corresponding Unified Address. Do not advertise hidden Zcash payouts or pay
miners directly from raw share counts. A separately reviewed accounting and
settlement service must consume authenticated journal snapshots before a
community-facing pool can be operated.
