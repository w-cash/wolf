# Wcash Testnet profile

The source tree contains a frozen Wcash engineering and mining-interoperability
Testnet profile so independently built nodes can agree on the same height-zero
block. Testnet coins have no monetary value. Mainnet is a separate, enabled
profile with its own genesis and monetary schedule; see
[`wcash-consensus.md`](wcash-consensus.md).

User-facing Testnet balances use `TWC` (Test Wcash) rather than mainnet `WEC`.
This label does not alter the eight-decimal integer-zatoshi encoding.
The Testnet genesis statement identifies its Zcash Testnet anchor. The current
transaction domain is Testnet v3 and the P2P/cache identity is v7. These names
version different parts of the profile. An older public endpoint or database
must not be assumed compatible merely because it is called Wcash Testnet.

> **Transaction-domain test boundary:** Wcash Testnet accepts only post-genesis V6
> transactions carrying its chain-specific branch ID `0x54ba2bfb`. Zcash,
> the retired Testnet v1 domain `0xb3cfd27e`, Wcash Mainnet `0xd9c6a7ee`, Wcash
> Regtest `0xc3a6678a`, and V1-V5 are rejected. The local tests below exercise
> controlled private transfers and transparent-coinbase shielding on isolated
> Regtest. Their results do not certify a public deployment, wallet release or
> pool settlement service.

## Frozen identity

The built-in `WcashTestnet` network commits to canonical Zcash Testnet block
4,362,016. Its own genesis timestamp is exactly 2026-09-17 23:00:00 UTC.

```text
Zcash Testnet block: 4362016
Zcash block hash:     00000e289ad21d2feeb17e16585790ecabec94323c2ef22925534ad104de73ac
Zcash block time:     1789685930 (2026-09-17 22:58:50 UTC)
Wcash genesis time:   1789686000 (2026-09-17 23:00:00 UTC)
Wcash genesis hash:   6b66fff119977d36d9c989093b516a876dbf6596536791ff35bb4c581e3fda98
P2P identity:         Wcash/testnet/v7
P2P magic:            49d2934b
P2P port:             38233
Suggested RPC port:   38232 (loopback only)
Transaction branch:   54ba2bfb
State cache:          wcashtestnet-v7
```

The exact external hash, complete Wcash genesis bytes, and derived Wcash hash
are frozen test vectors. Consensus never fetches or replaces an anchor at
runtime. The source-chain discriminator and Zcash-specific BLAKE2b
personalization prevent the anchor from being confused with a Bitcoin anchor.

Identity comes from [the frozen genesis](../zebra-chain/src/block/genesis.rs),
[anchor and P2P constants](../wcash-genesis/src/lib.rs),
[network namespaces and ports](../zebra-chain/src/parameters/network.rs), and
[transaction branch IDs](../zebra-chain/src/parameters/network_upgrade.rs).
Check a deployed node's actual genesis and branch against these values before
using it. This document does not provide a live seed or endpoint health list.

## Testnet monetary and difficulty rules

Testnet's subsidy is `floor(625,000,000 * height / 40,000)` zatoshi from genesis
through height 40,000, reaching 6.25 TWC. After the ramp it uses
`625,000,000 >> floor(max(height - 20,000, 0) / 1,680,000)`, with its first
halved block at height 1,700,000. Mainnet instead ramps to 21.99023255 WEC,
then decays smoothly to a permanent 0.375 WEC tail. Regtest starts at 6.25 TWC
at height 1 without a ramp and first halves at height 1,680,001. Do not apply
Regtest test balances or Testnet halvings to Mainnet supply.

Testnet targets 75-second blocks, uses a damped 17-block retarget, and starts
at proof-of-work limit `0x1e2fabe8`. A gap strictly greater than 450 seconds
permits a Testnet proof-of-work-limit block. Mainnet has the same launch ceiling
but no such exception; Regtest uses fixed difficulty. See the
[profile parameters](../zebra-chain/src/parameters/network/testnet.rs) and
[subsidy implementation](../zebra-chain/src/parameters/network/subsidy.rs).

## Network-winner coverage for ASIC jobs

Use the network-derived target policy for live mining:

```sh
export ZCASH_NETWORK=testnet
export WCASH_SHARE_TARGET=network
unset WCASH_TESTNET_PARENT_TARGET_SAMPLING
```

Every generation advertises the easier of its exact Wcash and Zcash targets,
so every possible winner on either chain reaches the coordinator. After the
first candidate is durably recorded and acknowledged, the listener immediately
retires that generation and prepares current work. This keeps an unusually easy
Zcash Testnet target from turning one stale generation into sustained share
traffic. Preflight must show `full_network_winner_coverage`, both capture flags
as `true`, and immediate network-winner rotation as `true`.

## Legacy parent-target sampling

The sampling switch remains available to reproduce older Testnet deployments.
It intentionally loses Zcash-only winners and must not be used when reward
coverage is the goal.

During this specific bootstrap condition, first run `native-job` and copy its
exact Wcash `child_target`, then configure `native-serve` as follows:

```sh
export ZCASH_NETWORK=testnet
export WCASH_SHARE_TARGET='<exact child_target from native-job>'
export WCASH_TESTNET_PARENT_TARGET_SAMPLING=1
```

The value must be exactly `1`, and startup rejects the mode for Mainnet,
Regtest, a child genesis other than the built-in Wcash Testnet, or more than one
connected-client slot. The harder advertised target continues to capture every
Wcash winner
(about one target share per 75 seconds at the calibrated launch rate) while
sampling only the Zcash winners that also meet it. This deliberately degrades
parent-winner coverage; it does not change either chain's consensus target or
the coordinator's independent winner classification.

Inspect each generation's preflight output before connecting hardware. It must
show `share_target_policy.mode` as `testnet_parent_target_sampling`,
`captures_all_wcash_winners` as `true`, and
`degraded_parent_coverage` as `true` while the parent target is easier. The
process also emits an explicit warning. A sampled network winner is validated
and synchronously recorded in the durable outbox before its ASIC ACK; the
listener then stops admitting that generation, joins in-flight handlers, and
explicitly flushes pending chain submissions before retirement or fresh work.
Crash recovery retains the same exact-byte outbox replay guarantee.
Do not increase the per-connection 64-submission-per-second safety cap. Replace
this legacy configuration with `WCASH_SHARE_TARGET=network` for full coverage.

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

The checked-in [`wcash-testnet.toml`](../wcash-testnet.toml) is a first-seed
baseline, not a list of live services. It exposes
P2P on port 38233, keeps RPC on loopback port 38232 with cookie authentication,
uses persistent network-isolated state, and never enables
`debug_force_finished_sync`. Its initial peer lists are empty. Obtain currently
supported Wcash Testnet peers from the operator and verify their network
identity, or coordinate inbound connections with another operator. Add explicit
peers to `initial_testnet_peers`; an empty list is not evidence that the node has
joined a public chain. Never add a Zcash seed or copy Zcash peer-cache data.

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
recoverable. The current coordinator requires a configured read-only incoming
viewing key to trial-decrypt the private coinbase and verify the exact recipient.
Supply that privacy-sensitive capability through its protected file interface,
never through arguments, environment variables or logs. See
[the coordinator configuration](../wcash-merge-miner/src/coordinator.rs).

## Reproduce the local mining and spend E2E

This script performs real proof-of-work solving and belongs on authorized local
test hardware. The checked-in hosted workflow uses fixed Equihash and AuxPoW
vectors instead of solving work. Run both kinds of checks for a release and
retain their results with the exact revision, features and artifact hashes;
the presence of a workflow file does not establish a successful run.
It uses ephemeral, loopback-only profiles with no peers and no cookie
authentication. The first Wcash Testnet-profile phase does not use a
`debug_force_finished_sync` override; the isolated Regtest wallet and parent
profiles do use that explicit test-only shortcut. The script also enables the
mempool at genesis and exposes loopback lightwalletd gRPC so the controlled
spend path is deterministic without public peers. None of those Regtest settings
may be copied into pool operations.

The first phase boots the frozen Testnet profile, asserts its identity, zero
initial supply, hardened target, AuxPoW candidate lifecycle, and proposal gate,
then verifies an authenticated ZIP-301 job transcript without attempting to
CPU-mine the ASIC-calibrated public target. The second phase boots a separate
Wcash Regtest node, uses the bundled reference miner to solve real Equihash
`(200, 9)`, requires consensus acceptance by Wcash and both isolated Zcash
Regtest parent nodes, explicitly selects private Ironwood payout, and exercises
the controlled wallet lifecycle. The coordinator and Wcash AuxPoW verifier
independently check every real solution. The isolated Zcash Regtest profiles
disable their native proof-of-work check, so their role in this gate is exact
parent block structure, target, proposal, and chain acceptance rather than a
second Equihash verification. The third phase boots a fresh
Wcash Regtest node, mines 101 transparent coinbases, and exercises the inherited
100-block maturity and mandatory-shielding path without reducing confirmations.
Bounded template retries cover asynchronous RPC, state, mempool, and
proposal-service startup.

```sh
scripts/build-wcash-testnet-binaries.sh
scripts/wcash-testnet-e2e.sh
```

The script checks that the frozen Testnet profile boots and that a
proposal-validated ZIP-301 job survives exact construction and authenticated
delivery without submitting synthetic public Testnet work. Its controlled
Regtest phase carries real Equihash/AuxPoW solutions through Wcash and both
Zcash validators, mines and scans three private 6.25-TWC coinbases, signs a
one-TWC V6 transfer under `0xc3a6678a`, observes the exact bytes and fee in the
mempool and `getblocktemplate`, checks duplicate broadcast, and requires both
standard Zcash Regtest nodes to reject those Wcash bytes. It mines the transfer
in a fourth AuxPoW block, rescans recipient and change, and requires all 25 TWC
to remain in Ironwood while the other pools remain zero.

The transparent phase requires all 99 rewards to remain pending at tip 99 and
rejects premature shielding. At tip 100, only the height-1 reward is spendable
by a height-101 transaction. A second wallet initialized with birthday 100 must
recover all 100 current coinbase UTXOs through the separate transparent scan.
The primary wallet persists and broadcasts the exact V6 transaction shielding
one mature coinbase, matches it in the mempool and template, and mines it at
height 101. Wallet and node checks require exactly 631.25 TWC across transparent
and Ironwood pools. Processing 101 child jobs also exercises candidate release
beyond the coordinator's 16-entry active cache.

Only the private Regtest transfer deliberately uses the explicit unsafe
one-confirmation test override. Transparent coinbase maturity remains 100
blocks. The public Testnet wallet policy remains 100 confirmations, and these
local checks do not establish deployed public-network or production-wallet
behavior. A release report must identify which revision actually passed.

## Controlled-key wallet and recovery boundary

The spendability gate uses fresh ephemeral state and deterministic test seeds
supplied outside command-line arguments and logs. The experimental wallet used
here performs one-shot operations against an attested loopback Wcash node.
Its CLI also supports explicitly selected Mainnet and Testnet; those public
profiles require at least 100 confirmations. `derive-address` reports a private
Unified Address and its related transparent P2PKH coinbase address.
Synchronization classifies transparent
coinbase history, balance output separates mature and pending coinbase value,
and `shield-coinbase` can build and persist one bounded V6 sweep of mature
coinbase inputs into the wallet's Ironwood receiver. The wallet also provides
one-shot Ironwood transfers, recovery by exported transaction ID, bounded and
paginated `list-pending` recovery with exact signed bytes, exact-byte broadcast,
and transaction-status inspection. It requires one exclusive writer per wallet
database. After any ambiguous post-sign outcome, an operator must enumerate all
pending pages, inspect status, and rebroadcast the persisted bytes; constructing
a replacement before resolving those records is unsafe. The protocol-v2 payout
interface provides a separate crash-idempotent batch-signing boundary on
Mainnet and Testnet. Regtest payout commands require `regtest-payout`. Pool
accounting, eligibility and settlement remain the pool's responsibility; see
[`wcash-wallet-payout.md`](wcash-wallet-payout.md).

Every newly initialized wallet database stores a Wcash-owned identity record
before the librustzcash schema is opened or migrated. The record binds the file
to the selected Wcash network, exact genesis hash, seed-KDF version, and
transaction branch ID, and all four values are checked on every open. Wallet
databases from retired profiles may have incompatible genesis, transaction
domains or seed-derived keys. Preserve the original database and seed backup;
do not edit its identity fields, delete it, or assume a same-seed restore on a
new Testnet recovers the old chain's funds. Identify its exact profile and use a
reviewed migration or a separate new-network database. Verify recovery before
retiring any backup.

On Unix, create the database beneath a directory owned by the wallet process's
effective UID and not writable by a group or other users. The full canonical
ancestor chain must also prevent another local account from replacing that
directory. New wallet files are created atomically with mode `0600`. An
existing file must be owned by the effective UID, have private permissions,
and have exactly one hard link. The wallet retains the securely opened file
while SQLite opens it and checks that the pathname still identifies the same
device and inode. Unsafe ownership, permissions, links, or replacement are
rejected instead of being silently repaired; correct them only after
confirming the intended file and path.

The script's controlled Regtest assertions cover this path:

1. It derives separate miner and recipient addresses in the Wcash Regtest
   namespace, mines three real AuxPoW child blocks to the controlled miner, and
   scans all three private 6.25-TWC coinbases.
2. It constructs a balanced one-TWC V6 Ironwood transfer under the Regtest
   branch ID `0xc3a6678a`, including verified private change, fee, and expiry
   height. It uses the explicit Regtest-only unsafe one-confirmation override; the public
   Testnet wallet policy remains 100 confirmations.
3. It submits the exact signed bytes, observes the transaction in the Wcash
   mempool, checks duplicate-broadcast behavior, and matches its exact bytes and
   fee in `getblocktemplate`.
4. Both standard Zcash Regtest nodes must reject the exact Wcash transaction and
   exclude it from their mempools. Consensus and cross-crate integration
   tests separately enforce exact two-way Wcash/Zcash branch-domain rejection
   and reject Wcash post-genesis V1-V5 transactions.
5. It mines and accepts the transfer in a fourth real AuxPoW child block,
   observes its mined status, and independently rescans the sender and
   recipient wallets.
6. The recipient must hold exactly one TWC, the sender 24 TWC, and the
   chain exactly 25 TWC entirely in Ironwood, with transparent,
   Sapling, and Orchard balances remaining zero.
7. A fresh wallet and chain mine 101 transparent coinbases through AuxPoW,
   rotating well beyond the coordinator's 16-entry active-candidate cache.
8. At tip 99, all 99 rewards must remain pending and `shield-coinbase` must fail.
   At tip 100, only the height-1 reward is mature for the height-101 transaction.
9. A wallet initialized with birthday 100 recovers every one of the 100 current
   transparent coinbase UTXOs through the separate current-UTXO scan.
10. The primary wallet persists and broadcasts the exact shielding transaction,
    the node independently decodes its one transparent input, zero transparent
    outputs, and two-action Ironwood bundle, and the template contains those
    same bytes and fee.
11. AuxPoW block 101 mines the transaction. The wallet and chain must independently
    report 631.25 TWC total across transparent and Ironwood, with zero value
    in Sapling and Orchard.

These assertions cover more than transaction serialization, but a successful
local run does not supply Internet-facing pool security or reorg-safe
settlement. The wallet payout interface additionally supports Ironwood-funded
transparent P2PKH outputs (`W1...` on Mainnet), with public recipient/amount,
empty transparent memos, and Ironwood change. That capability does not allow a
transparent coinbase to bypass mandatory shielding. The pool must check the
exact signer's `payout-capabilities`, freeze eligible allocations across reorgs,
and recover the same stored bytes after an uncertain result.

## Deployment and release evidence

Mainnet implementation, a Testnet profile and a local test harness are separate
from deployment evidence. Verify which Testnet generation an endpoint serves,
its observation time, peers, tip and recent block activity. A reachable indexer
with zero lag may be following a paused or retired chain. This source document
does not certify the availability of a public Testnet, project-operated seeds,
physical ASIC firmware, production wallets or a pool service.

Before exposing a service, record the exact node/coordinator/pool/wallet release
pairing and its test results. Exercise public-network confirmation policy,
multi-node and reorg behavior, ASIC interoperability, private-recipient
verification, payout recovery, backups and incident procedures. The native
coordinator is one component; Internet-facing authentication/TLS, variable
difficulty, account balances and settlement need the separately reviewed pool
deployment. Consult [`SECURITY.md`](../SECURITY.md) for vulnerability reporting.

The independent Zcash parent coinbase follows Zcash rules. A private Wcash
child reward requires an explicitly selected Unified Address and the protected
recipient-verification capability. That does not make a Zcash parent coinbase
private: its shielded coinbase is recoverable under Zcash's applicable rules.
Do not infer miner balances from raw share counts. The pool's accounting and
settlement service must consume authenticated backend evidence and enforce each
chain's independent acceptance, maturity and reorg rules.
