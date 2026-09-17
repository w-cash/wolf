# Wcash Testnet profile

The source tree contains a frozen Wcash engineering and mining-interoperability
Testnet profile so independently built nodes can agree on the same height-zero
block. Wcash mainnet remains disabled and
testnet coins must have no monetary value.

User-facing Testnet balances use `TWC` (Test Wcash) rather than mainnet `WEC`.
This label does not alter the eight-decimal integer-zatoshi encoding.
The public Testnet genesis statement identifies its Zcash Testnet anchor. Testnet v3 changes
the genesis, transaction domain, P2P identity, and cache namespace, so it is distinct
from every retired testing chain.

> **Transaction-domain test boundary:** Wcash Testnet accepts only post-genesis V6
> transactions carrying its chain-specific branch ID `0x54ba2bfb`. Zcash,
> the retired Testnet v1 domain `0xb3cfd27e`, the distinct Wcash Regtest domain
> `0xc3a6678a`, and V1-V5 are rejected. The controlled private-transfer and
> transparent-coinbase shielding lifecycles have passed on isolated Regtest, but
> no coin has monetary value and those local tests do not
> make the wallet, a public Testnet, a mining pool, or payout/settlement system
> production ready.

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

## Full Z15 coverage of both networks

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

The passing run proved that the frozen Testnet profile boots cleanly and that a
proposal-validated ZIP-301 job with the hardened launch target survives exact
construction and authenticated delivery without submitting synthetic public
Testnet work. In its controlled Regtest phase, the script carried real
Equihash/AuxPoW solutions through Wcash and both Zcash validators, mined and
scanned three private 6.25-TWC coinbases, signed a one-TWC
V6 transfer under the Regtest branch ID `0xc3a6678a`, observed the exact
transaction bytes and fee in the mempool and `getblocktemplate`, checked duplicate-broadcast
behavior, and required both standard Zcash Regtest nodes to reject those Wcash
bytes. It then mined the transfer in a fourth real AuxPoW block, rescanned the
recipient and private change, and verified that all 25 TWC remained in
Ironwood while the other pools remained zero.

The transparent phase reported all 99 rewards pending at tip 99 and rejected
premature shielding. At tip 100, only the height-1 reward was spendable by a
height-101 transaction. A second wallet initialized with birthday 100 recovered
all 100 current coinbase UTXOs, proving that transparent recovery does not depend
on the shielded birthday. The primary wallet persisted and broadcast the exact
V6 transaction that shielded one mature coinbase, matched it in the mempool and
block template, and mined it through AuxPoW at height 101. Final wallet and node
checks conserved exactly 631.25 TWC across the transparent and Ironwood pools.
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

Every newly initialized wallet database stores a Wcash-owned identity record
before the librustzcash schema is opened or migrated. The record binds the file
to the selected Wcash network, exact genesis hash, seed-KDF version, and
transaction branch ID, and all four values are checked on every open. Wallet
databases created before this record existed, including the pre-launch Testnet
v4 profile, are intentionally not migrated in place because the v5 genesis also
changed the keys derived from the same seed. Move an old file aside, initialize
a new v5 database from the seed, and rescan; retain the old file as a backup
until the rescan has been independently checked.

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

The passing automated run established this controlled local path:

1. It derived separate miner and recipient addresses in the Wcash Regtest
   namespace, mined three real AuxPoW child blocks to the controlled miner, and
   scanned all three private 6.25-TWC coinbases.
2. It constructed a balanced one-TWC V6 Ironwood transfer under the Regtest
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
6. The recipient held exactly one TWC, the sender retained 24 TWC, and the
   chain reported exactly 25 TWC entirely in Ironwood, with transparent,
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
    reported 631.25 TWC total across transparent and Ironwood, with zero value
    in Sapling and Orchard.

This local pass goes beyond transaction serialization and wallet unit tests. It
does not supply Internet-facing pool security, miner settlement, or reorg-safe
accounting. The wallet now supplies the isolated crash-idempotent Testnet batch
signing and exact-byte broadcast boundary documented in
[`wcash-wallet-payout.md`](wcash-wallet-payout.md); the pool ledger must still
freeze eligible allocations across reorgs before authorizing a batch.

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
