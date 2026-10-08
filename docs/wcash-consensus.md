# Wcash consensus snapshot

This document describes the built-in Wcash Mainnet, Testnet and Regtest rules
implemented in this source tree. Mainnet has a frozen genesis and an enabled
consensus profile. A source snapshot does not establish which binary a public
service runs, whether an endpoint is healthy, or whether a release has completed
independent security review. Verify those separately before operating a node,
wallet or pool.

Mainnet uses `WEC`; Testnet and Regtest use the valueless display ticker `TWC`
(Test Wcash). One unit is exactly 100,000,000 zatoshi on every network. Consensus
uses integer amounts, not floating-point coin values. Tickers are presentation
metadata and do not change a frozen genesis.

## Network identity and activation

`Network::try_new_wcash_mainnet()`, `Network::new_wcash_testnet()` and
`Network::new_wcash_regtest()` select distinct genesis blocks, transaction
domains, P2P identities and state/cache namespaces. All three activate NU6.3 /
Ironwood at height 1. The inherited genesis transaction is a special height-zero
case with one zero-valued transparent output and no spendable supply.

| Property | Mainnet | Testnet | Regtest |
| --- | --- | --- | --- |
| Node configuration name | `WcashMainnet` | `WcashTestnet` | `WcashRegtest` |
| P2P identity | `Wcash/mainnet/v1` | `Wcash/testnet/v7` | `Wcash/regtest/v5` |
| P2P magic, byte order as sent | `c1e0dc1b` | `49d2934b` | `4c237261` |
| Default P2P port | 48233 | 38233 | 28233 |
| Recommended loopback RPC port | 48232 | 38232 | 28232 |
| State/cache network namespace | `wcashmainnet-v1` | `wcashtestnet-v7` | `wcashregtest-v5` |
| Transaction branch ID | `d9c6a7ee` | `54ba2bfb` | `c3a6678a` |

RPC is disabled until configured; the recommended ports do not open listeners.
Wcash profiles do not inherit Zcash DNS seeds. Verify the network identity of
every peer, wallet service and mining backend selected for a network.

Mainnet freezes Zcash Mainnet block 3,488,810, hash
`0000000000463e4317bf50101ab176fe592298888a432f71d71f2fad04755e49`.
Its Wcash genesis timestamp is 2026-09-19 12:00:00 UTC (`1789819200`), and its
display-order genesis hash is
`5bae12c8662a577b04ce1591af1a137c128f0cb51018a5f1622d861d1bb6fc48`.

Testnet freezes Zcash Testnet block 4,362,016, hash
`00000e289ad21d2feeb17e16585790ecabec94323c2ef22925534ad104de73ac`.
Its Wcash genesis timestamp is 2026-09-17 23:00:00 UTC (`1789686000`), and its
display-order genesis hash is
`6b66fff119977d36d9c989093b516a876dbf6596536791ff35bb4c581e3fda98`.
The transaction domain is named Testnet v3 while its P2P/cache identity is v7;
these are separate version identifiers. Match the full identity, not a version
suffix or service hostname alone.

Regtest freezes Bitcoin Mainnet block 965,910, hash
`00000000000000000000bbbdb28d2ff098642c6fde0a5fd84a707c92d146b146`.
Its display-order Wcash genesis hash is
`70bf0bab17eff361a6331bb825b3b7253c8c96ff96407f948161d2912658bb1c`.
The exact Bitcoin header is retained as a local test vector. Public Mainnet and
Testnet instead use their separately frozen Zcash anchors. Consensus never
fetches an external chain to choose or replace an anchor at runtime; anchor
overrides are restricted to Regtest.

### Transaction version and replay domain

Every post-genesis Wcash transaction must use version 6 and its selected
network's branch ID. That ID participates in transaction IDs, signature hashes,
shielded authorization, parsing, block checks and chain-history commitments.
Validators compare the exact domain rather than treating shared NU6.3 features
as sufficient.

Wcash rejects post-genesis V1–V5 transactions and inherited Zcash domains.
Mainnet, Testnet and Regtest reject each other's transaction domains; Wcash V6
transactions are also rejected by the standard Zcash verifier. Standard Zcash
NU6.3 uses `37a5165b`, while retired Wcash Testnet v1 used `b3cfd27e`. P2P magic
and address prefixes are additional network separation, not substitutes for
transaction-domain checks. Both block and mempool verification enforce these
rules.

## Monetary policy

### Mainnet: slow start, smooth decay and permanent tail

Mainnet has no halvings and no fixed economic maximum supply. The normative
integer recurrence is in
[`wcash_mainnet.rs`](../zebra-chain/src/parameters/network/subsidy/wcash_mainnet.rs).
All values below are atomic zatoshi unless stated otherwise:

```text
M = 1,844,674,407,370,955
RAMP_PEAK = 2,199,023,255
TAIL = 37,500,000

height 0..40,000:
  subsidy = floor(RAMP_PEAK * height / 40,000)
  curve counter = 0

height >40,000:
  subsidy = max(floor(5 * (M - parent curve counter) / 4,194,304), TAIL)
  next curve counter = min(M, parent curve counter + subsidy)
```

Genesis therefore has zero subsidy. Height 40,000 reaches 21.99023255 WEC;
height 40,001 pays the same amount before the smooth decline. Ramp issuance is
real supply but is excluded from the curve counter. `M` is an emission parameter,
not a supply cap. The first canonical tail block is height 3,455,360; its reward
and every later reward is 0.375 WEC. Fees transfer existing value and never
advance the curve counter. Each coinbase must claim exactly subsidy plus fees.

### Testing networks: separate halving schedules

Testnet and Regtest retain testing schedules which must not be used to describe
Mainnet. Their implementation is in
[`subsidy.rs`](../zebra-chain/src/parameters/network/subsidy.rs).

| Rule | Testnet | Regtest |
| --- | --- | --- |
| Genesis subsidy | 0 | 0 |
| Slow start | `floor(625,000,000 * height / 40,000)` zatoshi through height 40,000 | None |
| Full initial subsidy | 6.25 TWC | 6.25 TWC from height 1 |
| First halved block | 1,700,000 | 1,680,001 |
| Later halving interval | 1,680,000 blocks | 1,680,000 blocks |

After Testnet's ramp, the subsidy is `625,000,000 >> k`, where
`k = floor(max(height - 20,000, 0) / 1,680,000)`. Regtest uses
`k = floor(max(height - 1, 0) / 1,680,000)` for non-genesis blocks. Right shifts
discard fractional zatoshi. Neither testing schedule has Mainnet's permanent
tail.

### Value bounds and allocation

The `wcash-consensus` build uses `i64::MAX / 2` as the finite technical aggregate
amount bound, leaving checked-arithmetic headroom for the permanent tail. This
is distinct from economic supply policy. Some inherited transaction-library
value types retain a separate 21-million-unit bound, and Wcash explicitly uses
that smaller bound for total coinbase input. It is not a general cap on every
balanced non-coinbase transaction under the Wcash aggregate bound. The
non-Wcash build retains Zcash's aggregate 21-million-unit limit. See
[`amount.rs`](../zebra-chain/src/amount.rs); changing a transaction bound to match
a website's supply label would change a different rule.

Wcash profiles clear founders rewards, funding streams and lockbox
disbursements. The complete subsidy plus fees belongs to the miner coinbase,
subject to the payout-mode rules below. This allocation rule does not establish
who mined the initial supply or the fairness of an operational launch.

## Block timing, difficulty and AuxPoW

The target spacing is 75 seconds. Mainnet and Testnet start at compact
proof-of-work limit `0x1e2fabe8` and use the inherited damped 17-block retarget.
The launch ceiling is 50 times the target encoded by the reference Zcash
Testnet compact target `0x1e00f414`. Regtest uses its fixed proof-of-work limit.

Only Testnet permits a proof-of-work-limit block when the candidate timestamp
is strictly more than 450 seconds after its predecessor; exactly 450 seconds
does not trigger that exception. Mainnet has no such minimum-difficulty
exception. Maximum block-time enforcement applies from height 1 on Testnet and
throughout Mainnet. Template construction uses the candidate height, including
the height-1 boundary.

Wcash validates Equihash `(200, 9)` AuxPoW against its own child target. The
Wcash header's compact difficulty is authoritative for Wcash; a parent header's
`nBits` cannot select an easier child target. A proof meeting Wcash's target
need not meet Zcash's target or appear in Zcash's chain. The coordinator checks
parent-target qualification and submits to each chain independently. See
[`wcash-merged-mining.md`](wcash-merged-mining.md).

## Coinbase payout modes

For every height above genesis, a Wcash coinbase can pay either an explicit
transparent Wcash address or a Wcash Unified Address with an Orchard receiver
for private Ironwood note encryption. The template builder never invents a
runtime payout address. It rejects inherited Zcash and wrong-network addresses;
standalone Sapling and TEX addresses are not coinbase destinations. Transparent
and Ironwood miner payouts cannot be mixed in one coinbase.

Transparent and Ironwood are the active value pools. Consensus rejects Sprout,
Sapling and legacy Orchard components in Wcash transactions. The Orchard
receiver payload in a Unified Address is used to address Ironwood; this does
not activate the legacy Orchard value pool.

Transparent coinbases mature after 100 blocks. A transaction spending them must
not create transparent outputs, so a pool must shield such value before later
transparent settlement. Optional Ironwood coinbase encryption uses no outgoing
viewing key, and consensus rejects outputs recoverable with Zcash's conventional
all-zero outgoing viewing key. Gross subsidy, fees and shielded value balances
remain auditable; parent-pool metadata can create off-chain correlation.

The native coordinator can verify an exact private recipient by trial-decrypting
every Ironwood action with a configured read-only incoming viewing key. That
capability cannot spend funds but is privacy-sensitive and belongs in a
protected file. This check does not remove the need to protect the node, RPC
credentials, configured payout address and pool signer.

## Payment-address domains and wallet support

Wcash reuses receiver payload formats with separate textual namespaces:

| Receiver | Mainnet | Testnet | Regtest |
| --- | --- | --- | --- |
| Unified | `wu1...` | `wutest1...` | `wuregtest1...` |
| Transparent-source-only P2PKH (TEX) | `wtex1...` | `wtextest1...` | `wtexregtest1...` |
| Transparent P2PKH | `W1...` | `WT...` | `WR...` |
| Transparent P2SH | `W3...` | `WU...` | `WS...` |

All three networks have implemented address codecs. Former Sapling namespaces
remain reserved and rejected. TEX encodes a transparent P2PKH receiver, not a
separate value pool. Unified Addresses bind their Wcash human-readable part into
both ZIP 316 jumbling and the Bech32m checksum; changing a Zcash string's visible
prefix does not produce a valid Wcash address. Parse an address with the
network-specific codec rather than classifying it by a prefix alone.

The repository's `wcash-wallet` explicitly selects Mainnet, Testnet or Regtest.
It derives addresses, synchronizes and inspects wallet state, shields mature
transparent coinbases, signs Wcash V6 transactions and broadcasts persisted
bytes. Its protocol-v2 payout signer supports Mainnet and Testnet; Regtest payout
commands require the `regtest-payout` feature.

The payout signer supports Ironwood-funded transfers to shielded Ironwood
receivers and optional transparent P2PKH receivers (`W1...` on Mainnet).
Transparent payouts expose recipient and amount and require an empty memo.
P2SH/W3, TEX, inherited Zcash and wrong-network destinations are rejected by
that payout interface even though some are recognized by other address codecs.
Inputs and internal change remain Ironwood. This is a wallet capability change,
not a change to coinbase maturity or the mandatory-shielding rule.

Keep one exclusive writer per wallet database. The versioned signer atomically
binds each ordered batch to its exact signed bytes. After an ambiguous outcome,
recover and inspect the stored operation and transaction status; retry the same
bytes instead of creating an unverified replacement. Generic pending operations
are available through bounded, paginated `list-pending` records. The signer does
not replace the pool's accounting, allocation eligibility or reorg-safe
settlement. See [`wcash-wallet-payout.md`](wcash-wallet-payout.md).

## Validation and operational evidence

Source tests cover frozen identities, exact transaction domains, subsidy
boundaries, AuxPoW and coinbase checks. The local
[`wcash-testnet-e2e.sh`](../scripts/wcash-testnet-e2e.sh) exercises the built-in
Testnet bootstrap and isolated Regtest private-transfer and transparent-coinbase
shielding paths. Its Regtest assertions include 100-block coinbase maturity and
631.25 TWC conserved across 101 blocks. Those are testing-network assertions and
must not be substituted for Mainnet emission or current release evidence.

For a release, retain results tied to its exact revision, features and artifact
digests. Verify public deployment, peers, synchronization, ASIC compatibility,
multi-node/reorg behavior, backup recovery, payout accounting and independent
review separately. The existence of source tests, a frozen identity or a
running endpoint does not certify these properties. Report suspected
vulnerabilities through [`SECURITY.md`](../SECURITY.md).

The `wcash-consensus` runtime accepts only Wcash network configurations. A
separately built non-Wcash profile is used for standard Zcash nodes, including
the parent validators in local tests. Wcash header wire version `0xd7430001`
distinguishes its variable-length witness before a `Network` is available;
its set high bit is forbidden in valid native Zcash headers.

## Source reference

- [External anchors and P2P identities](../wcash-genesis/src/lib.rs)
- [Frozen genesis blocks](../zebra-chain/src/block/genesis.rs)
- [Network parameters, ports and namespaces](../zebra-chain/src/parameters/network.rs)
- [Built-in consensus profiles](../zebra-chain/src/parameters/network/testnet.rs)
- [Transaction branch IDs](../zebra-chain/src/parameters/network_upgrade.rs)
- [Mainnet emission](../zebra-chain/src/parameters/network/subsidy/wcash_mainnet.rs)
- [Testing-network subsidy](../zebra-chain/src/parameters/network/subsidy.rs)
- [Wallet network selection](../wcash-wallet/src/network.rs)
- [Private recipient verification](../wcash-merge-miner/src/coordinator.rs)

For Testnet bootstrap and local test procedures, see
[`wcash-testnet.md`](wcash-testnet.md).
