# Wcash protocol direction

This document records the maintainer-approved direction for Wcash. It is the
short review contract for changes to Wolf: contributors should preserve these
properties unless a separate, explicit consensus proposal replaces one of them.
The implementation and frozen tests remain authoritative for exact consensus
bytes and arithmetic; this document explains the intended design those rules
serve.

## One independent chain, merge-mined

Wcash is an independent Zcash-derived chain, not a token on Zcash and not an
alternate name for ZEC. It has its own genesis, transaction domain, difficulty,
state and monetary policy.

Wcash reuses Zcash Equihash `(200, 9)` work through AuxPoW v2. A coordinator
commits the Wcash child block into a parent coinbase, but each chain still checks
the resulting work against its own target. A valid Wcash winner does not have to
be a valid Zcash winner, and ordinary Zcash mining software does not support
Wcash unless it implements the Wcash child protocol. The exact coordinator and
RPC contract is documented in [Wcash merged mining](wcash-merged-mining.md).

This design lets miners search for both outcomes with the same work while
keeping Wcash consensus, issuance and chain selection independent.

## Predictable, miner-funded issuance

Mainnet monetary policy has four deliberate properties:

1. **A 40,000-block linear slow start.** Genesis issues nothing. Subsidy rises
   from zero to the curve's initial reward instead of creating a launch cliff.
2. **Monero-style smooth emission after the ramp.** Each reward is computed from
   the remaining integer curve scale, so issuance declines gradually rather
   than through periodic halvings. "Monero-style" describes the smooth-decay
   design family; Wcash has its own constants, ramp and recurrence and does not
   adopt Monero's block-weight penalty.
3. **A permanent 0.375 WEC tail emission.** The subsidy never decays below
   37,500,000 atomic units. Mainnet therefore has no fixed maximum supply.
4. **No protocol tax.** There is no founders reward, development tax, funding
   stream, lockbox disbursement or premine allocation in the Wcash subsidy.
   Subsidy plus transaction fees belongs to the miner's coinbase.

The consensus recurrence, boundary heights and fixed vectors are in
[`wcash_mainnet.rs`](../zebra-chain/src/parameters/network/subsidy/wcash_mainnet.rs).
The curve scale is an input to the recurrence, not a supply cap. Slow-start
issuance is real circulating supply but is intentionally excluded from the
post-ramp decay counter. Fees move existing value and do not advance that
counter. See the [consensus reference](wcash-consensus.md) for the exact integer
formula and testing-network differences.

## Two active value pools

Wcash keeps exactly two active value pools:

- **Transparent**, for public amounts and recipients; and
- **Ironwood**, for shielded value.

Sprout, Sapling and legacy Orchard components are not additional Wcash pools and
are rejected in post-genesis Wcash transactions. Ironwood reuses
Orchard-family types, circuits and receiver payloads where the implementation
requires them, but it has a separate Wcash transaction domain, state, note tree,
nullifiers and value-pool identity. A Wcash Unified Address can therefore carry
an Orchard-shaped receiver payload for Ironwood without enabling the legacy
Orchard pool.

This boundary is intentional: Wcash should not accumulate a deprecated zoo of
shielded pools. Reusing cryptographic machinery is not permission to expose an
old pool as a Wcash feature.

A miner chooses one coinbase payout mode for a block: transparent or private
Ironwood. The two modes cannot be mixed in the same coinbase. A mature
transparent coinbase must be shielded before its value can later return to a
transparent recipient. These rules keep a transparent integration path while
making Ironwood the single privacy pool.

## Review rules derived from this direction

A change needs explicit maintainer design approval, consensus vectors and
independent review if it would:

- replace or bypass AuxPoW, or let the parent target define Wcash validity;
- change the slow start, smooth-decay recurrence, tail subsidy or fee treatment;
- add a protocol allocation, premine, founders reward, development tax, funding
  stream or lockbox payment;
- activate Sprout, Sapling, legacy Orchard or another shielded value pool;
- merge transparent and Ironwood coinbase payout modes; or
- reuse a Zcash network identity, transaction domain or address namespace.

Documentation, wallet, explorer and pool work must describe these boundaries
without claiming that source support proves a particular public deployment is
available or production-ready. Testing networks may deliberately use faster or
simpler schedules for tests; their values must not be presented as Mainnet
policy.

## Implementation map

| Direction | Source and tests |
| --- | --- |
| Frozen identities and external anchors | [`wcash-genesis`](../wcash-genesis/src/lib.rs) and [genesis blocks](../zebra-chain/src/block/genesis.rs) |
| Mainnet slow start, smooth decay and tail | [`wcash_mainnet.rs`](../zebra-chain/src/parameters/network/subsidy/wcash_mainnet.rs) |
| No protocol allocation | [network subsidy dispatch](../zebra-chain/src/parameters/network/subsidy.rs) |
| Wcash V6 and Ironwood domain | [network upgrades](../zebra-chain/src/parameters/network_upgrade.rs) and [transaction validation](../zebra-consensus/src/transaction.rs) |
| Active value-pool accounting | [value balances](../zebra-chain/src/value_balance.rs) and [state accounting](../zebra-state/src/service/non_finalized_state/chain.rs) |
| AuxPoW proof format | [`wcash-zcash-aux`](../wcash-zcash-aux/README.md) |
| Child/parent mining coordination | [`wcash-merge-miner`](../wcash-merge-miner/README.md) and [mining contract](wcash-merged-mining.md) |
| Wcash release checks | [release gate](../.github/workflows/wcash-release-gate.yml) |

When prose and a pinned implementation disagree, stop and resolve the mismatch;
do not silently reinterpret consensus from this summary.
