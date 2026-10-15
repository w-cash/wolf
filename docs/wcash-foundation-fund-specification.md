# Foundation allocation specification

Wcash Mainnet allocates 30% of each block subsidy to a fixed transparent
recipient for a finite term. The temporary fund supports development,
infrastructure and ecosystem integrations. Miners receive the remainder and all
transaction fees, with a choice of transparent or private Ironwood payout. The allocation
does not change gross emission. See the [operator upgrade guide](wcash-foundation-fund-upgrade.md)
for database migration, release checks and local reproduction.

## Mainnet policy

| Parameter | Value |
| --- | --- |
| First funded block | 33000 inclusive |
| Last funded block | 621454 inclusive |
| First unfunded block after expiry | 621455 |
| Allocation | 30%: `floor(3 * subsidy / 10)`, separately for each block |
| Transaction fees | Entirely to the miner |
| Recipient | `W1ZRudbQKz7jQdKmAarbaJsegscnZzXjpu2`, fixed Mainnet P2PKH |
| Foundation payout | Transparent in both miner modes |
| Miner payout | Transparent or private Ironwood |
| Renewal | Separate consensus upgrade; no automatic extension |
| Public Testnet and ordinary Regtest | No allocation |

For candidate height `h`, gross subsidy `S(h)` and validated aggregate fees `F`:

```text
active(h) = 33000 <= h < 621455
fund(h) = floor(3 * S(h) / 10) if active(h), otherwise 0
miner_subsidy(h) = S(h) - fund(h)
miner_payout(h) = miner_subsidy(h) + F
total_coinbase_value(h) = S(h) + F
```

All arithmetic uses integer atomic units: 100000000 atoms = 1 WEC. Multiply by
three in a `u128` intermediate before dividing by ten; dividing the subsidy
first would change the rounded result. Division remainders belong to miners.
Fees are excluded from the foundation calculation and emission curve counter.
After slow start, that counter advances by the full gross subsidy.
The 40000-block slow start, smooth decay, permanent tail, difficulty, AuxPoW,
active value pools and transaction amount limits retain their existing rules.

### Expiry

The allocation starts during slow start at height 33000. The first post-slow-start
block remains 40001, paying 2199023255 atoms, or 21.99023255 WEC. Its
mathematical half is 1099511627.5 atoms. Height 621455 is the first subsequent
height satisfying `2 * S(h) <= 2199023255`: subsidy is 1099512482 atoms at
621454 and 1099511171 atoms at 621455.

Activation and expiry are fixed consensus heights, independent of wall time.
The term contains 588455 funded blocks, including 7001 slow-start blocks at
heights 33000–40000. Slow-start issuance remains excluded from the post-ramp
curve counter. At the 75-second target spacing, the activation-to-expiry interval
is approximately 510.812 days; actual dates depend on block production.
The subsidy continues its smooth decay at expiry while the
allocation stops. Existing foundation outputs remain spendable under ordinary
maturity and transaction rules.

### Golden vectors

Amounts below are atoms with zero fees. Add all fees to the miner column.

| Height | Gross subsidy | Foundation | Miner subsidy |
| --- | --- | --- | --- |
| 32999 | 1814139209 | 0 | 1814139209 |
| 33000 | 1814194185 | 544258255 | 1269935930 |
| 33001 | 1814249160 | 544274748 | 1269974412 |
| 39999 | 2198968279 | 659690483 | 1539277796 |
| 40000 | 2199023255 | 659706976 | 1539316279 |
| 40001 | 2199023255 | 659706976 | 1539316279 |
| 40002 | 2199020634 | 659706190 | 1539314444 |
| 621454 | 1099512482 | 329853744 | 769658738 |
| 621455 | 1099511171 | 0 | 1099511171 |
| 621456 | 1099509860 | 0 | 1099509860 |

Totals across heights 33000–621454 inclusive are:

| Allocation | Atoms | WEC |
| --- | --- | --- |
| Gross subsidy | 936385853940821 | 9363858.53940821 |
| Foundation | 280915755917299 | 2809157.55917299 |
| Miner subsidy before fees | 655470098023522 | 6554700.98023522 |

The foundation total sums each block's rounded allocation. In general,
`sum(floor(3 * S / 10))` differs from `floor(3 * sum(S) / 10)`.

## Recipient

```text
address:                    W1ZRudbQKz7jQdKmAarbaJsegscnZzXjpu2
network:                    Wcash Mainnet
script type:                P2PKH
Base58Check version (hex):  1055
pubkey hash:                8708e78a0203f93b2691057281e1773fd2637020
scriptPubKey:               76a9148708e78a0203f93b2691057281e1773fd263702088ac
script bytes:               25
checksum (hex):             7a6f13f5
```

The destination is fixed for the entire term. Runtime configuration, RPC,
environment variables and command-line options cannot redirect it. Changing
the destination, including after key loss or compromise, requires another
consensus upgrade. [wcash.foundation](https://wcash.foundation) is informational;
consensus does not resolve recipients through a website or DNS.

This script is P2PKH, not on-chain multisig. Address encoding does not establish
legal ownership or key custody. Production release requires identification of
the controller and verification of receiving, recovery and spending under the
intended custody arrangement, without exposing private keys or recovery data.

## Coinbase rules

During the term, the coinbase must contain a transparent output with both the
exact recipient script and exact `floor(3 * S(h) / 10)` amount. Validation accounts
for one matching occurrence without changing serialized transaction bytes.
Splitting the required amount, merging it with another payment, overpaying that
output, or substituting a shielded payment does not satisfy this requirement.

The builder emits separate foundation and miner outputs even when their
addresses match. Additional identical transparent outputs are permitted within
an otherwise valid transparent miner remainder; only one occurrence is counted
as the compulsory allocation.

| Miner mode | Remaining payout after accounting for one foundation output |
| --- | --- |
| Transparent | Transparent outputs totaling `S - fund + F`; no shielded bundle |
| Private Ironwood | Ironwood value totaling `S - fund + F`; no other transparent output |

Transparent miner rewards may be split among outputs. During the term, the
foundation output is the sole exception to the prohibition on mixing private
Ironwood miner rewards with transparent outputs. Outside the term, a coinbase
is all-transparent or private Ironwood without a compulsory foundation output.

Full coinbase value must equal `S + F`. Checked arithmetic and
`MAX_WCASH_COINBASE_VALUE` apply to the gross total, not only the miner share.
Ironwood proofs, coinbase flags, transaction domains, note encryption and the
prohibition on publicly recoverable zero-OVK miner outputs remain enforced.
Sprout, Sapling and legacy Orchard components remain prohibited.

## Templates, RPCs and mining coordinator

The shared [foundation policy](../zebra-chain/src/parameters/network/subsidy/wcash_foundation.rs)
uses the network and candidate height. Templates at tip plus one, replacement
jobs and reorganizations recompute the allocation from that height.

| Interface | Behavior |
| --- | --- |
| `getblocksubsidy` | Gross `totalblocksubsidy`, reduced `miner`, active foundation entry and `fundingstreamstotal`; fees excluded |
| Foundation RPC entry | Wcash-encoded address, exact atomic `valueZat` and this specification's URL |
| `founders`, `lockboxtotal` | Zero |
| `createauxblock.coinbasevalue` | Net miner payout `S - fund + F`, excluding the foundation output |
| `funding-policy` | Compiled policy, including network, heights, numerator `3`, denominator `10`, recipient, script and fixture status |

Outside the term, no compulsory fund entry is advertised and its total is zero.
Default Zcash serialization and consensus behavior remain separate.

The coordinator independently checks the serialized foundation amount and
script before attesting the miner remainder. Its transparent mode requires the
configured miner recipient. Private mode uses a protected incoming viewing key
to trial-decrypt every miner action and match the configured receiver and total
value. The same checks apply to candidate creation and saved-winner replay;
cached jobs retain their exact block bytes and candidate height.

Pools credit miners from the net `coinbasevalue` and upgrade the node and
coordinator together. The independent Zcash parent reward and fees are
unaffected. See the [merged-mining guide](wcash-merged-mining.md).

## Stored-state compatibility

Before activation, upgraded software can reopen the existing database without
discarding historical blocks. At and above activation, including after expiry,
stored history requires foundation-rule coverage before it can be trusted.
Version 2 coverage binds the allocation numerator `3` and denominator `10`,
activation and expiry heights, and the exact chain tip. Nonfinalized coverage
also binds the backup block's stored bytes. Coverage with an incompatible format,
allocation ratio or activation height cannot establish validation under the fixed
rule. Such records are refused at and above activation, including after expiry;
their markers are never relabeled automatically.

Unknown or uncovered affected history is refused. An old executable cannot
append blocks to a covered database and have them silently trusted on the next
upgrade. Preserve incompatible state and rebuild into a new, empty cache
directory from verified upgraded peers, as described in the
[migration instructions](wcash-foundation-fund-upgrade.md#existing-databases-and-late-upgrades).

| Active-term candidate | Zero-allocation validator | 30% validator |
| --- | --- | --- |
| Transparent all-miner payout without fund | Can accept | Rejects |
| Correct transparent miner and fund | Can accept | Accepts |
| Correct private Ironwood miner and transparent fund | Rejects mixed payout | Accepts |
| Wrong or missing mandatory fund output | No foundation enforcement | Rejects |

These results assume other block rules are satisfied. Every participant must
enforce the same fixed allocation. A coordinator without foundation support can
reject a funded template even when a zero-allocation validator accepts its
transparent form. Software without foundation support is not a safe general
rollback after activation.

## Wallet maturity and recovery

Foundation outputs have the ordinary 100-block coinbase maturity and must first
be shielded. An output created in block 10 cannot be spent in block 109; it can
be spent in block 110. Expiry neither invalidates reserves nor accelerates
maturity.

When a coinbase pays both the miner and foundation to the same receiver,
spending one output leaves the sibling independently recoverable. Wallet sync
validates each current UTXO against the full creator and reconciles omitted
outputs with exact mined spending inputs at the pinned chain tip. An omission
alone is not spend proof. Repeated sync and seed restoration preserve spent
status without recreating spent siblings.

Interrupted recovery resumes with the same database. Cached creator bytes are
revalidated against current UTXOs; cached spend receipts require their recorded
chain anchor to remain canonical. Changed outputs, reorgs or damaged cached
evidence require fresh verification. Spending stays blocked until recovery
completes. See the [wallet guide](../wcash-wallet/README.md).

## Isolated local fixture

The non-default `wcash-funding-test-fixture` feature exercises the same policy,
validation, templates and coordinator logic on isolated WcashRegtest:

| Parameter | Fixture value |
| --- | --- |
| Funded blocks | 10–19 inclusive |
| First unfunded block | 20 |
| Recipient | `WRHUq9CTLZFa52NmAyHH5usN21Q8jZVskN1` |
| Gross subsidy | 6.25 TWC |
| Foundation per funded block | 1.875 TWC |
| Miner per funded block | 4.375 TWC plus all fees |
| Total foundation allocation | 18.75 TWC |

The recipient is disposable and must never hold real funds. Fixture binaries
identify themselves, refuse Mainnet and public Testnet use, and must not be
published as release artifacts. Public identities, subsidy curves, difficulty
and maturity are unchanged. Ordinary Regtest without the feature has no fund.

The [local harness](../scripts/wcash-foundation-fund-e2e.sh) exercises activation,
expiry, existing-state upgrades, compatibility, reorgs, fresh sync and wallet
recovery. Production constants and arithmetic have separate
[policy vectors](../zebra-chain/src/parameters/network/subsidy/wcash_foundation.rs).
Reproduction and artifact inspection commands are in the
[operator guide](wcash-foundation-fund-upgrade.md#reproduce-the-isolated-local-exercise).

## Implementation references

- [Mainnet emission recurrence](../zebra-chain/src/parameters/network/subsidy/wcash_mainnet.rs)
- [Subsidy accounting](../zebra-chain/src/parameters/network/subsidy.rs)
- [Coinbase consensus checks](../zebra-consensus/src/block/check.rs)
- [Coinbase builder](../zebra-rpc/src/methods/types/transaction.rs)
- [RPC funding metadata](../zebra-rpc/src/methods/types/subsidy.rs)
- [Coordinator payout verification](../wcash-merge-miner/src/coordinator.rs)
- [Nonfinalized backup coverage](../zebra-state/src/service/non_finalized_state/backup.rs)
- [Wcash release gate](../.github/workflows/wcash-release-gate.yml)
