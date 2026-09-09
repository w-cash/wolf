# Wcash consensus snapshot

This document describes the rules currently implemented on the built-in Wcash
Testnet and regtest networks. It is an implementation snapshot for review, not
a claim that mainnet or community-pool infrastructure is production ready.

`WEC` denotes the future mainnet currency. Testnet and regtest balances use the
valueless display ticker `TWC` (Test Wcash). The ticker is not serialized by
consensus: amounts remain integer zatoshi, and the frozen testing genesis
statements and hashes are unchanged.

## Activation status

`Network::new_wcash_testnet()` and `Network::new_wcash_regtest()` activate Wcash
consensus. Each has a distinct genesis block, P2P magic, default P2P/RPC ports,
and peer-cache domain. Both activate NU6.3 at height 1 and use the Ironwood
transaction format for every mined block.

The built-in Testnet profile freezes Bitcoin mainnet block 965,900, hash
`0000000000000000000056b59ff5f4af3ca8b47837f2eac5d83a271c3e6b9851`.
The release vector records Bitcoin height 966,011, or 112 confirmations counting
the anchor block. Its complete Wcash genesis block ID is
`d95a9f2f1daf07d48fb3c863ad7334ec630a4a7077da98c8f7e65f8c0e277cf1`.
Its `Wcash/testnet/v4` P2P magic is `2f229b8c`; its default P2P and
recommended loopback RPC ports are 38233 and 38232. It never inherits Zcash's
DNS seeds.

The designated mainnet reference is Bitcoin height 965,954, but that anchor is
intentionally not frozen and mainnet fails closed. Local regtest anchors its
deterministic genesis statement to Bitcoin mainnet block 965,910, hash
`00000000000000000000bbbdb28d2ff098642c6fde0a5fd84a707c92d146b146`.

The frozen local-regtest genesis block ID is
`70bf0bab17eff361a6331bb825b3b7253c8c96ff96407f948161d2912658bb1c`.
Its `Wcash/regtest/v5` P2P magic is `4c237261`.

The Testnet source freezes the exact Bitcoin header and hash, verifies its proof
of work, records sufficient confirmations, and reproduces the complete genesis
bytes in reviewed test vectors. Runtime network access never selects or replaces
a consensus anchor.

At audit tip 966,011, Blockstream and mempool.space agreed on the exact header,
hash, height, and best-chain status; Blockchain.com independently agreed on the
decoded timestamp, compact target, and nonce. Local SHA256d and `nBits` checks
reproduced the hash and verified its work. The audit tip is frozen provenance,
not a consensus or runtime input.

### Transaction version and replay domain

Wcash Testnet v1 uses consensus branch ID `0xb3cfd27e`; isolated Wcash Regtest
uses the separate ID `0xc3a6678a`. Both select NU6.3 / Ironwood semantics, while
standard Zcash NU6.3 keeps `0x37a5165b`. The selected Wcash ID is carried in
every post-genesis version-6 transaction and is used by transaction IDs,
signature hashes, shielded authorization, parsing, block checks, and
chain-history commitments. Validators compare the exact network-specific ID
rather than treating the shared NU6.3 feature set as sufficient.

Wcash rejects every post-genesis V1-V5 transaction. In particular, V4 has no
embedded branch ID and V5 belongs to an inherited Zcash domain, so neither is a
valid compatibility path. Testnet-domain and regtest-domain Wcash transactions
reject each other. Wcash-domain V6 transactions are rejected on Zcash Mainnet
and Testnet, and Zcash-domain V6 transactions are rejected on Wcash. Both the
block and mempool verifier paths enforce the same checks.

This makes value-transfer testing possible without relying on P2P
magic or address prefixes for replay protection. A controlled local Regtest E2E
has mined three private coinbases, scanned them into the experimental wallet,
signed and broadcast a one-TWC Wcash-domain transfer, observed its exact bytes
and fee in the mempool and block template, mined it through AuxPoW, and rescanned
the recipient and private change. A second isolated Regtest lifecycle mined 101
transparent coinbases, rejected shielding before maturity, made the height-1
reward spendable only for the height-101 transaction, and mined that exact
shielding transaction through AuxPoW. Only the private-transfer phase uses the
explicit Regtest-only one-confirmation override; transparent coinbase maturity
remains 100 blocks. The public Testnet wallet policy remains 100 confirmations.
Testnet coins remain test-only, and these local results are not a public-network
or production-wallet claim.

## Monetary policy

Wcash uses Zcash's monetary precision without modification: one WEC on mainnet,
or one TWC on Testnet and regtest, is exactly 100,000,000 zatoshi. The smallest
consensus amount is therefore 0.00000001 WEC or TWC. Amounts are encoded and
validated as integer zatoshi; consensus never uses floating-point coin values.

Genesis at height 0 has no subsidy. Heights 1 through 1,680,000 inclusive each
create 6.25 TWC on the testing networks; the future mainnet amount is 6.25 WEC.
The first halved block is height 1,680,001, and every later era contains exactly
1,680,000 blocks. At the 75-second target, each era is approximately four
years. Subsidies are integer zatoshi values and each era uses a right shift, so
fractional zatoshi are discarded.

The inherited genesis transaction contains one zero-valued transparent output.
It creates no spendable coins, and the chain supply at height 0 is exactly zero.

The last non-zero subsidy is 1 zatoshi at height 50,400,000. Subsidy is zero
from height 50,400,001 onward. Summing every era gives exactly:

```text
2,099,999,981,520,000 zatoshi
= 20,999,999.81520000 WEC or TWC, according to network
```

Wcash enforces a 21,000,000-WEC mainnet monetary-base hard cap and the same
2,100,000,000,000,000-zatoshi ceiling on testing networks, where it is displayed
as 21,000,000 TWC. Integer-zatoshi halving truncation makes exact scheduled
subsidy issuance 0.18480000 WEC or TWC lower than the ceiling. Transaction fees
are transfers of existing value and do not increase supply.

Wcash has no slow start, founders reward, funding stream, deferred pool,
lockbox disbursement, premine, or developer tax. The complete subsidy plus fees
belongs to the miner coinbase and is subject to the coinbase payout-mode rules below.

## Block timing and difficulty

The target spacing is 75 seconds. Regtest deliberately uses its fixed
proof-of-work limit. The built-in Testnet profile starts at compact target
`0x2007ffff`
(`2^251 - 1`) and uses the inherited damped 17-block retarget from launch. A
candidate strictly more than 450 seconds after its predecessor may use the
testnet proof-of-work limit; exactly 450 seconds does not trigger the rule.
The maximum block-time rule is enforced from height 1, and template construction
evaluates both rules using the candidate height, including the height-1 boundary.
These public-testnet boundary conditions have frozen unit tests, but long-running
multi-node difficulty and timestamp behavior still requires adversarial soak
testing before a mainnet design is selected.

The compact difficulty field in the Wcash header is authoritative for Wcash.
The Zcash parent header's `nBits` is retained only as diagnostic data and cannot
allow a miner to choose an easier Wcash target.

## Coinbase payout modes

For every height above genesis, a Wcash coinbase can use either of two explicit
payout modes:

- a transparent Wcash address, which is the recommended default for existing
  pool integration; or
- a Wcash Unified Address containing an Orchard receiver, which routes the
  reward into Ironwood with private note encryption.

The template builder selects the mode from the address supplied by the pool.
It never inserts a hard-coded runtime payout address, and it rejects inherited
Zcash addresses and wrong-network Wcash addresses. Standalone Sapling and TEX
destinations are not coinbase payout modes. Consensus rejects Sprout, Sapling,
and legacy Orchard components in every Wcash transaction; transparent and
Ironwood components remain active. Transparent and Ironwood miner payouts
cannot be mixed in one Wcash coinbase.

Transparent coinbase outputs retain Zcash's public-network spend policy: they
mature after 100 blocks, and a transaction spending them must not create any
transparent outputs. A pool therefore shields matured coinbase value before
ordinary settlement. The included experimental wallet exposes a seed-derived
transparent P2PKH coinbase address and can build a bounded, one-shot transaction
that shields only mature, fully classified coinbase outputs into the same
wallet's Ironwood receiver. It is not an automatic pool-shielding, payout, or
settlement service.

For the optional Ironwood mode, the builder passes no outgoing viewing key to
note encryption and consensus rejects outputs recoverable with Zcash's
conventional all-zero outgoing viewing key. This hides the reward recipient and
note plaintext. A transparent reward is intentionally public. Neither mode
hides the deterministic gross subsidy or transaction fees, and parent-pool
metadata can create additional off-chain correlation.

## Payment-address domains

Wcash reuses Zcash receiver payload formats, but not Zcash payment-address
strings. Its deliberately separate, compact textual namespaces are:

| Receiver | Mainnet | Testnet | Regtest |
| --- | --- | --- | --- |
| Unified | `wu1...` | `wutest1...` | `wuregtest1...` |
| Transparent-source-only P2PKH (TEX) | `wtex1...` | `wtextest1...` | `wtexregtest1...` |
| Transparent P2PKH | `W1...` | `WT...` | `WR...` |
| Transparent P2SH | `W3...` | `WU...` | `WS...` |

The Testnet and regtest forms are active; the mainnet forms remain reserved.
The former `ws...`, `wtestsapling...`, and `wregtestsapling...` namespaces are
also reserved, but are deliberately rejected because Wcash does not activate
Sapling. TEX is an encoding of a transparent P2PKH receiver, not another value
pool.
Unified Addresses include the Wcash human-readable part inside both the ZIP 316
jumbling construction and the Bech32m checksum. Replacing the visible prefix of
a Zcash address does not produce a valid Wcash address.

Wcash RPC and mining interfaces require the Wcash namespace when Wcash
consensus is active. A transparent Wcash address selects the normal public
coinbase path. A Wcash Unified Address with an Orchard receiver explicitly
selects a private Ironwood reward. Standalone Sapling, TEX, and inherited Zcash
addresses are not valid Wcash coinbase destinations.

The node implements payment-address parsing and encoding. The workspace also
contains an experimental one-shot wallet for controlled Testnet and regtest
operations. Its `derive-address` command reports both the private Unified
Address and the related transparent P2PKH coinbase address. After synchronization
has classified a coinbase and enforced its 100-block maturity, `shield-coinbase`
can build and persist one Wcash-domain V6 transaction that sends the selected
transparent inputs into the wallet's Ironwood receiver. The wallet also signs
Ironwood transfers and broadcasts exact signed bytes. `list-pending` returns
bounded, paginated recovery records containing the persisted transaction ID and
exact signed bytes. After any ambiguous post-sign outcome, an operator must
enumerate those pages, inspect transaction status, and rebroadcast the same bytes
rather than construct a replacement. The wallet is restricted operationally to
one exclusive writer per database. It deliberately does not implement automatic
shielding, batch payouts, pool accounting, durable settlement, or idempotent
payout requests.

The controlled-key private-transfer and transparent-coinbase shielding gates
have passed in isolated Regtest. The transparent phase recovered all current
UTXOs from a wallet initialized with a deliberately late birthday, exercised the
100-block maturity boundary across 101 AuxPoW-mined blocks, and conserved exactly
631.25 TWC across the transparent and Ironwood pools after shielding one
coinbase. These deterministic local results do not establish general wallet
interoperability, public Testnet operation, public reorg safety, or production
payout safety. Key control must never be inferred from a syntactically valid
payment address.

## Supply auditability

Validators independently check the subsidy for the height, sum transaction
fees, validate the selected payout mode, and require the post-NU6 coinbase
output value to equal its input value exactly. Transparent outputs and
Ironwood's value balance are both public transaction data. An auditor can
reproduce cumulative scheduled issuance and pool balance changes without
decrypting private reward notes.

## Inherited code and review boundary

Wcash currently reuses substantial Zebra and librustzcash consensus code,
including NU6.3 transaction parsing and Ironwood proofs. Reuse does not make the
fork automatically safe. Changes to block identity, AuxPoW validation,
difficulty, monetary limits, coinbase encryption, network identity, and wallet
interoperability require independent review and stable public test vectors.

Header deserialization must select the variable-length Wcash witness before a
`Network` value is available, so this fork reserves wire version `0xd7430001`.
Its high bit is deliberately set, which native Zcash consensus forbids, making
valid Zcash headers and Wcash headers unambiguous. The node runtime still rejects
every inherited Zcash network configuration because Wcash uses different
network and consensus rules; those definitions remain isolated in upstream
library tests.

Wcash Testnet has a frozen identity for future public engineering
interoperability, but no public Wcash Testnet is deployed. The controlled local
Regtest private-transfer and transparent-shielding lifecycles have passed; the
single-writer wallet remains experimental and is not a production or
pool-settlement service. Project-operated seeds, a separately implemented
pool-payout system, multi-node and pool soak and reorg testing, physical ASIC
runs, and an external security/consensus audit remain required before any
mainnet or community payout service. Mainnet remains disabled. See
[`wcash-testnet.md`](wcash-testnet.md).
