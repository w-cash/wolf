# Wcash consensus snapshot

This document describes the rules currently implemented on the built-in Wcash
Testnet and regtest networks. It is an implementation snapshot for review, not
a claim that mainnet or community-pool infrastructure is production ready.

## Activation status

`Network::new_wcash_testnet()` and `Network::new_wcash_regtest()` activate Wcash
consensus. Each has a distinct genesis block, P2P magic, default P2P/RPC ports,
and peer-cache domain. Both activate NU6.3 at height 1 and use the Ironwood
transaction format for every mined block.

Public Testnet freezes Bitcoin mainnet block 965,900, hash
`0000000000000000000056b59ff5f4af3ca8b47837f2eac5d83a271c3e6b9851`.
The release vector records Bitcoin height 966,011, or 112 confirmations counting
the anchor block. Its complete Wcash genesis block ID is
`78b292284bc7b03c6a16b62e29a3ab2015c40d6414cbc27ddcee878225600f10`.
Its P2P magic is `8ee56b56`; its default P2P and recommended loopback RPC
ports are 38233 and 38232. It never inherits Zcash's DNS seeds.

The designated mainnet reference is Bitcoin height 965,954, but that anchor is
intentionally not frozen and mainnet fails closed. Local regtest anchors its
deterministic genesis statement to Bitcoin mainnet block 965,910, hash
`00000000000000000000bbbdb28d2ff098642c6fde0a5fd84a707c92d146b146`.

The frozen local-regtest genesis block ID is
`b0ebe8618354e0563091d10b73ba03842cb3c112a801012616489269e58dbd61`.

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

Wcash Testnet v1 uses consensus branch ID `0xb3cfd27e` for its NU6.3 / Ironwood
rules. Standard Zcash NU6.3 keeps `0x37a5165b`. The Wcash ID is carried in every
post-genesis version-6 transaction and is used by transaction IDs, signature
hashes, shielded authorization, parsing, block checks, and chain-history
commitments. Validators compare the exact embedded ID rather than treating the
shared NU6.3 feature set as sufficient.

Wcash rejects every post-genesis V1-V5 transaction. In particular, V4 has no
embedded branch ID and V5 belongs to an inherited Zcash domain, so neither is a
valid compatibility path. Wcash-domain V6 transactions are rejected on Zcash
Mainnet and Testnet, and Zcash-domain V6 transactions are rejected on Wcash.
Both the block and mempool verifier paths enforce the same checks.

This makes value-bearing transaction testing possible without relying on P2P
magic or address prefixes for replay protection. Testnet coins remain test-only.
A controlled-key spend of a matured private coinbase, wallet recovery, mempool
admission, block-template inclusion, mining, and recipient rescan are still
release gates rather than assumptions.

## Monetary policy

Wcash uses Zcash's monetary precision without modification: one WCASH is exactly
100,000,000 zatoshi, so the smallest consensus amount is 0.00000001 WCASH.
Amounts are encoded and validated as integer zatoshi; consensus never uses
floating-point coin values.

Genesis at height 0 has no subsidy. Heights 1 through 1,680,000 inclusive each
create 6.25 WCASH. The first halved block is height 1,680,001, and every later
era contains exactly 1,680,000 blocks. At the 75-second target, each era is
approximately four years. Subsidies are integer zatoshi values and each era
uses a right shift, so fractional zatoshi are discarded.

The inherited genesis transaction contains one zero-valued transparent output.
It creates no spendable coins, and the chain supply at height 0 is exactly zero.

The last non-zero subsidy is 1 zatoshi at height 50,400,000. Subsidy is zero
from height 50,400,001 onward. Summing every era gives exactly:

```text
2,099,999,981,520,000 zatoshi
= 20,999,999.81520000 WCASH
```

Wcash enforces a 21,000,000-WCASH monetary-base hard cap
(2,100,000,000,000,000 zatoshi). Integer-zatoshi halving truncation makes exact
scheduled subsidy issuance 0.18480000 WCASH lower than the cap. Transaction fees
are transfers of existing value and do not increase supply.

Wcash has no slow start, founders reward, funding stream, deferred pool,
lockbox disbursement, premine, or developer tax. The complete subsidy plus fees
belongs to the miner coinbase and is subject to the shielded-output rules below.

## Block timing and difficulty

The target spacing is 75 seconds. Regtest deliberately uses its fixed
proof-of-work limit. Public Testnet starts at compact target `0x2007ffff`
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

## Mandatory private coinbase

For every height above genesis, consensus requires the coinbase transaction to:

- contain no transparent outputs;
- contain no Sapling component;
- contain no Orchard component;
- contain at least one Ironwood action;
- move every positive subsidy-plus-fee value into Ironwood; and
- make every Ironwood output unrecoverable with Zcash's conventional all-zero
  outgoing viewing key.

The block-template builder accepts only a Unified Address containing an Orchard
receiver, then routes that receiver to an Ironwood output. It passes no outgoing
viewing key to note encryption. These rules are consensus-checked again when a
block is validated; they are not merely wallet or pool defaults.

This policy prevents the standard public recovery of the reward recipient and
note plaintext. It cannot stop a miner from publishing their own viewing data,
and the parent Zcash coinbase may expose pool-identifying tags. The public
Ironwood value balance and the deterministic subsidy/fee equations reveal the
net amount entering the shielded pool. Therefore Wcash provides recipient
privacy, not secrecy of aggregate issuance or necessarily of the gross reward
for an individual block.

## Payment-address domains

Wcash reuses Zcash receiver payload formats, but not Zcash payment-address
strings. Its deliberately separate, compact textual namespaces are:

| Receiver | Mainnet | Testnet | Regtest |
| --- | --- | --- | --- |
| Unified | `wu1...` | `wutest1...` | `wuregtest1...` |
| Sapling | `ws1...` | `wtestsapling1...` | `wregtestsapling1...` |
| TEX | `wtex1...` | `wtextest1...` | `wtexregtest1...` |
| Transparent P2PKH | `W1...` | `WT...` | `WR...` |
| Transparent P2SH | `W3...` | `WU...` | `WS...` |

The Testnet and regtest forms are active; the mainnet forms remain reserved.
Unified Addresses include the Wcash human-readable part inside both the ZIP 316
jumbling construction and the Bech32m checksum. Replacing the visible prefix of
a Zcash address does not produce a valid Wcash address.

Wcash RPC and mining interfaces require the Wcash namespace when Wcash
consensus is active. A mining destination must be a Wcash Unified Address with
an Orchard receiver; the template builder uses that receiver payload to create
the mandatory Ironwood reward output. Standalone Sapling, TEX, and transparent
addresses are never valid coinbase destinations.

The node implements payment-address parsing and encoding. The workspace also
contains an experimental one-shot wallet for controlled Testnet and regtest
transfers. It derives Wcash-domain keys and receivers, scans a local SQLite
wallet from an attested loopback node, signs Wcash-domain V6 Ironwood transfers,
and broadcasts the exact signed bytes. It deliberately does not implement batch
payouts, pool accounting, durable settlement, or idempotent payout requests.
The controlled-key private-coinbase spend gate must pass before wallet
interoperability is considered complete. Key control must never be inferred
from a syntactically valid payment address.

## Supply auditability

Validators independently check the subsidy for the height, sum transaction
fees, forbid all other coinbase value destinations, and require the post-NU6
coinbase output value to equal its input value exactly. Ironwood's value balance
is part of the public transaction data. An auditor can reproduce cumulative
scheduled issuance and pool balance changes without decrypting reward notes.

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

Wcash Testnet has a frozen identity for public engineering interoperability.
It is not a production or value-bearing network: the complete wallet spend and
separately implemented pool-payout gates, project-operated seeds, multi-node and
pool soak testing, physical ASIC runs, and an external security/consensus audit
remain required before any mainnet or community payout service. See
[`wcash-testnet.md`](wcash-testnet.md).
