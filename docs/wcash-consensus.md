# Wcash consensus snapshot

This document describes the rules currently implemented on the built-in Wcash
regtest network. It is an implementation snapshot for review, not a claim that
mainnet or public testnet parameters are final.

## Activation status

Only `Network::new_wcash_regtest()` activates Wcash consensus. It has a distinct
local genesis block and P2P magic, activates NU6.3 at height 1, and uses the
Ironwood transaction format for every mined block.

The designated public genesis reference is Bitcoin mainnet block height
`965,954`. That anchor is intentionally not frozen in this source tree. Both
public anchor constants are `None`; mainnet and public testnet therefore fail
closed. Local regtest instead anchors its deterministic genesis
statement to Bitcoin mainnet block 965,910, hash
`00000000000000000000bbbdb28d2ff098642c6fde0a5fd84a707c92d146b146`.

The frozen local-regtest genesis block ID is
`b0ebe8618354e0563091d10b73ba03842cb3c112a801012616489269e58dbd61`.

Freezing a public anchor requires the exact Bitcoin header and hash, proof-of-work
verification, sufficient confirmations, independent reproduction, reviewed test
vectors, and a release that makes the values immutable. Runtime network access
must never select or replace a consensus anchor.

## Monetary policy

Genesis at height 0 has no subsidy. Heights 1 through 1,680,000 inclusive each
create 10 WCASH. The first halved block is height 1,680,001, and every later era
contains exactly 1,680,000 blocks. Subsidies are integer zatoshi values and each
era uses a right shift, so fractional zatoshi are discarded.

The last non-zero subsidy is 1 zatoshi at height 50,400,000. Subsidy is zero
from height 50,400,001 onward. Summing every era gives exactly:

```text
3,359,999,978,160,000 zatoshi
= 33,599,999.78160000 WCASH
```

This is scheduled issuance, not a rounded 33.6 million estimate. Transaction
fees are transfers of existing value and do not increase supply.

The inherited Ironwood transaction value type can represent at most 21 million
coins in a single transaction. Wcash therefore makes the same 21-million-coin
limit explicit for one block's subsidy-plus-fees, while aggregate chain value
uses the larger exact Wcash supply bound. Normal 10-WCASH rewards are nowhere
near this defensive per-block interoperability limit.

Wcash has no slow start, founders reward, funding stream, deferred pool,
lockbox disbursement, premine, or developer tax. The complete subsidy plus fees
belongs to the miner coinbase and is subject to the shielded-output rules below.

## Block timing and difficulty

The target spacing is 75 seconds. The current Wcash network is regtest, where
the inherited contextual difficulty logic deliberately uses the fixed regtest
proof-of-work limit rather than retargeting. This is suitable for isolated
testing only. Public-network difficulty limits, retarget behavior, launch
minimum work, and emergency rules require a separate specification and audit
before activation.

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

The mainnet and testnet forms reserve domains for future networks; only regtest
is active. Unified Addresses include the Wcash human-readable part inside both
the ZIP 316 jumbling construction and the Bech32m checksum. Replacing the
visible prefix of a Zcash address does not produce a valid Wcash address.

Wcash RPC and mining interfaces require the Wcash namespace when Wcash
consensus is active. A mining destination must be a Wcash Unified Address with
an Orchard receiver; the template builder uses that receiver payload to create
the mandatory Ironwood reward output. Standalone Sapling, TEX, and transparent
addresses are never valid coinbase destinations.

This node implements payment-address parsing and encoding only. It does not
provide a wallet, account derivation, or Wcash-specific encodings for spending
keys, viewing keys, extended keys, or seeds. Those key domains and wallet
interoperability remain separate release work and must not be inferred from the
payment-address prefixes above.

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
monetary bounds; those definitions remain only for upstream library tests.

No public deployment should occur until the public anchor and network domains
are frozen, full-node and pool interoperability tests pass, and an external
security/consensus audit is complete.
