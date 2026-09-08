# Wcash public Testnet

Wcash Testnet is an engineering and mining-interoperability network. Its
consensus identity is frozen so independently built nodes can agree on the same
height-zero block, but this is not a production-readiness claim. Wcash mainnet
remains disabled and testnet coins must have no monetary value.

> **Mining-only launch boundary:** Wcash Testnet consensus rejects every
> non-coinbase transaction. Mined rewards cannot be transferred. This fail-
> closed rule remains until a chain-specific NU6.3 signature and branch-ID
> domain is wired through the cryptographic dependencies. A testnet reset and
> consensus upgrade will be required before value-bearing transaction tests.

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
Wcash configuration also rejects inherited Zcash DNS seeds.

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
the smoke test can build a stale private fork. Because Wcash Testnet consensus
rejects every non-coinbase transaction, its template path is explicitly
coinbase-only and does not depend on mempool activation.

Treat the RPC cookie, node binary, Wcash payout receiver, and host as one
payout-critical boundary. The private Wcash coinbase deliberately prevents the
coordinator from publicly recovering its recipient, so the pool must trust its
loopback child node to construct the requested reward correctly.

## Reproduce the local mining smoke test

This smoke test is a mandatory local release check. It is intentionally not run
by GitHub Actions because it performs real proof-of-work solving; hosted CI uses
fixed Equihash and AuxPoW vectors and exercises the non-solving validation paths.
The smoke test uses an ephemeral, loopback-only profile with no
peers, no cookie authentication, and no `debug_force_finished_sync` override.
The inherited test-network policy deliberately permits its isolated template.
Its bounded template retry covers asynchronous RPC, state, and proposal-service
startup; it does not wait for or require mempool activation.
The test first asserts height zero, the exact genesis hash, the Wcash user agent,
zero initial supply, and successful `createauxblock`. It then serves a real
ZIP-301 job, solves Equihash `(200, 9)`, and requires consensus acceptance by
the Wcash Testnet node and by two isolated Zcash-regtest parent nodes:

```sh
cargo build --locked --release -p zebrad --bin zebrad --no-default-features
cp target/release/zebrad target/release/zcash-zebrad

cargo build --locked --release -p zebrad --bin zebrad \
  --no-default-features --features wcash-consensus
cp target/release/zebrad target/release/wcash-zebrad

cargo build --locked --release -p wcash-merge-miner --bin wcash-merge-miner
scripts/wcash-testnet-e2e.sh
```

Success proves that the frozen public-testnet genesis boots cleanly, the
test-network bootstrap policy provides a proposal-validated, coinbase-only child
template without a debug sync override or active mempool,
the exact authenticated AuxPoW survives the parent coinbase and block-
commitments paths, and a mined 6.25-WCASH reward increases only the Ironwood
value pool. The test fixture address is derived from public receiver bytes and
is not evidence that anyone controls a spending key.

## What is not ready

The mining-only consensus rule rejects all non-coinbase transactions, so testnet
rewards cannot move and the current chain must reset when the transaction
signature domain is activated. The repository also does not yet provide Wcash
wallet/account/key derivation, a spendability test for the private coinbase
fixture, project-operated seed infrastructure, or physical ASIC firmware
certification. The included pool
backend intentionally binds only loopback and uses one fixed share target. It
does not provide Internet-facing TLS, source-IP abuse controls, variable
difficulty, multi-generation late-share grace, balances, payouts, settlement,
monitoring, backups, or incident automation.

The independent Zcash parent coinbase follows standard Zcash rules and its
shielded recipient is publicly recoverable; only the Wcash child reward uses
the stronger private-coinbase policy. Do not advertise hidden Zcash payouts or
pay miners directly from raw share counts. A separately reviewed accounting
and settlement service must consume authenticated journal snapshots before a
community-facing pool can be operated.
