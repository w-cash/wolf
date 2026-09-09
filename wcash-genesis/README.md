# Wcash genesis and network identity

This crate defines the minimum identity and Bitcoin-anchor rules needed by the
Wcash node. It is part of the node workspace so the frozen identity values are
used directly by consensus and runtime configuration.

The canonical product name is `Wcash`, the ticker is `WEC`, and each Wcash
network has P2P magic derived from an explicit Wcash-only domain label. A node
must also use Wcash-only peer discovery, address encodings, ports, user-agent,
configuration, and data paths before any public network is enabled.

## Genesis anchor format

An anchor is exactly 40 bytes:

| Offset | Size | Meaning |
| ---: | ---: | --- |
| 0 | 1 | encoding version, exactly `1` |
| 1 | 1 | Wcash network: mainnet `0`, testnet `1`, regtest `2` |
| 2 | 1 | source chain, exactly `0` for Bitcoin mainnet |
| 3 | 1 | reserved, exactly zero |
| 4 | 4 | Bitcoin height, little endian |
| 8 | 32 | raw `SHA256d(80-byte header)` bytes |

The conventional explorer hash is the reverse of the raw digest bytes. The
genesis commitment is BLAKE2b-256 with the 16-byte personalization
`WcashBtcAnchorV1`. The deterministic ASCII coinbase text, including Wcash's
announcement timestamp, is:

```text
06/Sep/2026 Wcash (WEC) BTC #<height> <64-character display hash>
```

Neither consensus validation nor the generator performs HTTP requests.

## Frozen Testnet-profile anchor

The built-in engineering Testnet profile freezes Bitcoin mainnet block 965,900.
No public Wcash Testnet is deployed. The frozen identity is:

```text
hash:   0000000000000000000056b59ff5f4af3ca8b47837f2eac5d83a271c3e6b9851
header: 00203220700b5cbd51c3511db177feb5891754ee7e3ec5f4849a010000000000000000000444905fd34cdb083f512a1957ec2c7ffedf3a7ff5098d1f20a5aed9c8484b46c5719e6a5e35021706442381
time:   1788768709
nBits:  0x1702355e
nonce:  2166572038
```

At the frozen audit snapshot, the [Blockstream API](https://blockstream.info/api/block-height/965900)
and [mempool.space API](https://mempool.space/api/block-height/965900)
independently reported the same exact header, hash, height, and best-chain status
at tip 966,011. [Blockchain.com](https://www.blockchain.com/explorer/blocks/btc/965900)
independently agreed on the height, display hash, timestamp, compact target, and
nonce. Counting the anchor block, this was 112 confirmations. A local
reproduction double-SHA256 hashed the 80-byte header to the displayed hash,
decoded `nBits`, and verified that the header hash satisfies the encoded Bitcoin
target.

Height 966,011 is provenance for this review only. It is frozen in a test vector
and is never consulted as consensus or runtime input. Node consensus uses only
the immutable anchor fields, Wcash network discriminator, genesis statement,
commitment, and complete serialized Wcash genesis block.

The Testnet profile uses Wcash Testnet v1 branch ID `0xb3cfd27e` for NU6.3 /
Ironwood transactions; local Regtest uses the separate ID `0xc3a6678a`. Both
reject the standard Zcash NU6.3 ID and each other's Wcash ID. Every post-genesis
transaction must be V6, so V1-V5 and wrong-domain transactions are invalid in
both blocks and the mempool. A controlled local Regtest E2E has spent a private
coinbase through its Wcash V6 domain, but that does not establish public-network
wallet or pool-payout readiness. Testnet rewards have no value, and mainnet
remains disabled.

## Fail-closed mainnet launch

Bitcoin mainnet block 965,954 is designated for a possible Wcash mainnet
anchor, but it is not frozen by this source tree. At audit tip 966,011 it had
only 58 confirmations counting the block itself, below the required 100.
Mainnet therefore returns an error from `select_anchor`; command-line input
cannot activate or replace it.

Freezing a public anchor requires a reviewed source release that does all of
the following:

1. Waits for at least 100 Bitcoin confirmations after the selected block.
2. Obtains the height, hash, and exact 80-byte header from a locally validated
   Bitcoin Core node and at least two independent block-data providers.
3. Recomputes double SHA-256 over the header, checks its `nBits` target and
   proof of work, and confirms all sources agree on byte order.
4. Replaces the mainnet `None` anchor constant in `src/lib.rs` with
   the reviewed height and raw digest bytes.
5. Freezes the encoding, statement, commitment, and complete Wcash genesis
   block as test vectors reproduced by a second implementation.
6. Releases the same source and reproducible binary to every participant.

Checking an isolated Bitcoin header is not proof of its claimed height,
confirmations, or best-chain membership. This is why command-line input can
never activate or replace a public Wcash anchor.

## Local testing

The default regtest anchor is the frozen Bitcoin mainnet block at height
965,910. Its exact 80-byte header and hash are checked by the crate's test
vectors. It cannot be confused with a public Wcash anchor because the Wcash
network byte is part of the commitment.

```sh
cargo run --manifest-path wcash-genesis/Cargo.toml -- show-testnet
cargo run --manifest-path wcash-genesis/Cargo.toml -- show-regtest
cargo run --manifest-path wcash-genesis/Cargo.toml -- status
```

The helper can derive and inspect a candidate local anchor from any exact
Bitcoin mainnet header with valid isolated proof of work:

```sh
cargo run --manifest-path wcash-genesis/Cargo.toml -- \
  derive-regtest HEIGHT 160_HEX_CHARACTERS
```

This command does not alter a compiled genesis block. An explicitly selected
`WcashRegtest` always uses `REGTEST_ANCHOR` unless the source is changed and
rebuilt. `WcashTestnet` always uses the frozen `TESTNET_ANCHOR` and public-network
configuration cannot override it.
