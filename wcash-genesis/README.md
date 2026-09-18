# Wcash genesis and network identity

This crate defines the minimum identity and external-chain anchor rules needed by the
Wcash node. It is part of the node workspace so the frozen identity values are
used directly by consensus and runtime configuration.

The canonical product name is `Wcash`; mainnet uses the ticker `WEC`, while
Testnet and local regtest use the valueless display ticker `TWC` (Test Wcash).
Ticker selection is presentation metadata and does not change integer-zatoshi
amount serialization. Each Wcash network has P2P magic derived from an explicit
Wcash-only domain label. A node
must also use Wcash-only peer discovery, address encodings, ports, user-agent,
configuration, and data paths before any public network is enabled.

## Genesis anchor format

An anchor is exactly 40 bytes:

| Offset | Size | Meaning |
| ---: | ---: | --- |
| 0 | 1 | encoding version, exactly `1` |
| 1 | 1 | Wcash network: mainnet `0`, testnet `1`, regtest `2` |
| 2 | 1 | source chain: `0` Bitcoin Mainnet, `1` Zcash Testnet, `2` Zcash Mainnet |
| 3 | 1 | reserved, exactly zero |
| 4 | 4 | source-chain height, little endian |
| 8 | 32 | raw source block-hash bytes |

The conventional explorer hash is the reverse of the raw digest bytes. The
genesis commitment is BLAKE2b-256 with the 16-byte personalization
`WcashBtcAnchorV1`. Bitcoin-backed local Regtest retains its historical
deterministic ASCII coinbase text:

```text
06/Sep/2026 Wcash (WEC) BTC #<height> <64-character display hash>
```

The `WEC` token in this string is historical consensus-committed genesis text,
not the Testnet balance ticker. It remains unchanged so the frozen Testnet and
regtest genesis bytes and hashes do not rotate when user interfaces adopt
`TWC`.

Neither consensus validation nor the generator performs HTTP requests.

A future Zcash Mainnet-backed Wcash Mainnet anchor uses exactly this coinbase
format, with no date or project-name prefix:

```text
ZEC #<height> <64-character display hash>
```

## Frozen Testnet-profile anchor

The public Testnet profile freezes Zcash Testnet block 4,362,016, hash
`00000e289ad21d2feeb17e16585790ecabec94323c2ef22925534ad104de73ac`,
with source timestamp `1789685930`. The Wcash genesis timestamp is
`1789686000` (2026-09-17 23:00:00 UTC), and its complete frozen genesis ID is
`6b66fff119977d36d9c989093b516a876dbf6596536791ff35bb4c581e3fda98`.

The source-chain byte identifies Zcash Testnet and the anchor commitment uses
`WcashZecAnchorV1` personalization. The Testnet profile uses transaction branch
ID `0x54ba2bfb`, P2P identity `Wcash/testnet/v7`, magic `49d2934b`, and cache
namespace `wcashtestnet-v7`. These values prevent peers, state, and signed
transactions from retired chains from entering this reset network.

## Fail-closed mainnet launch

Wcash Mainnet will use a confirmed Zcash Mainnet block, but no height or hash
is frozen by this source tree. Mainnet therefore returns an error from
`select_anchor`; command-line input cannot activate or replace it. The source
already reserves a distinct Zcash Mainnet discriminator and its required
coinbase statement format without activating a placeholder anchor.

Freezing a public anchor requires a reviewed source release that does all of
the following:

1. Waits for at least 100 Zcash confirmations after the selected block.
2. Obtains the height, hash, and exact header from a locally validated Zcash
   Mainnet node and at least two independent block-data providers.
3. Recomputes the Zcash block ID from the exact header, checks its target and
   proof of work, and confirms all sources agree on byte order.
4. Replaces the mainnet `None` anchor constant in `src/lib.rs` with
   the reviewed height and raw digest bytes.
5. Freezes the encoding, statement, commitment, and complete Wcash genesis
   block as test vectors reproduced by a second implementation.
6. Releases the same source and reproducible binary to every participant.

Checking an isolated source header is not proof of its claimed height,
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
