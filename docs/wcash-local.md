# Run Wcash locally

This guide starts the built-in isolated Wcash regtest and exercises the local
Zcash-parent proof harness. It does not start a public testnet, produce a
production Zcash parent block, or make this code safe for real funds.

## 1. Build

Install Rust 1.91 or newer, `libclang`, a C++ compiler, and the platform build
dependencies inherited from Zebra. From the repository root:

```sh
cargo build --release -p zebrad --features internal-miner --bin zebrad
cargo build --release -p wcash-merge-miner --bin wcash-merge-miner
```

## 2. Review the local configuration

[`wcash-local.toml`](../wcash-local.toml) selects `WcashRegtest`, binds P2P to
`127.0.0.1:28233`, and binds RPC to `127.0.0.1:28232`. Peer caching is disabled
and chain state is ephemeral. RPC cookie authentication is disabled only to
make loopback testing simple; never copy that setting to a public listener.

The configured Unified Address is a public regtest fixture with an Orchard
receiver, which Wcash routes into Ironwood. It is not evidence that you possess
the spending key. Replace it with your own compatible regtest Unified Address
before testing reward spends.

`internal_miner = true` uses real Equihash `(200, 9)` to build a synthetic
Zcash-parent AuxPoW proof and submits the resulting Wcash block through the
normal validator. It does not submit a parent block to Zcash or earn ZEC.

The configuration also sets `debug_force_finished_sync = true` because this
isolated network has no public peers. That option deliberately bypasses the RPC
sync-readiness guard and must never be used as a public deployment default.

## 3. Start the node

```sh
./target/release/zebrad -c wcash-local.toml start
```

In another terminal, verify the loopback RPC:

```sh
curl --silent \
  --header 'content-type: application/json' \
  --data-binary '{"jsonrpc":"2.0","id":1,"method":"getblockcount","params":[]}' \
  http://127.0.0.1:28232
```

After the real solver finds work, `getblockcount` advances beyond genesis. The
node log reports `submit block accepted`; inspecting that block shows no
transparent, Sapling, or Orchard coinbase value and a positive Ironwood pool
delta.

The state disappears on clean shutdown because this profile sets
`state.ephemeral = true`. Set it to `false` only if you deliberately want a
persistent local regtest database under the isolated Wcash cache directory.

## 4. Exercise the proof harness

The development harness accepts a raw/little-endian Wcash child block ID and a
raw/little-endian 256-bit target. Its easiest `ff..ff` target is for a standalone
proof round trip only:

```sh
cargo run --release -p wcash-merge-miner -- mine \
  000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f \
  ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff \
  16
```

To expose one fixed job to a local solver:

```sh
cargo run --release -p wcash-merge-miner -- serve \
  000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f \
  ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff \
  127.0.0.1:28237
```

The server refuses non-loopback bind addresses, caps request frames, and serves
at most eight clients concurrently. Excess connections are closed, and one
idle or malformed client cannot terminate the listener. It supports
`mining.subscribe`, `mining.authorize`, `mining.get_job`, and `mining.submit`,
but authorization is only a development stub.

Do not submit the standalone example proof to the node. A node-valid block must
use the exact child ID and compact target from that block's template, then
attach the returned proof to the same header before `submitblock`. The node's
feature-gated internal miner automates this with a synthetic parent; the
loopback JSON-lines server itself does not automate the complete RPC flow.

## 5. What a successful local test proves

A successful harness solve proves that the local machine can construct the
Wcash commitment, solve real Equihash `(200, 9)`, and pass the same bounded
AuxPoW verifier used by consensus. A running node additionally checks the
Wcash genesis and configuration path.

It does not prove parent Zcash acceptance, production pool compatibility,
public difficulty behavior, wallet recovery, public network identity, or
security under adversarial load. Those require the live Zcash GBT/dual-submit
adapter, public anchor freeze, interoperability testing, and independent audit.
