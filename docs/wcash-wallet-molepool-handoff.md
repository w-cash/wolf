# Molepool operator wallet validation and handoff

This document records validation of the multiple-coinbase-address wallet at
source commit `41a39cab3f259509653c1d4930df3dae3dabba98` and gives the two
operator commands needed to expose a second deterministic coinbase receiver.
It does not authorize a pool, Stratum, node, payout-worker, miner, or Mainnet
transaction change.

## Current source status: 16 October 2026

The wallet implementation was merged in [Wolf PR 33](https://github.com/w-cash/wolf/pull/33)
on 10 October 2026. It is already included in Mainnet-upgrade commit
`c0be7a0f2b39d4afa5c63f7c61b4e1d6a389be15`, together with the later wallet
recovery fixes and foundation rules. The validation table below records the
original wallet revision; it is not a test certificate for every later commit.

The 16 October review repeated `cargo test -p wcash-wallet --locked` against
the current wallet source included in `c0be7a0f2b`: 151 library tests, 11 CLI
tests, and two transaction-domain tests passed, with no failures. Workspace
formatting and `cargo clippy -p wcash-wallet --all-targets --all-features --locked -- -D warnings`
also passed. The review changed documentation only. The real-node
Regtest results below are the original validation record, not a claim that the
full Regtest acceptance flow was rerun on 16 October.

This feature creates **two addresses in one wallet account**, not two independent
wallets. Use the existing database and account. Keep the current index-0 address
for the first mining instance and expose index 1 for the second. Both receivers
contribute to the same account balance and shield into the same Ironwood
receiver. Creating account 1 would separate the logical account and does not
implement this procedure.

The CLI manages indices `0..=9`. Repeating `ensure --index 1` returns the same
address. The account and receivers derive from the same seed; a fresh recovery
must use the same network, account derivation, and a birthday covering the
original history, then complete synchronization. Receiver assignment files are
useful operator records, not a replacement for seed recovery material.

### Build from reviewed source

An operator can build the pinned source on the intended Linux host or an
isolated Linux build machine with the documented
[native prerequisites](wcash-node.md). This is a source build, not an officially
signed release download:

```sh
git clone --filter=blob:none https://github.com/w-cash/wolf.git wolf-molepool
cd wolf-molepool
git checkout --detach c0be7a0f2b39d4afa5c63f7c61b4e1d6a389be15
cargo +1.91.0 build --locked --release -p wcash-wallet --bin wcash-wallet
sha256sum target/release/wcash-wallet
```

Record the source revision and resulting checksum. Review the resulting Linux
artifact before installing it. The macOS validation binary described below is
not a Linux artifact. Replacing an executable is separate from changing an
existing wallet database or mining route.

### Foundation upgrade and Stratum

The [foundation upgrade](wcash-foundation-fund-upgrade.md) is a separate node
upgrade required before Mainnet height 33000. A pool that delegates Wcash
candidate construction to the upgraded node's `createauxblock` does not need a
Stratum wire-protocol change for the foundation output. Integrations that build
or validate their own coinbase must follow the updated
[pool compatibility contract](wcash-pool-compatibility.md).

Different receivers give the two mining instances different coinbases and
candidate hashes at the same tip. They do not eliminate normal `tip changed`
retries. Preserve the pool's retry handling when the chain advances.

## Validation result

The source commit is the merge of Wolf pull request 33 into `main`. It includes
pull request 32 and the multiple-coinbase-address implementation from pull
request 33.

The implementation passed source review, wallet tests, strict linting, a
locked release build, and the repository's native real-node Regtest acceptance
test. No wallet code defect was found.

| Check | Result |
| --- | --- |
| `cargo fmt --all -- --check` | Passed |
| `cargo test -p wcash-wallet` | Passed: 140 tests, 0 failures |
| `cargo clippy -p wcash-wallet --all-targets --all-features -- -D warnings` | Passed |
| `cargo build --locked --release -p wcash-wallet --bin wcash-wallet` | Passed on macOS arm64 |
| `scripts/build-wcash-testnet-binaries.sh` | Passed |
| Native `scripts/wcash-testnet-e2e.sh` logic | Passed through Regtest height 102 |

The real-node Regtest run proved that:

- indices 0 and 1 are distinct receivers under the same ZIP 32 account;
- repeated `coinbase-address ensure --index 1` calls return the same receiver;
- `coinbase-address list` returns deterministic child-index order;
- index 0 is immature at tip 99 and spendable at the exact 100-block boundary;
- indices 0 and 1 are both mature at tip 101;
- one transaction shields both 6.25-WEC inputs into the same Ironwood receiver
  with a 20,000-zat fee;
- the shielding transaction is mined at height 102; and
- late-birthday and seed-only restores recover both transparent receivers,
  their combined history, and the confirmed Ironwood output.

The disposable Regtest data and runtime were removed after validation. No
production seed, private key, viewing key, wallet database, node, pool,
Stratum listener, payout worker, miner route, or Mainnet transaction was used
or changed.

## Release boundary

The validated local binary reports `wcash-wallet 0.1.0`. It was built with
`rustc 1.91.0 (f8297e351 2025-10-28)` for `aarch64-apple-darwin` and has
SHA-256 `9762c0bf6fc3bbb44e1e8ded9db4f12bf8dc91b187a0fa87cec7e3176a1056e1`.
It is local validation evidence only and must not be installed on the Linux
operator host.

The Linux x86_64 operator artifact, checksum, signature, SBOM, attestation,
tag, and release upload remain pending. GitHub Actions is currently disabled
for `w-cash/wolf`, so the repository release gate cannot produce or attest a
candidate. Do not describe this source revision as an official published
release until those steps are completed and reviewed.

## Preconditions

1. Verify the approved Linux candidate's source commit and checksum.
2. Stop every process that can write the wallet database. Do not copy a live
   SQLite database.
3. Verify that recovery material is backed up, readable, and controlled by the
   wallet operator. Never put it in a command argument, environment variable,
   pool host, log, ticket, or chat.
4. Record the current binary path and checksum, service definition, exact
   `--network mainnet` argument, and absolute wallet database path for rollback.
5. Keep the current index-0 route unchanged.

Set local shell variables deliberately:

```sh
WALLET_BIN=/absolute/path/to/verified/wcash-wallet
WALLET_DB=/absolute/private/wcash-wallet.sqlite
PUBLIC_ENDPOINT=https://mainnet.zecwec.com
```

The address-management commands below return public metadata and do not read
the seed.

## List the current receivers

```sh
"$WALLET_BIN" \
  --network mainnet \
  --db "$WALLET_DB" \
  coinbase-address list | tee coinbase-addresses.before.json
```

Before continuing, confirm index 0 is the address already used by the existing
route.

## Ensure index 1

```sh
"$WALLET_BIN" \
  --network mainnet \
  --db "$WALLET_DB" \
  coinbase-address ensure --index 1 | tee coinbase-address.index-1.json

"$WALLET_BIN" \
  --network mainnet \
  --db "$WALLET_DB" \
  coinbase-address ensure --index 1 | tee coinbase-address.index-1.repeated.json

cmp coinbase-address.index-1.json coinbase-address.index-1.repeated.json
```

`cmp` must exit successfully. Both calls must show child index `1`,
`is_default: false`, and the same `W1...` address.

## Verify indices 0 and 1

```sh
"$WALLET_BIN" \
  --network mainnet \
  --db "$WALLET_DB" \
  coinbase-address list > coinbase-addresses.after.json

python3 - coinbase-addresses.after.json <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as file:
    rows = json.load(file)

indices = [row["child_index"] for row in rows]
assert indices == sorted(set(indices)), rows
by_index = {row["child_index"]: row for row in rows}
assert 0 in by_index and 1 in by_index, rows
first, second = by_index[0], by_index[1]
assert first["is_default"] is True, rows
assert second["is_default"] is False, rows
assert len({row["account_id"] for row in rows}) == 1, rows
assert first["address"] != second["address"], rows
assert first["address"].startswith("W1"), rows
assert second["address"].startswith("W1"), rows
print("index_0=" + first["address"])
print("index_1=" + second["address"])
print("account_id=" + first["account_id"])
PY
```

Retain the JSON files in the private deployment record. They contain public
metadata, not recovery material.

## Assign the second route

Use only the `index_1=` value printed above as the second route's coinbase
destination. Apply it through Molepool's existing reviewed route configuration
and change-control procedure.

- Leave the current route and its index-0 address unchanged.
- Change no listener, Stratum port, miner URL, payout protocol, payout worker,
  or node setting as part of this wallet handoff.
- Before publishing work, ask the target node for one disposable AuxPoW
  candidate for index 0 and one for index 1 at the same tip. Verify both extend
  the same previous block and have different candidate hashes, then retire both
  unused candidates.
- After index 1 receives its first output, synchronize the same wallet and
  confirm the output is classified under child index 1. Wait for the exact
  100-block coinbase maturity boundary before shielding.

For this handoff, the operator supplied `https://mainnet.zecwec.com` as the
public TLS endpoint. Verify its certificate and Wcash Mainnet identity before
using it. Read-only chain synchronization writes wallet state and must keep the
normal single-writer discipline:

```sh
"$WALLET_BIN" \
  --network mainnet \
  --db "$WALLET_DB" \
  --lightwalletd "$PUBLIC_ENDPOINT" \
  sync
```

## Rollback

1. Stop new jobs on the second route.
2. Point only that route back to the recorded index-0 address through the
   existing Molepool procedure.
3. Restore the previously checksummed wallet binary if the executable itself
   must be rolled back.
4. Do not delete or replace the wallet database, and do not remove index 1 from
   the operator inventory.
5. Keep scanning both deterministic receivers. Index 1 remains wallet-owned
   after rollback, and late or immature coinbase outputs can still arrive.
6. Shield all mature outputs from both addresses into the same Ironwood
   receiver before retiring operational references to the second route.

Signing and shielding require spending authority and are outside this
two-command handoff.
