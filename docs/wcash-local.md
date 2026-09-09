# Run Wcash/Zcash merged mining locally

This procedure runs the same native template, proposal, solving, and dual-
submission path used by the merged-mining coordinator. It uses three isolated
loopback regtest nodes and real Equihash `(200, 9)`. The numbered manual flow is
mining-only; the mandatory automated E2E described below adds separate
controlled private-transfer and transparent-coinbase wallet phases. Neither is
a public Testnet deployment.

All Wcash balances in this local testing procedure use the valueless `TWC`
(Test Wcash) display ticker. Mainnet keeps `WEC`; both labels represent the same
eight-decimal integer-zatoshi precision and do not affect serialized amounts.

## 1. Build separate consensus binaries

Install Rust 1.91 or newer and Zebra's native build dependencies. Build the
default Zcash and feature-isolated Wcash binaries in separate Cargo target
directories:

```sh
scripts/build-wcash-testnet-binaries.sh
```

The feature boundary is deliberate. A Wcash-consensus binary rejects inherited
Zcash networks, and a default Zcash binary rejects `WcashRegtest`, preventing
either consensus profile from being used on the wrong chain. `--locked` also
makes this local build use the dependency graph reviewed and exercised by
the release gate instead of silently rewriting `Cargo.lock`. Separate target
directories also prevent a cached shared `target/release/zebrad` artifact from
being copied under the wrong consensus-profile name; the script fails if the
two published executables are byte-identical.

## 2. Start the three nodes

Open three terminals at the repository root:

```sh
./target/release/wcash-zebrad -c wcash-child-local.toml start
```

```sh
./target/release/zcash-zebrad -c zcash-parent-template-local.toml start
```

```sh
./target/release/zcash-zebrad -c zcash-parent-validator-local.toml start
```

The supplied profiles bind only literal loopback addresses, use ephemeral
state, disable cookie authentication for local testing, and force the isolated
regtest sync-readiness check. Never copy those authentication or sync-bypass
settings to a public listener. Parent regtest activates NU6.3 at height 1 so
its V6 coinbase has the authorization commitment required by AuxPoW v2.

## 3. Configure the coordinator

The current local identities are pinned explicitly:

```sh
export WCASH_EXPECTED_GENESIS_HASH=70bf0bab17eff361a6331bb825b3b7253c8c96ff96407f948161d2912658bb1c
export ZCASH_EXPECTED_GENESIS_HASH=029f11d80ef9765602235e1bc9727e3eb6ba20839319f761fee920d63401e327
WCASH_JOURNAL_DIR="$(mktemp -d "${TMPDIR:-/tmp}/wcash-native-e2e.XXXXXX")"
chmod 700 "$WCASH_JOURNAL_DIR"
export WCASH_SHARE_JOURNAL="$WCASH_JOURNAL_DIR/journal.jsonl"
export WCASH_PAYOUT_ADDRESS='WR64VqQpZRujxYnAJmqGK4d4fbqQZRZHazG'
export ZCASH_PAYOUT_ADDRESS='tmJymvcUCn1ctbghvTJpXBwHiMEB8P6wxNV'
```

These are public transparent regtest fixtures, not evidence that anyone has
their spending keys. They make the pool-integration default explicit without
silently installing a runtime payout destination. Replace both with
operator-owned addresses before testing spendability or settlement.
Transparent Wcash coinbase outputs mature after 100 blocks and must first be
spent into a shielded pool. The included wallet's `derive-address` output
contains the controlled transparent P2PKH coinbase address as well as its
private Unified Address. After `sync` has fetched and fully classified the
transparent history, `shield-coinbase` can build and persist one signed V6
transaction that sweeps a bounded number of mature coinbase outputs into the
wallet's Ironwood receiver. The operator must still inspect and broadcast that
transaction. The wallet requires one exclusive writer per database. If a wallet
operation has an ambiguous outcome after signing, enumerate every bounded
`list-pending` page, inspect the persisted transaction, and rebroadcast its exact
bytes; do not create a replacement. This is not an automatic or production
pool-shielding workflow.

The coordinator verifies an exact transparent Wcash recipient and value from
the serialized child candidate. Supplying a Wcash Unified Address with an
Orchard receiver instead opts into private Ironwood payout, whose recipient is
not publicly recoverable; that mode verifies the private-only shape and value
but trusts the reviewed loopback `createauxblock` implementation for recipient
correctness. The child binary, configuration, RPC socket, and host remain
payout-critical in both modes.

`ZCASH_PAYOUT_ADDRESS` must exactly match the canonical address configured as
`mining.miner_address` on both parent nodes. The normal example is transparent
for compatibility with existing pool operations; supported shielded Zcash
addresses remain explicit alternatives. The coordinator checks the
template-node commitment, exact coinbase payout policy, and the independent
validator's ordinary template before releasing work.

Prepare one job and exercise every preflight without mining:

```sh
./target/release/wcash-merge-miner native-job \
  http://127.0.0.1:28232 \
  http://127.0.0.1:18232 \
  http://127.0.0.1:18242 \
  -
```

The command succeeds only after the child candidate, both genesis pins, parent
payout policy, exact Zcash template reconstruction, commitment, target, both
Merkle roots, and an independent exact-block proposal check all pass.
It is not a read-only dry run: startup first replays any pending winners in the
selected journal, and `createauxblock` reserves a fresh child candidate.

## 4. Mine and submit both chains

Regtest targets make a bounded local real-solver run practical:

```sh
./target/release/wcash-merge-miner native-mine \
  http://127.0.0.1:28232 \
  http://127.0.0.1:18232 \
  http://127.0.0.1:18242 \
  - 64 0
```

On success, the JSON result reports both candidates and durable-outbox state.
Verify both tips independently:

```sh
curl --silent --header 'content-type: application/json' \
  --data-binary '{"jsonrpc":"2.0","id":1,"method":"getblockcount","params":[]}' \
  http://127.0.0.1:28232

curl --silent --header 'content-type: application/json' \
  --data-binary '{"jsonrpc":"2.0","id":1,"method":"getblockcount","params":[]}' \
  http://127.0.0.1:18232

curl --silent --header 'content-type: application/json' \
  --data-binary '{"jsonrpc":"2.0","id":1,"method":"getblockcount","params":[]}' \
  http://127.0.0.1:18242
```

The two Zcash nodes should report the same tip because a parent winner is
broadcast to the template node and proposal validator independently.

`getblockchaininfo` on the Wcash node exposes deterministic chain supply and
public value-pool totals. With the normal example address, a mined 6.25-TWC
child block increases the transparent pool by 6.25 while the shielded pools
remain zero. Supplying a controlled Wcash Unified Address instead moves that
reward into private Ironwood. Gross protocol issuance remains auditable in
either mode.

## 5. Exercise the ASIC protocol

Both native serving commands require an exact-worker credential registry.
Generate a distinct password hash for the local `rig01` worker and write a
private version-1 registry next to the journal:

```sh
export WCASH_STRATUM_PASSWORD='local-test-password-change-me'
WORKER_PASSWORD_HASH="$(
  ./target/release/wcash-merge-miner worker-password-hash |
    python3 -c 'import json,sys; print(json.load(sys.stdin)["password_hash"])'
)"
export WCASH_WORKER_CREDENTIALS="$WCASH_JOURNAL_DIR/workers.json"
python3 - "$WCASH_WORKER_CREDENTIALS" "$WORKER_PASSWORD_HASH" <<'PY'
import json
import os
import sys

path, password_hash = sys.argv[1:]
fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
with os.fdopen(fd, "w", encoding="utf-8") as output:
    json.dump({
        "version": 1,
        "workers": [{"name": "rig01", "password_hash": password_hash}],
    }, output, separators=(",", ":"))
    output.write("\n")
PY
unset WORKER_PASSWORD_HASH WCASH_STRATUM_PASSWORD
```

The server rejects a symlink or non-regular registry, a file accessible by
group or other users, and (on Unix) a parent directory writable by group or
other users. Generate a separate hash entry for each exact worker name; do not
reuse one pool-wide password.

For local protocol testing only, use the easiest possible share target and
start the persistent loopback listener:

```sh
export WCASH_SHARE_TARGET=ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
export WCASH_VALIDATION_LIMIT=4
export WCASH_AUTHENTICATION_LIMIT=4

./target/release/wcash-merge-miner native-serve \
  http://127.0.0.1:28232 \
  http://127.0.0.1:18232 \
  http://127.0.0.1:18242 \
  - \
  127.0.0.1:28237 256 0
```

Configure a ZIP-301-compatible Equihash miner with:

- pool: `stratum+tcp://127.0.0.1:28237`;
- worker: the exact registered identifier `rig01`;
- password: the plaintext password whose hash is registered for `rig01`.

The server does not read `WCASH_STRATUM_PASSWORD`. That variable is only a
non-command-line input to `worker-password-hash` and the bundled reference
miner. To perform a real Equihash solve and submit a canonical share over the
ZIP-301 socket, open another terminal with the coordinator environment and run:

```sh
export WCASH_STRATUM_PASSWORD='local-test-password-change-me'
./target/release/wcash-merge-miner zip301-mine \
  127.0.0.1:28237 rig01 64 0
```

This reference client constructs the 108-byte pre-nonce header and assigned
four-byte nonce prefix exclusively from ZIP-301 messages, solves Equihash
`(200, 9)`, and submits the five-field ZIP-301 share. A successful response
exercises the pool path rather than the direct `native-mine` path.

That address works only for an on-host software client. On a physical ASIC,
`127.0.0.1` means the ASIC itself. Hardware must connect to a separately
operated LAN or Internet-facing TCP/TLS edge that authenticates and rate-limits
miners before forwarding to this loopback backend.

The backend applies sliding per-connection authentication and submission caps,
globally limits concurrent memory-hard Argon2id verification with
`WCASH_AUTHENTICATION_LIMIT`, globally limits concurrent Equihash validation
with `WCASH_VALIDATION_LIMIT`, and serializes durable journal writes. The
maximum share target shown here would create unusable disk traffic on real
hardware; an operated edge must assign a meaningful variable-difficulty target.
It exercises ordinary share submission but does not make every share a block.
The automated E2E gate instead reads the equal local child/parent network target
from `native-job` so each accepted reference-client solution is a dual winner.

Both chain rewards go to the operator-configured addresses. A successful login
binds the exact registered worker identity to its journal records, but the
coordinator neither calculates balances nor sends payouts. The journal has a
1 GiB safety ceiling and no automatic compaction, so archive and replace it
under a controlled shutdown well before reaching that limit. The supervisor
locks and replays it once at startup, then durably records each unique job
activation and rotates the in-memory active generation without rescanning old
shares. A public service still needs checkpointed startup recovery and
controlled journal segments before it can sustain pool-scale share history.

The supervisor keeps one loopback socket bound across generations. It
proposal-checks fresh work and rotates after at most 45 seconds, or earlier when
either tip changes or the Wcash candidate expires. Rotation closes existing
sessions, so miners reconnect on the same port for the new job. Keeping the
socket open avoids a rebind failure while old connections pass through
`TIME_WAIT`. A public accounting edge should additionally retain a bounded
grace window for late shares from recently retired jobs.

## 6. Inspect authenticated share accounting

Stop `native-serve` before taking a final accounting snapshot. The running
coordinator holds an exclusive lock on the authoritative version-2 journal, so
the offline reporter intentionally fails rather than race the writer:

```sh
./target/release/wcash-merge-miner accounting-report \
  "$WCASH_SHARE_JOURNAL"
```

The command validates the complete journal and aggregates accepted shares,
Wcash winners, Zcash winners, exact-target buckets, and authentication provenance
under each worker name. It validates unique `job_activated` records but excludes
them from share totals. Older records without provenance are reported as
`legacy_unknown`. Identical duplicate records are counted once; conflicting or
terminated corrupt records fail closed. This single journal is also the
crash-safe winner outbox, avoiding a second-accounting-ledger durability gap.
The report is evidence for later accounting review, not a payout instruction.

## 7. What this proves

A successful manual mining run proves that this checkout can construct a
transparent Wcash coinbase, authenticate AuxPoW v2 through both Zcash transaction
commitments, solve real Equihash, pass an unmodified Zcash proposal validator,
submit exact blocks to both chains, and accept a canonical ZIP-301 share bound
to an exact authenticated worker.

The mandatory `scripts/wcash-testnet-e2e.sh` run adds two controlled Regtest
wallet phases. The private phase mines and scans three private 6.25-TWC
coinbases, signs and broadcasts a one-TWC Wcash V6 transfer, matches its exact
bytes and fee in the mempool and block template, verifies duplicate-broadcast
handling and rejection by two standard Zcash Regtest nodes, mines the
transaction in a fourth real AuxPoW block, and rescans the recipient and private
change. Its final supply check is exactly 25 TWC, entirely in Ironwood. This
phase uses the explicit Regtest-only unsafe one-confirmation override.

The transparent phase mines 101 AuxPoW blocks to a seed-derived Wcash address.
At tip 99 it reports all 99 rewards as pending and rejects premature shielding;
at tip 100 only the height-1 reward is spendable by the height-101 transaction.
A second wallet initialized with birthday 100 recovers all 100 existing
coinbase UTXOs through the separate current-UTXO scan. The primary wallet then
persists and broadcasts an exact V6 transaction that shields one mature
coinbase, matches it in the mempool and block template, and mines it in block
101. The final chain and wallet checks conserve exactly 631.25 TWC across the
transparent and Ironwood pools. Rotating and accepting 101 distinct child jobs
also exercises candidate release far beyond the coordinator's 16-entry active
cache bound. Transparent maturity is not overridden, and the public Testnet
wallet policy remains 100 confirmations.

Because these scripts perform real proof-of-work solving, they are mandatory
local release checks and are intentionally excluded from GitHub Actions. The
checked-in hosted workflows are configured to compile the same components and
validate fixed Equihash/AuxPoW vectors, consensus rules, RPC behavior, and
profile isolation without solving work. Repository Actions are currently
disabled, so this branch relies on the recorded local gates instead of a hosted
result.

The E2E proves only those deterministic local, single-writer wallet lifecycles.
It does not prove general or production wallet interoperability,
vendor-by-vendor ASIC interoperability, public variable-difficulty behavior,
payout correctness, Internet-facing security, public-network or long-running
reorg behavior, or independent consensus-review results. No public Wcash
Testnet, public pool, payout, or settlement service is deployed. No physical
ASIC model or firmware is certified, and Wcash mainnet remains disabled. Those
remain community-pool and network release gates.

Wcash payment namespaces are disjoint from Zcash: Unified `wu...`, transparent
`W...`, and transparent-source-only TEX `wtex...`, with separate testnet and
regtest forms. Sapling namespaces are reserved and rejected. Never create a
Wcash address by editing a Zcash prefix; checksums and Unified Address jumbling
bind the namespace.
