# Wcash/Zcash merged-mining coordinator

This crate contains the native merged-mining path for Wcash. It creates a
proof-free Wcash block, requests an exact commitment-aware Zcash
`getblocktemplate`, checks every consensus-relevant field locally, sends the
same serialized parent block through an independent Zcash proposal gate, and
only then releases Equihash `(200, 9)` work.

One valid solution can be an ordinary pool share, a Wcash winner, a Zcash
winner, or a winner on both chains. The chains have independent targets. The
Wcash target comes from the authenticated child template and is never selected
by parent `nBits` or by a miner.

## Native trust boundary

The production-path topology uses three nodes:

1. a Wcash child template/submission node;
2. a pool-controlled Zcash template/submission node built from this branch;
3. at least one separately operated, unmodified Zcash proposal validator.

The two template sources must be literal loopback IP endpoints because they
choose the Wcash and Zcash reward recipients. A proposal validator may be
remote over HTTPS, but the operator remains responsible for making it a truly
independent process and failure domain; endpoint inequality cannot prove
organizational independence.

The coordinator verifies the configured Zcash coinbase recipient and an exact
transparent Wcash child recipient directly from their serialized coinbases. A
Wcash Unified Address with an Orchard receiver explicitly selects a private
Ironwood child reward whose recipient cannot be publicly recovered. Private
mode verifies the Ironwood-only shape and value, then trusts the loopback Wcash
node and its reviewed `createauxblock` implementation for recipient correctness.
Protect the child binary, configuration, RPC socket, and host as one
payout-critical trust boundary.

RPC credentials are read only from paired environment variables. Redirects
are disabled, requests have deadlines, response sizes are bounded, credentials
are redacted from diagnostics, and every node is pinned to an operator-supplied
genesis hash before work is issued.

## Run a native local job

Use the three-node procedure in
[`docs/wcash-local.md`](../docs/wcash-local.md). Once the nodes are running, set
the network pins and an isolated journal:

```sh
export WCASH_EXPECTED_GENESIS_HASH=70bf0bab17eff361a6331bb825b3b7253c8c96ff96407f948161d2912658bb1c
export ZCASH_EXPECTED_GENESIS_HASH=029f11d80ef9765602235e1bc9727e3eb6ba20839319f761fee920d63401e327
export ZCASH_NETWORK=regtest
WCASH_JOURNAL_DIR="$(mktemp -d "${TMPDIR:-/tmp}/wcash-native.XXXXXX")"
chmod 700 "$WCASH_JOURNAL_DIR"
export WCASH_SHARE_JOURNAL="$WCASH_JOURNAL_DIR/journal.jsonl"
export WCASH_PAYOUT_ADDRESS='replace-with-an-operator-owned-Wcash-address'
export ZCASH_PAYOUT_ADDRESS='replace-with-the-parent-nodes-Zcash-address'
```

`ZCASH_NETWORK` is a fail-closed selection of the canonical parent subsidy and
upgrade schedule. It must agree with the pinned genesis, payout-address
namespace, and coinbase branch ID. The native coordinator exposes only the
three documented profiles and refuses parent work before NU6.3; a separately
configured node is detected when its active branch diverges from the selected
profile.

Use operator-owned transparent addresses for the normal pool-integration path.
No fallback payout address is compiled into the coordinator. A transparent
Wcash coinbase matures after 100 blocks and must be shielded before ordinary
settlement. The included wallet can derive a controlled transparent coinbase
address and construct one bounded shielding transaction after synchronization
has classified mature outputs, but it is not an automatic pool-shielding or
settlement service. Supplying a Wcash Unified Address with an Orchard receiver
opts into a direct private Ironwood reward instead.

Prepare and proposal-check one job without serving it:

```sh
cargo run --release -p wcash-merge-miner -- native-job \
  http://127.0.0.1:28232 \
  http://127.0.0.1:18232 \
  http://127.0.0.1:18242 \
  -
```

`native-job` is not a read-only dry run: startup replays any pending winners
from the selected durable journal, then asks the child node to reserve a fresh
candidate before checking the parent proposal. After producing the preflight
result it retires that unsolved candidate with its authenticated lease.

`native-mine` uses the real Equihash solver and submits any Wcash and Zcash
winners independently:

```sh
cargo run --release -p wcash-merge-miner -- native-mine \
  http://127.0.0.1:28232 \
  http://127.0.0.1:18232 \
  http://127.0.0.1:18242 \
  - 64 0
```

## Serve ASIC work

`native-serve` exposes an exact-worker-authenticated ZIP-301 listener on a
literal loopback address. Create a separate Argon2id password hash for every
worker and place the hashes in a private version-1 registry. The server never
loads plaintext worker passwords from configuration; a password received on
the loopback connection is verified against its registered hash and discarded.

For example, provision `rig01` in the same private directory as the journal:

```sh
export WCASH_STRATUM_PASSWORD='replace-with-a-long-unique-worker-secret'
WORKER_PASSWORD_HASH="$(
  cargo run --release -p wcash-merge-miner -- worker-password-hash |
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

The registry must be a regular, non-symlink file that is inaccessible to group
and other users; on Unix its parent directory must not be group- or
world-writable. Worker names are exact and may contain 1–128 ASCII letters,
digits, `.`, `-`, `_`, or `:`. Add another independently generated hash entry
for each worker. `worker-password-hash` accepts a 12–1,024-byte password and
emits the exact Argon2id PHC format accepted by the registry.

```sh
export WCASH_SHARE_TARGET=ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
export WCASH_VALIDATION_LIMIT=4
export WCASH_AUTHENTICATION_LIMIT=4

cargo run --release -p wcash-merge-miner -- native-serve \
  http://127.0.0.1:28232 \
  http://127.0.0.1:18232 \
  http://127.0.0.1:18242 \
  - \
  127.0.0.1:28237 256 0
```

The maximum target above is only for isolated regtest. A deployed accounting
edge must select a meaningful share target and implement variable difficulty.
It proves share submission, not block discovery: for a deterministic local
dual-winner test, use the equal child and parent network target printed by
`native-job`, as the automated E2E gate does.
The `-` address argument reads `WCASH_PAYOUT_ADDRESS` without exposing it in a
process listing or plaintext diagnostic field. Raw JSON-RPC body logging is
disabled so the loopback `createauxblock` request does not reintroduce that
disclosure. On-host software uses the local endpoint as its pool, an exact
worker name present in `WCASH_WORKER_CREDENTIALS`, and that worker's plaintext
password. `WCASH_STRATUM_PASSWORD` is not a server credential: it is read only
by `worker-password-hash` and the bundled reference client. A physical ASIC
needs a separately operated reachable edge because its own `127.0.0.1` is not
the coordinator host.

`ZCASH_PAYOUT_ADDRESS` must exactly match the canonical
`mining.miner_address` configured on both parent nodes. A transparent address is
the recommended integration default; a supported shielded address is an
explicit alternative. The value is redacted from plaintext diagnostic fields.
The template node attests its configuration, while the coordinator verifies
both serialized parent coinbases. It removes the configured miner outputs and
compares every remaining funding-stream and lockbox output exactly, allowing
honest fee-dependent miner amounts to differ between same-tip nodes. Exact
proposal validation still enforces the candidate's subsidy-plus-fee total
before work is released. Native preflight deliberately prints the exact parent
coinbase for interoperability inspection.

The listener supports ZIP-301 `mining.subscribe`, `mining.authorize`, and
`mining.submit`. It validates the full Equihash proof, session nonce prefix,
job ID, time, share target, both network targets, stale state, and duplicates
before recording a share. Some vendor-specific ASIC dialects may still need an
interoperability adapter. Argon2id verification and Equihash validation have
separate global concurrency caps (`WCASH_AUTHENTICATION_LIMIT`, default 4 and
maximum 256; `WCASH_VALIDATION_LIMIT`, default 4 and maximum 1,024). Durable
writes are serialized by one journal, and each connection has sliding
authentication and submission-rate caps; these are backend safety bounds, not
a substitute for account/IP controls at the edge.

Both coinbases pay the operator-configured addresses, not a worker-specific
address. A successfully authorized login is bound to the exact canonical worker
identity written to the journal. Protect the credential registry and payout
configuration as operator secrets; the coordinator records work but does not
calculate balances or send payouts.

The process retains one bound loopback listener across job generations. It
closes sessions for retired work, then reuses the same socket after preparing
and independently proposal-validating fresh work. A generation is limited to
45 seconds and can rotate earlier when either chain tip or the child candidate
becomes stale. This avoids a rebind/`TIME_WAIT` failure and avoids presenting a
frozen header as a fake refreshed job; clients must reconnect after rotation.

Each `createauxblock` response carries a random, candidate-specific retirement
capability. After all generation connections and share handlers have joined,
the coordinator calls `retireauxblock` for unsolved and parent-only work. It
never makes that call while the durable outbox contains a Wcash winner, and the
node independently refuses retirement after a decoded AuxPoW submission starts.
Retirement retries are idempotent. A transient retirement failure blocks the
next generation with bounded backoff, so rapid parent-tip rotation cannot fill
the node's 16-entry cache. Treat the capability as an RPC secret; the
coordinator redacts it from debug output.

To exercise the actual pool wire path with a real Equihash solution, run this
from another terminal while `native-serve` is listening:

```sh
export WCASH_STRATUM_PASSWORD='the-plaintext-password-for-rig01'
cargo run --release -p wcash-merge-miner -- zip301-mine \
  127.0.0.1:28237 rig01 64 0
```

`zip301-mine` reconstructs the 108-byte pre-nonce header and assigned nonce
prefix only from ZIP-301 messages, solves Equihash `(200, 9)`, and submits the
canonical five-field share through the socket. It is a bounded loopback
reference miner, not production mining software.

## Crash-safe winner handling

Every accepted network winner is appended and `sync_data`-flushed with its
exact Wcash and/or Zcash block bytes before either submission RPC is attempted.
The journal is single-writer locked, mode `0600` on Unix, capped at 1 GiB, and
recovers one unterminated crash tail. On Unix its parent directory must not be
writable by group or other users, and every append verifies that the pathname
still names the locked inode. It does not compact automatically; stop the
coordinator and archive/replace the journal well before the cap. The long-lived
supervisor opens, validates, locks, and replays the journal once at startup.
Before exposing each later generation it appends and flushes a `job_activated`
record, rejects every previously seen job ID, and switches the active generation
without rescanning historical shares. A long-running public pool still needs
checkpointed startup recovery and controlled segment rotation before this path
can scale to a large journal. The journal's parent-directory entry is synced
before work begins. An append or durability failure,
live path replacement, or panic while holding the journal lock poisons the live
writer until restart, preventing a later append from completing a corrupt
partial line or using inconsistent memory. On
restart, each chain is replayed independently. Exact blocks remain in the
outbox across outages and reorgs until they have 100 best-chain confirmations.
Zcash confirmation queries the exact winner hash and uses the lowest depth
reported by reachable pinned nodes; authoritative unknown/not-best-chain
answers veto maturity. Wcash confirmation uses `getauxblockstatus`, which binds
the exact AuxPoW witness and confirmation depth in one state snapshot; matching
only the witness-independent child block ID is never sufficient. Retained
winners already observed on the best chain are polled without replaying their
block submission on every generation. A different AuxPoW witness for the same
Wcash block ID is durably quarantined and reported as
`quarantined_conflicting_winners`; retries stay status-only while the conflict
or node uncertainty remains. Exact best-chain observation clears the quarantine.
An authoritative absence first flushes an orphan transition, then replays the
original exact bytes so a crash cannot lose or silently replace the winner.

Ordinary non-winning shares and authenticated worker identities are recorded in
the same authoritative version-2 journal as winner outbox records. After
stopping the pool, produce a strict read-only aggregate with:

```sh
cargo run --release -p wcash-merge-miner -- accounting-report \
  "$WCASH_SHARE_JOURNAL"
```

The reporter obtains a shared lock, validates the complete journal, deduplicates
identical records by their authenticated share ID, and reports accepted shares,
Wcash winners, Zcash winners, exact-target buckets, and authentication provenance
for each worker. Durable `job_activated` records are validated for uniqueness but
are not counted as shares. Records created before provenance was added are explicitly
reported as `legacy_unknown`, never silently upgraded to exact credentials. A live
coordinator holds the exclusive lock, so final reporting intentionally fails
while the pool is running. The journal is the single accounting source of
truth; there is no second ledger with a crash window. The report is not a
balance calculation or payout authorization.

## Why F2Pool cannot be used unchanged

An ordinary upstream Zcash job does not commit the current Wcash child ID. A
proxy cannot add that commitment without changing the coinbase authorization
digest, authorization-data root, block-commitments hash, and Equihash header.
The resulting work would no longer be valid for the upstream pool job.

Therefore the secure current design is a native pool backed by its own modified
Zcash template node. A third-party pool can add Wcash later by requesting the
same `wcashaux` GBT extension and submitting child winners; Wcash consensus and
ASICs do not need another change.

## Synthetic conformance harness

The `job`, `mine`, and `serve` commands remain available for deterministic
proof-format testing. They use a synthetic single-coinbase parent and do not
earn or submit ZEC:

```sh
cargo run --release -p wcash-merge-miner -- mine \
  000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f \
  ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff \
  16
```

## Verification

```sh
cargo test -p wcash-zcash-aux
cargo test -p wcash-zcash-aux --no-default-features
cargo test -p wcash-merge-miner
cargo clippy -p wcash-merge-miner --all-targets -- -D warnings
```

The expensive real-solver unit test is ignored in routine debug runs:

```sh
cargo test --release -p wcash-merge-miner \
  real_solver_produces_a_strictly_valid_round_trip -- --ignored --nocapture
```

## Public deployment boundary

The included listener is deliberately loopback-only. A community pool still
needs a hardened TLS edge, source-IP and account abuse controls, connection and
request limits, variable difficulty, a separately reviewed payout and
settlement engine, monitoring, backups, multi-generation share grace, and
operational incident procedures. Exact-worker authentication and the durable
journal/report do not turn this fixed-target local backend into a public pool.
No physical ASIC model or firmware has been certified. This coordinator is not
a custodial payout service.
