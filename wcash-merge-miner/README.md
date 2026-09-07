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

The coordinator can publicly recover and verify the Zcash coinbase recipient.
It cannot recover a deliberately private Wcash coinbase from a payment address
alone, so recipient correctness on the child chain trusts the loopback Wcash
node and its reviewed `createauxblock` implementation. Consensus still rejects
transparent or publicly recoverable Wcash coinbases. Protect the child binary,
configuration, RPC socket, and host as one payout-critical trust boundary.

RPC credentials are read only from paired environment variables. Redirects
are disabled, requests have deadlines, response sizes are bounded, credentials
are redacted from diagnostics, and every node is pinned to an operator-supplied
genesis hash before work is issued.

## Run a native local job

Use the three-node procedure in
[`docs/wcash-local.md`](../docs/wcash-local.md). Once the nodes are running, set
the network pins and an isolated journal:

```sh
export WCASH_EXPECTED_GENESIS_HASH=b0ebe8618354e0563091d10b73ba03842cb3c112a801012616489269e58dbd61
export ZCASH_EXPECTED_GENESIS_HASH=029f11d80ef9765602235e1bc9727e3eb6ba20839319f761fee920d63401e327
WCASH_JOURNAL_DIR="$(mktemp -d "${TMPDIR:-/tmp}/wcash-native.XXXXXX")"
chmod 700 "$WCASH_JOURNAL_DIR"
export WCASH_SHARE_JOURNAL="$WCASH_JOURNAL_DIR/journal.jsonl"
export WCASH_PAYOUT_ADDRESS='replace-with-a-Wcash-Unified-Address'
export ZCASH_PAYOUT_ADDRESS='replace-with-the-parent-nodes-shielded-Zcash-address'
```

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
candidate before checking the parent proposal.

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

`native-serve` exposes a password-protected ZIP-301 listener on a literal
loopback address. It retires stale work, disconnects the old generation, and
prepares a fresh proposal-validated job with bounded retry backoff.

```sh
export WCASH_STRATUM_PASSWORD='replace-with-a-long-local-secret'
export WCASH_SHARE_TARGET=ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
export WCASH_VALIDATION_LIMIT=4

cargo run --release -p wcash-merge-miner -- native-serve \
  http://127.0.0.1:28232 \
  http://127.0.0.1:18232 \
  http://127.0.0.1:18242 \
  - \
  127.0.0.1:28237 256 0
```

The maximum target above is only for isolated regtest. A deployed accounting
edge must select a meaningful share target and implement variable difficulty.
The `-` address argument reads `WCASH_PAYOUT_ADDRESS` without exposing it in a
process listing or plaintext diagnostic field. Raw JSON-RPC body logging is
disabled so the loopback `createauxblock` request does not reintroduce that
disclosure. On-host software uses the local endpoint as its pool, an arbitrary
worker name, and the exact `WCASH_STRATUM_PASSWORD` value as its pool password.
A physical ASIC needs a separately operated reachable edge because its own
`127.0.0.1` is not the coordinator host.

`ZCASH_PAYOUT_ADDRESS` must exactly match the canonical shielded
`mining.miner_address` configured on both parent nodes. It is redacted from
plaintext diagnostic fields. The template node attests its configuration,
while the coordinator also recovers recipients from both serialized parent
coinbases and compares their transparent funding outputs before it releases
work. Native preflight deliberately prints the exact parent coinbase for
interoperability inspection; its Zcash recipient remains publicly recoverable
with the protocol's conventional zero outgoing-viewing key.

The listener supports ZIP-301 `mining.subscribe`, `mining.authorize`, and
`mining.submit`. It validates the full Equihash proof, session nonce prefix,
job ID, time, share target, both network targets, stale state, and duplicates
before recording a share. Some vendor-specific ASIC dialects may still need an
interoperability adapter. Equihash validation has a global concurrency cap,
durable writes are serialized by one journal, and each connection has a sliding
submission-rate cap; these are backend safety bounds, not a substitute for
account/IP controls at the edge.

Both coinbases pay the operator-configured addresses, not a worker-specific
address. The built-in password is shared, so anyone who knows it can claim any
worker label. Never calculate payouts from the journal's worker field alone;
an authenticated accounting edge must bind each connection to an operator-
controlled account identity.

## Crash-safe winner handling

Every accepted network winner is appended and `sync_data`-flushed with its
exact Wcash and/or Zcash block bytes before either submission RPC is attempted.
The journal is single-writer locked, mode `0600` on Unix, capped at 1 GiB, and
recovers one unterminated crash tail. On Unix its parent directory must not be
writable by group or other users, and every append verifies that the pathname
still names the locked inode. It does not compact automatically; stop the
coordinator and archive/replace the journal well before the cap. Its parent-
directory entry is synced before work begins. An append or durability failure,
live path replacement, or panic while holding the journal lock poisons the live
writer until restart, preventing a later append from completing a corrupt
partial line or using inconsistent memory. On
restart, each chain is replayed independently. Exact blocks remain in the
outbox across outages and reorgs until they have 100 best-chain confirmations.
Zcash confirmation queries the exact winner hash and uses the lowest depth
reported by reachable pinned nodes; authoritative unknown/not-best-chain
answers veto maturity. Wcash confirmation uses `getauxblockstatus`, which binds
the exact AuxPoW witness and confirmation depth in one state snapshot; matching
only the witness-independent child block ID is never sufficient.

Ordinary non-winning shares are also journaled for an external accounting
system, but this crate does not calculate balances or send pool payouts.

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
needs a hardened TLS edge, per-account credentials, connection and request
rate limits, variable difficulty, durable accounting and payout policy,
monitoring, backups, multi-generation share grace, and operational incident
procedures. This coordinator is not a custodial payout service.
