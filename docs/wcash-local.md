# Run Wcash/Zcash merged mining locally

This procedure runs the same native template, proposal, solving, and dual-
submission path used by the merged-mining coordinator. It uses three isolated
loopback regtest nodes and real Equihash `(200, 9)`. It is not a public testnet
or a wallet-spend test.

## 1. Build separate consensus binaries

Install Rust 1.91 or newer and Zebra's native build dependencies. Build and
preserve one default Zcash binary before building the feature-isolated Wcash
binary:

```sh
cargo build --release -p zebrad --bin zebrad
cp target/release/zebrad target/release/zcash-zebrad

cargo build --release -p zebrad --bin zebrad --features wcash-consensus
cp target/release/zebrad target/release/wcash-zebrad

cargo build --release -p wcash-merge-miner --bin wcash-merge-miner
```

The feature boundary is deliberate. A Wcash-consensus binary rejects inherited
Zcash networks, and a default Zcash binary rejects `WcashRegtest`, preventing
the widened Wcash monetary range from being used on a Zcash node.

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
export WCASH_EXPECTED_GENESIS_HASH=b0ebe8618354e0563091d10b73ba03842cb3c112a801012616489269e58dbd61
export ZCASH_EXPECTED_GENESIS_HASH=029f11d80ef9765602235e1bc9727e3eb6ba20839319f761fee920d63401e327
WCASH_JOURNAL_DIR="$(mktemp -d "${TMPDIR:-/tmp}/wcash-native-e2e.XXXXXX")"
chmod 700 "$WCASH_JOURNAL_DIR"
export WCASH_SHARE_JOURNAL="$WCASH_JOURNAL_DIR/journal.jsonl"
export WCASH_PAYOUT_ADDRESS='wuregtest1d677slm064f84p6eqwz76ze3w5yuxn4vghyft5rv6nv5shxyza4jxqgt22ar0fsvpxa3ma4gkswum0ytnwpyhzm5xewjpa8dxszt62ck7ms9qka8qmfuradh3f0rwgu9k4sqzxkryre4450uj3fk8lhh9dcyhq5zmj8w7krpesuw9t6nt8pxyrpvwswy7akv0sux45sgjx9c5zgldrt'
export ZCASH_PAYOUT_ADDRESS='uregtest1efxggx6lduhm2fx5lnrhxv7h7kpztlpa3ahf3n4w0q0zj5epj4av9xjq6ljsja3xk8z7rzd067kc7mgpy9448rdfzpfjz5gq389zdmpgnk6rp4ykk0xk6cmqw6zqcrnmsuaxv3yzsvcwsd4gagtalh0uzrdvy03nhmltjz2eu0232qlcs0zvxuqyut73yucd9gy5jaudnyt7yqhgpqv'
```

The address is a public Wcash regtest fixture, not evidence that anyone has its
spending key. The node currently implements Wcash payment-address codecs and
private coinbase construction, but not a Wcash wallet or account/key
derivation. Replace it with a generated, spendable address once wallet support
exists.

Because that child coinbase is deliberately private, the coordinator cannot
recover its recipient from a payment address alone. It trusts the loopback
Wcash node's reviewed `createauxblock` implementation; the child binary,
configuration, RPC socket, and host are therefore payout-critical.

`ZCASH_PAYOUT_ADDRESS` must be the canonical shielded address configured as
`mining.miner_address` on both parent nodes. The coordinator checks a
domain-separated template-node commitment, recovers every shielded recipient
from both exact coinbases with Zcash's public zero OVK, and compares all
transparent outputs with the independent validator's ordinary template.

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
public value-pool totals. A mined 10-WCASH child block increases Ironwood by 10
while transparent, Sapling, and Orchard coinbase value remain zero. The
recipient and note contents are not publicly recoverable with the conventional
zero outgoing-viewing key; gross protocol issuance remains auditable. A miner
can still disclose its own viewing data voluntarily.

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

A successful run proves that this checkout can construct a private Wcash
coinbase, authenticate AuxPoW v2 through both Zcash transaction commitments,
solve real Equihash, pass an unmodified Zcash proposal validator, submit exact
blocks to both chains, and accept a canonical ZIP-301 share bound to an exact
authenticated worker. The automated native E2E test mines through two ZIP-301
generations and checks the resulting journal aggregate.

It does not prove wallet recovery, vendor-by-vendor ASIC interoperability,
public variable-difficulty behavior, payout correctness, Internet-facing
security, long-running reorg behavior, or independent consensus-review results.
No physical ASIC model or firmware is certified. Those remain public-testnet
release gates.

Wcash payment namespaces are disjoint from Zcash: Unified `wu...`, Sapling
`ws...`, TEX `wtex...`, and transparent `W...`, with separate testnet and
regtest forms. Never create a Wcash address by editing a Zcash prefix; checksums
and Unified Address jumbling bind the namespace.
