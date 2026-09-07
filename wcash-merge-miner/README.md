# Wcash local Zcash merge-miner

This workspace development crate creates and validates genuine Wcash AuxPoW
proofs backed by Zcash Equihash `(200, 9)`. It never enables a fake proof mode
and it does not weaken `wcash-zcash-aux` validation.

For a Wcash child block ID, the harness:

1. constructs a canonical NU6.3/v6 Zebra coinbase with an exact zero-value
   `OP_RETURN` Wcash commitment;
2. uses that coinbase txid as the root of a one-transaction parent Merkle tree;
3. searches 32-byte nonces using the real Tromp `(200, 9)` solver;
4. checks the complete parent header hash against the target supplied by Wcash;
5. runs the complete production AuxPoW validation pipeline; and
6. emits the canonical proof bytes that belong in the Wcash header witness.

## Build and mine

Run this crate from the node workspace:

```sh
cargo run --release -p wcash-merge-miner -- mine \
  000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f \
  ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff \
  16
```

The child hash and target are raw/little-endian 32-byte values. The easiest
target (`ff..ff`) is suitable only for a local chain. A run is intentionally
memory intensive; use `--release`.

The node's `internal-miner` feature uses the same pipeline automatically for a
Wcash template. It derives the proof-independent child ID and canonical target
from the Zebra header, searches the synthetic parent in small cancellable
batches, attaches the verified proof, and submits the resulting child block
through the normal `submitblock` path. Native Zcash templates continue to use
Zebra's original direct Equihash solver.

To inspect a job without solving it:

```sh
cargo run --release -p wcash-merge-miner -- job <child-hash-le> [target-le]
```

## Local JSON-lines interface

```sh
cargo run --release -p wcash-merge-miner -- serve <child-hash-le> [target-le] [127.0.0.1:28237]
```

The server refuses non-loopback bind addresses, caps each request at 64 KiB,
and serves at most eight clients concurrently. Excess connections are closed
immediately, and an idle, reset, or malformed client cannot stop the listener.
It supports `mining.subscribe`, `mining.authorize`, `mining.get_job`, and
`mining.submit`. A submit request is:

```json
{"id":3,"method":"mining.submit","params":{"job_id":"...","nonce_le":"64 hex chars","solution":"2688 hex chars"}}
```

An accepted response contains the encoded `auxpow_proof`. This is a deliberately
small Stratum-like interoperability surface, not a claim of compatibility with
every deployed Zcash pool dialect.

## Important parent-chain boundary

The default parent transaction is structurally canonical and sufficient for an
isolated AuxPoW round trip, but it intentionally has no ZEC payout. It is not a
complete current-height Zcash coinbase. A production merge-mining pool must:

- start from a live Zcash `getblocktemplate` and preserve its subsidy, funding,
  lockbox, and payout requirements;
- use a modified parent template producer or a complete Zcash coinbase builder
  to include exactly one zero-value Wcash commitment before shielded proofs and
  authorizations are finalized;
- rebuild, re-prove, and re-authorize the coinbase after commitment or
  extranonce changes, then recompute its mined txid, auth-data root, transaction
  Merkle root, and block-commitments hash as applicable;
- distribute the same Equihash job to miners;
- submit a share to Wcash whenever it meets the Wcash target; and
- also submit the parent block to Zcash when it meets the harder Zcash target.

The parent `nBits` field carried in a proof is never allowed to choose Wcash
difficulty. The authenticated Wcash target remains authoritative.

Blindly appending an output to a completed `coinbasetxn` is not a valid adapter:
modern shielded Zcash coinbases authenticate their transaction contents. The
parent node must support the commitment during coinbase construction, or the
adapter must perform the complete builder, proving, and authorization flow.

Wcash's shielded coinbase-reward policy is a child-chain consensus concern. It
does not make the independent Zcash parent coinbase payout private.

## Tests

```sh
cargo test -p wcash-merge-miner
cargo clippy -p wcash-merge-miner --all-targets -- -D warnings
```

The expensive real-solver test is ignored in routine debug runs. Run it
explicitly in release mode when validating a platform toolchain:

```sh
cargo test --release -p wcash-merge-miner \
  real_solver_produces_a_strictly_valid_round_trip -- --ignored --nocapture
```
