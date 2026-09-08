# Wcash/Zcash mining protocol compatibility

This document records the interoperability boundary of the native merged-mining
backend. It distinguishes source-level compatibility from behavior exercised by
the automated test suite and from physical hardware certification. A device is
not declared compatible merely because it accepts a pool URL in its web UI.

The audit baseline is 8 September 2026:

- [ZIP 301](https://zips.z.cash/zip-0301), `Active` in the Zcash ZIP repository
  at commit
  [`e753a6a301912cf77202db8f0d840f4796f5cca1`](https://github.com/zcash/zips/blob/e753a6a301912cf77202db8f0d840f4796f5cca1/zips/zip-0301.rst);
- S-NOMP `node-stratum-pool` commit
  [`aa3deddc8caa3ba5dfcff92c7db0f5360e812c10`](https://github.com/s-nomp/node-stratum-pool/tree/aa3deddc8caa3ba5dfcff92c7db0f5360e812c10);
- NiceHash `nheqminer` commit
  [`b9900ff8e3c6f8e5a46af18db454f2a2082d9f46`](https://github.com/nicehash/nheqminer/tree/b9900ff8e3c6f8e5a46af18db454f2a2082d9f46);
- the NiceHash extranonce-subscription specification at commit
  [`ac12d959c7824faaff544e266e7e7d80e4dbbcc0`](https://github.com/nicehash/Specifications/blob/ac12d959c7824faaff544e266e7e7d80e4dbbcc0/NiceHash_extranonce_subscribe_extension.txt);
- BITMAIN's official [Z15 setup guide](https://support.bitmain.com/hc/en-us/articles/7942533055641-How-to-set-up-Z15-Server)
  and [Z15 family specification](https://support.bitmain.com/hc/en-us/articles/900001323646-Z15-Series-Specifications).

## Compatibility matrix

| Client or protocol | Current status | Evidence and boundary |
|---|---|---|
| Canonical ZIP-301 client | Supported and tested | The server implements newline-framed JSON-RPC, `mining.subscribe`, exact-worker `mining.authorize`, `mining.set_target`, the eight-field `mining.notify`, and five-field `mining.submit`. The bundled reference client reconstructs work only from those messages, solves real Equihash, and the native end-to-end gate submits winners to both chains through the persistent listener across two job generations. |
| NiceHash `nheqminer` ZIP-301 client | Source-compatible; runtime test pending | Its [Stratum client](https://github.com/nicehash/nheqminer/blob/b9900ff8e3c6f8e5a46af18db454f2a2082d9f46/nheqminer/libstratum/StratumClient.cpp) accepts an eight-or-more-field notification, consumes a big-endian 256-bit target, uses the subscribed nonce prefix, and submits a CompactSize-prefixed 1,344-byte Equihash `(200, 9)` solution. It probes `mining.extranonce.subscribe`; this backend now returns the extension's documented negative response. The archived miner still needs a reproducible Linux build/run in CI before this row can be called tested. |
| S-NOMP/Z-NOMP-style Equihash client | Wire-compatible canonical subset; simulator or hardware test pending | S-NOMP uses a [four-byte nonce prefix and canonical submit tuple](https://github.com/s-nomp/node-stratum-pool/blob/aa3deddc8caa3ba5dfcff92c7db0f5360e812c10/lib/stratum.js). As a server it [appends optional algorithm and personalization fields](https://github.com/s-nomp/node-stratum-pool/blob/aa3deddc8caa3ba5dfcff92c7db0f5360e812c10/lib/blockTemplate.js#L284-L301) to `mining.notify`; Wcash emits only ZIP-301's canonical eight fields. A client that incorrectly requires S-NOMP's extra fields needs an adapter. |
| BITMAIN Antminer Z9/Z11/Z15 family | Candidate hardware; not certified | Z15 documentation confirms Equihash/Zcash support and pool URL, worker, and password configuration, but does not publish a complete wire contract. A physical device or a vendor firmware simulator must pass the acceptance test below. The built-in listener is loopback-only, so hardware also requires a separately secured LAN test edge. |
| NiceHash extranonce extension | Explicitly unsupported, interoperable probe | The backend returns `result: false` with error 20 as specified. It never changes `NONCE_1` in an active session; job rotation closes the connection and allocates a fresh prefix. |
| Unmodified S-NOMP or another ordinary Zcash pool server | Not a valid Wcash template source | An ordinary pool [requests its own GBT and constructs the job](https://github.com/s-nomp/node-stratum-pool/blob/aa3deddc8caa3ba5dfcff92c7db0f5360e812c10/lib/pool.js#L475-L493). It does not request the private `wcashaux` extension or preserve this coordinator's proposal bytes. Its miner-facing ideas can be reused in a new edge, but placing it between this coordinator and the Zcash node would break the authenticated child commitment. |
| Bitcoin-style Stratum V1 (`mining.set_difficulty` and Bitcoin coinbase/extranonce jobs) | Not compatible | Its job and nonce construction are different from ZIP-301. Translating a difficulty number alone is insufficient. Only an adapter that terminates the client protocol and constructs the exact Zcash header could bridge it. |
| Stratum V2 | Not implemented | Stratum V2 is a different binary, channel-based protocol. It is not required to validate the Wcash AuxPoW design and should be implemented as a separate authenticated edge if demanded by miners. |
| Public TLS pool with accounting and payouts | Local auth/accounting only; public pool not implemented | The native listener is a loopback, fixed-target backend with exact-worker Argon2id authentication and an authoritative journal plus offline aggregate report. Internet-facing TLS, source-IP/account abuse controls, variable difficulty, balance calculation, payouts, and production monitoring belong in a separately reviewed pool edge and settlement system. |

`Source-compatible` means that the published client source and this server agree
on every field needed to construct and submit a header. It is deliberately
weaker than `tested`: firmware can contain undocumented parsing limits,
timeouts, retry policy, or vendor extensions.

## Exact ZIP-301 contract emitted by Wcash

The sequence for a new connection is:

1. The miner sends `mining.subscribe`. The server returns
   `[null, NONCE_1]`. Session resumption is not offered.
2. The miner sends `mining.authorize` with an exact worker name and that
   worker's password. The server verifies it against the Argon2id PHC hash in
   the private version-1 registry and binds accepted shares to the canonical
   authenticated identity. Unknown workers and incorrect passwords receive the
   same failure response.
3. The server returns authorization success, then sends `mining.set_target`
   with one 64-hex-character, big-endian target.
4. The server sends `mining.notify` with job ID, little-endian version,
   previous-block hash, Merkle root, block-commitments field, time, compact
   difficulty, and `clean_jobs = true`.
5. The miner concatenates the four-byte `NONCE_1` with its 28-byte `NONCE_2`
   and solves Equihash `(200, 9)` using the `ZcashPoW` personalization.
6. The miner submits worker, job ID, frozen time, `NONCE_2`, and the canonical
   solution encoding: `fd4005` followed by exactly 1,344 solution bytes.

The fifth notification field is named `RESERVED` in the original ZIP-301 text.
In the [current Zcash block-header format](https://zips.z.cash/protocol/protocol.pdf#blockheader)
it carries `hashBlockCommitments`. Miners must treat those 32 bytes as opaque
header bytes; replacing them with zero would invalidate both ordinary Zcash
work and the authenticated Wcash commitment.

The listener accepts common harmless subscription deviations because it does
not depend on the user-agent, requested session, host, or port values. This
includes the historical `nheqminer` behavior of encoding the port as a JSON
string rather than ZIP-301's integer. Security decisions do not use those
untrusted fields.

Automatic `native-serve` limits an advertised generation to 45 seconds. It
disconnects that generation's sessions but keeps one loopback socket bound,
then prepares and proposal-validates a genuinely fresh child and parent job
before accepting miners again on the same port. This stays below S-NOMP's
documented 55-second liveness rebroadcast interval without pretending that the
same frozen header is new work and without rebinding through `TIME_WAIT`.
`native-serve-once` intentionally does not rotate on age because it is a
bounded integration-test command.

Both native serve modes require `WCASH_WORKER_CREDENTIALS`, a private regular,
non-symlink version-1 JSON registry containing exact worker names and Argon2id
PHC hashes. `worker-password-hash` creates the accepted hash format from the
selected plaintext `WCASH_STRATUM_PASSWORD`; the server itself never reads that
environment variable. Argon2id verification has a separately configurable
global concurrency bound via `WCASH_AUTHENTICATION_LIMIT` (default 4, maximum
256), in addition to per-connection authentication limits.

## Physical ASIC acceptance test

Certification for one exact device model and firmware revision requires a
packet capture and server journal from a real native job. Record the model,
firmware checksum, and configuration alongside the result. The test passes only
if all of the following hold:

1. The ASIC reconnects, subscribes, authorizes, and begins work without a
   vendor-specific notification field.
2. It preserves the four-byte server nonce prefix and submits a 28-byte
   `NONCE_2`.
3. It hashes the exact nonzero post-NU5 block-commitments field supplied in the
   job.
4. It submits the canonical 1,347-byte encoded solution and receives an
   acceptance for an Equihash-valid share at the advertised target.
5. Replaying that submission returns error 22, and submitting an old job after
   rotation returns error 21 or is prevented by disconnect.
6. A deliberately too-hard target produces no accepted low-difficulty share;
   malformed solutions are rejected locally and never reach either node.
7. In an isolated easy-difficulty run, one real header meeting both targets is
   submitted independently to the Wcash and Zcash nodes. Both nodes must accept
   the exact bytes that were derived from the proposal-validated job.

Do not use the all-`ff` share target with production-rate hardware for a soak
test. If an ASIC evaluates `R` Equihash solutions per second and the desired
mean share interval is `I` seconds, an initial uniformly distributed hash
target is approximately `floor(2^256 / (R * I))`. Start conservatively, observe
actual submissions, and keep the backend far below its per-connection limit of
64 submissions per second. A production edge must replace this fixed estimate
with bounded variable difficulty.

## Remaining protocol gaps

The following are release gates for a community-facing pool, not consensus
changes:

- a reachable TLS edge with individual account credentials and source-IP
  controls; the backend's exact-worker credentials authenticate local clients
  but do not provide transport security or Internet abuse controls;
- bounded variable difficulty with old-target grace tied to each job;
- in-session delivery of fresh generations plus a bounded grace window for
  recently retired jobs; the local backend's 45-second disconnect-and-reconnect
  rotation stays below the [55-second S-NOMP liveness interval](https://github.com/s-nomp/node-stratum-pool/blob/aa3deddc8caa3ba5dfcff92c7db0f5360e812c10/README.md#L179-L182),
  but a production edge should avoid reconnect churn while never replaying a
  frozen header as fake new work;
- a separately reviewed balance, payout, and settlement engine consuming
  snapshots from the authoritative version-2 journal; `accounting-report`
  validates and aggregates shares with explicit authentication provenance only
  after the pool releases its exclusive journal lock;
- checkpointed startup recovery and controlled journal segment rotation; the
  long-lived local supervisor replays once and rotates generations without a
  rescan, but startup cost and the 1 GiB hard ceiling still make an unsegmented
  journal unsuitable for sustained public share volume;
- vendor-by-vendor hardware runs, including reconnect and failover behavior;
- metrics for active workers, accepted/rejected shares, validation saturation,
  stale generations, journal health, and independent node submission results;
- restart, reorg, node-outage, duplicate-winner, and long-duration load tests.

None of these gaps allows an invalid AuxPoW block to pass consensus. They affect
miner reachability, payout correctness, availability, and operational safety.
The coordinator must continue to validate the exact parent proposal and both
network targets locally; an edge must never be allowed to replace that trust
boundary. The bundled `zip301-mine` client and automated two-generation test
cover the real socket and Equihash submission path, but they do not certify any
physical ASIC model or firmware.
