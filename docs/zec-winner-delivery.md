# ZEC winner delivery and tracking

This focused change follows the operator's direct request on October 23, 2026:
repair ZEC delivery and add tracking without per-share logging or diagnostic
work delaying submission. Wcash delivery and accounting policy are unchanged.

## Delivery design

The existing journal commits each qualified winner and its exact block bytes
before acknowledging the share. This remains the durable discovery authority;
there is no additional share-path sync, observer query, or log formatting.

For the Zcash profile, mined-block fanout reports local and non-local transport
outcomes. All-failed, local-only, empty and ambiguous results retain the existing
paced mined retry obligation. Ordinary gossip keeps its best-effort contract.
A successful non-local advertisement is transport evidence for an `INV` hash,
not proof of full-block receipt, validation or canonical inclusion.

The backend's background audit also re-admits retained, locally observed ZEC
winners while the exact hash is canonical and below coinbase maturity. It uses
the existing durable snapshot after a restart, with a paced cooldown and no
accounting schema change. It never replays an overtaken competing block,
side-chain proof or matured reward. Initial delivery remains independent of
historical audits. Supplementary delivery failures do not block share admission.

Diagnostics follow the result or failure of a submission. A stalled diagnostic
sink may lose text records but cannot hold a share or a block submission.
Exact discovery records remain durable even if optional diagnostics cannot start.

Pool delivery dispatches writable recipients concurrently. Each recipient checks
its own network identity and current predecessor before submitting the retained
bytes; an unavailable observer or another writable node cannot veto healthy
recipients. Re-delivery of an observed winner requires that recipient's exact
canonical hash below maturity. Strict all-node identity and confirmation checks
still gate accounting after delivery. The read-only observer never submits.

## Independent report

`scripts/zec-winner-tracking.py export --deployment UUID` reads the existing
PostgreSQL projection using the operator's existing `PG*` environment. Run it as
a SELECT-only identity. Its transaction is explicitly repeatable-read/read-only,
with a three-second statement timeout. It exports only public ZEC candidate
hashes, heights, local states and source event/projection provenance. A cap or
timeout fails explicitly; it never returns a partial all-history total as complete.
It does not open the live journal, invoke the backend API, or change payouts.

Run `observe` on the operator workstation, outside the pool:

```sh
python3 scripts/zec-winner-tracking.py observe \
  --snapshot zec-winners.json --observer observer.example:443 \
  --protoset lightwalletd_descriptor.bin --limit 1
```

Use an independently operated TLS lightwallet server, with its actual descriptor.
The export verifies the mainnet genesis. The external collector verifies the
frozen Zcash Sapling activation block at height 419200 (lightwallet servers need
not serve the genesis tree state), compares the exact hash at the fixed
candidate height, and fences each observation with a stable tip. Save each JSON
result as a new dated artifact; do not overwrite prior canonical/reorg evidence.
For more than one page, follow `next_offset`; incomplete coverage is explicit.
The default checks one candidate per invocation, with bounded read-only requests.
No public observer response controls submission, mining admission or accounting.

Report discovery, local lifecycle and independent canonical observation
separately. Projection timestamps are not exact historical discovery times.
Unavailable or inconsistent public evidence means unknown. A different canonical
hash can reflect a normal race or reorganization; it does not prove send failure.
The report cannot reconstruct attempt results omitted from historical text logs.

## Review vectors

Required fixtures cover all failed sends, local-only success, mixed/queued peer
outcomes, timeout and reconnect recovery, ordinary tip progress while mined work
retries, exact-byte replay after local observation/restart, competing/reorganized
and matured candidates, stalled/broken diagnostic sinks, provider outages and
fixed-height external reorg observations. Run them off production. No fabricated
or historical block is submitted to mainnet as a test.
Outer-supervisor fixtures must also hold each recipient or observer unavailable
and prove that an eligible healthy recipient receives the exact bytes first.

## Fresh ZEC announcement scheduling

Zcash gossip owns at most two concurrent attempts: the existing fair general
lane and one lane reserved for a fresh RPC-submitted hash. An older stalled
announcement therefore cannot by itself hold a fresh hash behind its six-second
deadline. Existing attempts remain owned until completion or their deadline;
retries keep their one-second pacing and share the same readiness/request
deadline. When both lanes
are occupied, further hashes wait within the existing bounded pending storage.
The Wcash profile retains its single-attempt scheduling policy.

The Zcash peer set retains up to 64 queued fanouts in enqueue order, skipping a
full caller channel so other retained work can progress. Queue overflow
returns an explicit retryable failure before issuing any new wire calls. A
disconnected peer or cancelled caller releases its unsent obligations. Cleanup
of those recipient sets preserves response futures already handed to an active
caller. Cancelling that caller or reaching its deadline still cancels its owned
attempt. A connected busy peer remains owed delivery. Wcash retains its previous
latest-hash replacement behavior.
Queued sends still require the peer set to be polled.

Advertisement result and failure records include `queue_wait_ms`,
`readiness_wait_ms`, `request_elapsed_ms`, `total_elapsed_ms`, and `lane`.
The queue timing excludes deliberate retry pacing; total elapsed time includes
the complete retained lifecycle. No per-share log or new pre-dispatch INFO
record is added. An RPC-submitted hash is not automatically a pool discovery:
correlate it with the durable winner journal before reporting it as one.

Paused-clock fixtures reproduce head-of-line waiting and recipient cleanup
failures. Local TCP fixtures hold an older announcement open, then request and
compare the complete bytes for the fresh block. These tests exercise real local
wire delivery without sending test blocks to public peers. They do not establish
future public receipt, validation or canonical inclusion, and two occupied lanes
can still delay subsequent announcements.
