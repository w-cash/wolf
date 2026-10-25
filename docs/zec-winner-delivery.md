# ZEC winner delivery and tracking

This focused change follows the operator's direct request on October 23, 2026:
repair ZEC delivery and add tracking without per-share logging or diagnostic
work delaying submission. Wcash submission policy and both chains' accounting
rules are unchanged.

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

## Independent chain workers

The backend owns four winner workers: one live first-attempt worker and one
retry/audit worker for each chain. A slow Wcash submission or status check no
longer occupies Zcash's worker, and a slow Zcash recipient does not occupy
Wcash's worker. This also applies when one share qualifies on both chains:
both exact proofs are committed atomically, then each chain can submit through
its own worker. The receipt order and durable journal format do not change.

Each live worker follows its chain's durable winners and publishes an exclusive
audit bound only after claiming the selected proof. Its audit worker cannot
take a fresh proof before that first attempt. Shared claims prevent concurrent
reconciliation of the same share and chain. Recovery retains exact bytes and
selects pending work separately for each chain. A filtered scan that reaches its
inspection budget continues scanning rather than sleeping before the journal
tail, so a run of other-chain proofs cannot strand new work.

The scheduler owns four persistent workers; existing per-recipient RPC fanout
remains unchanged. Work within each live lane remains serial. Local validation,
journal durability, recipient identity and predecessor checks, RPC deadlines,
and peer transport can still take time or defer delivery. Independent chain
scheduling is not a zero-delay guarantee or evidence that a public peer received
a future winner.

## ZEC submission timings

Result records identify the public winner hash, job, height and recipient role
with a numeric index. They separate recipient preflight, the `submitblock` call,
post-call canonical classification, and diagnostic queue wait. The measured
submit-call interval includes local request construction, client queueing and
network time; it is not a wire-send timestamp. No endpoint credentials, response
text, block bytes or wallet material are added to these records.

A bounded, nonblocking in-memory cache records the same-process time immediately
after a new ZEC winner's durable commit. When available, this permits
commit-to-reconciliation and commit-to-submit measurements. Replayed commits do
not reset the origin. Restarts, eviction or metadata contention leave origin
and attempt fields unknown rather than inventing a zero. These clocks do not
measure hardware discovery or the time before journal durability.

Timing records follow results and use the existing lossy, paced diagnostic
channel. The first same-process attempt bypasses the writer's retry-rate
suppression, including all recipient results and the aggregate result. Later
and unknown-origin attempts remain paced. Sink stalls, full queues and
repeated-result suppression can omit text;
drop and suppression counters make that limitation visible. The durable winner
journal remains authoritative, and timing collection adds no RPC, disk sync,
sleep or formatting before submission.

## Job preparation timings

`job_preparation_result` is emitted after backend actor activation, or after a
failed preparation attempt returns. Successful attempts below one second are
silent. The record separates network identity, candidate retirement, observer
catch-up, Wcash candidate RPC, local child decoding and payout validation,
Zcash template and proposal checks, exact-tip fences, generation journal work
and actor activation. Repeated phases accumulate exclusive monotonic durations;
parallel requests within a phase contribute caller elapsed time, not a sum of
overlapping request times. Cleanup during preparation error unwinding remains
part of the last active phase. Retirement after actor-activation failure occurs
after that failure record and is not included.

Only fixed phase names, safe error classes and public candidate heights are
recorded. The preparation thread keeps a fixed-size stack timer and uses the
existing bounded, paced diagnostic channel only after the result. It adds no
RPC, per-share record, synchronous diagnostic write or winner-path lock. Full
queues or a stalled writer can omit optional text; existing drop and
suppression counters remain the visibility boundary.

These records measure preparation through durable actor activation. They do
not include the preceding retry sleep, projector ingestion or delivery of a
Stratum notification to an ASIC. PostgreSQL `backend_events.recorded_at` is
projection time, so it must not be treated as an exact backend activation or
hardware-notification timestamp. Existing ZEC winner timing records have a
separate scope.

## Alternate Wcash payout cache

A merged job still needs a valid Wcash candidate before the Zcash proposal can
commit to it. If `createauxblock` requests a private recipient different from
the node's configured recipient, repeatedly creating an isolated coinbase cache
repeats shielded proving even when child height and fees have not changed.
Ordinary template prewarming for the configured recipient does not warm that
alternate recipient.

The Wcash RPC handler retains at most four alternate-recipient coinbase caches
across request clones. Exact miner parameters isolate recipients, coinbase data,
memo and payout commitments. Each cache retains the existing height, fee and
single-builder gates. The recipient-map lock is released before proving or
waiting; an evicted entry remains valid for requests already holding its cache.
Alternate requests never consume the configured recipient's precomputed block
template. Every candidate still passes payout, proposal and exact-tip checks.

This removes repeated same-height proving while an entry remains cached; a
new height, changed fee total or eviction can still require a proof. It is not
a zero-latency guarantee. The cache change requires an updated **Wcash node**;
the preparation records require an updated **native backend**. Updating only
the backend cannot repair node-side cache reuse. Neither change enables ZEC
payouts or changes recipient configuration, consensus or winner submission.

## Local same-height catch-up

When the public-chain observer and a work node report different hashes at the
same height, the backend can offer the observer's unknown block directly to
that work node. Both nodes must have the same canonical predecessor, and the
observer's exact serialized block, hash, coinbase height and previous hash must
match the sampled tip. Observer and recipient tip checks fence the write.
The recipient performs its ordinary consensus validation and fork choice;
only exact resulting tip agreement permits preparation to continue.

A block already known on the recipient's side chain is never resubmitted or
forced into its best chain. Pending or ambiguous writes are limited to one
attempt per recipient/hash every five seconds in a separate 128-entry cache.
The retry check precedes another block download. A post-commit gossip-admission
error or canonical-duplicate throttle is advisory only when authoritative tip
checks prove convergence; otherwise preparation remains deferred. Hard protocol
failures remain errors.

This affects preparation only. It adds no share-path logging, does not acquire
the winner-delivery cooldown, and retains all existing network, proposal, payout
and exact-tip gates. Already matching nodes take the existing fast path.
An ahead work node or a deeper fork is not forced backward. The optimization
does not remove ordinary block-rollover races or guarantee immediate convergence
when the recipient legitimately prefers another branch.

Forward catch-up applies the same authoritative canonical-height check when
`submitblock` returns the specific duplicate-gossip admission throttle. A peer
can commit the exact block between the initial tip read and the relay request;
another announcement being rate-limited does not undo that commit. The backend
continues only after the expected hash is canonical at each relayed height and
the recipient reaches the sampled observer tip. An unconverged throttle remains
a bounded preparation retry, including the node's exact missing-height replies
after that throttle; other RPC errors keep their existing handling. Only this
missing-height error path adds a recipient tip read. Healthy preparation and
winner delivery gain no polling, delay, logging or synchronization.

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
Chain-worker fixtures must hold Wcash and Zcash independently, including the
counterpart of a dual winner, and prove that the other chain dispatches before
release. Cover same-chain serialization, filtered scans beyond their inspection
budget, independent restart recovery, live/audit claim exclusion, and complete
worker cleanup after partial startup failure or shutdown.

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
