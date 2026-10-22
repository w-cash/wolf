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
