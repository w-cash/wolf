# Wcash merged mining with Zcash

Wcash uses one parent proof-of-work profile: Zcash Equihash `(200, 9)`. Other
algorithms and parent chains are deliberately outside this branch.

Testing-chain Wcash rewards are displayed as valueless `TWC` (Test Wcash).
`WEC` remains the mainnet ticker; the label does not change integer-zatoshi
amount serialization or any mining target.

The implementation has three separate trust boundaries:

1. A Wcash child node creates and caches an exact proof-independent candidate
   through `createauxblock`.
2. A pool-controlled Zcash template node adds that child ID to an otherwise
   ordinary parent coinbase and returns a complete `getblocktemplate` job.
3. The pool validates the template locally and sends the exact candidate to at
   least one distinct Zcash node in proposal mode before releasing work.

The same Equihash solution can satisfy neither, either, or both network
targets. Wcash always derives the required target from authenticated child
state; the parent header's `nBits` cannot lower it.

## Exact commitment profile

AuxPoW version 2 appends this 44-byte suffix to the Zcash coinbase input's
miner data:

```text
fabe6d6d || reverse(aux-root) || tree-size:u32-le || nonce:u32-le
```

There is no transparent `OP_RETURN` output and no script push wrapper. Pool
identification bytes may precede the suffix, but the marker must occur exactly
once and the suffix must be the final miner data. A parent pool must reserve 44
bytes inside Zcash's 100-byte coinbase-script limit.

ZIP-244 deliberately excludes coinbase miner data from the mined transaction
ID. Wcash therefore requires two independent parent proofs:

- the coinbase transaction ID follows a SHA-256d branch at index zero to the
  parent header's transaction Merkle root; and
- its authorizing-data digest follows a `ZcashAuthDatHash` branch at index zero
  to the authorization-data root, which combines with the supplied chain
  history root to reproduce the header's `hashBlockCommitments`.

This closes the malleability gap that would exist if consensus checked only the
transaction ID. Only canonical Zcash V5 and V6 coinbases are accepted. Proof
version 1 is intentionally rejected.

The complete binary layout, bounds, byte-order contract, and frozen
non-palindromic vector are documented in
[`wcash-zcash-aux/README.md`](../wcash-zcash-aux/README.md).

## Node RPC contract

The Wcash child node exposes an exact-candidate mining API:

```text
createauxblock("<Wcash payout address>")
submitauxblock("<candidate hash in display order>", "<AuxPoW v2 hex>")
getauxblockstatus("<candidate hash in display order>", "<AuxPoW v2 hex>")
retireauxblock("<candidate hash in display order>", "<retire token>")
```

`createauxblock` accepts an exact Wcash address in the selected network's
namespace. A transparent address selects the normal pool-integration path; a
Unified Address with an Orchard receiver explicitly selects a private Ironwood
coinbase. It proposal-validates the proof-free child,
rechecks the tip, and caches the exact block. The response includes `hash`, the
canonical proof-free block `data`, a secret 32-byte `retiretoken`, `chainid`, `previousblockhash`,
`coinbasevalue`, `target`, `bits`, and `height`. The coordinator independently
decodes `data` and verifies its hash, height, predecessor, wire version,
difficulty, and empty witness. Candidates are bounded to 16 entries and expire
after ten minutes. A candidate remains exact and retryable across a shallow
child reorg while its parent is retained in either committed chain.

When a caller selects a payout different from the node's configured recipient,
the handler retains a bounded set of four recipient-isolated coinbase caches.
Repeated requests with exactly the same miner parameters, height and fees can
reuse a proof-complete private coinbase across RPC clones. A recipient change
never reuses another recipient's coinbase or configured block template. The
existing height/fee limits, single-builder behavior, proposal verification and
tip fences still apply. A cold entry, changed height or fees, or recipient-cache
eviction can still require proving; ordinary configured-recipient prewarming
does not promise a ready alternate-recipient proof.

`createauxblock.coinbasevalue` is the miner's **net** payout including all fees:
`subsidy - foundation + fees`. It excludes the compulsory transparent foundation
output. During Mainnet heights 33000–621454, pool accounting must credit miners
from this net amount. The coordinator derives the fund amount and recipient
from its own candidate-height policy and checks the serialized coinbase before
attesting miner rewards. Candidate caches, replacement jobs and outbox replay
continue to bind the exact block and height. Both node and coordinator must
upgrade; the Zcash parent reward is unchanged. See the
[upgrade guide](wcash-foundation-fund-upgrade.md).

`retireauxblock` lets the pool release an unsolved or parent-only generation
when every listener and in-flight share handler has stopped. Its per-candidate
capability is generated from the operating system CSPRNG, is redacted from
coordinator diagnostics, and authorizes no other cache entry. Retirement is
idempotent after a successful call or node restart. Once any canonically
decoded `submitauxblock` attempt begins, retirement refuses to remove the
candidate; exact-witness retry and reorg safety take precedence over reclaiming
that slot. The native coordinator also refuses retirement while its durable
outbox contains a Wcash winner for the generation. Preparation failures are
retired before another candidate can be requested, and transient retirement
failures block rotation with bounded backoff instead of consuming more slots.

`submitauxblock` accepts a bounded canonical proof, attaches only the witness,
checks that the proof-independent block ID is unchanged, and submits through
the normal Wcash consensus and gossip paths. Unknown, expired, and stale
candidates fail closed.

`getauxblockstatus` reports `best_chain`, `side_chain`,
`conflicting_witness`, `pending`, or `unknown`. A `best_chain` response includes
the confirmation count read from the same state snapshot as the exact
witness-bearing block. This distinction is consensus-significant because two
different AuxPoW witnesses intentionally have the same Wcash block ID.

The private Zcash parent RPC `getblockstatus("<display-order block hash>")`
returns `best_chain` with exact hash, coinbase height, and atomic confirmations;
`side_chain` with exact hash and coinbase height from committed any-chain state;
or `unknown`. Queued or remembered hashes are never treated as committed.
Wcash rejects this method: its status must include the AuxPoW witness through
`getauxblockstatus`. The native pool backend needs the matching RPC release on
both the template node and every independent parent validator. Keep these
endpoints private and authenticated; no public RPC exposure or spending keys
are needed. Each parent must also prove its already identity-checked genesis
through this status API at startup and subsequent identity checks, with exact
height zero and positive best-chain confirmations. This rejects missing or
incompatible APIs before admitting new mining work.
All parents must positively identify the same exact committed
noncanonical proof before it can remain healthy pending work. It earns no
reward until ordinary best-chain observation and maturity checks pass. That
positive noncanonical evidence is durably recorded as `winner_side_chain`,
guarded by the required `winner_side_chain_v1` backend capability. Subsequent
monitoring is status-only: pruning or finalization of the losing fork cannot
trigger obsolete submissions or poison mining health. Absence alone never
establishes this state, and transport/protocol errors remain errors. The native
version-2 journal header stays unchanged and old records reopen without rewrite.
Once this new event is written, rollback requires a binary that understands it.
An older reader cannot reopen the extended journal; never truncate or reset
accounting history to force compatibility. The capability protects the live
pool/backend protocol, not backward readability of new journal records.

The Zcash parent template node accepts one private GBT extension:

```json
{
  "mode": "template",
  "capabilities": ["coinbasetxn", "proposal"],
  "wcashaux": {
    "blockhash": "<64-character display-order Wcash ID>",
    "nonce": 0
  }
}
```

The extension is valid only on a Zcash network, only in template mode, and not
with long polling. Zebra reserves the exact 44-byte carrier cost while selecting
transactions, creates the normal configured parent coinbase, then performs a
V5/V6-specific canonical wire splice into miner data. It proves the mined
transaction ID and shielded signature hash are unchanged, recomputes the
authorization digest, authorization-data root, and block-commitments hash, and
returns an ordinary complete template. Requests without `wcashaux` retain
standard Zebra behavior and an unmodified transaction-selection budget.

Independent proposal validators do not need this extension. They receive an
ordinary exact Zcash block proposal and retain ordinary Zcash consensus. For
the durable native pool backend they also need the read-only `getblockstatus`
method described above; this does not require the private GBT extension.

## Pool safety rules

The native adapter enforces these rules before a job reaches an ASIC:

- RPC credentials never appear in URLs, debug output, or command-line
  arguments. Plain HTTP is allowed only on loopback; remote RPC must use HTTPS.
- The parent configuration fields are private and are revalidated when clients
  connect, so library callers cannot bypass the loopback template-source or
  independent-validator requirements with a struct literal.
- RPC redirects are disabled, URL paths/query strings/fragments are rejected,
  every request has a deadline, and responses are capped at 8 MiB.
- Parent templates are decoded into a local JSON projection rather than shared
  node implementation types.
- The template node returns a domain-separated commitment to its canonical
  payout configuration. The coordinator verifies the exact `coinbasetxn`
  according to the configured Zcash address type: transparent scripts are
  matched directly, while shielded recipients are recovered with Zcash's
  consensus-public zero outgoing viewing key.
- Every proposal validator also supplies an ordinary, child-independent
  template on the same predecessor. Its payout must match the same configured
  address. Configured miner outputs are removed before the remaining
  funding-stream and lockbox outputs are compared exactly, so honest mempool
  fee differences do not stall work. Exact proposal validation independently
  enforces the candidate's subsidy-plus-fee total and prevents an extra
  diversion while allowing the validator to remain unmodified.
- For a transparent Wcash address, the coordinator checks the exact public
  recipient and value in the serialized child candidate. A private Ironwood
  coinbase is intentionally not publicly recoverable. The coordinator removes
  one exact mandatory foundation output, then verifies the private miner value
  and trial-decrypts every Ironwood action with the protected incoming viewing
  key, matching the configured receiver. The child
  binary, configuration, RPC socket, and host are payout-critical in either
  mode.
- Transaction bytes, transaction IDs, authorization digests, both Merkle
  roots, `hashBlockCommitments`, compact difficulty, expanded target, child
  commitment, and auxiliary nonce are independently recomputed or compared.
- Every configured proposal validator must use a distinct endpoint, agree on
  the parent tip, and return JSON `null` for the exact serialized proposal.
  Tips are checked again after proposal validation to close the race. The
  operator must still ensure those endpoints are independent processes and
  failure domains; an RPC client cannot prove operational independence.
- A winning parent block is assembled from those exact proposal bytes; only
  nonce and solution change. A winning child proof is submitted independently,
  so a failure on one chain never suppresses the other.
- Parent submissions are idempotent across ambiguous RPC failures by querying
  `getblockheader` with the exact winning hash. Confirmation retention uses the
  lowest depth reported by reachable pinned nodes. An exact-hash “not found” or
  not-best-chain response vetoes maturity, malformed replies fail closed, and
  only transport-unavailable nodes are omitted. Wcash recovery is stricter: it
  replays the complete block, then polls the exact AuxPoW witness and its depth
  atomically; a same-ID conflicting witness is never treated as success.
- Network winners are appended with exact child and parent block bytes to a
  single-writer, mode-`0600`, 1-GiB-capped JSON-lines outbox and synced before
  either submission. On Unix the parent directory must not be group/world
  writable, and every append verifies that the pathname still names the locked
  inode. The journal does not compact automatically. On restart each chain
  replays independently; blocks survive
  ambiguous RPC failures and reorg observation until 100 confirmations. Stable
  share IDs let payout/accounting software deduplicate retries. Any append or
  durability error, live pathname replacement, or in-lock panic poisons the
  live journal so no later write can turn inconsistent state or a torn tail
  into a valid-looking record. The parent-directory entry is synced before
  mining starts, and restart performs metadata-durable bounded tail recovery.

### Parent winner delivery evidence

Canonical acceptance, admission to the gossip queue, completion of a network
request, and receipt by an independent peer are separate observations. Accepted
shares and matching node tips alone do not establish delivery of a pool winner.
Correlate the exact winning hash across submission, outgoing gossip and an
independent observer before describing propagation as verified.

A pending Zcash winner remains eligible for identical-byte replay when the
template node has accepted that winner, including when it has become an
ancestor of the current template tip. A competing canonical block makes that
replay obsolete. Failed relay delivery or unconfirmed gossip admission retains
pending work; neither is reward credit. Confirmation and maturity still require
the existing independent canonical observations.

An exact best-chain Zcash duplicate submitted through `submitblock` recreates
its gossip notification. The node checks the committed bytes before doing so;
queued, absent and noncanonical duplicates do not qualify. Queue admission
failure returns an RPC error so the caller can retry the same bytes. This does
not change Wcash's separate witness-aware submission protocol.
Duplicate reannouncements have a global one-second admission interval shared
by RPC handler clones. Throttled duplicates return a retryable RPC error;
freshly accepted blocks bypass this limit. Failed queue admission does not
consume the interval.
Deploy and verify the matching node revision before relying on this behavior:
an older or third-party node's `duplicate` response alone does not establish
recreated gossip admission.

Block gossip retains up to 64 distinct received mined notifications, including
the active attempt, in addition to the existing bounded RPC channel. It bounds
readiness and request completion together, retries incomplete requests, and
gives current committed tips a bounded scheduling opportunity. A full pending
set stops draining the RPC channel; it does not provide unlimited storage.
These in-memory notifications do not survive a process restart. Recovery relies
on the durable pending winner and identical-byte resubmission.

The `sync.gossip` metrics distinguish queued mined work, backpressure, attempts,
retries and completed network requests. Completion is not a peer receipt
acknowledgment. Isolated tests can verify wire announcements and retrieval of
the full retained block; they cannot guarantee public-chain adoption or future
availability. Keep monitoring independent receipt and canonical confirmation
after deployment.

At the normal `info` filter, correlate `rpc_gossip_admission` (queue admission),
`request_started`, `peer_send_summary` (per-batch attempted/ok/failed counts),
and `network_request_completed` by the public block hash. A batch with any failed
peer send is a warning, including `partial_failure`; `all_failed` means every
send in that batch failed. These counts describe service request outcomes,
not independent receipt or canonical acceptance. An empty queued pass is silent;
`peer_send_no_candidates` instead means the all-peer broadcast started with no
ready or queued peer candidates. Different batches can cover the ready and
previously busy peers of the same broadcast.

Queue-full/closed admission failures, incomplete mined retries, ordinary-tip
drops and peer-send failures remain visible without enabling blanket debug
logging. Repeated failures use fixed-size 30-second warning budgets and report
suppressed counts on the next admitted event. A permanent peer-set readiness
failure emits `gossip_task_exit`. Diagnostics do not change delivery retries or
best-effort peer-set return values. No diagnostic adds a network probe, disk
sync, block validation or per-share event. Continue independent observation:
even a completed peer request is not evidence of a remote chain's adoption.

The native pool backend emits JSON diagnostics with
`component="native_pool_backend"`. `winner_durable` follows successful journal
fsync; an identical receipt replay emits no new discovery event.
`winner_submission_attempt` and `winner_submission_result` identify actual
submission calls. `winner_lifecycle_persisted` follows a successful durable CAS
transition, with its new event sequence and confirmation depth when available.
Stale CAS results and observation passes that make no transition stay silent.
`winner_reconciliation`, `winner_persistence_failure` and `journal_failure`
describe typed failures without raw RPC bodies, error payloads or paths.
`observer_header_failure` distinguishes unavailable exact-header evidence from
authoritative absence; it does not change the existing confirmation policy.

Only fixed public metadata is copied into a 128-event nonblocking lossy queue.
One writer owns a separate stderr descriptor and performs formatting and I/O;
mining threads never wait for that writer. Repeated noisy events use a fixed
128-key cache and a 32-record budget per 30 seconds. Durable lifecycle events
and initial hard storage failures bypass that noisy-event budget, while the
queue remains bounded. `suppressed_total` and `diagnostic_dropped_total` expose
suppression and queue loss when output is available. A stalled or failed sink
can still lose diagnostics: the preserved authoritative journal and independent
exact-hash observation remain the recovery evidence. No new diagnostics run
for ordinary shares, and no diagnostic adds an fsync or validation pass.

The included ZIP-301 listener validates Equihash and both targets locally,
requires authorization, assigns per-session nonce prefixes, rejects malformed,
low-difficulty, duplicate, and stale shares, bounds frames and sessions, and
binds a literal loopback IP only. It also bounds concurrent Equihash validation,
serializes journal durability, and applies a sliding submission-rate limit per
connection. The `native-serve` supervisor retires stale
generations through the authenticated candidate lease and proposal-checks
replacements with bounded retry backoff. It
disconnects miners during rotation; a public deployment should add a bounded
multi-generation grace window as well as TLS, per-account authentication, rate
limiting, variable difficulty, payout accounting, and monitoring in front of
the listener.

The native listener requires a private exact-worker credential registry. Each
accepted login is verified against that worker's Argon2id PHC hash, and the
canonical authenticated identity is recorded with every accepted share.
Unknown worker names and incorrect passwords receive the same failure response.
This makes the journal suitable as authenticated input to a separately reviewed
accounting system, but it is not itself a balance, payout, or settlement engine.
The loopback protocol also has no transport encryption or source-IP controls;
a public edge must provide both without weakening the coordinator's template
and proposal-validation boundary.

## Why an unchanged third-party pool cannot be proxied

An ordinary F2Pool-style job does not commit the Wcash child ID. A downstream
proxy cannot safely add the commitment after receiving work: changing the
coinbase authorizing data changes `hashAuthDataRoot` and
`hashBlockCommitments`, so it creates a different Equihash header. Shares for
that different header are not shares for the upstream pool's job.

For now, the safe topology is a native Wcash/Zcash pool with its own parent
template node and an independent proposal validator. A large pool can support
Wcash later by adding the same `wcashaux` template step and dual submission;
Wcash consensus and the `createauxblock`/`submitauxblock` API do not need to
change. The present code intentionally contains no credentialed F2Pool proxy.

## Privacy boundary

The normal pool configuration pays both Wcash and Zcash coinbases to explicit
transparent addresses. Those recipients and values are public, which matches
the operational assumptions of existing pool software. Transparent Wcash
coinbase outputs mature after 100 blocks and must be shielded before ordinary
settlement. Aggregate issuance is auditable in every payout mode.

An operator can opt into a private Wcash reward by supplying a Unified Address
with an Orchard receiver through `WCASH_PAYOUT_ADDRESS`. That reward goes to
Ironwood using ordinary private note encryption rather than Zcash's publicly
recoverable coinbase outgoing-viewing key. The recipient or miner can still
disclose their own viewing data out of band, and pool logs can create off-chain
metadata.

Operators should pass `-` for the Wcash address command-line argument and put
the selected address in `WCASH_PAYOUT_ADDRESS`. Preflight output exposes only
that an address was configured and its environment-variable source. Raw
RPC-body logging is disabled, avoiding plaintext payout material in ordinary
process listings and logs.

The current payout policy deliberately fails closed if a future Zcash upgrade
adds shielded funding-stream outputs or a new shielded pool. Supporting such an
upgrade requires an explicit recipient-classification review; it cannot
silently weaken the parent reward check.

## Release boundary

The consensus and exact native dual-submit path pass local three-node Regtest.
The frozen Testnet profile has a separate automated phase that boots a clean
height-zero node without the debug sync override, obtains a child candidate,
mines through the ZIP-301 listener, and requires Wcash plus two Zcash processes
to accept the resulting work. The same release script also passes a separate
controlled Regtest wallet lifecycle: three private coinbases are scanned, a
one-TWC V6 transfer is matched in the mempool and block template, rejected by
both standard Zcash Regtest nodes, mined in a fourth AuxPoW block, and rescanned
with the full 25-TWC supply remaining in Ironwood. That transfer uses the
explicit Regtest-only unsafe one-confirmation override; the public Testnet wallet
policy remains 100 confirmations.

A third phase mines 101 transparent Wcash coinbases through the real AuxPoW
path. It rejects shielding at tip 99, enforces the 100-block maturity boundary at
tip 100, recovers every current UTXO from a deliberately late wallet birthday,
persists and broadcasts the exact shielding transaction, matches it in the
mempool and block template, and mines it at height 101. Final wallet and chain
checks conserve exactly 631.25 TWC across transparent and Ironwood pools.
Rotating more than 16 accepted child candidates also covers release of confirmed
winners beyond the coordinator's bounded active-candidate cache. Transparent
maturity is not overridden.

Mainnet, Testnet and Regtest profiles are implemented; see the
[network reference](wcash-consensus.md). These local scenarios do not establish
the status of a public deployment. A community-facing pool needs separate
evidence for seeds, consensus review, multi-node soak and reorg behavior, ASIC
interoperability, and its TLS/variable-difficulty edge, accounting and settlement.

The wallet's supported networks, crash-idempotent batch signing and exact-byte
broadcast boundary are described in the [wallet guide](wcash-wallet-payout.md).
It requires one exclusive writer per database and is not the operated pool
ledger. After an ambiguous post-sign outcome, recover the exact batch and
rebroadcast the same bytes rather than create a replacement. Do not promote
experimental components based only on these local results.
