# Wcash merged mining with Zcash

Wcash uses one parent proof-of-work profile: Zcash Equihash `(200, 9)`. Other
algorithms and parent chains are deliberately outside this branch.

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
ordinary exact Zcash block proposal and can be unmodified Zebra nodes.

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
  coinbase is intentionally not publicly recoverable; the coordinator verifies
  its private-only shape and value, then trusts the loopback child node's
  reviewed `createauxblock` implementation for recipient correctness. The child
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
one-WEC V6 transfer is matched in the mempool and block template, rejected by
both standard Zcash Regtest nodes, mined in a fourth AuxPoW block, and rescanned
with the full 25-WEC supply remaining in Ironwood. That transfer uses the
explicit Regtest-only unsafe one-confirmation override; the public Testnet wallet
policy remains 100 confirmations.

A third phase mines 101 transparent Wcash coinbases through the real AuxPoW
path. It rejects shielding at tip 99, enforces the 100-block maturity boundary at
tip 100, recovers every current UTXO from a deliberately late wallet birthday,
persists and broadcasts the exact shielding transaction, matches it in the
mempool and block template, and mines it at height 101. Final wallet and chain
checks conserve exactly 631.25 WEC across transparent and Ironwood pools.
Rotating more than 16 accepted child candidates also covers release of confirmed
winners beyond the coordinator's bounded active-candidate cache. Transparent
maturity is not overridden.

No public Wcash Testnet or community pool is deployed. A community-facing pool
still needs project-operated seeds, an independently reviewed consensus
specification, multi-node soak and reorg tests, vendor ASIC interoperability,
and an operated TLS/variable-difficulty edge with durable accounting, payouts,
and reorg-safe shielded settlement. The experimental one-shot wallet in this
workspace can derive a transparent coinbase address and construct a bounded
mature-coinbase shielding transaction, but it requires one exclusive writer per
database and is not that operated payout system. After an ambiguous post-sign
outcome, operators must recover the paginated persisted record and rebroadcast
the exact signed bytes rather than create a replacement. Wcash mainnet remains
disabled.
Do not call this engineering profile production-ready or use it with funds of
real value.
