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

The Wcash child node exposes two Namecoin-shaped methods:

```text
createauxblock("<Wcash Unified Address>")
submitauxblock("<candidate hash in display order>", "<AuxPoW v2 hex>")
getauxblockstatus("<candidate hash in display order>", "<AuxPoW v2 hex>")
```

`createauxblock` accepts only a Wcash Unified Address with the receiver needed
for a private Ironwood coinbase. It proposal-validates the proof-free child,
rechecks the tip, and caches the exact block. The response includes `hash`, the
canonical proof-free block `data`, `chainid`, `previousblockhash`,
`coinbasevalue`, `target`, `bits`, and `height`. The coordinator independently
decodes `data` and verifies its hash, height, predecessor, wire version,
difficulty, and empty witness. Candidates are bounded to 16 entries, expire
after ten minutes, and become invalid immediately when the child tip changes.

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
transactions, creates the normal shielded parent coinbase, then performs a
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
  payout configuration. The coordinator independently recovers every shielded
  recipient from the exact `coinbasetxn` with Zcash's consensus-public zero
  outgoing viewing key and requires all of them to match the configured
  shielded address.
- Every proposal validator also supplies an ordinary, child-independent
  template on the same predecessor. Its shielded payout must match the same
  address, and its complete transparent-output vector must equal the candidate
  template. This prevents an extra transparent diversion while allowing the
  validator to remain unmodified.
- Wcash recipient verification has a different boundary: its coinbase is
  intentionally not publicly recoverable, so the coordinator cannot prove the
  recipient from a payment address alone. It validates the exact candidate,
  target, and private-only coinbase shape, then trusts the loopback child node's
  reviewed `createauxblock` implementation for recipient correctness. The child
  binary, configuration, RPC socket, and host are payout-critical.
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
  either submission. The journal does not compact automatically. On restart
  each chain replays independently; blocks survive
  ambiguous RPC failures and reorg observation until 100 confirmations. Stable
  share IDs let payout/accounting software deduplicate retries. Any append or
  durability error or in-lock panic poisons the live journal so no later write
  can turn inconsistent state or a torn tail into a valid-looking record. The
  parent-directory entry is synced before mining starts, and restart performs
  metadata-durable bounded tail recovery.

The included ZIP-301 listener validates Equihash and both targets locally,
requires authorization, assigns per-session nonce prefixes, rejects malformed,
low-difficulty, duplicate, and stale shares, bounds frames and sessions, and
binds a literal loopback IP only. It also bounds concurrent Equihash validation,
serializes journal durability, and applies a sliding submission-rate limit per
connection. The `native-serve` supervisor retires stale
generations and proposal-checks replacements with bounded retry backoff. It
disconnects miners during rotation; a public deployment should add a bounded
multi-generation grace window as well as TLS, per-account authentication, rate
limiting, variable difficulty, payout accounting, and monitoring in front of
the listener.

The built-in password is shared and worker names are self-asserted labels, so
the journal alone is not an authenticated payout ledger. Anyone holding that
password can impersonate another worker label. A public edge must bind each
connection to its own account identity before using shares for payouts.

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

The Wcash coinbase reward goes directly to Ironwood using ordinary private note
encryption, not the inherited publicly recoverable coinbase outgoing-viewing
key. This prevents protocol-wide public recovery of its recipient and note
contents, while deterministic subsidy, fees, and public value-pool balance
changes keep aggregate supply auditable. As with any shielded payment, the
recipient or miner can still disclose their own viewing data out of band.

This does not hide the independent Zcash parent payout: standard Zcash
coinbases are publicly recoverable by design. Both parent nodes must use the
same shielded address supplied through `ZCASH_PAYOUT_ADDRESS`; the coordinator
checks the actual serialized outputs before releasing work. Pool share logs or
payout systems can still create off-chain metadata.

Operators should pass `-` for the Wcash address argument and provide the
shielded receiver through `WCASH_PAYOUT_ADDRESS`. Preflight output then exposes
only that a receiver was configured and its environment-variable source. Raw
RPC-body logging is disabled, avoiding plaintext receiver disclosure in
ordinary process listings and logs.

The current payout policy deliberately fails closed if a future Zcash upgrade
adds shielded funding-stream outputs or a new shielded pool. Supporting such an
upgrade requires an explicit recipient-classification review; it cannot
silently weaken the parent reward check.

## Release boundary

The consensus and exact native dual-submit path are implemented and pass local
three-node regtest. A public testnet still requires a frozen public genesis
anchor, stable bootstrapping/network parameters, Wcash wallet key support, an
independently reviewed consensus specification, multi-node soak and reorg
tests, vendor ASIC interoperability, and an operated pool edge with payout
accounting. Do not call the pre-testnet branch production-ready or use it with
funds of real value.
