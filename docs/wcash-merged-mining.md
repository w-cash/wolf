# Wcash merged mining with Zcash

Wcash is currently designed for one parent proof-of-work algorithm only: Zcash
Equihash `(200, 9)`. SHA-256d, Scrypt, X11, RandomX, and other parent algorithms
are not part of this branch.

## Work flow

1. A Wcash node builds a child block header containing its own compact target
   and an empty AuxPoW placeholder.
2. The Wcash block ID is calculated without the AuxPoW witness, avoiding a
   circular commitment.
3. The parent template producer domain-separates that ID with the Wcash chain
   ID and includes the resulting auxiliary root in exactly one zero-value
   output while constructing the Zcash coinbase, before its shielded proofs and
   authorizations are finalized.
4. The producer rebuilds, proves, and authorizes the complete coinbase, then
   recomputes every affected parent commitment, including the mined txid,
   auth-data root, transaction Merkle root, and block-commitments hash, before
   distributing the resulting Equihash header job.
5. A share meeting the Wcash target is packaged as a canonical AuxPoW witness
   and submitted with the Wcash block.
6. If the same header also meets the live Zcash target and all parent rules, the
   complete parent block is submitted independently to the Zcash node.

The Wcash target and Zcash target are separate. A share can be valid for Wcash
without being a Zcash block. No proof may use the parent header's advertised
`nBits` as authority for the child target.

## Commitment and proof profile

The parent coinbase carrier is:

```text
OP_RETURN PUSH44 fabe6d6d reverse(aux-root) tree-size:u32-le nonce:u32-le
```

The auxiliary leaf is SHA-256d over the domain
`Wcash/ZcashAuxPoW/leaf/v1\0`, chain ID `0x57434153`, and the raw 32-byte Wcash
block ID. The canonical proof starts with `WCAZ`, format version 1, and contains
the parent coinbase, its parent Merkle branch and index, the auxiliary branch
and index, and the exact 1,487-byte Zcash header. The outer witness is bounded
at 256 KiB and each component has a tighter limit.

All proof hashes use raw serialized byte order. Displayed Zebra, zcashd, or
explorer hashes normally require a full 32-byte reversal before entering this
format. See the non-palindromic frozen vector in
`wcash-zcash-aux/test-vectors/auxpow-v1-non-palindromic.hex`.

Validation checks, in order:

- canonical bounded framing and version;
- the parent hash against the authenticated Wcash target;
- a canonical Zcash coinbase parsed by the pinned Zebra transaction codec;
- coinbase index zero and its SHA-256d path to the parent transaction root;
- exactly one correctly positioned, zero-value Wcash commitment output; and
- the complete parent Equihash `(200, 9)` solution.

The Wcash block ID is a domain-separated BLAKE2b-256 digest of the fixed header
fields and excludes the AuxPoW witness. Different witnesses for the same child
therefore have the same ID. Implementations must not permanently cache a failed
witness solely by Wcash block ID, because a later valid witness can share that
ID.

## What the current development harness does

The `wcash-merge-miner` crate builds a bounded synthetic one-transaction parent
job, runs the real Tromp Equihash solver, validates the complete proof, and can
serve a small loopback-only JSON-lines interface. It has no fake-proof mode.

Its synthetic parent coinbase has no live ZEC subsidy or payout and is not a
valid production Zcash block template. The interface is Stratum-like test
tooling, not universal Stratum compatibility and not a dual-chain pool.

## Production pool adapter still required

A production adapter must, at minimum:

- obtain and continuously refresh a live Zcash `getblocktemplate`;
- preserve every parent consensus field, transaction, subsidy, funding,
  lockbox, and pool payout requirement;
- use a modified parent template producer or a complete Zcash coinbase builder
  to include exactly one Wcash commitment before shielded authorization;
- rebuild, re-prove, and re-authorize the coinbase after commitment or
  extranonce changes, and recompute the mined txid, auth-data root, transaction
  Merkle path, and block-commitments hash as required by that template;
- construct the correct Wcash auxiliary branch and byte order;
- distribute real Equihash jobs with robust authentication, limits, vardiff,
  stale-job handling, and transport security;
- attach the verified witness to the matching Wcash block and call Wcash
  `submitblock` when its target is met; and
- assemble and call Zcash `submitblock` when the harder parent target is met.

Current Wcash consensus validates borrowed work and its cryptographic bindings;
it does not run a Zcash header chain or prove that the parent block was accepted
by the Zcash network. That is normal for some forms of blind merged mining, but
it also means a miner can construct work that secures Wcash without earning ZEC.
Operational claims of simultaneous Zcash mining require the live-template,
dual-submit path above.

A completed `coinbasetxn` from `getblocktemplate` cannot in general be edited by
simply appending the Wcash output. Modern shielded Zcash coinbases authenticate
the transaction contents, and changing them without rebuilding the proofs and
authorizations makes the parent transaction invalid.

The custom output carrier and `WCAZ` witness are not a drop-in implementation
of every Namecoin-style AuxPoW pool protocol. Interoperability vectors, an
independent consensus audit, load testing, and a live Zcash integration test
remain release blockers.
