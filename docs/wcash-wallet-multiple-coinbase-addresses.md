# Multiple Wcash coinbase receivers

`wcash-wallet` supports a fixed, recoverable set of transparent coinbase
receivers for one ZIP 32 account. The supported external child indices are
`0..=9`. Index zero remains the legacy default; indices one through nine allow
operators to separate mining routes without separating custody.

All managed receivers:

- derive from the same Wcash-domain-separated seed and account;
- are registered through the standard librustzcash wallet address API;
- contribute to the same transparent balance and transaction history;
- shield into the same deterministic Ironwood receiver; and
- are rediscovered by a seed-only restore within the standard transparent gap.

No command in this workflow changes consensus, block construction, pool share
validation, payout accounting, or a running deployment.

## Operator procedure

1. Stop if the wallet database is not backed by verified recovery material.
2. Confirm the exact `--network` and database path.
3. Run `coinbase-address list` and retain the JSON in the deployment record.
4. Run `coinbase-address ensure --index 1` for the second route.
5. Repeat `list`; confirm indices zero and one have the same `account_id`, are
   distinct addresses, and use the expected network prefix.
6. Ask the target node for one AuxPoW candidate for each address at the same
   tip. Confirm the candidate hashes differ and both extend the same previous
   block hash. Retire both unused candidates.
7. Change only the intended pool route's coinbase destination. Preserve the
   legacy index-zero route and all existing miner endpoints.
8. After the first payout, synchronize and verify the output under the expected
   child index. Wait for 100-block coinbase maturity before shielding.

`ensure` returns public metadata only:

```json
{
  "account_id": "00000000-0000-0000-0000-000000000000",
  "child_index": 1,
  "address": "W1...",
  "is_default": false
}
```

The actual account identifier and address are deterministic wallet values; the
abbreviated example above is not a usable payout destination.

Never copy recovery material into a pool, miner, command argument, environment
variable, log, issue, or deployment record.

## Recovery and rollback

A restored database must be initialized from the same seed and synchronized
from a birthday that covers the relevant chain history. Transparent recovery
queries every managed index from height one and completes only after all UTXO
and spend-history requests finish against one pinned tip. Unknown funded
receivers, inconsistent request ranges, cancellation, timeout, or a tip change
fail closed and leave recovery incomplete.

To roll back a route, stop publishing new jobs to the added receiver and point
that route back to index zero. Keep the old receiver in the operator inventory
and keep scanning it; deterministic receivers are not revoked, and late or
immature coinbase outputs can still arrive. Shield all mature outputs before
retiring operational references to the route.

## Release gate

Before a production rollout, require:

- wallet unit tests, strict Clippy, and formatting checks;
- a real-node Regtest run that funds indices zero and one, proves the exact
  100-block maturity boundary, shields both inputs together, and restores the
  combined result from the same seed;
- two same-tip `createauxblock` candidates with distinct hashes, followed by
  explicit retirement; and
- independent review of the source revision and test evidence.

Merge approval is not deployment approval. Roll out the new wallet binary
separately, without changing the existing node, pool, listeners, or miner URLs.

The dated validation evidence and exact operator commands for Molepool are in
the [Molepool operator wallet handoff](wcash-wallet-molepool-handoff.md).
