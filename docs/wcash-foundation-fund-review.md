# Foundation allocation implementation review

The implementation branch is `foundation-fund-20pct`, based on
`41a39cab3f259509653c1d4930df3dae3dabba98`. The maintainer authorized publishing
only this branch as a draft PR targeting `main`. Merge, release and activation
require the remaining checks and reviews below.
From this checkout, inspect the complete change with:

```sh
git log --oneline 41a39cab3f259509653c1d4930df3dae3dabba98..HEAD
git diff 41a39cab3f259509653c1d4930df3dae3dabba98..HEAD
```

Suggested change title:
`feat(consensus)!: allocate 20% of Wcash block subsidies to the foundation fund at heights 40001–621454`.

## Motivation

The maintainer directly requested a fixed, expiring foundation allocation while
preserving gross emission, all miner fees and the choice of transparent or
private Ironwood miner payout. This supersedes the previous no-allocation
policy. No issue or external approval is invented.

## Solution

The shared `zebra-chain` policy pins Mainnet heights 40001–621454 and the supplied
P2PKH address. Validation accounts for one exact transparent fund output before
checking miner mode and total value. Builders, RPCs, coordinator preflight and
saved-winner recovery use the same policy. Database coverage prevents an
uncovered legacy postactivation chain or backup from being silently trusted.
Backup publication makes coverage durable before exposing the block, retries
incomplete writes without a new tip, and preserves a verified restart prefix
after filesystem errors.
The isolated non-default Regtest fixture uses heights 10–19 and refuses public
network use. Packaging inspects compiled artifact policy before publication.

Wallet synchronization reconciles receipts from orphaned coinbases, including
rewinds to genesis. It preserves regular pending receipts, signed transaction
bytes and spending intent when a coinbase returns to the canonical chain.

## Tests

Read the [evidence report](wcash-foundation-fund-evidence.md) for exact executed
checks, status and unresolved limitations. The [operator guide](wcash-foundation-fund-upgrade.md)
contains reproduction, migration and rollback instructions. The committed
[implementation specification](wcash-foundation-fund-specification.md) is the
acceptance checklist, not evidence that every requirement passed.
The [review response](wcash-foundation-fund-review-response.md) records the
15 October persistence fix and fresh executable recovery evidence. The full
fixture state library passes 181 tests; the focused foundation filter passes
16. Both real permission-failure recovery cases and fresh active-term and
post-expiry old-binary append refusals pass against the rebuilt fixture node.
The required workspace Clippy check passes. The unfiltered workspace test run
reports 1,169 passed, three listener failures and two ignored, then stops;
the three failures are also reproduced on the pinned baseline. Actual paired
runs also reproduce the earlier five RPC, 73 node initialization, three EOS and
one configuration failures. The original logger hang is not reproduced in the
isolated comparisons and remains disclosed in the evidence report.

The follow-up review of `78cfa85ac` independently repeated both recovery
scenarios and closed F1 for the Unix implementation. Its two nonblocking
follow-ups are included: CI runs the 12 Python harness regressions, and the
operator guide documents the Unix-only covered-backup restriction. The
original reviewer documents and machine-local runtime evidence remain local;
the committed reports summarize their outcomes.

## Follow-up and release inputs

Legal controller, actual custody/spendability evidence, independent human
consensus review, complete release checks and operator adoption are required
before production promotion. Website text is prepared but has not been
published. Run the required full workspace checks and Wcash release gate on a
suitable Linux runner before merging. Keep the PR in draft while these checks
and independent human consensus review remain outstanding.

## AI disclosure

Codex was used for implementation, tests, local tooling, documentation and
cross-agent code review. The human contributor remains responsible for
understanding and independently reviewing the change. Commits carry no AI
co-author or generated-by footer.
