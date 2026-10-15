# Foundation allocation: operator upgrade guide

Upgrade Wcash validators and native mining coordinators together before
Mainnet block 33000. The allocation changes coinbase construction, miner reward
accounting and the validation coverage required for existing chain state. The
[technical specification](wcash-foundation-fund-specification.md) defines the
fixed policy and exact arithmetic.

## Fixed production policy

Mainnet blocks **33000 through 621454 inclusive** must contain a separate
transparent output of `floor(3 * gross subsidy / 10)` to:

```text
W1ZRudbQKz7jQdKmAarbaJsegscnZzXjpu2
76a9148708e78a0203f93b2691057281e1773fd263702088ac
```

The address is P2PKH. Production release requires identification of its legal
controller and verification of receiving, recovery and spending under the
intended custody arrangement. Changing this destination, including after key
loss or compromise, requires a separate consensus change. There is no operator
redirection switch.

Miners receive the remaining subsidy plus **all fees**, with transparent or
private Ironwood payout. The fund output stays transparent in both modes. Gross
issuance and the smooth emission curve counter are unchanged. The fund stops at
**621455**, the first subsidy below half the 21.99023255 WEC reward at the first
post-slow-start block, 40001. Funding begins during slow start at block 33000.
Expiry does not invalidate accumulated UTXOs or require spending reserves by
then. Renewal needs a new proposal, implementation, release and adoption; it is
never automatic. Calendar dates depend on block production.

`getblocksubsidy` reports gross `totalblocksubsidy`, reduced `miner`, and a direct
funding entry with Wcash address and exact atomic `valueZat`. Founders and
lockbox totals stay zero. `createauxblock.coinbasevalue` is the **net miner
payout including fees**, excluding the required fund output. Update pool
accounting and coinbase construction accordingly. The Zcash parent reward and
its fee distribution are unchanged.

The builder emits separate miner and foundation outputs even if their addresses
match. Consensus accounts for exactly one output having both the exact script
and amount. A split, merged or oversized fund payment cannot replace it.
Transparent miners may split their remainder or donate more within that
remainder. A private miner may have only the one required transparent output;
all miner actions still require valid private Ironwood proofs and encryption.

## Existing databases and late upgrades

An upgrade **before activation** retains the existing database and historical
blocks. Shut down cleanly, preserve the cache directory and journals, install
matching node and coordinator software, and reopen the same directory. Compare
tip hash and height before requesting new work. Recompute templates from the
candidate height after every restart and reorg.

The state format uses version 2 foundation-rule coverage, binding the numerator
`3` and denominator `10` as well as activation and expiry heights. Finalized
coverage is written atomically with its exact tip height and hash. Nonfinalized
backups have coverage sidecars that additionally bind the block's stored bytes.
The loader checks coverage before trusting reconstructed semantic results.
Coverage is required for history at or above activation even after expiry.
Missing or stale coverage is not proof of valid funding. A subsequent old binary
cannot extend a covered database and have those blocks silently trusted on the
next upgrade.

Coverage with an incompatible format, allocation ratio or activation height
cannot authorize history under the fixed rule. Affected history is refused even
after the allocation expires. Preserve that state and resynchronize into a new
cache directory as described below; do not copy markers or edit their ratio.
An upgrade before activation retains the original preactivation history.

On Unix, the backup writer makes coverage durable before atomically
exposing a block. A failed write retains the last verified prefix and is retried
while the node remains running, even if the tip does not change. After restoring
filesystem access, a restart can reopen that prefix and obtain missing blocks
from upgraded peers. It does not fabricate coverage for legacy records.

Covered backup publication and removal require Unix directory sync
support. On non-Unix targets, including native Windows, these operations return
`Unsupported`; retrying or restoring filesystem permissions cannot resolve that
platform limitation. Ordinary noncovered backups retain their single-file
fallback, which does not claim directory durability.

Unknown or uncovered affected history is refused with an actionable error.
The incompatible database is preserved; the node must not automatically delete
or bless it. Recovery is to stop the process, retain the original directory for
inspection, configure a **new, empty absolute `state.cache_dir`**, and sync from
independently verified upgraded peers. Keep `delete_old_database = false`.
Confirm the canonical chain with independent upgraded nodes first. Copying
coverage files or editing the version file is not a recovery procedure.

Upgrade both node and coordinator before height 33000. Software without
foundation enforcement has asymmetric compatibility: it can accept a correctly
funded all-transparent coinbase, but rejects the mixed transparent-fund/private-miner
form. It can also accept an unfunded block that the new rules reject. An old coordinator can
reject a new funded template even when an old validator accepts it. There is no
general safe old-binary rollback after activation. Preserve chain state and
accounting journals during incidents.

Upgrade every participant to the same fixed policy. Stored coverage must match
that policy; copying or relabeling incompatible markers cannot establish validity.

If Mainnet reaches 33000 before safe adoption, stop the production rollout and
report the missed activation. A new activation decision is required; do not
retroactively activate, rewrite history, or quietly change these constants.

## Reproduce the isolated local exercise

Use pinned Rust 1.91.0 and the committed lockfile. Start in a clean local checkout
with sufficient disk and RAM; all generated state is disposable and loopback
only. The build script uses four separate target directories and writes binary
hashes/features to `target/foundation-artifacts/manifest.json`:

```sh
scripts/build-wcash-foundation-fixtures.sh
scripts/wcash-foundation-fund-e2e.sh
```

After the full run stops cleanly, exercise an actual old-binary append using
the evidence path printed by that run:

```sh
scripts/wcash-foundation-fund-e2e.sh \
  --covered-restart-source /absolute/path/to/full-run/evidence.json
scripts/wcash-foundation-fund-e2e.sh \
  --covered-restart-source /absolute/path/to/full-run/evidence.json \
  --covered-restart-fork-height 10
```

This supplemental test copies the covered height-110 state, opens that copy
with the recorded baseline executable, mines block 111, and confirms the new
executable refuses the uncovered append. The second invocation first rolls its
copy back to covered height 10, then the baseline mines an unfunded block 11
during the active allocation. Both cases preserve the original state.

The baseline is `41a39cab3f259509653c1d4930df3dae3dabba98`. Production-policy
artifacts use `zebrad/wcash-consensus`. The fixture node and coordinator use
`wcash-funding-test-fixture`, with Regtest allocation at 10 through 19 and expiry
at 20. That feature never changes Mainnet constants, public identities,
difficulty, subsidy or maturity. Fixture executables identify themselves and
refuse Mainnet and public Testnet use. The wallet used by the fixture is the
ordinary production wallet. The baseline, shipping-policy and fixture node and
coordinator binaries are never substituted for one another.

The fixture treasury is `WRHUq9CTLZFa52NmAyHH5usN21Q8jZVskN1`, derived from the
public disposable 00-through-1f seed in the existing wallet vectors. It must
never hold real funds. Ordinary Regtest without the feature has no allocation.
Funded fixture blocks pay 1.875 TWC to the treasury and 4.375 TWC plus all fees to the
miner. Ten blocks allocate 18.75 TWC in total. Real Equihash/AuxPoW solving stays
local; hosted CI runs deterministic checks without solving. The Zcash Regtest
parents check structure, target, proposals and chain acceptance with their native
PoW bypass; they are not a second independent Equihash verifier.

The harness writes a result containing commands, executable hashes, block
hashes, decoded payouts, compatibility outcomes, restarts, reorgs, sync and
treasury recovery. Inspect that result for the checks completed by each run.
Transparent coinbase maturity remains 100 blocks; a block-10 output can be
spent in block 110, not block 109, and must first be shielded.

Additional local drivers cover [backup publication failures](../scripts/tests/wcash_foundation_backup_recovery.py)
and [partial same-address coinbase spends](../scripts/tests/wcash_foundation_partial_spend_e2e.py).
Run each with `--help` for its artifact and stopped-state input requirements.

Partial-spend `prepare` uses `baseline/wcash-wallet`, pinned to the baseline
revision and hash-checked against the artifact manifest before starting nodes.
Every preparation command, including the first shield, uses that old wallet.
For `verify`, supply the stopped preparation's `--source-evidence` and select
the fixed executable with `--new-wallet` and `--new-wallet-sha256`. The freshly
built `production/wcash-wallet` is suitable for verification, not preparation.
Keep the artifact manifest unchanged between phases.

Inspect shipping artifacts explicitly before packaging:

```sh
python3 scripts/verify-wcash-funding-artifacts.py \
  target/release/wcash-zebrad target/release/wcash-merge-miner
```

This check executes each binary's `funding-policy` command and compares start,
expiry, numerator `3`, denominator `10`, recipient and script. It refuses a
fixture or mismatched artifact even if it has a shipping filename. The normal build helper runs the
same check. Never publish fixture artifacts. The local fixture builds disable
release LTO and debug info to bound resources; use the ordinary release build
and full release gate for shipping artifacts.

## Production promotion checklist

1. Complete source, arithmetic, migration and private-output review; verify the
   address controller and custody arrangement without exposing secrets.
2. Complete required formatting, workspace lint/tests, both consensus profiles,
   feature-specific release checks and actual local solver/wallet scenarios.
   Leave failed or unexecuted checks explicitly unresolved.
3. Publish versioned node and coordinator artifacts with checksums and record
   minimum supported versions. Do not infer pool adoption from a merged PR.
4. Publish the allocation policy and upgrade instructions. Notify operators and
   record deployment acknowledgments from pools, wallet backends, explorers and
   other validators before activation.
5. Independently observe the preactivation canonical tip, first funded outputs,
   both miner modes, chain continuity and competing tips. Keep the fixed policy
   unchanged while monitoring.
