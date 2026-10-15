# Foundation allocation: operator upgrade and local review

This is a consensus upgrade prepared at the maintainer's direct request. It
changes the original zero-allocation policy. The maintainer has authorized
publishing `foundation-fund-20pct` as a draft PR targeting `main`. Required
Linux checks and independent human consensus review must complete before
merging; production custody, release and rollout prerequisites remain below.

## Fixed production policy

Mainnet blocks **40001 through 621454 inclusive** must contain a separate
transparent output of `floor(gross subsidy / 5)` to:

```text
W1ZRudbQKz7jQdKmAarbaJsegscnZzXjpu2
76a9148708e78a0203f93b2691057281e1773fd263702088ac
```

The address is P2PKH. It is not an on-chain multisig address. The actual legal
controller, custody design, independent receiving/recovery/spending evidence,
and human consensus review remain required release inputs. Do not describe the
recipient as an incorporated foundation or infer ownership from a domain.
Changing this destination, including after key loss or compromise, requires a
separate reviewed consensus change. There is no operator redirection switch.

Miners receive the remaining subsidy plus **all fees**, with transparent or
private Ironwood payout. The fund output stays transparent in both modes. Gross
issuance and the smooth emission curve counter are unchanged. The fund stops at
**621455**, the first subsidy below half the initial 21.99023255 WEC reward.
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

The state format adds foundation-rule coverage. Finalized coverage is written
atomically with its exact tip height and hash. Nonfinalized backups have
coverage sidecars bound to rule version, height, block hash and stored bytes.
The loader checks coverage before trusting reconstructed semantic results.
Coverage is required for history at or above activation even after expiry.
Missing or stale coverage is not proof of valid funding. A subsequent old binary
cannot extend a covered database and have those blocks silently trusted on the
next upgrade.

On Unix, the revised backup writer makes coverage durable before atomically
exposing a block. A failed write retains the last verified prefix and is retried
while the node remains running, even if the tip does not change. After restoring
filesystem access, a restart can reopen that prefix and obtain missing blocks
from upgraded peers. It does not fabricate coverage for legacy records.

Covered backup publication and removal currently require Unix directory sync
support. On non-Unix targets, including native Windows, these operations return
`Unsupported`; retrying or restoring filesystem permissions cannot resolve that
platform limitation. Native Windows is not certified for this covered backup
path. Ordinary noncovered backups retain their single-file fallback, which does
not claim directory durability.

Unknown or uncovered affected history is refused with an actionable error.
The incompatible database is preserved; the node must not automatically delete
or bless it. Recovery is to stop the process, retain the original directory for
inspection, configure a **new, empty absolute `state.cache_dir`**, and sync from
independently verified upgraded peers. Keep `delete_old_database = false`.
Confirm the canonical chain with a human reviewer first. Copying coverage files
or editing the version file is not a recovery procedure.

Upgrade both node and coordinator before height 40001. Old software has
asymmetric compatibility: it can accept a correctly funded all-transparent
coinbase, but rejects the mixed transparent-fund/private-miner form. It can
also accept an unfunded block that the new rules reject. An old coordinator can
reject a new funded template even when an old validator accepts it. There is no
general safe old-binary rollback after activation. Preserve chain state and
accounting journals during incidents.

If Mainnet reaches 40001 before safe adoption, stop the production rollout and
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
Funded fixture blocks pay 1.25 TWC to the treasury and 5 TWC plus all fees to the
miner. Ten blocks allocate 12.5 TWC in total. Real Equihash/AuxPoW solving stays
local; hosted CI runs deterministic checks without solving. The Zcash Regtest
parents check structure, target, proposals and chain acceptance with their native
PoW bypass; they are not a second independent Equihash verifier.

The harness records actual commands, executable hashes, block hashes, decoded
payouts, compatibility outcomes, restarts, reorgs, sync and treasury recovery.
Consult its result and the [evidence report](wcash-foundation-fund-evidence.md)
for which checks actually ran and passed. State tests also exercise finalized
coverage directly, avoiding a checkpoint that would conceal semantic checks.
Transparent coinbase maturity remains 100 blocks; a block-10 output can be spent
in block 110, not block 109, and must first be shielded.

Inspect shipping artifacts explicitly before packaging:

```sh
python3 scripts/verify-wcash-funding-artifacts.py \
  target/release/wcash-zebrad target/release/wcash-merge-miner
```

This check executes each binary's `funding-policy` command and compares start,
expiry, divisor, recipient and script. It refuses a fixture or mismatched
artifact even if it has a shipping filename. The normal build helper runs the
same check. Never publish fixture artifacts. The compact local evidence builds
disable release LTO and debug info to bound resources; ordinary release builds
and their full gate remain separate required evidence.

## Production promotion checklist

1. Complete independent source, arithmetic, migration and private-output review;
   finish the address-controller and custody evidence without exposing secrets.
2. Complete required formatting, workspace lint/tests, both consensus profiles,
   feature-specific release checks and actual local solver/wallet scenarios.
   Leave failed or unexecuted checks explicitly unresolved.
3. Publish versioned node and coordinator artifacts with checksums and record
   minimum supported versions. Do not infer pool adoption from a merged PR.
4. Publish the disclosure on wcash.foundation and w.cash as a separate approved
   action, notify operators, and record deployment acknowledgments from pools,
   wallet backends, explorers and other validators before activation.
5. Independently observe the preactivation canonical tip, first funded outputs,
   both miner modes, chain continuity and competing tips. Keep the fixed policy
   unchanged while monitoring.

## Draft website disclosure

Wcash proposes changing its original zero-allocation policy. Blocks 40001–621454
will direct 20% of each subsidy, rounded down in atomic units, to the transparent
P2PKH address `W1ZRudbQKz7jQdKmAarbaJsegscnZzXjpu2`. The miner receives the
remaining subsidy plus all transaction fees and may choose transparent or
private Ironwood payout; foundation payments remain transparent. Gross emission
is unchanged. The allocation expires at 621455, and existing reserves remain
usable. Any renewal requires a separately reviewed and adopted consensus
upgrade. Legal controller and custody description: **required before publishing
or releasing**. This text is prepared for review and has not been published.
