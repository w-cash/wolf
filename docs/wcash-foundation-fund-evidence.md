# Foundation fund local review evidence

This report records local validation collected before the maintainer authorized
a draft PR targeting `main`. **It is not release-ready:** the required full workspace
test run remains unresolved; production custody and independent human consensus
review are also outstanding.

The [15 October review response](wcash-foundation-fund-review-response.md)
records the backup persistence fix, fresh 181-test fixture state pass, required
workspace Clippy pass and fresh executable recovery/legacy-append checks.
The fresh unfiltered workspace test run reports 1,169 passed, three failed and
two ignored before stopping at the listener target. Those three failures also
occur at the pinned baseline. The response records further paired comparisons
and exact provenance. The original full-run evidence below remains tied to its
original source and binaries; it is not relabeled as execution of the fix.

The follow-up review of `78cfa85ac` independently repeated both recovery
scenarios and closed F1 for Unix. CI now runs the 12 existing Python harness
regressions, and the operator guide explicitly documents the Unix-only covered
backup path. These follow-ups change CI and documentation only. Required Linux
checks and independent human consensus review remain outstanding. The ignored
`target/` records and absolute local paths below are retained machine-local
evidence; they are not portable public attachments.

## Source and fixed policy

- Repository: `w-cash/wolf`; branch: `foundation-fund-20pct`.
- Baseline: `41a39cab3f259509653c1d4930df3dae3dabba98`, verified `origin/main`
  when cloned.
- Original full-run artifact source: `1e730487db1bd3723a858f2f50d3afddfe6bbca3`.
  The later backup persistence fix is `d9eaca23d8c0549fab9666da3595f176f7b828be`;
  the fresh fixture node is built from `2fd6b31b86675980c6b8aaedef72795c8c0c9151`.
  Fresh validation and its mixed-source overlay are recorded in the review
  response. Rust/Cargo inputs are unchanged across those two commits.
- Toolchain: pinned Rust 1.91.0 on aarch64-apple-darwin.
- Mainnet: 40001 inclusive to 621455 exclusive; subsidy-only integer division by
  five; transparent P2PKH `W1ZRudbQKz7jQdKmAarbaJsegscnZzXjpu2`.
- Miner receives the remainder plus all fees, transparently or privately in
  Ironwood. Gross issuance is unchanged. There is no automatic renewal.

An independent recurrence tests every funded height. It proves that 621455 is
exactly the first reward below half the initial 2,199,023,255 atoms: reward
621454 is 1,099,512,482; reward 621455 is 1,099,511,171. Per-block rounding
allocates exactly **184,467,517,026,194 atoms** over the term.

## Executed checks

The table and broad diagnostic runs below describe the original implementation
validation. The later persistence-fix checks and fresh required commands are
reported separately in the [review response](wcash-foundation-fund-review-response.md).
Their counts overlap and must not be added together as unique test coverage.

Exact commands, environments, exits, durations and named test summaries are in
ignored local `target/foundation-evidence/`: `checks.json`,
`workspace-retries.json`, `gate-remainder.json`, `wallet-final.json`,
`normal-e2e.json` and `summary.json`. Separate logs preserve diagnostics. Counts
below overlap across invocations; do not sum them as unique tests.

| Command scope | Passed | Failed | Ignored |
| --- | ---: | ---: | ---: |
| Wcash chain library | 325 | 0 | 0 |
| Wcash consensus library | 91 | 0 | 0 |
| Wcash RPC library | 173 | 0 | 1 |
| Wcash state library, corrected retry | 173 | 0 | 0 |
| Default Zcash chain library | 325 | 0 | 0 |
| Default Zcash consensus library | 153 | 0 | 0 |
| Default Zcash RPC library | 173 | 0 | 1 |
| Default Zcash state library | 173 | 0 | 0 |
| Fixture chain / consensus / RPC libraries | 326 / 91 / 173 | 0 | 0 / 0 / 1 |
| Fixture persistent state `foundation_` tests | 8 | 0 | 0 |
| Initial coordinator package | 280 | 0 | 2 |
| Fixture coordinator package | 282 | 0 | 2 |
| Final wallet package at `1e730487d` | 141 | 0 | 0 |

The final wallet run is 128 library, 11 binary and two transaction-domain tests.
Its new regression uses actual wallet rewind/reappearance, retains raw-known and
raw-NULL regular receipts, preserves signed bytes and spend intent, and checks
exact genesis/hash/session guards. The focused regression passes independently.

Workspace Clippy with `--all-targets --locked -- -D warnings` passes at
`34ff5915`; final wallet Clippy passes at `1e730487d` after its genesis follow-up.
Wcash chain, consensus, RPC, state, coordinator and wallet lint checks pass.
Exact profile checks from `.github/workflows/wcash-release-gate.yml` are recorded
separately from broad failing combinations. No zero-test filter is counted as a
pass. All **26 remaining profile checks pass**, covering 84 tests, the three
isolated release graphs and default-state Clippy. Exact startup filters,
transitive fixture guards, AuxPoW profiles, RPC serialization and network
identity checks are included; `gate-remainder-summary.json` records the scope.

Formatting, diff whitespace, Bash/Python syntax, ShellCheck on three scripts,
Actionlint on three workflows and Markdownlint on 15 changed documents pass.
The artifact verifier's eight tests and the harness's seven tests pass.
`static-final-pre-handoff.json` retains exact commands and outputs; final report
edits are checked again before the documentation commit.

## Unresolved broad checks

The required unfiltered `cargo test --workspace --locked` does **not** pass.
Its corrected unfiltered attempt stopped at three unchanged listener tests
binding `127.0.0.2`. This macOS host returns errno 49, "Can't assign requested
address"; the direct socket probe is in `network-loopback-platform.json`.

A subsequent workspace run explicitly skipped those three fully qualified
listener names and used `--no-fail-fast`. It also exited 101. Completed targets
report **1,768 passed / 78 failed**: five generic RPC deserialization tests and
73 node library failures involving an error-hook installation conflict and
poisoned initialization. The integration target separately reported 67 successful,
four failed and 43 ignored tests before a hung logger test was terminated.
Those partial integration counts are separate from completed-target totals.
Remaining documentation tests completed successfully.

The four integration failures concern end-of-support date/function/task tests
and generic configuration. The logger exceeded its own 90-second test limit;
its test process and owned child were stopped after about 4m41s. The captured
process tree and termination are in `workspace-hung-integration.json`; this
integration target is recorded as hung/terminated, never passed.

Additional broad node runs failed: Wcash 109 passed / 98 failed, default Zcash
132 passed / 73 failed. The Wcash run includes generic tests initializing Zcash
networks despite the existing compiled-profile guard, an obsolete amount
assertion, and the error-hook cascade. The duplicate amount assertions were
removed from the startup network-selection test; chain amount vectors retain
the bound checks. Exact startup/profile gate filters pass separately.

At the original handoff, the named failing generic tests/fixtures and their
supporting parsers were compared with the baseline and were unchanged, but no
independent baseline execution had occurred. The later actual baseline/revised
comparisons are recorded in the review response. These checks remain separate
from the required command: a filtered comparison does not replace a successful
full workspace run.

## Real solver and wallet evidence

**Fresh full run 6 passes, exit 0**, against the final immutable artifacts.
All node processes stopped cleanly. Its log is
`target/foundation-evidence/foundation-e2e-run-6-final.log`; an exact retained
copy of the original result is:

```text
target/foundation-evidence/runtime-final/full-run-6.json
```

Owner-private snapshots of all three passed result JSON files are retained in
`target/foundation-evidence/runtime-final/`. A consolidated sanitized proof is
`foundation-runtime-final-summary.json`. Both copied-state supplements bind to
the full result SHA256:

```text
0ec61f9d30c18cdd7120849fe321437168f52d2182ce8414bd88e8d0f6c126ab
```

The actual run retained baseline blocks 1–8 and the same database for upgrade
before activation, checked unfunded 9, funded 10–19 with alternating miner
modes, and unfunded 20 onward. Block 12 paid its 10,000-atom fee entirely to the
miner: **500,010,000 miner / 125,000,000 fund**. Restart21 and fresh peer sync21
matched historical hashes, chain supply, value pools and treasury UTXOs.

All nine incremental treasury wallet checks passed: rollback/reapply across
9, 10, 19 and 20, plus a replacement branch at 19–21. Rolling below activation
removed all fund receipts; rollback to18 retained 1,125,000,000 atoms; reapply
restored 1,250,000,000. Replacement19 changed the treasury outpoint without
changing total issuance or duplicating receipts. Candidate policy was recomputed
after each branch change and restart.

| Branch / height | Solved block hash | Fund atoms | Miner atoms including fees |
| --- | --- | ---: | ---: |
| initial / 9 | `e5340c5ff1a4d635301ff85d4d411fc5580dae18dd9d56f5e5f1dd48c71f351f` | 0 | 625,000,000 |
| initial / 10 | `3773ac765bdd88ba6cafc80d2b0d2cfa7cb1d808e220d574f5ffc8567012880b` | 125,000,000 | 500,000,000 |
| initial / 11 | `ad942791d632a42d28049e7914a81bd3a850c1a642cc2840f076109985e21d7a` | 125,000,000 | 500,000,000 |
| initial / 12 | `d2b1f006ee3df9bbabc4cdb30fee5eba3472ffab720d3813c6dd3b3057245048` | 125,000,000 | 500,010,000 |
| initial / 19 | `6b26ec7132c6a47ab1cb9fd3f57607e50f8048fce9756315b6441bd87160dbfa` | 125,000,000 | 500,000,000 |
| initial / 20 | `1d80674c92dbaba8e47a3dd8c92fff903a790021aac2c6896ee13927c5ee4c56` | 0 | 625,000,000 |
| initial / 21 | `52b51766ce5e708d0e0ba797caa5e35b773158215a3f183b49d8bac34c0ec756` | 0 | 625,000,000 |
| replacement / 19 | `11fee04135c393484c9824487793b59393512ae956177f39decf2eae1a957cf5` | 125,000,000 | 500,000,000 |
| replacement / 21 | `bf9fecd36a08196865875196b3bfbd07def64350c1d47d30f098207c489670b5` | 0 | 625,000,000 |
| replacement / 110 | `cd2a26ba0f9fdb1a08f9f43e3e19d8c0a9bc8e5a3109346cfb675eb5fd679b41` | 0 | 625,015,000 |

The **same signed transaction bytes** spending the height-10 fund output were
rejected by the node at candidate109 with an explicit immature-coinbase error,
then accepted unchanged at candidate110 after restoring the exact predecessor.
Shielding transaction:

```text
589c292c2261c2ef5bfc5c09d29cc9e3ecbc7fb7d4921cb522b64e61848b6578
```

Block110 paid its entire 15,000-atom fee to the miner (625,015,000 atoms).
A fresh wallet restored from the disposable seed recovered **1,125,000,000
transparent atoms plus 124,985,000 Ironwood atoms**; legacy pools and regular
transparent balance stayed zero. Maturity was never shortened. Final total
chain issuance is 68,750,000,000 atoms, equal to 110 gross subsidies; transparent
and Ironwood pools sum to that total.

Both actual old-binary restart supplements pass, exit0, using copies of this
passed covered state and the exact same artifact manifest. The original remains
unchanged and all supplement nodes stopped.

| Actual baseline append | Old solved hash | New-rule consequence |
| --- | --- | --- |
| Covered10 → unfunded active11 | `b91fa7d8c9898e992f26d3433498366cbbeeca687e9b5dd4842628db3c43edc7` | Required125,000,000, actual0; upgraded node refuses uncovered history |
| Covered110 → post-expiry111 | `38468c1391bae67946b7e3a1142c95f21e15405ac4d3c8d58aabe57ec87b3a8d` | No fund required; upgraded node still refuses uncovered history |

Logs: `foundation-covered-active11-final.log` and
`foundation-covered-expired111-final.log`. The active case first opens a copied
110 state with the new node, invalidates11 to retain covered10, then launches
the baseline executable to mine11. Both new-node refusals explicitly instruct
preserving the old directory and rebuilding a **NEW, empty state cache_dir**.
This is actual executable evidence, separate from simulated database tests.

The fixture funds 10–19, expires at 20 and leaves 100-block maturity unchanged.
Gross subsidy is 625,000,000 atoms; fund is 125,000,000; miner is 500,000,000 plus
all fees. Ten allocations total 1,250,000,000 atoms. The disposable recipient is
`WRHUq9CTLZFa52NmAyHH5usN21Q8jZVskN1`, recovered from public seed `00..1f`.
No production private keys are used.

The actual proposal compatibility matrix in the final fresh run is:

| Active-height coinbase | Baseline validator | New validator |
| --- | --- | --- |
| Old transparent, no fund | Accept | Reject: mandatory fund output missing |
| Fund plus transparent miner | Accept | Accept |
| Fund plus private Ironwood miner | Reject: mixed coinbase outputs | Accept |

The old coordinator also rejects the new net-miner template value. Proposal
checks deliberately reach semantic payout validation; invalid proof or Merkle
roots are not used as evidence of funding rejection.

Ordinary `scripts/wcash-merged-mining-e2e.sh` and
`scripts/wcash-testnet-e2e.sh` both pass, including private-coinbase spend,
transparent maturity and shielding. Their exact hashes are in
`normal-artifact-hashes.json`. They ran **before** the wallet repair and do not
represent execution of the final wallet bytes. Final wallet tests and the
separate foundation exercise cover that repair.

## Defects and failed attempts resolved during validation

Cross-agent review fixed saved-winner replay bypassing current funding/mode
checks and fixture guards overlooking dependency-only feature selection.
Recovered work is re-attested without altering solved or signed bytes. State
utilities, coordinator recovery and packaging now inspect the shared fixture
flag. Actual late-upgrade testing also exposed hidden startup recovery text;
Display-based actionable guidance is now visible.

An initial state build failed to locate installed Xcode libclang. After fixing
the local loader setup, a preexisting synthetic overflow test used an
individually unserializable amount. Its fixture was corrected without changing
production bounds; all 173 Wcash state tests then passed. Test-only lint and RPC
serialization ownership errors were also corrected.

Earlier harness attempts had overly specific error text, a hash-array versus
string comparison, and an invalidated-block restoration assumption. Their
failures are retained separately from later successful continuations.

Run 5's strict wallet check exposed a real bug: rollback to tip 8 left
1,250,000,000 atoms as regular transparent balance with no canonical UTXOs.
The repair removes only raw-proven orphaned coinbase receipts after exact-tip
recovery, preserving regular receipts, signed bytes and spending intent. A saved
wallet probe changed that phantom balance to zero and preserved all ten stored
transaction byte sequences. Independent review added the genesis rewind case.

Separate signed-maturity continuations proved the same signed height-10 fund
spend rejected at candidate 109 and accepted at 110, with a 15,000-atom fee and
wallet recovery. These diagnostics are retained separately; the final fresh run independently passes the same checks.

## Artifact provenance and reproduction

The final manifest is `target/foundation-artifacts/manifest.json`; build log is
`target/foundation-evidence/artifact-build-final-genesis-fixed.log`. The tracked
diff digest is the empty SHA256. The source digest independently matches the
clean tracked tree:

```text
a79364b8e17e44b50410e36e208a028617cf5b3f90cb7fa52a9a02ad9087620d
```

| Artifact | SHA256 |
| --- | --- |
| `baseline/wcash-merge-miner` | `79c063c751e5fdc8029f489f31255888d5528cf7ae36fe2ad044ed6939462cdd` |
| `baseline/wcash-wallet` | `d0bc75cde0a78438aeb2e2387244f3e7d229d8819f48c16c55a39576d933202a` |
| `baseline/wcash-zebrad` | `787b644afe88c1a34fd82480a434347734e7a5be73b16b3ec478022c89ed851c` |
| `fixture/wcash-merge-miner` | `ef74e76ad727f5ba92c8a57a2cf0afee86834ff80bd2286cf717562be7828726` |
| `fixture/wcash-wallet` | `530b93d0f648ead42104ac11bf96bdf357c04877cac1113db93c64ddc478bf1c` |
| `fixture/wcash-zebrad` | `288de7349a8ffa525cf0c728ca5ffe23304350bd51bbb55d96864cf3b4054ee9` |
| `parent/zcash-zebrad` | `0c57ca3bb69c7714d25ab900ec2d7c7b626375fa8e92c907754426ed4a7f3e33` |
| `production/wcash-merge-miner` | `43b4f71c7a2bfa2060b1c98eaddb8899753b83c7d8f0680c75d42552bd72adcf` |
| `production/wcash-wallet` | `530b93d0f648ead42104ac11bf96bdf357c04877cac1113db93c64ddc478bf1c` |
| `production/wcash-zebrad` | `dc4346432dfd6e841ae6484acd483db744f9b67758b2adedbc3f401224c2e641` |

Baseline and production profiles use `zebrad/wcash-consensus`; fixture node and
coordinator explicitly use `wcash-funding-test-fixture`; Zcash parent is compiled
with `--no-default-features`. The fixture uses the ordinary production wallet.
The normal artifact helper also passed compiled-policy inspection of shipping
names. No fixture was copied into the shipping artifact directory.

Local builds use four Cargo jobs, incremental compilation off, dev/test debug
info zero, release debug info zero, LTO disabled and 16 codegen units. These
compact evidence builds do not replace ordinary release packaging.

```sh
scripts/build-wcash-foundation-fixtures.sh
scripts/wcash-foundation-fund-e2e.sh --artifacts target/foundation-artifacts
```

The [operator guide](wcash-foundation-fund-upgrade.md) includes both covered-state
append supplement commands and safe database recovery. Each run records actual
commands, hashes, amounts and assertions. Keep wallet databases, IVKs and signed
operation logs private.

## Scope and remaining release inputs

Persistent database tests cover finalized rule/height/hash coverage, backup
byte binding, old/new/old append behavior, and refusal without deleting state.
Their synthetic fixtures are not proof of full semantic or AuxPoW validation.
The real solver exercise covers those paths separately. A long-chain real-solver
finalization run was not performed. Direct transparent spending after maturity
and expiry is prohibited in contextual validation tests.

Zcash Regtest parents check structure, targets, proposals and chain acceptance
with their native PoW bypass; they are not independent Equihash verifiers.
Wcash and coordinator acceptance use the actual local solving paths.

Address encoding/script tests establish neither legal controller nor production
key possession, custody, recovery or spendability. Those facts, independent
human monetary/consensus review, successful required checks, a safe activation
window and recorded operator adoption remain release requirements. Codex and
cross-agent review do not satisfy independent human review.
