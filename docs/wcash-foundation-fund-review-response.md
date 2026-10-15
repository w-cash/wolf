# Foundation fund review response

This is the local response to the 15 October 2026 review of
`de28bfa5d40e229568ec4db3db76b63ef37d8e37`. The maintainer requested the fixes
and a team review. The validation below was completed locally before the
maintainer authorized a draft PR. The original reviewer documents are preserved
unchanged as local files. Paths under ignored `target/` are machine-local
evidence, not portable public attachments.

F1's requested acceptance tests now pass. Production promotion remains blocked
by the required workspace test gate and the release prerequisites below.

The follow-up review of `78cfa85ac` independently repeated both recovery
scenarios and closed F1 for Unix. The two requested nonblocking follow-ups are
included: the release workflow runs the 12 Python harness regressions and the
operator guide explains the Unix-only covered-backup restriction. No Rust or
Cargo inputs changed after the reviewed revision. Required Linux checks and
independent human consensus review remain prerequisites for merging.

## F1: backup publication and restart recovery

The original fixture binary independently reproduced the reported restart
failure on a disposable copy of the passed 110-block state. Its SHA256 matches
the original artifact manifest. The original state and reviewer evidence were
not modified. The independent result is retained locally at
`target/foundation-review-20261015/implementation-before-fix-repro.json`.

The Rust fix and initial recovery driver are in local commit
`d9eaca23d8c0549fab9666da3595f176f7b828be`. The subsequent driver corrections
described below are committed in `1a53e251e`.

The revised writer publishes validation coverage before exposing the matching
block. Temporary files, file synchronization, atomic rename and directory
synchronization make interrupted writes retain a verified prefix. Cleanup
removes the block durably before its coverage. Failed updates are retried even
when the tip is unchanged. Startup still refuses missing, mismatched or obsolete
coverage; an existing filename is never proof of validation.

Pending cleanup paths survive subsequent directory listings even after the
block filename disappears, and are canceled if the block becomes canonical
again. A process-local cache avoids repeatedly serializing every unchanged
backup. Only records already checked against validated memory enter it; file
replacement or missing metadata triggers the full check again. No cache entry
is trusted across process restarts.

Eight new Rust regressions cover 15 publication boundaries, five cleanup
boundaries, partial temporary records, trusted-memory repair, directory
failure, metadata-cache invalidation, pending removals and idle-tip retry.
Fresh Rust 1.91.0 release-profile validation passes **16 focused foundation
tests** and the **full 181-test fixture state library**. These are actual builds
of the fix, rather than reruns of previously compiled artifacts.

The single-file fallback preserves ordinary noncovered backups on non-Unix
targets. Covered publication/removal explicitly refuses unsupported directory
durability before modifying records. Its policy guard has a logic regression
on this Unix host; native Windows execution is not certified. Unix crash-order
tests inject failures at publication boundaries; they are not physical
power-cut tests.

## Executed validation

Fresh Cargo checks use pinned Rust 1.91.0, the committed lockfile, two Cargo
jobs and incremental compilation off. Release evidence builds use debug info
zero, LTO disabled and 16 codegen units. Required workspace checks use a
separate dev/test target with debug info zero. These compact local builds do
not replace ordinary release packaging.

| Fresh check of the fixed Rust inputs | Result |
| --- | --- |
| Fixture state `foundation_`, serial | 16 passed, 0 failed |
| Full fixture state library, serial | 181 passed, 0 failed |
| Default-profile backup write/restore vector | 1 passed, 0 failed |
| Fixture state Clippy, library and tests, `-D warnings` | Passed |
| Pinned-toolchain formatting | Passed |
| Artifact-verifier Python unit tests | 8 passed |
| Recovery/harness Python unit tests | 12 passed |
| Required unfiltered workspace tests | 1,169 passed, 3 failed, 2 ignored; exit 101 |
| Required workspace Clippy, all targets, `-D warnings` | Passed; exit 0 |
| Wcash production-policy dev node build and artifact inspection | Passed |
| Production-profile state `foundation_`, serial | 6 bodies pass; 10 fixture guards return early |

The original reviewer feedback retains its exact SHA256 and is not edited or
included in these implementation commits. Final Markdownlint, local-link,
Python syntax and diff whitespace checks pass. The executed recovery driver
matches the committed bytes by SHA256.

Counts overlap across invocations and must not be summed as unique Rust
tests. Exact commands, compiler versions, environments, durations and logs
are retained under ignored `target/foundation-review-followup/`: fixture and
paired runs in `paired-baseline.json`, required workspace checks in
`required-workspace-20261015T081826Z/`, and the production supplement in
`production-profile-20261015T083959Z/`.
The fresh fixture state test executable SHA256 is:

```text
951dd4158ffaf099175fec352efbf50c31cbb3c2060097dddf8b36c542f8bc0c
```

## Fresh executable recovery and legacy rejection

After the maintainer freed disk space, the replacement fixture node built
successfully from `2fd6b31b86675980c6b8aaedef72795c8c0c9151`. Its compiled
policy reports the isolated Regtest 10–19 fixture. The artifact verifier accepts
it with the unchanged fixture coordinator. The fresh node SHA256 is:

```text
99c51f84761690a19fe153119818435cf5bc6428c131660c41b11533fd46d2f2
```

The mixed-source manifest in `target/foundation-review-followup/artifacts/`
replaces only the fixture node and records the source of every executable.
All ten original artifact hashes and the original 110-block state remain
unchanged. The earlier disk-floor attempt and all setup diagnostics are
retained separately; they are not counted as successful checks.

The [recovery driver](../scripts/tests/wcash_foundation_backup_recovery.py)
now passes both real permission-failure scenarios on disposable state copies:

- Hold the coverage directory at mode 0500 through accepted block 110 and normal
  shutdown. Neither block 110 nor its marker is exposed. After permissions are
  restored, restart recovers verified 109 without rebuilding. Replay the exact
  original 110, restart, and compare every source-recorded block hash, tip,
  gross supply, value pools and treasury UTXO with the original 110 snapshot.
- Restore permissions while the same node process remains at 110. The periodic
  retry durably repairs the pair in 5.09 seconds without another tip change.
  Restart at 110 again matches the complete source snapshot.

The harness now queries the full source-recorded height set, including
108/109/110. Its live-retry setup restarts at 109 to clear the invalidated-block
submission cache, then retains the same process throughout denial and repair.
A unit regression checks snapshot height coverage. No new block-110 proof of
work is solved or substituted.

Actual baseline binaries also freshly append unfunded active 11 and post-expiry
111 to copied covered state. The fixed node refuses both histories with the
actionable instruction to preserve the old directory and use a **NEW, empty
state cache_dir**. No missing or obsolete coverage is silently trusted.

All three runtime gates pass; all started nodes are stopped. Consolidated
provenance, exact commands, JSON hashes and preserved result copies are in
`target/foundation-review-followup/runtime-final/summary.json`. The recovery
result SHA256 is:

```text
00b5b5a2b28a1f821d1f1d30f432735a72166bad08e90565cc9a308753fa9458
```

Both fresh append guards bind to that result, which binds to the original
full 110-block evidence. Root independently verified these bindings and shutdown
records in `root-runtime-verification.json`. This is fresh acceptance evidence
for F1; the original full solving exercise remains historical evidence of its
original binaries.

## Fresh production-profile inspection

A separate dev-profile build with `--no-default-features --features
zebrad/wcash-consensus` passes. Its `funding-policy` reports WcashMainnet,
`fixture=false`, start 40001, exclusive expiry 621455, divisor 5, the supplied W1
recipient and the exact pinned script. Artifact verification passes against
the unchanged original production coordinator. The copied node is only at
`target/foundation-review-followup/production/wcash-zebrad`; its SHA256 is:

```text
53e263172b49764ea4342cff437c5399bcfbea5286c002456daa440ae764c9b9
```

The production state filter reports 16 passes, but 10 fixture-only tests return
early: the eight new F1 tests and two existing fixture tests. Six tests execute
their Mainnet policy/state bodies. The actual new fault cases execute in the
fresh fixture runs above. Exact commands and scopes are retained in
`production-profile-20261015T083959Z/production-proof.json`. Rust/Cargo inputs
are unchanged throughout. This dev artifact does not certify ordinary release
packaging or a production network connection.

## Workspace checks and production prerequisites

The fresh required `cargo test --workspace --locked` still exits 101: three
listener tests cannot bind `127.0.0.2` on this Mac and report errno 49,
`AddrNotAvailable`. It executes 1,169 passing tests and two ignored tests before
Cargo stops at those failures; later targets are not executed by this command.
No names are skipped, and no assertions or host networking are changed to make
the command green. Exact `cargo clippy --workspace --all-targets --locked --
-D warnings` passes separately. Logs, commands and source-input attestations
are in `required-workspace-20261015T081826Z/` under the follow-up directory.
All 858 recorded Rust/Cargo/toolchain inputs match HEAD `2fd6b31b8`. The
broader tracked tree changed during the run because of recovery-driver edits;
the evidence explicitly records this distinction.

Actual paired diagnostics now compare baseline `41a39cab3` with the fixed
Rust inputs at `2fd6b31b8`. Each side builds the same workspace release feature
graph with pinned compilers and matching environments. RPC serialization runs
serially; the listener, full node library and selected integration groups use
default test threading. Fresh executable hashes, child-node hashes, commands,
failure names and logs are retained in `paired-baseline.json`, with the compact
comparison in `paired-comparison-summary.json`.

| Diagnostic scope | Baseline passed / failed / ignored | Fixed passed / failed / ignored |
| --- | --- | --- |
| Full RPC serialization target | 27 / 5 / 0 | 27 / 5 / 0 |
| Listener filter | 16 / 3 / 0 | 16 / 3 / 0 |
| Full node library, default threads | 141 / 73 / 0 | 144 / 73 / 0 |
| End-of-support group, first baseline attempt | 5 / 1 / 1 | 3 / 3 / 1 |
| End-of-support group, second baseline attempt | 3 / 3 / 1 | Same fixed run above |
| Exact configuration test | 0 / 1 / 0 | 0 / 1 / 0 |
| Exact logger test | 1 / 0 / 0 | 1 / 0 / 0 |

The five RPC names and untagged-enum errors match exactly. The three listener
names and bind errors match exactly. All 73 failed node names match, with the
same already-installed `ErrorHook` and poisoned `Once` cascade. The three
additional fixed-branch node tests pass: shared fixture public-network guard,
funding-policy reporting and state-inspection fixture guard. Configuration
fails the same stored-config compatibility assertion on both revisions.

The EOS group races over global tracing initialization. Its first baseline
attempt fails `end_of_support_is_checked_at_start`; a single additional
default-thread baseline attempt fails the exact original
`end_of_support_date`, `end_of_support_function` and `end_of_support_task`
names, matching the fixed run's already-set global subscriber and poisoned
initialization errors. Both baseline attempts are retained. The original
logger hang is **not reproduced** by either isolated logger run and remains an
unresolved broad-run limitation.

These results establish baseline reproduction of the named failures above;
they do not make the required workspace command pass. No assertions are
removed to obtain this comparison. Early fingerprint stalls and
wrong-child-compiler attempts executed zero tests and remain separate
environment diagnostics. Integration runs check their child executable hash
and prevent concurrent Cargo replacement. Automatically generated property
seeds are preserved with hashes in ignored follow-up evidence and excluded
from implementation commits.

The fund rate, recipient, activation, expiry, miner fees and payout options are
unchanged. Production controller/custody evidence, independent human consensus
review, successful required release checks, a safe activation window and
recorded operator adoption remain required. Cross-agent review does not
substitute for independent human approval.
