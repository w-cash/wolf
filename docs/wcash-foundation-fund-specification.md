# Wcash foundation fund implementation specification

This is the recorded maintainer implementation brief. Current implementation and
validation status are in the [evidence report](wcash-foundation-fund-evidence.md);
operator migration is in the [upgrade guide](wcash-foundation-fund-upgrade.md).

Wcash development funding implementation handoff, prepared 14 October 2026. Implement a mandatory transparent allocation of one fifth of the block subsidy from Mainnet block 40001 inclusive until block 621455 exclusive. Preserve the miner's choice of transparent or private Ironwood payout. The fund expires automatically at the first subsidy at or below half the initial post-slow-start subsidy. Renewal requires a separately reviewed and adopted consensus upgrade.

This is the maintainer's requested design and implementation brief. No implementation, deployment, custody setup, or test run is represented as complete. The maintainer-supplied Mainnet payout address is recorded and its encoding verified below. The actual legal recipient/controller and custody evidence remain required release inputs.

# 1 Repository and implementation scope

Repository: [w-cash/wolf](https://github.com/w-cash/wolf). Target branch: `main`. Latest remote `main` verified on 14 October 2026: `41a39cab3f259509653c1d4930df3dae3dabba98`.

Start a feature branch named `foundation-fund-20pct` from freshly fetched `origin/main`. Preserve existing work; use an isolated worktree if needed. Record the actual starting SHA. If Mainnet emission, payout, or validation code has changed since the reviewed SHA, reconcile this specification against the new code and report discrepancies before changing the monetary constants.

Read `AGENTS.md`, `CONTRIBUTING.md`, `README.md`, `docs/README.md`, `docs/wcash-direction.md`, `docs/wcash-consensus.md`, affected component guides, and the PR template. Their existing “no protocol tax” policy is explicitly superseded by this maintainer request for this change. Update those statements narrowly; do not treat this request as permission to change other monetary or privacy rules.

Deliver one reviewable MR/PR targeting `main`, including implementation, tests, operator migration instructions, revised monetary-policy documentation, and release evidence. Title: `feat(consensus)!: allocate 20% of Wcash block subsidies to the foundation fund at heights 40001–621454`. Follow the repository's breaking-change, changelog-fragment, and AI-disclosure requirements. Do not merge or deploy as part of preparing the MR.

The production node is `zebrad` built with `wcash-consensus`; the build helper publishes it as `wcash-zebrad`. A default Zcash build is a separate product profile. Keep Zcash consensus behavior unchanged.

# 2 Exact Mainnet policy

| Parameter | Required value |
| --- | --- |
| First funded block | 40001 inclusive |
| Last funded block | 621454 inclusive |
| First unfunded block after expiry | 621455 |
| Allocation | 1/5 of subsidy, rounded down separately for each block |
| Transaction fees | 100% to the miner |
| Recipient | `W1ZRudbQKz7jQdKmAarbaJsegscnZzXjpu2` — fixed Mainnet P2PKH |
| Informational website | [wcash.foundation](https://wcash.foundation) |
| Miner payout | Transparent or private Ironwood |
| Treasury payout | Transparent in both miner modes |
| Renewal | No automatic renewal or operator toggle |
| Public Testnet and ordinary Regtest | Unchanged by default |

For candidate block height `h`, existing gross subsidy `S(h)`, and independently validated aggregate fees `fees`:

```text
active(h) = 40001 <= h < 621455

fund(h) = S(h) / 5       if active(h), integer division
          0             otherwise

miner_subsidy(h) = S(h) - fund(h)
miner_payout(h)  = miner_subsidy(h) + fees

total_coinbase_value = S(h) + fees
```

Use integer atomic units throughout: 100000000 atoms = 1 WEC. The remainder from division belongs to miners. Do not calculate 20% of subsidy plus fees. Do not create additional issuance. Continue advancing the emission curve counter by the entire gross subsidy, including the fund allocation. Fees never enter that counter.

Keep the existing 40000-block slow start, smooth decay, permanent tail, target spacing, difficulty, AuxPoW, active value pools, and transaction amount limits. Do not change `halving()` to implement the milestone: Mainnet uses smooth decay, not abrupt halvings.

## Exact expiry definition

The first post-slow-start subsidy, at height 40001, is 2199023255 atoms = 21.99023255 WEC. Its mathematical half is 1099511627.5 atoms.

The expiry is the first post-slow-start height satisfying:

```text
2 * S(h) <= 2199023255
```

Use a sufficiently wide integer for this comparison. The first matching height is **621455**. At 621454, subsidy is 1099512482 atoms, still above half; at 621455 it is 1099511171 atoms, below half.

Pin start and expiry as Mainnet consensus constants. A deterministic independent recurrence test must prove the expiry constant. Do not dynamically change the endpoint based on wall time, observed hash rate, token price, website content, or a later subsidy implementation edit.

This replaces the earlier proposed literal-11-WEC cutoff of 621083. There is no abrupt subsidy drop at 621455; the smooth reward crosses half its initial value and the allocation stops. The miner share consequently increases at expiry.

The term contains 581454 funded blocks. At 75 seconds per block, the target interval from activation to expiry is 504.734375 days, about 16.6 months. An exact calendar date cannot be guaranteed.

## Golden vectors

All values below are atomic units, with zero transaction fees. Add all fees to the miner column.

| Height | Gross subsidy | Fund | Miner subsidy |
| --- | --- | --- | --- |
| 39999 | 2198968279 | 0 | 2198968279 |
| 40000 | 2199023255 | 0 | 2199023255 |
| 40001 | 2199023255 | 439804651 | 1759218604 |
| 40002 | 2199020634 | 439804126 | 1759216508 |
| 621454 | 1099512482 | 219902496 | 879609986 |
| 621455 | 1099511171 | 0 | 1099511171 |
| 621456 | 1099509860 | 0 | 1099509860 |

Across 40001 through 621454 inclusive:

- Gross subsidy: 922337586294287 atoms = 9223375.86294287 WEC.
- Fund: 184467517026194 atoms = 1844675.17026194 WEC.
- Miner subsidy before fees: 737870069268093 atoms = 7378700.69268093 WEC.

Sum each block's rounded allocation. `sum(S / 5)` is not equivalent to `sum(S) / 5`.

# 3 Recipient and custody inputs

The project label is “Wcash Foundation,” associated with `wcash.foundation`. The domain is informational; consensus never fetches an address from DNS, HTTPS, a website, a registry, or a remote configuration.

Recipient details and remaining checks before a production-ready merge or release:

1. Address supplied by the maintainer: `W1ZRudbQKz7jQdKmAarbaJsegscnZzXjpu2`. Base58Check checksum, Mainnet P2PKH prefix, payload length, and canonical round trip verified.
2. Its decoded script type, payload, and exact serialized `scriptPubKey`, independently checked by a reviewer.
3. The actual recipient/controller and capacity in which they hold the funds. Do not claim that an incorporated foundation exists or legally owns the wallet merely because a domain exists.
4. Evidence of receiving, recovering, and spending from the intended script type using the actual custody design, with secrets kept outside the repository and logs.

Use the following fixed Mainnet P2PKH recipient and exact locking script. These values were decoded independently using Base58Check and checked against the reviewed repository's Wcash address-prefix and script-construction rules.

```text
address:      W1ZRudbQKz7jQdKmAarbaJsegscnZzXjpu2
network:      Wcash Mainnet
script type:  P2PKH
version:      1055
pubkey hash:  8708e78a0203f93b2691057281e1773fd2637020
scriptPubKey: 76a9148708e78a0203f93b2691057281e1773fd263702088ac
script bytes: 25
checksum:     7a6f13f5
```

Add implementation regression tests that parse this address through the repository's Mainnet-aware Wcash parser, reproduce this script exactly, round-trip the address, and reject it for Testnet and Regtest. Use separate controlled Regtest recipients for local tests. Address encoding validation does not prove possession of the private key, legal ownership, recovery, or spendability; those release checks remain outstanding. Do not generate production keys, substitute a sample destination, or silently disable the allocation.

The supplied destination is P2PKH, not an on-chain P2SH multisig address. Do not describe it as multisig without separate evidence of the custody design. If the maintainer later chooses a P2SH destination, obtain that exact replacement address and update the specification before freezing production constants; prove its signer/redeem-script and shielding workflow separately. The bundled wallet's P2PKH demonstration does not establish a working P2SH multisig wallet.

The address is fixed for the whole term. Changing it requires another reviewed consensus change; an operator config flag cannot redirect a mandatory payment. Document this consequence of key loss and custody compromise. An automatic shielding bot, spending governance, token voting, legal formation, spending reports, and fund distributions are outside this MR. Funds may accumulate and remain spendable after expiry under ordinary maturity and spending rules.

# 4 Consensus and coinbase construction

Create one shared Wcash funding-policy helper in `zebra-chain`, used consistently by subsidy accounting, validation, templates, RPCs, and coordinator attestation. Prefer a dedicated fixed-recipient Wcash policy over adapting Zcash's rotating-address schedule, which assumes a different halving model.

The helper should expose the active allocation and exact recipient script for a network and candidate height. Production Mainnet parameters must not be configurable through TOML, environment variables, RPC, or command-line switches. All non-Mainnet behavior stays unchanged except the explicitly isolated local test fixture described below.

## Mandatory output matching

During the active term, the coinbase must contain at least one transparent output with BOTH:

- the exact consensus recipient `scriptPubKey`;
- the exact amount `S(h) / 5`.

Match and account for one such output exactly once, for example by removing the first matching occurrence from a temporary view or multiset. Never mutate the serialized transaction while validating it.

A split across smaller outputs, a single larger combined payment, a wrong script, or payment only through a normal transaction or shielded output does not satisfy the mandatory-output rule. The template builder emits one separate mandatory output.

Do not impose an unnecessary uniqueness rule on identical transparent outputs. In an all-transparent coinbase, an additional identical output can be a voluntary use of the miner's remaining reward; it must remain in the miner remainder and count toward total value. Identical matching outputs are interchangeable for removing one mandatory occurrence.

If the miner selects the same transparent address as the fund, the canonical builder still emits separate fund and miner outputs. Validation must handle this correctly. A single merged output lacking the exact mandatory amount fails the rule.

## Transparent miner mode

After removing one mandatory fund output, the remaining miner payout is transparent with no Ironwood, Sapling, or legacy Orchard bundle. Preserve existing valid transparent output splitting at consensus level. The sum of remaining miner outputs must be `S - fund + fees`.

The coordinator may retain its stricter existing rule that every remaining payout output goes to the operator's configured miner address. Distinguish that operator attestation rule from consensus freedom to split transparent miner rewards.

## Ironwood miner mode

During the funded term:

- The only transparent coinbase output is the mandatory fund output.
- The miner payout remains private Ironwood.
- Net value entering Ironwood equals `S - fund + fees`.
- Existing Ironwood proof verification, coinbase flags, transaction-domain checks, and private-output restrictions remain enforced.
- Keep the prohibition on publicly recoverable zero-OVK miner outputs.
- Keep Sapling and legacy Orchard coinbase components prohibited.

This is a narrow exception to the current mixed-output prohibition. Any additional transparent output alongside the Ironwood miner payout remains invalid, even if it uses the fund address. Do not generally enable arbitrary mixed coinbases or weaken private recipient verification.

Before 40001 and from 621455 onward, the mandatory output disappears and the existing payout-mode rules apply again: all-transparent or private Ironwood, without arbitrary mixed payouts. Voluntary transparent donations remain possible within otherwise valid all-transparent miner payouts.

## Value conservation and limits

Validate full coinbase value across all transparent and shielded components against `S + fees`, exactly. Do not merely check the fund output and trust the rest. Preserve checked arithmetic and `MAX_WCASH_COINBASE_VALUE` against the TOTAL `S + fees`; checking only the reduced miner amount would weaken the current limit.

Apply the funding rule to candidate height, including template creation at tip plus one. Reorganizations must recompute policy from the new candidate height; no wall-clock or “activation already happened” latch is valid.

No new transaction format, proof system, transaction branch ID, genesis, P2P magic, or address prefix is specified by this change. Keep existing frozen identities. If implementation reveals a concrete need for an additional protocol change, document it for separate review rather than introducing one silently.

# 5 Code paths and operator interfaces

The paths below are verified at the reviewed SHA. Read their current versions before editing.

| Area | Required work |
| --- | --- |
| `zebra-chain/src/parameters/network/subsidy/wcash_mainnet.rs` | Preserve emission; add or reference fixed policy constants and independent endpoint vectors. |
| `zebra-chain/src/parameters/network/subsidy.rs` | Wcash currently returns empty funding streams and full miner subsidy. Integrate the shared allocation without altering inherited Zcash behavior. |
| `zebra-consensus/src/block/check.rs` | Enforce mandatory output; classify miner mode after accounting for that output; retain exact full-value and limit checks. Review `subsidy_is_valid`, `miner_fees_are_valid`, and `wcash_coinbase_payout_mode`. |
| `zebra-consensus/src/transaction/check.rs` | Preserve `wcash_coinbase_outputs_are_private` and all existing private-output checks. |
| `zebra-rpc/src/methods/types/transaction.rs` | Update `new_coinbase` and shared amount calculation; produce transparent fund plus either miner payout mode; check the gross value bound. |
| `zebra-rpc/src/methods.rs` and `types/subsidy.rs` | Correct `getblocksubsidy`, `createauxblock`, templates, and serialized funding metadata. |
| `wcash-merge-miner/src/coordinator.rs` | Update `validate_wcash_candidate_payout_with_authority` and `validate_transparent_wcash_payout`; verify fund independently, then attest the remaining miner reward. |
| `zebra-state` startup and backup restoration | Handle existing-state upgrade safety, including late upgrades and previously trusted semantic results. |
| Tests, release workflow, and docs | Cover both profiles, boundary migration, operator instructions, and revised policy. |

Do not pass a P2PKH fund recipient into the inherited Zcash P2SH-only funding assertion. Use explicit Wcash script matching. Required recipient construction must fail closed: do not silently omit the mandatory output through a filter_map or optional-address fallback.

For RPC behavior:

- `getblocksubsidy` must report gross `totalblocksubsidy`, reduced `miner`, one active fund entry, and correct `fundingstreamstotal`. Fees remain excluded from this subsidy RPC.
- Keep `founders` and `lockboxtotal` zero for this Wcash design. It is a direct transparent allocation, not a deferred lockbox.
- Include exact atomic `valueZat` and the actual Wcash-encoded address in the fund entry. The inherited `transparent::Address` display can emit Zcash t1/t3 addresses; explicitly preserve Wcash W1/W3 encoding while retaining existing Zcash serialization.
- Use `wcash.foundation` as informational recipient context and link the committed policy specification. Never invent an already-published website policy path.
- Outside the term, fund totals are zero and no mandatory fund entry is advertised.
- Preserve `createauxblock.coinbasevalue` as the miner's NET payout including fees: `S - fund + fees`. It excludes the mandatory fund output. Update documentation and fixtures to make this unambiguous.

The coordinator must derive allocation and script from its own network/height policy and verify the serialized candidate. It must not trust a supplied “fund valid” flag or arbitrary RPC destination. For private payouts, retain protected-file incoming viewing capability, trial decryption, exact configured receiver matching, and attribution of every miner action. Do not log the IVK.

Update native job/preflight behavior, proposed-block validation, cached candidates, and durable outbox replay consistently. New work after a boundary must use the current candidate height. Old and replacement jobs must not double-count a fund output.

Pools must update both node and coordinator/coinbase-construction software. External pool accounting should credit miners from the net miner amount, not gross subsidy. The independent Zcash parent reward and fee distribution must remain unchanged.

# 6 Tests for the actual production policy

Add tests that execute the real shared implementation, plus an independent integer reference calculation for the endpoint.

1. Golden vectors at 39999, 40000, 40001, 40002, 621454, 621455, and 621456, with zero and nonzero fees. Assert exact atomic conservation and rounding.
2. Recompute the existing smooth recurrence from curve counter zero at 40001. Assert the first `2*S <= initial_S` is 621455, the prior block fails that predicate, and summed per-block allocation equals 184467517026194 atoms. Do not use the production expiry constant to decide where this independent calculation stops.
3. Assert gross subsidy and curve counter are unchanged by funding. Keep the existing tail vectors and all Mainnet emission tests.
4. Accept correct transparent and Ironwood miner coinbases throughout the term. Test first/last funded and first unfunded blocks.
5. Reject omission, wrong recipient, wrong script, one-atom underpayment or overpayment of the required output, a merely split payment, a shielded substitute, and an attempted fee-inclusive allocation when no exact required output exists.
6. Reject total overclaim, underclaim, overflow, bad value balances, unsupported pools, publicly recoverable Ironwood coinbases, and arbitrary mixed outputs.
7. Test reordered outputs, a miner address equal to the treasury address, transparent splits, and an extra identical transparent output funded from the miner share. These must not be confused with double-counting the mandatory allocation.
8. Confirm Ironwood plus a second transparent output is rejected even if that second output repeats the fund tuple. Confirm mixed coinbases are rejected outside the term.
9. Builder-to-validator round trips for both modes and for `getblocktemplate`/`createauxblock`. Assert net `coinbasevalue`, RPC metadata, Wcash address encoding, and unchanged Zcash RPC snapshots.
10. Coordinator adversarial cases: correct node amount with wrong serialized recipient, malformed fund output, gross value advertised as miner payout, private action to another receiver, failed IVK attribution, and boundary-stale candidate. Preserve existing parent-chain and AuxPoW validation.
11. Check ordinary Wcash Testnet, ordinary Regtest, and default Zcash profiles remain unchanged. Verify no production configuration can override Mainnet start, expiry, rate, or destination.
12. Exercise actual full semantic validation. Built-in Wcash checkpoints are currently genesis-only; do not add checkpoints at or above the tested boundaries that would bypass the new rules.

Tests that mutate a solved block must rebuild the proof or deliberately target the proposal/semantic path. A failed AuxPoW or Merkle-root check is not evidence that the funding rule rejected a bad payout.

# 7 Accelerated local upgrade test

Add `scripts/wcash-foundation-fund-e2e.sh` and its controlled fixtures. This is a NEW required harness, not an existing executable command. Reuse the real solving, wallet, and proposal infrastructure from the existing local E2E scripts.

The current Wcash built-in networks reject inherited `[network.testnet_parameters]` overrides. `generate` and `generatetoaddress` are not working shortcuts for current WcashRegtest. Do not document either as a way to test this upgrade.

## Fixed local fixture

Add an explicit non-default testing feature or equivalently isolated test-only policy injection. Proposed feature name: `wcash-funding-test-fixture`.

| Fixture parameter | Value |
| --- | --- |
| Network | Isolated WcashRegtest |
| First funded block | 10 |
| First unfunded block | 20 |
| Rate | 1/5 |
| Treasury | Controlled disposable Regtest transparent script |
| Base subsidy | Existing Regtest 6.25 TWC |
| Active block payout | Fund 1.25 TWC; miner 5 TWC plus fees |
| Total issuance to fund | 12.5 TWC across blocks 10–19 |

Use the SAME policy, matching, accounting, validation, template, and coordinator logic as production; only network fixture parameters differ. The feature must be propagated consistently to the node and coordinator. Ordinary Regtest without the feature remains unchanged.

Fixture-enabled executables must clearly identify themselves and refuse Mainnet and public Testnet startup/use. They must not be published as release artifacts. Do not alter public network identities, subsidy curves, difficulty, timestamps, or coinbase maturity to accelerate the test. Retain Regtest's built-in identity for the controlled old/new comparison, but use isolated loopback peers and disposable data directories.

Use separate target directories and record hashes for baseline old binaries, upgraded production binaries, and upgraded fixture binaries. Do not accidentally overwrite the shipping node with a fixture build.

## Persistent-state migration

Use two upgraded Wcash nodes, an old Wcash node binary, a fresh-sync observer, the existing Zcash Regtest parent template node, and the independent parent validator as needed. Processes can be staged rather than all running simultaneously.

1. Build the baseline node and coordinator from the recorded pre-change `main` SHA.
2. Mine blocks 1–8 with the old pair into persistent disposable state. Include at least one controlled Ironwood miner coinbase, so the existing explicit Regtest one-confirmation wallet override can fund the nonzero-fee transaction during blocks 10–19. Do not shorten transparent coinbase maturity. Record genesis, tip hash, balances, state path, features, and executable hashes.
3. Stop cleanly. Start the new fixture node on the SAME state directory. Do not delete or reinitialize it. Verify the restored tip and historical block hashes.
4. Mine block 9 using the new pair. It must still pay the full 6.25 TWC subsidy to the miner with no compulsory fund output.
5. Inspect and mine block 10. It must allocate 1.25 TWC to the fixture fund and 5 TWC plus fees to the miner.
6. Mine through block 19, alternating transparent and Ironwood miner payouts. Inspect every serialized coinbase and policy result. Include a valid transaction with a known nonzero fee in at least one funded block; use the existing controlled private-spend fixture policy where necessary.
7. At block 20, verify full 6.25 TWC plus fees returns to the miner and no mandatory fund output is emitted. Private miner payout becomes Ironwood-only again.
8. Mine block 21. Restart and verify identical tip, balances, UTXOs, and inactive fund state.
9. Start a clean upgraded observer with an empty disposable database and sync over loopback P2P from the upgraded chain. Compare tested block hashes, supply, and value-pool totals.

The checked-in sample profiles use ephemeral state, so create persistent fixtures for this migration test. Existing settings to adapt are:

```toml
[state]
ephemeral = false
cache_dir = "<unique disposable absolute path>"
should_backup_non_finalized_state = true
debug_skip_non_finalized_state_backup_task = true
delete_old_database = false
```

The debug backup setting is only for rapid local testing. Assign unique loopback ports, explicit local peers, and suitable per-IP connection limits. Do not reuse production cookies, wallets, seed files, RPC listeners, databases, or journals.

## Compatibility matrix

| Candidate during active funding | Old validation | New validation |
| --- | --- | --- |
| Old transparent all-miner payout, no fund | Accepts if otherwise valid | Rejects missing mandatory output |
| Correct transparent miner plus transparent fund | Can accept under old rules | Accepts |
| Correct Ironwood miner plus transparent fund | Rejects old mixed-output rule | Accepts |
| Wrong or missing fund with otherwise valid value | No fund enforcement | Rejects |

Test this matrix with actual validator evidence. Do not claim every new funded block is rejected by every old node. An old coordinator can reject new funded templates even when an old node accepts their all-transparent form; test and document that incompatibility.

## Reorganizations

Using nonfinalized local branches and the existing `invalidateblock`/`reconsiderblock` capabilities or competing peers:

- Roll back across activation. A candidate below 10 is unfunded; crossing 10 again reinstates the allocation.
- Roll back from above expiry to a tip where candidate 19 is funded. Re-crossing 20 stops the allocation again.
- Replace funded blocks with a valid competing branch and verify treasury UTXOs and wallet balances roll back and reapply without duplicates.
- Restart at boundary tips and repeat the checks.
- Assert template caches and coordinator jobs use the new branch's candidate height and do not retain stale policy state.

## Late upgrade safety

Separately let an old node store no-fund blocks after fixture activation, then restart it with new software on the same state. Test nonfinalized backups and the corresponding finalized-state path.

The existing nonfinalized loader reconstructs `SemanticallyVerifiedBlock` from stored bytes; do not assume it reruns new funding validation automatically. The implementation must either revalidate affected stored blocks under the new policy or refuse safely with an actionable recovery procedure. An old invalid history must never become trusted merely because a new executable opened it.

Use explicit persisted validation-version/coverage metadata or an equivalently sound migration guard. Bind coverage to the rule version, covered height, and block hash/chain state, including restored nonfinalized backups. A global “upgraded” flag cannot cover blocks later appended by an old executable. Add a new-to-old-to-new restart regression in which the old executable appends an unfunded postactivation block after new-rule metadata was written. Unknown or uncovered postactivation history must be revalidated or refused. Missing metadata on legacy state beyond activation is not proof of new-rule validity. A normal upgrade before activation must retain the existing database without requiring a resync.

For incompatible late-upgrade state, a documented rebuild into a NEW directory is an acceptable recovery route if automated revalidation is not supported. Preserve the original database; never automatically delete, rewrite, or “bless” incompatible finalized history. Test the guard without relying on a new checkpoint to hide the invalid blocks.

## Treasury maturity and recovery

Keep the 100-block maturity rule unchanged. For a fund output created at fixture block 10:

- At tip 108, a proposed spend in block 109 must fail maturity.
- At tip 109, it may be spent in block 110.
- Mine the exact signed shielding transaction using the controlled treasury custody setup.
- Confirm inherited mandatory shielding is enforced; the transparent coinbase cannot bypass it through a direct transparent spend.
- Restore the controlled treasury wallet and verify receipts and resulting shielded balance.
- Confirm expiry does not invalidate accrued UTXOs or require spending them by block 20.

Test the intended production script type. A P2PKH test cannot certify multisig signing. The spending fixture may continue beyond 110 blocks; shortening maturity to make it fast is prohibited.

## Existing mining mechanism

The local harness should adapt the existing bounded native solver invocation:

```sh
./target/release/wcash-merge-miner native-mine \
  http://127.0.0.1:28232 \
  http://127.0.0.1:18232 \
  http://127.0.0.1:18242 \
  - 512 "$start_nonce"
```

For the new fixture, point to the separately built fixture coordinator instead of a shipping binary. The existing local environment contract includes `WCASH_EXPECTED_GENESIS_HASH`, `ZCASH_EXPECTED_GENESIS_HASH`, `ZCASH_NETWORK=regtest`, `WCASH_SHARE_JOURNAL`, `WCASH_PAYOUT_ADDRESS`, and `ZCASH_PAYOUT_ADDRESS`. Private payouts also use the protected `WCASH_PAYOUT_IVK_FILE`.

Pin the existing Regtest identities:

```text
Wcash genesis:
70bf0bab17eff361a6331bb825b3b7253c8c96ff96407f948161d2912658bb1c

Zcash parent Regtest genesis:
029f11d80ef9765602235e1bc9727e3eb6ba20839319f761fee920d63401e327
```

Derive controlled test recipients; do not assume documented example addresses have known keys. Bound retries, solver attempts, process startup, and RPC waits. Keep seeds, cookies, IVKs, and signed recovery state out of public logs.

Wcash and the coordinator must verify real Equihash/AuxPoW. Existing Zcash parent Regtest fixtures disable their own native PoW check; describe their result as parent structure/target/proposal/chain acceptance, not a second independent Equihash validation.

The 10/20 exercise proves switching, migration, payout construction, and custody behavior on an accelerated local network. The Mainnet golden-vector and production-artifact tests separately prove the actual 40001/621455 parameters. Passing local tests does not establish pool adoption or guarantee production safety.

# 8 Required build and release checks

Use the repository-pinned Rust 1.91.0 toolchain and committed lockfile. Required baseline commands include:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked

cargo test --locked -p zebra-chain --lib --features wcash-consensus
cargo test --locked -p zebra-consensus --lib --features wcash-consensus
cargo test --locked -p zebra-rpc --lib --features zebra-chain/wcash-consensus
cargo test --locked -p zebra-state --lib --features zebra-chain/wcash-consensus -- --test-threads=1
cargo test --locked -p wcash-merge-miner
cargo test --locked -p wcash-wallet
```

Also run every applicable check in `.github/workflows/wcash-release-gate.yml`, including Wcash feature-specific linting, default Zcash regressions, transaction-domain boundaries, and separately compiled profiles. Add non-solving funding and migration checks to that gate. Do not replace broad required checks with a filter that accidentally selects zero tests.

Existing local release commands:

```sh
scripts/build-wcash-testnet-binaries.sh
scripts/wcash-merged-mining-e2e.sh
scripts/wcash-testnet-e2e.sh
```

After implementing it, run the NEW harness:

```sh
scripts/wcash-foundation-fund-e2e.sh
```

Provide its exact build/features and invocation in committed documentation. Hosted CI must not solve proof of work; run real-solver E2Es on authorized isolated local hardware. Existing normal-network E2Es should retain their original no-fund expectations. The separate fixture E2E exercises the fund.

Inspect final shipping executable features and policy constants. Release packaging must exclude the test fixture and fail if fixture binaries are substituted. Verify a production build still rejects all attempts to override Mainnet funding parameters.

# 9 Documentation and deployment sequence

Update `README.md`, `AGENTS.md`, `.github/copilot-instructions.md`, `docs/wcash-direction.md`, `docs/wcash-consensus.md`, affected mining/RPC/pool guides, relevant snapshots, and the release notes. Search for stale “no tax,” full-miner-reward, and private-only-coinbase claims. Scope statements by network and height; historical blocks and default testing profiles remain different.

Add a committed specification and an operator upgrade guide. Prepare matching text for `wcash.foundation` and `w.cash`; publishing those websites is a separate action. The disclosure must state:

- This changes the original zero-allocation policy.
- 20% of subsidy, excluding fees, is compulsory for blocks 40001–621454.
- The destination, actual controller/recipient, and custody description.
- Payments are transparent even when miners choose private Ironwood rewards.
- Gross emission is unchanged.
- The allocation expires at 621455. Existing reserves remain usable.
- Renewal requires a new proposal, implementation, release, and adoption; there is no automatic extension or pool-controlled voting mechanism.

Release procedure:

1. Finish implementation, custody/address verification, reproducible checks, and independent consensus review.
2. Produce versioned node and coordinator binaries with checksums and clear minimum supported versions.
3. Publish operator instructions and give pools, wallet backends, explorers, and other validating services time to upgrade before 40001. Record actual deployment acknowledgments; a merged PR is not evidence of adoption.
4. Verify upgraded nodes retain the canonical preactivation tip and issue correct candidate templates at the boundary.
5. Monitor first funded blocks, recipient script/amount, both payout modes, chain continuity, and competing tips using independent upgraded nodes.
6. Continue monitoring without changing the published parameters through a runtime switch.

If Mainnet reaches 40001 before the change is safely adopted, stop and report the missed activation. Do not quietly activate retrospectively, rewrite historical consensus, or claim that a late release will be harmless. A new activation decision would be required.

Before activation, rollback to the previous binary is subject to normal database compatibility checks. After activation, an old binary is not a safe general rollback: it can follow a chain incompatible with the adopted rules. Incident instructions must preserve databases and identify the canonical upgraded chain.

The fund sunset is deliberate. Teams may present results and request a renewal before it, but this MR must not include an automatic prolongation, emergency admin switch, holder vote, or implied guaranteed renewal.

# 10 MR acceptance and evidence

The MR is ready for final review only when:

- The supplied Mainnet address and exact script are pinned and independently verified.
- Mainnet start 40001 and end-exclusive 621455 match the golden vectors.
- Both miner payout modes pay the transparent fund correctly.
- Consensus, builders, RPCs, coordinator, and stored-state migration agree.
- The isolated 10/20 activation, expiry, reorg, fresh-sync, and restart scenarios pass.
- Missing-fund legacy state after activation is rejected or safely revalidated.
- Treasury maturity, shielding, backup recovery, and intended script-type signing are demonstrated.
- The default Zcash and ordinary Wcash test profiles retain their existing behavior.
- Required checks pass, or failures are explicitly unresolved and the MR remains non-release-ready.
- A human independent review covers monetary arithmetic, payout matching, private-output enforcement, and migration behavior.
- Documentation, changelog, operator instructions, and production artifact checks are complete.

Attach an evidence report containing base/head SHAs, toolchain, feature sets, binary hashes, exact commands and exit codes, named test counts, tested heights and block hashes, expected/observed amounts, compatibility-matrix results, restart/reorg evidence, and any unresolved limitations. Use only disposable test credentials in reproduction instructions.

# 11 Source references

These are implementation anchors, not evidence that the proposed change has already passed tests.

- [Repository and reviewed revision](https://github.com/w-cash/wolf/tree/41a39cab3f259509653c1d4930df3dae3dabba98).
- [Mainnet emission recurrence](https://github.com/w-cash/wolf/blob/41a39cab3f259509653c1d4930df3dae3dabba98/zebra-chain/src/parameters/network/subsidy/wcash_mainnet.rs).
- [Subsidy and miner accounting](https://github.com/w-cash/wolf/blob/41a39cab3f259509653c1d4930df3dae3dabba98/zebra-chain/src/parameters/network/subsidy.rs).
- [Coinbase value and payout-mode validation](https://github.com/w-cash/wolf/blob/41a39cab3f259509653c1d4930df3dae3dabba98/zebra-consensus/src/block/check.rs).
- [Coinbase builder](https://github.com/w-cash/wolf/blob/41a39cab3f259509653c1d4930df3dae3dabba98/zebra-rpc/src/methods/types/transaction.rs).
- [Coordinator payout attestation](https://github.com/w-cash/wolf/blob/41a39cab3f259509653c1d4930df3dae3dabba98/wcash-merge-miner/src/coordinator.rs).
- [Nonfinalized-state restoration](https://github.com/w-cash/wolf/blob/41a39cab3f259509653c1d4930df3dae3dabba98/zebra-state/src/service/non_finalized_state/backup.rs).
- [Wcash local mining guide](https://github.com/w-cash/wolf/blob/41a39cab3f259509653c1d4930df3dae3dabba98/docs/wcash-local.md).
- [Existing real-solver and wallet E2E](https://github.com/w-cash/wolf/blob/41a39cab3f259509653c1d4930df3dae3dabba98/scripts/wcash-testnet-e2e.sh).
- [Release gate](https://github.com/w-cash/wolf/blob/41a39cab3f259509653c1d4930df3dae3dabba98/.github/workflows/wcash-release-gate.yml).
