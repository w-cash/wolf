# Wcash-maintained ZEC parent node

This release line starts at stable Zebra 7.0.0, commit
`e8db87eb69254e0d4eedd058eadd48bdfa09065a`. It preserves the complete upstream
Zcash NU7 consensus implementation and adds the private contracts required by
the Wcash merged-mining backend. It is maintained in `w-cash/wolf` on the
`zec-parent-v7.0.0` source line. The ordinary Wolf source line continues to own
the WEC node and pool backend.

The intended chain pairing is **WEC at 75 seconds and ZEC at 25 seconds**.
This artifact runs Zcash consensus. It does not introduce WEC transaction
domains, WEC emission/foundation rules, or a WEC database migration.

## Contracts

- The authenticated, private `getblocktemplate.wcashaux` request reserves the
  exact 44-byte carrier, transforms a clone of the selected parent template,
  preserves its mined transaction ID and recomputes its authentication and
  block commitments. Ordinary templates remain child-independent.
- `wcashparentpayoutcommitment` attests the configured Zcash payout-address
  encoding using the backend's existing hash domain. Payout addresses and memos
  remain redacted from mining-configuration diagnostics.
- `wcashparentcontext` is returned only for a `wcashaux` template. Its version-1
  object contains `previousblockhash` (canonical display-order hash), `height`
  (candidate height), and `parentnsmvaluebalance` (unsigned integer zatoshis after
  that predecessor, excluding any seed added at candidate activation). These
  values are retained from the same state view that constructed the cached
  template and coinbase. Ordinary GBT responses omit this field; adding a child
  commitment never resamples the live tip or invents a zero balance.
- `getwcashparentcontext(previousblockhash, height)` independently returns the
  same version-1 object without constructing another template or shielded
  coinbase proof. The node checks exact best-chain membership and actual parent
  height, reads value pools **by hash**, then rechecks membership. Unknown and
  side-chain blocks, mismatched heights, absent value records and failures are
  errors. One one-second deadline covers all readiness and state reads. Clients
  must require matching context from their independent proposal validators and
  retain their existing predecessor fences around proposal validation; this
  lookup alone is neither a consensus vote nor a guarantee against a later reorg.
- `getblockstatus` reports exact committed best-chain, side-chain and unknown
  membership. Best-chain confirmations and the block come from the same state
  view. Its implementation must preserve that membership view across reorgs.
- Mined-block advertisement retains bounded delivery obligations and typed
  local/nonlocal write outcomes over Zebra 7's bounded advertisement queues.
  A completed local socket write is not independent peer receipt or canonical
  block acceptance.

`wcash-parent-template` contains only bounded carrier/hash helpers. Its source
domains and vectors match the existing backend contract. It contains no WEC
consensus profile, transaction parser or proof-of-work validator.

## Build and qualification

Use the committed Rust toolchain and lockfile. The operator artifact is
`zebrad`; build it in a target directory dedicated to this source line:

```sh
cargo build --locked --release -p zebrad --bin zebrad \
  --no-default-features --features prometheus,release_max_level_info
```

Run formatting, strict all-target lint, the applicable native test suite and the
new RPC/state/gossip cases before promoting an artifact. Keep the upstream NU7
activation, fee/NSM, transaction-version and reorg tests. Every new NU7 coinbase
fixture must assert its actual embedded branch is `0x77190AD9`; a fixture that
falls back to NU6.3 is not NU7 evidence. Label parser-only and unsolved fixtures.
Real full winning-proof and complete merged-stack qualification remain
separate release gates.

The template construction preserves upstream's parent NSM balance and gross-fee
cache context. NU7 miner fee totals exclude the NSM contribution, while ordinary
transaction fee fields remain gross. Do not replace the upstream cache or
coinbase construction with the old height/fee-only implementation.

For a local reward calculation, use `parentnsmvaluebalance` with the
parent-dependent subsidy helper, which applies any activation seed itself.
Require the exact candidate height and predecessor hash to match the template
and every independent context response. Reject missing or unsupported context
versions; never substitute a live `getblocksubsidy` sample or an assumed zero.
Independent validation of the exact prepared proposal remains mandatory.

Record source and build features, the executable SHA-256, configuration
identity, all test results and unexecuted gates. Preserve upstream's version and
git-commit diagnostics so this fork can be distinguished from stock Zebra.

## Deployment checkpoint

The observer, template and independent validator/submission roles all need
NU7-capable artifacts before Zcash Mainnet height 3,543,405. The deadline is a
height, not a guaranteed calendar time. Check each role's scheduled upgrade,
protocol version, exact tip, RPC contract and independent proposal validation.
Keep RPC authenticated and private.

Build off the production pool VPS. Before upgrading, rehearse consistent node
state backups and restoration, capacity and a qualified NU7 standby. Zebra 7
migrates its Zcash state from v28 to v29. Old Zebra cannot read migrated state;
after NU7 activation it also cannot validate canonical Zcash, even with a v28
backup. Recovery then needs a qualified NU7 artifact or a repair forward.

Keep node restoration separate from accounting, wallet data, signed payouts,
reserves and journals. A tested parent node does not establish end-to-end
merged-mining readiness: the backend's parser/reward rules and the coordinated
WEC validator acceptance upgrade must also pass their release gates.

This change prepares source and tests. Merging, server installation, restart,
migration and deployment require their own maintainer approval.

## References

- [Zebra 7.0.0 and migration](https://github.com/ZcashFoundation/zebra/releases/tag/v7.0.0)
- [NU7 deployment](https://zips.z.cash/zip-0259)
- [25-second target spacing](https://zips.z.cash/zip-0218)
- [Fee allocation](https://zips.z.cash/zip-0235)
