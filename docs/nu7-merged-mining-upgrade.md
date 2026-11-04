# Zcash NU7 merged-mining upgrade

The maintainer-approved direction is Wcash at its existing 75-second target
spacing and Zcash at NU7's 25-second target spacing. The chains check work
against independent targets; matching their block intervals is unnecessary.
Wcash's smooth emission, permanent tail, foundation allocation, transaction
domain and network identity remain release invariants.

This is a staged implementation plan, not a deployment or readiness claim.
Zcash NU7 is scheduled at Mainnet height 3,543,405, with branch ID
`0x77190AD9` and minimum peer protocol 170190. Use the
[deployment specification](https://zips.z.cash/zip-0259) and
[stable Zebra release](https://github.com/ZcashFoundation/zebra/releases/tag/v7.0.0)
as the upstream references. The initial audited upstream commit is
`e8db87eb69254e0d4eedd058eadd48bdfa09065a`; the production backend baseline is
`153492512ebca3a3dcc104fec3ffeaab83cce41f`.

## First prerequisite: explicit parent-domain rules

`ParentCoinbaseRules::Legacy` freezes the 15 branch IDs recognized by the
existing production parent parser, including its Wcash domains. Both the
existing `AuxPowProof::validate()` method and the node's explicit policy call
retain that set. Canonical parsing, both transaction digests, commitments,
Merkle paths, the authenticated Wcash target and Equihash remain mandatory.

The legacy policy rejects the official NU7 branch, unknown branch IDs and
experimental placeholders. Standard production parsing already rejects those
values. Explicitly freezing the domain prevents a future library update from
silently changing Wcash block acceptance. Experimental placeholder builds are
not a supported production consensus profile.

This prerequisite does not add NU7 parser support or activate new Wcash proofs.
It does not change proof framing, Wcash subsidy, foundation policy, block
spacing, wallets, accounting, PostgreSQL schema or node-state storage.

## Remaining compatibility changes

1. Port the required private template, payout-commitment and block-status RPC
   contracts onto the complete stable NU7 parent-node implementation. Preserve
   independent proposal validation and exact winning-block submission. Upgrade
   every Zcash role; an unmodified stock node cannot replace the custom template
   provider.
2. Update Wolf's canonical parent parsing, height-selected branch, subsidy,
   funding and fee checks. Preserve the immutable template and retained-job
   recovery contracts. Do not enable experimental NU7 flags as a substitute for
   complete consensus support.
3. Add a new parent-proof policy and select it from the candidate Wcash network
   and height. Agree a future Wcash activation height with validating operators
   before enabling new-domain proofs. No Mainnet height is assigned here.
   Never use an external live Zcash tip as Wcash consensus input. Preserve
   historical supported proof behavior across reorgs and replay.
4. Test the separate pool transports, assignment/winner handling, durable
   journals and accounting against the new backend. Change their code or schema
   only when an integration requirement establishes the need.

Upgrading seed nodes alone is insufficient. Pool validators, other validating
operators and services must adopt compatible software before the agreed Wcash
height. Molepool's deployed version has not been inspected. A new-domain
Wcash-only proof can be constructed before Zcash activation, so new parsing
must not immediately widen acceptance during a partial rollout.

## Qualification and release checkpoints

Test the cross-product of Wcash activation height minus one, at activation and
plus one with the corresponding three Zcash NU7 heights. Cover genuine full
Wcash-only and dual-winning proofs, wrong branches, fee and subsidy rounding,
historical Wcash blocks, reorgs across both boundaries, late winners, durable
submission retries and restart recovery. Parser fixtures and deliberately
unsolved proofs must be labeled as such; they do not establish winning-block
delivery.

Measure parent observation through actual miner socket notification, and
winning proof through durable recording, submission, independent receipt and
canonical confirmation. Record latency distributions, eligible work, stale and
unknown work, queue bounds and resource use with bounded metrics. A maximum
job lease is not the refresh interval.

Build and test off the production pool VPS. Prepare a consistent backup and
restore rehearsal before Zebra's v28-to-v29 state migration, plus a qualified
NU7-capable standby. Preserve payout reserves, signed artifacts, wallet data and
accounting journals independently of node-state restoration. Each tested MR is
presented for maintainer approval before merge and production deployment.

## Contingency

Before NU7, recovery may use the previous application and a compatible node
state backup. After NU7, Zebra 6.x cannot follow the canonical Zcash chain even
with its old database restored. Use a qualified NU7 standby or repair forward.
After Wcash accepts new-domain proofs, its old parser is likewise unsuitable
for canonical-chain rollback.

If the merged stack cannot qualify in time, prepare a separately tested Zcash-only
route or a miner-approved move to a verified compatible pool. The current
backend has no qualified production switch for that mode. Stop unsupported
jobs if no verified route exists, preserving all accounting and proof evidence.
External Zcash mining does not itself maintain Wcash block production.
