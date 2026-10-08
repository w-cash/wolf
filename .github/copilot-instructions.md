# Wcash Wolf — Copilot Review Instructions

You are reviewing PRs for Wolf, the Rust node and protocol workspace for Wcash.
It is derived from Zebra and retains many upstream crate names, but a Wcash
artifact, network and contribution are not interchangeable with their Zcash
counterparts. Read [`AGENTS.md`](../AGENTS.md), the
[protocol direction](../docs/wcash-direction.md), and the relevant source before
reviewing. Prioritize correctness, consensus safety, DoS resistance and
maintainability. Avoid style-only feedback unless it clearly prevents bugs.

If the diff or PR description is incomplete, ask questions before making strong claims.

## Contribution Process Checks

Before reviewing code quality, verify:

- [ ] Scope was acknowledged by a Wcash maintainer, or the PR records a direct maintainer request
- [ ] PR description includes Motivation, Solution, and Tests sections
- [ ] PR title follows conventional commits
- [ ] If AI tools were used, disclosure is present in the PR description

If these are missing, note it in the review.

## Architecture Constraints

```text
zebrad (CLI orchestration)
  ├── zebra-consensus (verification)
  ├── zebra-state (storage/service boundaries)
  ├── zebra-network (P2P)
  └── zebra-rpc (JSON-RPC and gRPC)
        └── zebra-node-services (service traits)
              └── zebra-chain (data types; sync-only)
```

- Dependencies flow **downward only** (lower crates must not depend on higher crates)
- `zebra-chain` is **sync-only** (no async / tokio / Tower services)
- State uses `ReadRequest` for queries, `Request` for mutations

## Wcash Direction Checks

- A Wcash node is `zebrad` built with `wcash-consensus`; do not accept a default
  Zcash artifact merely relabelled as Wcash.
- Preserve AuxPoW merge mining and validate Wcash and parent targets
  independently. Parent-chain acceptance is not required for a Wcash winner.
- Mainnet uses a 40,000-block slow start, Monero-style integer smooth decay and
  permanent 0.375-WEC tail emission. It has no halvings or fixed supply cap.
- Reject any founders reward, development tax, funding stream, lockbox or other
  protocol allocation in Wcash profiles.
- Transparent and Ironwood are the only active Wcash value pools. Inherited
  zero-valued RPC compatibility fields do not activate Sprout, Sapling, legacy
  Orchard or lockbox value.
- Keep Mainnet, Testnet and Regtest identities, transaction domains, addresses,
  storage and subsidy schedules separate.
- Preserve monetary values as exact integer atomic units. Do not infer an
  economic supply cap from a technical amount bound.
- Treat mining, signing, broadcasting, payout and candidate-retirement commands
  as state-changing even when a wrapper calls them a preflight.

## High-Signal Checks

### Tower Service Pattern

If the PR touches a Tower `Service` implementation:

- Bounds must include `Send + Clone + 'static` on services, `Send + 'static` on futures
- `poll_ready` must call `poll_ready` on all inner services
- Services must be cloned before moving into async blocks

### Error Handling

- Prefer `thiserror` with `#[from]` / `#[source]`
- `expect()` messages must explain **why** the invariant holds:

  ```rust
  .expect("block hash exists because we just inserted it")  // good
  .expect("failed to get block")                            // bad
  ```

- Don't turn invariant violations into misleading `None`/defaults

### Numeric Safety

- External/untrusted values: prefer `saturating_*` / `checked_*`
- All `as` casts must have a comment justifying safety

### Async & Concurrency

- CPU-heavy crypto/proof work: must use `tokio::task::spawn_blocking`
- All external waits need timeouts and must be cancellation-safe
- Prefer `watch` channels over `Mutex` for shared async state
- Progress tracking: prefer freshness ("time since last change") over static state

### DoS / Resource Bounds

- Anything from attacker-controlled data must be bounded
- Use `TrustedPreallocate` for deserialization lists/collections
- Avoid unbounded loops/allocations

### Performance

- Prefer existing indexed structures (maps/sets) over iteration
- Avoid unnecessary clones (structs may grow over time)

### Complexity (YAGNI)

When the PR adds abstraction, flags, generics, or refactors:

- Ask: "Is the difference important enough to complicate the code?"
- Prefer minimal, reviewable changes; suggest splitting PRs when needed

### Testing

- New behavior needs tests
- Async tests: `#[tokio::test]` with timeouts for long-running tests
- Test configs must use realistic network parameters
- Wcash behavior needs the applicable checks from
  `.github/workflows/wcash-release-gate.yml`; default-profile tests alone are
  insufficient evidence

### Observability

- Metrics use dot-separated hierarchical names with existing prefixes (`checkpoint.*`, `state.*`, `sync.*`, `rpc.*`, `peer.*`, `zcash.chain.*`)
- Use `#[instrument(skip(large_arg))]` for tracing spans

### Changelog & Release Process

- Changelogs are generated by [changie](https://changie.dev) from `.changes/`: never edit `CHANGELOG.md` or a crate's `CHANGELOG.md` by hand
- User-visible changes need a change fragment: `changie new -j zebrad -k Added -b "..."`
- Library-consumer-visible changes need one fragment per affected crate: `changie new -j zebra-chain -j zebra-state -k breaking -b "..."`
- PR title and branch commits follow conventional commits (merged to main with a merge commit, so branch commits are preserved)

## Extra Scrutiny Areas

- **`zebra-consensus` / `zebra-chain`**: Consensus-critical; check serialization, edge cases, overflow, test coverage
- **`zebra-state`**: Read/write separation, long-lived locks, timeouts, database migrations
- **`zebra-network`**: All inputs are attacker-controlled; check bounds, rate limits, protocol compatibility
- **`zebra-rpc`**: zcashd compatibility where intended, Wcash identity and pool semantics, response shapes, errors, timeouts, user-facing behavior
- **`zebra-script`**: FFI memory safety, lifetime/ownership across boundaries

## Output Format

Categorize findings by severity:

- **BLOCKER**: Must fix (bugs, security, correctness, consensus safety)
- **IMPORTANT**: Should fix (maintainability, likely future bugs)
- **SUGGESTION**: Optional improvement
- **NITPICK**: Minor style/clarity (keep brief)
- **QUESTION**: Clarification needed

For each finding, include the file path and an actionable suggestion explaining the "why".
