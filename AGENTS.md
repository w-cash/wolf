# Wcash Wolf — Agent Guidelines

This file is for coding agents working in **w-cash/wolf**, not for submitting
changes to ZcashFoundation/zebra. For agents querying Wcash infrastructure, read
[the integration guide](docs/wcash-agents.md). `CLAUDE.md` points to this file.

## Contribution scope and approval

Every pull request requires human review. Use [CONTRIBUTING.md](CONTRIBUTING.md)
and the repository's [PR template](.github/pull_request_template.md).

- A Wcash maintainer's direct request is sufficient scope approval; reference
  that request honestly rather than inventing an issue number.
- For unsolicited work, first obtain acknowledgment from a Wcash maintainer in
  the repository's enabled discussion channel. If Issues are disabled, do not
  redirect Wcash requests to Zebra's issue tracker.
- Consensus, networking, genesis, monetary-policy, privacy and mining changes
  need an explicit design discussion, vectors and review before implementation.
- Keep changes focused. Documentation requests do not authorize changing
  consensus, enabling a public RPC listener, modifying production services,
  moving funds, rotating keys or merging a PR.
- Upstream Zebra contributions follow upstream's own approval policy separately.

## Read before editing

1. Read [README.md](README.md) and the [documentation index](docs/README.md).
2. Read the guide and implementation for the affected component; do not use a
   website, old README or inherited Zebra example as sole authority.
3. Record the current commit, intended network and build features. Preserve
   existing user work and check for more specific instructions.
4. Review every diff and disclose the checks actually run and their results.

## Wcash-specific boundaries

- `zebrad` is still the node executable. `wcash-consensus` selects Wcash;
  a default Zcash artifact cannot be relabeled as a Wcash node.
- Mainnet is implemented. The former "mainnet disabled" description is stale.
  Implementation, deployment, successful CI and audited release readiness are
  different claims; require evidence for each.
- Mainnet, Testnet and Regtest have distinct genesis, branch IDs and storage
  namespaces. They also have different subsidy schedules. Use the
  [consensus reference](docs/wcash-consensus.md) and linked source.
- Preserve the maintainer-approved [protocol direction](docs/wcash-direction.md):
  merge mining, the 40,000-block Mainnet slow start, Monero-style integer smooth
  decay, permanent tail emission, no protocol tax, and exactly the transparent
  and Ironwood active value pools. Changing any of these is not a cleanup.
- Never change frozen genesis, branch IDs, wire versions or monetary constants
  as a documentation cleanup. Mainnet has no fixed economic supply cap; its
  curve scale and per-transaction amount bounds are not maximum supply.
- Preserve exact integer atomic units. Do not convert RPC decimal tokens through
  binary floating-point or assume all APIs encode amounts in the same shape.
- Wcash AuxPoW and parent-chain inclusion are separate claims. A child winner
  need not meet the Zcash parent target.
- `wcash-wallet` in this workspace is an experimental local tool. It is not the
  desktop/mobile app or `wcash-cli` from another repository.
- Mining job creation, wallet initialization/sync, signing, broadcasting,
  candidate retirement and payout commands have side effects. A command called
  a "preflight" or a network request using HTTP POST is not inherently read-only.
- Keep cookie files, seeds, keys, wallet databases and private payout metadata
  out of prompts, commits, logs and examples. Test with disposable local state.
- RPC cookie authentication is not a read-only permission. Do not give an agent
  broad credentials when a restricted read-only adapter will suffice.
- Never delete databases, reset journals or create replacement signed
  transactions to recover from an uncertain operation. Follow the documented
  recovery protocol and obtain operator approval.

## Security reports

Use [SECURITY.md](SECURITY.md) for suspected vulnerabilities. Do not publish
exploit details in issues, PRs or documentation. Reproduce with the exact Wcash
revision and safe local fixtures; distinguish confirmed behavior from a
hypothesis. If unmodified upstream software is also affected, coordinate its
private disclosure separately. Upstream maintainers are not responsible for
Wcash releases.

## AI disclosure and authorship

Disclose the AI tool and its scope in the PR description. The human contributor
remains responsible for understanding and reviewing the change. Do not add
`Co-Authored-By` tags for AI tools or generated-by footers to commits.

## Project Structure & Module Organization

Wolf is a Rust workspace derived from Zebra. Main crates include:

- `zebrad/` (node CLI/orchestration),
- core libraries like `zebra-chain/`, `zebra-consensus/`, `zebra-network/`, `zebra-state/`, `zebra-rpc/`,
- Wcash tooling: `wcash-genesis/`, `wcash-zcash-aux/`, `wcash-merge-miner/`, and `wcash-wallet/`,
- support crates like `zebra-node-services/`, `zebra-test/`, `zebra-utils/`, `tower-batch-control/`, and `tower-fallback/`.

Code is primarily in each crate's `src/`; integration tests are in `*/tests/`; many unit/property tests are colocated in `src/**/tests/` (for example `prop.rs`, `vectors.rs`, `preallocate.rs`). Wcash guides are indexed in `docs/README.md`; inherited documentation is in `book/` and architecture decisions in `docs/decisions/`. CI and policy automation live in `.github/workflows/`.

## Build, Test, and Development Commands

Use the pinned toolchain and committed lockfile. For Rust changes, all required
formatting, lint and test checks must pass before promotion:

```bash
# Optional full build check
cargo build --workspace --locked

# Required checks for Rust changes
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked

# Run a single crate's tests
cargo test -p zebra-chain
cargo test -p zebra-state

# Run a single test by name
cargo test -p zebra-chain -- test_name

# CI-like nextest profile (unit + integration, excludes stateful and E2E)
cargo nextest run --profile ci --locked --release --features default-release-binaries --run-ignored=all

# Zebrad test category and GCP profile examples are maintained in zebrad/tests/main.rs.
```

For Wcash behavior, also run the applicable feature-specific tests in
[the Wcash release gate](.github/workflows/wcash-release-gate.yml). Default
workspace checks alone do not establish Wcash profile coverage. Local real-solver
E2E scripts are release checks with CPU and state side effects; do not launch
them against production services or without an appropriate local test environment.

For documentation-only changes, check changed Markdown with the repository
[lint configuration](.trunk/configs/.markdownlint.yaml) and
[spelling configuration](.codespellrc), resolve relative links and anchors,
parse configuration/request examples, and compare every protocol claim to source.
Record any unexecuted examples or baseline build failures; do not claim a passing
test or audited release. A reviewer decides whether additional checks are needed.

## Commit & Pull Request Guidelines

- PR titles must follow [conventional commits](https://www.conventionalcommits.org/en/v1.0.0/#specification) (PRs are merged with a merge commit — the PR title becomes the merge commit message)
- Branch commits are preserved in history, so each commit message must be meaningful and follow conventional commits too — release-plz reads the commits that land on `main` when it picks the next version
- A breaking change needs the `!` marker on the PR title _and_ on the branch commit that introduces it: the PR gate reads the title, release-plz reads the commits
- Do not add `Co-Authored-By` tags for AI tools, in _any_ commit on the branch — every one of them is preserved on `main`, not just the PR title
- Do not add "Generated with [tool]" footers, in any commit on the branch
- Use `.github/pull_request_template.md` and include motivation, solution summary, test evidence, an issue link when one exists (otherwise the direct maintainer request), and AI disclosure.
- For user-visible changes, add a `.changes/unreleased/` fragment per the [Changelog Guidelines](book/src/dev/changelog-guidelines.md); do not edit generated changelogs.

## Project Overview

Wolf implements Wcash node profiles alongside isolated inherited Zcash code.
Node validation and synchronization remain central. The workspace additionally
contains Wcash AuxPoW, genesis and experimental wallet/operator tooling; public
wallet apps, explorer and pool account services are separate projects.

- **Rust edition**: 2021
- **MSRV**: 1.88 (libraries), 1.91 (zebrad binary)
- **Database format version**: defined in `zebra-state/src/constants.rs`

## Crate Architecture

```text
zebrad (CLI orchestration)
  ├── zebra-consensus (block/transaction verification)
  │     └── zebra-script (script validation via FFI)
  ├── zebra-state (finalized + non-finalized storage)
  ├── zebra-network (P2P, peer management)
  └── zebra-rpc (JSON-RPC + gRPC)
        └── zebra-node-services (service trait aliases)
              └── zebra-chain (core data types, no async)
```

**Dependency rules**:

- Dependencies flow **downward only** — lower crates must not depend on higher ones
- `zebra-chain` is **sync-only**: no async, no tokio, no Tower services
- `zebra-node-services` defines service trait aliases used across crates
- `zebrad` orchestrates all components but contains minimal logic
- Utility crates: `tower-batch-control`, `tower-fallback`, `zebra-test`

### Per-Crate Concerns

| Crate | Key Concerns |
| --- | --- |
| `zebra-chain` | Serialization correctness, no async, consensus-critical data structures |
| `zebra-network` | Protocol correctness, peer handling, rate limiting, DoS resistance |
| `zebra-consensus` | Verification completeness, error handling, checkpoint vs semantic paths |
| `zebra-state` | Read/write separation (`ReadRequest` vs `Request`), database migrations |
| `zebra-rpc` | zcashd compatibility, error responses, timeout handling |
| `zebra-script` | FFI safety, memory management, lifetime/ownership across boundaries |

## Coding Style & Naming Conventions

- Rust 2021 conventions and `rustfmt` defaults apply across the workspace (4-space indentation).
- Naming: `snake_case` for functions/modules/files, `CamelCase` for types/traits, `SCREAMING_SNAKE_CASE` for constants.
- Respect workspace lint policy in `.cargo/config.toml` and crate-specific lint config in `clippy.toml`.
- Keep dependencies flowing downward across crates; maintain `zebra-chain` as sync-only.

## Code Patterns

### Tower Services

All services must include these bounds:

```rust
S: Service<Req, Response = Resp, Error = BoxError> + Send + Clone + 'static,
S::Future: Send + 'static,
```

- `poll_ready` must check all inner services
- Clone services before moving into async blocks

### Error Handling

- Use `thiserror` with `#[from]` / `#[source]` for error chaining
- `expect()` messages must explain **why** the invariant holds, not what happens if it fails:

  ```rust
  .expect("block hash exists because we just inserted it")  // good
  .expect("failed to get block")                            // bad
  ```

- Don't turn invariant violations into misleading `None`/default values

### Numeric Safety

- External/untrusted values: use `saturating_*` / `checked_*` arithmetic
- All `as` casts must have a comment explaining why the cast is safe

### Async & Concurrency

- CPU-heavy work (crypto, proofs): use `tokio::task::spawn_blocking`
- All external waits need timeouts (network, state, channels)
- Prefer `tokio::sync::watch` over `Mutex` for shared async state
- Prefer freshness tracking ("time since last change") to detect stalls

### Security

- Use `TrustedPreallocate` for deserializing collections from untrusted sources
- Bound all loops/allocations over attacker-controlled data
- Validate at system boundaries (network, RPC, disk)

### Performance

- Prefer existing indexed structures (maps/sets) over scanning/iterating
- Avoid unnecessary clones — structs may grow in size over time
- Use `impl Into<T>` to reduce verbose `.into()` at call sites
- Don't add unnecessary comments, docstrings, or type annotations to code you didn't change

## Testing Guidelines

- **Unit/property tests**: `src/*/tests/` within each crate (`prop.rs`, `vectors.rs`, `preallocate.rs`)
- **zebrad tests**: `zebrad/tests/main.rs` is the canonical source for test tiers and local `cargo nextest` examples.
- **Adding new zebrad tests**: Place the test in the appropriate module tier. No nextest config changes needed.
- Async tests: `#[tokio::test]` with timeouts for long-running tests
- Test configs must match real network parameters (don't rely on defaults)

```bash
# Unit tests (all crates)
cargo test --workspace

# zebrad unit + integration tests (default nextest profile excludes stateful and E2E)
cargo nextest run

# Zebrad test category and GCP profile examples are maintained in zebrad/tests/main.rs.
```

## Metrics & Observability

- Metrics use dot-separated hierarchical names with existing prefixes: `checkpoint.*`, `state.*`, `sync.*`, `rpc.*`, `peer.*`, `zcash.chain.*`
- Use `#[instrument(skip(large_arg))]` for tracing spans on important operations
- Errors must be logged with context

## Changelog

- Changelogs are generated by [changie](https://changie.dev) — never edit `CHANGELOG.md` or a crate's `CHANGELOG.md` by hand, they are regenerated from `.changes/`
- Add a change fragment instead, one `-j` per affected project: `changie new -j zebrad -k Added -b "..."`, or `changie new -j zebra-chain -j zebra-state -k breaking -b "..."` for a change spanning crates
- Kinds are `breaking` (`Breaking Changes`), `Added`, `Changed`, `Deprecated`, `Removed`, `Fixed`, `Security`
- See the [Changelog Guidelines](book/src/dev/changelog-guidelines.md) for detailed formatting rules

## Configuration

```rust
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    /// Documentation for field
    pub field: Type,
}
```

- Use `#[serde(deny_unknown_fields)]` for strict validation
- Use `#[serde(default)]` for backward compatibility
- All fields must have documentation
- Defaults must be sensible for production
