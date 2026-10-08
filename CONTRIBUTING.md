# Contributing

- [Contributing](#contributing)
  - [Running and Debugging](#running-and-debugging)
  - [Bug Reports](#bug-reports)
  - [Pull Requests](#pull-requests)
  - [AI-Assisted Contributions](#ai-assisted-contributions)
  - [When We Close PRs](#when-we-close-prs)
  - [Code Standards](#code-standards)

## Running and Debugging

Start with the [documentation index](docs/README.md), the
[node operator guide](docs/wcash-node.md), or the
[isolated local guide](docs/wcash-local.md) for Regtest. The upstream
[Zebra documentation](https://zebra.zfnd.org/user.html) remains useful for
inherited build and instrumentation details, but its public-network parameters
do not apply to Wcash.

## Bug Reports

Use the [Wcash issue tracker](https://github.com/w-cash/wolf/issues) when Issues
are enabled. If they are disabled, coordinate non-sensitive work with a Wcash
maintainer through an existing relevant PR or a direct maintainer request; do
not open a Wcash issue in Zebra's tracker. Report security-sensitive issues
through the private process in [SECURITY.md](SECURITY.md).

## Pull Requests

PRs are welcome, but every PR requires human review time. To make that time count:

1. **Agree on scope.** Check existing work and obtain Wcash maintainer acknowledgment before unsolicited changes. A direct request from a Wcash maintainer is sufficient approval; describe that request in the PR without inventing an issue link.
2. **Coordinate consensus changes.** Discuss consensus, networking, genesis, monetary-policy, privacy, and merged-mining changes with Wcash maintainers before implementation. These changes require explicit test vectors and independent review.
3. **Keep PRs focused.** One logical change per PR. If you're planning multiple related PRs, discuss the overall plan with the team first.
4. **Follow conventional commits.** PRs are merged to main with a merge commit, so the PR title becomes the merge commit message and your branch commits are preserved in history. Follow the [conventional commits](https://www.conventionalcommits.org/en/v1.0.0/#specification) standard for both.
5. **Declare breaking changes.** If your change breaks a published crate's public Rust API, add `!` after the type and scope in the PR title (`feat(zebra-chain)!: ...`) **and** in the branch commit that introduces the break. The PR gate reads the title: that is what skips `semver-checks` and requires a `breaking` change fragment. release-plz reads the commits that land on `main`, which now include your branch commits, so the marker belongs on both for the release to bump the major version.

The consensus node should remain narrowly scoped, but Wcash's AuxPoW adapters,
pool interoperability tooling, wallet compatibility, and audit infrastructure
are in scope when they have clear ownership and test boundaries.

## AI-Assisted Contributions

We welcome contributions that use AI tools. What matters is the quality of the result and the contributor's understanding of it, not whether AI was involved.

**What we ask:**

- **Disclose AI usage** in your PR description: Specify the tool and what it was used for (e.g., "Used Claude for test boilerplate"). This helps reviewers calibrate their review.
- **Understand your code:** You are the sole responsible author. If asked during review, you must be able to explain the logic and design trade-offs of every change.
- **Don't submit without reviewing:** Review every line and run the checks appropriate to the change in [AGENTS.md](AGENTS.md). State failures and unexecuted checks explicitly; documentation-only checks do not certify node or wallet behavior.

Tab-completion, spell checking, and syntax highlighting don't need disclosure.

## When We Close PRs

Any team member may close a PR. We'll leave a comment explaining why and invite you to create an issue if you believe the change has value. Common reasons for closure:

- No acknowledged scope or direct maintainer request
- Feature or refactor nobody requested
- Low-effort changes (typo fixes, minor formatting) not requested by the team
- Missing test evidence or inability to explain the changes
- Out of scope for Wcash or incompatible with the reviewed consensus design

This is not personal; it's about managing review capacity. We encourage you to reach out first so your effort counts.

## Code Standards

Wcash retains Zebra's core engineering conventions. For architecture rules, code patterns, testing requirements, and security considerations, see [`AGENTS.md`](AGENTS.md). The key points:

- **Rust changes**: `cargo fmt`, `cargo clippy`, `cargo test` and applicable Wcash feature checks must pass before promotion; document blocked checks
- **Documentation changes**: Check Markdown, spelling, links, examples and source consistency
- **Architecture**: Dependencies flow downward only; `zebra-chain` is sync-only
- **Error handling**: Use `thiserror`; `expect()` messages explain why the invariant holds
- **Async**: CPU-heavy work in `spawn_blocking`; all waits need timeouts
- **Security**: Bound allocations from untrusted data; validate at system boundaries
- **Changelog**: Add an appropriate `.changes/unreleased/` fragment for user-visible changes; never edit generated changelogs by hand (see [Changelog Guidelines](book/src/dev/changelog-guidelines.md))
