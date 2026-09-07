# Contributing

- [Contributing](#contributing)
  - [Running and Debugging](#running-and-debugging)
  - [Bug Reports](#bug-reports)
  - [Pull Requests](#pull-requests)
  - [AI-Assisted Contributions](#ai-assisted-contributions)
  - [When We Close PRs](#when-we-close-prs)
  - [Code Standards](#code-standards)

## Running and Debugging

Start with the [local Wcash guide](docs/wcash-local.md). The upstream
[Zebra documentation](https://zebra.zfnd.org/user.html) remains useful for
inherited build and instrumentation details, but its public-network parameters
do not apply to Wcash.

## Bug Reports

Please create a non-sensitive bug report in the
[Wcash issue tracker](https://github.com/w-cash/wolf/issues). Report
security-sensitive issues through the private process in [SECURITY.md](SECURITY.md).

## Pull Requests

PRs are welcome, but every PR requires human review time. To make that time count:

1. **Start with an issue.** Check the [Wcash issue tracker](https://github.com/w-cash/wolf/issues) for existing work or create an issue describing what you want to change and why.
2. **Coordinate consensus changes.** Discuss consensus, networking, genesis, monetary-policy, privacy, and merged-mining changes in a Wcash issue before opening a PR. These changes require explicit test vectors and independent review.
3. **Keep PRs focused.** One logical change per PR. If you're planning multiple related PRs, discuss the overall plan with the team first.
4. **Follow conventional commits.** PRs are squash-merged to main, so the PR title becomes the commit message. Follow the [conventional commits](https://www.conventionalcommits.org/en/v1.0.0/#specification) standard.
5. **Declare breaking changes.** If your change breaks a published crate's public Rust API, add `!` after the type and scope in the PR title (`feat(zebra-chain)!: ...`). The semver-checks gate requires it, and the same marker tells the release to bump the major version.

The consensus node should remain narrowly scoped, but Wcash's AuxPoW adapters,
pool interoperability tooling, wallet compatibility, and audit infrastructure
are in scope when they have clear ownership and test boundaries.

## AI-Assisted Contributions

We welcome contributions that use AI tools. What matters is the quality of the result and the contributor's understanding of it, not whether AI was involved.

**What we ask:**

- **Disclose AI usage** in your PR description: Specify the tool and what it was used for (e.g., "Used Claude for test boilerplate"). This helps reviewers calibrate their review.
- **Understand your code:** You are the sole responsible author. If asked during review, you must be able to explain the logic and design trade-offs of every change.
- **Don't submit without reviewing:** Run the full test suite locally and review every line before opening a PR.

Tab-completion, spell checking, and syntax highlighting don't need disclosure.

## When We Close PRs

Any team member may close a PR. We'll leave a comment explaining why and invite you to create an issue if you believe the change has value. Common reasons for closure:

- No linked issue, or issue exists but no team member has responded to it
- Feature or refactor nobody requested
- Low-effort changes (typo fixes, minor formatting) not requested by the team
- Missing test evidence or inability to explain the changes
- Out of scope for Wcash or incompatible with the reviewed consensus design

This is not personal; it's about managing review capacity. We encourage you to reach out first so your effort counts.

## Code Standards

Zebra enforces code quality through review. For the full list of architecture rules, code patterns, testing requirements, and security considerations, see [`AGENTS.md`](AGENTS.md). The key points:

- **Build requirements**: `cargo fmt`, `cargo clippy`, and `cargo test` must all pass
- **Architecture**: Dependencies flow downward only; `zebra-chain` is sync-only
- **Error handling**: Use `thiserror`; `expect()` messages explain why the invariant holds
- **Async**: CPU-heavy work in `spawn_blocking`; all waits need timeouts
- **Security**: Bound allocations from untrusted data; validate at system boundaries
- **Changelog**: Update `CHANGELOG.md` for user-visible changes (see [Changelog Guidelines](https://zebra.zfnd.org/dev/changelog-guidelines.html))
