---
name: "Wcash Hotfix Readiness"
about: "Review a Wolf hotfix without publishing it"
title: "fix: prepare Wolf hotfix (version)"
labels: "A-release"
assignees: ""
---

# Wcash hotfix readiness

This template collects review evidence only. Wolf's inherited Zebra release
automation is not an approved Wcash publication path. Do not publish crates,
tags, images, releases or binaries from this PR.

- [ ] Identify the affected Wcash release, exact source tag/commit and impact.
- [ ] Base the hotfix on the reviewed Wcash source being fixed; do not substitute
      an upstream Zebra tag or binary.
- [ ] Add the smallest safe fix, regression tests and an unreleased changelog
      fragment.
- [ ] Run the Wcash release gate and all checks applicable to the affected
      consensus, network, state, wallet, mining or RPC behavior.
- [ ] Reproduce the original failure and the fixed behavior with exact commands,
      features and artifact hashes.
- [ ] Obtain independent security/consensus review where applicable.
- [ ] Document operator impact, rollback and recovery without exposing private
      vulnerability details.

Publication requires a separately implemented and approved Wcash release
pipeline that builds the `wcash-consensus` profile and verifies its network
identity. Until that exists, hand the reviewed source commit to the maintainer;
do not repurpose inherited Zcash Foundation workflows or credentials.
