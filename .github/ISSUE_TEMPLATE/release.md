---
name: "🚀 Wolf Release"
about: "Wcash maintainer use only"
title: "Publish next Wolf release: (version)"
labels: "A-release"
type: Task
assignees: ""
---

# Prepare for the Release

Record evidence for the exact Wolf commit and build features. An upstream Zebra
workflow, inherited version number, local Regtest result, or reachable endpoint
does not certify a Wcash release.

- [ ] Freeze the release scope and identify the Mainnet, Testnet, and Regtest
      behavior changed by the release.
- [ ] Confirm every consensus, network, genesis, monetary-policy, privacy,
      mining, wallet, and state-format change has explicit Wcash review and
      applicable vectors.
- [ ] Run `.github/workflows/wcash-release-gate.yml` successfully on the release
      commit and retain the run URL and artifact hashes.
- [ ] Run applicable local real-solver, wallet recovery, full-sync, migration,
      multi-node, and reorg checks. Record which checks were not applicable or
      not run; do not substitute an upstream result.
- [ ] Verify release artifacts were built with the intended Wcash features and
      reproduce their genesis, transaction branch, signatures, checksums, and
      software bill of materials.
- [ ] Review `SECURITY.md`, open advisories, dependency/audit results, and every
      documented operational limitation before promotion.
- [ ] Update the canonical repository docs and affected website/service docs,
      preserving historical notes with an explicit supersession notice.

# Prepare and Publish the Release

- [ ] Obtain the required human approvals for the exact release commit.
- [ ] Confirm all required checks pass on the latest commit and review the
      generated release notes against `.changes/` fragments.
- [ ] Publish only through the reviewed Wolf release workflow; verify the tag,
      release assets, signatures, hashes, and public download links afterward.
- [ ] Record rollback, incident, and operator communication owners before
      advertising the release for value-bearing use.
