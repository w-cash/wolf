---
name: "🚀 Wolf Release"
about: "Wcash maintainer use only"
title: "Review Wolf release readiness: (version)"
labels: "A-release"
type: Task
assignees: ""
---

# Prepare Wcash Release Evidence

Record evidence for the exact Wolf commit and build features. An upstream Zebra
workflow, inherited version number, local Regtest result, or reachable endpoint
does not certify a Wcash release.

> [!IMPORTANT]
> Wolf does not currently have an enabled public-release pipeline. The inherited
> Zebra publication workflow is owner-gated and the inherited binary workflow
> does not build a Wcash release artifact. This issue can collect readiness
> evidence, but must not be used to publish crates, tags, images or binaries.

- [ ] Freeze the release scope and identify the Mainnet, Testnet, and Regtest
      behavior changed by the release.
- [ ] Confirm every consensus, network, genesis, monetary-policy, privacy,
      mining, wallet, and state-format change has explicit Wcash review and
      applicable vectors.
- [ ] Run `.github/workflows/wcash-release-gate.yml` successfully on the candidate
      commit and retain the run URL and artifact hashes.
- [ ] Run applicable local real-solver, wallet recovery, full-sync, migration,
      multi-node, and reorg checks. Record which checks were not applicable or
      not run; do not substitute an upstream result.
- [ ] Verify release artifacts were built with the intended Wcash features and
      reproduce their genesis, transaction branch, signatures, checksums, and
      software bill of materials.
- [ ] Review `SECURITY.md`, open advisories, dependency/audit results, and every
      documented operational limitation before promotion.
- [ ] Update the canonical repository documentation and preserve historical
      notes with an explicit supersession notice where needed.

# Dedicated Release-Pipeline Gate

- [ ] Implement and independently review a Wcash-specific release pipeline in a
      separate change. It must build with `wcash-consensus`, fail on a default
      Zcash profile, bind artifacts to the source commit, and verify network
      identity before publication.
- [ ] Review every destination and credential scope for crates, tags, GitHub
      releases, images and binary assets. Do not inherit Zcash Foundation
      publication targets or credentials.
- [ ] Add artifact signature, checksum, provenance, rollback, incident and
      operator-communication procedures.
- [ ] Obtain explicit maintainer approval before enabling any publication event
      or write permission in the Wcash repository.
