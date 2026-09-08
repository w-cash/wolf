# Wcash downstream patches

This directory vendors `zcash_primitives` 0.30.1 from the upstream
`zcash/librustzcash` release published on 2026-08-18. The upstream license files,
release changelog, original manifest, and README are retained beside the source.

Upstream provenance:

- repository: `https://github.com/zcash/librustzcash`
- upstream commit: `97aefdc39a037da9c4f19a0e8a450d2c7932f53e`
- crates.io archive SHA-256:
  `403d5be1e96339534be098e3377fb8a78d68ca7585b1780133d884b810277418`

The downstream changes teach the transaction implementation about
`BranchId::WcashTestnetV1` from the adjacent vendored `zcash_protocol` crate:

- the branch uses NU6.3 / Ironwood bundle and circuit semantics;
- only transaction version 6 is valid for this Wcash branch, so every
  post-genesis transaction embeds its chain-specific branch ID on the wire;
- transaction hashes and signature hashes use the Wcash branch ID and therefore
  differ from otherwise identical Zcash NU6.3 transactions; and
- `Builder::new_with_branch_id` exposes a checked explicit-domain constructor for
  chain-boundary code. It rejects a caller-supplied ID unless it exactly matches
  the ID selected by the consensus parameters at the target height.

Standard Zcash branch mappings and transaction-version rules are unchanged.
