# Wcash downstream patches

This directory vendors `zcash_protocol` 0.10.5 from the upstream
`zcash/librustzcash` release published on 2026-08-18. The upstream license files,
release changelog, original manifest, and README are retained beside the source.

Upstream provenance:

- repository: `https://github.com/zcash/librustzcash`
- upstream commit: `97aefdc39a037da9c4f19a0e8a450d2c7932f53e`
- crates.io archive SHA-256:
  `314329b91ec4bbb517441840e47d0b2029bf0b946f086980c96c889c2d92dc5d`

Wcash makes two consensus-critical extensions:

- `BranchId::WcashTestnetV1` is `0xb3cfd27e`. It is the first four bytes, in
  display order, of SHA-256 over the exact UTF-8 string
  `Wcash/NU6.3/Ironwood/v0` (full digest
  `b3cfd27eff03e141cd26208e3dca62c2e230efb03bde6a1589f61e57a1b48fd3`).
- The ID gives Wcash Testnet v1 a signature and transaction-hash domain that is
  distinct from Zcash NU6.3 (`0x37a5165b`) while retaining NU6.3 / Ironwood
  protocol semantics.
- `BranchId::WcashRegtestV1` is `0xc3a6678a`. It is the first four bytes, in
  display order, of SHA-256 over the exact UTF-8 string
  `Wcash/regtest/NU6.3/Ironwood/v1` (full digest
  `c3a6678a4f55e6cd8cb50c8d745bd06df2aa5d3fbf1c55301a2e8260fcf21c4d`).
  It separates disposable local Regtest transactions from Wcash Testnet as well
  as from Zcash.
- `Parameters::branch_id_for_upgrade` lets an independent network select its
  branch ID without changing any standard Zcash mapping.

These identifiers are frozen for Wcash Testnet v1 and local Regtest only. A
future Wcash mainnet must allocate and review a different branch ID; it must not
reuse either non-production ID.

The candidate was checked against the Zcash branch IDs known to this release,
the reserved development/test identifiers used by Zebra, and Wcash AuxPoW chain
and header identifiers. The test suite additionally asserts that standard Zcash
NU6.3 continues to resolve to `0x37a5165b`.
