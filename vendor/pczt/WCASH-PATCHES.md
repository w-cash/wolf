# Wcash downstream patches

This directory vendors `pczt` 0.9.3 from `zcash/librustzcash` because the
upstream crate exhaustively matches the closed `zcash_protocol::BranchId` enum.
The source comes from upstream commit
`202f173b36f5eca9a7adbe1010f41bd8b8bb4535`; the crates.io archive SHA-256 is
`a5592f4f3eba7f9344cc423f45b6e65911e0630c9b89968d5a20792aadd5a0eb`.
The upstream manifests, README, changelog, tests, and dual MIT/Apache-2.0
license texts are retained.

The minimal downstream patch treats `BranchId::WcashTestnetV1` and the distinct
`BranchId::WcashRegtestV1` as V6 transaction domains and permits V6
deferred-anchor updates for them. It does not change any Zcash branch behavior
or PCZT encoding. Tests assert that the exact selected Wcash branch ID is
retained in the PCZT global data.
