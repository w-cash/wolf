# Synthetic exact dual-winner vector

`wcash-valid-dual-auxpow-nonzero-parent.txt` contains one canonical Wcash block
with a real Equihash `(200, 9)` AuxPoW parent that meets both encoded targets.
The parent has a nonzero predecessor and timestamp, so its exact header can
also form a valid pool protocol job descriptor. No protocol field is replaced
with a placeholder in the regression fixture.

This is a public synthetic test vector, not a canonical Mainnet or Regtest
block. It validates proof/framing/metadata binding and scheduler isolation.
It does not establish full contextual consensus acceptance, independent public
receipt, live hardware-share validation, or entitlement to a real reward.
No production records, RPC endpoints, wallet state, addresses, or keys were used.

## Generation inputs

Generation used the existing public
`wcash-valid-auxpow-block-nonzero-prev.txt` child block unchanged except for its
AuxPoW witness; its proof-independent child block ID is unchanged. The predecessor, transaction
bytes, funding outputs, target, height, and payout metadata were preserved.

Construct `PreparedJob::from_wcash_header` with:

```rust,ignore
JobConfig {
    previous_block_hash: [0x55; 32],
    timestamp: 1,
    ..JobConfig::default()
}
```

The remaining explicit defaults are parent height `1`, version `4`, chain-history
root `[0; 32]`, advertised bits `0x207fffff`, auxiliary nonce `0`, empty parent
payout-output list, and coinbase extra data `Wcash/ZcashAuxPoW/local/v1`.
Run the real solver `job.solve(0, 8)` once in a bounded, offline process. The
committed result uses nonce `6` encoded little endian into the first eight bytes
of a 32-byte nonce, with the remaining bytes zero. Attach the verified proof
with `job.attach_to_header`, preserve the child's transactions, and serialize
the resulting child block using `ZcashSerialize::zcash_serialize_to_vec`.
The parent block is the proof's exact 1487-byte parent header, CompactSize `1`,
and exact coinbase bytes. Both Merkle branches are empty.

Generation and strict Equihash/dual-target verification ran on a workstation,
with one solver process, two build jobs, nice level 10, and a 180-second total
timeout. Build plus generation completed in approximately 60 seconds; the
tests consume only the static bytes and never run the solver.

## Exact identities

All hashes below use the stated byte order; SHA-256 digests are uninterpreted.

| Fact | Value |
| --- | --- |
| Child block ID, raw little endian | `8163ae22e4bc8fd73abccb39b9a7aa969950efef2f7df879ca6592f11fe8a6d4` |
| Parent block hash, raw little endian | `df57500b30821a707c59e5e653632ce2d7246003ccc3069bd0f8a155ea2f7e0d` |
| Canonical child bytes | `1925` |
| Canonical parent bytes | `1626` |
| SHA-256 of child block bytes | `639b920aa536727f57006b4f07d62bccdf0228f67f4e4af41af8b513e8691298` |
| SHA-256 of vector text including newline | `094fc208e3203a147319720a391fe38ff15f4878e21701ff211994e7c4706a71` |
| SHA-256 of original child vector text | `a16d9e17141c426c40dbcdd9355827fbc7670d90e92017d4c5890869d7cece76` |
