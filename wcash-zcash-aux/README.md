# Wcash Zcash AuxPoW primitives

This crate defines the bounded proof format and validation pipeline for Wcash
blocks merge-mined by Zcash Equihash `(200, 9)` work.

It deliberately has one parent algorithm and one carrier:

```text
OP_RETURN PUSH44 fabe6d6d reverse(aux-root) tree-size:u32-le nonce:u32-le
```

The auxiliary leaf is not a bare Wcash block hash. It is SHA-256d over the
fixed `Wcash/ZcashAuxPoW/leaf/v1\0` domain, Wcash chain ID `0x57434153`, and the
32-byte auxiliary block hash. The standard marker and slot formula remain
familiar to AuxPoW pool software while the leaf cannot be reused under a
different chain domain.

## Consensus byte order

Every 32-byte Wcash block ID, Zcash transaction ID, Merkle leaf, Merkle root,
and parent or auxiliary branch node uses **raw serialized order**. These are the
exact bytes stored in a Zcash header and concatenated by SHA-256d Merkle
hashing. For SHA-256d identifiers this is little-endian numeric order and is
the reverse of the big-endian hexadecimal display used by Zebra, zcashd, RPCs,
and block explorers. A pool starting with displayed hash hex must decode it and
reverse all 32 bytes before using this format. The chain ID is serialized as
little-endian `53414357` inside the auxiliary-leaf preimage.

The complete, exact 1,707-byte proof encoding is published in
[`test-vectors/auxpow-v1-non-palindromic.hex`](test-vectors/auxpow-v1-non-palindromic.hex)
and checked byte-for-byte in `src/proof.rs`. Its SHA-256 digest is
`38d7a19f0627a850b76f77920e7422e54935001b5ce50bc3372e8193226f556b`.
The vector's raw Wcash block ID is
`000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f`,
its leaf is
`02738bd4cbffaecb1ac3265b1dccb2018cd2b4d64277a41509734167c0b28257`,
its raw auxiliary branch is `20..3f`, then `40..5f`, its nonce is
`0x0d0c0b0a`, its deterministic index is `3`, its raw auxiliary root is
`9b48d0cb7ba1925f0ef43d4c401d50a6f129c8aa98a032db880df56285a84cc2`,
and its exact commitment script is
`6a2cfabe6d6dc24ca88562f50d88db32a098aac829f1a6501d404c3df40e5f92a17bcbd0489b040000000a0b0c0d`.

Complete validation checks:

1. proof magic/version, canonical CompactSize encoding, fixed header size, and
   explicit allocation limits;
2. an exact canonical Zcash coinbase parsed by the pinned Zebra transaction
   implementation, including its current version-specific mined txid;
3. coinbase position zero and its SHA-256d path to the parent header's
   transaction Merkle root;
4. exactly one exact zero-value transparent commitment output, including the
   auxiliary root, power-of-two tree size, and deterministic Wcash slot; and
5. the SHA-256d parent header hash against a target supplied by authenticated
   **Wcash** chain state, followed by the full Equihash `(200, 9)` solution.

The parent header's `nBits` field is diagnostic data only. Trusting it would let
a miner choose Wcash difficulty, so it never authorizes work here.

`validate()` uses the source tree's Zebra coinbase codec and the real Equihash
implementation. Explicit adapter hooks exist for independent interoperability
testing; a consensus caller must not substitute permissive adapters.

This crate does not attach the proof to a Wcash block encoding, select the
Wcash difficulty for a height, construct pool jobs, or activate a network rule.
Those are node-consensus and pool integration responsibilities.
