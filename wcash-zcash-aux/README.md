# Wcash Zcash AuxPoW primitives

This crate defines Wcash Zcash-AuxPoW proof version 2: a bounded proof format,
an unambiguous coinbase carrier, and a fail-closed validation pipeline for
Wcash blocks secured by Zcash Equihash `(200, 9)` work.

Version 2 has exactly one carrier. The following 44 bytes are appended as the
exact suffix of the Zcash coinbase input's miner data (the bytes after its
canonical height):

```text
fabe6d6d || reverse(aux-root) || tree-size:u32-le || nonce:u32-le
```

No script push opcode wraps these bytes. Arbitrary pool-identification data may
precede the carrier. The `fabe6d6d` marker must occur exactly once in the whole
miner-data field and the carrier must end that field. Proof version 1 and its
transparent `OP_RETURN` carrier are not accepted.

The auxiliary leaf is not a bare Wcash block ID. It is SHA-256d over these
bytes:

```text
"Wcash/ZcashAuxPoW/leaf/v2\0" || 53414357 || raw-wcash-block-id
```

`53414357` is Wcash chain ID `0x57434153` encoded little-endian. The standard
AuxPoW marker, power-of-two tree size, nonce, and deterministic Namecoin slot
formula remain familiar to pool software, while domain and chain separation
prevent cross-chain leaf replay.

## Why the auth-data proof is required

In ZIP-244 transactions, the mined transaction ID does not bind the coinbase
input's authorizing data. The miner-data carrier is therefore authenticated in
two independent paths:

1. the canonical coinbase mined transaction ID follows a SHA-256d branch at
   index zero to the parent header's transaction Merkle root; and
2. the same coinbase's ZIP-244 authorizing-data digest follows a
   `ZcashAuthDatHash` branch at index zero to `hashAuthDataRoot`. The proof's
   chain-history root and that auth-data root reproduce the header's
   `hashBlockCommitments` using the `ZcashBlockCommit` construction.

Both paths must have the same depth. Version 2 accepts only canonical Zcash V5
and V6 coinbases, for which the pinned Zebra implementation calculates both
digests. Binding only the mined transaction ID would leave a consensus-critical
malleability gap.

## Proof encoding

Every proof is encoded in this exact order:

```text
"WCAZ"                                      4 bytes
version                                      u8 (= 2)
coinbase length                              CompactSize
canonical Zcash coinbase                     coinbase length bytes
transaction Merkle branch length             CompactSize
transaction Merkle branch                    32 bytes per node
coinbase transaction index                   u32-le (= 0)
auth-data Merkle branch length                CompactSize
auth-data Merkle branch                      32 bytes per node
coinbase auth-data index                     u32-le (= 0)
parent chain-history root                    32 bytes
auxiliary Merkle branch length                CompactSize
auxiliary Merkle branch                      32 bytes per node
auxiliary index                              u32-le
canonical Zcash Equihash (200,9) header       1,487 bytes
```

Decoding rejects unknown versions, non-minimal CompactSize forms, trailing
bytes, oversized fields, nonzero coinbase indexes, and mismatched parent-branch
depths before consensus validation.

## Consensus byte order

Every 32-byte Wcash block ID, Zcash transaction ID, digest, Merkle node, and
root uses **raw serialized order**. For SHA-256d identifiers this is
little-endian numeric order and is the reverse of the conventional big-endian
hexadecimal display used by Zebra, zcashd, RPCs, and block explorers. A pool
starting with displayed block-hash hex must decode it and reverse all 32 bytes.
Only the auxiliary root is reversed when placed in the 44-byte carrier.

The complete non-palindromic 1,806-byte proof is published in
[`test-vectors/auxpow-v2-non-palindromic.hex`](test-vectors/auxpow-v2-non-palindromic.hex)
and checked byte-for-byte in `src/proof.rs`. Its SHA-256 digest is
`d94e88abf717c604db33e42ea319f727bc072b86f7e7e0077d2ab5b7d40c18cf`.
The vector uses:

- raw Wcash block ID
  `000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f`;
- v2 leaf
  `b38059dd1ca081e4a23cd86d4743aa48ce241675c5e89a1692976f4f509a8c82`;
- raw auxiliary branches `20..3f` and `40..5f`, nonce `0x0d0c0b0a`,
  deterministic index `3`, and raw auxiliary root
  `8d5950951d66307c42153ca11df77463bb828206cc18a1711a24b0e346781b92`;
- exact miner-data suffix
  `fabe6d6d921b7846e3b0241a71a118cc068282bb6374f71da13c15427c30661d9550598d040000000a0b0c0d`;
- raw coinbase mined transaction ID
  `4de1adc9e0576890004dc1f70468fb06877dd5d64fb2a99fdeeb1bacab672ff0`;
- raw coinbase authorizing-data digest
  `065d4020b28e88546e8059198691348d3ab5175285bbdd26009e87e9420eed15`;
- raw auth-data root
  `57d3d3462d84ed7d862dd4660b5b1ab90af5992eada5a28a5b4db93cb2795644`;
  and
- raw parent block-commitments hash
  `c8c145287afb11de88a57e1f5efe5026855684f906dbfb7c8bcd4ea96b86b165`.

## Validation and integration

`AuxPowProof::validate()` performs all framing, canonical coinbase, transaction
Merkle, auth-data Merkle, block-commitments, carrier, authenticated Wcash
target, header hash, and full Equihash checks. The parent header's `nBits` is
diagnostic only; trusting it would let a miner choose Wcash difficulty.

Template builders should call `miner_data_commitment()` for a fixed `[u8; 44]`
or `commitment_payload()` for the same bytes as a `Vec<u8>`, append the result
to existing miner data, then use `validate_miner_data_commitment()` as a local
fail-closed assertion. `auth_data_merkle_root()` and
`block_commitments_hash()` expose the exact lower-level constructions needed by
native parent-template providers.

The profile caps complete proofs at 256 KiB, coinbases at 128 KiB, parent
transaction and auth-data paths at 32 levels each, and auxiliary trees at 16
levels. The normal Zcash coinbase-script consensus limit is considerably
tighter and is enforced by Zebra when parsing the supplied transaction.

This crate authenticates proof of work; it does not prove that the parent block
was accepted into the Zcash best chain. It also does not attach the proof to a
Wcash block encoding, choose the Wcash target for a height, construct pool jobs,
or activate a network rule. Those remain node-consensus and pool-integration
responsibilities.
