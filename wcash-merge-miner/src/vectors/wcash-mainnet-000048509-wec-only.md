# Public WEC-only AuxPoW block

`wcash-mainnet-000048509-wec-only.txt` preserves the exact serialized public
Wcash Mainnet block at height **48,509**, observed canonical on
**2 November 2026 at 19:05:28 UTC**. Its block ID is
`3c36883dd11261485149b5943b3d388a46a6263bf4395a6045c8a803f0c899aa`.
The source node was on height 48,511; repeated height/hash reads established
the collection-time canonical fence. The accompanying JSON retains the public
collection and classification evidence. This observation is historical.

Collection used only bounded read-only `getblockchaininfo`, `getblockhash`, and
`getblock` RPCs against an existing node. No block, transaction, wallet, payout,
accounting record, seed, credential, or test work was created or changed. Only
public block bytes are included.

The original child target is `0x1d0e8947`; the original parent header advertises
`0x1c008620`. The unchanged parent header hash in display order is
`00000001bb252d1801db683f3ead9754f292a25b90f4e81b67f82259108ac8ef`: it
meets the WEC target and misses the original ZEC target. The captured parent
coinbase encodes ZEC height **3,539,592**. No target, nonce, time, commitment,
transaction, header, solution, or proof byte was adjusted to get this result.

The regression parses the exact block and coinbase, checks the recorded WEC
block ID and heights, and calls the real AuxPoW validator for commitment,
Equihash `(200, 9)`, and original child-target checks. It then verifies the
original parent-target miss before testing rejection from late ZEC recovery.
The runtime authority, journal genesis, payout configuration, and clock remain
modeled actor fixtures. This test does not establish current canonical parents,
contextual ZEC block validity, miner hardware installation, or entitlement to a
real reward.

The canonical block contains **29,913 bytes**. Its raw-byte SHA-256 is
`28079d3d286480baedc4dffd230a184fd730b04ea4879eb1b0275877f49ceacd`;
the vector text SHA-256 including newline is
`07140b3f938abba539601546bdd4f66806b489c6ab4e6ad2fafec25fd271479d`.
