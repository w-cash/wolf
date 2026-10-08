# Wcash payout signer boundary

`wcash-wallet` protocol version 2 is the isolated WEC signing boundary for the
pool settlement service. It supports explicitly selected Mainnet and Testnet networks. Regtest payout
commands require the `regtest-payout` build feature. No consensus rules change.

The pool first obtains the exact public identity:

```console
wcash-wallet --network testnet --db /absolute/private/wallet.sqlite payout-identity
```

Before creating a payout batch, the pool obtains a short-lived reconciliation
observation from the same protected wallet and its loopback Wcash compact-block
service:

```console
wcash-wallet \
  --network testnet \
  --db /absolute/private/wallet.sqlite \
  --lightwalletd http://127.0.0.1:38234 \
  payout-observe
```

The single JSON response contains `protocol_version`, `network`,
`genesis_hash`, `branch_id`, `account_id`, `collector_payout_commitment`,
`fund_source`, `synchronized`, `wallet_state_digest`, `wallet_spendable_zat`,
`best_tip_hash`, `best_tip_height`, `observed_at`, and `valid_until`.
`collector_payout_commitment` is lowercase hex encoding of
`SHA256("Wcash/Wcash child payout address/v1\0" || canonical Ironwood UA)`;
the address and viewing keys are not exposed. `best_tip_hash` is
lowercase hex in compact-block/wire byte order. `wallet_spendable_zat` is only
confirmed, unlocked Ironwood value; transparent or legacy value is never
reported as spendable payout authority.

The wallet holds its shared operation lock while it compares the stored fully
scanned tip with a latest-block response, an exact block-at-height response,
and a second latest-block response. Any unsynchronized wallet, incomplete
transparent-recovery marker, zero-height tip, account ambiguity, legacy-pool
balance, endpoint error, or tip mismatch fails without producing an
observation. The response is valid for four minutes. Its domain-separated
BLAKE2b-256 state digest binds the schema version, Wcash network/genesis/branch,
account UUID, exact collector commitment, Ironwood funding mode, synchronized
flag, spendable value, and exact tip; timestamps are deliberately excluded so
unchanged state has a stable digest.

This is an integrity snapshot, not a remote consensus oracle. Its chain
authority is the endpoint authenticated and network-attested by
`wcash-wallet`; production therefore uses a protected literal-loopback endpoint
backed by the pool's own Wcash node. The pool must also enforce expiry against
its database clock and must never accept operator-supplied replacements for any
response field.

The pool commits that identity, an ordered non-empty allocation list, a minimum
of 100 confirmations, and a maximum fee into its own durable payout intent. It
then sends one protocol-version-2 JSON request on non-terminal standard input:

```console
wcash-wallet \
  --network testnet \
  --db /absolute/private/wallet.sqlite \
  --lightwalletd https://attested-loopback-or-private-service \
  payout-sign --seed-file /run/credentials/wcash-payout.seed
```

The JSON input is bounded to 512 KiB and rejects unknown fields. It contains
`batch_id`, `request_commitment`, the exact `identity`, ordered `outputs`,
`confirmations`, and `max_fee_zat`. Each output contains a canonical allocation
UUID, canonical Wcash address, exact `receiver_kind`, positive `amount_zat`,
and canonical lowercase `memo_hex`. Ironwood supports at most 512 memo bytes.
Transparent P2PKH requires an empty memo. UUIDs and 32-byte commitments must use canonical lowercase forms.

The seed credential path may appear in process metadata, but its contents may
not. On Unix the file must be an absolute, lexically canonical, owner-owned
regular file with mode 0400 or 0600, no symlink, and exactly one hard link. The
seed is zeroized after use. Destinations, memos, and signed bytes travel only in
bounded standard input/output JSON, never command-line arguments or environment
variables.

The signer inserts the batch binding before proving and signing inside the same
SQLite transaction used by librustzcash to store the wallet transaction. It
commits the exact raw bytes, txid, raw SHA-256, effecting-data digest, fee,
heights, ordered outputs, and native request-facts digest atomically. A crash
before commit leaves neither batch nor wallet mutation. A crash after commit is
recovered by `batch_id` and `request_commitment`; it never signs a replacement.
The same batch ID with any changed commitment or signing fact is a fatal
conflict.

Recovery and independent byte inspection accept bounded JSON on non-terminal
standard input:

```console
wcash-wallet --network testnet --db /absolute/private/wallet.sqlite payout-recover
wcash-wallet --network testnet --db /absolute/private/wallet.sqlite payout-inspect
```

`payout-recover` accepts only `batch_id` and `request_commitment`.
`payout-inspect` additionally requires the exact `txid` and
`raw_transaction_hex`; it returns the complete durable binding only when every
byte matches. Inspection and broadcast JSON are bounded to 4.5 MB.

Broadcast uses the same inspection request and an attested endpoint:

```console
wcash-wallet \
  --network testnet \
  --db /absolute/private/wallet.sqlite \
  --lightwalletd https://attested-loopback-or-private-service \
  payout-broadcast
```

The result is exactly one of `accepted`, `already_known`, `rejected`, or
`ambiguous`. Transport failures, timeouts, unavailable status, and protocol
uncertainty are ambiguous; retry the same stored bytes. An authoritative node
rejection is returned only after the exact txid is still absent. The pool must
not authorize a replacement merely because broadcast acknowledgement was lost.

This component does not calculate balances from shares, choose eligible miners,
freeze allocations across reorgs, hold portal credentials, or expose an
Internet-facing endpoint. Those remain responsibilities of the independently
reviewed pool ledger and its local authenticated adapter.

## Optional public P2PKH destinations

Shielded Ironwood (`wu1…` on Mainnet) remains recommended. A Mainnet W1 payment
exposes the destination and transferred amount. P2SH/W3, TEX, Zcash t-addresses,
wrong-network addresses and noncanonical encodings are rejected. Classification
uses the authoritative Wcash parser, never string prefixes.

Before enabling public payouts, the pool must pin the compatible wallet binary
and check its seedless capability response:

```console
wcash-wallet --network mainnet payout-capabilities
```

```json
{"protocol_version":2,"receiver_kinds":["ironwood","transparent_p2pkh"]}
```

The following output examples abbreviate addresses and are not executable
requests. Use actual canonical addresses from the chosen network:

```json
{"allocation_id":"00000000-0000-0000-0000-000000000001","canonical_address":"wu1…","receiver_kind":"ironwood","amount_zat":1000000,"memo_hex":""}
```

```json
{"allocation_id":"00000000-0000-0000-0000-000000000002","canonical_address":"W1…","receiver_kind":"transparent_p2pkh","amount_zat":1000000,"memo_hex":""}
```

Pass one address on standard input to `wcash-wallet --network mainnet
validate-address`. A successful W1 response has this shape (address abbreviated):

```json
{"network":"mainnet","receiver_kind":"transparent_p2pkh","canonical":"W1…"}
```

The declared kind must match both the parsed kind and canonical address. W1
outputs cannot carry shielded memos, including payout allocation memos. Inputs
and internal change remain Ironwood; V6, network/branch, expiry, exact fees,
nullifiers, output scripts/values/multiplicity and private change are checked.

Protocol version 2 and the journal schema remain unchanged: the existing
`transparent_p2pkh` JSON enum value is used. The canonical stored address uniquely
binds the kind and is reparsed during recovery. Existing Ironwood rows remain
recoverable; repeated requests recover the stored bytes. The compatibility test
`old_ironwood_v2_journal_recovers_after_upgrade_and_restart` creates an Ironwood
journal using the frozen writer from commit
`1ecc5a4d611ad0cc4aa97e8cf1417345a4368e15`, then reopens SQLite and recovers it
with the current code. The W1 proof/restart test exercises the current writer.
Both retry through a disconnected client and require identical signed bytes,
so recovery cannot silently create a replacement transaction.

Store payout addresses in the miner's pool account rather than ASIC password
fields. Never log seed phrases, seed material, full payout addresses or wallet
request bodies. Logs should use batch/allocation identifiers and masked addresses.
