# Wcash Testnet payout signer boundary

`wcash-wallet` protocol version 1 is the isolated WEC signing boundary for the
pool settlement service. It is deliberately enabled only with
`--network testnet`; Wcash Regtest and the disabled mainnet cannot use these
commands.

The pool first obtains the exact public identity:

```console
wcash-wallet --network testnet --db /absolute/private/wallet.sqlite payout-identity
```

The pool commits that identity, an ordered non-empty allocation list, a minimum
of 100 confirmations, and a maximum fee into its own durable payout intent. It
then sends one protocol-version-1 JSON request on non-terminal standard input:

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
UUID, canonical Wcash Unified Address, `receiver_kind: "ironwood"`, positive
`amount_zat`, and at most 512 memo bytes encoded as canonical lowercase
`memo_hex`. UUIDs and 32-byte commitments must use canonical lowercase forms.

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
