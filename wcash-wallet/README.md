# Experimental Wcash wallet

`wcash-wallet` is a local operator and developer tool in the Wolf workspace. It
derives Wcash addresses, validates network-specific addresses, initializes and
synchronizes one SQLite wallet account, inspects balances, creates and persists
signed Wcash V6 transactions, shields mature transparent coinbase funds, and
supports the versioned pool-payout protocol.

It is not the consumer desktop/mobile wallet, a browser wallet, a custody
service, or a general remote wallet API. Treat it as experimental software and
verify the exact source revision, network and endpoint before using value-bearing
keys or funds.

## Build and inspect the interface

Use the repository's pinned Rust toolchain and lockfile:

```sh
cargo build --locked --release -p wcash-wallet --bin wcash-wallet
./target/release/wcash-wallet --help
./target/release/wcash-wallet --network mainnet payout-capabilities
```

Every invocation requires an explicit `--network mainnet`, `testnet`, or
`regtest`. Mainnet, Testnet and Regtest addresses, transaction domains and
databases are not interchangeable. Regtest payout commands require a separate
build with `--features regtest-payout`; that feature is for isolated tests, not
production policy.

## Authority and side effects

| Command group | Authority and effect |
| --- | --- |
| `validate-address`, `payout-capabilities` | Parses standard input or reports compiled capabilities; no wallet database or network connection |
| `derive-address` | Reads a seed from redirected standard input and prints derived public addresses; does not create a database |
| `derive-collector` | Reads a seed and creates a new owner-private incoming-viewing-key file at the requested absolute path |
| `balance`, `export`, `list-pending`, `payout-identity`, `payout-recover`, `payout-inspect` | Opens private wallet state for inspection or durable-operation recovery; keep a single writer and review each command's input contract |
| `init`, `sync`, `status` | Connects to the selected compact-block endpoint; `init` and `sync` write wallet state |
| `payout-observe` | Opens private wallet state and contacts the selected endpoint to create a short-lived, tip-bound observation |
| `shield-coinbase`, `transfer`, `payout-sign` | Uses spending authority, constructs signed transactions and persists operation state; it can also reserve inputs or notes |
| `broadcast`, `payout-broadcast` | Sends exact signed transaction bytes to the configured endpoint |

Run `wcash-wallet --network <network> <command> --help` for the current command
contract. A command named `observe`, `inspect`, or `status` can still open
private state or contact a service; command names are not an access-control
boundary.

## Keys, databases and endpoints

General seed-taking commands refuse interactive terminal input and accept one
bounded hexadecimal seed through redirected standard input. Do not place seeds
in command-line arguments, environment variables, shell history, logs, issue
reports, prompts or committed fixtures. The pool signer additionally supports
a strictly checked owner-private seed file or a bounded binary input frame; see
the [payout protocol](../docs/wcash-wallet-payout.md) instead of inventing an
adapter.

Use an absolute, owner-private database path and maintain one exclusive writer.
Do not copy a live database while it is open. Signed operations are persisted so
an ambiguous result can be recovered and the same bytes inspected or retried;
do not create a replacement transaction merely because a response was lost.

Commands that access chain data require `--lightwalletd`. The client accepts
HTTPS or plaintext on a literal loopback address for local development. It
attests the selected Wcash genesis and transaction domain, but that does not make
an arbitrary endpoint trusted, private, synchronized or production-ready. This
repository does not designate a public wallet endpoint.

Successful commands emit JSON on standard output. A rejected operation exits
non-zero and emits one JSON object on standard error with `protocol_version`,
`code`, and `error`. The stable code categories are `rejected`, `unavailable`,
`idempotency_conflict`, and `ambiguous`. Automation must check the process exit
status before parsing the corresponding stream and must treat `ambiguous` as a
recovery/reconciliation requirement, not permission to create a replacement
transaction.

## Minimal safe discovery

Address validation does not need a database, seed or network request. This
valueless Testnet golden vector has an all-zero receiver payload and must never
be used to receive funds:

```sh
printf '%s\n' 'WT6kWkxJzyp4LdwrjtvvuVFRbkMhH2SsBeq' | \
  ./target/release/wcash-wallet --network testnet validate-address
```

For wallet initialization, first create a private directory and a reviewed seed
file outside the repository. The command below contacts the selected service and
writes the database; replace every path and endpoint deliberately:

```sh
./target/release/wcash-wallet \
  --network mainnet \
  --db /absolute/private/wcash-wallet.sqlite \
  --lightwalletd https://reviewed.example.invalid:443 \
  init < /absolute/private/seed.hex
```

The reserved `.invalid` endpoint is intentionally non-routable and the example
must not be run unchanged. Never weaken TLS or network-identity checks to make an
unknown service connect.

## Related documentation

- [Protocol direction](../docs/wcash-direction.md)
- [Consensus identities and active pools](../docs/wcash-consensus.md)
- [Local disposable Regtest exercise](../docs/wcash-local.md)
- [Pool payout, recovery and custody boundary](../docs/wcash-wallet-payout.md)
- [Agent and read-only integration guidance](../docs/wcash-agents.md)

For exact CLI behavior, inspect [`src/main.rs`](src/main.rs) at the same revision
as the binary. Source capability is not evidence that a public wallet service or
consumer release exists.
