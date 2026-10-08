# Build and observe a Wcash node

Wolf is the Wcash node implementation in this repository. Its executable is
still named `zebrad`. A node downloads and validates the chain; running one does
not create a wallet, earn mining rewards, or give you WEC.

Choose the instructions for your role:

| Your goal | Start here |
| --- | --- |
| Understand Wcash or use a consumer wallet | [Wcash](https://w.cash/) and [Wcash Wallet](https://wcashwallet.com/); check the wallet's current platform and release limitations |
| Build a node and inspect Mainnet | Follow this guide |
| Reproduce mining or wallet experiments with valueless coins | [Local regtest guide](wcash-local.md) |
| Integrate a pool | [Pool compatibility](wcash-pool-compatibility.md) and [merged-mining design](wcash-merged-mining.md) |
| Use an AI agent to work in this repository | [Agent workflow](wcash-agents.md) |

This guide uses the current source's `WcashMainnet` profile. Mainnet is enabled
in source; old prelaunch statements in engineering documents must not be used
to select a network. Mainnet operation and successful local regtest experiments
are separate evidence: a regtest result does not establish production readiness.

## 1. Prepare and build the Wcash profile

The commands below use a Bash-compatible shell on Linux or macOS. Have Git,
curl, a C/C++ build toolchain, Clang/libclang, the Protobuf compiler (`protoc`),
and Rust available. The repository pins Rust **1.91.0** in
[`rust-toolchain.toml`](../rust-toolchain.toml). Use that toolchain and the checked-in
`Cargo.lock`. The native dependency setup used by Linux CI is recorded in
[`setup-zebra-build`](../.github/actions/setup-zebra-build/action.yml); the
[`Dockerfile`](../docker/Dockerfile) also records its build dependencies.
Native dependency installation varies by OS. This guide does not certify a
particular OS build or substitute for verifying a release's provenance.

For a new checkout:

```sh
git clone https://github.com/w-cash/wolf.git
cd wolf
```

If you already have a checkout, enter its root instead. Record the revision you
intend to run, inspect any local changes, and build the Wcash-only profile:

```sh
git rev-parse HEAD
git status --short
rustc --version
cargo --version

cargo build --locked --release -p zebrad --bin zebrad \
  --no-default-features --features wcash-consensus \
  --target-dir target/wcash-node

./target/wcash-node/release/zebrad --help
./target/wcash-node/release/zebrad --version
WOLF_REPO="$(pwd)"
```

`wcash-consensus` selects Wcash validation rules. An ordinary upstream Zcash
`zebrad` binary is not interchangeable with this build. The separate target
directory makes the chosen artifact explicit when both profiles are built in
one checkout. This command does not enable the internal miner.

The commands describe how to build; they are not a claim that a binary was
built or tested on your machine. A successful compilation also does not prove
that your node has synchronized with Mainnet.

## 2. Create a private observation configuration

Choose a new directory outside the checkout for the configuration, chain state,
peer cache, and local RPC credential. The following command deliberately fails
if the example directory already exists; inspect an existing setup instead of
overwriting it.

```sh
WOLF_NODE_DIR="$HOME/wolf-mainnet"
mkdir -m 700 "$WOLF_NODE_DIR"
cd "$WOLF_NODE_DIR"
```

Use a text editor to create `wcash.toml` inside that directory with this content:

```toml
[network]
network = "WcashMainnet"
listen_addr = "127.0.0.1:48233"
cache_dir = "peers"

[state]
cache_dir = "state"
ephemeral = false
delete_old_database = false

[rpc]
listen_addr = "127.0.0.1:48232"
cookie_dir = "rpc"
enable_cookie_auth = true
debug_force_finished_sync = false

[mining]
internal_miner = false

[tracing]
filter = "info"
```

The relative cache and cookie paths resolve from the **working directory**.
Always start this configuration from `WOLF_NODE_DIR`. State is persistent;
`delete_old_database = false` also retains old database versions during upgrades.
Budget disk space and investigate errors before removing any state.

This configuration accepts P2P connections only on loopback, while still making
outbound connections to Wcash peers. Omitting `initial_mainnet_peers` selects the
built-in Wcash bootstrap set, currently `node.w.cash:48233` and three IP endpoints
listed in [`zebra-network/src/config.rs`](../zebra-network/src/config.rs).
Those entries are bootstrap configuration, not a guarantee of availability or
independent operators. Do not substitute Zcash seeds or copy a Zcash peer cache.

JSON-RPC is available only to local clients and requires a cookie. No indexer,
lightwalletd gRPC, metrics, or health listener is enabled here. In particular,
RPC cookie authentication does **not** protect the separate lightwalletd gRPC
listener. Leave it disabled for this walkthrough. This is not a public RPC,
wallet-service, or pool deployment guide.

Configuration values can be overridden by `WCASH_SECTION__KEY` environment
variables. Before starting, check the _names_ of any exported Wcash variables:

```sh
env | cut -d= -f1 | sort | grep '^WCASH_'
```

No matches is normal. Review and remove unintended overrides in your shell;
do not print or share secret values. The `WOLF_REPO` and `WOLF_NODE_DIR` shell
variables used here are not node configuration overrides.

## 3. Start and stop the node

From the same shell used for the build:

```sh
cd "$WOLF_NODE_DIR"
"$WOLF_REPO/target/wcash-node/release/zebrad" \
  -c "$WOLF_NODE_DIR/wcash.toml" start
```

The node now contacts the public Wcash network and writes local chain state.
Keep this terminal open and watch for configuration, peer-discovery, and
validation errors. Synchronization time and storage needs depend on the chain
and your hardware; there is no fixed completion time in this guide.

To stop, press **Ctrl-C** in that terminal and wait for shutdown to finish.
Start with the same configuration and working directory to resume. Do not
run two nodes against the same state or cookie directory.

## 4. Query the authenticated local RPC

Open another terminal on the same computer. The node creates
`$HOME/wolf-mainnet/rpc/.cookie` when the JSON-RPC server starts. It contains a
local credential, not a wallet seed. Keep it private; do not paste it into
issues, chat, screenshots, or public service configuration. The cookie changes
when the server restarts.

Define this helper in the second terminal. It passes the credential through
curl's standard input instead of putting it in curl's command-line arguments.
It also bypasses proxy environment settings for this loopback request and
ignores any personal curl configuration file.

```sh
WOLF_NODE_DIR="$HOME/wolf-mainnet"

wcash_rpc() (
  set +x
  rpc_cookie="$(cat "$WOLF_NODE_DIR/rpc/.cookie")" || exit 1
  printf 'user = "%s"\n' "$rpc_cookie" |
    curl --disable --config - --noproxy '*' \
      --silent --show-error --fail \
      --connect-timeout 5 --max-time 30 \
      --header 'content-type: application/json' \
      --data-binary "$1" \
      http://127.0.0.1:48232
)
```

Do not add shell tracing or curl's verbose/trace options around credential
handling. These examples only read node state:

```sh
wcash_rpc '{"jsonrpc":"2.0","id":1,"method":"getblockhash","params":[0]}'
wcash_rpc '{"jsonrpc":"2.0","id":2,"method":"getblockchaininfo","params":[]}'
wcash_rpc '{"jsonrpc":"2.0","id":3,"method":"getnetworkinfo","params":[]}'
wcash_rpc '{"jsonrpc":"2.0","id":4,"method":"getpeerinfo","params":[]}'
```

Check the JSON-RPC `error` field as well as the HTTP result. An HTTP success
alone does not mean the RPC succeeded. During initial startup, a chain query
may not yet have a tip to return; inspect the node logs and retry after it has
initialized.

## 5. Verify identity, then synchronization

First verify that `getblockhash` for height zero returns this exact Mainnet
genesis hash:

```text
5bae12c8662a577b04ce1591af1a137c128f0cb51018a5f1622d861d1bb6fc48
```

It is frozen in [`zebra-chain/src/block/genesis.rs`](../zebra-chain/src/block/genesis.rs).
Wcash Mainnet's genesis timestamp is **19 September 2026, 12:00 UTC**, and its
external anchor is Zcash Mainnet block **3,488,810**. The anchor identifies the
launch boundary; Wcash has its own genesis, blocks, transactions, and balances.
It is not the Zcash chain.

Then inspect these `getblockchaininfo` fields:

| Field | What to check |
| --- | --- |
| `chain` | `main`; Zcash Mainnet also uses this label, so it cannot replace the genesis check |
| `consensus.nextblock` | `d9c6a7ee`, the current Wcash Mainnet V6 transaction domain |
| `consensus.chaintip` | `d9c6a7ee` once the tip is at height 1 or later; genesis precedes that activation |
| `blocks` and `bestblockhash` | Your locally validated tip; record both when comparing observations |
| `estimatedheight` and `verificationprogress` | Synchronization estimates, not proof of peer agreement or safety |
| `chainSupply` and `valuePools` | Aggregate chain values, not a wallet balance or a list of shielded recipients |

Use `getnetworkinfo.connections`, `getpeerinfo`, and the logs to check whether
peers are available. Sample the tip again later; a single unchanged reading is
not proof that the node is broken. Compare the same block height with another
node you operate or an external observation such as
[Wcash Explorer](https://wcashexplorer.com/). Short-lived tip differences can
occur during synchronization or reorganizations. A matching explorer reading
is a useful cross-check, not independent validation of its operator or backend.

Stop this walkthrough and investigate if the genesis or transaction domain is
wrong. Do not try to fix a network mismatch by disabling validation, forcing
the sync-ready flag, or mixing cache directories.

## 6. Keep experiments separate

`WcashMainnet`, `WcashTestnet`, and `WcashRegtest` are different profiles:

| Profile | Purpose | P2P / suggested local RPC ports |
| --- | --- | --- |
| `WcashMainnet` | Observe the live WEC chain | `48233` / `48232` |
| `WcashTestnet` | Engineering tests using valueless TWC | `38233` / `38232` |
| `WcashRegtest` | Isolated, local development using valueless TWC | `28233` / `28232` |

The feature-isolated build above does not include the internal miner required
by `wcash-local.toml`. For the separate multi-node mining and wallet experiment,
follow the [local regtest guide](wcash-local.md), which builds its own binaries
with [`scripts/build-wcash-testnet-binaries.sh`](../scripts/build-wcash-testnet-binaries.sh).
Read that procedure before executing it: mining, wallet, and end-to-end test
commands can write databases, create candidates, sign, and broadcast on their
configured networks. They are not observation commands.

Do not copy regtest's ephemeral state, disabled cookie authentication, synthetic
mining, or `debug_force_finished_sync` settings into a public-network deployment.
A checked-in Testnet profile is not proof that a public Testnet peer or service
is currently available.

## Troubleshooting and next steps

| Symptom | First check |
| --- | --- |
| Rust version or native-library build failure | Check the pinned toolchain, compiler/libclang, and `protoc`; preserve `Cargo.lock` rather than changing dependencies to hide the error |
| Configuration rejected or unexpected network | Check `wcash-consensus`, the explicit config path, and exported `WCASH_` override names |
| Connection refused | Check that the node is running and the RPC listener is `127.0.0.1:48232`; do not open firewall ports to repair a local request |
| Missing cookie or HTTP 401 | Check startup logs, working directory, and `cookie_dir`; read the current cookie instead of disabling authentication |
| No peers or tip stops advancing | Check connectivity, clock, bootstrap availability, peer results, and validation logs; preserve the state while diagnosing |
| Address already in use or database lock | Check for another process using the same listener or data directory before starting another instance |

When requesting help, include the source revision, build feature flags, OS,
network, error text, and observation time. Remove cookies, credentials, private
addresses, and unnecessary host details from logs. A node observation report
never needs a wallet seed or private key.

Continue with the [documentation index](README.md),
[Wcash project overview](https://w.cash/), or the
[Equihash merged-mining explanation](https://equihash.com/merged-mining).
The repository's node software, consumer wallets, pool operations, and external
websites have different roles; check the applicable release and service status
before relying on each one.
