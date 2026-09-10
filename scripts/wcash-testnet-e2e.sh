#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
runtime_dir="$(mktemp -d "${TMPDIR:-/tmp}/wcash-testnet-e2e.XXXXXX")"
declare -a child_pids=()

wcash_rpc=http://127.0.0.1:38232
zcash_template_rpc=http://127.0.0.1:18232
zcash_validator_rpc=http://127.0.0.1:18242
wcash_genesis=d95a9f2f1daf07d48fb3c863ad7334ec630a4a7077da98c8f7e65f8c0e277cf1
wcash_payout_address=WT6kWkxJzyp4LdwrjtvvuVFRbkMhH2SsBeq

cleanup() {
  local status=$1
  local pid

  trap - EXIT INT TERM
  for pid in "${child_pids[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill -INT "$pid" 2>/dev/null || true
    fi
  done
  for _attempt in {1..20}; do
    local running=false
    for pid in "${child_pids[@]}"; do
      if kill -0 "$pid" 2>/dev/null; then
        running=true
      fi
    done
    [[ "$running" == false ]] && break
    sleep 0.25
  done
  for pid in "${child_pids[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill -TERM "$pid" 2>/dev/null || true
    fi
  done
  for _attempt in {1..20}; do
    local running=false
    for pid in "${child_pids[@]}"; do
      if kill -0 "$pid" 2>/dev/null; then
        running=true
      fi
    done
    [[ "$running" == false ]] && break
    sleep 0.25
  done
  for pid in "${child_pids[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill -KILL "$pid" 2>/dev/null || true
    fi
    wait "$pid" 2>/dev/null || true
  done

  if [[ $status -eq 0 ]]; then
    rm -rf -- "$runtime_dir"
  else
    echo "Wcash Testnet E2E failed; logs retained at $runtime_dir" >&2
  fi
  exit "$status"
}
trap 'cleanup $?' EXIT
trap 'cleanup 130' INT TERM

for binary in zcash-zebrad wcash-zebrad wcash-merge-miner wcash-wallet; do
  if [[ ! -x "$repo_root/target/release/$binary" ]]; then
    echo "missing release binary: target/release/$binary" >&2
    exit 1
  fi
done

rpc_call_with_timeout() {
  local timeout_seconds=$1
  local url=$2
  local method=$3
  local params=${4:-[]}
  curl --fail --silent --show-error --max-time "$timeout_seconds" --noproxy '*' \
    --header 'content-type: application/json' \
    --data-binary "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"$method\",\"params\":$params}" \
    "$url"
}

rpc_call() {
  rpc_call_with_timeout 5 "$@"
}

rpc_result() {
  rpc_call "$1" "$2" "${3:-[]}" | python3 -c \
    'import json,sys; response=json.load(sys.stdin); assert response.get("error") in (None, False), response; print(response["result"])'
}

wait_for_rpc() {
  local url=$1
  local name=$2
  for _attempt in {1..60}; do
    if rpc_call "$url" getblockcount >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "$name RPC did not become ready" >&2
  return 1
}

wait_for_tcp() {
  local port=$1
  local name=$2
  for _attempt in {1..120}; do
    if python3 - "$port" <<'PY' >/dev/null 2>&1
import socket
import sys
with socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=0.2):
    pass
PY
    then
      return 0
    fi
    sleep 0.25
  done
  echo "$name did not become ready on port $port" >&2
  return 1
}

wait_for_height() {
  local url=$1
  local name=$2
  local expected=$3
  for _attempt in {1..240}; do
    if [[ "$(rpc_result "$url" getblockcount 2>/dev/null || true)" == "$expected" ]]; then
      return 0
    fi
    sleep 0.25
  done
  echo "$name did not reach height $expected" >&2
  return 1
}

"$repo_root/target/release/wcash-zebrad" \
  -c "$repo_root/scripts/wcash-testnet-ci.toml" start \
  >"$runtime_dir/wcash-testnet.log" 2>&1 &
child_pids+=("$!")
"$repo_root/target/release/zcash-zebrad" \
  -c "$repo_root/zcash-parent-template-local.toml" start \
  >"$runtime_dir/zcash-template.log" 2>&1 &
child_pids+=("$!")
"$repo_root/target/release/zcash-zebrad" \
  -c "$repo_root/zcash-parent-validator-local.toml" start \
  >"$runtime_dir/zcash-validator.log" 2>&1 &
child_pids+=("$!")

wait_for_rpc "$wcash_rpc" WcashTestnet
wait_for_rpc "$zcash_template_rpc" Zcash-template
wait_for_rpc "$zcash_validator_rpc" Zcash-validator
wait_for_tcp 38233 "Wcash Testnet P2P listener"

[[ "$(rpc_result "$wcash_rpc" getblockcount)" == 0 ]]
[[ "$(rpc_result "$wcash_rpc" getblockhash '[0]')" == "$wcash_genesis" ]]
rpc_call "$wcash_rpc" getnetworkinfo | python3 -c '
import json,sys
result=json.load(sys.stdin)["result"]
assert result["subversion"].startswith("/Wcash:"), result
'
rpc_call "$wcash_rpc" getblockchaininfo | python3 -c '
import json,sys
result=json.load(sys.stdin)["result"]
assert result["blocks"] == 0, result
assert result["bestblockhash"] == "d95a9f2f1daf07d48fb3c863ad7334ec630a4a7077da98c8f7e65f8c0e277cf1", result
assert result["chainSupply"]["chainValueZat"] == 0, result
assert all(pool["chainValueZat"] == 0 for pool in result["valuePools"]), result
'

# A valid address from the separate Wcash regtest namespace must not be usable
# on public Testnet, even though its receiver payload has the same shape.
wcash_regtest_address="$(sed -n "s/^export WCASH_PAYOUT_ADDRESS='\([^']*\)'/\1/p" "$repo_root/docs/wcash-local.md")"
rpc_call "$wcash_rpc" createauxblock "[\"$wcash_regtest_address\"]" \
  >"$runtime_dir/rejected-regtest-address.json"
python3 - "$runtime_dir/rejected-regtest-address.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as response_file:
    response = json.load(response_file)
assert response.get("result") is None, response
assert response.get("error"), response
message = response["error"]["message"].lower()
assert "address" in message and "regtest" in message and "test" in message, response
PY

# The RPC listener can become reachable before every state and proposal service
# is ready. Retry that bounded startup window without a debug sync override.
# This fresh isolated node has no submitted transactions, so its first Wcash
# Testnet template contains only coinbase. The template path still queries the
# normal mempool and can include valid Wcash-domain V6 transfers.
aux_template_ready=false
for _attempt in {1..60}; do
  if rpc_call "$wcash_rpc" createauxblock "[\"$wcash_payout_address\"]" \
    >"$runtime_dir/createauxblock.json" &&
    python3 - "$runtime_dir/createauxblock.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as response_file:
    response = json.load(response_file)
raise SystemExit(0 if response.get("result") else 1)
PY
  then
    aux_template_ready=true
    break
  fi
  sleep 1
done
if [[ "$aux_template_ready" != true ]]; then
  echo "Wcash Testnet clean-tip template service did not become ready" >&2
  exit 1
fi
python3 - "$runtime_dir/createauxblock.json" "$wcash_genesis" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as response_file:
    response = json.load(response_file)
assert response.get("error") in (None, False), response
result = response["result"]
assert result["height"] == 1, result
assert result["previousblockhash"] == sys.argv[2], result
assert result["coinbasevalue"] == 625_000_000, result
assert len(result["hash"]) == 64, result
assert len(result["retiretoken"]) == 64, result
assert len(result["target"]) == 64, result
assert result["data"], result
PY

read -r startup_candidate startup_retire_token < <(
  python3 - "$runtime_dir/createauxblock.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as response_file:
    result = json.load(response_file)["result"]
print(result["hash"], result["retiretoken"])
PY
)
rpc_call "$wcash_rpc" retireauxblock \
  "[\"$startup_candidate\",\"$startup_retire_token\"]" \
  >"$runtime_dir/retireauxblock.json"
python3 - "$runtime_dir/retireauxblock.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as response_file:
    response = json.load(response_file)
assert response.get("error") in (None, False), response
assert response["result"] == {"state": "retired"}, response
PY
rpc_call "$wcash_rpc" retireauxblock \
  "[\"$startup_candidate\",\"$startup_retire_token\"]" |
  python3 -c 'import json,sys; response=json.load(sys.stdin); assert response["result"] == {"state": "already_absent"}, response'

zcash_payout_address="$(sed -n "s/^export ZCASH_PAYOUT_ADDRESS='\([^']*\)'/\1/p" "$repo_root/docs/wcash-local.md")"
if [[ -z "$zcash_payout_address" ]]; then
  echo "Zcash local payout fixture is missing from docs/wcash-local.md" >&2
  exit 1
fi

export WCASH_EXPECTED_GENESIS_HASH="$wcash_genesis"
export ZCASH_EXPECTED_GENESIS_HASH=029f11d80ef9765602235e1bc9727e3eb6ba20839319f761fee920d63401e327
export WCASH_SHARE_JOURNAL="$runtime_dir/native-job-journal.jsonl"
export WCASH_PAYOUT_ADDRESS="$wcash_payout_address"
export ZCASH_PAYOUT_ADDRESS="$zcash_payout_address"

native_args=(
  "$wcash_rpc"
  "$zcash_template_rpc"
  "$zcash_validator_rpc"
  -
)

"$repo_root/target/release/wcash-merge-miner" native-job \
  "${native_args[@]}" >"$runtime_dir/native-job.json"

# `native-job` durably activates its exact job ID. The long-running pool owns a
# separate authoritative journal so its identical first template is not
# mistaken for an unsafe in-ledger job-ID replay.
export WCASH_SHARE_JOURNAL="$runtime_dir/journal.jsonl"

# The backend target must be at least as easy as both networks. Reconnecting
# the reference miner until a Wcash winner is found remains bounded because the
# initial testnet target is at the public proof-of-work limit.
share_target="$(python3 - "$runtime_dir/native-job.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as result_file:
    result = json.load(result_file)
child = int(result["wcash"]["child_target"], 16)
parent = int(result["zcash"]["parent_target"], 16)
assert parent >= child, "local Zcash target must include a Wcash Testnet winner"
print(f"{max(child, parent):064x}")
PY
)"
if [[ ! "$share_target" =~ ^[0-9a-f]{64}$ ]]; then
  echo "derived share target is not exactly 32 bytes of lowercase hex" >&2
  exit 1
fi
export WCASH_SHARE_TARGET="$share_target"
export WCASH_VALIDATION_LIMIT=4
export WCASH_AUTHENTICATION_LIMIT=2
export WCASH_STRATUM_PASSWORD=local-testnet-password-change-me
worker_password_hash="$(
  "$repo_root/target/release/wcash-merge-miner" worker-password-hash |
    python3 -c 'import json,sys; print(json.load(sys.stdin)["password_hash"])'
)"
unset WCASH_STRATUM_PASSWORD
export WCASH_WORKER_CREDENTIALS="$runtime_dir/workers.json"
python3 - "$WCASH_WORKER_CREDENTIALS" "$worker_password_hash" <<'PY'
import json
import os
import sys

path, password_hash = sys.argv[1:]
descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
with os.fdopen(descriptor, "w", encoding="utf-8") as credential_file:
    json.dump({
        "version": 1,
        "workers": [{
            "name": "rig.testnet-e2e",
            "password_hash": password_hash,
        }],
    }, credential_file, separators=(",", ":"))
    credential_file.write("\n")
PY

"$repo_root/target/release/wcash-merge-miner" native-serve \
  "${native_args[@]}" 127.0.0.1:38237 16 0 \
  >"$runtime_dir/native-serve.json" 2>"$runtime_dir/native-serve.log" &
pool_pid=$!
child_pids+=("$pool_pid")

export WCASH_STRATUM_PASSWORD=local-testnet-password-change-me
wcash_height=0
for attempt in {0..15}; do
  wait_for_tcp 38237 "Wcash Testnet pool"
  start_nonce=$((attempt * 512))
  if "$repo_root/target/release/wcash-merge-miner" zip301-mine \
    127.0.0.1:38237 rig.testnet-e2e 512 "$start_nonce" \
    >"$runtime_dir/zip301-mine-$attempt.json" \
    2>"$runtime_dir/zip301-mine-$attempt.log"; then
    :
  fi
  # A connection can close after durable submission but before the client reads
  # its response, so chain state—not client exit status—is authoritative.
  wcash_height="$(rpc_result "$wcash_rpc" getblockcount 2>/dev/null || true)"
  [[ "$wcash_height" == 1 ]] && break
done
unset WCASH_STRATUM_PASSWORD

if [[ "$wcash_height" != 1 ]]; then
  echo "ZIP-301 testnet miner did not find a Wcash block in 16 bounded shares" >&2
  exit 1
fi

for _attempt in {1..120}; do
  template_height="$(rpc_result "$zcash_template_rpc" getblockcount 2>/dev/null || true)"
  validator_height="$(rpc_result "$zcash_validator_rpc" getblockcount 2>/dev/null || true)"
  if [[ "$template_height" =~ ^[1-9][0-9]*$ && "$template_height" == "$validator_height" ]]; then
    break
  fi
  sleep 0.25
done
if [[ ! "$template_height" =~ ^[1-9][0-9]*$ || "$template_height" != "$validator_height" ]]; then
  echo "the two Zcash parent nodes did not accept the same mined chain" >&2
  exit 1
fi
[[ "$(rpc_result "$zcash_template_rpc" getbestblockhash)" == \
   "$(rpc_result "$zcash_validator_rpc" getbestblockhash)" ]]

rpc_call "$wcash_rpc" getblockchaininfo | python3 -c '
import json,sys
result=json.load(sys.stdin)["result"]
assert result["blocks"] == 1, result
assert result["chainSupply"]["chainValue"] == 6.25, result
assert result["chainSupply"]["chainValueZat"] == 625_000_000, result
pools={pool["id"]: pool for pool in result["valuePools"]}
assert pools["transparent"]["chainValue"] == 6.25, pools
assert pools["transparent"]["chainValueZat"] == 625_000_000, pools
assert all(pool["chainValueZat"] == 0 for name,pool in pools.items() if name != "transparent"), pools
'

kill -TERM "$pool_pid" 2>/dev/null || true
for _attempt in {1..40}; do
  kill -0 "$pool_pid" 2>/dev/null || break
  sleep 0.25
done
if kill -0 "$pool_pid" 2>/dev/null; then
  kill -KILL "$pool_pid" 2>/dev/null || true
fi
wait "$pool_pid" 2>/dev/null || true

"$repo_root/target/release/wcash-merge-miner" accounting-report \
  "$WCASH_SHARE_JOURNAL" >"$runtime_dir/accounting.json"
python3 - "$runtime_dir/accounting.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as result_file:
    result = json.load(result_file)
worker = result["workers"]["rig.testnet-e2e"]
assert worker["accepted_shares"] >= 1, worker
assert worker["shares_by_authentication"] == {
    "exact_credential": worker["accepted_shares"],
}, worker
assert 1 <= worker["wcash_winners"] <= worker["accepted_shares"], worker
assert worker["zcash_winners"] == worker["accepted_shares"], worker
PY

echo "Wcash Testnet genesis and native ZIP-301 mining phase passed at $wcash_genesis"

# Run a second, isolated Wcash Regtest child against the same two local Zcash
# parent validators. This phase uses public test-only seeds, supplied to the
# wallet exclusively over stdin, to prove the private coinbase is recoverable
# and spendable without weakening the public Testnet 100-confirmation policy.
wcash_wallet_rpc=http://127.0.0.1:48232
wcash_wallet_grpc=http://127.0.0.1:48234
wcash_regtest_genesis=70bf0bab17eff361a6331bb825b3b7253c8c96ff96407f948161d2912658bb1c
sender_seed=000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f
recipient_seed=202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f
expected_sender_address=wuregtest1xryxj7ddyajw4mv7jpelftnfhkwu3v5w03smp88kk6fkmfvlewpzrs26pxqs4wycul43485lg0h9ry8zzxkj9q8gvh7dmg0uh5e2t28k
sender_db="$runtime_dir/sender.sqlite"
recipient_db="$runtime_dir/recipient.sqlite"

# Coordinator variables use the same WCASH_ namespace that Zebra's config
# loader inspects. Clear the completed Testnet mining phase before starting a
# second node, otherwise coordinator-only keys are rejected as unknown Zebra
# configuration fields.
unset WCASH_EXPECTED_GENESIS_HASH ZCASH_EXPECTED_GENESIS_HASH \
  WCASH_SHARE_JOURNAL WCASH_PAYOUT_ADDRESS ZCASH_PAYOUT_ADDRESS \
  WCASH_SHARE_TARGET WCASH_VALIDATION_LIMIT WCASH_AUTHENTICATION_LIMIT \
  WCASH_WORKER_CREDENTIALS WCASH_STRATUM_PASSWORD

wallet_with_seed() {
  local seed=$1
  shift
  printf '%s\n' "$seed" | "$repo_root/target/release/wcash-wallet" "$@"
}

sender_address="$(
  wallet_with_seed "$sender_seed" --network regtest derive-address |
    python3 -c 'import json,sys; print(json.load(sys.stdin)["address"])'
)"
recipient_address="$(
  wallet_with_seed "$recipient_seed" --network regtest derive-address |
    python3 -c 'import json,sys; print(json.load(sys.stdin)["address"])'
)"
[[ "$sender_address" == "$expected_sender_address" ]]
[[ "$recipient_address" == wuregtest1* ]]
[[ "$recipient_address" != "$sender_address" ]]

"$repo_root/target/release/wcash-zebrad" \
  -c "$repo_root/wcash-wallet/tests/wcash-regtest-e2e.toml" start \
  >"$runtime_dir/wcash-wallet-regtest.log" 2>&1 &
private_wallet_node_pid=$!
child_pids+=("$private_wallet_node_pid")

wait_for_rpc "$wcash_wallet_rpc" WcashWalletRegtest
wait_for_tcp 48234 "Wcash wallet compact-block service"
[[ "$(rpc_result "$wcash_wallet_rpc" getblockcount)" == 0 ]]
[[ "$(rpc_result "$wcash_wallet_rpc" getblockhash '[0]')" == "$wcash_regtest_genesis" ]]

wallet_with_seed "$sender_seed" \
  --network regtest --db "$sender_db" --lightwalletd "$wcash_wallet_grpc" \
  init --birthday 1 >"$runtime_dir/sender-init.json"
wallet_with_seed "$recipient_seed" \
  --network regtest --db "$recipient_db" --lightwalletd "$wcash_wallet_grpc" \
  init --birthday 1 >"$runtime_dir/recipient-init.json"
python3 - "$runtime_dir/sender-init.json" "$runtime_dir/recipient-init.json" \
  "$sender_address" "$recipient_address" <<'PY'
import json
import sys

sender_path, recipient_path, sender_address, recipient_address = sys.argv[1:]
with open(sender_path, encoding="utf-8") as sender_file:
    sender = json.load(sender_file)
with open(recipient_path, encoding="utf-8") as recipient_file:
    recipient = json.load(recipient_file)
assert sender["created"] is True and sender["birthday_height"] == 1, sender
assert recipient["created"] is True and recipient["birthday_height"] == 1, recipient
assert sender["address"] == sender_address, sender
assert recipient["address"] == recipient_address, recipient
PY

export WCASH_EXPECTED_GENESIS_HASH="$wcash_regtest_genesis"
export ZCASH_EXPECTED_GENESIS_HASH=029f11d80ef9765602235e1bc9727e3eb6ba20839319f761fee920d63401e327
export WCASH_SHARE_JOURNAL="$runtime_dir/wallet-spend-journal.jsonl"
export WCASH_PAYOUT_ADDRESS="$sender_address"
export ZCASH_PAYOUT_ADDRESS="$zcash_payout_address"

wallet_native_args=(
  "$wcash_wallet_rpc"
  "$zcash_template_rpc"
  "$zcash_validator_rpc"
  -
)
parent_start_height="$(rpc_result "$zcash_template_rpc" getblockcount)"
if [[ ! "$parent_start_height" =~ ^[0-9]+$ ]]; then
  echo "invalid starting Zcash parent height: $parent_start_height" >&2
  exit 1
fi

mine_wallet_generation() {
  local generation=$1
  local expected_parent_height=$((parent_start_height + generation))
  local start_nonce=$((generation * 512))

  "$repo_root/target/release/wcash-merge-miner" native-mine \
    "${wallet_native_args[@]}" 512 "$start_nonce" \
    >"$runtime_dir/wallet-mine-$generation.json" \
    2>"$runtime_dir/wallet-mine-$generation.log"
  python3 - "$runtime_dir/wallet-mine-$generation.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as result_file:
    result = json.load(result_file)
assert result["result"] == "processed", result
assert result["wcash_candidate"] is True, result
assert result["zcash_candidate"] is True, result
PY
  wait_for_height "$wcash_wallet_rpc" WcashWalletRegtest "$generation"
  wait_for_height "$zcash_template_rpc" Zcash-template "$expected_parent_height"
  wait_for_height "$zcash_validator_rpc" Zcash-validator "$expected_parent_height"
  [[ "$(rpc_result "$zcash_template_rpc" getbestblockhash)" == \
     "$(rpc_result "$zcash_validator_rpc" getbestblockhash)" ]]
}

mine_wallet_generation 1
mine_wallet_generation 2
mine_wallet_generation 3

"$repo_root/target/release/wcash-wallet" \
  --network regtest --db "$sender_db" --lightwalletd "$wcash_wallet_grpc" \
  sync --batch-size 100 >"$runtime_dir/sender-sync-before.json"
"$repo_root/target/release/wcash-wallet" \
  --network regtest --db "$recipient_db" --lightwalletd "$wcash_wallet_grpc" \
  sync --batch-size 100 >"$runtime_dir/recipient-sync-before.json"
python3 - "$runtime_dir/sender-sync-before.json" \
  "$runtime_dir/recipient-sync-before.json" <<'PY'
import json
import sys

def only_account(path):
    with open(path, encoding="utf-8") as summary_file:
        summary = json.load(summary_file)
    assert summary["synchronized"] is True, summary
    assert summary["chain_tip_height"] == 3, summary
    assert summary["fully_scanned_height"] == 3, summary
    assert len(summary["accounts"]) == 1, summary
    return summary["accounts"][0]

sender = only_account(sys.argv[1])
recipient = only_account(sys.argv[2])
assert sender["ironwood_total_zat"] == 1_875_000_000, sender
assert recipient["ironwood_total_zat"] == 0, recipient
for account in (sender, recipient):
    assert account["transparent_total_zat"] == 0, account
    assert account["sapling_total_zat"] == 0, account
    assert account["orchard_total_zat"] == 0, account
PY

wallet_with_seed "$sender_seed" \
  --network regtest --db "$sender_db" --lightwalletd "$wcash_wallet_grpc" \
  transfer --recipient "$recipient_address" --amount-zat 100000000 \
  --confirmations 1 --unsafe-regtest-confirmations \
  --expiry-delta 40 --lock-for-blocks 100 \
  >"$runtime_dir/signed-transfer.json"

read -r transfer_txid raw_transaction transfer_fee < <(
  python3 - "$runtime_dir/signed-transfer.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as result_file:
    result = json.load(result_file)
assert result["branch_id"] == "c3a6678a", result
assert result["target_height"] == 4, result
assert result["expiry_height"] == 44, result
assert result["internal_change_receiver_verified"] is True, result
assert len(result["txid"]) == 64, result
assert result["raw_transaction_hex"], result
assert result["fee_zat"] > 0, result
print(result["txid"], result["raw_transaction_hex"], result["fee_zat"])
PY
)

printf '%s\n' "$raw_transaction" |
  "$repo_root/target/release/wcash-wallet" \
    --network regtest --lightwalletd "$wcash_wallet_grpc" broadcast \
    >"$runtime_dir/broadcast-first.json"
printf '%s\n' "$raw_transaction" |
  "$repo_root/target/release/wcash-wallet" \
    --network regtest --lightwalletd "$wcash_wallet_grpc" broadcast \
    >"$runtime_dir/broadcast-duplicate.json"
python3 - "$runtime_dir/broadcast-first.json" \
  "$runtime_dir/broadcast-duplicate.json" "$transfer_txid" <<'PY'
import json
import sys

first_path, duplicate_path, expected_txid = sys.argv[1:]
with open(first_path, encoding="utf-8") as first_file:
    first = json.load(first_file)
with open(duplicate_path, encoding="utf-8") as duplicate_file:
    duplicate = json.load(duplicate_file)
assert first["txid"] == expected_txid, first
assert first["disposition"] == "submitted", first
assert first["status"]["state"] == "mempool", first
assert duplicate["txid"] == expected_txid, duplicate
assert duplicate["disposition"] == "already_known", duplicate
assert duplicate["status"]["state"] == "mempool", duplicate
PY

"$repo_root/target/release/wcash-wallet" \
  --network regtest --lightwalletd "$wcash_wallet_grpc" \
  status --txid "$transfer_txid" >"$runtime_dir/status-mempool.json"
python3 - "$runtime_dir/status-mempool.json" "$transfer_txid" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as status_file:
    status = json.load(status_file)
assert status["txid"] == sys.argv[2], status
assert status["status"]["state"] == "mempool", status
PY

rpc_call "$wcash_wallet_rpc" getrawmempool \
  >"$runtime_dir/wcash-mempool-before.json"
# Ironwood coinbase proving is intentionally more expensive than ordinary RPC
# service, especially on the two-core deployment target. Keep it bounded
# without weakening the five-second timeout used by all other test calls.
rpc_call_with_timeout 30 "$wcash_wallet_rpc" getblocktemplate \
  '[{"mode":"template","capabilities":["coinbasetxn"]}]' \
  >"$runtime_dir/wcash-template-before.json"
python3 - "$runtime_dir/wcash-mempool-before.json" \
  "$runtime_dir/wcash-template-before.json" "$transfer_txid" \
  "$raw_transaction" "$transfer_fee" <<'PY'
import json
import sys

mempool_path, template_path, txid, raw, fee = sys.argv[1:]
with open(mempool_path, encoding="utf-8") as mempool_file:
    mempool_response = json.load(mempool_file)
with open(template_path, encoding="utf-8") as template_file:
    template_response = json.load(template_file)
assert mempool_response.get("error") in (None, False), mempool_response
assert txid in mempool_response["result"], mempool_response
assert template_response.get("error") in (None, False), template_response
template = template_response["result"]
matches = [transaction for transaction in template["transactions"] if transaction["hash"] == txid]
assert len(matches) == 1, template
assert matches[0]["data"] == raw, matches[0]
assert matches[0]["fee"] == int(fee), matches[0]
PY

# The same signed bytes must fail at both unmodified Zcash-domain validators.
for parent_rpc in "$zcash_template_rpc" "$zcash_validator_rpc"; do
  label="$(printf '%s' "$parent_rpc" | tr -cd '0-9')"
  rpc_call "$parent_rpc" sendrawtransaction "[\"$raw_transaction\"]" \
    >"$runtime_dir/zcash-reject-$label.json"
  python3 - "$runtime_dir/zcash-reject-$label.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as response_file:
    response = json.load(response_file)
assert response.get("result") is None, response
assert response.get("error"), response
PY
  rpc_call "$parent_rpc" getrawmempool >"$runtime_dir/zcash-mempool-$label.json"
  python3 - "$runtime_dir/zcash-mempool-$label.json" "$transfer_txid" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as response_file:
    response = json.load(response_file)
assert response.get("error") in (None, False), response
assert sys.argv[2] not in response["result"], response
PY
done

mine_wallet_generation 4

rpc_call "$wcash_wallet_rpc" getrawmempool \
  >"$runtime_dir/wcash-mempool-after.json"
rpc_call "$wcash_wallet_rpc" getblock '["4",1]' \
  >"$runtime_dir/wcash-spend-block.json"
python3 - "$runtime_dir/wcash-mempool-after.json" \
  "$runtime_dir/wcash-spend-block.json" "$transfer_txid" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as mempool_file:
    mempool = json.load(mempool_file)
with open(sys.argv[2], encoding="utf-8") as block_file:
    block = json.load(block_file)
assert mempool.get("error") in (None, False), mempool
assert sys.argv[3] not in mempool["result"], mempool
assert block.get("error") in (None, False), block
assert sys.argv[3] in block["result"]["tx"], block
PY

"$repo_root/target/release/wcash-wallet" \
  --network regtest --lightwalletd "$wcash_wallet_grpc" \
  status --txid "$transfer_txid" >"$runtime_dir/status-mined.json"
printf '%s\n' "$raw_transaction" |
  "$repo_root/target/release/wcash-wallet" \
    --network regtest --lightwalletd "$wcash_wallet_grpc" broadcast \
    >"$runtime_dir/broadcast-mined.json"
python3 - "$runtime_dir/status-mined.json" \
  "$runtime_dir/broadcast-mined.json" "$transfer_txid" <<'PY'
import json
import sys

status_path, broadcast_path, expected_txid = sys.argv[1:]
with open(status_path, encoding="utf-8") as status_file:
    status = json.load(status_file)
with open(broadcast_path, encoding="utf-8") as broadcast_file:
    broadcast = json.load(broadcast_file)
assert status["txid"] == expected_txid, status
assert status["status"] == {"state": "mined", "height": 4}, status
assert broadcast["txid"] == expected_txid, broadcast
assert broadcast["disposition"] == "already_known", broadcast
assert broadcast["status"] == {"state": "mined", "height": 4}, broadcast
PY

"$repo_root/target/release/wcash-wallet" \
  --network regtest --db "$sender_db" --lightwalletd "$wcash_wallet_grpc" \
  sync --batch-size 100 >"$runtime_dir/sender-sync-after.json"
"$repo_root/target/release/wcash-wallet" \
  --network regtest --db "$recipient_db" --lightwalletd "$wcash_wallet_grpc" \
  sync --batch-size 100 >"$runtime_dir/recipient-sync-after.json"
python3 - "$runtime_dir/sender-sync-after.json" \
  "$runtime_dir/recipient-sync-after.json" <<'PY'
import json
import sys

def only_account(path):
    with open(path, encoding="utf-8") as summary_file:
        summary = json.load(summary_file)
    assert summary["synchronized"] is True, summary
    assert summary["chain_tip_height"] == 4, summary
    assert summary["fully_scanned_height"] == 4, summary
    assert len(summary["accounts"]) == 1, summary
    return summary["accounts"][0]

sender = only_account(sys.argv[1])
recipient = only_account(sys.argv[2])
assert sender["ironwood_total_zat"] == 2_400_000_000, sender
assert recipient["ironwood_total_zat"] == 100_000_000, recipient
assert sender["ironwood_total_zat"] + recipient["ironwood_total_zat"] == 2_500_000_000
for account in (sender, recipient):
    assert account["transparent_total_zat"] == 0, account
    assert account["sapling_total_zat"] == 0, account
    assert account["orchard_total_zat"] == 0, account
PY

rpc_call "$wcash_wallet_rpc" getblockchaininfo \
  >"$runtime_dir/wcash-wallet-chain-info.json"
python3 - "$runtime_dir/wcash-wallet-chain-info.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as response_file:
    response = json.load(response_file)
assert response.get("error") in (None, False), response
result = response["result"]
assert result["blocks"] == 4, result
assert result["chainSupply"]["chainValueZat"] == 2_500_000_000, result
pools = {pool["id"]: pool for pool in result["valuePools"]}
assert pools["ironwood"]["chainValueZat"] == 2_500_000_000, pools
assert all(pool["chainValueZat"] == 0 for name, pool in pools.items() if name != "ironwood"), pools
PY

echo "Wcash controlled private-coinbase spend E2E passed at $wcash_regtest_genesis"

# Restart the isolated Wcash Regtest node with a fresh ephemeral chain, then
# prove the default transparent coinbase lifecycle at the exact consensus
# maturity boundary. Every block in this phase is a real Equihash AuxPoW block
# independently accepted by the Wcash child and both Zcash parent validators.
# The public Testnet maturity rule is not bypassed or reconfigured.
kill -TERM "$private_wallet_node_pid" 2>/dev/null || true
for _attempt in {1..80}; do
  kill -0 "$private_wallet_node_pid" 2>/dev/null || break
  sleep 0.25
done
if kill -0 "$private_wallet_node_pid" 2>/dev/null; then
  kill -KILL "$private_wallet_node_pid" 2>/dev/null || true
fi
wait "$private_wallet_node_pid" 2>/dev/null || true

unset WCASH_EXPECTED_GENESIS_HASH ZCASH_EXPECTED_GENESIS_HASH \
  WCASH_SHARE_JOURNAL WCASH_PAYOUT_ADDRESS ZCASH_PAYOUT_ADDRESS

transparent_sender_db="$runtime_dir/transparent-sender.sqlite"
sender_transparent_address="$(
  wallet_with_seed "$sender_seed" --network regtest derive-address |
    python3 -c 'import json,sys; print(json.load(sys.stdin)["transparent_coinbase_address"])'
)"
[[ "$sender_transparent_address" == WR* ]]
[[ "$sender_transparent_address" != "$sender_address" ]]

"$repo_root/target/release/wcash-zebrad" \
  -c "$repo_root/wcash-wallet/tests/wcash-regtest-e2e.toml" start \
  >"$runtime_dir/wcash-transparent-regtest.log" 2>&1 &
transparent_wallet_node_pid=$!
child_pids+=("$transparent_wallet_node_pid")

wait_for_rpc "$wcash_wallet_rpc" WcashTransparentRegtest
wait_for_tcp 48234 "Wcash transparent-wallet compact-block service"
[[ "$(rpc_result "$wcash_wallet_rpc" getblockcount)" == 0 ]]
[[ "$(rpc_result "$wcash_wallet_rpc" getblockhash '[0]')" == "$wcash_regtest_genesis" ]]

wallet_with_seed "$sender_seed" \
  --network regtest --db "$transparent_sender_db" --lightwalletd "$wcash_wallet_grpc" \
  init --birthday 1 >"$runtime_dir/transparent-sender-init.json"
python3 - "$runtime_dir/transparent-sender-init.json" \
  "$sender_address" "$sender_transparent_address" <<'PY'
import json
import sys

path, expected_private, expected_transparent = sys.argv[1:]
with open(path, encoding="utf-8") as result_file:
    result = json.load(result_file)
assert result["created"] is True and result["birthday_height"] == 1, result
assert result["address"] == expected_private, result
assert result["transparent_coinbase_address"] == expected_transparent, result
PY

export WCASH_EXPECTED_GENESIS_HASH="$wcash_regtest_genesis"
export ZCASH_EXPECTED_GENESIS_HASH=029f11d80ef9765602235e1bc9727e3eb6ba20839319f761fee920d63401e327
export WCASH_SHARE_JOURNAL="$runtime_dir/transparent-coinbase-journal.jsonl"
export WCASH_PAYOUT_ADDRESS="$sender_transparent_address"
export ZCASH_PAYOUT_ADDRESS="$zcash_payout_address"

transparent_native_args=(
  "$wcash_wallet_rpc"
  "$zcash_template_rpc"
  "$zcash_validator_rpc"
  -
)
transparent_parent_start_height="$(rpc_result "$zcash_template_rpc" getblockcount)"
if [[ ! "$transparent_parent_start_height" =~ ^[0-9]+$ ]]; then
  echo "invalid transparent-phase Zcash parent height: $transparent_parent_start_height" >&2
  exit 1
fi

mine_transparent_generation() {
  local generation=$1
  local expected_parent_height=$((transparent_parent_start_height + generation))
  local start_nonce=$(((1000 + generation) * 512))

  "$repo_root/target/release/wcash-merge-miner" native-mine \
    "${transparent_native_args[@]}" 512 "$start_nonce" \
    >"$runtime_dir/transparent-mine-$generation.json" \
    2>"$runtime_dir/transparent-mine-$generation.log"
  python3 - "$runtime_dir/transparent-mine-$generation.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as result_file:
    result = json.load(result_file)
assert result["result"] == "processed", result
assert result["wcash_candidate"] is True, result
assert result["zcash_candidate"] is True, result
PY
  wait_for_height "$wcash_wallet_rpc" WcashTransparentRegtest "$generation"
  wait_for_height "$zcash_template_rpc" Zcash-template "$expected_parent_height"
  wait_for_height "$zcash_validator_rpc" Zcash-validator "$expected_parent_height"
  [[ "$(rpc_result "$zcash_template_rpc" getbestblockhash)" == \
     "$(rpc_result "$zcash_validator_rpc" getbestblockhash)" ]]
}

# At tip 99 the first height-1 coinbase would be spent at target height 100,
# one block before its exact 100-block maturity boundary.
for generation in {1..99}; do
  mine_transparent_generation "$generation"
done

"$repo_root/target/release/wcash-wallet" \
  --network regtest --db "$transparent_sender_db" --lightwalletd "$wcash_wallet_grpc" \
  sync --batch-size 100 >"$runtime_dir/transparent-sync-immature.json"
python3 - "$runtime_dir/transparent-sync-immature.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as summary_file:
    summary = json.load(summary_file)
assert summary["synchronized"] is True, summary
assert summary["chain_tip_height"] == 99, summary
assert summary["fully_scanned_height"] == 99, summary
assert len(summary["accounts"]) == 1, summary
account = summary["accounts"][0]
expected = 99 * 625_000_000
assert account["transparent_total_zat"] == expected, account
assert account["transparent_coinbase_total_zat"] == expected, account
assert account["transparent_coinbase_spendable_zat"] == 0, account
assert account["transparent_coinbase_pending_zat"] == expected, account
assert account["transparent_regular_total_zat"] == 0, account
assert account["ironwood_total_zat"] == 0, account
assert account["sapling_total_zat"] == 0, account
assert account["orchard_total_zat"] == 0, account
PY

if wallet_with_seed "$sender_seed" \
  --network regtest --db "$transparent_sender_db" --lightwalletd "$wcash_wallet_grpc" \
  shield-coinbase --max-inputs 1 --expiry-delta 40 --lock-for-blocks 100 \
  >"$runtime_dir/immature-shielding.json" \
  2>"$runtime_dir/immature-shielding-error.json"; then
  echo "transparent coinbase shielding unexpectedly succeeded before maturity" >&2
  exit 1
fi
python3 - "$runtime_dir/immature-shielding-error.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as error_file:
    error = json.load(error_file)
message = error["error"].lower()
assert "mature" in message and "100" in message, error
PY

# At tip 100 the target height is 101, exactly 100 blocks after height 1.
mine_transparent_generation 100
"$repo_root/target/release/wcash-wallet" \
  --network regtest --db "$transparent_sender_db" --lightwalletd "$wcash_wallet_grpc" \
  sync --batch-size 100 >"$runtime_dir/transparent-sync-mature.json"
python3 - "$runtime_dir/transparent-sync-mature.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as summary_file:
    summary = json.load(summary_file)
assert summary["synchronized"] is True, summary
assert summary["chain_tip_height"] == 100, summary
assert summary["fully_scanned_height"] == 100, summary
assert len(summary["accounts"]) == 1, summary
account = summary["accounts"][0]
expected = 100 * 625_000_000
assert account["transparent_coinbase_total_zat"] == expected, account
assert account["transparent_coinbase_spendable_zat"] == 625_000_000, account
assert account["transparent_coinbase_pending_zat"] == expected - 625_000_000, account
assert account["transparent_regular_total_zat"] == 0, account
PY

# Recreate the same wallet with a deliberately late shielded birthday. The
# fixed transparent receiver was already paid from height 1, so this proves
# the separate current-UTXO recovery pass does not silently lose pre-birthday
# coinbase rewards.
transparent_late_restore_db="$runtime_dir/transparent-late-restore.sqlite"
wallet_with_seed "$sender_seed" \
  --network regtest --db "$transparent_late_restore_db" --lightwalletd "$wcash_wallet_grpc" \
  init --birthday 100 >"$runtime_dir/transparent-late-restore-init.json"
python3 - "$runtime_dir/transparent-late-restore-init.json" \
  "$sender_address" "$sender_transparent_address" <<'PY'
import json
import sys

path, expected_private, expected_transparent = sys.argv[1:]
with open(path, encoding="utf-8") as result_file:
    result = json.load(result_file)
assert result["created"] is True and result["birthday_height"] == 100, result
assert result["address"] == expected_private, result
assert result["transparent_coinbase_address"] == expected_transparent, result
PY
"$repo_root/target/release/wcash-wallet" \
  --network regtest --db "$transparent_late_restore_db" --lightwalletd "$wcash_wallet_grpc" \
  sync --batch-size 100 >"$runtime_dir/transparent-late-restore-sync.json"
python3 - "$runtime_dir/transparent-late-restore-sync.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as summary_file:
    summary = json.load(summary_file)
assert summary["synchronized"] is True, summary
assert summary["chain_tip_height"] == 100, summary
assert summary["fully_scanned_height"] == 100, summary
assert len(summary["accounts"]) == 1, summary
account = summary["accounts"][0]
expected = 100 * 625_000_000
assert account["transparent_total_zat"] == expected, account
assert account["transparent_coinbase_total_zat"] == expected, account
assert account["transparent_coinbase_spendable_zat"] == 625_000_000, account
assert account["transparent_coinbase_pending_zat"] == expected - 625_000_000, account
assert account["transparent_regular_total_zat"] == 0, account
assert account["ironwood_total_zat"] == 0, account
PY

wallet_with_seed "$sender_seed" \
  --network regtest --db "$transparent_sender_db" --lightwalletd "$wcash_wallet_grpc" \
  shield-coinbase --max-inputs 1 --expiry-delta 40 --lock-for-blocks 100 \
  >"$runtime_dir/signed-coinbase-shielding.json"

read -r shielding_txid shielding_raw shielding_fee < <(
  python3 - "$runtime_dir/signed-coinbase-shielding.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as result_file:
    result = json.load(result_file)
assert result["branch_id"] == "c3a6678a", result
assert result["target_height"] == 101, result
assert result["expiry_height"] == 141, result
assert result["internal_change_receiver_verified"] is True, result
assert len(result["txid"]) == 64, result
assert result["raw_transaction_hex"], result
assert 0 < result["fee_zat"] < 625_000_000, result
print(result["txid"], result["raw_transaction_hex"], result["fee_zat"])
PY
)

printf '%s\n' "$shielding_raw" |
  "$repo_root/target/release/wcash-wallet" \
    --network regtest --lightwalletd "$wcash_wallet_grpc" broadcast \
    >"$runtime_dir/broadcast-coinbase-shielding.json"
python3 - "$runtime_dir/broadcast-coinbase-shielding.json" "$shielding_txid" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as result_file:
    result = json.load(result_file)
assert result["txid"] == sys.argv[2], result
assert result["disposition"] == "submitted", result
assert result["status"]["state"] == "mempool", result
PY

# Decode through the node as an independent wire-format check: Wcash V6,
# exactly one transparent input, no transparent output or legacy shielded
# component, and the canonical two-action Ironwood bundle (one private output
# plus one protocol padding action).
rpc_call "$wcash_wallet_rpc" getrawtransaction \
  "[\"$shielding_txid\",1]" >"$runtime_dir/decoded-coinbase-shielding.json"
python3 - "$runtime_dir/decoded-coinbase-shielding.json" \
  "$shielding_txid" "$shielding_raw" "$shielding_fee" <<'PY'
import json
import sys

path, expected_txid, expected_raw, fee = sys.argv[1:]
with open(path, encoding="utf-8") as response_file:
    response = json.load(response_file)
assert response.get("error") in (None, False), response
transaction = response["result"]
assert transaction["txid"] == expected_txid, transaction
assert transaction["hex"] == expected_raw, transaction
assert transaction["version"] == 6 and transaction["overwintered"] is True, transaction
assert len(transaction["vin"]) == 1 and "coinbase" not in transaction["vin"][0], transaction
assert transaction["vout"] == [], transaction
assert transaction["vShieldedSpend"] == [], transaction
assert transaction["vShieldedOutput"] == [], transaction
assert transaction["vjoinsplit"] == [], transaction
assert transaction["orchard"]["actions"] == [], transaction
assert len(transaction["ironwood"]["actions"]) == 2, transaction
assert transaction["ironwood"]["valueBalanceZat"] == -(625_000_000 - int(fee)), transaction
PY

rpc_call_with_timeout 30 "$wcash_wallet_rpc" getblocktemplate \
  '[{"mode":"template","capabilities":["coinbasetxn"]}]' \
  >"$runtime_dir/transparent-template-with-shielding.json"
python3 - "$runtime_dir/transparent-template-with-shielding.json" \
  "$shielding_txid" "$shielding_raw" "$shielding_fee" <<'PY'
import json
import sys

path, expected_txid, expected_raw, fee = sys.argv[1:]
with open(path, encoding="utf-8") as response_file:
    response = json.load(response_file)
assert response.get("error") in (None, False), response
template = response["result"]
assert template["height"] == 101, template
matches = [transaction for transaction in template["transactions"] if transaction["hash"] == expected_txid]
assert len(matches) == 1, template
assert matches[0]["data"] == expected_raw, matches[0]
assert matches[0]["fee"] == int(fee), matches[0]
template_fees = sum(transaction["fee"] for transaction in template["transactions"])
assert template_fees == int(fee), template
coinbase = template["coinbasetxn"]
assert coinbase["required"] is True, coinbase
assert coinbase["depends"] == [], coinbase
assert coinbase["data"] and coinbase["hash"], coinbase
assert coinbase["fee"] == -template_fees, coinbase
PY

mine_transparent_generation 101

"$repo_root/target/release/wcash-wallet" \
  --network regtest --lightwalletd "$wcash_wallet_grpc" \
  status --txid "$shielding_txid" >"$runtime_dir/status-coinbase-shielding.json"
rpc_call "$wcash_wallet_rpc" getrawmempool \
  >"$runtime_dir/mempool-after-coinbase-shielding.json"
python3 - "$runtime_dir/status-coinbase-shielding.json" \
  "$runtime_dir/mempool-after-coinbase-shielding.json" "$shielding_txid" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as status_file:
    status = json.load(status_file)
with open(sys.argv[2], encoding="utf-8") as mempool_file:
    mempool = json.load(mempool_file)
assert status["txid"] == sys.argv[3], status
assert status["status"] == {"state": "mined", "height": 101}, status
assert mempool.get("error") in (None, False), mempool
assert sys.argv[3] not in mempool["result"], mempool
PY

"$repo_root/target/release/wcash-wallet" \
  --network regtest --db "$transparent_sender_db" --lightwalletd "$wcash_wallet_grpc" \
  sync --batch-size 100 >"$runtime_dir/transparent-sync-shielded.json"
python3 - "$runtime_dir/transparent-sync-shielded.json" "$shielding_fee" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as summary_file:
    summary = json.load(summary_file)
fee = int(sys.argv[2])
assert summary["synchronized"] is True, summary
assert summary["chain_tip_height"] == 101, summary
assert summary["fully_scanned_height"] == 101, summary
assert len(summary["accounts"]) == 1, summary
account = summary["accounts"][0]
expected_transparent = 100 * 625_000_000 + fee
expected_ironwood = 625_000_000 - fee
assert account["transparent_total_zat"] == expected_transparent, account
assert account["transparent_coinbase_total_zat"] == expected_transparent, account
assert account["transparent_coinbase_spendable_zat"] == 625_000_000, account
assert account["transparent_coinbase_pending_zat"] == 99 * 625_000_000 + fee, account
assert account["transparent_regular_total_zat"] == 0, account
assert account["ironwood_total_zat"] == expected_ironwood, account
assert account["sapling_total_zat"] == 0, account
assert account["orchard_total_zat"] == 0, account
assert account["transparent_total_zat"] + account["ironwood_total_zat"] == 101 * 625_000_000
PY

rpc_call "$wcash_wallet_rpc" getblockchaininfo \
  >"$runtime_dir/transparent-shielding-chain-info.json"
python3 - "$runtime_dir/transparent-shielding-chain-info.json" "$shielding_fee" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as response_file:
    response = json.load(response_file)
fee = int(sys.argv[2])
assert response.get("error") in (None, False), response
result = response["result"]
assert result["blocks"] == 101, result
assert result["chainSupply"]["chainValueZat"] == 101 * 625_000_000, result
pools = {pool["id"]: pool for pool in result["valuePools"]}
assert pools["transparent"]["chainValueZat"] == 100 * 625_000_000 + fee, pools
assert pools["ironwood"]["chainValueZat"] == 625_000_000 - fee, pools
assert pools["sapling"]["chainValueZat"] == 0, pools
assert pools["orchard"]["chainValueZat"] == 0, pools
PY

echo "Wcash transparent coinbase maturity and shielding E2E passed at $wcash_regtest_genesis"
