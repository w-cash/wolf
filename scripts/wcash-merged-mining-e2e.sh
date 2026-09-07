#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
runtime_dir="$(mktemp -d "${TMPDIR:-/tmp}/wcash-merged-mining-e2e.XXXXXX")"
declare -a child_pids=()

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
    if [[ "$running" == false ]]; then
      break
    fi
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
    if [[ "$running" == false ]]; then
      break
    fi
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
    echo "merged-mining E2E failed; logs retained at $runtime_dir" >&2
  fi
  exit "$status"
}
trap 'cleanup $?' EXIT
trap 'cleanup 130' INT TERM

for binary in zcash-zebrad wcash-zebrad wcash-merge-miner; do
  if [[ ! -x "$repo_root/target/release/$binary" ]]; then
    echo "missing release binary: target/release/$binary" >&2
    exit 1
  fi
done

rpc_call() {
  local url=$1
  local method=$2
  curl --fail --silent --show-error --max-time 5 \
    --header 'content-type: application/json' \
    --data-binary "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"$method\",\"params\":[]}" \
    "$url"
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

rpc_result() {
  rpc_call "$1" "$2" | python3 -c \
    'import json,sys; response=json.load(sys.stdin); assert response.get("error") in (None, False); print(response["result"])'
}

"$repo_root/target/release/wcash-zebrad" \
  -c "$repo_root/wcash-child-local.toml" start \
  >"$runtime_dir/wcash.log" 2>&1 &
child_pids+=("$!")
"$repo_root/target/release/zcash-zebrad" \
  -c "$repo_root/zcash-parent-template-local.toml" start \
  >"$runtime_dir/zcash-template.log" 2>&1 &
child_pids+=("$!")
"$repo_root/target/release/zcash-zebrad" \
  -c "$repo_root/zcash-parent-validator-local.toml" start \
  >"$runtime_dir/zcash-validator.log" 2>&1 &
child_pids+=("$!")

wait_for_rpc http://127.0.0.1:28232 Wcash
wait_for_rpc http://127.0.0.1:18232 Zcash-template
wait_for_rpc http://127.0.0.1:18242 Zcash-validator

wcash_payout_address="$(sed -n "s/^export WCASH_PAYOUT_ADDRESS='\([^']*\)'/\1/p" "$repo_root/docs/wcash-local.md")"
zcash_payout_address="$(sed -n "s/^export ZCASH_PAYOUT_ADDRESS='\([^']*\)'/\1/p" "$repo_root/docs/wcash-local.md")"
if [[ -z "$wcash_payout_address" || -z "$zcash_payout_address" ]]; then
  echo "local payout fixtures are missing from docs/wcash-local.md" >&2
  exit 1
fi

export WCASH_EXPECTED_GENESIS_HASH=b0ebe8618354e0563091d10b73ba03842cb3c112a801012616489269e58dbd61
export ZCASH_EXPECTED_GENESIS_HASH=029f11d80ef9765602235e1bc9727e3eb6ba20839319f761fee920d63401e327
export WCASH_SHARE_JOURNAL="$runtime_dir/journal.jsonl"
export WCASH_PAYOUT_ADDRESS="$wcash_payout_address"
export ZCASH_PAYOUT_ADDRESS="$zcash_payout_address"

native_args=(
  http://127.0.0.1:28232
  http://127.0.0.1:18232
  http://127.0.0.1:18242
  -
)

"$repo_root/target/release/wcash-merge-miner" native-job \
  "${native_args[@]}" >"$runtime_dir/native-job.json"

if ZCASH_PAYOUT_ADDRESS=tmJymvcUCn1ctbghvTJpXBwHiMEB8P6wxNV \
  "$repo_root/target/release/wcash-merge-miner" native-job \
  "${native_args[@]}" >"$runtime_dir/rejected-job.log" 2>&1; then
  echo "mismatched parent payout address was accepted" >&2
  exit 1
fi
grep --quiet 'payout commitment differs' "$runtime_dir/rejected-job.log"

"$repo_root/target/release/wcash-merge-miner" native-mine \
  "${native_args[@]}" 64 0 \
  >"$runtime_dir/native-mine.json" 2>"$runtime_dir/native-mine.log"

python3 - "$runtime_dir/native-mine.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as result_file:
    result = json.load(result_file)
assert result["result"] == "processed"
assert result["wcash_candidate"] is True
assert result["zcash_candidate"] is True
assert result["durable_outbox"]["pending_wcash_winners"] == 1
assert result["durable_outbox"]["pending_zcash_winners"] == 1
PY

[[ "$(rpc_result http://127.0.0.1:28232 getblockcount)" == 1 ]]
[[ "$(rpc_result http://127.0.0.1:18232 getblockcount)" == 1 ]]
[[ "$(rpc_result http://127.0.0.1:18242 getblockcount)" == 1 ]]

template_tip="$(rpc_result http://127.0.0.1:18232 getbestblockhash)"
validator_tip="$(rpc_result http://127.0.0.1:18242 getbestblockhash)"
[[ "$template_tip" == "$validator_tip" ]]

rpc_call http://127.0.0.1:28232 getblockchaininfo | python3 -c '
import json,sys
result=json.load(sys.stdin)["result"]
assert result["chainSupply"]["chainValueZat"] == 1_000_000_000
pools={pool["id"]: pool["chainValueZat"] for pool in result["valuePools"]}
assert pools["ironwood"] == 1_000_000_000
assert all(value == 0 for name,value in pools.items() if name != "ironwood")
'

export WCASH_STRATUM_PASSWORD=local-test-password-change-me
export WCASH_SHARE_TARGET=ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
export WCASH_VALIDATION_LIMIT=4
"$repo_root/target/release/wcash-merge-miner" native-serve-once \
  "${native_args[@]}" 127.0.0.1:28237 16 0 \
  >"$runtime_dir/native-serve.json" 2>"$runtime_dir/native-serve.log" &
pool_pid=$!
child_pids+=("$pool_pid")

python3 - <<'PY'
import json
import socket
import time

deadline = time.monotonic() + 30
while True:
    try:
        sock = socket.create_connection(("127.0.0.1", 28237), timeout=2)
        break
    except OSError:
        if time.monotonic() >= deadline:
            raise
        time.sleep(0.25)

sock.settimeout(5)
stream = sock.makefile("rwb", buffering=0)

def send(message):
    stream.write(json.dumps(message, separators=(",", ":")).encode() + b"\n")

def receive():
    line = stream.readline()
    if not line:
        raise RuntimeError("ZIP-301 listener closed unexpectedly")
    return json.loads(line)

send({"id": 1, "method": "mining.submit", "params": []})
assert receive()["error"][0] == 25
send({"id": 2, "method": "mining.subscribe", "params": []})
assert len(receive()["result"][1]) == 8
send({"id": 3, "method": "mining.authorize", "params": ["rig01", "wrong-password"]})
assert receive()["error"][0] == 24
send({"id": 4, "method": "mining.authorize", "params": ["rig01", "local-test-password-change-me"]})
assert receive()["result"] is True
assert receive()["method"] == "mining.set_target"
notification = receive()
assert notification["method"] == "mining.notify"
assert len(notification["params"][0]) == 64
assert notification["params"][-1] is True
PY

echo "Wcash/Zcash native merged-mining E2E passed at parent tip $template_tip"
