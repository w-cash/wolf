#!/usr/bin/env bash
# Real Equihash/AuxPoW, persistent-state and custody gate on isolated Regtest.
set -euo pipefail
repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
exec python3 "$repo_root/scripts/tests/wcash_foundation_fund_e2e.py" "$@"
