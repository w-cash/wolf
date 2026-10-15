#!/usr/bin/env bash
# Build isolated OLD, shipping-policy, test-fixture and Zcash-parent artifacts.
set -euo pipefail
repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
base_sha=41a39cab3f259509653c1d4930df3dae3dabba98
baseline_source="${WCASH_FOUNDATION_BASELINE_SOURCE:-$repo_root/target/foundation-source-baseline}"
artifacts="$repo_root/target/foundation-artifacts"
cargo_bin="${CARGO:-cargo}"

# Local evidence builds are deliberately space bounded. Shipping releases must
# separately pass the ordinary release packaging workflow and independent review.
export CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-4}"
export CARGO_INCREMENTAL=0
export CARGO_PROFILE_RELEASE_DEBUG=0
export CARGO_PROFILE_RELEASE_LTO="${CARGO_PROFILE_RELEASE_LTO:-false}"
export CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16
mkdir -p "$artifacts" "$repo_root/target/foundation-evidence"
if [[ ! -e "$baseline_source" ]]; then
  git worktree add --detach "$baseline_source" "$base_sha"
fi
[[ "$(git -C "$baseline_source" rev-parse HEAD)" == "$base_sha" ]] || {
  echo 'baseline checkout does not match the pinned pre-upgrade revision' >&2; exit 1;
}
[[ -z "$(git -C "$baseline_source" status --porcelain)" ]] || {
  echo 'baseline checkout contains local modifications' >&2; exit 1;
}

build_wcash() {
  local source=$1 target=$2 destination=$3 features=$4
  mkdir -p "$destination"
  (cd "$source"; "$cargo_bin" build --locked --release --target-dir "$target" \
    -p zebrad -p wcash-merge-miner -p wcash-wallet --no-default-features \
    --features "$features")
  cp "$target/release/zebrad" "$destination/wcash-zebrad"
  cp "$target/release/wcash-merge-miner" "$destination/wcash-merge-miner"
  cp "$target/release/wcash-wallet" "$destination/wcash-wallet"
}
build_wcash "$baseline_source" "$repo_root/target/foundation-baseline" \
  "$artifacts/baseline" zebrad/wcash-consensus
# APFS clones share immutable dependency pages while keeping four separate
# writable build directories. Cargo still rebuilds changed sources/features.
clone_cache() {
  if [[ "$(uname -s)" == Darwin && ! -e "$2" ]]; then
    cp -cR "$1" "$2"
  fi
}
clone_cache "$repo_root/target/foundation-baseline" "$repo_root/target/foundation-production"
build_wcash "$repo_root" "$repo_root/target/foundation-production" \
  "$artifacts/production" zebrad/wcash-consensus

mkdir -p "$artifacts/fixture"
clone_cache "$repo_root/target/foundation-production" "$repo_root/target/foundation-fixture"
"$cargo_bin" build --locked --release --target-dir "$repo_root/target/foundation-fixture" \
  -p zebrad -p wcash-merge-miner --no-default-features \
  --features zebrad/wcash-funding-test-fixture,wcash-merge-miner/wcash-funding-test-fixture
cp "$repo_root/target/foundation-fixture/release/zebrad" "$artifacts/fixture/wcash-zebrad"
cp "$repo_root/target/foundation-fixture/release/wcash-merge-miner" "$artifacts/fixture/wcash-merge-miner"
# Wallet operations use the ordinary production wallet; it has no policy override.
cp "$artifacts/production/wcash-wallet" "$artifacts/fixture/wcash-wallet"

mkdir -p "$artifacts/parent"
clone_cache "$repo_root/target/foundation-production" "$repo_root/target/foundation-parent"
"$cargo_bin" build --locked --release --target-dir "$repo_root/target/foundation-parent" \
  -p zebrad --bin zebrad --no-default-features
cp "$repo_root/target/foundation-parent/release/zebrad" "$artifacts/parent/zcash-zebrad"
python3 scripts/verify-wcash-funding-artifacts.py \
  "$artifacts/production/wcash-zebrad" "$artifacts/production/wcash-merge-miner"
python3 scripts/verify-wcash-funding-artifacts.py --fixture \
  "$artifacts/fixture/wcash-zebrad" "$artifacts/fixture/wcash-merge-miner"
python3 scripts/verify-wcash-funding-artifacts.py --allow-zcash "$artifacts/parent/zcash-zebrad"
python3 - "$artifacts" "$base_sha" <<'PY'
import hashlib, json, os, pathlib, subprocess, sys
root, base = pathlib.Path(sys.argv[1]), sys.argv[2]
source_digest = hashlib.sha256()
paths = subprocess.check_output(["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"]).split(b"\0")
for name in sorted(set(paths) - {b""}):
    path = pathlib.Path(os.fsdecode(name))
    if path.is_file():
        source_digest.update(name + b"\0" + hashlib.sha256(path.read_bytes()).digest())
manifest = {
    "baseline_sha": base,
    "head_sha": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
    "tracked_diff_sha256": hashlib.sha256(subprocess.check_output(["git", "diff", "HEAD"])).hexdigest(),
    "source_tree_sha256": source_digest.hexdigest(),
    "rustc": subprocess.check_output(["rustc", "--version"], text=True).strip(),
    "release_lto": os.environ["CARGO_PROFILE_RELEASE_LTO"],
    "release_debug": 0,
    "codegen_units": 16,
    "features": {"baseline": "zebrad/wcash-consensus", "production": "zebrad/wcash-consensus",
                 "fixture": "zebrad/wcash-funding-test-fixture,wcash-merge-miner/wcash-funding-test-fixture",
                 "parent": "no-default-features"},
    "binaries": {},
}
for path in sorted(root.glob("*/*")):
    if path.is_file():
        manifest["binaries"][str(path.relative_to(root))] = hashlib.sha256(path.read_bytes()).hexdigest()
(root / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
print(root / "manifest.json")
PY
echo "Isolated artifacts ready in $artifacts; no fixture was copied to target/release."
