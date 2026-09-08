#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
profile_target_root="${WCASH_PROFILE_TARGET_DIR:-$repo_root/target/wcash-profile-builds}"
release_dir="$repo_root/target/release"
cargo_bin="${CARGO:-cargo}"

zcash_target="$profile_target_root/zcash"
wcash_target="$profile_target_root/wcash"

"$cargo_bin" build --locked --release -p zebrad --bin zebrad \
  --no-default-features --target-dir "$zcash_target"
"$cargo_bin" build --locked --release -p zebrad --bin zebrad \
  --no-default-features --features wcash-consensus --target-dir "$wcash_target"

"$cargo_bin" build --locked --release \
  -p wcash-merge-miner --bin wcash-merge-miner \
  -p wcash-wallet --bin wcash-wallet \
  --target-dir "$repo_root/target"

mkdir -p "$release_dir"
cp "$zcash_target/release/zebrad" "$release_dir/zcash-zebrad"
cp "$wcash_target/release/zebrad" "$release_dir/wcash-zebrad"
chmod 0755 "$release_dir/zcash-zebrad" "$release_dir/wcash-zebrad"

if cmp -s "$release_dir/zcash-zebrad" "$release_dir/wcash-zebrad"; then
  echo "refusing identical Zcash and Wcash consensus binaries" >&2
  exit 1
fi

echo "Built isolated consensus binaries:"
echo "  $release_dir/zcash-zebrad"
echo "  $release_dir/wcash-zebrad"
echo "Built coordinator and wallet:"
echo "  $release_dir/wcash-merge-miner"
echo "  $release_dir/wcash-wallet"
