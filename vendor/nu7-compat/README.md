# Isolated NU7 dependencies

These published crate snapshots keep released Zcash parent libraries separate
from the exact prerelease libraries already used by Wcash. Cargo cannot resolve
some semver-compatible release/prerelease pairs from one registry source. Merely
adding registry patches does not create that separation.

Only dependency source paths in the normalized `Cargo.toml` files change. Every
Rust source file, feature, dependency version, test vector and license retains
its published bytes. `SNAPSHOTS.json` records each registry archive checksum and
every original file checksum. No cryptographic implementation is patched.
The narrowly selected parent-library patches route released dependencies to
explicit sibling paths; legacy Wcash dependencies retain their original sources.
Modern address parsing uses F4Jumble 0.1.2, while the legacy address library keeps
0.1.1 and its original accepted-length range.

Verify the snapshots and the resolved legacy dependency closures from the
repository root, using Python 3.11 or newer:

```sh
cargo metadata --locked --format-version 1 > /tmp/wcash-nu7-metadata.json
python3 .github/scripts/verify-nu7-parent-snapshots.py \
  --metadata /tmp/wcash-nu7-metadata.json
```

The parent chain types remain pinned to Zebra commit
`e8db87eb69254e0d4eedd058eadd48bdfa09065a`. The committed lockfile fixes the whole
resolved graph. In particular, CTUtils 0.4.8 is an intentional parent-only
dependency update from that release's lockfile; its security review and tests
must be recorded separately. Snapshot verification alone does not establish
consensus or deployment readiness.
