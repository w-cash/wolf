#!/usr/bin/env python3
"""Verify immutable crate source and the optional Cargo isolation report."""

import argparse
import hashlib
import json
from pathlib import Path
import re
import tomllib


def digest(data):
    return hashlib.sha256(data).hexdigest()


def verify_sources(root):
    snapshots = root / "vendor/nu7-compat"
    entries = json.loads((snapshots / "SNAPSHOTS.json").read_text())
    names = {entry["name"] for entry in entries}
    assert {path.name for path in snapshots.iterdir() if path.is_dir()} == names

    def check_paths(value):
        for key, child in value.items():
            if key in {"dependencies", "dev-dependencies", "build-dependencies"}:
                for alias, specification in child.items():
                    if isinstance(specification, dict) and "path" in specification:
                        package = specification.get("package", alias)
                        assert package in names and specification["path"] == f"../{package}"
            elif isinstance(child, dict):
                check_paths(child)

    for entry in entries:
        directory = snapshots / entry["name"]
        check_paths(tomllib.loads((directory / "Cargo.toml").read_text()))
        expected_files = entry["files"]
        actual_files = {
            str(path.relative_to(directory))
            for path in directory.rglob("*")
            if path.is_file()
        }
        assert actual_files == set(expected_files), entry["name"]
        for relative, expected_hash in expected_files.items():
            data = (directory / relative).read_bytes()
            if relative == "Cargo.toml":
                # Registry manifests have no local paths. Removing only these
                # explicit sibling-source additions must recover the exact
                # authenticated published manifest, including versions/features.
                data = re.sub(rb'^path = "\.\./[^"\n]+"\n', b"", data, flags=re.M)
            assert digest(data) == expected_hash, (entry["name"], relative)
    return len(entries)


def verify_graph(root, metadata_file):
    metadata = json.loads(metadata_file.read_text())
    packages = {package["id"]: package for package in metadata["packages"]}
    nodes = {node["id"]: node for node in metadata["resolve"]["nodes"]}
    snapshots = (root / "vendor/nu7-compat").resolve()

    def is_snapshot(package_id):
        manifest = Path(packages[package_id]["manifest_path"]).resolve()
        return manifest.is_relative_to(snapshots)

    assert not any(is_snapshot(key) for key in metadata["workspace_members"])
    checked = []
    for name, version in [
        ("zebra-chain", "12.0.0"),
        ("zcash_primitives", "0.30.1"),
        ("zcash_protocol", "0.10.5"),
        ("zcash_address", "0.13.0"),
    ]:
        matches = [
            key
            for key, value in packages.items()
            if value["name"] == name and value["version"] == version
        ]
        assert len(matches) == 1, (name, version)
        visited, pending = set(), matches.copy()
        while pending:
            key = pending.pop()
            if key in visited:
                continue
            visited.add(key)
            assert not is_snapshot(key), (name, packages[key]["name"])
            pending.extend(nodes[key]["dependencies"])
        if name == "zcash_address":
            jumble = [packages[key] for key in visited if packages[key]["name"] == "f4jumble"]
            assert len(jumble) == 1 and jumble[0]["version"] == "0.1.1"
            assert jumble[0]["source"].startswith("registry+")
        checked.append(f"{name}@{version}")
    return checked


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--metadata", type=Path, help="cargo metadata --locked JSON")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    result = {"snapshots_verified": verify_sources(root)}
    if args.metadata:
        result["legacy_dependency_closures_verified"] = verify_graph(root, args.metadata)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
