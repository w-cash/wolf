#!/usr/bin/env python3
"""Validate relationships in the Wcash release manifest."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


PLACEHOLDERS = {"unknown", "not released", "not provided"}


def is_recorded(value: Any) -> bool:
    """Return true when a string contains recorded evidence."""

    return isinstance(value, str) and value not in PLACEHOLDERS


def validate_manifest(manifest: dict[str, Any]) -> list[str]:
    """Return cross-field consistency errors for a schema-valid manifest."""

    errors: list[str] = []
    release = manifest["release"]
    source = manifest["source"]
    artifacts = manifest["artifacts"]
    signatures = manifest["signatures"]
    ci = manifest["ci"]
    deployment = manifest["deployment"]

    release_status = release["status"]
    artifact_status = artifacts["status"]
    if artifact_status != release_status:
        errors.append(
            "artifacts.status must equal release.status "
            f"({artifact_status!r} != {release_status!r})"
        )

    if release_status in {"candidate", "released"}:
        for field in ("version", "tag"):
            if not is_recorded(release[field]):
                errors.append(
                    f"release.{field} must be recorded for {release_status!r}"
                )

    ci_status = ci["status"]
    if ci_status in {"passed", "failed"}:
        if not is_recorded(ci["commit"]):
            errors.append(f"ci.commit must be recorded when ci.status is {ci_status!r}")
        if not is_recorded(ci["runUrl"]):
            errors.append(f"ci.runUrl must be recorded when ci.status is {ci_status!r}")
        if ci["commit"] != source["commit"]:
            errors.append("ci.commit must equal source.commit")

    if release_status == "released" and ci_status != "passed":
        errors.append("ci.status must be 'passed' for a released manifest")

    artifact_names = {item["name"] for item in artifacts["items"]}
    artifact_hashes = {item["sha256"] for item in artifacts["items"]}

    if signatures["status"] == "provided" and not signatures["items"]:
        errors.append(
            "signatures.items must contain at least one entry when "
            "signatures.status is 'provided'"
        )

    for signature in signatures["items"]:
        if signature["artifact"] not in artifact_names:
            errors.append(
                "signature artifact must name an entry in artifacts.items: "
                f"{signature['artifact']!r}"
            )

    if deployment["status"] == "deployed":
        if release_status != "released":
            errors.append("deployment.status 'deployed' requires release.status 'released'")
        if deployment["sourceCommit"] != source["commit"]:
            errors.append("deployment.sourceCommit must equal source.commit")
        if deployment["artifactSha256"] not in artifact_hashes:
            errors.append(
                "deployment.artifactSha256 must match an artifact in artifacts.items"
            )

    return errors


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate cross-field Wcash release manifest rules."
    )
    parser.add_argument(
        "manifest",
        nargs="?",
        default="wcash-release-manifest.json",
        type=Path,
    )
    args = parser.parse_args()

    try:
        with args.manifest.open(encoding="utf-8") as manifest_file:
            manifest = json.load(manifest_file)
    except (OSError, json.JSONDecodeError) as error:
        print(f"{args.manifest}: {error}", file=sys.stderr)
        return 1

    errors = validate_manifest(manifest)
    if errors:
        for error in errors:
            print(f"{args.manifest}: {error}", file=sys.stderr)
        return 1

    print(f"{args.manifest}: release evidence is consistent")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
