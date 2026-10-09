#!/usr/bin/env python3
"""Regression tests for Wcash release manifest consistency checks."""

from __future__ import annotations

import copy
import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from typing import Any, Callable


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
MANIFEST_PATH = REPOSITORY_ROOT / "wcash-release-manifest.json"
VALIDATOR_PATH = (
    REPOSITORY_ROOT / ".github/scripts/validate-wcash-release-manifest.py"
)
SOURCE_COMMIT = "a" * 40
ARTIFACT_SHA256 = "b" * 64


def released_manifest() -> dict[str, Any]:
    with MANIFEST_PATH.open(encoding="utf-8") as manifest_file:
        manifest = json.load(manifest_file)

    manifest["release"] = {
        "status": "released",
        "version": "1.0.0",
        "tag": "v1.0.0",
        "publishedAt": "2026-10-09T00:00:00Z",
    }
    manifest["source"] = {"commit": SOURCE_COMMIT, "treeState": "clean"}
    manifest["build"]["target"] = "x86_64-unknown-linux-gnu"
    manifest["artifacts"] = {
        "status": "released",
        "items": [
            {
                "name": "zebrad-x86_64-unknown-linux-gnu.tar.gz",
                "target": "x86_64-unknown-linux-gnu",
                "bytes": 1,
                "sha256": ARTIFACT_SHA256,
                "downloadUrl": "https://example.com/zebrad.tar.gz",
            }
        ],
    }
    manifest["ci"] = {
        "status": "passed",
        "workflow": ".github/workflows/wcash-release-gate.yml",
        "commit": SOURCE_COMMIT,
        "runUrl": "https://github.com/w-cash/wolf/actions/runs/1",
    }
    return manifest


def run_validator(manifest: dict[str, Any]) -> subprocess.CompletedProcess[str]:
    with tempfile.TemporaryDirectory() as temporary_directory:
        manifest_path = Path(temporary_directory) / "manifest.json"
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        return subprocess.run(
            ["python3", str(VALIDATOR_PATH), str(manifest_path)],
            check=False,
            capture_output=True,
            text=True,
        )


class ReleaseManifestValidatorTests(unittest.TestCase):
    def assert_invalid(
        self,
        manifest: dict[str, Any],
        expected_error: str,
    ) -> None:
        result = run_validator(manifest)
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn(expected_error, result.stderr)

    def test_committed_manifest_is_valid(self) -> None:
        with MANIFEST_PATH.open(encoding="utf-8") as manifest_file:
            result = run_validator(json.load(manifest_file))
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_unsigned_release_is_valid(self) -> None:
        result = run_validator(released_manifest())
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_matching_deployment_is_valid(self) -> None:
        manifest = released_manifest()
        manifest["deployment"] = {
            "status": "deployed",
            "environment": "production",
            "sourceCommit": SOURCE_COMMIT,
            "artifactSha256": ARTIFACT_SHA256,
            "configurationRevision": "config-v1",
            "deployedAt": "2026-10-09T01:00:00Z",
            "evidenceUrl": "https://example.com/deployment",
        }
        result = run_validator(manifest)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_contradictory_fixtures_are_rejected(self) -> None:
        with MANIFEST_PATH.open(encoding="utf-8") as manifest_file:
            unreleased = json.load(manifest_file)

        fixture_changes: dict[
            str, tuple[dict[str, Any], Callable[[dict[str, Any]], None], str]
        ] = {
            "released artifacts for an unreleased release": (
                unreleased,
                lambda manifest: manifest["artifacts"].update(status="released"),
                "artifacts.status must equal release.status",
            ),
            "passed CI without evidence": (
                unreleased,
                lambda manifest: manifest["ci"].update(status="passed"),
                "ci.commit must be recorded",
            ),
            "provided signatures without entries": (
                unreleased,
                lambda manifest: manifest["signatures"].update(status="provided"),
                "signatures.items must contain at least one entry",
            ),
            "placeholder release identity": (
                released_manifest(),
                lambda manifest: manifest["release"].update(
                    version="unknown", tag="unknown"
                ),
                "release.version must be recorded",
            ),
            "different source and CI commits": (
                released_manifest(),
                lambda manifest: manifest["ci"].update(commit="c" * 40),
                "ci.commit must equal source.commit",
            ),
            "deployment unrelated to release evidence": (
                released_manifest(),
                lambda manifest: manifest.update(
                    deployment={
                        "status": "deployed",
                        "environment": "production",
                        "sourceCommit": "d" * 40,
                        "artifactSha256": "e" * 64,
                        "configurationRevision": "config-v2",
                        "deployedAt": "2026-10-09T01:00:00Z",
                        "evidenceUrl": "https://example.com/deployment",
                    }
                ),
                "deployment.sourceCommit must equal source.commit",
            ),
        }

        for name, (base, mutate, expected_error) in fixture_changes.items():
            with self.subTest(name=name):
                fixture = copy.deepcopy(base)
                mutate(fixture)
                self.assert_invalid(fixture, expected_error)


if __name__ == "__main__":
    unittest.main()
