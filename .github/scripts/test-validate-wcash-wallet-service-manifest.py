#!/usr/bin/env python3
"""Contradiction tests for the Wcash wallet-service manifest."""

from __future__ import annotations

import copy
import importlib.util
import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
VALIDATOR_PATH = ROOT / ".github/scripts/validate-wcash-wallet-service-manifest.py"
SPEC = importlib.util.spec_from_file_location("wallet_service_manifest", VALIDATOR_PATH)
assert SPEC and SPEC.loader
validator = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(validator)

with (ROOT / "wcash-wallet-service-manifest.json").open(encoding="utf-8") as file:
    BASE = json.load(file)


def ready_manifest() -> dict[str, Any]:
    manifest = copy.deepcopy(BASE)
    service = manifest["canonicalService"]
    service.update(
        {
            "status": "ready",
            "endpoint": "https://wallet-mainnet.wcashwallet.com:443",
            "transport": "tls",
            "implementation": "wolf-compact-tx-streamer",
        }
    )
    service["deployment"] = {
        "sourceCommit": "a" * 40,
        "artifactSha256": "b" * 64,
        "buildFeatures": ["wcash-consensus"],
        "configurationRevision": "deployment-2026-10-09",
        "deployedAt": "2026-10-09T16:00:00Z",
        "evidenceUrl": "https://example.com/deployment/1",
    }
    service["validation"] = {
        "status": "passed",
        "endpoint": "https://wallet-mainnet.wcashwallet.com:443",
        "checkedAt": "2026-10-09T16:05:00Z",
        "verifier": "scripts/verify-wcash-wallet-service.py",
        "resultUrl": "https://example.com/deployment/1/check",
    }
    manifest["knownLimitations"] = [
        "The integrated CompactTxStreamer service remains experimental until "
        "separately qualified with the wallet applications.",
        "Endpoint readiness does not establish desktop or mobile wallet "
        "release readiness.",
    ]
    return manifest


def expect_valid(label: str, manifest: dict[str, Any]) -> None:
    errors = validator.validate_manifest(manifest)
    if errors:
        raise AssertionError(f"{label}: expected valid manifest, got {errors}")


def expect_invalid(label: str, manifest: dict[str, Any]) -> None:
    if not validator.validate_manifest(manifest):
        raise AssertionError(f"{label}: contradictory manifest was accepted")


def changed(base: dict[str, Any], path: tuple[str, ...], value: Any) -> dict[str, Any]:
    manifest = copy.deepcopy(base)
    target: dict[str, Any] = manifest
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    return manifest


def main() -> None:
    expect_valid("initial manifest", BASE)
    ready = ready_manifest()
    expect_valid("fully evidenced ready service", ready)

    contradictions = {
        "unsupported withdrawn service": changed(
            changed(
                changed(
                    changed(
                        BASE,
                        ("canonicalService", "status"),
                        "withdrawn",
                    ),
                    ("canonicalService", "endpoint"),
                    "http://evil.example:9999",
                ),
                ("canonicalService", "transport"),
                "tls",
            ),
            ("canonicalService", "implementation"),
            "wolf-compact-tx-streamer",
        ),
        "plaintext ready service": changed(
            ready, ("canonicalService", "transport"), "not deployed"
        ),
        "HTTP ready endpoint": changed(
            ready,
            ("canonicalService", "endpoint"),
            "http://wallet-mainnet.wcashwallet.com:443",
        ),
        "wrong TLS port": changed(
            ready,
            ("canonicalService", "endpoint"),
            "https://wallet-mainnet.wcashwallet.com:8443",
        ),
        "loopback ready endpoint": changed(
            ready,
            ("canonicalService", "endpoint"),
            "https://127.0.0.1:443",
        ),
        "unknown implementation": changed(
            ready, ("canonicalService", "implementation"), "unknown"
        ),
        "unknown source": changed(
            ready,
            ("canonicalService", "deployment", "sourceCommit"),
            "unknown",
        ),
        "unknown artifact": changed(
            ready,
            ("canonicalService", "deployment", "artifactSha256"),
            "unknown",
        ),
        "missing Wcash feature": changed(
            ready,
            ("canonicalService", "deployment", "buildFeatures"),
            [],
        ),
        "unknown configuration": changed(
            ready,
            ("canonicalService", "deployment", "configurationRevision"),
            "unknown",
        ),
        "placeholder configuration": changed(
            ready,
            ("canonicalService", "deployment", "configurationRevision"),
            "not provided",
        ),
        "ready without check": changed(
            ready,
            ("canonicalService", "validation", "status"),
            "not run",
        ),
        "ready with not-deployed limitation": changed(
            ready,
            ("knownLimitations",),
            [sorted(validator.NOT_DEPLOYED_LIMITATIONS)[0]],
        ),
        "not deployed with stale check evidence": changed(
            BASE,
            ("canonicalService", "validation", "checkedAt"),
            "2026-10-09T16:05:00Z",
        ),
        "candidate not run with check endpoint": changed(
            changed(
                ready,
                ("canonicalService", "validation", "status"),
                "not run",
            ),
            ("canonicalService", "validation", "endpoint"),
            "https://wallet-mainnet.wcashwallet.com:443",
        ),
        "passed without timestamp": changed(
            ready,
            ("canonicalService", "validation", "checkedAt"),
            "unknown",
        ),
        "check for another endpoint": changed(
            ready,
            ("canonicalService", "validation", "endpoint"),
            "https://other.example.com:443",
        ),
        "check before deployment": changed(
            changed(
                ready,
                ("canonicalService", "deployment", "deployedAt"),
                "2026-10-09T17:00:00Z",
            ),
            ("canonicalService", "validation", "checkedAt"),
            "2026-10-09T16:00:00Z",
        ),
        "plaintext made canonical": changed(
            ready,
            ("legacyObservations",),
            [
                {
                    **ready["legacyObservations"][0],
                    "endpoint": ready["canonicalService"]["endpoint"],
                }
            ],
        ),
        "remote source treated as proven": changed(
            ready,
            ("legacyObservations",),
            [
                {
                    **ready["legacyObservations"][0],
                    "sourceCommit": "c" * 40,
                }
            ],
        ),
    }
    for label, manifest in contradictions.items():
        expect_invalid(label, manifest)

    print("wallet service manifest: all contradiction cases passed")


if __name__ == "__main__":
    main()
