#!/usr/bin/env python3
"""Validate relationships in the Wcash wallet-service manifest."""

from __future__ import annotations

import argparse
import ipaddress
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit


PLACEHOLDERS = {
    "unknown",
    "not deployed",
    "not provided",
    "not recorded",
    "not released",
    "not run",
}
NOT_DEPLOYED_LIMITATIONS = {
    "No canonical TLS Wcash Mainnet wallet service is recorded as deployed.",
    "No exact production source commit, artifact digest or configuration revision is recorded.",
}


def is_recorded(value: Any) -> bool:
    return isinstance(value, str) and value not in PLACEHOLDERS


def is_canonical_tls_endpoint(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    parsed = urlsplit(value)
    try:
        port = parsed.port
    except ValueError:
        return False
    hostname = parsed.hostname
    if hostname is None or hostname.lower() == "localhost":
        return False
    try:
        ipaddress.ip_address(hostname)
        return False
    except ValueError:
        pass
    return (
        parsed.scheme == "https"
        and "." in hostname
        and port == 443
        and parsed.username is None
        and parsed.password is None
        and parsed.path in {"", "/"}
        and not parsed.query
        and not parsed.fragment
    )


def parse_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str) or value in PLACEHOLDERS:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def validate_manifest(manifest: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    service = manifest["canonicalService"]
    deployment = service["deployment"]
    validation = service["validation"]
    status = service["status"]

    if status == "not deployed":
        if service["endpoint"] != "not deployed":
            errors.append("not deployed service must use endpoint 'not deployed'")
        if service["transport"] != "not deployed":
            errors.append("not deployed service must use transport 'not deployed'")
        if service["implementation"] != "unknown":
            errors.append("not deployed service implementation must be 'unknown'")
        for field in (
            "sourceCommit",
            "artifactSha256",
            "configurationRevision",
            "deployedAt",
            "evidenceUrl",
        ):
            if deployment[field] != "unknown":
                errors.append(f"not deployed service must leave deployment.{field} unknown")
        if deployment["buildFeatures"]:
            errors.append("not deployed service must have no deployment build features")
        if validation["status"] != "not run":
            errors.append("not deployed service validation status must be 'not run'")
        for field in ("endpoint", "checkedAt", "resultUrl"):
            if validation[field] != "unknown":
                errors.append(
                    f"not deployed service must leave validation.{field} unknown"
                )

    if status in {"candidate", "ready"}:
        if service["transport"] != "tls":
            errors.append(f"{status} service transport must be 'tls'")
        if not is_canonical_tls_endpoint(service["endpoint"]):
            errors.append(f"{status} service endpoint must be an HTTPS URL on port 443")
        if service["implementation"] != "wolf-compact-tx-streamer":
            errors.append(f"{status} service must identify the Wolf implementation")
        for field in (
            "sourceCommit",
            "artifactSha256",
            "configurationRevision",
            "deployedAt",
            "evidenceUrl",
        ):
            if not is_recorded(deployment[field]):
                errors.append(f"deployment.{field} must be recorded for {status!r}")
        if "wcash-consensus" not in deployment["buildFeatures"]:
            errors.append(f"deployment build features must include wcash-consensus for {status!r}")
        stale_limitations = NOT_DEPLOYED_LIMITATIONS.intersection(
            manifest["knownLimitations"]
        )
        if stale_limitations:
            errors.append(
                f"{status} service retains not-deployed limitation text: "
                f"{sorted(stale_limitations)!r}"
            )

    validation_status = validation["status"]
    if validation_status in {"passed", "failed"}:
        for field in ("endpoint", "checkedAt", "resultUrl"):
            if not is_recorded(validation[field]):
                errors.append(
                    f"validation.{field} must be recorded when validation status is "
                    f"{validation_status!r}"
                )
        if validation["endpoint"] != service["endpoint"]:
            errors.append("validation.endpoint must equal canonicalService.endpoint")

        deployed_at = parse_datetime(deployment["deployedAt"])
        checked_at = parse_datetime(validation["checkedAt"])
        if deployed_at is not None and checked_at is not None and checked_at < deployed_at:
            errors.append("validation.checkedAt must not predate deployment.deployedAt")

    if validation_status == "not run":
        for field in ("endpoint", "checkedAt", "resultUrl"):
            if validation[field] != "unknown":
                errors.append(
                    f"validation.{field} must be unknown when validation was not run"
                )

    if status == "ready" and validation_status != "passed":
        errors.append("ready service requires passed validation")

    if validation_status == "passed" and status not in {"candidate", "ready"}:
        errors.append("passed validation requires a candidate or ready service")

    canonical_endpoint = service["endpoint"]
    for observation in manifest["legacyObservations"]:
        if observation["endpoint"] == canonical_endpoint:
            errors.append("a legacy plaintext endpoint cannot be canonical")
        if observation["sourceCommit"] != "unknown":
            errors.append(
                "legacy observation sourceCommit must stay unknown without operator evidence"
            )

    return errors


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate Wcash wallet-service manifest relationships."
    )
    parser.add_argument(
        "manifest",
        nargs="?",
        default="wcash-wallet-service-manifest.json",
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

    print(f"{args.manifest}: wallet-service evidence is consistent")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
