#!/usr/bin/env python3
"""Verify a Wcash Mainnet CompactTxStreamer endpoint."""

from __future__ import annotations

import argparse
import base64
import binascii
import ipaddress
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlsplit


SERVICE = "cash.z.wallet.sdk.rpc.CompactTxStreamer"
PROTO_DIR = Path(__file__).resolve().parents[1] / "zebra-rpc" / "proto"
GENESIS_DISPLAY_HASH = (
    "5bae12c8662a577b04ce1591af1a137c128f0cb51018a5f1622d861d1bb6fc48"
)
GENESIS_GRPC_HASH = base64.b64encode(
    bytes.fromhex(GENESIS_DISPLAY_HASH)[::-1]
).decode("ascii")
ZERO_HASH = base64.b64encode(bytes(32)).decode("ascii")
BRANCH_ID = "d9c6a7ee"


class VerificationError(RuntimeError):
    """The endpoint did not meet the Wcash wallet-service contract."""


def endpoint_parts(endpoint: str) -> tuple[str, int]:
    """Return a strictly parsed grpcurl host:port endpoint."""

    if "://" in endpoint or "/" in endpoint:
        raise VerificationError("endpoint must use host:port syntax without a URL scheme")

    parsed = urlsplit(f"//{endpoint}")
    try:
        port = parsed.port
    except ValueError as error:
        raise VerificationError(f"invalid endpoint: {error}") from error

    if (
        not parsed.hostname
        or port is None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path
        or parsed.query
        or parsed.fragment
    ):
        raise VerificationError("endpoint must include a host and port")
    return parsed.hostname, port


def is_loopback_host(host: str) -> bool:
    """Return true for an explicit loopback host."""

    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def validate_endpoint(endpoint: str, *, plaintext_loopback: bool) -> None:
    """Enforce the local-plaintext or public-production endpoint boundary."""

    host, port = endpoint_parts(endpoint)
    if plaintext_loopback:
        if not is_loopback_host(host):
            raise VerificationError("plaintext mode requires a literal loopback IP")
        return

    try:
        ipaddress.ip_address(host)
    except ValueError:
        pass
    else:
        raise VerificationError("TLS production endpoint requires a DNS hostname")
    if host.lower() == "localhost" or "." not in host or port != 443:
        raise VerificationError("TLS production endpoint requires a DNS name on port 443")


def integer_field(record: dict[str, Any], field: str) -> int:
    """Read a non-negative protobuf integer represented as JSON text or a number."""

    value = record.get(field)
    if isinstance(value, bool):
        raise VerificationError(f"{field} must be a non-negative integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as error:
        raise VerificationError(f"{field} must be a non-negative integer") from error
    if parsed < 0 or str(parsed) != str(value):
        raise VerificationError(f"{field} must be a non-negative integer")
    return parsed


def require_equal(record: dict[str, Any], field: str, expected: Any) -> None:
    """Require an exact response field value."""

    actual = record.get(field)
    if actual != expected:
        raise VerificationError(f"unexpected {field}: {actual!r}; expected {expected!r}")


def hash_field(record: dict[str, Any], field: str) -> str:
    """Read a canonical base64-encoded 32-byte hash."""

    value = record.get(field)
    if not isinstance(value, str):
        raise VerificationError(f"{field} must be a base64-encoded 32-byte hash")
    try:
        decoded = base64.b64decode(value, validate=True)
    except (ValueError, binascii.Error) as error:
        raise VerificationError(
            f"{field} must be a base64-encoded 32-byte hash"
        ) from error
    if len(decoded) != 32 or base64.b64encode(decoded).decode("ascii") != value:
        raise VerificationError(f"{field} must be a base64-encoded 32-byte hash")
    return value


def validate_lightd_identity(lightd_info: dict[str, Any]) -> dict[str, str]:
    """Validate the fields that identify the Wcash service profile."""

    require_equal(lightd_info, "chainName", "main")
    require_equal(lightd_info, "consensusBranchId", BRANCH_ID)
    require_equal(lightd_info, "taddrSupport", True)
    subversion = lightd_info.get("zcashdSubversion")
    if not (
        isinstance(subversion, str)
        and subversion.startswith("/Wcash:")
        and subversion.endswith("/")
        and len(subversion) > len("/Wcash:/")
    ):
        raise VerificationError("zcashdSubversion does not identify a Wcash build")

    version = lightd_info.get("version")
    vendor = lightd_info.get("vendor")
    build = lightd_info.get("zcashdBuild")
    if not all(isinstance(value, str) and value for value in (version, vendor, build)):
        raise VerificationError("version, vendor, and zcashdBuild must be recorded")
    return {
        "version": version,
        "vendor": vendor,
        "build": build,
        "subversion": subversion,
    }


def validate_snapshot(
    lightd_info: dict[str, Any],
    genesis: dict[str, Any],
    latest: dict[str, Any],
    latest_block: dict[str, Any],
    streamed_block: dict[str, Any],
    *,
    now: int,
    max_tip_age_seconds: int,
) -> dict[str, Any]:
    """Validate related CompactTxStreamer responses and return safe evidence."""

    identity = validate_lightd_identity(lightd_info)

    require_equal(genesis, "hash", GENESIS_GRPC_HASH)
    require_equal(genesis, "prevHash", ZERO_HASH)
    if "height" in genesis and integer_field(genesis, "height") != 0:
        raise VerificationError("genesis response has a nonzero height")

    info_height = integer_field(lightd_info, "blockHeight")
    estimated_height = integer_field(lightd_info, "estimatedHeight")
    latest_height = integer_field(latest, "height")
    latest_hash = hash_field(latest, "hash")

    require_equal(latest_block, "hash", latest_hash)
    hash_field(latest_block, "hash")
    block_height = integer_field(latest_block, "height")
    if block_height != latest_height:
        raise VerificationError("GetLatestBlock and GetBlock heights do not match")
    if latest_height < info_height or latest_height - info_height > 2:
        raise VerificationError("GetLightdInfo and GetLatestBlock tips are inconsistent")
    if estimated_height < info_height:
        raise VerificationError("estimatedHeight is below blockHeight")

    require_equal(streamed_block, "hash", latest_hash)
    hash_field(streamed_block, "hash")
    if integer_field(streamed_block, "height") != latest_height:
        raise VerificationError("GetBlockRange did not stream the requested latest block")

    block_time = integer_field(latest_block, "time")
    age = now - block_time
    if age < -300:
        raise VerificationError("latest block time is more than five minutes in the future")
    if age > max_tip_age_seconds:
        raise VerificationError(
            f"latest block is {age} seconds old; limit is {max_tip_age_seconds}"
        )

    return {
        "network": "Wcash Mainnet",
        "genesis": GENESIS_DISPLAY_HASH,
        "branchId": f"0x{BRANCH_ID}",
        **identity,
        "height": latest_height,
        "estimatedHeight": estimated_height,
        "latestHashBase64": latest_hash,
        "latestBlockTime": block_time,
        "tipAgeSeconds": max(age, 0),
    }


def grpc_call(
    grpcurl: Path,
    endpoint: str,
    method: str,
    request: dict[str, Any],
    *,
    plaintext: bool,
    timeout: int,
) -> dict[str, Any]:
    """Call one gRPC method with grpcurl and the committed protobuf files."""

    command = [
        str(grpcurl),
        "-max-time",
        str(timeout),
        "-import-path",
        str(PROTO_DIR),
        "-proto",
        "service.proto",
    ]
    if plaintext:
        command.append("-plaintext")
    command.extend(["-d", json.dumps(request, separators=(",", ":"))])
    command.extend([endpoint, f"{SERVICE}/{method}"])

    try:
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout + 5,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise VerificationError(f"grpcurl failed to run: {error}") from error

    if completed.returncode != 0:
        detail = completed.stderr.strip() or "no error detail"
        raise VerificationError(f"{method} failed: {detail}")
    try:
        response = json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        raise VerificationError(f"{method} returned invalid JSON") from error
    if not isinstance(response, dict):
        raise VerificationError(f"{method} returned a non-object response")
    return response


def verify_endpoint(
    endpoint: str,
    grpcurl: Path,
    *,
    plaintext_loopback: bool,
    timeout: int,
    max_tip_age_seconds: int,
    now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
) -> dict[str, Any]:
    """Query and validate a wallet endpoint."""

    validate_endpoint(endpoint, plaintext_loopback=plaintext_loopback)

    def call(method: str, request: dict[str, Any]) -> dict[str, Any]:
        return grpc_call(
            grpcurl,
            endpoint,
            method,
            request,
            plaintext=plaintext_loopback,
            timeout=timeout,
        )

    lightd_info = call("GetLightdInfo", {})
    validate_lightd_identity(lightd_info)
    genesis = call("GetBlock", {"hash": GENESIS_GRPC_HASH})
    latest = call("GetLatestBlock", {})
    latest_hash = hash_field(latest, "hash")
    latest_block = call("GetBlock", {"hash": latest_hash})
    latest_height = integer_field(latest, "height")
    streamed_block = call(
        "GetBlockRange",
        {
            "start": {"height": latest_height},
            "end": {"height": latest_height},
        },
    )

    observed_at = now()
    evidence = validate_snapshot(
        lightd_info,
        genesis,
        latest,
        latest_block,
        streamed_block,
        now=int(observed_at.timestamp()),
        max_tip_age_seconds=max_tip_age_seconds,
    )
    evidence.update(
        {
            "endpoint": endpoint,
            "transport": "plaintext-loopback" if plaintext_loopback else "tls",
            "observedAt": observed_at.isoformat(),
            "maxTipAgeSeconds": max_tip_age_seconds,
        }
    )
    return evidence


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Verify a TLS Wcash Mainnet CompactTxStreamer endpoint."
    )
    parser.add_argument("endpoint", help="gRPC endpoint in host:port form")
    parser.add_argument(
        "--grpcurl",
        type=Path,
        default=Path("grpcurl"),
        help="grpcurl executable (default: grpcurl from PATH)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=10,
        help="per-request timeout in seconds (default: 10)",
    )
    parser.add_argument(
        "--max-tip-age-seconds",
        type=int,
        default=1800,
        help="maximum accepted latest-block age (default: 1800)",
    )
    parser.add_argument(
        "--plaintext-loopback",
        action="store_true",
        help="allow plaintext only when endpoint uses a literal loopback IP",
    )
    args = parser.parse_args()

    if args.timeout < 1:
        parser.error("--timeout must be at least 1")
    if args.max_tip_age_seconds < 1:
        parser.error("--max-tip-age-seconds must be at least 1")

    try:
        evidence = verify_endpoint(
            args.endpoint,
            args.grpcurl,
            plaintext_loopback=args.plaintext_loopback,
            timeout=args.timeout,
            max_tip_age_seconds=args.max_tip_age_seconds,
        )
    except VerificationError as error:
        print(f"wallet service verification failed: {error}", file=sys.stderr)
        return 1

    print(json.dumps(evidence, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
