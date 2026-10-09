#!/usr/bin/env python3
"""Regression tests for the Wcash wallet-service verifier."""

from __future__ import annotations

import copy
import importlib.util
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "verify-wcash-wallet-service.py"
SPEC = importlib.util.spec_from_file_location("wallet_service_verifier", SCRIPT)
assert SPEC and SPEC.loader
verifier = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(verifier)


NOW = 1_800_000_000
LIGHTD_INFO = {
    "version": "v6.3.0",
    "vendor": "ZcashFoundation/zebra",
    "chainName": "main",
    "taddrSupport": True,
    "consensusBranchId": "d9c6a7ee",
    "blockHeight": "23164",
    "estimatedHeight": "23166",
    "zcashdBuild": "v6.3.0",
    "zcashdSubversion": "/Wcash:6.3.0/",
}
GENESIS = {
    "hash": verifier.GENESIS_GRPC_HASH,
    "prevHash": verifier.ZERO_HASH,
}
LATEST_HASH = "ERERERERERERERERERERERERERERERERERERERERERE="
LATEST = {"height": "23165", "hash": LATEST_HASH}
LATEST_BLOCK = {
    "height": "23165",
    "hash": LATEST_HASH,
    "time": NOW - 75,
}
STREAMED_BLOCK = {"height": "23165", "hash": LATEST_HASH}


def validate(
    lightd_info: dict[str, Any] = LIGHTD_INFO,
    genesis: dict[str, Any] = GENESIS,
    latest: dict[str, Any] = LATEST,
    latest_block: dict[str, Any] = LATEST_BLOCK,
    streamed_block: dict[str, Any] = STREAMED_BLOCK,
) -> dict[str, Any]:
    return verifier.validate_snapshot(
        lightd_info,
        genesis,
        latest,
        latest_block,
        streamed_block,
        now=NOW,
        max_tip_age_seconds=1800,
    )


def expect_failure(label: str, **changes: dict[str, Any]) -> None:
    fixtures = {
        "lightd_info": copy.deepcopy(LIGHTD_INFO),
        "genesis": copy.deepcopy(GENESIS),
        "latest": copy.deepcopy(LATEST),
        "latest_block": copy.deepcopy(LATEST_BLOCK),
        "streamed_block": copy.deepcopy(STREAMED_BLOCK),
    }
    for fixture, update in changes.items():
        fixtures[fixture].update(update)
    try:
        validate(**fixtures)
    except verifier.VerificationError:
        return
    raise AssertionError(f"{label}: contradictory evidence was accepted")


def main() -> None:
    evidence = validate()
    assert evidence["genesis"] == verifier.GENESIS_DISPLAY_HASH
    assert evidence["branchId"] == "0xd9c6a7ee"
    assert evidence["height"] == 23165

    expect_failure("wrong chain", lightd_info={"chainName": "test"})
    expect_failure("wrong branch", lightd_info={"consensusBranchId": "c2d6d0b4"})
    expect_failure("no transparent support", lightd_info={"taddrSupport": False})
    expect_failure(
        "not Wcash",
        lightd_info={"zcashdSubversion": "/Zebra:6.3.0/"},
    )
    expect_failure("wrong genesis", genesis={"hash": "wrong"})
    expect_failure("non-genesis predecessor", genesis={"prevHash": "not-zero"})
    expect_failure("nonzero genesis height", genesis={"height": "999"})
    expect_failure("malformed latest hash", latest={"hash": "not-base64"})
    expect_failure(
        "short latest hash",
        latest={"hash": "c2hvcnQ="},
    )
    expect_failure(
        "tip hash mismatch",
        latest_block={"hash": "IiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiI="},
    )
    expect_failure("tip height mismatch", latest_block={"height": "23164"})
    expect_failure("stale lightd info", lightd_info={"blockHeight": "23160"})
    expect_failure("impossible estimate", lightd_info={"estimatedHeight": "23163"})
    expect_failure("stale tip", latest_block={"time": NOW - 1801})
    expect_failure("future tip", latest_block={"time": NOW + 301})
    expect_failure("boolean height", latest={"height": True})
    expect_failure(
        "stream hash mismatch",
        streamed_block={"hash": "IiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiI="},
    )
    expect_failure("stream height mismatch", streamed_block={"height": "23164"})

    assert verifier.is_loopback_host("127.0.0.1")
    assert verifier.is_loopback_host("::1")
    assert not verifier.is_loopback_host("localhost")
    assert not verifier.is_loopback_host("mainnet.example.com")
    assert verifier.endpoint_parts("mainnet.example.com:443") == (
        "mainnet.example.com",
        443,
    )

    verifier.validate_endpoint("127.0.0.1:48234", plaintext_loopback=True)
    verifier.validate_endpoint("mainnet.example.com:443", plaintext_loopback=False)

    invalid_endpoints = [
        "https://mainnet.example.com:443",
        "mainnet.example.com",
        "mainnet.example.com/path",
        "user@127.0.0.1:48234",
        "127.0.0.1:48234?x=1",
    ]
    for endpoint in invalid_endpoints:
        try:
            verifier.endpoint_parts(endpoint)
        except verifier.VerificationError:
            continue
        raise AssertionError(f"invalid endpoint was accepted: {endpoint}")

    invalid_plaintext = ["localhost:48234", "mainnet.example.com:48234"]
    for endpoint in invalid_plaintext:
        try:
            verifier.validate_endpoint(endpoint, plaintext_loopback=True)
        except verifier.VerificationError:
            continue
        raise AssertionError(f"non-literal loopback was accepted: {endpoint}")

    invalid_tls = ["127.0.0.1:443", "localhost:443", "mainnet.example.com:8443"]
    for endpoint in invalid_tls:
        try:
            verifier.validate_endpoint(endpoint, plaintext_loopback=False)
        except verifier.VerificationError:
            continue
        raise AssertionError(f"invalid production TLS endpoint was accepted: {endpoint}")

    print("wallet service verifier: all regression cases passed")


if __name__ == "__main__":
    main()
