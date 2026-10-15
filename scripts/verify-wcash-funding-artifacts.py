#!/usr/bin/env python3
"""Fail closed if a release node/coordinator contains the local funding fixture."""

import argparse
import json
import subprocess


def verify(path, fixture=False, allow_zcash=False):
    report = json.loads(subprocess.check_output([str(path), "funding-policy"], timeout=30))
    if not isinstance(report, dict):
        raise SystemExit(f"refusing funding artifact {path}: policy report must be an object")
    if allow_zcash:
        if fixture:
            raise SystemExit("--allow-zcash cannot be combined with --fixture")
        if report.get("fixture") is not False:
            raise SystemExit(f"refusing release artifact {path}: fixture must be false")
        if report.get("consensus_profile") == "zcash":
            return report
        if report.get("consensus_profile") != "wcash":
            raise SystemExit(f"refusing release artifact {path}: unknown consensus profile")
    expected = {
        "fixture": fixture,
        "network": "WcashRegtest" if fixture else "WcashMainnet",
        "start_height": 10 if fixture else 40001,
        "end_height_exclusive": 20 if fixture else 621455,
        "recipient": "WRHUq9CTLZFa52NmAyHH5usN21Q8jZVskN1" if fixture else
                     "W1ZRudbQKz7jQdKmAarbaJsegscnZzXjpu2",
        "script_pub_key": "76a9147d437bac720b610692ae373e43fa654c50c196f288ac" if fixture else
                          "76a9148708e78a0203f93b2691057281e1773fd263702088ac",
        "divisor": 5,
    }
    for key, value in expected.items():
        if type(report.get(key)) is not type(value) or report.get(key) != value:
            raise SystemExit(f"refusing funding artifact {path}: {key}={report.get(key)!r}, expected {value!r}")
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binaries", nargs="+")
    parser.add_argument("--fixture", action="store_true", help="verify isolated local artifacts instead of release artifacts")
    parser.add_argument("--allow-zcash", action="store_true", help="also accept explicitly identified default Zcash nodes in shared packaging")
    args = parser.parse_args()
    for binary in args.binaries:
        print(json.dumps({"binary": binary, "policy": verify(binary, args.fixture, args.allow_zcash)}, sort_keys=True))
