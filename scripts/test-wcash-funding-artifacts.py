#!/usr/bin/env python3
"""Non-solving regression checks for release artifact policy inspection."""

import importlib.util
import json
import pathlib
import unittest
from unittest.mock import patch


spec = importlib.util.spec_from_file_location(
    "funding_artifacts", pathlib.Path(__file__).with_name("verify-wcash-funding-artifacts.py")
)
verifier = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verifier)


class ReleaseFundingArtifacts(unittest.TestCase):
    def setUp(self):
        self.report = {
            "consensus_profile": "wcash",
            "fixture": False,
            "network": "WcashMainnet",
            "start_height": 40001,
            "end_height_exclusive": 621455,
            "recipient": "W1ZRudbQKz7jQdKmAarbaJsegscnZzXjpu2",
            "script_pub_key": "76a9148708e78a0203f93b2691057281e1773fd263702088ac",
            "divisor": 5,
        }

    def verify(self, report=None, **kwargs):
        encoded = json.dumps(self.report if report is None else report).encode()
        with patch.object(verifier.subprocess, "check_output", return_value=encoded) as command:
            result = verifier.verify("/disposable/test-node", **kwargs)
            command.assert_called_once_with(
                ["/disposable/test-node", "funding-policy"], timeout=30
            )
            return result

    def test_production_parameters_are_accepted(self):
        self.assertEqual(self.verify(), self.report)
        self.assertEqual(self.verify(allow_zcash=True), self.report)

    def test_changed_destination_term_or_rate_is_refused(self):
        for key in ["recipient", "script_pub_key", "start_height", "end_height_exclusive", "divisor"]:
            with self.subTest(key=key):
                report = dict(self.report, **{key: "altered"})
                with self.assertRaises(SystemExit):
                    self.verify(report)

    def test_fixture_substitution_is_refused_by_release_paths(self):
        report = dict(self.report, fixture=True)
        for allow_zcash in [False, True]:
            with self.subTest(allow_zcash=allow_zcash), self.assertRaises(SystemExit):
                self.verify(report, allow_zcash=allow_zcash)

    def test_default_zcash_requires_explicit_shared_packaging_option(self):
        report = {"consensus_profile": "zcash", "fixture": False}
        with self.assertRaises(SystemExit):
            self.verify(report)
        self.assertEqual(self.verify(report, allow_zcash=True), report)

    def test_unknown_or_missing_profile_is_refused_by_shared_packaging(self):
        for profile in [None, "unknown"]:
            with self.subTest(profile=profile), self.assertRaises(SystemExit):
                self.verify(dict(self.report, consensus_profile=profile), allow_zcash=True)

    def test_missing_or_falsey_fixture_flags_are_not_shipping_evidence(self):
        for value in [None, 0, "false", True]:
            for profile in ["zcash", "wcash"]:
                with self.subTest(value=value, profile=profile), self.assertRaises(SystemExit):
                    self.verify(dict(self.report, consensus_profile=profile, fixture=value), allow_zcash=True)

    def test_fixture_and_shared_release_options_cannot_be_combined(self):
        with self.assertRaises(SystemExit):
            self.verify(fixture=True, allow_zcash=True)

    def test_non_object_report_is_refused(self):
        with self.assertRaises(SystemExit):
            self.verify([])


if __name__ == "__main__":
    unittest.main()
