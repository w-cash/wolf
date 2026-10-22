"""Disposable tracker fixtures; no database, network, solver or submission."""
import base64
import hashlib
import importlib.util
import pathlib
import unittest
from unittest.mock import patch

PATH = pathlib.Path(__file__).resolve().parents[1] / "zec-winner-tracking.py"
SPEC = importlib.util.spec_from_file_location("zec_tracking", PATH)
TRACK = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(TRACK)


def snapshot():
    return {"schema": 1, "chain": "zcash", "network": "mainnet", "genesis": TRACK.GENESIS,
            "deployment": "af99f42c-f66d-4332-acd2-8eebbe154da0",
            "journal_stream": "18b67152-b064-4b52-9b91-a2cefb41c3f9", "watermark": 20,
            "distinct_candidates": 1, "qualified_proofs": 1,
            "candidates": [{"hash": "12" * 32, "height": 10, "local_state": "observed",
                            "found_event_seq": 9, "projected_at": "2026-10-23T00:00:00Z"}]}


class TrackingTests(unittest.TestCase):
    def test_external_identity_anchor_matches_frozen_zcash_header(self):
        vector = PATH.parent.parent / "zebra-test/src/vectors/block-main-0-419-200.txt"
        block = bytes.fromhex(vector.read_text().strip())
        self.assertEqual(block[140:143], bytes.fromhex("fd4005"))
        header_hash = hashlib.sha256(hashlib.sha256(block[:1487]).digest()).digest()[::-1].hex()
        self.assertEqual(header_hash, TRACK.ANCHOR_HASH)

    def test_no_wec_count_or_wrong_genesis(self):
        for field, value in [("chain", "wcash"), ("genesis", "34" * 32), ("network", "testnet")]:
            value_snapshot = snapshot()
            value_snapshot[field] = value
            with self.assertRaises(ValueError):
                TRACK.validate_snapshot(value_snapshot)

    def test_duplicate_candidates_rejected(self):
        value = snapshot()
        value["candidates"] *= 2
        with self.assertRaises(ValueError):
            TRACK.validate_snapshot(value)
        value = snapshot()
        value["candidates"][0]["found_event_seq"] = 21
        with self.assertRaises(ValueError):
            TRACK.validate_snapshot(value)

    def test_incomplete_saved_snapshot_cannot_claim_complete_coverage(self):
        value = snapshot()
        value["distinct_candidates"] = 2
        with self.assertRaises(ValueError):
            TRACK.observe(value, "fixture.invalid:443", "fixture", 1, 0)
        value = snapshot()
        value["qualified_proofs"] = 0
        with self.assertRaises(ValueError):
            TRACK.validate_snapshot(value)

    def test_tip_byte_order_is_explicit(self):
        raw = bytes(range(32))
        self.assertEqual(TRACK.tip({"height": "10", "hash": base64.b64encode(raw).decode()}),
                         {"height": 10, "hash": raw[::-1].hex()})

    def test_canonical_depth_and_competing_block(self):
        candidate = snapshot()["candidates"][0]
        tip = {"height": 15, "hash": "34" * 32}
        canonical = {"height": 10, "network": "main", "hash": candidate["hash"]}
        result = TRACK.classify(candidate, tip, canonical, tip)
        self.assertEqual((result["state"], result["confirmations"]), ("canonical", 6))
        canonical["hash"] = "56" * 32
        result = TRACK.classify(candidate, tip, canonical, tip)
        self.assertEqual(result["state"], "noncanonical_at_sample")
        self.assertNotIn("lost", result.values())

    def test_tip_change_and_behind_are_unknown(self):
        candidate = snapshot()["candidates"][0]
        tip = {"height": 9, "hash": "34" * 32}
        self.assertEqual(TRACK.classify(candidate, tip, {}, tip)["reason"], "observer_behind")
        self.assertEqual(TRACK.classify(candidate, tip, {}, {"height": 10})["reason"],
                         "observer_tip_changed")

    def test_wrong_fixed_height_is_not_acceptance(self):
        tip = {"height": 11, "hash": "34" * 32}
        with self.assertRaises(ValueError):
            TRACK.classify(snapshot()["candidates"][0], tip,
                           {"height": 9, "network": "main", "hash": "12" * 32}, tip)

    def test_provider_outage_preserves_found_unknown(self):
        def unavailable(*_):
            raise OSError("offline")
        result = TRACK.observe(snapshot(), "fixture.invalid:443", "fixture", 1, 0, unavailable)
        self.assertEqual(result["distinct_candidates"], 1)
        self.assertEqual(result["candidates"][0]["state"], "unknown")
        self.assertTrue(result["coverage_complete"])

    def test_external_reorg_can_change_a_later_observation(self):
        def make_call(canonical_hash):
            def call(_endpoint, _protoset, method, request):
                if method == "GetLightdInfo":
                    return {"chainName": "main"}
                if method == "GetLatestBlock":
                    return {"height": 15, "hash": base64.b64encode(bytes.fromhex("34" * 32)).decode()}
                if request["height"] == TRACK.ANCHOR_HEIGHT:
                    return {"height": TRACK.ANCHOR_HEIGHT, "network": "main", "hash": TRACK.ANCHOR_HASH}
                return {"height": request["height"], "network": "main", "hash": canonical_hash}
            return call
        accepted = TRACK.observe(snapshot(), "fixture.invalid:443", "fixture", 1, 0,
                                 make_call("12" * 32))
        orphaned = TRACK.observe(snapshot(), "fixture.invalid:443", "fixture", 1, 0,
                                 make_call("56" * 32))
        self.assertEqual(accepted["candidates"][0]["state"], "canonical")
        self.assertEqual(orphaned["candidates"][0]["state"], "noncanonical_at_sample")

    def test_invalid_tls_endpoint_never_launches_process(self):
        with patch.object(TRACK.subprocess, "run") as run:
            for endpoint in ["http://fixture.invalid", "user:secret@fixture.invalid:443", "fixture.invalid;touch /tmp/a"]:
                with self.assertRaises(ValueError):
                    TRACK.grpc(endpoint, "fixture", "GetLatestBlock", {})
            run.assert_not_called()

    def test_export_uses_read_only_transaction_and_strict_uuid(self):
        class Result:
            returncode = 0
            stdout = TRACK.json.dumps(snapshot()).encode()
        with patch.object(TRACK.subprocess, "run", return_value=Result()) as run:
            TRACK.export_snapshot(snapshot()["deployment"])
            query = run.call_args.args[0][-1]
            self.assertIn("REPEATABLE READ READ ONLY", query)
            self.assertIn("statement_timeout = '3s'", query)
            self.assertIn("w.chain='zcash'", query)
            with self.assertRaises(ValueError):
                TRACK.export_snapshot("'; DELETE FROM winners; --")
            self.assertEqual(run.call_count, 1)


if __name__ == "__main__":
    unittest.main()
