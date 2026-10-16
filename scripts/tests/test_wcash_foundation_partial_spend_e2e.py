"""Non-solving checks that keep partial-spend runtime evidence fail-closed."""
import copy
import hashlib
import json
from pathlib import Path
import struct
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

import wcash_foundation_partial_spend_e2e as partial
from wcash_foundation_partial_spend_e2e import old_partial_error, selected_single_input, signed_transparent_inputs


def raw_inputs(inputs, branch=0xC3A6678A):
    raw = struct.pack("<IIIII", 0x80000006, 0xD884B698, branch, 0, 0)
    raw += bytes([len(inputs)])
    for txid, index in inputs:
        raw += bytes.fromhex(txid)[::-1] + struct.pack("<I", index) + b"\x00" + b"\xff" * 4
    return raw.hex()


class PartialSpendEvidenceTests(unittest.TestCase):
    def test_input_identity_uses_display_hash_and_exact_index(self):
        txid = bytes(range(32)).hex()
        raw = raw_inputs([(txid, 7)])
        first = {"txid": txid, "outputIndex": 2, "satoshis": 187_500_000}
        second = {"txid": txid, "outputIndex": 7, "satoshis": 437_500_000}
        self.assertEqual(signed_transparent_inputs(raw), [(txid, 7)])
        self.assertEqual(selected_single_input(raw, [first, second]), second)
        with self.assertRaisesRegex(RuntimeError, "exact current"):
            selected_single_input(raw, [first])
        with self.assertRaisesRegex(RuntimeError, "already-spent"):
            selected_single_input(raw, [first, second], forbidden=[(txid, 7)])

    def test_wrong_domain_duplicate_and_multiple_inputs_fail(self):
        point = ("12" * 32, 0)
        with self.assertRaisesRegex(RuntimeError, "Regtest domain"):
            signed_transparent_inputs(raw_inputs([point], branch=0))
        with self.assertRaisesRegex(RuntimeError, "duplicate"):
            signed_transparent_inputs(raw_inputs([point, point]))
        with self.assertRaisesRegex(RuntimeError, "exactly one"):
            selected_single_input(raw_inputs([point, ("34" * 32, 1)]), [])

    def test_only_specific_old_partial_creator_error_is_expected(self):
        self.assertTrue(old_partial_error(
            "creator transaction abc has a payout-address output absent from the current UTXO set"))
        for reason in ["connection refused", "TransparentUtxoMismatch", "creator transaction wrong script",
                       "output absent from current UTXO set"]:
            self.assertFalse(old_partial_error(reason))


class PartialSpendArtifactSelectionTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def executable(self, path, data):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        path.chmod(0o700)
        return path

    def artifacts(self, name="artifacts"):
        root = self.root / name
        data = {
            "baseline/wcash-wallet": b"pinned pre-fix wallet",
            "production/wcash-wallet": b"fresh fixed production wallet",
            "fixture/wcash-zebrad": b"fixture node",
            "fixture/wcash-merge-miner": b"fixture coordinator",
            "parent/zcash-zebrad": b"parent node",
        }
        for relative, encoded in data.items():
            self.executable(root / relative, encoded)
        manifest = {
            "baseline_sha": partial.BASE_SHA,
            "head_sha": "11" * 20,
            "features": {"fixture": "wcash-funding-test-fixture"},
            "binaries": {name: hashlib.sha256(raw).hexdigest() for name, raw in data.items()},
        }
        self.write_manifest(root, manifest)
        return root, manifest

    @staticmethod
    def write_manifest(root, manifest):
        (root / "manifest.json").write_text(json.dumps(manifest))

    def gate(self, root, manifest, phase="prepare"):
        gate = partial.PartialSpendGate.__new__(partial.PartialSpendGate)
        gate.args = SimpleNamespace(phase=phase)
        gate.artifacts = root
        gate.runtime = self.root / "unused-runtime"
        gate.manifest = manifest
        gate.wallet_override = None
        gate.evidence = {"binaries": {}}
        gate.save = Mock()
        for boundary in ("command", "node", "native"):
            setattr(gate, boundary, Mock(side_effect=AssertionError("external execution is forbidden")))
        return gate

    def source(self, root, manifest):
        old_hash = manifest["binaries"]["baseline/wcash-wallet"]
        return {
            "old_wallet": {
                "artifact": "baseline/wcash-wallet",
                "source_revision": partial.BASE_SHA,
                "path": str(root / "baseline/wcash-wallet"),
                "sha256": old_hash,
            },
            "binaries": dict(manifest["binaries"]),
            "build_manifest": copy.deepcopy(manifest),
        }

    def assert_wallet_routing(self, gate, expected):
        gate.command.side_effect = None
        gate.command.return_value = {}
        for action in ("derive-address", "init", "sync", "shield-coinbase"):
            gate.wallet(action, action, db="treasury", node={"grpc": "http://127.0.0.1:1"})
        self.assertEqual(gate.command.call_count, 4)
        for call in gate.command.call_args_list:
            self.assertEqual(call.args[1][0], expected)
        gate.node.assert_not_called()
        gate.native.assert_not_called()

    def test_prepare_routes_wallet_operations_to_hash_verified_pinned_baseline(self):
        root, manifest = self.artifacts()
        gate = self.gate(root, manifest)
        gate.select_prepare_wallet()
        expected = root / "baseline/wcash-wallet"
        self.assertEqual(gate.wallet_override, expected)
        self.assertEqual(gate.evidence["old_wallet"], self.source(root, manifest)["old_wallet"])
        self.assertEqual(gate.evidence["binaries"]["baseline/wcash-wallet"],
                         manifest["binaries"]["baseline/wcash-wallet"])
        self.assert_wallet_routing(gate, expected)

    def test_invalid_prepare_baseline_stops_before_commands_nodes_or_mining(self):
        for defect in ("missing-file", "not-executable", "changed-bytes", "missing-hash",
                       "wrong-hash", "wrong-revision", "identical-production"):
            with self.subTest(defect=defect):
                root, manifest = self.artifacts(defect)
                old = root / "baseline/wcash-wallet"
                if defect == "missing-file":
                    old.unlink()
                elif defect == "not-executable":
                    old.chmod(0o600)
                elif defect == "changed-bytes":
                    old.write_bytes(b"different binary")
                elif defect == "missing-hash":
                    del manifest["binaries"]["baseline/wcash-wallet"]
                elif defect == "wrong-hash":
                    manifest["binaries"]["baseline/wcash-wallet"] = "00" * 32
                elif defect == "wrong-revision":
                    manifest["baseline_sha"] = "22" * 20
                elif defect == "identical-production":
                    (root / "production/wcash-wallet").write_bytes(old.read_bytes())
                    manifest["binaries"]["production/wcash-wallet"] = manifest["binaries"]["baseline/wcash-wallet"]
                self.write_manifest(root, manifest)
                gate = self.gate(root, manifest)
                with self.assertRaises(RuntimeError):
                    gate.setup()
                gate.command.assert_not_called()
                gate.node.assert_not_called()
                gate.native.assert_not_called()

    def test_unselected_wallet_has_no_production_fallback(self):
        root, manifest = self.artifacts()
        gate = self.gate(root, manifest)
        with self.assertRaises(RuntimeError):
            gate.wallet("unselected", "derive-address")
        gate.command.assert_not_called()
        self.assertEqual(gate.evidence["binaries"], {})

    def test_old_fixture_rate_is_rejected_before_derivation_nodes_or_mining(self):
        valid = {"fixture": True, "network": "WcashRegtest", "start_height": 10,
                 "end_height_exclusive": 20, "recipient": partial.TREASURY,
                 "script_pub_key": partial.TREASURY_SCRIPT, "numerator": 3, "denominator": 10}
        for wrong_role in ["fixture-policy", "fixture-coordinator-policy"]:
            for wrong_rate in [{"numerator": 1, "denominator": 5}, {"divisor": 5}]:
                with self.subTest(role=wrong_role, rate=wrong_rate):
                    root, manifest = self.artifacts(wrong_role + str(len(wrong_rate)))
                    gate = self.gate(root, manifest)

                    def command(label, _arguments, **_kwargs):
                        self.assertIn(label, ["fixture-policy", "fixture-coordinator-policy"])
                        if label != wrong_role:
                            return dict(valid)
                        return {key: value for key, value in valid.items()
                                if key not in ("numerator", "denominator")} | wrong_rate

                    gate.command.side_effect = command
                    with self.assertRaisesRegex(RuntimeError, "numerator 3 / denominator 10"):
                        gate.setup()
                    gate.node.assert_not_called()
                    gate.native.assert_not_called()

    def test_verify_routes_to_new_wallet_even_when_it_is_current_production(self):
        root, manifest = self.artifacts()
        source = self.source(root, manifest)
        del source["binaries"]["production/wcash-wallet"]
        new = root / "production/wcash-wallet"
        expected = partial.checked_new_wallet(new, manifest["binaries"]["production/wcash-wallet"], source)
        self.assertEqual(expected, new.resolve())
        gate = self.gate(root, manifest, phase="verify")
        gate.wallet_override = expected
        self.assert_wallet_routing(gate, expected)

    def test_verify_never_executes_or_requires_retired_wallet_files(self):
        root, manifest = self.artifacts()
        source = self.source(root, manifest)
        new = self.executable(self.root / "new-wallet", b"new separately verified wallet")
        (root / "baseline/wcash-wallet").unlink()
        (root / "production/wcash-wallet").unlink()
        checked = partial.checked_new_wallet(new, partial.file_digest(new), source)
        gate = self.gate(root, manifest, phase="verify")
        gate.wallet_override = checked
        policy = {"fixture": True, "network": "WcashRegtest", "start_height": 10,
                  "end_height_exclusive": 20, "recipient": partial.TREASURY,
                  "script_pub_key": partial.TREASURY_SCRIPT, "numerator": 3, "denominator": 10}

        def command(label, arguments, **_kwargs):
            if label in ["fixture-policy", "fixture-coordinator-policy"]:
                expected = "wcash-zebrad" if label == "fixture-policy" else "wcash-merge-miner"
                self.assertEqual(arguments[0], root / "fixture" / expected)
                return policy
            self.assertEqual(arguments[0], checked)
            return {"transparent_coinbase_address": partial.TREASURY if label == "derive-treasury" else "test-miner"}

        gate.command.side_effect = command
        gate.setup()
        self.assertEqual(gate.command.call_count, 4)
        gate.node.assert_not_called()
        gate.native.assert_not_called()

    def test_new_wallet_must_be_executable_match_hash_and_differ_from_actual_old(self):
        root, manifest = self.artifacts()
        source = self.source(root, manifest)
        old = root / "baseline/wcash-wallet"
        new = root / "production/wcash-wallet"
        with self.assertRaises(RuntimeError):
            partial.checked_new_wallet(old, partial.file_digest(old), source)
        with self.assertRaises(RuntimeError):
            partial.checked_new_wallet(new, "00" * 32, source)
        with self.assertRaises(RuntimeError):
            partial.checked_new_wallet(self.root / "missing", manifest["binaries"]["production/wcash-wallet"], source)
        new.chmod(0o600)
        with self.assertRaises(RuntimeError):
            partial.checked_new_wallet(new, partial.file_digest(new), source)

    def test_present_old_wallet_metadata_is_strict_and_never_uses_legacy_fallback(self):
        root, manifest = self.artifacts()
        good = self.source(root, manifest)
        self.assertEqual(partial.prepared_old_wallet_sha256(good), manifest["binaries"]["baseline/wcash-wallet"])
        for defect in ("null", "empty", "artifact", "revision", "hash-format", "receipt-hash", "manifest-hash"):
            with self.subTest(defect=defect):
                source = copy.deepcopy(good)
                if defect == "null":
                    source["old_wallet"] = None
                elif defect == "empty":
                    source["old_wallet"] = {}
                elif defect == "artifact":
                    source["old_wallet"]["artifact"] = "production/wcash-wallet"
                elif defect == "revision":
                    source["old_wallet"]["source_revision"] = "22" * 20
                elif defect == "hash-format":
                    source["old_wallet"]["sha256"] = "not-a-sha256"
                elif defect == "receipt-hash":
                    source["binaries"]["baseline/wcash-wallet"] = "33" * 32
                elif defect == "manifest-hash":
                    source["build_manifest"]["binaries"]["baseline/wcash-wallet"] = "44" * 32
                with self.assertRaises(RuntimeError):
                    partial.prepared_old_wallet_sha256(source)

    def test_legacy_production_old_hash_is_supported_without_the_old_file(self):
        root, manifest = self.artifacts()
        source = self.source(root, manifest)
        del source["old_wallet"]
        old_hash = manifest["binaries"]["baseline/wcash-wallet"]
        source["binaries"]["production/wcash-wallet"] = old_hash
        source["build_manifest"]["binaries"]["production/wcash-wallet"] = old_hash
        (root / "baseline/wcash-wallet").unlink()
        (root / "production/wcash-wallet").unlink()
        self.assertEqual(partial.prepared_old_wallet_sha256(source), old_hash)
        new = self.executable(self.root / "fresh-wallet", b"new fixed wallet")
        self.assertEqual(partial.checked_new_wallet(new, partial.file_digest(new), source), new.resolve())
        source["binaries"]["production/wcash-wallet"] = "bad-hash"
        with self.assertRaises(RuntimeError):
            partial.prepared_old_wallet_sha256(source)


if __name__ == "__main__":
    unittest.main()
