"""Non-solving integrity guards for the actual copied-state recovery driver."""
import copy
from pathlib import Path
import tempfile
import struct
import unittest

from wcash_foundation_backup_recovery import (
    BASE_SHA, FIXED_NODE, RecoveryGate, covered_pair, fixture_coverage, verify_overlay_sources,
)


class OverlaySourceTests(unittest.TestCase):
    def setUp(self):
        self.original = {"baseline_sha": BASE_SHA, "head_sha": "1" * 40,
                         "binaries": {"baseline/wcash-zebrad": "a" * 64,
                                      FIXED_NODE: "b" * 64, "parent/zcash-zebrad": "c" * 64}}
        self.overlay = copy.deepcopy(self.original)
        self.overlay.update(head_sha="2" * 40, source_tree_sha256="3" * 64,
                            binary_sources={"baseline/wcash-zebrad": BASE_SHA,
                                            FIXED_NODE: "2" * 40, "parent/zcash-zebrad": "1" * 40})
        self.overlay["binaries"][FIXED_NODE] = "d" * 64

    def test_only_attested_fixture_replacement_is_allowed(self):
        verify_overlay_sources(self.original, self.overlay)
        for path in ("baseline/wcash-zebrad", "parent/zcash-zebrad"):
            changed = copy.deepcopy(self.overlay)
            changed["binaries"][path] = "e" * 64
            with self.assertRaisesRegex(RuntimeError, "only the fixed fixture node"):
                verify_overlay_sources(self.original, changed)

    def test_original_bundle_is_allowed_without_a_replacement(self):
        verify_overlay_sources(self.original, copy.deepcopy(self.original))
        changed = copy.deepcopy(self.original)
        changed["binaries"][FIXED_NODE] = "e" * 64
        with self.assertRaisesRegex(RuntimeError, "source tree digest"):
            verify_overlay_sources(self.original, changed)

    def test_inherited_binary_cannot_claim_the_fixed_nodes_source(self):
        self.overlay["binary_sources"]["parent/zcash-zebrad"] = self.overlay["head_sha"]
        with self.assertRaisesRegex(RuntimeError, "real source revision"):
            verify_overlay_sources(self.original, self.overlay)


class CoveredPairTests(unittest.TestCase):
    def test_marker_alone_partial_bytes_and_wrong_version_never_prove_repair(self):
        backup = bytes(8) + b"exact retained solved block bytes"
        marker = fixture_coverage(110, "12" * 32, backup)
        with tempfile.TemporaryDirectory() as directory:
            block_file, marker_file = Path(directory) / "block", Path(directory) / "marker"
            marker_file.write_bytes(marker)
            self.assertFalse(covered_pair(block_file, marker_file, backup, marker))
            block_file.write_bytes(backup[:10])
            self.assertFalse(covered_pair(block_file, marker_file, backup, marker))
            block_file.write_bytes(backup)
            marker_file.write_bytes(marker.replace(b"validation-v2", b"validation-v1"))
            self.assertFalse(covered_pair(block_file, marker_file, backup, marker))
            marker_file.write_bytes(marker)
            self.assertTrue(covered_pair(block_file, marker_file, backup, marker))
            block_file.write_bytes(bytes([1]) + backup[1:])
            self.assertFalse(covered_pair(block_file, marker_file, backup, marker))

    def test_marker_binds_fixture_height_hash_and_serialized_balance_prefix(self):
        backup = bytes(8) + b"retained block"
        block_hash = bytes(range(32)).hex()
        marker = fixture_coverage(110, block_hash, backup)
        self.assertNotEqual(marker, fixture_coverage(109, block_hash, backup))
        self.assertNotEqual(marker, fixture_coverage(110, "00" * 32, backup))
        self.assertNotEqual(marker, fixture_coverage(110, block_hash, b"\x01" + backup[1:]))
        self.assertEqual(marker[-64:-32], bytes(range(32))[::-1])
        prefix = b"wcash-foundation-fund-validation-v2\0"
        self.assertTrue(marker.startswith(prefix + struct.pack(">QQIII", 3, 10, 10, 20, 110)))
        for numerator, denominator in ((2, 10), (3, 5)):
            wrong = prefix + struct.pack(">QQ", numerator, denominator) + marker[len(prefix) + 16:]
            self.assertNotEqual(marker, wrong)


class SourceSnapshotTests(unittest.TestCase):
    def test_all_recorded_source_hashes_including_maturity_heights_are_queried(self):
        gate = object.__new__(RecoveryGate)
        source_heights = (0, 8, 9, 10, 19, 20, 21, 108, 109, 110)
        gate.expected_final = {"hashes": {str(height): f"hash{height}" for height in source_heights}}
        gate.event = lambda *args, **kwargs: None
        for tip in (109, 110):
            queried = []

            def rpc(node, method, params=None):
                if method == "getblockhash":
                    queried.append(params[0])
                    return f"hash{params[0]}"
                return {"getblockcount": tip, "getbestblockhash": f"hash{tip}",
                        "getblockchaininfo": {"chainSupply": {}, "valuePools": []},
                        "getaddressutxos": []}[method]

            gate.rpc = rpc
            snapshot = gate.source_snapshot(None, "source-height-regression")
            expected = {str(height): f"hash{height}" for height in source_heights if height <= tip}
            self.assertEqual(snapshot["hashes"], expected)
            self.assertEqual(queried, [height for height in source_heights if height <= tip])


if __name__ == "__main__":
    unittest.main()
