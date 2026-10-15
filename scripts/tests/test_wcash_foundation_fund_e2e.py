"""Non-solving tests for serialized coinbase inspection and safe source guards."""
import json
from pathlib import Path
import struct
import tempfile
import unittest

from wcash_foundation_fund_e2e import FUND, TREASURY_SCRIPT, Gate, WireReader, displayed_hashes, inspect_coinbase


def coinbase_prefix(outputs, actions=0, value_balance=0):
    data = struct.pack("<IIIII", 0x80000006, 0xD884B698, 0xC3A6678A, 0, 0)
    data += b"\x01" + bytes(32) + b"\xff" * 4 + b"\x02\x01\x0a" + b"\xff" * 4
    data += bytes([len(outputs)])
    for value, script in outputs:
        data += struct.pack("<q", value) + bytes([len(script)]) + script
    data += b"\x00\x00\x00" + bytes([actions])
    if actions:
        data += bytes(820 * actions) + b"\x02" + struct.pack("<q", value_balance)
    return data


class CoinbaseReaderTests(unittest.TestCase):
    def test_funded_transparent_outputs_are_kept_separate(self):
        script = bytes.fromhex(TREASURY_SCRIPT)
        data = coinbase_prefix([(500_000_007, b"\x51"), (FUND, script)])
        result = inspect_coinbase(data.hex(), is_block=False)
        self.assertEqual(result["outputs"], [{"valueZat": 500_000_007, "script": "51"},
                                             {"valueZat": FUND, "script": TREASURY_SCRIPT}])
        self.assertEqual(result["ironwood_actions"], 0)
        self.assertEqual(result["branch_id"], "c3a6678a")

    def test_private_balance_follows_actions_and_flags(self):
        data = coinbase_prefix([(FUND, bytes.fromhex(TREASURY_SCRIPT))], actions=2, value_balance=-500_000_007)
        result = inspect_coinbase(data.hex(), is_block=False)
        self.assertEqual(result["private_valueZat"], 500_000_007)
        self.assertEqual(result["ironwood_actions"], 2)

    def test_block_header_witness_length_is_respected(self):
        prefix = struct.pack("<I", 0xD7430001) + bytes(136) + b"\x03abc\x01"
        result = inspect_coinbase((prefix + coinbase_prefix([(625_000_000, b"\x51")])).hex())
        self.assertEqual(result["outputs"][0]["valueZat"], 625_000_000)

    def test_reconsider_hash_bytes_use_rpc_display_order(self):
        raw = list(range(32))
        expected = bytes(raw)[::-1].hex()
        self.assertEqual(displayed_hashes([raw]), {expected})
        self.assertEqual(displayed_hashes([expected.upper()]), {expected})
        for bad in [[raw[:-1]], [[True] * 32], ["not-a-hash"]]:
            with self.assertRaises(RuntimeError):
                displayed_hashes(bad)

    def test_truncation_wrong_domain_shape_and_noncanonical_lengths_fail_closed(self):
        data = coinbase_prefix([(1, b"\x51")])
        for bad in [data[:19], data[:-1], bytes(4) + data[4:]]:
            with self.assertRaises(RuntimeError):
                inspect_coinbase(bad.hex(), is_block=False)
        for bad in [b"\xfd\x01\x00", b"\xfe\xfd\x00\x00\x00", b"\xff" + bytes(8)]:
            with self.assertRaises(RuntimeError):
                WireReader(bad).compact()


class CoveredRestartSourceTests(unittest.TestCase):
    def test_invalid_fork_is_rejected_before_reading_or_copying_source(self):
        with self.assertRaisesRegex(RuntimeError, "fork must be 10 or 110"):
            Gate.run_covered_restart_regression(None, Path("does-not-exist"), 9)

    def test_failed_run_cannot_supply_covered_state(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory)
            (source / "evidence.json").write_text(json.dumps({"status": "failed"}))
            for fork in (10, 110):
                with self.assertRaisesRegex(RuntimeError, "requires passed110 evidence"):
                    Gate.run_covered_restart_regression(None, source, fork)


if __name__ == "__main__":
    unittest.main()
