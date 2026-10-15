"""Deterministic lifecycle checks for the local foundation runtime harness."""
import json
from pathlib import Path
import signal
import subprocess
import tempfile
import unittest
from unittest.mock import call, patch

from wcash_foundation_fund_e2e import Gate


REQUIRED_REFUSAL = (
    "cannot trust stored Wcash foundation history",
    "current rule validation coverage",
    "NEW, empty state cache_dir",
)


class FakeProcess:
    """A reaped return code or a bounded, explicit sequence of wait outcomes."""

    def __init__(self, returncode=None, waits=(0,)):
        self.pid = 987654
        self.returncode = returncode
        self.waits = list(waits)
        self.wait_timeouts = []

    def poll(self):
        return self.returncode

    def wait(self, timeout=None):
        self.wait_timeouts.append(timeout)
        if self.returncode is not None:
            return self.returncode
        if not self.waits:
            raise AssertionError("shutdown unexpectedly waited again")
        outcome = self.waits.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        self.returncode = outcome
        return outcome


class ShutdownTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.gate = Gate.__new__(Gate)
        self.gate.runtime = Path(self.directory.name)
        self.gate.nodes = []
        self.gate.evidence = {"status": "passed", "events": []}

    def tearDown(self):
        for node in self.gate.nodes:
            if not node["log"].closed:
                node["log"].close()
        self.directory.cleanup()

    def node(self, name="ordinary-node", *, returncode=None, waits=(0,), diagnostic=""):
        path = self.gate.runtime / f"{name}.log"
        path.write_text(diagnostic)
        node = {"name": name, "process": FakeProcess(returncode, waits), "log": path.open("a")}
        self.gate.nodes.append(node)
        return node

    def stops(self, node):
        return [event for event in self.gate.evidence["events"]
                if event["event"] == "node-stop" and event["name"] == node["name"]]

    def assert_failed(self):
        self.assertEqual(self.gate.evidence["status"], "failed")
        self.gate.evidence["status"] = "passed"
        self.gate.save()
        self.assertEqual(self.gate.evidence["status"], "failed")
        self.assertEqual(json.loads((self.gate.runtime / "evidence.json").read_text())["status"], "failed")

    @patch("wcash_foundation_fund_e2e.os.killpg")
    def test_live_sigint_zero_is_clean_and_reaped_stop_is_idempotent(self, killpg):
        node = self.node()
        self.gate.stop(node)
        killpg.assert_called_once_with(node["process"].pid, signal.SIGINT)
        self.assertEqual(node["process"].wait_timeouts, [45])
        self.assertTrue(node["log"].closed)
        self.assertTrue(node["stopped"])
        self.assertEqual(self.stops(node)[0]["exit_code"], 0)
        self.assertEqual(self.gate.evidence["status"], "passed")
        self.gate.stop(node)
        self.assertEqual(len(self.stops(node)), 1)
        self.assertEqual(killpg.call_count, 1)

    @patch("wcash_foundation_fund_e2e.os.killpg")
    def test_preexited_nodes_fail_even_zero_or_a_refusal_like_name(self, killpg):
        for index, code in enumerate((0, 2, -6)):
            with self.subTest(exit_code=code):
                node = self.node(f"late-upgrade-refusal-{index}", returncode=code)
                with self.assertRaises(RuntimeError):
                    self.gate.stop(node)
                self.assertTrue(node["log"].closed)
                self.assertTrue(node["stopped"])
                self.assertEqual(self.stops(node)[0]["exit_code"], code)
                self.assert_failed()
        killpg.assert_not_called()

    @patch("wcash_foundation_fund_e2e.os.killpg")
    def test_nonzero_after_requested_sigint_is_not_clean(self, killpg):
        node = self.node(waits=(-6,))
        with self.assertRaises(RuntimeError):
            self.gate.stop(node)
        killpg.assert_called_once_with(node["process"].pid, signal.SIGINT)
        self.assertTrue(node["log"].closed)
        self.assertEqual(self.stops(node)[0]["exit_code"], -6)
        self.assert_failed()

    @patch("wcash_foundation_fund_e2e.os.killpg")
    def test_exact_exited_startup_refusal_is_explicitly_accepted(self, killpg):
        node = self.node("verified-refusal", returncode=-6,
                         diagnostic="\n".join(REQUIRED_REFUSAL))
        self.gate.accept_startup_refusal(node, REQUIRED_REFUSAL)
        killpg.assert_not_called()
        self.assertTrue(node["log"].closed)
        self.assertTrue(node["stopped"])
        self.assertEqual(self.stops(node)[0]["exit_code"], -6)
        self.assertEqual(self.gate.evidence["status"], "passed")
        self.gate.close()
        self.assertEqual(len(self.stops(node)), 1)
        self.assertEqual(self.gate.evidence["status"], "passed")

    @patch("wcash_foundation_fund_e2e.os.killpg")
    def test_zero_or_missing_or_wrong_refusal_diagnostics_fail(self, killpg):
        cases = ((0, "\n".join(REQUIRED_REFUSAL)),
                 (-6, "\n".join(REQUIRED_REFUSAL[:-1])),
                 (-6, "unrelated panic after configuration startup"))
        for index, (code, diagnostic) in enumerate(cases):
            with self.subTest(exit_code=code, diagnostic=diagnostic):
                node = self.node(f"invalid-refusal-{index}", returncode=code, diagnostic=diagnostic)
                with self.assertRaises(RuntimeError):
                    self.gate.accept_startup_refusal(node, REQUIRED_REFUSAL)
                self.assertFalse(node.get("startup_refusal_verified", False))
                self.gate.close()
                self.assertTrue(node["log"].closed)
                self.assert_failed()
        killpg.assert_not_called()

    @patch("wcash_foundation_fund_e2e.os.killpg")
    def test_diagnostic_text_from_another_node_cannot_authorize_refusal(self, killpg):
        self.node("other-node", returncode=-6, diagnostic="\n".join(REQUIRED_REFUSAL))
        node = self.node("wrong-own-log", returncode=-6, diagnostic="unrelated failure")
        with self.assertRaises(RuntimeError):
            self.gate.accept_startup_refusal(node, REQUIRED_REFUSAL)
        self.assertFalse(node.get("startup_refusal_verified", False))
        self.gate.close()
        self.assertTrue(node["log"].closed)
        self.assert_failed()
        killpg.assert_not_called()

    @patch("wcash_foundation_fund_e2e.os.killpg")
    def test_invalid_required_diagnostics_cannot_authorize_any_nonzero_exit(self, killpg):
        for index, required in enumerate(((), ("",), (REQUIRED_REFUSAL[0], ""),
                                          REQUIRED_REFUSAL[0], iter(REQUIRED_REFUSAL), None)):
            with self.subTest(required=required):
                node = self.node(f"empty-proof-{index}", returncode=-6,
                                 diagnostic="\n".join(REQUIRED_REFUSAL))
                with self.assertRaises(RuntimeError):
                    self.gate.accept_startup_refusal(node, required)
                self.assertFalse(node.get("startup_refusal_verified", False))
                self.gate.close()
                self.assertTrue(node["log"].closed)
                self.assert_failed()
        killpg.assert_not_called()

    @patch("wcash_foundation_fund_e2e.os.killpg")
    def test_running_process_with_refusal_text_is_not_an_accepted_refusal(self, killpg):
        node = self.node(diagnostic="\n".join(REQUIRED_REFUSAL))
        with self.assertRaises(RuntimeError):
            self.gate.accept_startup_refusal(node, REQUIRED_REFUSAL)
        self.assertFalse(node.get("startup_refusal_verified", False))
        self.gate.close()
        self.assertTrue(node["log"].closed)
        killpg.assert_called_once_with(node["process"].pid, signal.SIGINT)
        self.assert_failed()

    @patch("wcash_foundation_fund_e2e.os.killpg")
    def test_sigint_timeout_then_term_zero_still_fails_and_closes_log(self, killpg):
        node = self.node(waits=(subprocess.TimeoutExpired("mock-node", 45), 0))
        with self.assertRaises(RuntimeError):
            self.gate.stop(node)
        self.assertEqual(killpg.call_args_list, [call(node["process"].pid, signal.SIGINT),
                                                call(node["process"].pid, signal.SIGTERM)])
        self.assertEqual(node["process"].wait_timeouts, [45, 10])
        self.assertTrue(node["log"].closed)
        self.assertTrue(node["stopped"])
        self.assertEqual(self.stops(node)[0]["exit_code"], 0)
        self.assert_failed()

    @patch("wcash_foundation_fund_e2e.os.killpg")
    def test_sigint_and_term_timeouts_kill_reap_and_fail_with_closed_log(self, killpg):
        node = self.node(waits=(subprocess.TimeoutExpired("mock-node", 45),
                                subprocess.TimeoutExpired("mock-node", 10), -9))
        with self.assertRaises(RuntimeError):
            self.gate.stop(node)
        self.assertEqual(killpg.call_args_list, [call(node["process"].pid, signal.SIGINT),
                                                call(node["process"].pid, signal.SIGTERM),
                                                call(node["process"].pid, signal.SIGKILL)])
        self.assertEqual(node["process"].wait_timeouts, [45, 10, 10])
        self.assertTrue(node["log"].closed)
        self.assertTrue(node["stopped"])
        self.assertEqual(self.stops(node)[0]["exit_code"], -9)
        self.assert_failed()

    @patch("wcash_foundation_fund_e2e.os.killpg")
    def test_sigkill_wait_timeout_is_bounded_and_never_marks_process_reaped(self, killpg):
        node = self.node(waits=(subprocess.TimeoutExpired("mock-node", 45),
                                subprocess.TimeoutExpired("mock-node", 10),
                                subprocess.TimeoutExpired("mock-node", 10)))
        with self.assertRaises(RuntimeError):
            self.gate.stop(node)
        self.assertEqual(killpg.call_args_list, [call(node["process"].pid, signal.SIGINT),
                                                call(node["process"].pid, signal.SIGTERM),
                                                call(node["process"].pid, signal.SIGKILL)])
        self.assertEqual(node["process"].wait_timeouts, [45, 10, 10])
        self.assertTrue(node["log"].closed)
        self.assertFalse(node["stopped"])
        self.assertIsNone(self.stops(node)[0]["exit_code"])
        self.assert_failed()

    @patch("wcash_foundation_fund_e2e.os.killpg")
    def test_close_continues_after_failure_and_preserves_first_failure(self, killpg):
        self.gate.record_failure(RuntimeError("original verification failure"))
        good = self.node("still-running")
        bad = self.node("unexpected-exit", returncode=2)
        self.gate.close()
        self.assertTrue(bad["log"].closed and good["log"].closed)
        self.assertTrue(bad["stopped"] and good["stopped"])
        self.assertEqual(len(self.stops(bad)), 1)
        self.assertEqual(len(self.stops(good)), 1)
        killpg.assert_called_once_with(good["process"].pid, signal.SIGINT)
        self.assertIn("original verification failure", self.gate.evidence["failure"])
        self.assertTrue(any(event["event"] == "cleanup-error"
                            for event in self.gate.evidence["events"]))
        self.assert_failed()

    @patch("wcash_foundation_fund_e2e.os.killpg")
    def test_close_reaps_a_live_process_even_when_its_log_is_already_closed(self, killpg):
        node = self.node()
        node["log"].close()
        self.gate.close()
        killpg.assert_called_once_with(node["process"].pid, signal.SIGINT)
        self.assertTrue(node["stopped"])
        self.assertEqual(self.stops(node)[0]["exit_code"], 0)

    @patch("wcash_foundation_fund_e2e.os.killpg")
    def test_evidence_save_failure_does_not_prevent_remaining_process_cleanup(self, killpg):
        first = self.node("first-node")
        second = self.node("second-node")
        with patch.object(self.gate, "save", side_effect=OSError("mock evidence storage failure")):
            self.gate.close()
        self.assertTrue(first["log"].closed and second["log"].closed)
        self.assertTrue(first["stopped"] and second["stopped"])
        self.assertEqual(len(self.stops(first)), 1)
        self.assertEqual(len(self.stops(second)), 1)
        self.assertEqual(killpg.call_count, 2)
        self.assertEqual(self.gate.evidence["status"], "failed")
        self.assertTrue(any(event["event"] == "cleanup-error"
                            for event in self.gate.evidence["events"]))
        self.assert_failed()

    def test_failed_popen_closes_log_without_registering_an_owned_process(self):
        (self.gate.runtime / "private").mkdir()
        self.gate.env = {}
        self.gate.used_ports = set()
        self.gate.miner = {"transparent_coinbase_address": "disposable-test-recipient"}
        log_handles = []
        real_open = Path.open

        def tracked_open(path, *args, **kwargs):
            handle = real_open(path, *args, **kwargs)
            if path.name == "launch-failure.log":
                log_handles.append(handle)
            return handle

        with patch.object(Gate, "port", side_effect=(30001, 30002, 30003)), \
                patch.object(self.gate, "binary", return_value=Path("/mock/wcash-zebrad")), \
                patch("wcash_foundation_fund_e2e.subprocess.Popen", side_effect=OSError("mock launch failure")), \
                patch.object(Path, "open", tracked_open):
            with self.assertRaisesRegex(OSError, "mock launch failure"):
                self.gate.node("launch-failure", "fixture")
        self.assertEqual(len(log_handles), 1)
        self.assertTrue(log_handles[0].closed)
        self.assertEqual(self.gate.nodes, [])
        self.assertFalse(any(event["event"] == "node-start" for event in self.gate.evidence["events"]))


if __name__ == "__main__":
    unittest.main()
