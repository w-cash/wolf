#!/usr/bin/env python3
"""Exercise recoverable foundation backup publication on copies of a passed110 chain.

This performs no new proof of work and never opens the original state with a
node. Use the original artifact bundle, or a separate overlay containing only a
freshly built fixture node. An overlay must identify each binary's source in
binary_sources; all compatibility binaries remain byte-identical.

After this script passes, its printed evidence can supply --covered-restart-source
to wcash-foundation-fund-e2e.sh for the actual active11 and expired111 regressions.
That evidence explicitly retains the original solved-chain provenance.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import struct
import sys
import time

from wcash_foundation_fund_e2e import (
    BASE_SHA, Gate, REPO, TREASURY, TREASURY_SCRIPT, require, require_foundation_rate,
)

RULE_VERSION = b"wcash-foundation-fund-validation-v2\0"
FIXED_NODE = "fixture/wcash-zebrad"


def digest_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def digest_tree(path):
    """Detect writes to the original state without opening it as a database."""
    digest = hashlib.sha256()
    for item in sorted(path.rglob("*")):
        relative = item.relative_to(path).as_posix().encode()
        if item.is_symlink():
            digest.update(b"link\0" + relative + b"\0" + os.readlink(item).encode())
        elif item.is_file():
            digest.update(b"file\0" + relative + b"\0" + bytes.fromhex(digest_file(item)))
        elif item.is_dir():
            digest.update(b"dir\0" + relative + b"\0")
        digest.update(stat.S_IMODE(item.lstat().st_mode).to_bytes(4, "big"))
    return digest.hexdigest()


def fixture_coverage(height, block_hash, backup):
    """Independently bind the version, fixture boundaries, block and exact stored bytes."""
    return (RULE_VERSION + struct.pack(">QQIII", 3, 10, 10, 20, height) + bytes.fromhex(block_hash)[::-1]
            + hashlib.sha256(backup).digest())


def covered_pair(block_file, marker_file, expected_backup, expected_marker):
    try:
        return block_file.read_bytes() == expected_backup and marker_file.read_bytes() == expected_marker
    except OSError:
        return False


def verify_overlay_sources(original, overlay):
    """An updated node must never hide a replacement of the old compatibility binaries."""
    require(original["baseline_sha"] == overlay["baseline_sha"] == BASE_SHA,
            "artifact baseline differs from the pinned legacy executable")
    if original == overlay:
        return
    require(set(original["binaries"]) == set(overlay["binaries"]), "overlay binary set differs")
    changed = {name for name, digest in original["binaries"].items()
               if overlay["binaries"][name] != digest}
    require(changed == {FIXED_NODE}, "only the fixed fixture node may replace an original binary")
    require(re.fullmatch(r"[0-9a-f]{40}", overlay.get("head_sha", "")) is not None,
            "overlay needs the exact fixed node source revision")
    require(re.fullmatch(r"[0-9a-f]{64}", overlay.get("source_tree_sha256", "")) is not None,
            "overlay needs the fixed node build's source tree digest")
    require(overlay["head_sha"] != original["head_sha"], "overlay still identifies the old node revision")
    expected_sources = {name: (BASE_SHA if name.startswith("baseline/") else original["head_sha"])
                        for name in original["binaries"]}
    expected_sources[FIXED_NODE] = overlay["head_sha"]
    require(overlay.get("binary_sources") == expected_sources,
            "mixed overlay must name the real source revision for every binary")


def state_snapshot(event):
    return {key: event[key] for key in ("height", "tip", "hashes", "supply", "pools", "treasury_utxos")}


class RecoveryGate(Gate):
    def source_snapshot(self, node, label):
        return self.snapshot(node, label, heights=tuple(int(height) for height in self.expected_final["hashes"]))

    def load_source(self):
        require(os.name == "posix" and os.geteuid() != 0,
                "permission failure regression requires an unprivileged POSIX user")
        source_file = self.args.source_evidence.resolve()
        source = json.loads(source_file.read_text())
        require(source["status"] == "passed", "source must be a stopped passed110 run")
        require({e["name"] for e in source["events"] if e["event"] == "node-start"}
                <= {e["name"] for e in source["events"] if e["event"] == "node-stop"},
                "stop every source node before copying its state")
        final = next((e for e in reversed(source["events"])
                      if e["event"] == "state-snapshot" and e.get("label") == "final110"), None)
        require(final is not None and final["height"] == 110, "source lacks validated final110 evidence")
        self.expected_final = state_snapshot(final)
        self.original_state = Path(next(e["state"] for e in reversed(source["events"])
                                        if e["event"] == "node-start" and e["profile"] == "fixture")).resolve()
        require(self.original_state.is_dir() and
                self.original_state.is_relative_to(Path(source["runtime"]).resolve()),
                "source state must belong to the recorded disposable runtime")
        old_artifacts = self.args.source_artifacts.resolve()
        old_manifest_file = old_artifacts / "manifest.json"
        original = json.loads(old_manifest_file.read_text())
        require(source["build_manifest"] == original, "original artifact manifest differs from full-run provenance")
        self.manifest = json.loads((self.artifacts / "manifest.json").read_text())
        verify_overlay_sources(original, self.manifest)
        for name, digest in original["binaries"].items():
            require(digest_file(old_artifacts / name) == digest, f"original artifact changed: {name}")
            if name in source["binaries"]:
                require(source["binaries"][name] == digest, f"source executable digest differs: {name}")
            self.binary(*name.split("/", 1))
        self.evidence.update({"scenario": "copied-state-foundation-backup-permission-recovery",
                              "new_proof_of_work": False, "source_evidence": str(source_file),
                              "source_evidence_sha256": digest_file(source_file),
                              "original_build_manifest": original, "build_manifest": self.manifest,
                              "original_manifest_sha256": digest_file(old_manifest_file),
                              "original_state": str(self.original_state)})
        self.original_digest = digest_tree(self.original_state)
        self.evidence["original_state_tree_sha256"] = self.original_digest
        self.miner = {"transparent_coinbase_address": TREASURY}
        policy = self.command("fixed-node-policy", [self.binary("fixture", "wcash-zebrad"), "funding-policy"])
        require_foundation_rate(policy)
        require(policy["fixture"] is True and policy["network"] == "WcashRegtest" and
                policy["start_height"] == 10 and policy["end_height_exclusive"] == 20 and
                policy["recipient"] == TREASURY and policy["script_pub_key"] == TREASURY_SCRIPT,
                "fixed binary does not report the isolated fixture policy")
        self.event("artifact-policy", profile="fixture", binary="wcash-zebrad", policy=policy)

    def copy_source(self, name):
        state = self.runtime / f"{name}-state-copy"
        shutil.copytree(self.original_state, state)
        self.event("state-copy", label=name, original=str(self.original_state), copied=str(state))
        directories = list(state.rglob("foundation-validation"))
        require(len(directories) == 1, "expected one disposable foundation validation directory")
        return state, directories[0]

    def rewind(self, name, state, directory, backup_task=False):
        node = self.node(f"{name}-source110", "fixture", state=state, backup_task=backup_task)
        self.ready(node, 110)
        require(self.source_snapshot(node, f"{name}-source110") == self.expected_final,
                "opening copied110 changed source state")
        block_hash = self.rpc(node, "getblockhash", [110])
        raw = self.rpc(node, "getblock", [block_hash, 0])
        block_file, marker_file = directory.parent / block_hash, directory / block_hash
        expected_backup = block_file.read_bytes()
        require(expected_backup[8:] == bytes.fromhex(raw), "stored backup does not bind the exact solved110 bytes")
        expected_marker = fixture_coverage(110, block_hash, expected_backup)
        require(covered_pair(block_file, marker_file, expected_backup, expected_marker),
                "original110 does not have exact rule/height/hash/byte coverage")
        (self.runtime / "private" / f"{name}-retained-block110.hex").write_text(raw + "\n")
        require(self.rpc(node, "invalidateblock", [block_hash]) is None, "could not invalidate copied110")
        self.wait(f"{name} reaches109", lambda: self.rpc(node, "getblockcount") == 109)
        self.wait(f"{name} removes orphan110 backup", lambda: not block_file.exists() and not marker_file.exists(), timeout=30)
        prefix = self.source_snapshot(node, f"{name}-verified-prefix109")
        return node, block_hash, raw, block_file, marker_file, expected_backup, expected_marker, prefix

    def denied_submission(self, node, directory, raw, block_file, marker_file, label):
        directory.chmod(0o500)
        require(self.rpc(node, "submitblock", [raw]) is None, "node rejected the exact previously solved110")
        self.wait(f"{label} accepts110 in memory", lambda: self.rpc(node, "getblockcount") == 110)
        log_file = self.runtime / "private" / f"{node['name']}.log"
        self.wait(f"{label} actually hits denied filesystem write",
                  lambda: "PermissionDenied" in log_file.read_text(), timeout=30)
        require(not block_file.exists() and not marker_file.exists(),
                "marker failure exposed an uncovered block110 instead of retaining verified109")
        self.event("publication-denied", label=label, memory_height=110, durable_prefix_height=109,
                   block110_published=False, marker110_published=False)

    def held_denial(self):
        state, directory = self.copy_source("held-denial")
        original_mode = stat.S_IMODE(directory.stat().st_mode)
        try:
            node, block_hash, raw, block_file, marker_file, backup, marker, prefix = self.rewind(
                "held-denial", state, directory)
            self.stop(node)
            node = self.node("held-denial-submit110", "fixture", state=state)
            self.ready(node, 109)
            self.denied_submission(node, directory, raw, block_file, marker_file, "held-through-stop")
            self.stop(node)
            require(node["process"].returncode == 0, "held-denial node did not stop normally")
            require(not block_file.exists() and not marker_file.exists(),
                    "failed shutdown persistence published an uncovered110")
            directory.chmod(original_mode)
            node = self.node("held-denial-restart-prefix109", "fixture", state=state)
            self.ready(node, 109)
            require(self.source_snapshot(node, "recovered-prefix109") == prefix,
                    "restart did not retain exactly the verified109 prefix")
            self.event("verified-prefix-recovered", height=109, tip=prefix["tip"], rebuild_required=False)
            require(self.rpc(node, "submitblock", [raw]) is None, "exact110 retry was rejected")
            self.wait("exact110 retry reaches110", lambda: self.rpc(node, "getblockcount") == 110)
            self.wait("exact110 retry publishes covered pair", lambda: covered_pair(block_file, marker_file, backup, marker))
            self.stop(node)
            node = self.node("held-denial-restart-repaired110", "fixture", state=state)
            self.ready(node, 110)
            require(self.source_snapshot(node, "exact-replay-repaired110") == self.expected_final,
                    "exact110 replay changed canonical hashes, supply, pools or UTXOs")
            self.event("exact-replay-repaired", height=110, tip=block_hash,
                       raw_sha256=hashlib.sha256(bytes.fromhex(raw)).hexdigest())
            self.stop(node)
        finally:
            directory.chmod(original_mode)

    def live_retry(self):
        state, directory = self.copy_source("live-retry")
        original_mode = stat.S_IMODE(directory.stat().st_mode)
        try:
            node, block_hash, raw, block_file, marker_file, backup, marker, prefix = self.rewind(
                "live-retry", state, directory, backup_task=True)
            # Invalidation retains the submitted-hash cache until restart. Start the live retry
            # interval from clean109, then keep this same process throughout failure and repair.
            self.stop(node)
            node = self.node("live-retry-submit110", "fixture", state=state, backup_task=True)
            self.ready(node, 109)
            require(self.source_snapshot(node, "live-retry-restarted-prefix109") == prefix,
                    "live retry setup restart changed the verified109 prefix")
            self.denied_submission(node, directory, raw, block_file, marker_file, "live-retry")
            same_pid = node["process"].pid
            restored = time.monotonic()
            directory.chmod(original_mode)
            self.wait("same-process retry publishes covered110 without another block",
                      lambda: covered_pair(block_file, marker_file, backup, marker), timeout=30)
            require(node["process"].poll() is None and node["process"].pid == same_pid and
                    self.rpc(node, "getblockcount") == 110 and self.rpc(node, "getbestblockhash") == block_hash,
                    "live retry must retain the same process and exact unchanged tip")
            self.event("periodic-live-retry-repaired", height=110, tip=block_hash, new_tip_required=False,
                       seconds_after_permission_restore=round(time.monotonic() - restored, 2))
            self.stop(node)
            node = self.node("live-retry-restart110", "fixture", state=state)
            self.ready(node, 110)
            require(self.source_snapshot(node, "final110") == self.expected_final,
                    "live-retry restart changed canonical hashes, supply, pools or UTXOs")
            self.stop(node)
        finally:
            directory.chmod(original_mode)

    def run(self):
        self.load_source()
        self.held_denial()
        self.live_retry()
        require(digest_tree(self.original_state) == self.original_digest, "original state changed during copied-state test")
        self.event("original-state-preserved", tree_sha256=self.original_digest)
        self.evidence["status"] = "passed"
        self.save()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifacts", type=Path, required=True, help="original artifact bundle or a verified fixture-node overlay")
    parser.add_argument("--source-artifacts", type=Path, default=REPO / "target/foundation-artifacts")
    parser.add_argument("--source-evidence", type=Path, required=True, help="passed full110 evidence; its state is copied only")
    parser.add_argument("--runtime-parent", type=Path)
    parser.add_argument("--overall-timeout", type=int, default=10 * 60)
    args = parser.parse_args()
    if args.overall_timeout < 1:
        parser.error("overall timeout must be positive")
    os.umask(0o077)
    gate = RecoveryGate(args)
    try:
        gate.run()
    except BaseException as error:
        gate.record_failure(error)
        gate.save()
        print(f"Backup recovery failed: {gate.evidence['failure']}; evidence retained at {gate.runtime}", file=sys.stderr)
    finally:
        gate.close()
        if hasattr(gate, "original_digest"):
            try:
                after = digest_tree(gate.original_state)
                gate.evidence["original_state_tree_sha256_after"] = after
                gate.evidence["original_state_preserved"] = after == gate.original_digest
            except OSError as error:
                gate.evidence["original_state_preserved"] = False
                gate.evidence["original_state_digest_error"] = type(error).__name__
            if not gate.evidence["original_state_preserved"]:
                gate.record_failure(RuntimeError("original source state was not preserved"))
                gate.evidence["original_state_preservation_failure"] = True
            gate.save()
    print(f"Backup recovery {gate.evidence['status']}: {gate.runtime / 'evidence.json'}", flush=True)
    return 0 if gate.evidence["status"] == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())
