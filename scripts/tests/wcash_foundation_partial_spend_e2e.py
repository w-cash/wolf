#!/usr/bin/env python3
"""Reproduce and verify partial shielding of a same-address split coinbase.

prepare uses the manifest-pinned baseline wallet to mine a disposable fixture
chain and reproduce its sync failure after one height-10 output is shielded in 110.
verify copies the stopped prepared state and wallet, then uses an explicitly
hashed new wallet. Original state, artifacts and signed bytes are never changed.
All seeds are public disposable vectors; raw wallet evidence stays owner-private.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sys
import tempfile

from wcash_foundation_fund_e2e import (
    BASE_SHA, FUND, MINER_SEED, REPO, SUBSIDY, TREASURY, TREASURY_SCRIPT,
    TREASURY_SEED, Gate, WireReader, require, require_foundation_rate,
)


def file_digest(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def tree_digest(path):
    digest = hashlib.sha256()
    for item in sorted(Path(path).rglob("*")):
        relative = item.relative_to(path).as_posix().encode()
        require(not item.is_symlink(), "disposable state must not contain symlinks")
        if item.is_file():
            digest.update(b"file\0" + relative + b"\0" + bytes.fromhex(file_digest(item)))
        elif item.is_dir():
            digest.update(b"dir\0" + relative)
        digest.update(b"\0")
    return digest.hexdigest()


def outpoint(utxo):
    return utxo["txid"], utxo["outputIndex"]


def signed_transparent_inputs(raw):
    """Read exact V6 input outpoints independently; node acceptance checks the tx."""
    reader = WireReader(bytes.fromhex(raw))
    require(reader.uint(4) == 0x80000006, "shielding transaction must be V6")
    require(reader.uint(4) == 0xD884B698, "shielding transaction has wrong version group")
    require(reader.uint(4) == 0xC3A6678A, "shielding transaction has wrong Regtest domain")
    reader.take(8)
    inputs = []
    for _ in range(reader.compact()):
        txid, index = reader.take(32)[::-1].hex(), reader.uint(4)
        reader.take(reader.compact())
        reader.take(4)
        inputs.append((txid, index))
    require(len(set(inputs)) == len(inputs), "duplicate shielding input")
    return inputs


def selected_single_input(raw, current_utxos, forbidden=()):
    inputs = signed_transparent_inputs(raw)
    require(len(inputs) == 1, "--max-inputs1 did not select exactly one input")
    selected = inputs[0]
    require(selected not in forbidden, "signed transaction resurrects an already-spent input")
    matches = [utxo for utxo in current_utxos if outpoint(utxo) == selected]
    require(len(matches) == 1, "signed input is not one exact current treasury UTXO")
    return matches[0]


def old_partial_error(reason):
    return ("payout-address output absent from the current UTXO set" in reason
            and "creator transaction" in reason)


def sha256_value(value, label):
    require(isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None,
            f"{label} must be a lowercase SHA256 digest")
    return value


def prepared_old_wallet_sha256(source):
    """Read the selected old identity, without reopening its retired executable."""
    binaries, manifest = source.get("binaries"), source.get("build_manifest")
    require(isinstance(binaries, dict) and isinstance(manifest, dict)
            and isinstance(manifest.get("binaries"), dict), "prepared artifact metadata is missing")
    require(manifest.get("baseline_sha") == BASE_SHA, "prepared baseline revision mismatch")
    if "old_wallet" in source:
        old = source["old_wallet"]
        require(isinstance(old, dict) and old.get("artifact") == "baseline/wcash-wallet"
                and old.get("source_revision") == BASE_SHA, "invalid selected old-wallet metadata")
        artifact, digest = old["artifact"], old.get("sha256")
    else:
        # Earlier preparations used a preserved pre-fix production artifact.
        artifact = "production/wcash-wallet"
        digest = binaries.get(artifact)
    digest = sha256_value(digest, "old wallet SHA256")
    require(binaries.get(artifact) == digest and manifest["binaries"].get(artifact) == digest,
            "old wallet SHA256 differs from its prepared artifact manifest")
    return digest


def checked_new_wallet(wallet, expected_sha256, source):
    old_sha256 = prepared_old_wallet_sha256(source)
    expected_sha256 = sha256_value(expected_sha256, "new wallet SHA256")
    wallet = Path(wallet).resolve()
    require(wallet.is_file() and os.access(wallet, os.X_OK), "new wallet is not executable")
    require(file_digest(wallet) == expected_sha256, "new wallet SHA256 mismatch")
    require(expected_sha256 != old_sha256, "new-wallet path still contains the old failing wallet")
    return wallet


class PartialSpendGate(Gate):
    def __init__(self, args):
        super().__init__(args)
        self.wallet_override = None
        self.original_sources = {}
        self.evidence.update({"scenario": "same-address-coinbase-partial-shield",
                              "phase": args.phase,
                              "driver_sha256": file_digest(Path(__file__)),
                              "gate_sha256": file_digest(Path(__file__).with_name("wcash_foundation_fund_e2e.py")),
                              "network_scope": "disposable literal-loopback Regtest only"})
        self.save()

    def wallet_binary(self):
        require(self.wallet_override is not None, "select a wallet before running wallet commands")
        return self.wallet_override

    def select_prepare_wallet(self):
        require(self.manifest.get("baseline_sha") == BASE_SHA, "artifact baseline mismatch")
        artifact = "baseline/wcash-wallet"
        wallet = self.binary("baseline", "wcash-wallet")
        digest = self.evidence["binaries"][artifact]
        require(digest != self.manifest["binaries"].get("production/wcash-wallet"),
                "baseline and production wallets must be distinct artifacts")
        self.wallet_override = wallet
        self.evidence["old_wallet"] = {"path": str(wallet), "artifact": artifact,
                                       "source_revision": BASE_SHA, "sha256": digest}
        self.save()

    def wallet(self, label, *arguments, seed=None, db=None, node=None, success=True):
        args = [self.wallet_binary(), "--network", "regtest"]
        if db:
            args.extend(["--db", self.runtime / "private" / f"{db}.sqlite"])
        if node:
            args.extend(["--lightwalletd", node["grpc"]])
        return self.command(label, args + list(arguments), stdin=seed + "\n" if seed else None,
                            timeout=900, success=success)

    def setup(self):
        manifest_path = self.artifacts / "manifest.json"
        require(manifest_path.is_file(), "build manifest missing; run scripts/build-wcash-foundation-fixtures.sh first")
        self.manifest = json.loads(manifest_path.read_text())
        require(self.manifest["baseline_sha"] == BASE_SHA, "artifact baseline mismatch")
        require("wcash-funding-test-fixture" in self.manifest["features"]["fixture"],
                "artifacts do not identify the isolated fixture")
        self.evidence["build_manifest"] = self.manifest
        if self.args.phase == "prepare":
            self.select_prepare_wallet()
        else:
            self.wallet_binary()
        for profile, name in [("fixture", "wcash-zebrad"), ("fixture", "wcash-merge-miner"),
                              ("parent", "zcash-zebrad")]:
            self.binary(profile, name)
        policy = self.command("fixture-policy", [self.binary("fixture", "wcash-zebrad"), "funding-policy"])
        require_foundation_rate(policy)
        require(policy["fixture"] is True and policy["network"] == "WcashRegtest"
                and policy["start_height"] == 10 and policy["end_height_exclusive"] == 20
                and policy["recipient"] == TREASURY and policy["script_pub_key"] == TREASURY_SCRIPT,
                "node does not have the exact isolated fixture policy")
        coordinator_policy = self.command("fixture-coordinator-policy", [
            self.binary("fixture", "wcash-merge-miner"), "funding-policy"])
        require_foundation_rate(coordinator_policy)
        require(coordinator_policy == policy, "fixture node and coordinator policies differ")
        self.miner = self.wallet("derive-miner", "derive-address", seed=MINER_SEED)
        treasury = self.wallet("derive-treasury", "derive-address", seed=TREASURY_SEED)
        require(treasury["transparent_coinbase_address"] == TREASURY,
                "public disposable treasury vector mismatch")
        require(self.miner["transparent_coinbase_address"] != TREASURY,
                "ordinary mining recipient must differ from treasury")
        self.save()

    def mine(self, node, treasury_miner=False, label=None):
        previous = self.miner
        if treasury_miner:
            self.miner = dict(previous, transparent_coinbase_address=TREASURY)
        try:
            return self.native(node, "fixture", private=False, label=label)
        finally:
            self.miner = previous

    def treasury_utxos(self, node):
        return self.rpc(node, "getaddressutxos", [{"addresses": [TREASURY]}])

    def shield(self, node, label, forbidden=()):
        before = self.treasury_utxos(node)
        signed = self.wallet(label, "shield-coinbase", "--max-inputs", "1",
                             "--expiry-delta", "40", "--lock-for-blocks", "100",
                             seed=TREASURY_SEED, db="treasury", node=node)
        selected = selected_single_input(signed["raw_transaction_hex"], before, forbidden)
        target = self.rpc(node, "getblockcount") + 1
        require(signed["target_height"] == target and target >= selected["height"] + 100,
                "shielding changed the exact coinbase maturity rule")
        require(0 < signed["fee_zat"] < selected["satoshis"]
                and signed["internal_change_receiver_verified"] is True,
                "shielding fee or internal change check failed")
        self.command(label + "-broadcast", [self.wallet_binary(), "--network", "regtest",
                    "--lightwalletd", node["grpc"], "broadcast"],
                     stdin=signed["raw_transaction_hex"] + "\n")
        self.wait(label + " in template", lambda: any(tx["hash"] == signed["txid"]
                  for tx in self.rpc(node, "getblocktemplate")["transactions"]), timeout=900)
        decoded = self.rpc(node, "getrawtransaction", [signed["txid"], 1])
        require(decoded["vout"] == [] and len(decoded["vin"]) == 1
                and decoded["ironwood"]["valueBalanceZat"] == -(selected["satoshis"] - signed["fee_zat"]),
                "signed transaction has unexpected input/output/value shape")
        mined = self.mine(node, label=label + "-mined")
        require(mined["fees"] == signed["fee_zat"], "shielding fee did not all reach the miner")
        after = self.treasury_utxos(node)
        require(outpoint(selected) not in {outpoint(utxo) for utxo in after},
                "mined shield input remains live")
        self.event("partial-shield", label=label, target_height=target, txid=signed["txid"],
                   raw_sha256=hashlib.sha256(bytes.fromhex(signed["raw_transaction_hex"])).hexdigest(),
                   selected=selected, fee_zat=signed["fee_zat"],
                   shielded_zat=selected["satoshis"] - signed["fee_zat"])
        return {"txid": signed["txid"], "selected": selected, "fee_zat": signed["fee_zat"],
                "shielded_zat": selected["satoshis"] - signed["fee_zat"]}

    def assert_balance(self, account, transparent, ironwood):
        require(account["transparent_coinbase_total_zat"] == transparent,
                "wallet lost or resurrected a transparent coinbase receipt")
        require(account["transparent_regular_total_zat"] == 0,
                "coinbase receipt was misclassified as ordinary transparent value")
        require(account["ironwood_total_zat"] == ironwood,
                "wallet Ironwood total does not match exact shielding values")
        require(account["sapling_total_zat"] == 0 and account["orchard_total_zat"] == 0,
                "legacy value pools must remain empty")

    def stop_sources(self, node):
        sources = {}
        for label, current in [("child", node), ("parent", self.parent), ("validator", self.validator)]:
            self.stop(current)
            sources[label] = {"path": str(current["state"]), "sha256": tree_digest(current["state"])}
        self.evidence["stopped_states"] = sources
        self.save()

    def prepare(self):
        self.setup()
        self.parent = self.node("parent-template", "parent", parent=True)
        self.validator = self.node("parent-validator", "parent", parent=True)
        node = self.node("partial-child", "fixture")
        for current in [self.parent, self.validator, node]:
            self.ready(current)
        for height in range(1, 110):
            self.mine(node, treasury_miner=height == 10)
        utxos = self.treasury_utxos(node)
        siblings = [utxo for utxo in utxos if utxo["height"] == 10]
        require(len(siblings) == 2 and len({utxo["txid"] for utxo in siblings}) == 1
                and sorted(utxo["satoshis"] for utxo in siblings) == [FUND, SUBSIDY - FUND]
                and all(utxo["script"] == TREASURY_SCRIPT for utxo in siblings),
                "height10 is not the intended two-output same-address coinbase")
        total = 10 * FUND + SUBSIDY - FUND
        require(sum(utxo["satoshis"] for utxo in utxos) == total,
                "unexpected treasury receipts outside the intended fixture term")
        self.wallet("init-old-treasury", "init", "--birthday", "1", seed=TREASURY_SEED,
                    db="treasury", node=node)
        account = self.sync_wallet("treasury", node)
        self.assert_balance(account, total, 0)
        require(account["transparent_coinbase_spendable_zat"] == SUBSIDY,
                "both height10 siblings must mature together at target110")
        shield = self.shield(node, "old-wallet-first-shield")
        require(outpoint(shield["selected"]) in {outpoint(utxo) for utxo in siblings},
                "first shield must spend one of the two height10 siblings")
        remaining = [utxo for utxo in self.treasury_utxos(node) if utxo["height"] == 10]
        require(len(remaining) == 1 and outpoint(remaining[0]) != outpoint(shield["selected"]),
                "first shield must leave exactly one sibling live")
        reason = self.wallet("old-wallet-expected-partial-sync-failure", "sync", "--batch-size", "16",
                             db="treasury", node=node, success=False)
        require(old_partial_error(reason), "old wallet failed for a reason other than the partial-creator mismatch")
        self.event("old-wallet-partial-sync-reproduced", error_category="TransparentUtxoMismatch",
                   old_wallet_sha256=self.evidence["old_wallet"]["sha256"],
                   surviving_sibling=remaining[0])
        self.evidence.update({"first_shield": shield, "height10_siblings": siblings,
                              "initial_treasury_total_zat": total,
                              "remaining_transparent_zat": total - shield["selected"]["satoshis"],
                              "expected_ironwood_zat": shield["shielded_zat"]})
        self.snapshot(node, "prepared110", heights=tuple(range(111)))
        self.stop_sources(node)
        self.evidence["prepared_wallet_files"] = {str(path.relative_to(self.runtime)): file_digest(path)
            for path in sorted((self.runtime / "private").glob("treasury.sqlite*")) if path.is_file()}
        require(file_digest(self.wallet_binary()) == self.evidence["old_wallet"]["sha256"],
                "old wallet changed during preparation")
        self.evidence["status"] = "reproduced"
        self.save()

    def verify(self):
        source_file = self.args.source_evidence.resolve()
        source = json.loads(source_file.read_text())
        require(source["scenario"] == "same-address-coinbase-partial-shield"
                and source["phase"] == "prepare" and source["status"] == "reproduced",
                "verification requires a stopped successful old-wallet reproduction")
        source_runtime = Path(source["runtime"]).resolve()
        require(source_file.parent == source_runtime, "source evidence does not belong to its runtime")
        require({event["name"] for event in source["events"] if event["event"] == "node-start"}
                <= {event["name"] for event in source["events"] if event["event"] == "node-stop"},
                "all prepared source nodes must be stopped")
        wallet = checked_new_wallet(self.args.new_wallet, self.args.new_wallet_sha256, source)
        self.wallet_override = wallet
        self.evidence["new_wallet"] = {"path": str(wallet), "sha256": file_digest(wallet)}
        if self.args.new_wallet_provenance:
            proof = self.args.new_wallet_provenance.resolve()
            self.evidence["new_wallet"]["external_build_provenance"] = {
                "path": str(proof), "sha256": file_digest(proof)}
        self.setup()
        require(self.manifest == source["build_manifest"], "immutable source artifact manifest changed")
        copied = {}
        for label, item in source["stopped_states"].items():
            path = Path(item["path"]).resolve()
            require(path.is_dir() and path.is_relative_to(source_runtime),
                    "prepared source state is outside its disposable runtime")
            require(tree_digest(path) == item["sha256"], "prepared source state changed before verification")
            self.original_sources[path] = item["sha256"]
            dest = self.runtime / f"{label}-state-copy"
            shutil.copytree(path, dest)
            copied[label] = dest
        for relative, expected in source["prepared_wallet_files"].items():
            path = (source_runtime / relative).resolve()
            require(path.is_relative_to(source_runtime / "private") and path.is_file()
                    and file_digest(path) == expected, "prepared wallet file changed or escaped private runtime")
            destination = self.runtime / relative
            destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            shutil.copy2(path, destination)
        self.evidence.update({"source_evidence": str(source_file),
                              "source_evidence_sha256": file_digest(source_file),
                              "original_build_manifest": source["build_manifest"]})
        self.parent = self.node("parent-template", "parent", state=copied["parent"], parent=True)
        self.validator = self.node("parent-validator", "parent", state=copied["validator"], parent=True)
        node = self.node("fixed-wallet-child", "fixture", state=copied["child"])
        self.ready(self.parent, None)
        self.ready(self.validator, None)
        self.ready(node, 110)
        expected = next(event for event in source["events"]
                        if event["event"] == "state-snapshot" and event["label"] == "prepared110")
        actual = self.snapshot(node, "copied110", heights=tuple(range(111)))
        require(actual == {key: expected[key] for key in actual}, "copied source110 snapshot differs")
        for iteration in range(3):
            account = self.sync_wallet("treasury", node)
            self.assert_balance(account, source["remaining_transparent_zat"], source["expected_ironwood_zat"])
            self.event("fixed-wallet-repeat-sync", iteration=iteration + 1, account=account)
        self.wallet("restore-fixed-treasury110", "init", "--birthday", "1", seed=TREASURY_SEED,
                    db="treasury-restored110", node=node)
        restored = self.sync_wallet("treasury-restored110", node)
        self.assert_balance(restored, source["remaining_transparent_zat"], source["expected_ironwood_zat"])
        first = source["first_shield"]
        second = self.shield(node, "fixed-wallet-second-shield", forbidden=[outpoint(first["selected"])])
        sibling = next(utxo for utxo in source["height10_siblings"]
                       if outpoint(utxo) != outpoint(first["selected"]))
        spent_remaining_sibling = outpoint(second["selected"]) == outpoint(sibling)
        transparent = source["remaining_transparent_zat"] - second["selected"]["satoshis"]
        ironwood = source["expected_ironwood_zat"] + second["shielded_zat"]
        for db in ["treasury", "treasury-restored110"]:
            for _ in range(2):
                self.assert_balance(self.sync_wallet(db, node), transparent, ironwood)
        self.wallet("restore-fixed-treasury111", "init", "--birthday", "1", seed=TREASURY_SEED,
                    db="treasury-restored111", node=node)
        self.assert_balance(self.sync_wallet("treasury-restored111", node), transparent, ironwood)
        self.evidence.update({"second_shield": second,
                              "second_shield_spent_remaining_height10_sibling": spent_remaining_sibling,
                              "final_transparent_zat": transparent, "final_ironwood_zat": ironwood})
        self.snapshot(node, "verified111", heights=tuple(range(112)))
        self.stop_sources(node)
        require(all(tree_digest(path) == expected for path, expected in self.original_sources.items()),
                "verification modified an original prepared source state")
        for relative, expected in source["prepared_wallet_files"].items():
            require(file_digest(source_runtime / relative) == expected,
                    "verification modified an original prepared wallet file")
        require(file_digest(wallet) == self.args.new_wallet_sha256, "new wallet changed during verification")
        self.evidence["status"] = "passed"
        self.save()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", choices=("prepare", "verify"), required=True,
                        help="prepare selects the pinned baseline wallet; verify requires a hashed new wallet")
    parser.add_argument("--artifacts", type=Path, default=REPO / "target/foundation-artifacts")
    parser.add_argument("--runtime-parent", type=Path, default=Path(tempfile.gettempdir()))
    parser.add_argument("--overall-timeout", type=int, default=14_400)
    parser.add_argument("--source-evidence", type=Path)
    parser.add_argument("--new-wallet", type=Path)
    parser.add_argument("--new-wallet-sha256")
    parser.add_argument("--new-wallet-provenance", type=Path)
    args = parser.parse_args()
    if args.overall_timeout < 1:
        parser.error("overall timeout must be positive")
    if args.phase == "verify" and not all([args.source_evidence, args.new_wallet, args.new_wallet_sha256]):
        parser.error("verify requires --source-evidence, --new-wallet and --new-wallet-sha256")
    os.umask(0o077)
    gate = PartialSpendGate(args)
    try:
        getattr(gate, args.phase)()
    except BaseException as error:
        gate.record_failure(error)
        gate.save()
        print(f"Partial-spend {args.phase} failed; owner-private evidence: {gate.runtime / 'evidence.json'}", file=sys.stderr)
        return 1
    finally:
        gate.close()
    expected_status = "reproduced" if args.phase == "prepare" else "passed"
    if gate.evidence["status"] != expected_status:
        print(f"Partial-spend cleanup failed; inspect {gate.runtime / 'evidence.json'}", file=sys.stderr)
        return 1
    print(f"Partial-spend {args.phase} {expected_status}; evidence: {gate.runtime / 'evidence.json'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
