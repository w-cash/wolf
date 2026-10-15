#!/usr/bin/env python3
"""Bounded real-solver foundation gate. Uses only disposable loopback Regtest.

Build first with scripts/build-wcash-foundation-fixtures.sh. Every run retains
owner-private state and evidence, even on failure. No public endpoints or
credentials are read. Parent fixtures prove structure/target/proposal/chain
acceptance; their disabled native PoW check is not independent Equihash proof.
After a passed run, --covered-restart-source copies its stopped state for an
actual baseline downgrade. Fork10 appends unfunded active11; fork110 appends
post-expiry111. Both require the upgraded executable's actionable refusal.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

REPO = Path(__file__).resolve().parents[2]
WCASH_GENESIS = "70bf0bab17eff361a6331bb825b3b7253c8c96ff96407f948161d2912658bb1c"
ZCASH_GENESIS = "029f11d80ef9765602235e1bc9727e3eb6ba20839319f761fee920d63401e327"
BASE_SHA = "41a39cab3f259509653c1d4930df3dae3dabba98"
TREASURY = "WRHUq9CTLZFa52NmAyHH5usN21Q8jZVskN1"
TREASURY_SCRIPT = "76a9147d437bac720b610692ae373e43fa654c50c196f288ac"
PARENT_PAYOUT = "tmJymvcUCn1ctbghvTJpXBwHiMEB8P6wxNV"
SUBSIDY, FUND = 625_000_000, 125_000_000
TREASURY_SEED = bytes(range(32)).hex()
MINER_SEED = bytes(range(32, 64)).hex()
RECEIVER_SEED = bytes(range(64, 96)).hex()


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


class RpcFailure(RuntimeError):
    pass


def displayed_hashes(result):
    """Normalize the existing reconsiderblock RPC's raw little-endian hashes."""
    require(isinstance(result, list), "reconsiderblock must return an array")
    hashes = set()
    for value in result:
        if isinstance(value, str):
            require(len(value) == 64 and all(char in "0123456789abcdefABCDEF" for char in value),
                    "invalid reconsiderblock display hash")
            hashes.add(value.lower())
        else:
            require(isinstance(value, list) and len(value) == 32 and
                    all(type(atom) is int and 0 <= atom <= 255 for atom in value),
                    "invalid reconsiderblock serialized hash")
            hashes.add(bytes(value)[::-1].hex())
    return hashes


class WireReader:
    """Small independent prefix reader; it never claims to validate a proof."""
    def __init__(self, data):
        self.data, self.pos = data, 0

    def take(self, size):
        require(0 <= size <= len(self.data) - self.pos, "truncated serialized candidate")
        result = self.data[self.pos:self.pos + size]
        self.pos += size
        return result

    def uint(self, size):
        return int.from_bytes(self.take(size), "little")

    def compact(self):
        value = self.uint(1)
        if value < 253:
            return value
        size = {253: 2, 254: 4, 255: 8}[value]
        value = self.uint(size)
        require(value >= {2: 253, 4: 65536, 8: 4294967296}[size], "noncanonical CompactSize")
        require(value <= 2_000_000, "oversized vector in local candidate")
        return value


def inspect_coinbase(raw, is_block=True):
    reader = WireReader(bytes.fromhex(raw))
    if is_block:
        require(reader.uint(4) == 0xD7430001, "candidate is not Wcash wire version")
        reader.take(136)
        reader.take(reader.compact())
        require(reader.compact() >= 1, "candidate has no transactions")
    require(reader.uint(4) == 0x80000006, "coinbase is not V6")
    require(reader.uint(4) == 0xD884B698, "unexpected V6 version group")
    branch_id = reader.uint(4)
    reader.take(8)  # lock time and expiry
    require(reader.compact() == 1, "coinbase must have one input")
    require(reader.take(36) == bytes(32) + b"\xff" * 4, "input is not coinbase")
    script = reader.take(reader.compact())
    reader.take(4)
    outputs = []
    for _ in range(reader.compact()):
        value = struct.unpack("<q", reader.take(8))[0]
        outputs.append({"valueZat": value, "script": reader.take(reader.compact()).hex()})
    require(reader.compact() == 0 and reader.compact() == 0, "unexpected Sapling coinbase component")
    require(reader.compact() == 0, "unexpected legacy Orchard coinbase component")
    actions = reader.compact()
    private_value = 0
    if actions:
        # Orchard-shaped Ironwood action serialization is 820 bytes per action.
        reader.take(actions * 820)
        flags = reader.uint(1)
        require(flags & 1 == 0, "coinbase Ironwood spends are enabled")
        private_value = -struct.unpack("<q", reader.take(8))[0]
    return {"branch_id": f"{branch_id:08x}", "input_script": script.hex(),
            "outputs": outputs, "ironwood_actions": actions, "private_valueZat": private_value}


class Gate:
    def __init__(self, args):
        self.args = args
        self.artifacts = args.artifacts.resolve()
        self.runtime = Path(tempfile.mkdtemp(prefix="wcash-foundation-e2e-", dir=args.runtime_parent)).resolve()
        self.runtime.chmod(0o700)
        (self.runtime / "private").mkdir(mode=0o700)
        self.env = {key: value for key, value in os.environ.items()
                    if not key.startswith(("WCASH_", "ZCASH_", "ZEBRA_", "ZEBRAD_"))
                    and key.lower() not in ("http_proxy", "https_proxy", "all_proxy", "no_proxy")}
        self.env["RUST_BACKTRACE"] = "0"
        self.nodes = []
        self.counter, self.nonce = 0, 0
        self.retained_blocks = {}
        self.used_ports = set()
        self.deadline = time.monotonic() + args.overall_timeout
        self.evidence = {"status": "running", "base_sha": BASE_SHA, "runtime": str(self.runtime),
                         "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                         "parent_validation": "structure, target, proposal and chain acceptance; not independent Equihash verification",
                         "events": [], "binaries": {}}
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        print(f"Foundation E2E evidence: {self.runtime / 'evidence.json'}", flush=True)
        self.save()

    def save(self):
        temporary = self.runtime / "evidence.tmp"
        temporary.write_text(json.dumps(self.evidence, indent=2) + "\n")
        temporary.replace(self.runtime / "evidence.json")

    def event(self, kind, **details):
        self.evidence["events"].append({"event": kind, **details})
        self.save()
        if kind == "mined-block":
            print(f"Mined {details['node']} height {details['height']} private={details['private']} fundZat={details['fundZat']} feesZat={details['feesZat']}", flush=True)
        elif kind in ("node-start", "node-stop", "state-snapshot", "compatibility-matrix", "late-upgrade-refused", "competing-branch-replacement", "treasury-custody"):
            print(f"Phase {kind}: {details.get('name', details.get('label', 'recorded'))}", flush=True)

    def bounded(self, timeout):
        remaining = self.deadline - time.monotonic()
        require(remaining > 0, "overall foundation E2E deadline exceeded")
        return min(timeout, remaining)

    def binary(self, profile, name):
        binary = self.artifacts / profile / name
        require(binary.is_file() and os.access(binary, os.X_OK), f"missing executable {binary}")
        label = f"{profile}/{name}"
        if label not in self.evidence["binaries"]:
            digest = hashlib.sha256()
            with binary.open("rb") as source:
                for chunk in iter(lambda: source.read(1024 * 1024), b""):
                    digest.update(chunk)
            self.evidence["binaries"][label] = digest.hexdigest()
            if hasattr(self, "manifest"):
                require(self.manifest["binaries"].get(label) == digest.hexdigest(), f"artifact hash differs from build manifest: {label}")
            self.save()
        return binary

    def command(self, label, arguments, stdin=None, env=None, success=True, timeout=180):
        self.counter += 1
        prefix = self.runtime / "private" / f"{self.counter:04d}-{label}"
        started = time.monotonic()
        print(f"Command {self.counter}: {label}", flush=True)
        command_timeout = self.bounded(timeout)
        with prefix.with_suffix(".stdout").open("w") as out, prefix.with_suffix(".stderr").open("w") as err:
            process = subprocess.Popen([str(value) for value in arguments], stdin=subprocess.PIPE,
                                       stdout=out, stderr=err, env=env or self.env, start_new_session=True)
            try:
                process.communicate(input=stdin.encode() if stdin is not None else None,
                                    timeout=command_timeout)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(10)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
                self.event("command", label=label, exit_code="timeout", seconds=round(time.monotonic() - started, 2))
                raise RuntimeError(f"{label} exceeded its bounded timeout; private output retained")
            except BaseException:
                if process.poll() is None:
                    os.killpg(process.pid, signal.SIGTERM)
                    try:
                        process.wait(10)
                    except subprocess.TimeoutExpired:
                        os.killpg(process.pid, signal.SIGKILL)
                        process.wait()
                raise
        self.event("command", label=label, command=[str(value) for value in arguments], exit_code=process.returncode,
                   seconds=round(time.monotonic() - started, 2))
        require((process.returncode == 0) == success, f"{label} returned {process.returncode}; inspect owner-private diagnostics")
        output = prefix.with_suffix(".stdout").read_text()
        if not success:
            return prefix.with_suffix(".stderr").read_text() + output
        try:
            return json.loads(output)
        except json.JSONDecodeError:
            raise RuntimeError(f"{label} did not return JSON; private output retained") from None

    def rpc(self, node, method, params=None, timeout=120):
        payload = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params or []}).encode()
        request = urllib.request.Request(node["rpc"], payload, {"content-type": "application/json"})
        with self.opener.open(request, timeout=self.bounded(timeout)) as response:
            result = json.load(response)
        if result.get("error") is not None:
            error = result["error"]
            raise RpcFailure(f"{node['name']} {method}: {error.get('code')} {error.get('message')}")
        return result.get("result")

    def wait(self, label, callback, timeout=90):
        until = time.monotonic() + self.bounded(timeout)
        last_error = "condition unchanged"
        while time.monotonic() < until:
            try:
                value = callback()
                if value:
                    return value
            except (OSError, ValueError, RpcFailure) as error:
                last_error = type(error).__name__
            time.sleep(0.25)
        raise RuntimeError(f"timeout waiting for {label} ({last_error})")

    @staticmethod
    def port():
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            return listener.getsockname()[1]

    def node(self, name, profile, state=None, parent=False, peers=(), backup_task=False):
        ports = []
        while len(ports) < 3:
            port = self.port()
            if port not in self.used_ports:
                ports.append(port)
                self.used_ports.add(port)
        state = state or self.runtime / f"{name}-state"
        state.mkdir(mode=0o700, exist_ok=True)
        config = self.runtime / f"{name}.toml"
        quoted_peers = ", ".join(json.dumps(peer) for peer in peers)
        config.write_text(f'''[network]
network = "{'Regtest' if parent else 'WcashRegtest'}"
listen_addr = "127.0.0.1:{ports[0]}"
initial_mainnet_peers = []
initial_testnet_peers = [{quoted_peers}]
cache_dir = false
peerset_initial_target_size = 8
max_connections_per_ip = 8
''' + ('''[network.testnet_parameters.activation_heights]
"NU6.3" = 1
''' if parent else "") + f'''
[state]
ephemeral = false
cache_dir = {json.dumps(str(state))}
should_backup_non_finalized_state = true
debug_skip_non_finalized_state_backup_task = {str(not backup_task).lower()}
delete_old_database = false
[rpc]
listen_addr = "127.0.0.1:{ports[1]}"
''' + ("" if parent else f'lightwalletd_listen_addr = "127.0.0.1:{ports[2]}"\n') + f'''enable_cookie_auth = false
debug_force_finished_sync = true
[mempool]
debug_enable_at_height = 0
[mining]
miner_address = {json.dumps(PARENT_PAYOUT if parent else self.miner['transparent_coinbase_address'])}
extra_coinbase_data = "WcashFoundationLocalGate"
internal_miner = false
[tracing]
filter = "info"
''')
        binary = self.binary(profile, "zcash-zebrad" if parent else "wcash-zebrad")
        log = (self.runtime / "private" / f"{name}.log").open("a")
        process = subprocess.Popen([str(binary), "-c", str(config), "start"], stdout=log, stderr=subprocess.STDOUT,
                                   env=self.env, start_new_session=True)
        node = {"name": name, "profile": profile, "state": state, "rpc": f"http://127.0.0.1:{ports[1]}",
                "grpc": f"http://127.0.0.1:{ports[2]}", "peer": f"127.0.0.1:{ports[0]}", "process": process,
                "log": log, "config": config}
        self.nodes.append(node)
        self.event("node-start", name=name, profile=profile, state=str(state), rpc=node["rpc"],
                   command=[str(binary), "-c", str(config), "start"])
        return node

    def ready(self, node, expected=0):
        def started():
            require(node["process"].poll() is None, f"{node['name']} exited during startup; inspect private node log")
            height = self.rpc(node, "getblockcount")
            return (expected is None or height == expected) and self.rpc(node, "getblockhash", [0]) == (
                ZCASH_GENESIS if node["profile"] == "parent" else WCASH_GENESIS)
        self.wait(f"{node['name']} RPC at height {expected}", started)
        if node["profile"] != "parent":
            def grpc_ready():
                with socket.create_connection(("127.0.0.1", int(node["grpc"].rsplit(":", 1)[1])), timeout=1):
                    return True
            self.wait(f"{node['name']} compact-block service", grpc_ready)

    def stop(self, node):
        process = node["process"]
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGINT)
            try:
                process.wait(45)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(10)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
                    raise RuntimeError(f"{node['name']} did not shut down cleanly; state retained") from None
        node["log"].close()
        self.event("node-stop", name=node["name"], exit_code=process.returncode)

    def wallet(self, label, *arguments, seed=None, db=None, node=None, success=True):
        args = [self.binary("production", "wcash-wallet"), "--network", "regtest"]
        if db:
            args.extend(["--db", self.runtime / "private" / f"{db}.sqlite"])
        if node:
            args.extend(["--lightwalletd", node["grpc"]])
        return self.command(label, args + list(arguments), stdin=seed + "\n" if seed else None,
                            timeout=900, success=success)

    def sync_wallet(self, db, node):
        result = self.wallet(f"{db}-sync", "sync", "--batch-size", "16", db=db, node=node)
        height = self.rpc(node, "getblockcount")
        require(result.get("synchronized") is True and result["fully_scanned_height"] == height,
                f"{db} did not fully synchronize")
        return result["accounts"][0]

    def miner_env(self, node, address, private, label):
        env = dict(self.env)
        env.update({"WCASH_EXPECTED_GENESIS_HASH": WCASH_GENESIS, "ZCASH_EXPECTED_GENESIS_HASH": ZCASH_GENESIS,
                    "ZCASH_NETWORK": "regtest", "WCASH_SHARE_JOURNAL": str(self.runtime / "private" / f"{label}.jsonl"),
                    "WCASH_PAYOUT_ADDRESS": address, "ZCASH_PAYOUT_ADDRESS": PARENT_PAYOUT})
        if private:
            env["WCASH_PAYOUT_IVK_FILE"] = str(self.miner_ivk)
        return env

    def native(self, node, profile, private=False, command="native-mine", success=True, label=None):
        height = self.rpc(node, "getblockcount") + 1
        label = label or f"{profile}-{command}-{height}"
        payout = self.miner["address"] if private else self.miner["transparent_coinbase_address"]
        args = [self.binary(profile, "wcash-merge-miner"), command, node["rpc"], self.parent["rpc"],
                self.validator["rpc"], "-"]
        self.nonce += 512
        if command == "native-mine":
            args.extend([512, self.nonce])
        result = self.command(label, args, env=self.miner_env(node, payout, private, label),
                              timeout=900, success=success)
        if not success:
            return result
        if command == "native-mine":
            require(result.get("result") == "processed" and result.get("wcash_candidate") is True,
                    f"{label} did not produce a child winner")
            self.wait(f"{node['name']} block {height}", lambda: self.rpc(node, "getblockcount") == height)
            self.wait("both parents agree", lambda: self.rpc(self.parent, "getbestblockhash") ==
                      self.rpc(self.validator, "getbestblockhash"))
            return self.inspect_mined(node, height, private, funding_policy=profile != "baseline")
        return result

    def inspect(self, raw, height, private, fees=0, funding_policy=True):
        parsed = inspect_coinbase(raw)
        require(parsed["branch_id"] == "c3a6678a", "wrong Wcash Regtest transaction domain")
        active = funding_policy and 10 <= height < 20
        outputs = list(parsed["outputs"])
        fund = {"valueZat": FUND, "script": TREASURY_SCRIPT}
        if active:
            require(fund in outputs, f"height {height}: missing exact mandatory fund output")
            outputs.remove(fund)
        if private:
            require(not outputs and parsed["ironwood_actions"] > 0, f"height {height}: invalid private payout shape")
            require(parsed["private_valueZat"] == SUBSIDY - (FUND if active else 0) + fees,
                    f"height {height}: wrong net private value")
        else:
            require(parsed["ironwood_actions"] == 0 and len(outputs) == 1,
                    f"height {height}: invalid transparent miner shape")
            require(outputs[0]["valueZat"] == SUBSIDY - (FUND if active else 0) + fees,
                    f"height {height}: wrong net transparent value")
        return parsed

    def inspect_mined(self, node, height, private, funding_policy=True):
        block = self.rpc(node, "getblock", [str(height), 2])
        raw = self.rpc(node, "getblock", [str(height), 0])
        coinbase = block["tx"][0]
        ironwood = coinbase.get("ironwood") or {}
        total_out = sum(output["valueZat"] for output in coinbase["vout"]) - ironwood.get("valueBalanceZat", 0)
        fees = total_out - SUBSIDY
        require(fees >= 0, "coinbase underclaims gross subsidy")
        parsed = self.inspect(raw, height, private, fees, funding_policy=funding_policy)
        self.retained_blocks[height] = raw
        subsidy = self.rpc(node, "getblocksubsidy", [height])
        self.event("mined-block", node=node["name"], height=height, hash=block["hash"], private=private,
                   feesZat=fees, fundZat=FUND if funding_policy and 10 <= height < 20 else 0, coinbase=parsed, subsidy=subsidy)
        return {"hash": block["hash"], "raw": raw, "fees": fees}

    def candidate(self, node, private, funded=True):
        address = self.miner["address"] if private else self.miner["transparent_coinbase_address"]
        expected_height = self.rpc(node, "getblockcount") + 1
        result = self.rpc(node, "createauxblock", [address], timeout=900)
        height = result["height"]
        require(height == expected_height, "candidate cache retained a stale height after a branch change")
        if funded:
            parsed = self.inspect(result["data"], height, private)
        else:
            parsed = inspect_coinbase(result["data"])
        self.event("candidate", node=node["name"], height=height, hash=result["hash"],
                   private=private, advertised_miner_valueZat=result["coinbasevalue"], coinbase=parsed)
        expected = SUBSIDY - (FUND if funded and 10 <= height < 20 else 0)
        require(result["coinbasevalue"] == expected, "createauxblock advertises gross rather than net miner value")
        return result

    def retire(self, node, candidate):
        result = self.rpc(node, "retireauxblock", [candidate["hash"], candidate["retiretoken"]])
        require(result["state"] in ("retired", "already_absent"), "candidate retirement failed")

    def snapshot(self, node, label, heights=(0, 8, 9, 10, 19, 20, 21)):
        height = self.rpc(node, "getblockcount")
        info = self.rpc(node, "getblockchaininfo")
        snapshot = {"height": height, "tip": self.rpc(node, "getbestblockhash"),
                    "hashes": {str(h): self.rpc(node, "getblockhash", [h]) for h in heights if h <= height},
                    "supply": info["chainSupply"], "pools": info["valuePools"],
                    "treasury_utxos": self.rpc(node, "getaddressutxos", [{"addresses": [TREASURY]}])}
        self.event("state-snapshot", label=label, **snapshot)
        return snapshot

    def check_treasury_reorg_balance(self, node, label, snapshot):
        account = self.sync_wallet("treasury", node)
        expected = sum(utxo["satoshis"] for utxo in snapshot["treasury_utxos"])
        require(account["transparent_coinbase_total_zat"] == expected and
                account["transparent_total_zat"] == expected and
                account["transparent_coinbase_spendable_zat"] == 0 and
                account["ironwood_total_zat"] == 0,
                f"treasury incremental wallet balance differs from canonical UTXOs at {label}")
        self.event("treasury-reorg-wallet", label=label, height=snapshot["height"],
                   canonical_coinbase_valueZat=expected,
                   wallet_coinbase_valueZat=account["transparent_coinbase_total_zat"],
                   wallet_spendable_valueZat=account["transparent_coinbase_spendable_zat"])

    def proposal(self, node, candidate):
        return self.rpc(node, "getblocktemplate", [{"mode": "proposal", "data": candidate["data"]}], timeout=900)

    def artifact_isolation_preflight(self):
        """Exercise each executable's fail-closed configuration boundary locally."""
        preflight = self.runtime / "artifact-preflight"
        preflight.mkdir(mode=0o700)

        def config(name, network, addition=""):
            path = preflight / f"{name}.toml"
            state = preflight / f"{name}-state"
            state.mkdir(mode=0o700)
            path.write_text(f'''[network]
network = {json.dumps(network)}
listen_addr = "127.0.0.1:0"
initial_mainnet_peers = []
initial_testnet_peers = []
cache_dir = false
[state]
ephemeral = false
cache_dir = {json.dumps(str(state))}
delete_old_database = false
[tracing]
filter = "info"
''' + addition)
            return path, state

        fixture = self.binary("fixture", "wcash-zebrad")
        for network in ("WcashMainnet", "WcashTestnet"):
            path, state = config(f"fixture-{network}", network)
            reason = self.command(f"fixture-refuses-{network}", [fixture, "-c", path, "start"],
                                  success=False, timeout=30)
            require("LOCAL FUNDING TEST FIXTURE only supports network" in reason and "WcashRegtest" in reason,
                    f"fixture {network} startup failed for an unrelated reason")
            self.event("artifact-isolation-rejection", profile="fixture", action="start", network=network,
                       reason="LOCAL FUNDING TEST FIXTURE only supports WcashRegtest")
            reason = self.command(f"fixture-tip-refuses-{network}", [fixture, "-c", path, "tip-height",
                                  "--network", network, "--cache-dir", state], success=False, timeout=30)
            if network == "WcashMainnet":
                require("LOCAL FUNDING TEST FIXTURE only supports WcashRegtest state" in reason,
                        "fixture Mainnet state inspection failed for an unrelated reason")
                rejection = "LOCAL FUNDING TEST FIXTURE only supports WcashRegtest state"
            else:
                # Network::FromStr has no public Wcash Testnet CLI variant, so
                # this route fails closed before opening any state directory.
                require("Invalid network: WcashTestnet" in reason,
                        "fixture Testnet state inspection failed for an unrelated reason")
                rejection = "CLI parser rejects WcashTestnet state selection"
            self.event("artifact-isolation-rejection", profile="fixture", action="tip-height", network=network,
                       reason=rejection)

        production = self.binary("production", "wcash-zebrad")
        attempts = {"start_height": "1", "end_height_exclusive": "9999999", "rate": "1",
                    "recipient": json.dumps(TREASURY)}
        for field, value in attempts.items():
            path, _ = config(f"production-override-{field}", "WcashMainnet", f"\n[foundation]\n{field} = {value}\n")
            reason = self.command(f"production-refuses-{field}-override", [production, "-c", path, "start"],
                                  success=False, timeout=30)
            require("unknown field" in reason.lower() and "foundation" in reason.lower(),
                    f"production {field} override failed for an unrelated reason")
            self.event("artifact-override-rejection", profile="production", source="TOML", field=field,
                       reason="unknown foundation configuration field")
        path, _ = config("production-env-override", "WcashMainnet")
        env = dict(self.env)
        env["WCASH_FOUNDATION__START_HEIGHT"] = "1"
        reason = self.command("production-refuses-env-override", [production, "-c", path, "start"],
                              env=env, success=False, timeout=30)
        require("unknown field" in reason.lower() and "foundation" in reason.lower(),
                "production environment override failed for an unrelated reason")
        self.event("artifact-override-rejection", profile="production", source="environment",
                   field="WCASH_FOUNDATION__START_HEIGHT", reason="unknown foundation configuration field")
        reason = self.command("production-refuses-cli-recipient", [production, "-c", path, "start",
                              "--foundation-address", TREASURY], success=False, timeout=30)
        require("unexpected argument" in reason.lower() and "--foundation-address" in reason,
                "production command-line override failed for an unrelated reason")
        self.event("artifact-override-rejection", profile="production", source="CLI", field="--foundation-address",
                   reason="unexpected argument")

    def run_covered_restart_regression(self, source, fork_height=110):
        """Run actual old append/new refusal on copied covered active or expired state."""
        require(fork_height in (10, 110), "covered restart fork must be 10 or 110")
        source = source.resolve()
        source_file = source if source.is_file() else source / "evidence.json"
        source_runtime = source_file.parent
        prior = json.loads(source_file.read_text())
        require(prior.get("status") in ("passed", "continuation-passed"),
                "covered restart requires passed110 evidence or an explicitly linked passed custody continuation")
        original_runtime = None
        original_evidence = None
        if prior.get("status") == "continuation-passed":
            original_runtime = Path(prior["continued_from"]).resolve()
            original_evidence = json.loads((original_runtime / "evidence.json").read_text())
            require(original_evidence.get("failure") == "reconsidering exact maturity predecessor failed",
                    "unsupported continuation source; preserve its full failure provenance")
        opened = {event["name"] for event in prior["events"] if event["event"] == "node-start"}
        closed = {event["name"] for event in prior["events"] if event["event"] == "node-stop"}
        require(opened <= closed, "stop every source-run node cleanly before copying covered state")
        final = next((event for event in reversed(prior["events"])
                      if event["event"] == "state-snapshot" and event.get("label") == "final110"), None)
        require(final is not None and final["height"] == 110, "covered source must end at fully validated110")
        self.manifest = json.loads((self.artifacts / "manifest.json").read_text())
        require(self.manifest["baseline_sha"] == BASE_SHA, "covered restart baseline revision mismatch")
        self.evidence["build_manifest"] = self.manifest
        self.evidence["scenario"] = "actual-covered-new-to-old-append-to-new"
        self.evidence["fork_height"] = fork_height
        self.evidence["source_evidence"] = str(source_file)
        self.evidence["source_evidence_sha256"] = hashlib.sha256(source_file.read_bytes()).hexdigest()
        self.evidence["source_status"] = prior["status"]
        if original_runtime is not None:
            self.evidence["original_run_evidence"] = str(original_runtime / "evidence.json")
            self.evidence["original_run_status"] = original_evidence["status"]
            self.evidence["original_run_failure"] = original_evidence["failure"]
        source_binaries = dict(original_evidence["binaries"]) if original_evidence is not None else {}
        source_binaries.update(prior["binaries"])
        for relative, digest in source_binaries.items():
            profile, name = relative.split("/", 1)
            self.binary(profile, name)
            require(self.evidence["binaries"][relative] == digest, "covered restart must use the exact source-run artifacts")
        source_state = Path(next(event["state"] for event in reversed(prior["events"])
                                 if event["event"] == "node-start" and event["profile"] == "fixture")).resolve()
        allowed_runtime = original_runtime or source_runtime
        require(source_state.is_dir() and source_state.is_relative_to(allowed_runtime),
                "covered source state must be the disposable directory recorded by the run/continuation")
        copied_state = self.runtime / "covered-state-copy"
        shutil.copytree(source_state, copied_state)
        self.event("covered-state-copy", original=str(source_state), copied=str(copied_state),
                   covered_height=110, covered_hash=final["tip"])
        self.miner = self.wallet("derive-covered-miner", "derive-address", seed=MINER_SEED)
        self.parent = self.node("covered-parent-template", "parent", parent=True)
        self.validator = self.node("covered-parent-validator", "parent", parent=True)
        self.ready(self.parent)
        self.ready(self.validator)
        expected = final
        if fork_height == 10:
            covered = self.node("covered-active-fork", "fixture", state=copied_state)
            self.ready(covered, 110)
            require(self.rpc(covered, "getbestblockhash") == final["tip"],
                    "fixture executable did not restore the source covered tip")
            first_orphan = self.rpc(covered, "getblockhash", [11])
            require(self.rpc(covered, "invalidateblock", [first_orphan]) is None,
                    "could not fork covered state at active height10")
            self.wait("covered active fork reaches10", lambda: self.rpc(covered, "getblockcount") == 10)
            expected = self.snapshot(covered, "covered-active-fork10", heights=(0, 8, 9, 10))
            require(len(expected["treasury_utxos"]) == 1 and
                    expected["treasury_utxos"][0]["satoshis"] == FUND,
                    "covered active fork must retain exactly the first foundation receipt")
            self.stop(covered)
        old = self.node("covered-downgrade-baseline", "baseline", state=copied_state)
        self.ready(old, fork_height)
        require(self.rpc(old, "getbestblockhash") == expected["tip"],
                "baseline executable did not restore the covered new-rule tip")
        before = self.snapshot(old, "covered-before-old-append", heights=(0, 10, 19, 20, 21, 109, 110))
        require(before["supply"] == expected["supply"] and before["pools"] == expected["pools"] and
                before["treasury_utxos"] == expected["treasury_utxos"],
                "baseline opening changed covered state before appending")
        appended_height = fork_height + 1
        appended = self.native(old, "baseline", private=False,
                               label=f"actual-baseline-appends-after-new-coverage{appended_height}")
        require(self.rpc(old, "getblockcount") == appended_height,
                f"baseline executable failed to append actual{appended_height}")
        raw = inspect_coinbase(appended["raw"])
        require(len(raw["outputs"]) == 1 and raw["outputs"][0]["valueZat"] == SUBSIDY and
                raw["ironwood_actions"] == 0,
                "baseline append must be an ordinary transparent full-miner reward")
        active = 10 <= appended_height < 20
        self.event("covered-old-append", baseline_sha=BASE_SHA, height=appended_height, hash=appended["hash"],
                   before_height=fork_height, before_hash=expected["tip"],
                   required_fund_valueZat=FUND if active else 0, actual_fund_valueZat=0,
                   violates_active_foundation_policy=active)
        self.stop(old)
        upgraded = self.node("covered-new-refuses-old-append", "fixture", state=copied_state)
        self.wait("new executable refuses uncovered old append", lambda: upgraded["process"].poll() is not None, timeout=60)
        upgraded["log"].flush()
        reason = (self.runtime / "private" / "covered-new-refuses-old-append.log").read_text()
        require(upgraded["process"].returncode != 0 and all(message in reason for message in (
            "cannot trust stored Wcash foundation history", "current rule validation coverage",
            "NEW, empty state cache_dir")), "covered downgrade failed for a reason other than uncovered validation history")
        self.event("covered-new-refused", original_preserved=str(source_state), copied_state=str(copied_state),
                   actual_old_append_height=appended_height, actual_old_append_hash=appended["hash"],
                   reason="current rule validation coverage missing or mismatched after actual baseline append; preserve original and rebuild NEW cache_dir")
        self.evidence["status"] = "passed"
        self.save()

    def run(self):
        manifest_path = self.artifacts / "manifest.json"
        require(manifest_path.is_file(), "build manifest missing; run scripts/build-wcash-foundation-fixtures.sh first")
        self.manifest = json.loads(manifest_path.read_text())
        require(self.manifest["baseline_sha"] == BASE_SHA, "baseline manifest must pin the reviewed pre-change main revision")
        require("wcash-funding-test-fixture" in self.manifest["features"]["fixture"], "manifest lacks the isolated fixture feature")
        require("wcash-funding-test-fixture" not in self.manifest["features"]["production"], "production manifest includes fixture feature")
        self.evidence["build_manifest"] = self.manifest
        self.save()
        self.miner_ivk = self.runtime / "private" / "miner.ivk"
        self.miner = self.wallet("derive-miner", "derive-collector", "--ivk-file", self.miner_ivk, seed=MINER_SEED)
        treasury = self.wallet("derive-treasury", "derive-address", seed=TREASURY_SEED)
        receiver = self.wallet("derive-receiver", "derive-address", seed=RECEIVER_SEED)
        require(treasury["transparent_coinbase_address"] == TREASURY, "disposable treasury key derivation mismatch")
        for profile, fixture in [("production", False), ("fixture", True)]:
            for binary in ["wcash-zebrad", "wcash-merge-miner"]:
                policy = self.command(f"{profile}-{binary}-policy", [self.binary(profile, binary), "funding-policy"])
                require(policy["fixture"] is fixture, "compiled feature profile mismatch")
                require((policy["start_height"], policy["end_height_exclusive"]) == ((10, 20) if fixture else (40001, 621455)),
                        "compiled policy heights mismatch")
                if fixture:
                    require(policy["recipient"] == TREASURY and policy["script_pub_key"] == TREASURY_SCRIPT,
                            "fixture treasury/script mismatch")
                self.event("artifact-policy", profile=profile, binary=binary, policy=policy)
        self.artifact_isolation_preflight()
        self.parent = self.node("parent-template", "parent", parent=True)
        self.validator = self.node("parent-validator", "parent", parent=True)
        self.ready(self.parent)
        self.ready(self.validator)
        old = self.node("baseline-child", "baseline")
        self.ready(old)
        for height in range(1, 9):
            # Inspect baseline outputs without imposing the future policy.
            result = self.native(old, "baseline", private=height == 1)
            require(height == self.rpc(old, "getblockcount"), "baseline height mismatch")
        pre = self.snapshot(old, "baseline-before-upgrade")
        state = old["state"]
        self.stop(old)
        compatibility_state = self.runtime / "old-compatibility-state"
        shutil.copytree(state, compatibility_state)
        new = self.node("upgraded-child", "fixture", state=state)
        self.ready(new, 8)
        require(self.snapshot(new, "restored-before-activation") == pre, "same-directory upgrade changed historical state")
        nine = self.native(new, "fixture", private=False)
        compatibility = self.node("compatibility-old", "baseline", state=compatibility_state)
        self.ready(compatibility, 8)
        require(self.rpc(compatibility, "submitblock", [nine["raw"]]) is None, "old node rejected valid unfunded block9")
        self.wait("old compatibility node reaches9", lambda: self.rpc(compatibility, "getblockcount") == 9)
        old_ten = self.candidate(compatibility, False, funded=False)
        transparent_ten = self.candidate(new, False)
        private_ten = self.candidate(new, True)
        matrix = {"old_no_fund_old": self.proposal(compatibility, old_ten),
                  "old_no_fund_new": self.proposal(new, old_ten),
                  "funded_transparent_old": self.proposal(compatibility, transparent_ten),
                  "funded_transparent_new": self.proposal(new, transparent_ten),
                  "funded_private_old": self.proposal(compatibility, private_ten),
                  "funded_private_new": self.proposal(new, private_ten)}
        self.event("compatibility-matrix", results=matrix)
        require(matrix["old_no_fund_old"] is None and matrix["funded_transparent_old"] is None,
                "old transparent compatibility matrix differs")
        require(matrix["funded_transparent_new"] is None and matrix["funded_private_new"] is None,
                "upgraded node rejected funded candidate")
        require(isinstance(matrix["old_no_fund_new"], str) and "foundation" in matrix["old_no_fund_new"].lower(),
                "missing fund was not rejected by the actual foundation semantic rule")
        require(isinstance(matrix["funded_private_old"], str) and
                "wcashmixedcoinbaseoutputs" in matrix["funded_private_old"].lower(),
                "old node did not reject the new private mixed shape")
        for node, candidate in [(compatibility, old_ten), (new, transparent_ten), (new, private_ten)]:
            self.retire(node, candidate)
        rejected = self.native(new, "baseline", command="native-job", success=False, label="old-coordinator-rejects-new-funded-template")
        require("coinbasevalue 500000000 is below the Wcash miner subsidy 625000000 at height 10" in rejected,
                "old coordinator failed for a reason other than its incompatible gross-subsidy expectation")
        self.event("old-coordinator-incompatibility", reason="net coinbasevalue500000000 is below old gross miner subsidy625000000 at height10")
        # Old software extends its private compatibility copy with a missing-fund block.
        # Its solved bytes are then presented to the upgraded state loader unchanged.
        self.native(compatibility, "baseline", private=False, label="legacy-missing-fund-block10")
        self.stop(compatibility)
        late = self.node("late-upgrade-refusal", "fixture", state=compatibility_state)
        self.wait("late upgrade refuses incompatible state", lambda: late["process"].poll() is not None, timeout=60)
        late["log"].flush()
        late_reason = (self.runtime / "private" / "late-upgrade-refusal.log").read_text()
        require(all(message in late_reason for message in (
            "cannot trust stored Wcash foundation history", "current rule validation coverage",
            "NEW, empty state cache_dir")), "late startup failed for a reason unrelated to foundation validation coverage")
        require(late["process"].returncode != 0, "incompatible legacy startup exited successfully")
        self.event("late-upgrade-refused", state=str(compatibility_state), exit_code=late["process"].returncode)
        self.wallet("init-miner", "init", "--birthday", "1", seed=MINER_SEED, db="miner", node=new)
        self.wallet("init-treasury", "init", "--birthday", "1", seed=TREASURY_SEED, db="treasury", node=new)
        transfer = None
        for height in range(10, 22):
            if height == 12:
                self.sync_wallet("miner", new)
                transfer = self.wallet("sign-funded-fee-transfer", "transfer", "--recipient", receiver["address"],
                    "--amount-zat", "100000000", "--confirmations", "1", "--unsafe-regtest-confirmations",
                    "--expiry-delta", "40", "--lock-for-blocks", "100", seed=MINER_SEED, db="miner", node=new)
                require(transfer["fee_zat"] > 0 and transfer["target_height"] == height, "funded transaction fee/height mismatch")
                self.command("broadcast-funded-fee-transfer", [self.binary("production", "wcash-wallet"), "--network", "regtest",
                    "--lightwalletd", new["grpc"], "broadcast"], stdin=transfer["raw_transaction_hex"] + "\n", timeout=180)
                self.wait("funded transfer in template", lambda: any(tx["hash"] == transfer["txid"]
                    for tx in self.rpc(new, "getblocktemplate")["transactions"]), timeout=900)
            mined = self.native(new, "fixture", private=height % 2 == 1)
            if height == 12:
                require(mined["fees"] == transfer["fee_zat"], "fees were not wholly attributed to the miner")
        treasury_balance = self.sync_wallet("treasury", new)
        require(treasury_balance["transparent_coinbase_total_zat"] == 10 * FUND, "fund issuance is not exactly12.5TWC")
        stable = self.snapshot(new, "after-expiry21")
        self.stop(new)
        new = self.node("restarted21", "fixture", state=state)
        self.ready(new, 21)
        require(self.snapshot(new, "restart21") == stable, "restart changed tip, supply, pools or treasury UTXOs")
        observer = self.node("fresh-observer", "fixture", peers=[new["peer"]])
        self.ready(observer, None)
        self.wait("fresh observer fully syncs", lambda: self.rpc(observer, "getblockcount") == 21, timeout=180)
        require(self.snapshot(observer, "fresh-sync21") == stable, "fresh-sync hash/supply/pool/UTXO mismatch")
        self.stop(observer)
        # Both nonfinalized boundaries are exercised in both directions. Templates
        # are inspected after each tip change and after each boundary restart.
        for fork_height, rollback_tip in [(9, 8), (10, 9), (19, 18), (20, 19)]:
            block_hash = self.rpc(new, "getblockhash", [fork_height])
            self.rpc(new, "invalidateblock", [block_hash])
            self.wait(f"rollback across{fork_height}", lambda: self.rpc(new, "getblockcount") == rollback_tip)
            candidate = self.candidate(new, private=True)
            self.retire(new, candidate)
            rolled = self.snapshot(new, f"rollback{fork_height}")
            self.check_treasury_reorg_balance(new, f"rollback{fork_height}", rolled)
            self.stop(new)
            new = self.node(f"boundary-restart{fork_height}", "fixture", state=state)
            self.ready(new, rollback_tip)
            require(self.snapshot(new, f"boundary-restart{fork_height}") == rolled, "boundary restart changed state")
            # Invalidated blocks are in-memory, so use exact retained solved bytes
            # to replay the suffix after restart rather than relying on a cache.
            for height in range(fork_height, 22):
                raw = self.retained_blocks[height]
                require(self.rpc(new, "submitblock", [raw]) is None, "reapplying exact boundary block failed")
            self.wait("boundary branch reapplied", lambda: self.rpc(new, "getblockcount") == 21)
            reapplied = self.snapshot(new, f"reapplied{fork_height}")
            require(reapplied == stable, "reapplying branch duplicated treasury outputs")
            self.check_treasury_reorg_balance(new, f"reapplied{fork_height}", reapplied)
        # A new, independently solved suffix replaces a funded block and its
        # treasury UTXO. Keep payout modes identical so aggregate pool values
        # remain directly comparable while the coinbase commitments differ.
        replaced_hash = self.rpc(new, "getblockhash", [19])
        replaced_utxos = self.rpc(new, "getaddressutxos", [{"addresses": [TREASURY]}])
        self.rpc(new, "invalidateblock", [replaced_hash])
        self.wait("competing branch starts at19", lambda: self.rpc(new, "getblockcount") == 18)
        for height in range(19, 22):
            self.native(new, "fixture", private=height % 2 == 1, label=f"replacement-branch-{height}")
        require(self.rpc(new, "getblockhash", [19]) != replaced_hash, "replacement branch reused the original funded block")
        replacement = self.snapshot(new, "competing-branch21")
        require(replacement["supply"] == stable["supply"] and replacement["pools"] == stable["pools"],
                "competing branch changed gross issuance or aggregate pools")
        require(sum(utxo["satoshis"] for utxo in replacement["treasury_utxos"]) == 10 * FUND,
                "competing branch lost or duplicated treasury receipts")
        require(replacement["treasury_utxos"] != replaced_utxos, "funded UTXO was not replaced during the reorg")
        self.check_treasury_reorg_balance(new, "competing-branch21", replacement)
        self.event("competing-branch-replacement", replaced_height=19, old_hash=replaced_hash,
                   new_hash=self.rpc(new, "getblockhash", [19]))

        for height in range(22, 109):
            self.native(new, "fixture", private=False)
        immature = self.sync_wallet("treasury", new)
        require(immature["transparent_coinbase_total_zat"] == 10 * FUND and
                immature["transparent_coinbase_spendable_zat"] == 0, "treasury fund total/maturity is wrong before target110")
        rejection = self.wallet("treasury-immature-shield-rejected", "shield-coinbase", "--max-inputs", "1",
            "--expiry-delta", "40", "--lock-for-blocks", "100", seed=TREASURY_SEED, db="treasury", node=new, success=False)
        require("mature" in rejection.lower() and "100" in rejection, "immature shielding failed for an unrelated reason")
        self.native(new, "fixture", private=False)
        mature = self.sync_wallet("treasury", new)
        require(mature["transparent_coinbase_spendable_zat"] == FUND, "height10 fund must mature at target110")
        shielding = self.wallet("sign-treasury-shielding", "shield-coinbase", "--max-inputs", "1", "--expiry-delta", "40",
            "--lock-for-blocks", "100", seed=TREASURY_SEED, db="treasury", node=new)
        require(shielding["target_height"] == 110 and 0 < shielding["fee_zat"] < FUND, "treasury shielding target/fee mismatch")
        require(shielding["internal_change_receiver_verified"] is True, "treasury shielding did not verify its private change receiver")
        # Prove the node's contextual rule with exactly these signed bytes.
        # Target height is not a signed transaction field; branch/expiry/locktime
        # remain unchanged when the same spend is checked one block earlier.
        # Restore the exact solved predecessor before broadcasting for110.
        predecessor_hash = self.rpc(new, "getblockhash", [109])
        predecessor_raw = self.retained_blocks[109]
        self.rpc(new, "invalidateblock", [predecessor_hash])
        self.wait("maturity rollback to108", lambda: self.rpc(new, "getblockcount") == 108)
        self.wait("maturity candidate109", lambda: self.rpc(new, "getblocktemplate")["height"] == 109, timeout=900)
        try:
            self.rpc(new, "sendrawtransaction", [shielding["raw_transaction_hex"]], timeout=180)
        except RpcFailure as error:
            reason = str(error)
            require("immature" in reason.lower() and "109" in reason and "110" in reason,
                    "signed immature spend failed for a reason other than its exact maturity boundary")
            self.event("signed-maturity-rejection", created_height=10, candidate_height=109,
                       first_valid_height=110, txid=shielding["txid"], reason=reason)
        else:
            raise RuntimeError("the node accepted the signed height10 treasury spend in candidate109")
        restored_hashes = self.rpc(new, "reconsiderblock", [predecessor_hash])
        require(predecessor_hash in displayed_hashes(restored_hashes), "reconsidering exact maturity predecessor failed")
        self.wait("maturity predecessor109 restored", lambda: self.rpc(new, "getblockcount") == 109)
        require(self.rpc(new, "getblock", [str(109), 0]) == predecessor_raw,
                "reconsidering maturity predecessor changed its exact solved bytes")

        self.command("broadcast-treasury-shielding", [self.binary("production", "wcash-wallet"), "--network", "regtest",
            "--lightwalletd", new["grpc"], "broadcast"], stdin=shielding["raw_transaction_hex"] + "\n")
        self.wait("treasury shielding in template", lambda: any(tx["hash"] == shielding["txid"]
            for tx in self.rpc(new, "getblocktemplate")["transactions"]), timeout=900)
        decoded = self.rpc(new, "getrawtransaction", [shielding["txid"], 1])
        require(decoded["vout"] == [] and len(decoded["vin"]) == 1 and
                decoded["ironwood"]["valueBalanceZat"] == -(FUND - shielding["fee_zat"]), "wrong shielding transaction shape")
        mined = self.native(new, "fixture", private=False)
        require(mined["fees"] == shielding["fee_zat"], "shielding fee not paid wholly to the miner")
        self.wallet("restore-treasury", "init", "--birthday", "1", seed=TREASURY_SEED, db="treasury-restored", node=new)
        restored = self.sync_wallet("treasury-restored", new)
        require(restored["transparent_coinbase_total_zat"] == 9 * FUND and
                restored["ironwood_total_zat"] == FUND - shielding["fee_zat"], "treasury restore lost receipts/shielded funds")
        self.event("treasury-custody", matured_input_height=10, rejected_target_height=109, accepted_target_height=110,
                   shield_txid=shielding["txid"], feeZat=shielding["fee_zat"], restored=restored)
        self.snapshot(new, "final110", heights=(0, 8, 9, 10, 19, 20, 21, 108, 109, 110))
        self.evidence["status"] = "passed"
        self.evidence["limitations"] = ["Direct transparent bypass and finalized/new-old-new metadata guards are separate semantic/unit gates; this runtime proves signed node-level maturity rejection at candidate109 and acceptance at110."]
        self.save()

    def close(self):
        for node in reversed(self.nodes):
            if not node["log"].closed:
                try:
                    self.stop(node)
                except Exception as error:
                    self.event("cleanup-error", name=node["name"], error=type(error).__name__)
                    self.evidence["status"] = "failed"
        # IVKs remain only in protected runtime state during execution. Preserve
        # wallet databases and signed operation records for accountable recovery.
        if hasattr(self, "miner_ivk"):
            self.miner_ivk.unlink(missing_ok=True)
        self.save()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifacts", type=Path, default=REPO / "target" / "foundation-artifacts")
    parser.add_argument("--runtime-parent", type=Path, default=Path(tempfile.gettempdir()))
    parser.add_argument("--overall-timeout", type=int, default=4 * 60 * 60)
    parser.add_argument("--covered-restart-source", type=Path,
                        help="run baseline append/new refusal on a COPY of stopped passed110 evidence (or linked custody continuation)")
    parser.add_argument("--covered-restart-fork-height", type=int, choices=(10, 110), default=110,
                        help="covered fork: 10 tests invalid unfunded active11; 110 tests unknown post-expiry111")
    args = parser.parse_args()
    if args.overall_timeout < 1:
        parser.error("--overall-timeout must be a positive number of seconds")
    os.umask(0o077)
    gate = Gate(args)
    try:
        if args.covered_restart_source:
            gate.run_covered_restart_regression(args.covered_restart_source, args.covered_restart_fork_height)
        else:
            gate.run()
    except BaseException as error:
        gate.evidence["status"] = "failed"
        gate.evidence["failure"] = str(error) if isinstance(error, RuntimeError) else type(error).__name__
        gate.save()
        print(f"Foundation E2E failed: {gate.evidence['failure']}. Evidence retained: {gate.runtime}", file=sys.stderr)
        return 1
    finally:
        gate.close()
    if gate.evidence["status"] != "passed":
        print(f"Foundation E2E cleanup failed; inspect {gate.runtime / 'evidence.json'}", file=sys.stderr)
        return 1
    print(f"Foundation E2E passed. Evidence retained: {gate.runtime / 'evidence.json'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
