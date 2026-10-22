#!/usr/bin/env python3
"""Read-only ZEC winner export and off-pool independent canonical observations.

Export uses existing PG* environment credentials, SELECTs only, and a bounded
repeatable-read transaction. Observe runs on the operator workstation, never in
the miner or submission worker. It does not submit blocks or change accounting.
"""
import argparse
import base64
import datetime
import json
import os
import pathlib
import re
import subprocess
import uuid

GENESIS = "00040fe8ec8471911baa1db1266ea15dd06b4a8a5c453883c000b031973dce08"
# Frozen header in zebra-test/src/vectors/block-main-0-419-200.txt. Public
# lightwallet servers commonly do not serve pre-Sapling blocks or tree states.
ANCHOR_HEIGHT = 419200
ANCHOR_HASH = "00000000025a57200d898ac7f21e26bf29028bbe96ec46e05b2c17cc9db9e4f3"
MAX_BYTES = 1_048_576
MAX_CANDIDATES = 1000
HASH = re.compile(r"[0-9a-f]{64}\Z")
STATES = {"submitted", "side_chain", "observed", "matured", "orphaned", "quarantined", "requeued"}


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def require(condition, message):
    if not condition:
        raise ValueError(message)


def read_json(path):
    with pathlib.Path(path).open("rb") as stream:
        raw = stream.read(MAX_BYTES + 1)
    require(len(raw) <= MAX_BYTES, "snapshot exceeds byte cap")
    return json.loads(raw)


def validate_snapshot(snapshot):
    require(snapshot.get("schema") == 1 and snapshot.get("chain") == "zcash", "not a ZEC snapshot")
    require(snapshot.get("network") == "mainnet" and snapshot.get("genesis") == GENESIS,
            "wrong network identity")
    uuid.UUID(snapshot["deployment"])
    uuid.UUID(snapshot["journal_stream"])
    require(type(snapshot["watermark"]) is int and snapshot["watermark"] >= 0, "invalid watermark")
    rows = snapshot["candidates"]
    require(isinstance(rows, list) and len(rows) <= MAX_CANDIDATES, "candidate cap exceeded")
    require(type(snapshot["distinct_candidates"]) is int and snapshot["distinct_candidates"] == len(rows),
            "incomplete candidate coverage")
    require(type(snapshot["qualified_proofs"]) is int and snapshot["qualified_proofs"] >= len(rows),
            "invalid proof count")
    seen = set()
    for row in rows:
        require(HASH.fullmatch(row["hash"]) is not None, "invalid public hash")
        require(row["hash"] not in seen, "duplicate candidate")
        seen.add(row["hash"])
        require(type(row["height"]) is int and row["height"] > 0, "invalid height")
        require(row["local_state"] in STATES, "invalid local state")
        require(type(row["found_event_seq"]) is int and
                0 < row["found_event_seq"] <= snapshot["watermark"], "invalid source event")
    return snapshot


def export_snapshot(deployment):
    deployment = str(uuid.UUID(deployment))
    # The UUID is parsed before interpolation. No arbitrary SQL, URL or shell.
    query = f"""
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout = '3s';
SET LOCAL lock_timeout = '1s';
SELECT json_build_object(
 'schema',1,'chain','zcash','network',d.network,
 'genesis',encode(reverse_bytes.zcash_genesis,'hex'),
 'deployment',d.id,'journal_stream',d.journal_stream,
 'watermark',c.last_event_seq,'projection_updated_at',c.updated_at,
 'exported_at',clock_timestamp(),
 'distinct_candidates',(SELECT count(*) FROM winners WHERE deployment_id=d.id AND chain='zcash'),
 'qualified_proofs',(SELECT count(*) FROM winner_proofs WHERE deployment_id=d.id AND chain='zcash'),
 'candidates',COALESCE((SELECT json_agg(r ORDER BY r.hash) FROM (
   SELECT encode((SELECT string_agg(substring(w.block_hash_le FROM n FOR 1),''::bytea ORDER BY n DESC)
                 FROM generate_series(1,32) n),'hex') AS hash,
          w.height,w.state AS local_state,s.event_seq AS found_event_seq,
          e.recorded_at AS projected_at
   FROM winners w JOIN shares s ON s.deployment_id=w.deployment_id AND s.share_id=w.share_id
   JOIN backend_events e ON e.deployment_id=s.deployment_id AND e.event_seq=s.event_seq
   WHERE w.deployment_id=d.id AND w.chain='zcash'
   ORDER BY w.block_hash_le LIMIT {MAX_CANDIDATES + 1}
 ) r),'[]'::json))
FROM deployments d JOIN backend_cursors c ON c.deployment_id=d.id
CROSS JOIN LATERAL (SELECT string_agg(substring(d.zcash_genesis FROM n FOR 1),''::bytea ORDER BY n DESC)
                   AS zcash_genesis FROM generate_series(1,32) n) reverse_bytes
WHERE d.id='{deployment}'::uuid;
COMMIT;
"""
    env = os.environ.copy()
    env["PGCONNECT_TIMEOUT"] = "3"
    result = subprocess.run(["psql", "-XqAt", "-v", "ON_ERROR_STOP=1", "-c", query],
                            env=env, capture_output=True, timeout=12, check=False)
    require(result.returncode == 0, "read-only export failed; database error text suppressed")
    require(len(result.stdout) <= MAX_BYTES, "export exceeds byte cap")
    snapshot = validate_snapshot(json.loads(result.stdout))
    require(snapshot["distinct_candidates"] == len(snapshot["candidates"]),
            "incomplete export: candidate cap or projection mismatch")
    return snapshot


def grpc(endpoint, protoset, method, request):
    # grpcurl validates TLS. Public observations never use plaintext or write RPCs.
    require(re.fullmatch(r"[a-zA-Z0-9.-]+:[0-9]{1,5}", endpoint) is not None,
            "observer must be a credential-free TLS host:port")
    require(method in {"GetLightdInfo", "GetLatestBlock", "GetTreeState"}, "read-only method required")
    command = ["grpcurl", "-max-time", "4", "-max-msg-sz", str(MAX_BYTES),
               "-protoset", str(protoset), "-d", json.dumps(request), endpoint,
               "cash.z.wallet.sdk.rpc.CompactTxStreamer/" + method]
    result = subprocess.run(command, capture_output=True, timeout=6, check=False)
    require(result.returncode == 0 and len(result.stdout) <= MAX_BYTES,
            "independent observer unavailable")
    return json.loads(result.stdout)


def tip(value):
    height = int(value["height"])
    raw = base64.b64decode(value["hash"], validate=True)
    require(height >= 0 and len(raw) == 32, "invalid observer tip")
    return {"height": height, "hash": raw[::-1].hex()}


def classify(candidate, before, canonical, after):
    if before != after:
        return {"state": "unknown", "reason": "observer_tip_changed"}
    if before["height"] < candidate["height"]:
        return {"state": "unknown", "reason": "observer_behind"}
    require(int(canonical["height"]) == candidate["height"] and
            canonical.get("network") == "main" and
            HASH.fullmatch(canonical["hash"]) is not None, "invalid fixed-height response")
    matches = canonical["hash"] == candidate["hash"]
    return {"state": "canonical" if matches else "noncanonical_at_sample",
            "canonical_hash": canonical["hash"],
            "confirmations": before["height"] - candidate["height"] + 1 if matches else 0}


def observe(snapshot, endpoint, protoset, limit, offset, call=grpc):
    validate_snapshot(snapshot)
    result = {"schema": 1, "chain": "zcash", "checked_at": now(),
              "deployment": snapshot["deployment"], "journal_stream": snapshot["journal_stream"],
              "watermark": snapshot["watermark"], "observer": endpoint,
              "distinct_candidates": snapshot["distinct_candidates"],
              "qualified_proofs": snapshot["qualified_proofs"],
              "scope": "Independent TLS provider canonical view; not a P2P delivery ACK or local consensus proof",
              "find_time_provenance": "projected_at is projection time; historical discovery UTC is unknown",
              "candidates": [], "next_offset": None}
    rows = snapshot["candidates"][offset:offset + limit]
    try:
        identity = call(endpoint, protoset, "GetLightdInfo", {})
        anchor = call(endpoint, protoset, "GetTreeState", {"height": ANCHOR_HEIGHT})
        require(identity.get("chainName") == "main" and
                int(anchor["height"]) == ANCHOR_HEIGHT and anchor.get("network") == "main" and
                anchor.get("hash") == ANCHOR_HASH,
                "independent observer network mismatch")
        result["network_anchor"] = {"height": ANCHOR_HEIGHT, "hash": ANCHOR_HASH}
        for candidate in rows:
            row = dict(candidate, checked_at=now())
            try:
                before = tip(call(endpoint, protoset, "GetLatestBlock", {}))
                canonical = call(endpoint, protoset, "GetTreeState", {"height": candidate["height"]})
                after = tip(call(endpoint, protoset, "GetLatestBlock", {}))
                row.update(classify(candidate, before, canonical, after))
                row["sampled_tip"] = after
            except (ValueError, KeyError, TypeError, OSError, subprocess.SubprocessError):
                row.update(state="unknown", reason="observation_failed")
            result["candidates"].append(row)
    except (ValueError, KeyError, TypeError, OSError, subprocess.SubprocessError):
        result["candidates"] = [dict(row, state="unknown", reason="observer_identity_unavailable") for row in rows]
    if offset + len(rows) < len(snapshot["candidates"]):
        result["next_offset"] = offset + len(rows)
    result["coverage_complete"] = offset == 0 and result["next_offset"] is None
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    export = sub.add_parser("export")
    export.add_argument("--deployment", required=True)
    observation = sub.add_parser("observe")
    observation.add_argument("--snapshot", type=pathlib.Path, required=True)
    observation.add_argument("--observer", required=True)
    observation.add_argument("--protoset", type=pathlib.Path, required=True)
    observation.add_argument("--limit", type=int, default=1, choices=range(1, 6))
    observation.add_argument("--offset", type=int, default=0)
    args = parser.parse_args()
    try:
        if args.command == "export":
            result = export_snapshot(args.deployment)
        else:
            require(args.offset >= 0, "negative offset")
            result = observe(read_json(args.snapshot), args.observer, args.protoset, args.limit, args.offset)
        print(json.dumps(result, sort_keys=True))
    except (ValueError, KeyError, TypeError, OSError, subprocess.SubprocessError):
        print(json.dumps({"error": "bounded_zec_tracking_failed", "chain": "zcash"}))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
