"""Construct unsolved NU7 coinbase codec fixtures and independent ZIP-244 hashes.

Uses only Python's standard library. These transparent OP_TRUE/zero-value
transactions are parser/commitment vectors, not parent-consensus-valid rewards
or solved mining proofs. No branch rewriting or wallet data is used.
"""

import hashlib
import json
import pathlib
import struct

ROOT = pathlib.Path(__file__).resolve().parent
BRANCH = 0x77190AD9
HEIGHT = 3_543_405
CARRIER = bytes.fromhex(
    "fabe6d6d828c9a504f6f9792169ae8c5751624ce48aa43476dd83ca2e481a01cdd5980b3"
    "0100000007000000"
)


def digest(person, data=b""):
    return hashlib.blake2b(data, digest_size=32, person=person).digest()


def fixture(version):
    group = {5: 0x26A7270A, 6: 0xD884B698}[version]
    header = struct.pack("<IIIII", 0x80000000 | version, group, BRANCH, 0, HEIGHT)
    height = HEIGHT.to_bytes(4, "little").rstrip(b"\0")
    script = bytes([len(height)]) + height + b"nu7-fixture" + CARRIER
    script_field = bytes([len(script)]) + script
    prevout = bytes(32) + struct.pack("<I", 0xFFFFFFFF)
    sequence = struct.pack("<I", 0xFFFFFFFF)
    output = struct.pack("<Q", 0) + bytes([1, 0x51])
    raw = header + bytes([1]) + prevout + script_field + sequence
    raw += bytes([1]) + output + bytes(3 + (version == 6))
    transparent = digest(
        b"ZTxIdTranspaHash",
        digest(b"ZTxIdPrevoutHash", prevout)
        + digest(b"ZTxIdSequencHash", sequence)
        + digest(b"ZTxIdOutputsHash", output),
    )
    tx_components = digest(b"ZTxIdHeadersHash", header) + transparent
    tx_components += digest(b"ZTxIdSaplingHash")
    tx_components += digest(b"ZTxIdOrchardHash" if version == 5 else b"ZTxIdOrchardH_v6")
    if version == 6:
        tx_components += digest(b"ZTxIdIronwd_H_v6")
    txid = digest(b"ZcashTxHash_" + struct.pack("<I", BRANCH), tx_components)
    auth_components = digest(b"ZTxAuthTransHash", script_field)
    auth_components += digest(b"ZTxAuthSapliHash" if version == 5 else b"ZTxAuthSapliH_v6")
    auth_components += digest(b"ZTxAuthOrchaHash" if version == 5 else b"ZTxAuthOrchaH_v6")
    if version == 6:
        auth_components += digest(b"ZTxAuthIrnwdH_v6")
    auth = digest(b"ZTxAuthHash_" + struct.pack("<I", BRANCH), auth_components)
    return raw, {
        "version": version,
        "branch_id": f"{BRANCH:08x}",
        "height": HEIGHT,
        "transaction_id_raw": txid.hex(),
        "authorizing_data_digest_raw": auth.hex(),
        "miner_data": (b"nu7-fixture" + CARRIER).hex(),
        "classification": "constructed coinbase codec fixture; unsolved; not a valid parent reward",
    }


if __name__ == "__main__":
    metadata = []
    for version in [5, 6]:
        raw, info = fixture(version)
        (ROOT / f"nu7-v{version}-coinbase.hex").write_text(raw.hex() + "\n")
        metadata.append(info)
    (ROOT / "nu7-coinbase.json").write_text(json.dumps(metadata, indent=2) + "\n")
