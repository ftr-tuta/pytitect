#!/usr/bin/env python3
"""Execute the authoritative corpus against raw Python boundaries; never pre-decode JSON."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

from pytitect.contracts import ContractManifest
from pytitect.core import Limits
from pytitect.messaging import ExactJsonMessageCodec, JsonMessageCodec
from pytitect.sync import ExactJsonSha256Integrity, decode_sync_raw, select_sync_integrity
from pytitect.wire import WireError, WireProfileError, decode_wire

ROOT = Path(__file__).resolve().parents[1]
CORPUS = ROOT / "interop/conformance"
LEGACY_SHA256 = "4b8e07fd58687ef10b56bd34da890442aa925c67d142382f8c48feac04ba37dd"


def execute(vector: dict[str, Any]) -> dict[str, Any]:
    try:
        wire = (vector["wire"] + " " * vector.get("appendSpaces", 0)).encode("utf-8")
        limits = Limits(**vector.get("limits", {}))
        profile = vector["profile"]
        if profile == "titect-sync/1":
            acknowledgement = vector.get("acknowledgement")
            selected = select_sync_integrity(
                tuple(vector.get("requested", [])),
                acknowledgement,
                policies=[ExactJsonSha256Integrity()],
            )
            encoded = decode_sync_raw(
                wire,
                wire_limits=limits if "limits" in vector else None,
                integrity=selected,
                acknowledgement=acknowledgement,
            ).encode()
        elif profile == "titect-message/1":
            codec = JsonMessageCodec(limits=limits)
            encoded = codec.encode(codec.decode_raw(wire))
        elif profile == "titect-message/2":
            exact_codec = ExactJsonMessageCodec(limits=limits)
            encoded = exact_codec.encode(exact_codec.decode(wire))
        elif profile == "exact-json":
            encoded = decode_wire(wire, limits=limits).encode()
        else:
            raise WireProfileError()
        return {"name": vector["name"], "accepted": True, "roundTrip": encoded.decode("utf-8")}
    except WireError as error:
        return {"name": vector["name"], "accepted": False, "problem": error.code}


def corpus_manifest(root: Path = CORPUS) -> dict[str, Any]:
    return ContractManifest.from_paths(
        root, [path for path in root.glob("*.json") if path.name != "manifest.json"]
    ).to_dict()


def load_corpus(root: Path = CORPUS) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if corpus_manifest(root) != json.loads((root / "manifest.json").read_text()):
        raise ValueError("authoritative corpus hash or inventory drift")
    if hashlib.sha256((root / "legacy-vectors.json").read_bytes()).hexdigest() != LEGACY_SHA256:
        raise ValueError("historical corpus bytes changed")
    original = json.loads((root / "legacy-vectors.json").read_text())
    vectors = json.loads((root / "vectors.json").read_text())
    expectations = json.loads((root / "expectations.json").read_text())
    if len(original) != 156 or vectors[:156] != original:
        raise ValueError("legacy corpus must retain all 156 cases unchanged")
    names = [item["name"] for item in vectors]
    if len(set(names)) != len(names) or names != [item["name"] for item in expectations]:
        raise ValueError("duplicate, reordered, substituted or missing expectations")
    return vectors, expectations


def compare(expected: list[dict[str, Any]], actual: list[dict[str, Any]]) -> list[str]:
    if len(actual) != len(expected):
        return ["case_count"]
    return [left["name"] for left, right in zip(expected, actual, strict=True) if left != right]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--corpus", type=Path, default=CORPUS)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    vectors, expected = load_corpus(args.corpus)
    outcomes = [execute(vector) for vector in vectors]
    divergences = compare(expected, outcomes)
    report = {
        "schemaVersion": 1,
        "status": "failed" if divergences else "passed",
        "corpusSha256": hashlib.sha256((args.corpus / "vectors.json").read_bytes()).hexdigest(),
        "corpusDigest": corpus_manifest(args.corpus)["digest"],
        "caseCount": len(vectors),
        "outcomes": outcomes,
        "divergences": divergences,
    }
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")
    print(f"Python raw conformance: {len(vectors)} cases; {len(divergences)} divergences.")
    if divergences:
        print("\n".join(divergences))
    return int(bool(divergences))


if __name__ == "__main__":
    raise SystemExit(main())
