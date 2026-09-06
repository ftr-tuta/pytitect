#!/usr/bin/env python3
"""Validate the neutral titect-sync/1 bundle and its deterministic manifest."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from pytitect.contracts import ContractManifest
from pytitect.observability import pseudonymous_attribute
from pytitect.sync import decode_sync_raw
from pytitect.trace import parse_trace_context

ROOT = Path(__file__).resolve().parents[1]
BUNDLE = ROOT / "interop" / "titect-sync" / "1"
MANIFEST = BUNDLE / "manifest.json"


def bundle_manifest() -> ContractManifest:
    paths = [path for path in BUNDLE.rglob("*.json") if path != MANIFEST]
    return ContractManifest.from_paths(BUNDLE, paths)


def validate_fixtures() -> None:
    for path in sorted((BUNDLE / "fixtures" / "positive").glob("*.json")):
        if path.name in {"trace.json", "pseudonym.json"}:
            continue
        decode_sync_raw(path.read_bytes())

    negative_documents = json.loads(
        (BUNDLE / "fixtures" / "negative" / "documents.json").read_text()
    )
    for case in negative_documents["cases"]:
        try:
            decode_sync_raw(json.dumps(case["document"]).encode())
        except ValueError:
            continue
        raise ValueError(f"negative document fixture was accepted: {case['name']}")

    positive_trace = json.loads((BUNDLE / "fixtures" / "positive" / "trace.json").read_text())
    trace = parse_trace_context(positive_trace["traceparent"], positive_trace["tracestate"])
    if trace.sampled != positive_trace["sampled"]:
        raise ValueError("positive trace sampled fixture does not match")
    if trace.random_trace_id != positive_trace["random_trace_id"]:
        raise ValueError("positive trace random flag fixture does not match")

    negative_traces = json.loads((BUNDLE / "fixtures" / "negative" / "trace.json").read_text())
    for case in negative_traces["cases"]:
        try:
            parse_trace_context(case["traceparent"], case.get("tracestate"))
        except ValueError:
            continue
        raise ValueError(f"negative trace fixture was accepted: {case['name']}")

    pseudonym = json.loads((BUNDLE / "fixtures" / "positive" / "pseudonym.json").read_text())
    actual = pseudonymous_attribute(
        pseudonym["input"], key=pseudonym["synthetic_key_utf8"].encode()
    )
    if actual != pseudonym["hex"]:
        raise ValueError("pseudonymous hashing fixture does not match")


def validate_artifacts() -> None:
    schema = json.loads((BUNDLE / "schema.json").read_text())
    if schema.get("$schema") != "https://json-schema.org/draft/2020-12/schema":
        raise ValueError("sync schema must use JSON Schema 2020-12")
    openapi = json.loads((BUNDLE / "openapi-components.json").read_text())
    if openapi.get("openapi") != "3.1.0" or openapi.get("paths") != {}:
        raise ValueError("OpenAPI artifact must be 3.1.0 components without routes")
    if not openapi.get("components", {}).get("schemas"):
        raise ValueError("OpenAPI artifact must expose schema components")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--print", action="store_true", dest="print_only")
    args = parser.parse_args()
    validate_artifacts()
    validate_fixtures()
    actual = bundle_manifest().to_dict()
    if args.print_only:
        print(json.dumps(actual, indent=2, sort_keys=True))
        return 0
    expected = json.loads(MANIFEST.read_text())
    if actual != expected:
        print(json.dumps(actual, indent=2, sort_keys=True))
        return 1
    print(f"titect-sync/1 manifest matches ({len(actual['files'])} files).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
