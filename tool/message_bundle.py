#!/usr/bin/env python3
"""Validate the neutral titect-message/1 bundle and deterministic manifest."""

from __future__ import annotations

import json
from pathlib import Path

from pytitect.contracts import ContractManifest
from pytitect.messaging import JsonMessageCodec

ROOT = Path(__file__).resolve().parents[1]
BUNDLE = ROOT / "interop" / "titect-message" / "1"
MANIFEST = BUNDLE / "manifest.json"


def main() -> int:
    positive = (BUNDLE / "fixtures" / "positive" / "message.json").read_bytes().rstrip(b"\n")
    codec = JsonMessageCodec()
    if codec.encode(codec.decode(positive)) != positive:
        raise SystemExit("positive message fixture is not canonical")
    negative = (BUNDLE / "fixtures" / "negative" / "unknown-field.json").read_bytes()
    try:
        codec.decode(negative)
    except ValueError:
        pass
    else:
        raise SystemExit("negative message fixture was accepted")
    paths = [path for path in BUNDLE.rglob("*.json") if path != MANIFEST]
    actual = ContractManifest.from_paths(BUNDLE, paths).to_dict()
    expected = json.loads(MANIFEST.read_text(encoding="utf-8"))
    if actual != expected:
        print(json.dumps(actual, indent=2, sort_keys=True))
        return 1
    print(f"titect-message/1 manifest matches ({len(paths)} files).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
