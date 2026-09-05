#!/usr/bin/env python3
"""Validate the neutral message bundles and deterministic manifests."""

from __future__ import annotations

import json
from pathlib import Path

from pytitect.contracts import ContractManifest
from pytitect.messaging import ExactJsonMessageCodec, JsonMessageCodec

ROOT = Path(__file__).resolve().parents[1]
BUNDLE = ROOT / "interop" / "titect-message" / "1"


def validate_bundle(bundle: Path, codec: JsonMessageCodec | ExactJsonMessageCodec) -> int:
    asyncapi = json.loads((bundle / "asyncapi.json").read_text(encoding="utf-8"))
    if asyncapi.get("asyncapi") != "3.1.0":
        raise SystemExit("message AsyncAPI document must use version 3.1.0")
    if asyncapi.get("channels") != {} or asyncapi.get("operations") != {}:
        raise SystemExit("message AsyncAPI document must remain route-neutral")
    positive = (bundle / "fixtures" / "positive" / "message.json").read_bytes().rstrip(b"\n")
    if codec.encode(codec.decode(positive)) != positive:
        raise SystemExit("positive message fixture is not canonical")
    negative = (bundle / "fixtures" / "negative" / "unknown-field.json").read_bytes()
    try:
        codec.decode(negative)
    except ValueError:
        pass
    else:
        raise SystemExit("negative message fixture was accepted")
    paths = [path for path in bundle.rglob("*.json") if path != bundle / "manifest.json"]
    actual = ContractManifest.from_paths(bundle, paths).to_dict()
    expected = json.loads((bundle / "manifest.json").read_text(encoding="utf-8"))
    if actual != expected:
        print(json.dumps(actual, indent=2, sort_keys=True))
        return 1
    print(f"titect-message/{bundle.name} manifest matches ({len(paths)} files).")
    return 0


def main() -> int:
    return max(
        validate_bundle(ROOT / "interop/titect-message/1", JsonMessageCodec()),
        validate_bundle(ROOT / "interop/titect-message/2", ExactJsonMessageCodec()),
    )


if __name__ == "__main__":
    raise SystemExit(main())
