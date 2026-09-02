#!/usr/bin/env python3
"""Compare a committed contract manifest with a local tree."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from pytitect.contracts import ContractManifest


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument("manifest", type=Path)
    args = parser.parse_args()
    expected = json.loads(args.manifest.read_text())
    actual = ContractManifest.from_tree(args.root).to_dict()
    if actual != expected:
        print(json.dumps(actual, indent=2, sort_keys=True))
        return 1
    print(f"Contract manifest matches {args.manifest}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
