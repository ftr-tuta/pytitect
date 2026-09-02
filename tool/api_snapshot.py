#!/usr/bin/env python3
"""Check the committed public API snapshot."""

from __future__ import annotations

import argparse
import importlib
import inspect
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SNAPSHOT = ROOT / "tool" / "public-api.txt"
MODULES = (
    "pytitect",
    "pytitect.canaries",
    "pytitect.checkpoints",
    "pytitect.contracts",
    "pytitect.core",
    "pytitect.django",
    "pytitect.drf",
    "pytitect.http",
    "pytitect.idempotency",
    "pytitect.inbox",
    "pytitect.leases",
    "pytitect.observability",
    "pytitect.outbox",
    "pytitect.receipts",
    "pytitect.security",
)


def public_api() -> list[str]:
    entries: set[str] = set()
    for module_name in MODULES:
        module = importlib.import_module(module_name)
        declared = getattr(module, "__all__", None)
        if declared is not None:
            names = declared
        else:
            names = [
                name
                for name, value in vars(module).items()
                if not name.startswith("_")
                and (inspect.isclass(value) or inspect.isfunction(value))
                and getattr(value, "__module__", None) == module_name
            ]
        entries.update(f"{module_name}:{name}" for name in names)
    return sorted(entries)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--print", action="store_true", dest="print_only")
    args = parser.parse_args()
    actual = public_api()
    if args.print_only:
        print("\n".join(actual))
        return 0
    expected = [
        line for line in SNAPSHOT.read_text().splitlines() if line and not line.startswith("#")
    ]
    if actual != expected:
        added = sorted(set(actual) - set(expected))
        removed = sorted(set(expected) - set(actual))
        if added:
            print("Public API additions:", *added, sep="\n  ")
        if removed:
            print("Public API removals:", *removed, sep="\n  ")
        return 1
    print(f"Public API snapshot matches ({len(actual)} symbols).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
