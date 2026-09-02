#!/usr/bin/env python3
"""Verify that the root import does not load frameworks or security adapters."""

from __future__ import annotations

import json
import subprocess
import sys

OPTIONAL_PREFIXES = (
    "django",
    "rest_framework",
    "drf_spectacular",
    "yaml",
    "rfc8785",
    "jwt",
    "cryptography",
    "http_message_signatures",
)


def main() -> int:
    code = """
import json
import sys
import pytitect
print(json.dumps(sorted(sys.modules)))
"""
    result = subprocess.run(
        [sys.executable, "-I", "-c", code], check=False, capture_output=True, text=True
    )
    if result.returncode:
        print(result.stderr, file=sys.stderr)
        return result.returncode
    loaded = json.loads(result.stdout)
    forbidden = [
        name
        for name in loaded
        if any(name == prefix or name.startswith(prefix + ".") for prefix in OPTIONAL_PREFIXES)
    ]
    if forbidden:
        print("Optional modules loaded by import pytitect:", *forbidden, sep="\n  ")
        return 1
    print("Root import is free of optional modules.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
