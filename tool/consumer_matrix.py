#!/usr/bin/env python3
"""Run a read-only compatibility command in consumer paths supplied by the caller."""

from __future__ import annotations

import argparse
import shlex
import subprocess
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("consumer", nargs="+", type=Path)
    parser.add_argument(
        "--command",
        default="python -I -c 'import pytitect; print(pytitect.__version__)'",
        help="Command run in each path (default: isolated import smoke test)",
    )
    args = parser.parse_args()
    command = shlex.split(args.command)
    for consumer in args.consumer:
        path = consumer.resolve(strict=True)
        if not path.is_dir():
            raise SystemExit(f"consumer is not a directory: {path}")
        print(f"==> {path}")
        completed = subprocess.run(command, cwd=path, check=False)
        if completed.returncode:
            return completed.returncode
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
