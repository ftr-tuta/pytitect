#!/usr/bin/env python3
"""Run read-only compatibility commands in caller-supplied consumer paths."""

from __future__ import annotations

import argparse
import hashlib
import json
import shlex
import subprocess
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("consumer", nargs="+", type=Path)
    parser.add_argument("--wheel", required=True, type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--command",
        default="python -I -c 'import pytitect; print(pytitect.__version__)'",
        help="Read-only command run in each path",
    )
    args = parser.parse_args()
    wheel = args.wheel.resolve(strict=True)
    if not wheel.is_file() or wheel.suffix != ".whl":
        raise SystemExit(f"candidate wheel is invalid: {wheel}")
    command = shlex.split(args.command)
    results: list[dict[str, object]] = []
    exit_code = 0
    for consumer in args.consumer:
        path = consumer.resolve(strict=True)
        if not path.is_dir():
            raise SystemExit(f"consumer is not a directory: {path}")
        completed = subprocess.run(
            command,
            cwd=path,
            check=False,
            capture_output=True,
            text=True,
        )
        results.append(
            {
                "consumer": str(path),
                "returncode": completed.returncode,
                "stdout": completed.stdout,
                "stderr": completed.stderr,
            }
        )
        if completed.returncode and not exit_code:
            exit_code = completed.returncode
    document = {
        "schema_version": 1,
        "wheel": str(wheel),
        "wheel_sha256": hashlib.sha256(wheel.read_bytes()).hexdigest(),
        "command": command,
        "results": results,
    }
    rendered = json.dumps(document, indent=2, sort_keys=True) + "\n"
    if args.output is None:
        print(rendered, end="")
    else:
        args.output.write_text(rendered)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
