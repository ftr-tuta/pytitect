#!/usr/bin/env python3
"""Required representative live reliability and finite capacity gate."""

import argparse
import subprocess
import sys
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    subprocess.run([sys.executable, "-m", "pytest", "-m", "not aws_live"], check=True)
    subprocess.run([sys.executable, "tool/coverage_gate.py"], check=True)
    subprocess.run(
        [sys.executable, "-m", "benchmarks.python.run", "--output", str(args.output)], check=True
    )


if __name__ == "__main__":
    main()
