#!/usr/bin/env python3
"""Run the local quality gate without modifying repository files."""

from __future__ import annotations

import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FORBIDDEN_PACKAGE_PATHS = {"migrations", "urls.py", "middleware.py", "signals.py", "workers.py"}


def run(*command: str) -> None:
    print("+", *command, flush=True)
    subprocess.run(command, cwd=ROOT, check=True)


def main() -> int:
    python = sys.executable
    run(python, "-m", "ruff", "format", "--check", ".")
    run(python, "-m", "ruff", "check", ".")
    run(python, "-m", "mypy")
    run(python, "-m", "pytest")
    run(python, "tool/api_snapshot.py")
    run(python, "tool/optional_imports.py")
    forbidden = [
        path
        for path in (ROOT / "src" / "pytitect").rglob("*")
        if path.name in FORBIDDEN_PACKAGE_PATHS
    ]
    if forbidden:
        raise SystemExit(f"Forbidden package-owned integration files: {forbidden}")
    with tempfile.TemporaryDirectory(prefix="pytitect-dist-") as directory:
        run(python, "-m", "build", "--no-isolation", "--outdir", directory)
        artifacts = sorted(str(path) for path in Path(directory).iterdir())
        run(python, "-m", "twine", "check", *artifacts)
    print("Pytitect verification completed successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
