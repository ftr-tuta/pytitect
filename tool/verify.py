#!/usr/bin/env python3
"""Run the local quality gate without modifying repository files."""

from __future__ import annotations

import argparse
import ast
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FORBIDDEN_PACKAGE_PATHS = {"migrations", "urls.py", "middleware.py", "signals.py", "workers.py"}


def run(*command: str) -> None:
    print("+", *command, flush=True)
    subprocess.run(command, cwd=ROOT, check=True)


def verify_models_are_abstract() -> None:
    package = ROOT / "src" / "pytitect"
    violations: list[str] = []
    for path in package.rglob("*.py"):
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in (item for item in ast.walk(tree) if isinstance(item, ast.ClassDef)):
            model_base = any(
                isinstance(base, ast.Attribute) and base.attr == "Model" for base in node.bases
            )
            if not model_base:
                continue
            meta = next(
                (
                    child
                    for child in node.body
                    if isinstance(child, ast.ClassDef) and child.name == "Meta"
                ),
                None,
            )
            abstract = bool(
                meta
                and any(
                    isinstance(statement, ast.Assign)
                    and any(
                        isinstance(target, ast.Name) and target.id == "abstract"
                        for target in statement.targets
                    )
                    and isinstance(statement.value, ast.Constant)
                    and statement.value.value is True
                    for statement in meta.body
                )
            )
            if not abstract:
                violations.append(f"{path.relative_to(ROOT)}:{node.lineno}:{node.name}")
    if violations:
        raise SystemExit(f"Concrete package-owned Django models are forbidden: {violations}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--skip-build",
        action="store_true",
        help="Run checks without the final isolated package build",
    )
    args = parser.parse_args()
    python = sys.executable
    run(python, "-m", "ruff", "format", "--check", ".")
    run(python, "-m", "ruff", "check", ".")
    run(python, "-m", "mypy")
    run(python, "-m", "pytest")
    run(python, "tool/api_snapshot.py")
    run(python, "tool/sync_bundle.py")
    run(python, "tool/optional_imports.py")
    forbidden = [
        path
        for path in (ROOT / "src" / "pytitect").rglob("*")
        if path.name in FORBIDDEN_PACKAGE_PATHS
    ]
    if forbidden:
        raise SystemExit(f"Forbidden package-owned integration files: {forbidden}")
    verify_models_are_abstract()
    if not args.skip_build:
        with tempfile.TemporaryDirectory(prefix="pytitect-dist-") as directory:
            run(python, "-m", "build", "--no-isolation", "--outdir", directory)
            artifacts = sorted(str(path) for path in Path(directory).iterdir())
            run(python, "-m", "twine", "check", *artifacts)
    print("Pytitect verification completed successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
