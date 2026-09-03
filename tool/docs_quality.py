#!/usr/bin/env python3
"""Check local documentation, generated compatibility data, and API classifications."""

from __future__ import annotations

import importlib
import re
import subprocess
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
COMPATIBILITY = ROOT / "docs" / "compatibility.md"
SNAPSHOT = ROOT / "tool" / "public-api.txt"
ABOUT = ROOT / "src" / "pytitect" / "__about__.py"

REMOVED_SYMBOLS = (
    ("pytitect.checkpoints", "CheckpointCoordinator"),
    ("pytitect.django", "DjangoFencedCommitFactory"),
)


def _requirement(project: dict[str, object], extra: str, distribution: str) -> str:
    optional = project["optional-dependencies"]
    assert isinstance(optional, dict)
    requirements = optional[extra]
    assert isinstance(requirements, list)
    for value in requirements:
        assert isinstance(value, str)
        if value.lower().startswith(distribution.lower()):
            return value[len(distribution) :]
    raise SystemExit(f"{distribution} is missing from the {extra} extra")


def compatibility_document() -> str:
    configuration = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    project = configuration["project"]
    assert isinstance(project, dict)
    python = project["requires-python"]
    assert isinstance(python, str)
    django = _requirement(project, "django", "Django")
    drf = _requirement(project, "drf", "djangorestframework")
    spectacular = _requirement(project, "contracts", "drf-spectacular")
    return f"""# Compatibility

This table is generated from `pyproject.toml` and checked by `tool/docs_quality.py`.

| Surface | Declared support | CI evidence |
| --- | --- | --- |
| CPython | `{python}` | 3.12, 3.13, and 3.14 unit matrix |
| Django | `{django}` | 5.2 minimum/latest jobs and PostgreSQL consumers |
| Django REST Framework | `{drf}` | minimum/latest adapter jobs |
| drf-spectacular | `{spectacular}` | contracts smoke and schema tests |

Optional dependencies remain isolated: the core imports with none installed. `pytitect.aio` is a
reserved namespace; 1.0 has no FastAPI, ASGI, or async-store implementation. The package wheel used
by the extras smokes is also installed unchanged in the Django canary and reference project.
"""


def check_links() -> None:
    pattern = re.compile(r"(?<!!)\[[^]]*\]\(([^)]+)\)")
    broken: list[str] = []
    markdown = sorted(ROOT.glob("*.md")) + sorted((ROOT / "docs").rglob("*.md"))
    markdown += sorted((ROOT / "examples").rglob("*.md"))
    for document in markdown:
        for line_number, line in enumerate(document.read_text(encoding="utf-8").splitlines(), 1):
            for match in pattern.finditer(line):
                raw = match.group(1).split(maxsplit=1)[0].strip("<>")
                if not raw or raw.startswith(("#", "http://", "https://", "mailto:")):
                    continue
                target = raw.split("#", 1)[0]
                if target and not (document.parent / target).resolve().exists():
                    broken.append(f"{document.relative_to(ROOT)}:{line_number}: {raw}")
    if broken:
        raise SystemExit("Broken local documentation links:\n" + "\n".join(broken))


def check_executable_snippets() -> None:
    pattern = re.compile(
        r"<!-- executable -->\s*```python\n(?P<code>.*?)\n```",
        re.DOTALL,
    )
    documents = sorted(ROOT.glob("*.md")) + sorted((ROOT / "docs").rglob("*.md"))
    snippets = 0
    for document in documents:
        for match in pattern.finditer(document.read_text(encoding="utf-8")):
            snippets += 1
            result = subprocess.run(
                [sys.executable, "-I", "-c", match.group("code")],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
            )
            if result.returncode:
                raise SystemExit(
                    f"Executable snippet failed in {document.relative_to(ROOT)}:\n"
                    + result.stdout
                    + result.stderr
                )
    if snippets == 0:
        raise SystemExit("at least one executable Python documentation snippet is required")


def check_removed_symbols() -> None:
    present = [
        f"{module}:{symbol}"
        for module, symbol in REMOVED_SYMBOLS
        if hasattr(importlib.import_module(module), symbol)
    ]
    if present:
        raise SystemExit(f"Removed compatibility symbols are still public: {present}")


def _stability(entry: str) -> str:
    module, symbol = entry.split(":", 1)
    if module == "pytitect.canaries" or symbol.endswith("Harness"):
        return "Testing"
    if module == "pytitect.sync":
        return "Preview"
    if module in {
        "pytitect.contracts",
        "pytitect.django",
        "pytitect.drf",
        "pytitect.security",
    }:
        return "Low-level"
    return "Stable"


def check_api_stability() -> None:
    policy = (ROOT / "docs" / "api-stability.md").read_text(encoding="utf-8")
    for label in ("Stable", "Preview", "Testing", "Low-level"):
        if f"## {label}" not in policy:
            raise SystemExit(f"API stability policy is missing {label}")
    entries = [
        line
        for line in SNAPSHOT.read_text(encoding="utf-8").splitlines()
        if line and not line.startswith("#")
    ]
    classifications = {_stability(entry) for entry in entries}
    if classifications != {"Stable", "Preview", "Testing", "Low-level"}:
        raise SystemExit("the public API snapshot is not completely classified")


def check_versions() -> None:
    about = ABOUT.read_text(encoding="utf-8")
    match = re.fullmatch(r'__version__ = "([^"]+)"\n?', about)
    if match is None:
        raise SystemExit("package version source is malformed")
    version = match.group(1)
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    versioning = (ROOT / "docs" / "versioning.md").read_text(encoding="utf-8")
    sync = (ROOT / "docs" / "sync.md").read_text(encoding="utf-8")
    if version not in readme or version not in versioning:
        raise SystemExit("package version status drifted from documentation")
    if "titect-sync/1" not in sync or "independently" not in sync:
        raise SystemExit("sync protocol version independence is undocumented")
    workflow = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    if 'python: ["3.12", "3.13", "3.14"]' not in workflow:
        raise SystemExit("documented Python compatibility differs from the CI matrix")


def main() -> int:
    expected = compatibility_document()
    if COMPATIBILITY.read_text(encoding="utf-8") != expected:
        raise SystemExit("docs/compatibility.md differs from pyproject.toml")
    check_links()
    check_executable_snippets()
    check_removed_symbols()
    check_api_stability()
    check_versions()
    print("Documentation links, snippets, compatibility, API stability, and versions match.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
