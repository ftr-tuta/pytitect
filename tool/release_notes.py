#!/usr/bin/env python3
"""Extract the dated changelog section for the current release."""

from __future__ import annotations

import argparse
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CHANGELOG = ROOT / "CHANGELOG.md"
ABOUT = ROOT / "src" / "pytitect" / "__about__.py"


def current_version() -> str:
    match = re.fullmatch(
        r'__version__ = "([^"]+)"\n?',
        ABOUT.read_text(encoding="utf-8"),
    )
    if match is None:
        raise ValueError("package version source is malformed")
    return match.group(1)


def release_notes(changelog: str, version: str) -> str:
    heading = re.search(
        rf"^## {re.escape(version)} - \d{{4}}-\d{{2}}-\d{{2}}$",
        changelog,
        re.MULTILINE,
    )
    if heading is None:
        raise ValueError(f"dated changelog section is missing for {version}")
    following = changelog.find("\n## ", heading.end())
    end = len(changelog) if following < 0 else following
    body = changelog[heading.end() : end].strip()
    if not body or "### " not in body:
        raise ValueError(f"changelog section for {version} has no release notes")
    return f"# Pytitect {version}\n\n{body}\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--changelog", type=Path, default=CHANGELOG)
    parser.add_argument("--version", default=current_version())
    parser.add_argument("--output", type=Path)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    notes = release_notes(args.changelog.read_text(encoding="utf-8"), args.version)
    if args.output is not None:
        args.output.write_text(notes, encoding="utf-8")
    elif not args.check:
        print(notes, end="")
    if args.check:
        print(f"Release notes match the {args.version} changelog section.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
