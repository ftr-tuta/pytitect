#!/usr/bin/env python3
"""Prove that the 1.0.0 promotion differs from rc1 only in release metadata."""

from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ABOUT = Path("src/pytitect/__about__.py")
CHANGELOG = Path("CHANGELOG.md")
MANIFEST = Path("interop/manifest.json")
PYPROJECT = Path("pyproject.toml")
ALLOWED_PROMOTION_PATHS = {ABOUT, CHANGELOG, MANIFEST, PYPROJECT}


def _git(*arguments: str, check: bool = True) -> str:
    result = subprocess.run(
        ["git", *arguments],
        cwd=ROOT,
        check=check,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def _tag_commit(tag: str) -> str | None:
    value = _git("rev-list", "-n", "1", tag, check=False)
    return value or None


def _tag_file(tag: str, path: Path) -> str:
    return _git("show", f"{tag}:{path.as_posix()}") + "\n"


def main() -> int:
    about = (ROOT / ABOUT).read_text(encoding="utf-8")
    match = re.fullmatch(r'__version__ = "([^"]+)"\n?', about)
    if match is None:
        raise SystemExit("package version source is malformed")
    version = match.group(1)
    if version != "1.0.0":
        print("Stable-promotion tree gate is not applicable to this source version.")
        return 0

    head = _git("rev-parse", "HEAD")
    stable_tag = "v1.0.0"
    stable_commit = _tag_commit(stable_tag)
    if stable_commit is not None:
        if stable_commit != head:
            raise SystemExit(f"{stable_tag} does not point to HEAD")
        print(f"Stable-promotion tree is the protected {stable_tag} commit.")
        return 0

    candidate_tag = "v1.0.0-rc.1"
    if _tag_commit(candidate_tag) is None:
        raise SystemExit(f"required candidate tag is missing: {candidate_tag}")
    changed = {
        Path(path)
        for path in _git("diff", "--name-only", f"{candidate_tag}..HEAD").splitlines()
        if path
    }
    if changed != ALLOWED_PROMOTION_PATHS:
        unexpected = sorted(str(path) for path in changed - ALLOWED_PROMOTION_PATHS)
        missing = sorted(str(path) for path in ALLOWED_PROMOTION_PATHS - changed)
        raise SystemExit(
            f"invalid stable promotion paths; unexpected={unexpected}, missing={missing}"
        )

    expected_about = _tag_file(candidate_tag, ABOUT).replace(
        '__version__ = "1.0.0rc1"',
        '__version__ = "1.0.0"',
    )
    if about != expected_about:
        raise SystemExit("stable promotion changed more than the package version source")

    expected_project = _tag_file(candidate_tag, PYPROJECT).replace(
        '"Development Status :: 4 - Beta"',
        '"Development Status :: 5 - Production/Stable"',
    )
    if (ROOT / PYPROJECT).read_text(encoding="utf-8") != expected_project:
        raise SystemExit("stable promotion changed more than the development-status classifier")

    expected_manifest = json.loads(_tag_file(candidate_tag, MANIFEST))
    expected_manifest["package_version"] = "1.0.0"
    expected_manifest["release_tag"] = stable_tag
    actual_manifest = json.loads((ROOT / MANIFEST).read_text(encoding="utf-8"))
    if actual_manifest != expected_manifest:
        raise SystemExit("stable promotion changed more than the release manifest identity")

    changelog = (ROOT / CHANGELOG).read_text(encoding="utf-8")
    if "## 1.0.0 - " not in changelog or "No functional changes since `1.0.0rc1`." not in changelog:
        raise SystemExit("stable changelog must attest that the RC tree is unchanged")
    print("Stable 1.0.0 tree differs from rc1 only in release metadata.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
