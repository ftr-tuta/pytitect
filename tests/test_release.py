from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from pytitect import __version__


def test_current_release_notes_are_bounded_to_the_version_section() -> None:
    result = subprocess.run(
        [sys.executable, "tool/release_notes.py", "--version", __version__],
        check=True,
        capture_output=True,
        text=True,
    )

    assert result.stdout.startswith(f"# Pytitect {__version__}\n")
    assert "## 0.9.0a1" not in result.stdout


def test_release_notes_stop_at_the_next_version_and_require_content(tmp_path: Path) -> None:
    changelog = (
        "# Changelog\n\n"
        "## 1.2.0 - 2026-01-02\n\n### Added\n\n- A.\n\n"
        "## 1.1.0 - 2026-01-01\n\n### Added\n\n- B.\n"
    )
    changelog_path = tmp_path / "CHANGELOG.md"
    output = tmp_path / "notes.md"
    changelog_path.write_text(changelog, encoding="utf-8")

    subprocess.run(
        [
            sys.executable,
            "tool/release_notes.py",
            "--changelog",
            str(changelog_path),
            "--version",
            "1.2.0",
            "--output",
            str(output),
        ],
        check=True,
    )
    assert output.read_text(encoding="utf-8") == "# Pytitect 1.2.0\n\n### Added\n\n- A.\n"

    missing = subprocess.run(
        [
            sys.executable,
            "tool/release_notes.py",
            "--changelog",
            str(changelog_path),
            "--version",
            "2.0.0",
            "--check",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert missing.returncode != 0
    assert "dated changelog section is missing" in missing.stderr
