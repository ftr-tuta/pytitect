from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def test_sync_bundle_and_manifest() -> None:
    root = Path(__file__).parents[1]
    result = subprocess.run(
        [sys.executable, "tool/sync_bundle.py"],
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
