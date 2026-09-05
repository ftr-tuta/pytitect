#!/usr/bin/env python3
"""Enforce global and aggregate branch-coverage quality tiers."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]

CORE_FILES = {"src/pytitect/core.py", "src/pytitect/http.py"}
RISK_FILES = {
    "src/pytitect/idempotency.py",
    "src/pytitect/leases.py",
    "src/pytitect/trace.py",
    "src/pytitect/wire.py",
    "src/pytitect/security/canonical.py",
    "src/pytitect/security/digest.py",
    "src/pytitect/security/encoding.py",
}
EVENT_PLATFORM_FILES = {
    "src/pytitect/aio/quarantine.py",
    "src/pytitect/aio/resilience.py",
    "src/pytitect/aio/observation.py",
    "src/pytitect/aio/stores.py",
    "src/pytitect/aio/idempotency.py",
    "src/pytitect/aio/receipts.py",
    "src/pytitect/aio/event_sourcing.py",
    "src/pytitect/aio/jobs.py",
    "src/pytitect/aio/processes.py",
    "src/pytitect/aio/projections.py",
    "src/pytitect/sqlalchemy/stores.py",
    "src/pytitect/sqlalchemy/relay.py",
    "src/pytitect/sqlalchemy/idempotency.py",
    "src/pytitect/sqlalchemy/uow.py",
    "src/pytitect/sqlalchemy/events.py",
    "src/pytitect/sqlalchemy/workflows.py",
    "src/pytitect/sqlalchemy/projections.py",
    "src/pytitect/sqlalchemy/maintenance.py",
    "src/pytitect/aio/runtime.py",
    "src/pytitect/aio/uow.py",
    "src/pytitect/application.py",
    "src/pytitect/aws/transport.py",
    "src/pytitect/event_sourcing.py",
    "src/pytitect/jobs.py",
    "src/pytitect/nats/transport.py",
    "src/pytitect/processes.py",
    "src/pytitect/projections.py",
}


def _aggregate(files: dict[str, Any], selected: Callable[[str], bool]) -> float:
    covered = total = 0
    for path, details in files.items():
        if not selected(path):
            continue
        summary = details["summary"]
        covered += summary["covered_lines"] + summary["covered_branches"]
        total += summary["num_statements"] + summary["num_branches"]
    if not total:
        raise SystemExit("coverage tier selected no source files")
    return covered * 100 / total


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="pytitect-coverage-") as directory:
        report = Path(directory) / "coverage.json"
        subprocess.run(
            [sys.executable, "-m", "coverage", "json", "-o", str(report)],
            cwd=ROOT,
            check=True,
        )
        document = json.loads(report.read_text(encoding="utf-8"))
    files = document["files"]
    tiers = {
        "global": (float(document["totals"]["percent_covered"]), 85.0),
        "core/http/contracts": (
            _aggregate(
                files,
                lambda path: path in CORE_FILES or path.startswith("src/pytitect/contracts/"),
            ),
            90.0,
        ),
        "idempotency/sync/leases/security parsers": (
            _aggregate(
                files,
                lambda path: path in RISK_FILES or path.startswith("src/pytitect/sync/"),
            ),
            95.0,
        ),
        "event platform critical paths": (
            _aggregate(
                files,
                lambda path: (
                    path in EVENT_PLATFORM_FILES or path.startswith("src/pytitect/messaging/")
                ),
            ),
            95.0,
        ),
    }
    failures: list[str] = []
    for name, (actual, required) in tiers.items():
        print(f"Coverage {name}: {actual:.2f}% (required {required:.2f}%)")
        if actual + 1e-9 < required:
            failures.append(f"{name}={actual:.2f}%<{required:.2f}%")
    if failures:
        raise SystemExit("Coverage gates failed: " + ", ".join(failures))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
