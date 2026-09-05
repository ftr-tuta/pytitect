#!/usr/bin/env python3
"""Required candidate conformance, durable recovery and real-client capacity orchestration."""

from __future__ import annotations

import argparse
import json
import math
import os
import signal
import subprocess
import sys
from contextlib import suppress
from pathlib import Path
from typing import Any

from tool.paired_conformance import ROOT, candidate_reference, sha256, verify_source, write_json

RECOVERY_SCENARIOS = frozenset(
    {
        "local_before_commit",
        "local_after_commit",
        "remote_before_commit",
        "remote_after_commit",
        "response_received",
        "bootstrap_before_commit",
        "bootstrap_after_commit",
        "page_during_apply",
        "page_before_commit",
        "page_after_commit",
        "checkpoint_before_commit",
        "checkpoint_after_commit",
        "fencing/page_before_apply/acquire",
        "fencing/checkpoint_before_commit/acquire",
        "fencing/checkpoint_before_commit/expire",
        "storage-failure-rollback",
        "paired-storm",
        "persistent-chrome-recovery",
        "pending-shadow-retention-and-expired-cursor",
        "django-persistent-mutations",
        "corrupted-page-rejection",
        "negotiated-policy-mismatch",
        "exact-number-persistence",
        "integrity-failure-state-and-checkpoint-unchanged",
    }
)
RESOURCES = frozenset(
    {
        "activeAuthorities",
        "childProcesses",
        "openDatabases",
        "openHttpClients",
        "postgresConnections",
        "queuedTasks",
        "runningTasks",
    }
)


def run_stage(
    command: list[str], *, cwd: Path, env: dict[str, str], log: Path, timeout: float
) -> None:
    """Retain logs and terminate the owned process group on failure or cancellation."""

    with log.open("w") as stream:
        process = subprocess.Popen(
            command,
            cwd=cwd,
            env=env,
            stdout=stream,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        try:
            code = process.wait(timeout=timeout)
            if code:
                raise ValueError(f"paired stage failed ({code}); see {log.name}")
        finally:
            # Only the process group created by this invocation is addressed.
            with suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=5)
            with suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)


def validate_shared(report: dict[str, Any], reference: dict[str, Any]) -> None:
    if report.get("schemaVersion") != 1 or report.get("status") != "passed":
        raise ValueError("unsupported, failed or incomplete paired evidence")
    if report.get("reference") != reference or report.get("releaseEligible") is not False:
        raise ValueError("stale reference or invalid release claim")
    resources = report.get("residualResources", {})
    if set(resources) != RESOURCES or any(
        type(value) is not int or value != 0 for value in resources.values()
    ):
        raise ValueError("missing cleanup evidence or residual resources")
    if report.get("unverified", []) or report.get("unresolvedContracts", []):
        raise ValueError("unresolved paired acceptance criteria")


def validate_recovery(report: dict[str, Any], reference: dict[str, Any]) -> None:
    validate_shared(report, reference)
    scenarios = report.get("scenarios", [])
    names = [row["name"] for row in scenarios]
    if len(set(names)) != len(names) or not set(names) >= RECOVERY_SCENARIOS:
        raise ValueError("durable scenarios are missing, duplicated or substituted")
    if any(row.get("passed") is not True for row in scenarios):
        raise ValueError("durable recovery scenario failed")


def validate_capacity(report: dict[str, Any], reference: dict[str, Any]) -> None:
    validate_shared(report, reference)
    results = report.get("results", [])
    names = [row["scenario"] for row in results]
    if sorted(names) != ["offered", "recovery", "saturation"]:
        raise ValueError("representative paired workload is incomplete")
    for row in results:
        if row.get("passed") is not True or row.get("failures"):
            raise ValueError("paired capacity correctness or resource gate failed")
        required = {
            "offered",
            "statuses",
            "duration_seconds",
            "useful_operations",
            "useful_throughput",
            "latency_seconds",
            "peak_observations",
            "recovery_seconds",
            "durable",
            "load_generator",
        }
        if not required <= row.keys():
            raise ValueError("error-inclusive capacity measurements are missing")
        if (
            type(row["offered"]) is not int
            or row["offered"] <= 0
            or not row["statuses"]
            or any(type(count) is not int or count < 0 for count in row["statuses"].values())
            or sum(row["statuses"].values()) != row["offered"]
        ):
            raise ValueError("offered and rejected work do not reconcile")
        if not {"p50", "p95", "p99", "max"} <= row["latency_seconds"].keys():
            raise ValueError("latency tail measurements are missing")
        if (
            not {"rss_kib", "tasks", "connections", "database_lock_waiters", "backlog_age_seconds"}
            <= row["peak_observations"].keys()
        ):
            raise ValueError("resource, wait or backlog measurements are missing")
        measurements = [
            row["duration_seconds"],
            row["useful_throughput"],
            row["recovery_seconds"],
            *row["latency_seconds"].values(),
            *(
                row["peak_observations"][key]
                for key in (
                    "rss_kib",
                    "tasks",
                    "connections",
                    "database_lock_waiters",
                    "backlog_age_seconds",
                )
            ),
        ]
        if any(
            isinstance(value, bool)
            or not isinstance(value, int | float)
            or not math.isfinite(value)
            or value < 0
            for value in measurements
        ):
            raise ValueError("invalid, negative or nonfinite measurements")
        latency = row["latency_seconds"]
        if not latency["p50"] <= latency["p95"] <= latency["p99"] <= latency["max"]:
            raise ValueError("latency percentiles are inconsistent")
        peak = row["peak_observations"]
        if peak["rss_kib"] > 512 * 1024 or peak["tasks"] > 100 or peak["connections"] > 8:
            raise ValueError("existing server resource limits were exceeded")
        if (
            type(row["useful_operations"]) is not int
            or row["useful_operations"] <= 0
            or row["duration_seconds"] <= 0
        ):
            raise ValueError("no measured useful work")
        expected_throughput = row["useful_operations"] / (
            row["duration_seconds"] + row["recovery_seconds"]
        )
        if not math.isclose(row["useful_throughput"], expected_throughput, rel_tol=1e-9):
            raise ValueError("useful throughput does not match the reported observation interval")
        generator = row["load_generator"]
        if not {"workers", "queue_capacity", "offered_rate"} <= generator.keys():
            raise ValueError("finite load generator configuration is missing")
        if (
            any(
                type(generator[key]) is not int or generator[key] <= 0
                for key in ("workers", "queue_capacity")
            )
            or not isinstance(generator["offered_rate"], int | float)
            or isinstance(generator["offered_rate"], bool)
            or not math.isfinite(generator["offered_rate"])
            or generator["offered_rate"] <= 0
        ):
            raise ValueError("load generator configuration is invalid")
        durable = row["durable"]
        if (
            not {"receipts", "outbox", "inbox", "publication_retries", "pending_outbox"}
            <= durable.keys()
        ):
            raise ValueError("durable recovery measurements are missing")
        if any(type(value) is not int or value < 0 for value in durable.values()):
            raise ValueError("durable counts and retries must be non-negative integers")
        if (
            durable["pending_outbox"] != 0
            or durable["receipts"] != durable["outbox"]
            or durable["outbox"] != durable["inbox"]
            or row["useful_operations"] != durable["inbox"]
        ):
            raise ValueError("durable counts do not reconcile after finite drain")


def load_report(path: Path) -> dict[str, Any]:
    data = path.read_bytes()
    if path.with_suffix(".sha256").read_text() != f"{sha256(data)}  {path.name}\n":
        raise ValueError("paired report checksum drift")
    return json.loads(data)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dart-root", type=Path, required=True)
    parser.add_argument("--dart-sha", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--dart", default="dart")
    parser.add_argument("--mode", choices=["candidate", "integrated"], default="candidate")
    args = parser.parse_args()
    args.dart_root = args.dart_root.resolve()
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=False)
    report: dict[str, Any] = {
        "schemaVersion": 1,
        "status": "failed",
        "releaseEligible": False,
        "stages": [],
    }
    env = os.environ.copy()
    try:
        for name in ("TEST_POSTGRES_DSN", "TEST_NATS_URL", "CHROME_EXECUTABLE"):
            if not env.get(name):
                raise ValueError(f"required paired infrastructure is missing: {name}")
        reference = candidate_reference(args.dart_root, args.dart_sha, mode=args.mode)
        report["reference"] = reference
        manifest = args.output / "candidate-reference.json"
        write_json(manifest, reference)
        env["TITECT_POSTGRES_DSN"] = env["TEST_POSTGRES_DSN"]
        env["TITECT_NATS_URL"] = env["TEST_NATS_URL"]
        conformance = [
            sys.executable,
            "-m",
            "tool.paired_conformance",
            "--dart-root",
            str(args.dart_root),
            "--dart-sha",
            args.dart_sha,
            "--dart",
            args.dart,
            "--mode",
            args.mode,
            "--output",
            str(args.output / "conformance"),
        ]
        run_stage(conformance, cwd=ROOT, env=env, log=args.output / "conformance.log", timeout=450)
        report["stages"].append("conformance")
        native = args.output / "native"
        run_stage(
            [
                args.dart,
                "build",
                "cli",
                "--root-package",
                "dartitect_drift",
                "-t",
                "tool/titect_fixture/composition/native_actor.dart",
                "-o",
                str(native),
            ],
            cwd=args.dart_root,
            env=env,
            log=args.output / "native-build.log",
            timeout=300,
        )
        actor = native / "bundle/bin/native_actor"
        for name, validator in (("recovery", validate_recovery), ("capacity", validate_capacity)):
            stage_output = args.output / name
            stage_output.mkdir(exist_ok=True)
            command = [
                sys.executable,
                str(args.dart_root / f"tool/run_titect_{name}.py"),
                "--python-root",
                str(ROOT),
                "--actor",
                str(actor),
                "--reference-manifest",
                str(manifest),
                "--output",
                str(stage_output),
            ]
            if name == "recovery":
                command.extend(["--django-python", sys.executable])
            run_stage(
                command, cwd=args.dart_root, env=env, log=args.output / f"{name}.log", timeout=600
            )
            stage_report = load_report(stage_output / f"{name}.json")
            if stage_report.get("nativeActorSha256") != sha256(actor.read_bytes()):
                raise ValueError("native actor artifact is stale or substituted")
            validator(stage_report, reference)
            report["stages"].append(name)
        verify_source(ROOT, reference["pythonSha"])
        verify_source(args.dart_root, reference["dartSha"])
        report["status"] = "passed"
    except Exception as error:
        report["failure"] = {"type": type(error).__name__, "reason": str(error)}
    finally:
        write_json(args.output / "paired-gate.json", report)
    print(f"Paired candidate gate: {report['status']}; reports retained at {args.output}")
    return int(report["status"] != "passed")


if __name__ == "__main__":
    raise SystemExit(main())
