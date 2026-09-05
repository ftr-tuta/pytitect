"""Paired evidence must fail for drift, substituted cases and candidate release claims."""

import copy
import json
import shutil
from pathlib import Path

import pytest
from tool.paired_conformance import validate_report, verify_source, write_json
from tool.wire_conformance import CORPUS, compare, execute, load_corpus


def test_authoritative_corpus_runs_at_raw_boundaries():
    vectors, expected = load_corpus()
    assert len(vectors) >= 232
    assert compare(expected, [execute(vector) for vector in vectors]) == []
    assert compare(expected, expected[:-1]) == ["case_count"]
    assert compare(expected, list(reversed(expected)))
    changed = copy.deepcopy(expected)
    changed[0]["roundTrip"] += " "
    assert compare(expected, changed) == [expected[0]["name"]]


@pytest.mark.parametrize(
    "filename", ["vectors.json", "legacy-vectors.json", "expectations.json", "manifest.json"]
)
def test_fixture_and_manifest_drift_fail_closed(tmp_path, filename):
    shutil.copytree(CORPUS, tmp_path / "corpus")
    path = tmp_path / "corpus" / filename
    path.write_bytes(path.read_bytes() + b" ")
    if filename == "manifest.json":
        document = json.loads(path.read_text())
        document["digest"] = "0" * 64
        path.write_text(json.dumps(document))
    with pytest.raises(ValueError):
        load_corpus(tmp_path / "corpus")


def valid_report(output):
    _, expected = load_corpus()
    reference = {"pythonSha": "1" * 40, "dartSha": "2" * 40, "mode": "candidate"}
    reference_digest = write_json(output / "candidate-reference.json", reference)
    report = {
        "status": "passed",
        "releaseEligible": False,
        "reference": reference,
        "referenceSha256": reference_digest,
        "residualResources": {"runnerSubprocesses": 0},
        "targets": {},
    }
    for target in ["python", "vm", "chrome"]:
        report["targets"][target] = {
            "outcomesSha256": write_json(output / f"{target}.json", expected),
            "divergences": [],
        }
    return reference, report


@pytest.mark.parametrize(
    "change",
    [
        "stale-sha",
        "reference-hash",
        "missing-chrome",
        "failed",
        "residual",
        "result-hash",
        "divergence",
        "release",
    ],
)
def test_forged_stale_or_incomplete_evidence_is_rejected(tmp_path, change):
    reference, report = valid_report(tmp_path)
    validate_report(report, reference, tmp_path)
    report = copy.deepcopy(report)
    if change == "stale-sha":
        report["reference"]["pythonSha"] = "3" * 40
    elif change == "reference-hash":
        report["referenceSha256"] = "0" * 64
    elif change == "missing-chrome":
        del report["targets"]["chrome"]
    elif change == "failed":
        report["status"] = "failed"
    elif change == "residual":
        report["residualResources"]["runnerSubprocesses"] = 1
    elif change == "result-hash":
        (tmp_path / "vm.json").write_text("[]")
    elif change == "divergence":
        report["targets"]["chrome"]["divergences"] = ["precision"]
    elif change == "release":
        report["releaseEligible"] = True
    with pytest.raises(ValueError):
        validate_report(report, reference, tmp_path)


def test_unsupported_profile_and_unpinned_source_fail():
    assert execute({"name": "unknown", "profile": "titect-message/999", "wire": "{}"}) == {
        "name": "unknown",
        "accepted": False,
        "problem": "unsupported_profile",
    }
    with pytest.raises(ValueError):
        verify_source(Path("."), "main")
    with pytest.raises(ValueError):
        verify_source(Path("."), "0" * 40)


def recovery_report(reference):
    from tool.paired_gate import RECOVERY_SCENARIOS, RESOURCES

    return {
        "schemaVersion": 1,
        "status": "passed",
        "reference": reference,
        "releaseEligible": False,
        "residualResources": dict.fromkeys(RESOURCES, 0),
        "scenarios": [{"name": name, "passed": True} for name in sorted(RECOVERY_SCENARIOS)],
    }


def test_recovery_requires_all_historical_and_integrity_scenarios():
    from tool.paired_gate import validate_recovery

    reference = {"pythonSha": "1" * 40, "dartSha": "2" * 40, "mode": "candidate"}
    report = recovery_report(reference)
    validate_recovery(report, reference)
    for key, value in [
        ("schemaVersion", 2),
        ("status", "incomplete"),
        ("releaseEligible", True),
        ("reference", {}),
        ("residualResources", {}),
        ("unverified", ["integrity"]),
    ]:
        with pytest.raises(ValueError):
            validate_recovery({**report, key: value}, reference)
    for scenarios in [
        report["scenarios"][:-1],
        [*report["scenarios"], report["scenarios"][0]],
        [{**row, "passed": False} for row in report["scenarios"]],
    ]:
        with pytest.raises(ValueError):
            validate_recovery({**report, "scenarios": scenarios}, reference)


def capacity_report(reference):
    report = recovery_report(reference)
    del report["scenarios"]
    report["results"] = [
        {
            "scenario": name,
            "passed": True,
            "failures": [],
            "offered": 10,
            "statuses": {"200": 9, "rejected": 1},
            "duration_seconds": 2,
            "useful_operations": 9,
            "useful_throughput": 3,
            "recovery_seconds": 1,
            "latency_seconds": {"p50": 0.1, "p95": 0.2, "p99": 0.3, "max": 0.4},
            "peak_observations": {
                "rss_kib": 1000,
                "tasks": 10,
                "connections": 2,
                "database_lock_waiters": 1,
                "backlog_age_seconds": 1,
            },
            "durable": {
                "receipts": 9,
                "outbox": 9,
                "inbox": 9,
                "publication_retries": 0,
                "pending_outbox": 0,
            },
            "load_generator": {"workers": 2, "queue_capacity": 4, "offered_rate": 5},
        }
        for name in ["offered", "saturation", "recovery"]
    ]
    return report


@pytest.mark.parametrize(
    "path,value",
    [
        (("passed",), False),
        (("statuses",), {"200": 9}),
        (("offered",), True),
        (("duration_seconds",), 0),
        (("useful_operations",), 0),
        (("useful_throughput",), 9),
        (("latency_seconds", "p50"), float("nan")),
        (("latency_seconds", "p95"), 1),
        (("peak_observations", "tasks"), 101),
        (("peak_observations", "connections"), 9),
        (("peak_observations", "rss_kib"), 512 * 1024 + 1),
        (("durable", "pending_outbox"), 1),
        (("durable", "inbox"), 8),
        (("durable", "publication_retries"), 0.5),
    ],
)
def test_capacity_requires_error_inclusive_finite_consistent_evidence(path, value):
    from tool.paired_gate import validate_capacity

    reference = {"pythonSha": "1" * 40, "dartSha": "2" * 40, "mode": "candidate"}
    report = capacity_report(reference)
    validate_capacity(report, reference)
    target = report["results"][0]
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    with pytest.raises(ValueError):
        validate_capacity(report, reference)


def test_evidence_checksums_and_stage_timeout_preserve_failures(tmp_path):
    import os
    import subprocess
    import sys

    from tool.paired_gate import load_report, run_stage

    path = tmp_path / "recovery.json"
    write_json(path, {"status": "failed"})
    assert load_report(path)["status"] == "failed"
    path.write_text("{}")
    with pytest.raises(ValueError):
        load_report(path)
    pid_file = tmp_path / "pid"
    command = [
        sys.executable,
        "-c",
        'import os,sys,time; open(sys.argv[1],"w").write(str(os.getpid())); time.sleep(30)',
        str(pid_file),
    ]
    with pytest.raises(subprocess.TimeoutExpired):
        run_stage(
            command, cwd=tmp_path, env=os.environ.copy(), log=tmp_path / "timeout.log", timeout=1
        )
    with pytest.raises(ProcessLookupError):
        os.kill(int(pid_file.read_text()), 0)
    with pytest.raises(ValueError):
        run_stage(
            [sys.executable, "-c", "raise SystemExit(2)"],
            cwd=tmp_path,
            env=os.environ.copy(),
            log=tmp_path / "failed.log",
            timeout=5,
        )


def test_missing_infrastructure_fails_with_retained_report(tmp_path):
    import os
    import subprocess
    import sys

    env = {
        key: value
        for key, value in os.environ.items()
        if key not in {"TEST_POSTGRES_DSN", "TEST_NATS_URL", "CHROME_EXECUTABLE"}
    }
    output = tmp_path / "evidence"
    command = [
        sys.executable,
        "-m",
        "tool.paired_gate",
        "--dart-root",
        str(tmp_path),
        "--dart-sha",
        "0" * 40,
        "--output",
        str(output),
    ]
    result = subprocess.run(command, env=env, capture_output=True, timeout=10)
    assert result.returncode != 0
    report = json.loads((output / "paired-gate.json").read_text())
    assert (
        report["status"] == "failed" and "infrastructure is missing" in report["failure"]["reason"]
    )
    before = (output / "paired-gate.json").read_bytes()
    assert subprocess.run(command, env=env, capture_output=True, timeout=10).returncode != 0
    assert (output / "paired-gate.json").read_bytes() == before


def test_paired_capacity_retains_the_existing_python_measurement_schema():
    from tool.paired_gate import validate_capacity

    reference = {"mode": "candidate"}
    original = json.loads(
        (CORPUS.parents[1] / "docs/evidence/2026-09-05-python/capacity.json").read_text()
    )
    report = recovery_report(reference)
    report["results"] = original["results"]
    validate_capacity(report, reference)
