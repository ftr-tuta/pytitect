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
