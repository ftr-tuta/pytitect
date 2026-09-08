#!/usr/bin/env python3
"""Run committed Python, Dart VM and Chrome against one verified authoritative corpus."""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import os
import platform
import secrets
import subprocess
from pathlib import Path
from typing import Any

from tool.wire_conformance import CORPUS, ROOT, compare, execute, load_corpus

from pytitect.contracts import ContractManifest

PROFILES = ("titect-sync/1", "titect-message/1", "titect-message/2")


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def git(root: Path, *arguments: str) -> str:
    return subprocess.check_output(
        ["git", "-C", str(root), *arguments], text=True, timeout=30
    ).strip()


def verify_source(root: Path, revision: str) -> None:
    if len(revision) != 40 or any(character not in "0123456789abcdef" for character in revision):
        raise ValueError("source revisions must be full committed SHAs")
    if git(root, "rev-parse", "HEAD") != revision or git(root, "status", "--porcelain"):
        raise ValueError("source revision is stale, dirty, or substituted")


def bundle_identity(root: Path) -> dict[str, str]:
    manifest_path = root / "manifest.json"
    manifest_bytes = manifest_path.read_bytes()
    manifest = json.loads(manifest_bytes)
    actual = ContractManifest.from_paths(
        root, [path for path in root.rglob("*.json") if path != manifest_path]
    ).to_dict()
    if actual != manifest:
        raise ValueError("bundle hash, inventory, or version drift")
    return {"manifestSha256": sha256(manifest_bytes), "bundleDigest": manifest["digest"]}


def candidate_reference(dart_root: Path, dart_sha: str, *, mode: str) -> dict[str, Any]:
    python_sha = git(ROOT, "rev-parse", "HEAD")
    verify_source(ROOT, python_sha)
    verify_source(dart_root, dart_sha)
    load_corpus()
    if (dart_root / "tool/titect_fixture/vectors.json").read_bytes() != (
        CORPUS / "vectors.json"
    ).read_bytes():
        raise ValueError("Dart corpus differs from the authoritative Python corpus")
    bundles = {}
    for profile in PROFILES:
        python_identity = bundle_identity(ROOT / "interop" / profile)
        if bundle_identity(dart_root / "tool/titect_fixture/bundles" / profile) != python_identity:
            raise ValueError("Python and Dart bundle identities differ")
        bundles[profile] = python_identity
    import pytitect

    if not Path(pytitect.__file__).resolve().is_relative_to(ROOT):
        raise ValueError("Python imported an ambient installation")
    version_tree = ast.parse((ROOT / "src/pytitect/__about__.py").read_text())
    python_version = next(
        ast.literal_eval(node.value)
        for node in version_tree.body
        if isinstance(node, ast.Assign)
        and any(
            isinstance(target, ast.Name) and target.id == "__version__" for target in node.targets
        )
    )
    dart_version = json.loads((dart_root / "tool/package_release_contract.json").read_text())[
        "workspaceCohort"
    ]["version"]
    pin = json.loads((ROOT / "interop/dart/candidate.json").read_text())
    if (
        pin.get("schemaVersion") != 1
        or pin.get("dartSha") != dart_sha
        or pin.get("dartVersion") != dart_version
    ):
        raise ValueError("Dart candidate version or source differs from the committed pin")
    if mode == "integrated":
        subprocess.run(
            [
                "git",
                "-C",
                str(ROOT),
                "merge-base",
                "--is-ancestor",
                python_sha,
                "refs/remotes/origin/main",
            ],
            check=True,
            timeout=30,
        )
    return {
        "schemaVersion": 1,
        "executionId": secrets.token_hex(16),
        "mode": mode,
        "releaseEligible": False,
        "pythonSha": python_sha,
        "pythonTree": git(ROOT, "rev-parse", "HEAD^{tree}"),
        "dartSha": dart_sha,
        "dartTree": git(dart_root, "rev-parse", "HEAD^{tree}"),
        "sourceVersions": {"pytitect": python_version, "dartitect": dart_version},
        "bundles": bundles,
        "corpusSha256": sha256((CORPUS / "vectors.json").read_bytes()),
        "expectationsSha256": sha256((CORPUS / "expectations.json").read_bytes()),
        "corpusManifestSha256": sha256((CORPUS / "manifest.json").read_bytes()),
        "executionModes": ["python", "vm", "chrome"],
    }


def dart_outcomes(dart_root: Path, dart: str, target: str, output: Path) -> list[dict[str, Any]]:
    result = subprocess.run(
        [
            dart,
            "test",
            "--reporter",
            "json",
            "--platform",
            target,
            "test/titect_conformance_test.dart",
        ],
        cwd=dart_root / "packages/dartitect_sync",
        capture_output=True,
        text=True,
        timeout=180,
    )
    (output / f"{target}.jsonl").write_text(result.stdout)
    (output / f"{target}.stderr.log").write_text(result.stderr)
    if result.returncode:
        raise ValueError(f"Dart {target} execution failed; logs retained")
    found = []
    for line in result.stdout.splitlines():
        try:
            event = json.loads(line)
        except ValueError:
            continue
        message = event.get("message", "")
        if event.get("type") == "print" and message.startswith("TITECT_RESULTS:"):
            found.append(json.loads(message.removeprefix("TITECT_RESULTS:")))
    if len(found) != 1 or not isinstance(found[0], list):
        raise ValueError("missing or duplicate Dart conformance results")
    return found[0]


def write_json(path: Path, value: object) -> str:
    data = (json.dumps(value, indent=2, ensure_ascii=False, sort_keys=True) + "\n").encode()
    path.write_bytes(data)
    digest = sha256(data)
    path.with_suffix(".sha256").write_text(f"{digest}  {path.name}\n")
    return digest


def validate_report(report: dict[str, Any], reference: dict[str, Any], output: Path) -> None:
    """Reject stale identities, altered results, missing targets and unexplained divergences."""

    if report.get("reference") != reference or report.get("referenceSha256") != sha256(
        (output / "candidate-reference.json").read_bytes()
    ):
        raise ValueError("forged or stale candidate identity")
    if report.get("status") != "passed" or report.get("residualResources") != {
        "runnerSubprocesses": 0
    }:
        raise ValueError("failed or incomplete conformance evidence")
    if set(report.get("targets", {})) != {"python", "vm", "chrome"}:
        raise ValueError("required runtime evidence is missing")
    _, expected = load_corpus()
    for name, target in report["targets"].items():
        data = (output / f"{name}.json").read_bytes()
        if (
            target.get("outcomesSha256") != sha256(data)
            or compare(expected, json.loads(data))
            or target.get("divergences") != []
        ):
            raise ValueError("altered, divergent or substituted runtime evidence")
    if reference["mode"] == "candidate" and report.get("releaseEligible") is not False:
        raise ValueError("candidate evidence cannot establish release acceptance")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dart-root", type=Path, required=True)
    parser.add_argument("--dart-sha", required=True)
    parser.add_argument("--dart", default="dart")
    parser.add_argument("--mode", choices=["candidate", "integrated"], default="candidate")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    report: dict[str, Any] = {
        "schemaVersion": 1,
        "status": "failed",
        "releaseEligible": False,
        "targets": {},
        "residualResources": None,
        "environment": {
            "python": platform.python_version(),
            "platform": platform.platform(),
            "runId": os.environ.get("GITHUB_RUN_ID"),
            "runAttempt": os.environ.get("GITHUB_RUN_ATTEMPT"),
        },
    }
    try:
        reference = candidate_reference(args.dart_root.resolve(), args.dart_sha, mode=args.mode)
        report["reference"] = reference
        report["referenceSha256"] = write_json(args.output / "candidate-reference.json", reference)
        chrome = os.environ.get("CHROME_EXECUTABLE")
        if not chrome:
            raise ValueError("Chrome is required; no target fallback")
        report["environment"]["chrome"] = subprocess.check_output(
            [chrome, "--version"], text=True, timeout=15
        ).strip()
        report["environment"]["dart"] = subprocess.check_output(
            [args.dart, "--version"], text=True, timeout=15
        ).strip()
        vectors, expected = load_corpus()
        for target in ("python", "vm", "chrome"):
            outcomes = (
                [execute(vector) for vector in vectors]
                if target == "python"
                else dart_outcomes(args.dart_root, args.dart, target, args.output)
            )
            report["targets"][target] = {
                "outcomesSha256": write_json(args.output / f"{target}.json", outcomes),
                "divergences": compare(expected, outcomes),
            }
        verify_source(ROOT, reference["pythonSha"])
        verify_source(args.dart_root, reference["dartSha"])
        report["residualResources"] = {"runnerSubprocesses": 0}
        report["status"] = "passed"
        validate_report(report, reference, args.output)
    except Exception as error:
        report["status"] = "failed"
        report["failure"] = {"type": type(error).__name__, "reason": str(error)}
    finally:
        write_json(args.output / "conformance.json", report)
    print(f"Paired conformance: {report['status']}; evidence retained at {args.output}")
    return int(report["status"] != "passed")


if __name__ == "__main__":
    raise SystemExit(main())
