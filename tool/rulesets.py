#!/usr/bin/env python3
"""Idempotently verify GitHub rulesets against versioned policy (no mutations)."""

from __future__ import annotations

import argparse
import json
import os
import urllib.request
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]


def request_json(url: str, token: str) -> Any:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "pytitect-ruleset-verifier",
        },
    )
    with urllib.request.urlopen(request, timeout=15) as response:
        return json.load(response)


def comparable(value: dict[str, Any]) -> dict[str, Any]:
    keys = {"name", "target", "enforcement", "bypass_actors", "conditions", "rules"}
    return {key: value[key] for key in keys}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repository", default="ftr-tuta/pytitect")
    args = parser.parse_args()
    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        raise SystemExit("GITHUB_TOKEN is required")
    desired = [
        json.loads(path.read_text())
        for path in sorted((ROOT / ".github" / "rulesets").glob("*.json"))
    ]
    summaries = request_json(f"https://api.github.com/repos/{args.repository}/rulesets", token)
    actual = [
        comparable(
            request_json(
                f"https://api.github.com/repos/{args.repository}/rulesets/{item['id']}", token
            )
        )
        for item in summaries
        if item["name"] in {rule["name"] for rule in desired}
    ]
    if sorted(actual, key=lambda item: item["name"]) != sorted(
        desired, key=lambda item: item["name"]
    ):
        print("GitHub rulesets differ from versioned policy.")
        return 1
    print("GitHub rulesets match versioned policy.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
