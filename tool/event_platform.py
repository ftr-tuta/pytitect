#!/usr/bin/env python3
"""Validate release-level event-platform invariants and pinned canary versions."""

from __future__ import annotations

import json
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _requirements() -> dict[str, list[str]]:
    document = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    project = document["project"]
    assert isinstance(project, dict)
    optional = project["optional-dependencies"]
    assert isinstance(optional, dict)
    return optional


def main() -> int:
    requirements = _requirements()
    expected = {
        "fastapi": "fastapi>=0.141,<0.142",
        "sqlalchemy": "SQLAlchemy[asyncio]>=2.0.52,<2.1",
        "nats": "nats-py>=2.15,<3",
        "aws": "boto3>=1.43.88,<2",
        "faststream-nats": "faststream[nats]>=0.7.5,<0.8",
    }
    for extra, requirement in expected.items():
        if requirement not in requirements.get(extra, []):
            raise SystemExit(f"event-platform dependency drift: {requirement}")

    asyncapi = json.loads(
        (ROOT / "interop" / "titect-message" / "1" / "asyncapi.json").read_text(encoding="utf-8")
    )
    if asyncapi.get("asyncapi") != "3.1.0":
        raise SystemExit("AsyncAPI 3.1.0 is required")
    if asyncapi.get("channels") or asyncapi.get("operations") or asyncapi.get("servers"):
        raise SystemExit("package AsyncAPI must remain route- and topology-neutral")

    expanded = (ROOT / ".github" / "workflows" / "event-platform.yml").read_text(encoding="utf-8")
    expanded += (ROOT / "tool" / "integration_environment.py").read_text(encoding="utf-8")
    for required in (
        'postgres: ["15", "16", "17", "18"]',
        "nats:2.14.5",
        "localstack/localstack:4.14.0",
    ):
        if required not in expanded:
            raise SystemExit(f"expanded event-platform matrix is missing {required}")
    aws = (ROOT / ".github" / "workflows" / "aws-canary.yml").read_text(encoding="utf-8")
    for required in ("AWS_CANARY_ROLE_ARN", "AWS_CANARY_REGION", "us-east-1", "id-token: write"):
        if required not in aws:
            raise SystemExit(f"AWS canary is missing {required}")
    print("Event-platform contracts, dependencies, and canary matrices match.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
