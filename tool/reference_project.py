#!/usr/bin/env python3
"""Validate the committed reference-project OpenAPI document and local sync references."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from typing import Any, cast

from pytitect.contracts import LocalRefResolver, ResolvedDocument

ROOT = Path(__file__).resolve().parents[1]
REFERENCE = ROOT / "examples" / "django_reference"
OPENAPI = REFERENCE / "openapi.json"


def generated_openapi() -> dict[str, Any]:
    source = REFERENCE / "reference_project" / "openapi.py"
    spec = importlib.util.spec_from_file_location("reference_openapi", source)
    if spec is None or spec.loader is None:
        raise SystemExit("reference OpenAPI builder could not be loaded")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return cast(dict[str, Any], module.build_openapi())


def main() -> int:
    committed = json.loads(OPENAPI.read_text(encoding="utf-8"))
    if committed != generated_openapi():
        raise SystemExit("reference openapi.json differs from its deterministic builder")
    resolved = LocalRefResolver(ROOT).resolve(OPENAPI.relative_to(ROOT))
    if not isinstance(resolved, ResolvedDocument):
        raise SystemExit(f"reference OpenAPI local references failed: {resolved}")
    paths = committed.get("paths", {})
    expected = {
        "/reference/legacy/mutations/{mutation_id}",
        "/reference/sync/1/mutations/{mutation_id}",
    }
    if set(paths) != expected:
        raise SystemExit("reference OpenAPI must expose exactly the legacy and versioned examples")
    if resolved.references < 8:
        raise SystemExit("reference OpenAPI did not resolve its local component graph")
    print(
        "Reference OpenAPI matches its builder and resolves "
        f"{resolved.references} local references."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
