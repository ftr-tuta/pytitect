#!/usr/bin/env python3
"""Create a compact SPDX 2.3 JSON SBOM from uv.lock."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import tomllib
import uuid
from datetime import UTC, datetime
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any


def package_version(package: dict[str, Any]) -> str | None:
    """Resolve lockfile versions, including uv's unversioned editable root entry."""
    locked_version = package.get("version")
    if isinstance(locked_version, str):
        return locked_version
    package_name = package.get("name")
    if not isinstance(package_name, str):
        return None
    if package_name == "pytitect" and "editable" in package.get("source", {}):
        from pytitect.__about__ import __version__

        return __version__
    try:
        return version(package_name)
    except PackageNotFoundError:
        return None


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("lock", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    payload = args.lock.read_bytes()
    lock: dict[str, Any] = tomllib.loads(payload.decode())
    timestamp = datetime.fromtimestamp(int(os.environ.get("SOURCE_DATE_EPOCH", "0")), UTC)
    locked_packages = lock.get("package", [])
    identifiers = {
        package["name"]: f"SPDXRef-Package-{index}"
        for index, package in enumerate(locked_packages, start=1)
    }
    packages = []
    relationships = []
    root_id: str | None = None
    for index, package in enumerate(locked_packages, start=1):
        spdx_id = f"SPDXRef-Package-{index}"
        entry = {
            "SPDXID": spdx_id,
            "name": package["name"],
            "downloadLocation": "NOASSERTION",
            "filesAnalyzed": False,
            "licenseConcluded": "NOASSERTION",
            "licenseDeclared": "NOASSERTION",
        }
        resolved_version = package_version(package)
        if resolved_version is not None:
            entry["versionInfo"] = resolved_version
        packages.append(entry)
        if package["name"] == "pytitect":
            root_id = spdx_id
    if root_id is None:
        raise SystemExit("uv.lock does not contain the pytitect root package")
    relationships.append(
        {
            "spdxElementId": "SPDXRef-DOCUMENT",
            "relationshipType": "DESCRIBES",
            "relatedSpdxElement": root_id,
        }
    )
    dependency_relations: set[tuple[str, str]] = set()
    for package in locked_packages:
        source_id = identifiers[package["name"]]
        declared = list(package.get("dependencies", []))
        for values in package.get("optional-dependencies", {}).values():
            declared.extend(values)
        for dependency in declared:
            target_id = identifiers.get(dependency["name"])
            if target_id is not None:
                dependency_relations.add((source_id, target_id))
    relationships.extend(
        {
            "spdxElementId": source,
            "relationshipType": "DEPENDS_ON",
            "relatedSpdxElement": target,
        }
        for source, target in sorted(dependency_relations)
    )
    digest = hashlib.sha256(payload).hexdigest()
    document = {
        "spdxVersion": "SPDX-2.3",
        "dataLicense": "CC0-1.0",
        "SPDXID": "SPDXRef-DOCUMENT",
        "name": "pytitect-uv-lock",
        "documentNamespace": (
            f"https://github.com/ftr-tuta/pytitect/sbom/{uuid.uuid5(uuid.NAMESPACE_URL, digest)}"
        ),
        "creationInfo": {
            "created": timestamp.isoformat().replace("+00:00", "Z"),
            "creators": ["Tool: pytitect/tool/sbom.py"],
        },
        "packages": packages,
        "relationships": relationships,
    }
    args.output.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
