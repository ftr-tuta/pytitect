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
from pathlib import Path
from typing import Any


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("lock", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    payload = args.lock.read_bytes()
    lock: dict[str, Any] = tomllib.loads(payload.decode())
    timestamp = datetime.fromtimestamp(int(os.environ.get("SOURCE_DATE_EPOCH", "0")), UTC)
    packages = []
    relationships = []
    for index, package in enumerate(lock.get("package", []), start=1):
        spdx_id = f"SPDXRef-Package-{index}"
        packages.append(
            {
                "SPDXID": spdx_id,
                "name": package["name"],
                "versionInfo": package["version"],
                "downloadLocation": "NOASSERTION",
                "filesAnalyzed": False,
                "licenseConcluded": "NOASSERTION",
                "licenseDeclared": "NOASSERTION",
            }
        )
        relationships.append(
            {
                "spdxElementId": "SPDXRef-DOCUMENT",
                "relationshipType": "DESCRIBES",
                "relatedSpdxElement": spdx_id,
            }
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
