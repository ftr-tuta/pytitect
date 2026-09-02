from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path


def test_sbom_resolves_unversioned_editable_root(tmp_path: Path) -> None:
    lock = tmp_path / "uv.lock"
    output = tmp_path / "pytitect.spdx.json"
    lock.write_text(
        "\n".join(
            (
                "version = 1",
                "",
                "[[package]]",
                'name = "dependency"',
                'version = "1.2.3"',
                "",
                "[[package]]",
                'name = "pytitect"',
                'source = { editable = "." }',
                'dependencies = [{ name = "dependency" }]',
            )
        )
    )
    environment = os.environ.copy()
    environment["SOURCE_DATE_EPOCH"] = "0"

    subprocess.run(
        [sys.executable, "tool/sbom.py", str(lock), str(output)],
        check=True,
        env=environment,
    )

    document = json.loads(output.read_text())
    versions = {package["name"]: package.get("versionInfo") for package in document["packages"]}
    assert versions == {"dependency": "1.2.3", "pytitect": "1.0.0rc1"}
    assert document["creationInfo"]["created"] == "1970-01-01T00:00:00Z"
    describes = [
        relation
        for relation in document["relationships"]
        if relation["relationshipType"] == "DESCRIBES"
    ]
    depends = [
        relation
        for relation in document["relationships"]
        if relation["relationshipType"] == "DEPENDS_ON"
    ]
    assert len(describes) == 1
    assert len(depends) == 1
