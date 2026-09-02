from __future__ import annotations

import json
from pathlib import Path

import pytest

from pytitect.contracts import (
    ContractAccepted,
    ContractManifest,
    ExactVersionPolicy,
    LocalRefResolver,
    MissingCapabilities,
    MissingVersion,
    ProtocolDescriptor,
    RefRejected,
    ResolvedDocument,
    ResolverLimits,
    VersionMismatch,
)
from pytitect.contracts.spectacular import problem_response_schema


def test_exact_version_and_capability_decisions() -> None:
    descriptor = ProtocolDescriptor("example", "2", frozenset({"receipts"}))
    policy = ExactVersionPolicy(descriptor)
    assert policy.decide(None) == MissingVersion("2")
    assert policy.decide("1") == VersionMismatch("2", "1")
    assert policy.decide("2", required_capabilities=frozenset({"leases"})) == MissingCapabilities(
        frozenset({"leases"})
    )
    assert policy.decide("2") == ContractAccepted(descriptor)
    with pytest.raises(ValueError):
        ProtocolDescriptor("", "1")


def test_manifest_is_deterministic_and_confined(tmp_path: Path) -> None:
    (tmp_path / "z.json").write_text('{"z":1}')
    (tmp_path / "a.yaml").write_text("a: 1\n")
    first = ContractManifest.from_tree(tmp_path)
    second = ContractManifest.from_paths(tmp_path, ["z.json", "a.yaml"])
    assert first == second
    assert first.digest == second.digest
    assert first.to_dict()["algorithm"] == "sha256"
    outside = tmp_path.parent / "outside.json"
    outside.write_text("{}")
    with pytest.raises(ValueError):
        ContractManifest.from_paths(tmp_path, [outside])


def test_resolver_internal_external_pointer_and_siblings(tmp_path: Path) -> None:
    (tmp_path / "defs.json").write_text(
        json.dumps({"defs": {"a/b": {"type": "string"}, "til~de": {"type": "integer"}}})
    )
    (tmp_path / "root.json").write_text(
        json.dumps(
            {
                "$defs": {"local": {"type": "boolean"}},
                "properties": {
                    "local": {"$ref": "#/$defs/local", "description": "kept"},
                    "slash": {"$ref": "defs.json#/defs/a~1b"},
                    "tilde": {"$ref": "defs.json#/defs/til~0de"},
                },
            }
        )
    )
    result = LocalRefResolver(tmp_path).resolve(Path("root.json"))
    assert isinstance(result, ResolvedDocument)
    assert result.references == 3
    assert result.value["properties"]["local"] == {  # type: ignore[index]
        "type": "boolean",
        "description": "kept",
    }


@pytest.mark.parametrize(
    ("document", "expected"),
    [
        ({"$ref": "https://example.invalid/schema.json"}, "network_ref"),
        ({"$ref": "/tmp/schema.json"}, "absolute_path"),
        ({"$ref": "../outside.json"}, "root_escape"),
        ({"$ref": "#/missing"}, "missing_pointer"),
        ({"$ref": "#/loop", "loop": {"$ref": "#"}}, "cycle"),
    ],
)
def test_resolver_rejections(tmp_path: Path, document: object, expected: str) -> None:
    (tmp_path.parent / "outside.json").write_text("{}")
    (tmp_path / "root.json").write_text(json.dumps(document))
    result = LocalRefResolver(tmp_path).resolve(Path("root.json"))
    assert isinstance(result, RefRejected)
    assert result.code == expected


def test_resolver_symlink_depth_reference_and_byte_limits(tmp_path: Path) -> None:
    outside = tmp_path.parent / "secret.json"
    outside.write_text("{}")
    (tmp_path / "escape.json").symlink_to(outside)
    (tmp_path / "root.json").write_text('{"$ref":"escape.json"}')
    result = LocalRefResolver(tmp_path).resolve(Path("root.json"))
    assert isinstance(result, RefRejected) and result.code == "root_escape"

    (tmp_path / "root.json").write_text('{"x":1}')
    tiny = LocalRefResolver(tmp_path, limits=ResolverLimits(max_total_bytes=1)).resolve(
        Path("root.json")
    )
    assert isinstance(tiny, RefRejected) and tiny.code == "byte_limit"

    (tmp_path / "root.json").write_text(
        '{"properties":{"a":{"$ref":"#/value"},"b":{"$ref":"#/value"}},"value":1}'
    )
    too_many = LocalRefResolver(tmp_path, limits=ResolverLimits(max_references=1)).resolve(
        Path("root.json")
    )
    assert isinstance(too_many, RefRejected) and too_many.code == "reference_limit"

    (tmp_path / "broken.yaml").write_text("value: [unterminated")
    malformed = LocalRefResolver(tmp_path).resolve(Path("broken.yaml"))
    assert isinstance(malformed, RefRejected) and malformed.code == "malformed_document"
    with pytest.raises(ValueError):
        ResolverLimits(max_depth=0)


def test_problem_schema_is_not_registered_globally() -> None:
    schema = problem_response_schema()
    assert schema["required"] == ["type", "title", "status"]
