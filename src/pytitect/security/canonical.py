"""I-JSON validation and optional RFC 8785 canonicalization."""

from __future__ import annotations

import json
import math
from typing import Any, cast

from pytitect.core import JsonValue

_MAX_SAFE_INTEGER = 9_007_199_254_740_991


def validate_ijson(value: JsonValue) -> None:
    def visit(item: JsonValue) -> None:
        if item is None or isinstance(item, bool):
            return
        if isinstance(item, int):
            if abs(item) > _MAX_SAFE_INTEGER:
                raise ValueError("I-JSON integers must be IEEE-754 interoperable")
            return
        if isinstance(item, float):
            if not math.isfinite(item):
                raise ValueError("I-JSON numbers must be finite")
            return
        if isinstance(item, str):
            if any(0xD800 <= ord(char) <= 0xDFFF for char in item):
                raise ValueError("I-JSON strings must not contain lone surrogates")
            return
        if isinstance(item, list):
            for child in item:
                visit(child)
            return
        if isinstance(item, dict):
            for key, child in item.items():
                if not isinstance(key, str):
                    raise ValueError("I-JSON object keys must be strings")
                visit(key)
                visit(child)
            return
        raise ValueError(f"unsupported I-JSON value: {type(item).__name__}")

    visit(value)


def parse_ijson(payload: bytes | str) -> JsonValue:
    def object_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        output: dict[str, Any] = {}
        for key, value in pairs:
            if key in output:
                raise ValueError(f"duplicate JSON member: {key}")
            output[key] = value
        return output

    def invalid_constant(value: str) -> None:
        raise ValueError(f"non-finite JSON number: {value}")

    loaded = json.loads(payload, object_pairs_hook=object_pairs, parse_constant=invalid_constant)
    value = cast(JsonValue, loaded)
    validate_ijson(value)
    return value


def canonical_json(value: JsonValue) -> bytes:
    validate_ijson(value)
    try:
        import rfc8785
    except ImportError as error:
        raise RuntimeError("install pytitect[canonical-json] for RFC 8785 support") from error
    try:
        return bytes(rfc8785.dumps(value))
    except (TypeError, ValueError, OverflowError) as error:
        raise ValueError(f"value cannot be canonicalized as RFC 8785: {error}") from error
