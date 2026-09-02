"""DRF fields that reject coercion and bound structured input."""

from __future__ import annotations

import json
import uuid
from decimal import Decimal
from typing import Any

from rest_framework import serializers

from pytitect.core import Limits, validate_json


class ClosedSerializer(serializers.Serializer[Any]):
    """Serializer that rejects unknown keys; nested closed serializers do the same recursively."""

    def to_internal_value(self, data: Any) -> Any:
        if not isinstance(data, dict):
            self.fail("invalid")
        unknown = set(data) - set(self.fields)
        if unknown:
            raise serializers.ValidationError(
                {name: ["Unknown field."] for name in sorted(unknown)}, code="unknown"
            )
        return super().to_internal_value(data)


class StrictCharField(serializers.CharField):
    def to_internal_value(self, data: Any) -> str:
        if type(data) is not str:
            self.fail("invalid")
        return super().to_internal_value(data)


class StrictIntegerField(serializers.IntegerField):
    def to_internal_value(self, data: Any) -> int:
        if type(data) is not int:
            self.fail("invalid")
        return super().to_internal_value(data)


class StrictBooleanField(serializers.BooleanField):
    def to_internal_value(self, data: Any) -> bool:
        if type(data) is not bool:
            self.fail("invalid")
        return super().to_internal_value(data)


class StrictUUIDField(serializers.UUIDField):
    def to_internal_value(self, data: Any) -> uuid.UUID:
        if type(data) is not str:
            self.fail("invalid")
        parsed = super().to_internal_value(data)
        if str(parsed) != data.lower():
            self.fail("invalid")
        return parsed


class StrictDecimalField(serializers.DecimalField):
    def to_internal_value(self, data: Any) -> Decimal:
        if type(data) is not str:
            self.fail("invalid")
        return super().to_internal_value(data)


class StrictListField(serializers.ListField):
    def __init__(self, *args: Any, max_length: int = 1_000, **kwargs: Any) -> None:
        if max_length <= 0:
            raise ValueError("max_length must be positive")
        super().__init__(*args, max_length=max_length, **kwargs)

    def to_internal_value(self, data: Any) -> list[Any]:
        if type(data) is not list:
            self.fail("not_a_list", input_type=type(data).__name__)
        return super().to_internal_value(data)


class BoundedJSONField(serializers.JSONField):
    def __init__(self, *args: Any, limits: Limits | None = None, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._limits = limits or Limits()

    def to_internal_value(self, data: Any) -> Any:
        try:
            encoded = json.dumps(data, ensure_ascii=False, allow_nan=False).encode()
            if len(encoded) > self._limits.max_body_bytes:
                raise ValueError("JSON encoding exceeds max_body_bytes")
            validate_json(data, limits=self._limits)
        except (TypeError, ValueError) as error:
            raise serializers.ValidationError("Invalid JSON value.", code="invalid") from error
        return super().to_internal_value(data)
