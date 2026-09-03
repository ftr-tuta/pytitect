"""Route-neutral OpenAPI 3.1 components for event-platform endpoints."""

from __future__ import annotations

from typing import Any


def event_platform_openapi_components() -> dict[str, Any]:
    return {
        "schemas": {
            "TitectMessageV1": {
                "type": "object",
                "additionalProperties": False,
                "required": [
                    "id",
                    "source",
                    "specversion",
                    "type",
                    "subject",
                    "time",
                    "dataschema",
                    "datacontenttype",
                    "profile",
                    "data",
                ],
                "properties": {
                    "id": {"type": "string"},
                    "source": {"type": "string"},
                    "specversion": {"const": "1.0"},
                    "type": {"type": "string"},
                    "subject": {"type": "string"},
                    "time": {"type": "string", "format": "date-time"},
                    "dataschema": {"type": "string"},
                    "datacontenttype": {"const": "application/json"},
                    "profile": {"const": "titect-message/1"},
                    "data": {},
                    "correlationid": {"type": "string"},
                    "causationid": {"type": "string"},
                },
            },
            "Problem": {
                "type": "object",
                "additionalProperties": True,
                "required": ["type", "title", "status"],
                "properties": {
                    "type": {"type": "string"},
                    "title": {"type": "string"},
                    "status": {"type": "integer", "minimum": 100, "maximum": 599},
                    "detail": {"type": "string"},
                    "instance": {"type": "string"},
                },
            },
        },
        "parameters": {
            "IdempotencyKey": {
                "name": "Idempotency-Key",
                "in": "header",
                "required": True,
                "schema": {"type": "string", "minLength": 1, "maxLength": 255},
            }
        },
    }
