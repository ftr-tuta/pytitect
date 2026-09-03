from __future__ import annotations

import json
from typing import Any


def build_openapi() -> dict[str, Any]:
    sync_schema = "../../interop/titect-sync/1/schema.json"
    mutation_request = {
        "type": "object",
        "additionalProperties": False,
        "required": ["idempotency_key", "value"],
        "properties": {
            "idempotency_key": {"type": "string", "minLength": 1, "maxLength": 255},
            "value": {"type": "integer"},
        },
    }
    mutation_response = {
        "type": "object",
        "additionalProperties": False,
        "required": ["state", "value"],
        "properties": {
            "state": {"enum": ["applied", "replayed", "conflict", "processing", "uncertain"]},
            "value": {"type": ["object", "null"]},
        },
    }

    def operation(summary: str) -> dict[str, Any]:
        return {
            "summary": summary,
            "parameters": [
                {
                    "name": "mutation_id",
                    "in": "path",
                    "required": True,
                    "schema": {"type": "string", "minLength": 1, "maxLength": 255},
                }
            ],
            "requestBody": {
                "required": True,
                "content": {
                    "application/json": {"schema": {"$ref": "#/components/schemas/MutationRequest"}}
                },
            },
            "responses": {
                "200": {
                    "description": "Mutation completed or replayed.",
                    "content": {
                        "application/json": {
                            "schema": {"$ref": "#/components/schemas/MutationResponse"}
                        }
                    },
                },
                "409": {"description": "Mutation identity is busy, conflicting, or uncertain."},
            },
        }

    return {
        "openapi": "3.1.0",
        "info": {"title": "Synthetic Pytitect reference", "version": "titect-sync/1"},
        "paths": {
            "/reference/legacy/mutations/{mutation_id}": {
                "post": operation("Legacy boundary over the shared mutation service")
            },
            "/reference/sync/1/mutations/{mutation_id}": {
                "post": operation("titect-sync/1 boundary over the shared mutation service")
            },
        },
        "components": {
            "schemas": {
                "MutationRequest": mutation_request,
                "MutationResponse": mutation_response,
                "SyncDocument": {"$ref": sync_schema},
                "BootstrapRequest": {"$ref": f"{sync_schema}#/$defs/bootstrapRequestPayload"},
                "BootstrapResponse": {"$ref": f"{sync_schema}#/$defs/bootstrapResponsePayload"},
                "MutationOutcome": {"$ref": f"{sync_schema}#/$defs/mutationOutcomePayload"},
            }
        },
    }


def main() -> None:
    print(json.dumps(build_openapi(), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
