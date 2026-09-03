from __future__ import annotations

import json
from typing import Any

from django.http import HttpRequest, HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt

from pytitect.django.transactions import TransactionalOperationCommitted
from pytitect.idempotency import Conflict, InProgress, Replay, Uncertain
from reference_app.service import execute_mutation


def _error(message: str, *, status: int = 400) -> JsonResponse:
    return JsonResponse({"error": message}, status=status)


def _decode_request(request: HttpRequest) -> tuple[str, int] | JsonResponse:
    if len(request.body) > 16_384:
        return _error("request body exceeds 16384 bytes", status=413)
    try:
        value: Any = json.loads(request.body)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return _error("request body must be valid JSON")
    if not isinstance(value, dict) or set(value) != {"idempotency_key", "value"}:
        return _error("request body fields must be exactly idempotency_key and value")
    key = value["idempotency_key"]
    number = value["value"]
    if not isinstance(key, str) or not key or len(key) > 255:
        return _error("idempotency_key must be a non-empty string of at most 255 characters")
    if isinstance(number, bool) or not isinstance(number, int):
        return _error("value must be an integer")
    return key, number


def _mutation_boundary(request: HttpRequest, mutation_id: str) -> JsonResponse:
    if not mutation_id or len(mutation_id) > 255:
        return _error("mutation_id must contain at most 255 characters")
    decoded = _decode_request(request)
    if isinstance(decoded, JsonResponse):
        return decoded
    key, value = decoded
    outcome = execute_mutation(
        mutation_id=mutation_id,
        idempotency_key=key,
        value=value,
    )
    if isinstance(outcome, TransactionalOperationCommitted):
        return JsonResponse({"state": "applied", "value": outcome.value})
    if isinstance(outcome, Replay):
        return JsonResponse({"state": "replayed", "value": outcome.value})
    if isinstance(outcome, Conflict):
        return JsonResponse({"state": "conflict", "value": None}, status=409)
    if isinstance(outcome, InProgress):
        return JsonResponse({"state": "processing", "value": None}, status=409)
    if isinstance(outcome, Uncertain):
        return JsonResponse({"state": "uncertain", "value": None}, status=409)
    return _error("transactional compare-and-set failed", status=409)


@csrf_exempt
def legacy_mutation(request: HttpRequest, mutation_id: str) -> HttpResponse:
    if request.method != "POST":
        return _error("only POST is supported", status=405)
    return _mutation_boundary(request, mutation_id)


@csrf_exempt
def sync_mutation(request: HttpRequest, mutation_id: str) -> HttpResponse:
    if request.method != "POST":
        return _error("only POST is supported", status=405)
    return _mutation_boundary(request, mutation_id)
