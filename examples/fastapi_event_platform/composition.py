"""Consumer-owned FastAPI transaction and response mapping; no import-time app."""

from collections.abc import Awaitable, Callable

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from pytitect.core import JsonValue, OpaqueId
from pytitect.fastapi import idempotency_key_from_headers
from pytitect.idempotency import Conflict, IdempotencyScope, Replay, RequestFingerprint
from pytitect.operations import ReadinessPolicy, evaluate_readiness
from pytitect.sqlalchemy import RequestCommitted, SQLAlchemyIdempotentRequest


def build_app(
    *,
    requests: SQLAlchemyIdempotentRequest[JsonValue],
    mutate: Callable[[AsyncSession, JsonValue], Awaitable[JsonValue]],
    request_scope: Callable[[Request], IdempotencyScope],
    receipt_identity: Callable[[IdempotencyScope, str], OpaqueId[object]],
    readiness_policy: ReadinessPolicy,
) -> FastAPI:
    """The caller supplies authentication/scope, models, serializers and local mutation.

    ``mutate`` writes domain state and outbox through the provided session. It must
    not commit that session or perform external effects. HTTP mappings below are
    example application choices, not package protocol or authorization policy.
    """
    app = FastAPI(title="Synthetic event platform")

    @app.post("/operations")
    async def operation(request: Request) -> JSONResponse:
        scope = request_scope(request)
        key = idempotency_key_from_headers(request.headers).value
        payload = await request.json()
        result = await requests.execute(
            scope=scope,
            key=key,
            fingerprint=RequestFingerprint.from_json(payload),
            receipt_id=receipt_identity(scope, key),
            mutate=lambda session: mutate(session, payload),
        )
        if isinstance(result, RequestCommitted):
            return JSONResponse({"result": result.value}, status_code=201)
        if isinstance(result, Replay):
            return JSONResponse({"result": result.value})
        if isinstance(result, Conflict):
            return JSONResponse({"status": "conflict"}, status_code=409)
        return JSONResponse({"status": "pending", "key": key}, status_code=202)

    @app.post("/reconciliation")
    async def reconciliation(request: Request) -> JSONResponse:
        key = idempotency_key_from_headers(request.headers).value
        result = await requests.reconcile(
            scope=request_scope(request),
            key=key,
            fingerprint=RequestFingerprint.from_json(await request.json()),
        )
        if isinstance(result, Replay):
            return JSONResponse({"result": result.value})
        if isinstance(result, Conflict):
            return JSONResponse({"status": "conflict"}, status_code=409)
        return JSONResponse({"status": "uncertain", "key": key}, status_code=202)

    @app.get("/readiness")
    async def readiness() -> JSONResponse:
        report = await evaluate_readiness(readiness_policy)
        return JSONResponse({"ready": report.ready}, status_code=200 if report.ready else 503)

    return app
