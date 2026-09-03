"""Synthetic FastAPI composition root; importing it does not construct an app."""

from fastapi import FastAPI

from pytitect.fastapi import event_platform_openapi_components


def build_app() -> FastAPI:
    app = FastAPI(title="Synthetic event platform")

    @app.get("/readiness")
    async def readiness() -> dict[str, bool]:
        return {"ready": True}

    generated = app.openapi

    def openapi() -> dict[str, object]:
        document = generated()
        document.setdefault("components", {}).update(event_platform_openapi_components())
        return document

    app.openapi = openapi
    return app
