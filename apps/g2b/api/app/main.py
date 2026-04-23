from __future__ import annotations

from fastapi import FastAPI

from apps.g2b.api.app.api.v1.routers.g2b import router as g2b_router
from apps.g2b.api.app.core.config import get_settings
from core.runtime.app_service.runtime.middleware import ServiceRequestMiddleware


app = FastAPI(title="g2b-api", version="0.1.0")
settings = get_settings()
app.add_middleware(
    ServiceRequestMiddleware,
    service_name="g2b-api",
    database_url=settings.checkpoint_database_url,
)
app.include_router(g2b_router)


@app.get("/health/live")
async def live() -> dict[str, str]:
    return {"status": "ok"}
