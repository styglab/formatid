from __future__ import annotations

from fastapi import FastAPI

from apps.spec_rag.api.app.api.v1.routers.spec_rag import router as spec_rag_router
from apps.spec_rag.api.app.core.config import get_settings
from core.runtime.app_service.runtime.middleware import ServiceRequestMiddleware


settings = get_settings()
app = FastAPI(title="spec-rag-api", version="0.1.0")
app.add_middleware(
    ServiceRequestMiddleware,
    service_name="spec-rag-api",
    database_url=settings.checkpoint_database_url,
)
app.include_router(spec_rag_router)


@app.get("/health/live")
async def live() -> dict[str, str]:
    return {"status": "live"}


@app.get("/health/ready")
async def ready() -> dict[str, str]:
    return {"status": "ready"}
