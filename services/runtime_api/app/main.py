from fastapi import FastAPI

from services.runtime_api.app.config import get_settings
from services.runtime_api.app.routers.checkpoints import router as checkpoints_router
from services.runtime_api.app.routers.dashboard import router as dashboard_router
from services.runtime_api.app.routers.health import router as health_router
from services.runtime_api.app.routers.logs import router as logs_router
from services.runtime_api.app.routers.observability import router as observability_router
from core.runtime.app_service.runtime.middleware import ServiceRequestMiddleware


settings = get_settings()
app = FastAPI(
    title=settings.app_name,
    version="0.1.0",
)
app.add_middleware(
    ServiceRequestMiddleware,
    service_name=settings.app_name,
    database_url=settings.checkpoint_database_url,
)
app.include_router(health_router)
app.include_router(checkpoints_router)
app.include_router(observability_router)
app.include_router(dashboard_router)
app.include_router(logs_router)


@app.get("/")
async def root() -> dict[str, str]:
    return {
        "service": settings.app_name,
        "status": "ok",
    }
