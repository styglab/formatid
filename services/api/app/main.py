from fastapi import FastAPI

from services.api.app.config import get_settings
from services.api.app.routers.checkpoints import router as checkpoints_router
from services.api.app.routers.health import router as health_router


settings = get_settings()
app = FastAPI(
    title=settings.app_name,
    version="0.1.0",
)
app.include_router(health_router)
app.include_router(checkpoints_router)


@app.get("/")
async def root() -> dict[str, str]:
    return {
        "service": settings.app_name,
        "status": "ok",
    }
