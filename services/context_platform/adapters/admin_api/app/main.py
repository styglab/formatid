from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from services.context_platform.adapters.admin_api.app.context_platform import (
    router as context_platform_router,
)
from services.context_platform.adapters.admin_api.app.system import (
    router as system_router,
)
from services.context_platform.internal.storage import ContextPlatformRepository


app = FastAPI(title="Context Platform Admin API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://127.0.0.1:8081",
        "http://localhost:8081",
    ],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(system_router)
app.include_router(context_platform_router)


@app.on_event("startup")
def ensure_control_plane_schema() -> None:
    ContextPlatformRepository().ensure_schema()
