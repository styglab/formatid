from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from services.semantic_platform.adapters.admin_api.app.catalog import (
    router as catalog_router,
)
from services.semantic_platform.adapters.admin_api.app.execution import (
    router as execution_router,
)
from services.semantic_platform.adapters.admin_api.app.onboarding import (
    router as onboarding_router,
)
from services.semantic_platform.adapters.admin_api.app.system import (
    router as system_router,
)
from services.semantic_platform.internal.storage import SemanticLayerRepository


app = FastAPI(title="Semantic Platform Admin API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://127.0.0.1:8018",
        "http://localhost:8018",
    ],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(system_router)
app.include_router(onboarding_router)
app.include_router(execution_router)
app.include_router(catalog_router)


@app.on_event("startup")
def ensure_control_plane_schema() -> None:
    SemanticLayerRepository().ensure_control_plane_schema()
