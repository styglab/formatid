from __future__ import annotations

from typing import Any

from fastapi import APIRouter

from services.semantic_platform.internal.context import build_runtime_context
from services.semantic_platform.internal.storage import SemanticLayerRepository


router = APIRouter()


@router.get("/health/ready")
def ready() -> dict[str, str]:
    return {"status": "ready"}


@router.get("/context")
def context() -> dict[str, object]:
    return build_runtime_context()


@router.get("/api/overview")
def overview() -> dict[str, Any]:
    return SemanticLayerRepository().overview()


@router.get("/semantic/catalog")
def semantic_catalog() -> dict[str, object]:
    return SemanticLayerRepository().semantic_catalog()


@router.post("/semantic-types/seed")
def seed_semantic_types() -> dict[str, object]:
    return SemanticLayerRepository().seed_semantic_type_registry()
