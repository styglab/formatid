from __future__ import annotations

from typing import Any

from fastapi import APIRouter

from services.context_platform.internal.context import build_runtime_context
from services.context_platform.internal.storage import ContextPlatformRepository


router = APIRouter()


@router.get("/health/ready")
def ready() -> dict[str, str]:
    return {"status": "ready"}


@router.get("/context")
def context() -> dict[str, object]:
    return build_runtime_context()


@router.get("/api/overview")
def overview() -> dict[str, Any]:
    return ContextPlatformRepository().overview()
