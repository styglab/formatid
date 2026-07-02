from __future__ import annotations

from typing import Any

from services.context_platform.internal.planner import build_runtime_context_payload


def build_runtime_context() -> dict[str, Any]:
    return build_runtime_context_payload()
