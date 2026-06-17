from __future__ import annotations

from typing import Any


def build_runtime_context() -> dict[str, Any]:
    return {
        "service": "semantic_platform",
        "context_model": "entity_aspect_relationship",
        "planner_reads": "approved_context_only",
    }
