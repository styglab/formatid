from __future__ import annotations

from typing import Any

from services.semantic_platform.planner.execution_planner.context import planner_context
from services.semantic_platform.planner.execution_planner.llm import generate_execution_plan, llm_mode
from services.semantic_platform.planner.execution_planner.validator import validate_plan


def plan_execution(query: str, limit: int = 12, manual_plan: dict[str, Any] | None = None) -> dict[str, Any]:
    context = planner_context(query, limit)
    mode = llm_mode()
    raw_plan = manual_plan if mode == "codex_manual" and isinstance(manual_plan, dict) else generate_execution_plan(query, context)
    return validate_plan(query, raw_plan, context, mode)
