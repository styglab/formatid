from __future__ import annotations

import os
from typing import Any

import requests


CONTEXT_PLATFORM_PLANNER_API_URL = os.getenv(
    "CONTEXT_PLATFORM_PLANNER_API_URL",
    "http://context-platform-planner-api:8000",
)
CONTEXT_PLATFORM_API_TIMEOUT = float(os.getenv("CONTEXT_PLATFORM_API_TIMEOUT", "2"))


def load_catalog() -> dict[str, Any]:
    return _get("/runtime-context") or {"service": "context_platform", "status": "unavailable"}


def plan_request(
    query: str,
    canonical_inputs: dict[str, Any] | None = None,
    limit: int = 8,
) -> dict[str, Any]:
    return _post(
        "/planner/plan",
        {
            "query": query,
            "canonical_inputs": canonical_inputs or {},
            "limit": limit,
        },
    ) or _unavailable("planner_unavailable")


def execute_plan(plan_id: str) -> dict[str, Any]:
    return _post("/planner/execute", {"plan_id": plan_id}) or _unavailable("planner_unavailable")


def explain_plan(plan_id: str | None = None, plan: dict[str, Any] | None = None) -> dict[str, Any]:
    resolved = plan if isinstance(plan, dict) else None
    if resolved is None and plan_id:
        resolved = _get(f"/planner/plans/{plan_id}")
    if not isinstance(resolved, dict):
        return _unavailable("plan_not_found")
    return {
        "plan_id": resolved.get("plan_id") or resolved.get("id") or plan_id,
        "status": (resolved.get("planner") or {}).get("status") or resolved.get("status"),
        "selected_capability_id": resolved.get("selected_capability_id"),
        "selected_source_operation_id": resolved.get("selected_source_operation_id"),
        "canonical_inputs": resolved.get("canonical_inputs") or {},
        "parameter_bindings": resolved.get("parameter_bindings") or [],
        "expected_outputs": resolved.get("expected_outputs") or [],
        "confidence": resolved.get("confidence"),
        "requires_confirmation": resolved.get("requires_confirmation"),
        "validation": resolved.get("validation") or resolved.get("validation_result") or {},
    }


def search_capabilities(query: str = "", status: str = "approved") -> dict[str, Any]:
    data = _get("/planner/capabilities", params={"query": query, "status": status})
    return {"capabilities": data if isinstance(data, list) else []}


def get_capability(capability_id: str) -> dict[str, Any]:
    return _get(f"/planner/capabilities/{capability_id}") or _unavailable("capability_not_found")


def get_canonical_model() -> dict[str, Any]:
    return _get("/planner/canonical-model") or _unavailable("canonical_model_unavailable")


def get_operation_bindings(source_operation_id: str) -> dict[str, Any]:
    data = _get("/planner/operation-bindings", params={"source_operation_id": source_operation_id})
    return {"source_operation_id": source_operation_id, "bindings": data if isinstance(data, list) else []}


def _get(path: str, params: dict[str, Any] | None = None) -> Any:
    try:
        response = requests.get(
            f"{CONTEXT_PLATFORM_PLANNER_API_URL.rstrip('/')}{path}",
            params=params or {},
            timeout=CONTEXT_PLATFORM_API_TIMEOUT,
        )
        response.raise_for_status()
        return response.json()
    except requests.RequestException:
        return None


def _post(path: str, payload: dict[str, Any]) -> dict[str, Any] | None:
    try:
        response = requests.post(
            f"{CONTEXT_PLATFORM_PLANNER_API_URL.rstrip('/')}{path}",
            json=payload,
            timeout=CONTEXT_PLATFORM_API_TIMEOUT,
        )
        response.raise_for_status()
        data = response.json()
    except requests.RequestException:
        return None
    return data if isinstance(data, dict) else None


def _unavailable(code: str) -> dict[str, Any]:
    return {"status": "error", "errors": [{"code": code}]}
