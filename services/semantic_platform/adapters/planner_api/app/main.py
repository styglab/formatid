from __future__ import annotations

from typing import Any

from fastapi import FastAPI

from services.semantic_platform.internal.context import build_runtime_context
from services.semantic_platform.internal.planner import (
    build_execution_plan,
    load_execution_contracts,
    record_endpoint_check as record_endpoint_check_service,
    validate_plan,
)


app = FastAPI(title="Semantic Platform Planner API")


@app.get("/health/ready")
def ready() -> dict[str, str]:
    return {"status": "ready"}


@app.get("/runtime-context")
def runtime_context() -> dict[str, Any]:
    return build_runtime_context()


@app.post("/plan")
def plan(payload: dict[str, Any]) -> dict[str, Any]:
    return build_execution_plan(payload)


@app.post("/semantic/planner/execution-plan")
def semantic_execution_plan(payload: dict[str, Any]) -> dict[str, Any]:
    return build_execution_plan(payload)


@app.get("/semantic/execution/contracts")
def execution_contracts() -> dict[str, Any]:
    return load_execution_contracts()


@app.post("/semantic/execution/checks")
def record_endpoint_check(payload: dict[str, Any]) -> dict[str, Any]:
    return record_endpoint_check_service(payload)


@app.post("/plans/validate")
def validate(payload: dict[str, Any]) -> dict[str, Any]:
    approved = set(payload.get("approved_operation_ids", []))
    return validate_plan(payload.get("plan", {}), approved_operation_ids=approved)
