from __future__ import annotations

from typing import Any


def build_not_found_plan(reason: str = "capability_not_found") -> dict[str, Any]:
    return {
        "planner": {"status": "not_found", "reason": reason},
        "execution_graph": {"type": "dag", "status": "not_found", "nodes": []},
        "errors": [{"code": reason}],
    }


def validate_plan(plan: dict[str, Any], approved_operation_ids: set[str] | None = None) -> dict[str, Any]:
    errors: list[dict[str, str]] = []
    if not isinstance(plan.get("planner"), dict):
        errors.append({"code": "missing_planner"})
    graph = plan.get("execution_graph")
    if not isinstance(graph, dict):
        errors.append({"code": "missing_execution_graph"})
    elif graph.get("type") != "dag":
        errors.append({"code": "unsupported_graph_type"})

    if approved_operation_ids is not None:
        for node in graph.get("nodes", []) if isinstance(graph, dict) else []:
            if not isinstance(node, dict):
                continue
            operation_id = node.get("operation_id")
            if operation_id and operation_id not in approved_operation_ids:
                errors.append({"code": "unapproved_operation_id", "operation_id": str(operation_id)})

    return {"valid": not errors, "errors": errors}
