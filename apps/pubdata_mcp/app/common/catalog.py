from __future__ import annotations

import os
from functools import lru_cache
from typing import Any

import requests


SEMANTIC_PLATFORM_API_URL = os.getenv("SEMANTIC_PLATFORM_API_URL", "http://semantic-platform-api:8000")
SEMANTIC_PLATFORM_PLANNER_API_URL = os.getenv(
    "SEMANTIC_PLATFORM_PLANNER_API_URL",
    "http://semantic-platform-planner-api:8000",
)
SEMANTIC_PLATFORM_API_TIMEOUT = float(os.getenv("SEMANTIC_PLATFORM_API_TIMEOUT", "2"))


def load_catalog() -> dict[str, Any]:
    return _get("/semantic/catalog", runtime=True) or {"core": {}, "domains": {}, "mappings": {}}


def semantic_plan_query(query: str, limit: int = 8, manual_plan: dict[str, Any] | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {"query": query, "limit": limit}
    if isinstance(manual_plan, dict):
        payload["manual_plan"] = manual_plan
    return _post("/semantic/planner/execution-plan", payload, runtime=True) or {
        "query": query,
        "planner": {"name": "unavailable"},
        "semantic_context": {},
        "execution_graph": {"type": "dag", "status": "not_found", "nodes": [], "joins": []},
    }


def semantic_query(
    query: str,
    limit: int = 8,
    execute: bool = False,
    manual_plan: dict[str, Any] | None = None,
) -> dict[str, Any]:
    plan = semantic_plan_query(query, limit, manual_plan)
    graph = plan.get("execution_graph", {})
    nodes = graph.get("nodes", []) if isinstance(graph, dict) else []
    execution_contracts = load_execution_contracts()
    implementations = _normalize_capability_implementations(execution_contracts.get("capability_implementations", {}))
    operation_contracts = execution_contracts.get("operation_contracts", {})
    operation_variants = execution_contracts.get("operation_variants", {})
    resources = execution_contracts.get("resources", {})
    readiness = []
    for node in nodes:
        if not isinstance(node, dict):
            continue
        capability = str(node.get("capability") or "")
        status = _execution_path_status(
            capability=capability,
            node=node,
            implementations=implementations,
            operation_contracts=operation_contracts if isinstance(operation_contracts, dict) else {},
            operation_variants=operation_variants if isinstance(operation_variants, dict) else {},
            resources=resources if isinstance(resources, dict) else {},
        )
        readiness.append(
            {
                "node": node.get("id"),
                "capability": capability,
                "variant_id": node.get("variant_id"),
                "operation_id": node.get("operation_id"),
                **status,
            }
        )

    execution = {
        "requested": execute,
        "status": "plan_only",
        "message": "semantic_query returned a semantic DAG and implementation readiness.",
    }
    if execute:
        from apps.pubdata_mcp.app.common.execution import execute_semantic_plan

        execution = execute_semantic_plan(plan, execution_contracts, load_catalog())
    execution_results = execution.get("results", []) if isinstance(execution, dict) and isinstance(execution.get("results"), list) else []
    selected_capabilities = [
        {
            "node": node.get("id"),
            "capability": node.get("capability"),
            "operation_id": node.get("operation_id"),
            "variant_id": node.get("variant_id"),
        }
        for node in nodes
        if isinstance(node, dict)
    ]

    return {
        "query": query,
        "status": graph.get("status", "not_found") if isinstance(graph, dict) else "not_found",
        "result_status": _semantic_query_result_status(execute, graph, execution_results),
        "selected_capabilities": selected_capabilities,
        "execution_graph": graph if isinstance(graph, dict) else {"type": "dag", "status": "not_found", "nodes": []},
        "results": execution_results,
        "errors": _semantic_query_errors(plan, execution_results),
        "evidence": _semantic_query_evidence(execution_results),
        "execution": execution,
        "plan": plan,
        "capability_readiness": readiness,
        "catalog_usage": {
            "source": "semantic_platform",
            "capability_implementations": sum(len(items) for items in implementations.values()),
            "operation_field_mappings": len(execution_contracts.get("operation_field_mappings", {})),
            "operation_contracts": len(execution_contracts.get("operation_contracts", {})),
            "operation_variants": len(execution_contracts.get("operation_variants", {})),
        },
    }


def semantic_smoke_test_operation(
    operation_id: str | None = None,
    semantic_arguments: dict[str, Any] | None = None,
    persist: bool = True,
    variant_id: str | None = None,
) -> dict[str, Any]:
    from apps.pubdata_mcp.app.common.execution import smoke_test_operation

    return smoke_test_operation(
        operation_id=operation_id or "",
        variant_id=variant_id,
        semantic_arguments=semantic_arguments or {},
        execution_contracts=load_execution_contracts(),
        catalog=load_catalog(),
        persist=persist,
    )


@lru_cache(maxsize=1)
def load_execution_contracts() -> dict[str, Any]:
    data = _get("/semantic/execution/contracts", runtime=True)
    if isinstance(data, dict):
        return {
            "capability_implementations": data.get("capability_implementations", {}),
            "operation_field_mappings": data.get("operation_field_mappings", {}),
            "operation_contracts": data.get("operation_contracts", {}),
            "operation_variants": data.get("operation_variants", {}),
            "resources": data.get("resources", {}),
        }
    return {
        "capability_implementations": {},
        "operation_field_mappings": {},
        "operation_contracts": {},
        "operation_variants": {},
        "resources": {},
    }


def record_endpoint_check(check: dict[str, Any]) -> dict[str, Any] | None:
    return _post("/semantic/execution/checks", check, runtime=True)


def _normalize_capability_implementations(implementations: Any) -> dict[str, list[dict[str, Any]]]:
    if not isinstance(implementations, dict):
        return {}
    return {
        str(name): [item for item in value if isinstance(item, dict)] if isinstance(value, list) else [value]
        for name, value in implementations.items()
        if isinstance(value, (list, dict))
    }


def _execution_path_status(
    *,
    capability: str,
    node: dict[str, Any],
    implementations: dict[str, list[dict[str, Any]]],
    operation_contracts: dict[str, Any],
    operation_variants: dict[str, Any],
    resources: dict[str, Any],
) -> dict[str, Any]:
    variant_id = str(node.get("variant_id") or "")
    variant = operation_variants.get(variant_id) if variant_id else {}
    operation_id = str(node.get("operation_id") or (variant or {}).get("operation_id") or "")
    contract = operation_contracts.get(operation_id) if operation_id else {}
    candidates = implementations.get(capability, [])
    matching_implementations = [
        item
        for item in candidates
        if (not operation_id or item.get("operation_id") == operation_id)
        and (not variant_id or item.get("variant_id") == variant_id)
    ]
    available_tools = [
        item
        for item in matching_implementations
        if item.get("status") == "available" and item.get("tool")
    ]
    resource_id = str((contract or {}).get("resource_id") or "")
    resource = resources.get(resource_id) if resource_id else {}
    generic_http_ready = bool(
        contract
        and isinstance(resource, dict)
        and resource.get("base_url")
        and contract.get("method")
        and contract.get("path")
    )
    if available_tools or generic_http_ready:
        return {
            "status": "executable",
            "execution_path": "registered_tool" if available_tools else "generic_http_contract",
            "implementation_count": len(matching_implementations),
            "has_operation_contract": bool(contract),
            "has_resource": bool(resource),
        }
    return {
        "status": "planned",
        "execution_path": "missing_contract_or_resource",
        "implementation_count": len(matching_implementations),
        "has_operation_contract": bool(contract),
        "has_resource": bool(resource),
    }


def _semantic_query_errors(plan: dict[str, Any], results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    errors = [
        {"type": "planner_error", "message": str(error)}
        for error in plan.get("errors", [])
        if error
    ]
    for result in results:
        if not isinstance(result, dict):
            continue
        if result.get("status") == "error":
            error = (result.get("result") or {}).get("error") if isinstance(result.get("result"), dict) else {}
            errors.append(
                {
                    "type": error.get("type") if isinstance(error, dict) else "execution_error",
                    "message": error.get("message") if isinstance(error, dict) else None,
                    "node": result.get("node"),
                    "capability": result.get("capability"),
                }
            )
        elif result.get("status") == "skipped":
            errors.append(
                {
                    "type": "execution_skipped",
                    "message": result.get("reason"),
                    "node": result.get("node"),
                    "capability": result.get("capability"),
                }
            )
    return errors


def _semantic_query_result_status(execute: bool, graph: Any, results: list[dict[str, Any]]) -> str:
    graph_status = graph.get("status") if isinstance(graph, dict) else None
    if not execute:
        return "plan_only"
    if not results:
        return "capability_not_found" if graph_status == "not_found" else "not_executed"

    statuses = [str(result.get("result_status") or result.get("status") or "") for result in results if isinstance(result, dict)]
    if not statuses:
        return "not_executed"
    error_statuses = {
        "provider_error",
        "timeout",
        "transport_error",
        "invalid_response",
        "execution_error",
        "validation_error",
        "not_executable",
    }
    has_success = any(status in {"executed_with_items", "executed_empty"} for status in statuses)
    has_error = any(status in error_statuses for status in statuses)
    if has_success and has_error:
        return "partial_success"
    if has_error:
        return statuses[0] if len(set(statuses)) == 1 else "execution_failed"
    if "executed_with_items" in statuses:
        return "executed_with_items"
    if "executed_empty" in statuses:
        return "executed_empty"
    return statuses[0]


def _semantic_query_evidence(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    evidence = []
    for result in results:
        raw_result = result.get("result") if isinstance(result, dict) else {}
        raw_evidence = raw_result.get("evidence") if isinstance(raw_result, dict) else None
        if isinstance(raw_evidence, dict):
            evidence.append(raw_evidence)
    return evidence


def _get(path: str, *, runtime: bool = False) -> dict[str, Any] | None:
    base_url = SEMANTIC_PLATFORM_PLANNER_API_URL if runtime else SEMANTIC_PLATFORM_API_URL
    try:
        response = requests.get(
            f"{base_url.rstrip('/')}{path}",
            timeout=SEMANTIC_PLATFORM_API_TIMEOUT,
        )
        response.raise_for_status()
        data = response.json()
    except requests.RequestException:
        return None
    return data if isinstance(data, dict) else None


def _post(path: str, payload: dict[str, Any], *, runtime: bool = False) -> dict[str, Any] | None:
    base_url = SEMANTIC_PLATFORM_PLANNER_API_URL if runtime else SEMANTIC_PLATFORM_API_URL
    try:
        response = requests.post(
            f"{base_url.rstrip('/')}{path}",
            json=payload,
            timeout=SEMANTIC_PLATFORM_API_TIMEOUT,
        )
        response.raise_for_status()
        data = response.json()
    except requests.RequestException:
        return None
    return data if isinstance(data, dict) else None
