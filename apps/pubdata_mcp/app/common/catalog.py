from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any

import requests
import yaml


SEMANTIC_PLATFORM_API_URL = os.getenv("SEMANTIC_PLATFORM_API_URL", "http://semantic-platform-api:8000")
SEMANTIC_PLATFORM_API_TIMEOUT = float(os.getenv("SEMANTIC_PLATFORM_API_TIMEOUT", "2"))
EXECUTION_CONTRACTS_PATH = Path(
    os.getenv(
        "SEMANTIC_PLATFORM_EXECUTION_CONTRACTS_PATH",
        "services/semantic_platform/catalog/execution/capability_implementations.yaml",
    )
)


def load_catalog() -> dict[str, Any]:
    return _get("/semantic/catalog") or {"core": {}, "domains": {}, "mappings": {}}


def semantic_resolve(query: str, limit: int = 10) -> dict[str, Any]:
    return _post("/semantic/resolve", {"query": query, "limit": limit}) or {"query": query, "matches": []}


def semantic_find_capabilities(
    entity: str | None = None,
    properties: list[str] | None = None,
    limit: int = 20,
) -> dict[str, Any]:
    return _post(
        "/semantic/capabilities/find",
        {"entity": entity, "properties": properties or [], "limit": limit},
    ) or {"entity": entity, "properties": properties or [], "capabilities": []}


def semantic_plan_join(from_entity: str, to_entity: str) -> dict[str, Any]:
    return _post(
        "/semantic/join/plan",
        {"from_entity": from_entity, "to_entity": to_entity},
    ) or {"from": from_entity, "to": to_entity, "join_paths": [], "status": "not_found"}


def semantic_get_context(query: str, limit: int = 8) -> dict[str, Any]:
    return _post("/runtime/context", {"query": query, "limit": limit}) or {
        "query": query,
        "runtime_context": {
            "semantic_types": [],
            "entities": [],
            "capabilities": [],
            "relations": [],
            "join_keys": [],
            "execution_hints": [],
        },
    }


def semantic_plan_query(query: str, limit: int = 8, manual_plan: dict[str, Any] | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {"query": query, "limit": limit}
    if isinstance(manual_plan, dict):
        payload["manual_plan"] = manual_plan
    return _post("/planner/plan", payload) or {
        "query": query,
        "planner": {"name": "unavailable"},
        "semantic_context": {},
        "execution_graph": {"type": "dag", "status": "not_found", "nodes": [], "joins": []},
    }


def semantic_parse_intent(
    query: str,
    limit: int = 8,
    manual_intent: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {"query": query, "limit": limit}
    if isinstance(manual_intent, dict):
        payload["manual_intent"] = manual_intent
    return _post("/planner/intent", payload) or {
        "query": query,
        "semantic_intent": {
            "entities": [],
            "semantic_types": [],
            "filters": [],
            "metrics": [],
            "constraints": [],
            "confidence": 0,
            "parser": {"name": "unavailable"},
        },
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
    readiness = []
    for node in nodes:
        if not isinstance(node, dict):
            continue
        capability = str(node.get("capability") or "")
        candidates = implementations.get(capability, [])
        available = [item for item in candidates if item.get("status") == "available" and item.get("tool")]
        readiness.append(
            {
                "node": node.get("id"),
                "capability": capability,
                "status": "available" if available else "planned",
                "implementation_count": len(available),
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

    return {
        "query": query,
        "status": graph.get("status", "not_found") if isinstance(graph, dict) else "not_found",
        "execution": execution,
        "plan": plan,
        "capability_readiness": readiness,
        "catalog_usage": {
            "source": "semantic_platform",
            "capability_implementations": sum(len(items) for items in implementations.values()),
            "operation_field_mappings": len(execution_contracts.get("operation_field_mappings", {})),
            "operation_contracts": len(execution_contracts.get("operation_contracts", {})),
        },
    }


@lru_cache(maxsize=1)
def load_execution_contracts() -> dict[str, Any]:
    data = _get("/semantic/execution/contracts")
    if isinstance(data, dict):
        return {
            "capability_implementations": data.get("capability_implementations", {}),
            "operation_field_mappings": data.get("operation_field_mappings", {}),
            "operation_contracts": data.get("operation_contracts", {}),
        }
    fallback = _load_execution_contracts_from_files()
    return fallback


def _load_execution_contracts_from_files() -> dict[str, Any]:
    if not EXECUTION_CONTRACTS_PATH.exists():
        return {"capability_implementations": {}, "operation_field_mappings": {}, "operation_contracts": {}}
    data = yaml.safe_load(EXECUTION_CONTRACTS_PATH.read_text(encoding="utf-8")) or {}
    operation_mappings_path = EXECUTION_CONTRACTS_PATH.parent / "operation_field_mappings.yaml"
    operation_mappings = {}
    if operation_mappings_path.exists():
        operation_document = yaml.safe_load(operation_mappings_path.read_text(encoding="utf-8")) or {}
        if isinstance(operation_document, dict):
            operation_mappings = operation_document.get("operation_field_mappings", {})
    operation_contracts_path = EXECUTION_CONTRACTS_PATH.parent / "operation_contracts.yaml"
    operation_contracts = {}
    if operation_contracts_path.exists():
        operation_contracts_document = yaml.safe_load(operation_contracts_path.read_text(encoding="utf-8")) or {}
        if isinstance(operation_contracts_document, dict):
            operation_contracts = operation_contracts_document.get("operation_contracts", {})
    return {
        "capability_implementations": data.get("capability_implementations", {}) if isinstance(data, dict) else {},
        "operation_field_mappings": operation_mappings,
        "operation_contracts": operation_contracts,
    }


def _normalize_capability_implementations(implementations: Any) -> dict[str, list[dict[str, Any]]]:
    if not isinstance(implementations, dict):
        return {}
    return {
        str(name): [item for item in value if isinstance(item, dict)]
        for name, value in implementations.items()
        if isinstance(value, list)
    }


def _get(path: str) -> dict[str, Any] | None:
    try:
        response = requests.get(
            f"{SEMANTIC_PLATFORM_API_URL.rstrip('/')}{path}",
            timeout=SEMANTIC_PLATFORM_API_TIMEOUT,
        )
        response.raise_for_status()
        data = response.json()
    except requests.RequestException:
        return None
    return data if isinstance(data, dict) else None


def _post(path: str, payload: dict[str, Any]) -> dict[str, Any] | None:
    try:
        response = requests.post(
            f"{SEMANTIC_PLATFORM_API_URL.rstrip('/')}{path}",
            json=payload,
            timeout=SEMANTIC_PLATFORM_API_TIMEOUT,
        )
        response.raise_for_status()
        data = response.json()
    except requests.RequestException:
        return None
    return data if isinstance(data, dict) else None
