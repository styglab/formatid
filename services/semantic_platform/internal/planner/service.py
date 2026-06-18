from __future__ import annotations

import re
from typing import Any

from services.semantic_platform.internal.planner.contracts import (
    build_execution_contract_catalog,
)
from services.semantic_platform.internal.storage import SemanticLayerRepository


def build_not_found_plan(reason: str = "capability_not_found") -> dict[str, Any]:
    return {
        "planner": {"status": "not_found", "reason": reason},
        "execution_graph": {"type": "dag", "status": "not_found", "nodes": []},
        "errors": [{"code": reason}],
    }


def build_runtime_context_payload(repository: SemanticLayerRepository | None = None) -> dict[str, Any]:
    repo = repository or SemanticLayerRepository()
    contracts = build_execution_contract_catalog(repo)
    semantic_types = repo.list_semantic_types(status="approved")
    return {
        "service": "semantic_platform",
        "context_model": "entity_aspect_relationship",
        "planner_reads": "approved_context_only",
        "counts": {
            "semantic_types": len(semantic_types),
            "capabilities": len(contracts["capabilities"]),
            "operation_contracts": len(contracts["operation_contracts"]),
            "operation_variants": len(contracts["operation_variants"]),
            "resources": len(contracts["resources"]),
        },
        "capability_catalog": [
            {
                "id": item["id"],
                "capability_key": item.get("capability_key"),
                "name": item.get("name"),
                "description": item.get("description"),
                "input_semantic_types": item.get("input_semantic_types") or [],
                "output_semantic_types": item.get("output_semantic_types") or [],
            }
            for item in contracts["capabilities"].values()
        ],
    }


def load_execution_contracts(repository: SemanticLayerRepository | None = None) -> dict[str, Any]:
    repo = repository or SemanticLayerRepository()
    return build_execution_contract_catalog(repo)


def build_execution_plan(payload: dict[str, Any], repository: SemanticLayerRepository | None = None) -> dict[str, Any]:
    repo = repository or SemanticLayerRepository()
    contracts = load_execution_contracts(repo)
    query = _extract_query_text(payload)
    if not query:
        return build_not_found_plan("empty_query")

    capabilities = list(contracts["capabilities"].values())
    ranked = sorted(
        (
            {
                "capability": capability,
                "score": _score_capability_match(query, capability),
            }
            for capability in capabilities
        ),
        key=lambda item: item["score"],
        reverse=True,
    )
    ranked = [item for item in ranked if item["score"] > 0]
    if not ranked:
        return build_not_found_plan()

    selected = ranked[0]
    capability = selected["capability"]
    bindings = contracts["capability_implementations"].get(capability["id"], [])
    if not bindings:
        return build_not_found_plan("operation_contract_not_found")
    binding = bindings[0]
    variant_id = binding["variant_ids"][0] if binding.get("variant_ids") else None

    node = {
        "id": f"node_{capability['id']}",
        "capability_id": capability["id"],
        "capability_key": capability.get("capability_key") or capability["id"],
        "operation_id": binding["operation_id"],
        "variant_id": variant_id,
        "semantic_arguments": payload.get("semantic_arguments") if isinstance(payload.get("semantic_arguments"), dict) else {},
        "input_text": query,
    }
    return {
        "planner": {
            "status": "ready",
            "mode": "deterministic_retrieval",
            "selected_capability_id": capability["id"],
            "selected_operation_id": binding["operation_id"],
            "confidence": min(round(selected["score"] / 10, 2), 0.99),
        },
        "execution_graph": {
            "type": "dag",
            "status": "ready",
            "nodes": [node],
            "edges": [],
        },
        "retrieval": {
            "query": query,
            "candidates": [
                {
                    "capability_id": item["capability"]["id"],
                    "capability_key": item["capability"].get("capability_key") or item["capability"]["id"],
                    "score": item["score"],
                }
                for item in ranked[:5]
            ],
        },
        "errors": [],
    }


def record_endpoint_check(payload: dict[str, Any], repository: SemanticLayerRepository | None = None) -> dict[str, Any]:
    repo = repository or SemanticLayerRepository()
    check = repo.record_access_path_check(payload)
    return {"status": "recorded", "check": check}


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


def _extract_query_text(payload: dict[str, Any]) -> str:
    for key in ("query", "question", "input_text", "text"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    user_input = payload.get("input")
    if isinstance(user_input, dict):
        for key in ("query", "question", "text"):
            value = user_input.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return ""


def _score_capability_match(query: str, capability: dict[str, Any]) -> int:
    query_tokens = _tokenize(query)
    haystack = " ".join(
        str(value or "")
        for value in [
            capability.get("capability_key"),
            capability.get("name"),
            capability.get("description"),
            " ".join(str(item) for item in capability.get("input_semantic_types") or []),
            " ".join(str(item) for item in capability.get("output_semantic_types") or []),
        ]
    )
    candidate_tokens = _tokenize(haystack)
    overlap = query_tokens.intersection(candidate_tokens)
    score = len(overlap) * 3
    name = str(capability.get("name") or "").lower()
    key = str(capability.get("capability_key") or "").lower()
    query_lower = query.lower()
    if name and name in query_lower:
        score += 4
    if key and key in query_lower:
        score += 4
    return score


def _tokenize(value: str) -> set[str]:
    return {token for token in re.split(r"[^0-9A-Za-z_]+", value.lower()) if len(token) > 1}
