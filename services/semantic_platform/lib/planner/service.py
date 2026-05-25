from __future__ import annotations

import json
import os
import hashlib
from typing import Any
from urllib import request
from urllib.error import HTTPError, URLError

from services.semantic_platform.lib.storage import SemanticCatalogRepository


LLM_MODES = {"disabled", "codex_manual", "openai"}
OPENAI_API_URL = os.getenv("SEMANTIC_PLATFORM_LLM_API_URL", "https://api.openai.com/v1/chat/completions")
OPENAI_MODEL = os.getenv("SEMANTIC_PLATFORM_LLM_MODEL", "gpt-4.1-mini")
LLM_TIMEOUT_SECONDS = float(os.getenv("SEMANTIC_PLATFORM_LLM_TIMEOUT_SECONDS", "20"))


def plan_execution(
    query: str,
    limit: int = 12,
    manual_plan: dict[str, Any] | None = None,
    repo: SemanticCatalogRepository | None = None,
) -> dict[str, Any]:
    repository = repo or SemanticCatalogRepository()
    catalog = repository.catalog()
    retrieved = repository.retrieve_capabilities(query, limit=limit)
    context = planner_context(catalog, limit, retrieved)
    mode = llm_mode()
    raw_plan = manual_plan if mode == "codex_manual" and isinstance(manual_plan, dict) else _call_llm(query, context, mode)
    plan = validate_plan(query, raw_plan, context, mode)
    _store_execution_graph(repository, plan, retrieved)
    return plan


def planner_context(
    catalog: dict[str, Any],
    limit: int = 12,
    retrieved: dict[str, Any] | None = None,
) -> dict[str, Any]:
    operation_contracts = catalog.get("operation_contracts", {})
    operation_variants = catalog.get("operation_variants", {})
    capabilities = catalog.get("capabilities", {})
    semantic_types = catalog.get("semantic_types", {})
    retrieved_ids = [
        str((match.get("document") or {}).get("capability_id") or "")
        for match in (retrieved or {}).get("matches", [])
        if isinstance(match, dict)
    ]
    selected_capabilities = [
        capabilities[capability_id]
        for capability_id in retrieved_ids
        if capability_id in capabilities
    ]
    if not selected_capabilities:
        selected_capabilities = list(capabilities.values())[:limit]
    selected_capability_ids = {
        str(capability.get("id") or capability.get("capability_id") or "")
        for capability in selected_capabilities
    }
    if selected_capability_ids:
        selected_variants = {
            variant_id: variant
            for variant_id, variant in operation_variants.items()
            if str(variant.get("capability_id") or variant.get("capability") or "") in selected_capability_ids
        }
        selected_operation_ids = {
            str(variant.get("operation_id") or "")
            for variant in selected_variants.values()
            if variant.get("operation_id")
        }
        selected_operation_ids.update(
            str(operation_id)
            for operation_id, contract in operation_contracts.items()
            if str(contract.get("capability_id") or contract.get("capability") or "") in selected_capability_ids
        )
        selected_contracts = {
            operation_id: contract
            for operation_id, contract in operation_contracts.items()
            if str(operation_id) in selected_operation_ids
        }
    else:
        selected_variants = operation_variants
        selected_contracts = operation_contracts
    return {
        "retrieval": retrieved or {"matches": []},
        "capabilities": selected_capabilities[:limit],
        "semantic_types": list(semantic_types.values())[: max(limit * 3, 30)],
        "operation_contracts": {
            operation_id: {
                "operation_id": operation_id,
                "capability": contract.get("capability_id"),
                "provider": contract.get("provider"),
                "resource_id": contract.get("resource_id"),
                "method": contract.get("method"),
                "path": contract.get("path"),
                "request": contract.get("request") or {},
                "response": contract.get("response") or {},
                "selectors": contract.get("selectors") or {},
            }
            for operation_id, contract in selected_contracts.items()
        },
        "operation_variants": {
            variant_id: {
                "variant_id": variant_id,
                "operation_id": variant.get("operation_id"),
                "capability": variant.get("capability_id"),
                "name": variant.get("name"),
                "fixed_semantic_arguments": variant.get("fixed_semantic_arguments") or {},
                "fixed_raw_arguments": variant.get("fixed_raw_arguments") or {},
                "verification": variant.get("verification") or {},
                "operation_contract": selected_contracts.get(str(variant.get("operation_id") or ""), {}),
            }
            for variant_id, variant in selected_variants.items()
        },
        "rules": [
            "Prefer variant_id values present in operation_variants.",
            "Each variant_id selects one physical operation_id plus fixed semantic/raw arguments.",
            "Use only operation_id values present in operation_contracts.",
            "Use semantic_arguments keys from semantic_types or operation contract request semantic_type values.",
            "For dependent calls, use argument_bindings from previous node semantic output to next node semantic input.",
            "Do not invent URLs, provider fields, service keys, or raw request parameters.",
            "Plan only with retrieved capabilities unless the context is empty.",
        ],
        "required_plan_shape": {
            "execution_graph": {
                "type": "dag",
                "status": "planned",
                "nodes": [
                    {
                        "id": "stable node id",
                        "capability": "capability id",
                        "variant_id": "approved operation variant id when available",
                        "operation_id": "approved operation id",
                        "call": {"semantic_arguments": {}},
                        "argument_bindings": {},
                        "post_filters": [],
                    }
                ],
            }
        },
    }


def validate_plan(query: str, raw_plan: dict[str, Any] | None, context: dict[str, Any], mode: str) -> dict[str, Any]:
    if not isinstance(raw_plan, dict):
        return {
            "query": query,
            "planner": {"mode": mode, "status": "not_planned"},
            "semantic_context": context,
            "execution_graph": {"type": "dag", "status": "not_planned", "nodes": [], "joins": []},
            "errors": ["llm_plan_missing"],
        }
    graph = raw_plan.get("execution_graph", raw_plan)
    nodes = graph.get("nodes", []) if isinstance(graph, dict) else []
    graph_status = str(graph.get("status") or "") if isinstance(graph, dict) else ""
    allowed_operations = context.get("operation_contracts", {})
    allowed_variants = context.get("operation_variants", {})
    allowed_capabilities = {
        str(capability.get("id") or capability.get("capability_id") or "")
        for capability in context.get("capabilities", [])
        if isinstance(capability, dict)
    }
    validated_nodes = []
    errors = []
    for index, node in enumerate(nodes):
        if not isinstance(node, dict):
            errors.append(f"node[{index}] is not an object")
            continue
        variant_id = str(node.get("variant_id") or "")
        variant = allowed_variants.get(variant_id) if variant_id else None
        operation_id = str(node.get("operation_id") or (variant or {}).get("operation_id") or "")
        contract = allowed_operations.get(operation_id)
        if not contract:
            errors.append(f"node[{index}] uses unknown operation_id: {operation_id}")
            continue
        if variant_id and not variant:
            errors.append(f"node[{index}] uses unknown variant_id: {variant_id}")
            continue
        capability = str(node.get("capability") or (variant or {}).get("capability") or contract.get("capability") or "")
        if allowed_capabilities and capability not in allowed_capabilities:
            errors.append(f"node[{index}] uses capability outside retrieved context: {capability}")
            continue
        if variant and capability != str(variant.get("capability") or ""):
            errors.append(f"node[{index}] capability does not match variant: {capability} != {variant.get('capability')}")
            continue
        call = node.get("call") if isinstance(node.get("call"), dict) else {}
        semantic_arguments = call.get("semantic_arguments") if isinstance(call.get("semantic_arguments"), dict) else {}
        fixed_semantic = (variant or {}).get("fixed_semantic_arguments") or {}
        semantic_arguments = {**fixed_semantic, **semantic_arguments}
        validated_nodes.append(
            {
                "id": str(node.get("id") or f"node_{index + 1}"),
                "capability": capability,
                "variant_id": variant_id or None,
                "operation_id": operation_id,
                "call": {"semantic_arguments": semantic_arguments},
                "argument_bindings": node.get("argument_bindings") if isinstance(node.get("argument_bindings"), dict) else {},
                "post_filters": node.get("post_filters") if isinstance(node.get("post_filters"), list) else [],
                "operation_contract": contract,
                "operation_variant": variant,
            }
        )
    if not validated_nodes and not errors and graph_status in {"not_found", "capability_not_found"}:
        planner_status = "not_found"
        execution_status = "not_found"
    else:
        planner_status = "valid" if validated_nodes and not errors else "invalid" if errors else "empty"
        execution_status = "planned" if validated_nodes and not errors else "invalid" if errors else "not_found"
    return {
        "query": query,
        "planner": {
            "mode": mode,
            "status": planner_status,
            "model": OPENAI_MODEL if mode == "openai" else None,
            "reason": graph.get("reason") if isinstance(graph, dict) else None,
        },
        "semantic_context": context,
        "execution_graph": {
            "type": "dag",
            "status": execution_status,
            "nodes": validated_nodes,
            "joins": graph.get("joins", []) if isinstance(graph, dict) and isinstance(graph.get("joins"), list) else [],
        },
        "errors": errors,
    }


def _call_llm(query: str, context: dict[str, Any], mode: str) -> dict[str, Any] | None:
    if mode != "openai":
        return None
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return None
    payload = {
        "model": OPENAI_MODEL,
        "temperature": 0,
        "response_format": {"type": "json_object"},
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are an execution planner for a public-data semantic runtime. "
                    "Return JSON only. Select approved operation_id values from operation_contracts. "
                    "Plan semantic arguments, post filters, and argument bindings."
                ),
            },
            {
                "role": "user",
                "content": json.dumps({"query": query, "planner_context": context}, ensure_ascii=False),
            },
        ],
    }
    try:
        http_request = request.Request(
            OPENAI_API_URL,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            method="POST",
        )
        with request.urlopen(http_request, timeout=LLM_TIMEOUT_SECONDS) as response:
            body = json.loads(response.read().decode("utf-8"))
        content = body["choices"][0]["message"]["content"]
        parsed = json.loads(content)
    except (HTTPError, URLError, TimeoutError, KeyError, IndexError, json.JSONDecodeError, ValueError):
        return None
    return parsed if isinstance(parsed, dict) else None


def _store_execution_graph(
    repository: SemanticCatalogRepository,
    plan: dict[str, Any],
    retrieved: dict[str, Any],
) -> None:
    graph = plan.get("execution_graph", {}) if isinstance(plan.get("execution_graph"), dict) else {}
    digest = hashlib.sha256(
        json.dumps({"query": plan.get("query"), "graph": graph}, ensure_ascii=False, default=str).encode("utf-8")
    ).hexdigest()[:16]
    graph_id = f"graph.{digest}"
    try:
        repository.upsert_execution_graph(
            {
                "id": graph_id,
                "query": str(plan.get("query") or ""),
                "graph": graph,
                "planner": plan.get("planner", {}),
                "retrieved_capabilities": retrieved.get("matches", []) if isinstance(retrieved, dict) else [],
                "errors": plan.get("errors", []),
                "status": graph.get("status", "planned"),
            }
        )
    except Exception:
        return


def llm_mode() -> str:
    mode = os.getenv("SEMANTIC_PLATFORM_LLM_MODE") or os.getenv("LLM_MODE") or "disabled"
    normalized = mode.strip().lower()
    return normalized if normalized in LLM_MODES else "disabled"
