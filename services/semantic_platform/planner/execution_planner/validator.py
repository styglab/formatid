from __future__ import annotations

from typing import Any


def validate_plan(query: str, raw_plan: dict[str, Any] | None, context: dict[str, Any], mode: str) -> dict[str, Any]:
    if not isinstance(raw_plan, dict):
        return _empty_plan(query, mode, "llm_plan_not_available")

    operation_contracts = context.get("operation_contracts", {})
    graph = raw_plan.get("execution_graph", {}) if isinstance(raw_plan.get("execution_graph"), dict) else {}
    nodes = graph.get("nodes", []) if isinstance(graph.get("nodes"), list) else []
    validated_nodes = []
    warnings = []
    seen_nodes = set()

    for index, node in enumerate(nodes, start=1):
        if not isinstance(node, dict):
            warnings.append({"type": "invalid_node", "index": index})
            continue
        operation_id = str(node.get("operation_id") or "")
        contract = operation_contracts.get(operation_id)
        if not isinstance(contract, dict):
            warnings.append({"type": "unknown_operation_id", "node": node.get("id"), "operation_id": operation_id})
            continue
        node_id = str(node.get("id") or f"step_{index}")
        validated = {
            "id": node_id,
            "order": index,
            "operation_id": operation_id,
            "capability": str(node.get("capability") or contract.get("capability") or ""),
            "call": _call(node),
            "arguments": _call(node).get("semantic_arguments", {}),
            "post_filters": _list(node.get("post_filters")),
            "depends_on": [value for value in _list(node.get("depends_on")) if str(value) in seen_nodes],
            "argument_bindings": _bindings(node.get("argument_bindings"), seen_nodes),
            "produces": _list(node.get("produces")) or _operation_produces(contract),
            "operation_contract": {
                "provider": contract.get("provider"),
                "resource_id": contract.get("resource_id"),
                "method": contract.get("method"),
                "path": contract.get("path"),
                "request": contract.get("request", {}),
                "response": contract.get("response", {}),
            },
        }
        validated_nodes.append(validated)
        seen_nodes.add(node_id)

    integration = graph.get("integration") if isinstance(graph.get("integration"), dict) else None
    return {
        "query": query,
        "planner": {
            "name": "semantic_platform.llm_execution_planner",
            "version": "0.1.0",
            "mode": mode,
        },
        "intent": raw_plan.get("intent", {}) if isinstance(raw_plan.get("intent"), dict) else {},
        "semantic_context": context,
        "execution_graph": {
            "type": "dag",
            "status": "planned" if validated_nodes else "not_found",
            "nodes": validated_nodes,
            "integration": integration,
            "notes": [
                "Planner output is semantic-level. pubdata_mcp compiles operation_id and semantic arguments into physical provider calls.",
            ],
        },
        "validation": {
            "status": "valid" if validated_nodes and not warnings else ("partial" if validated_nodes else "invalid"),
            "warnings": warnings,
        },
    }


def _empty_plan(query: str, mode: str, reason: str) -> dict[str, Any]:
    return {
        "query": query,
        "planner": {
            "name": "semantic_platform.llm_execution_planner",
            "version": "0.1.0",
            "mode": mode,
        },
        "intent": {},
        "semantic_context": {},
        "execution_graph": {
            "type": "dag",
            "status": "not_planned",
            "nodes": [],
            "integration": None,
            "notes": ["No LLM execution plan was generated."],
        },
        "validation": {"status": "invalid", "warnings": [{"type": reason}]},
    }


def _call(node: dict[str, Any]) -> dict[str, Any]:
    call = node.get("call") if isinstance(node.get("call"), dict) else {}
    semantic_arguments = call.get("semantic_arguments")
    if not isinstance(semantic_arguments, dict):
        semantic_arguments = node.get("arguments") if isinstance(node.get("arguments"), dict) else {}
    return {"semantic_arguments": semantic_arguments}


def _bindings(value: Any, seen_nodes: set[str]) -> dict[str, dict[str, Any]]:
    if not isinstance(value, dict):
        return {}
    result = {}
    for semantic_type, binding in value.items():
        if not isinstance(binding, dict):
            continue
        from_node = str(binding.get("from_node") or "")
        if from_node not in seen_nodes:
            continue
        result[str(semantic_type)] = {
            "from_node": from_node,
            "semantic_type": str(binding.get("semantic_type") or semantic_type),
            "mode": str(binding.get("mode") or "single"),
            "deduplicate": bool(binding.get("deduplicate", True)),
        }
    return result


def _operation_produces(contract: dict[str, Any]) -> list[str]:
    response = contract.get("response", {}) if isinstance(contract.get("response"), dict) else {}
    values = []
    fields = response.get("fields", {}) if isinstance(response.get("fields"), dict) else {}
    values.extend(str(value) for value in fields.values() if value)
    extractors = response.get("extractors", {}) if isinstance(response.get("extractors"), dict) else {}
    for extractor in extractors.values():
        if not isinstance(extractor, dict):
            continue
        produces = extractor.get("produces", {})
        if isinstance(produces, dict):
            values.extend(str(value) for value in produces.values() if value)
    return list(dict.fromkeys(values))


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []
