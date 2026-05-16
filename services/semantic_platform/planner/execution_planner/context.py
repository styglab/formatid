from __future__ import annotations

from typing import Any

from services.semantic_platform.api.app.domain.service import load_catalog, runtime_context


def planner_context(query: str, limit: int = 12) -> dict[str, Any]:
    catalog = load_catalog()
    runtime = catalog.get("runtime", {})
    execution = catalog.get("execution", {})
    context = runtime_context(query, limit).get("runtime_context", {})
    candidate_capability_names = {
        str(item.get("name"))
        for item in context.get("capabilities", [])
        if isinstance(item, dict) and item.get("name")
    }
    candidate_capability_names = _expand_downstream_capabilities(
        runtime.get("capabilities", {}),
        candidate_capability_names,
    )
    operations = _operation_context(execution.get("operation_contracts", {}), candidate_capability_names)
    return {
        "semantic_types": _semantic_type_context(runtime.get("semantic_types", {}), context.get("semantic_types", [])),
        "entities": context.get("entities", []),
        "capabilities": _capability_context(runtime.get("capabilities", {}), candidate_capability_names),
        "operation_contracts": operations,
        "field_mappings": _field_mapping_context(execution.get("operation_field_mappings", {}), set(operations)),
        "runtime_context": context,
    }


def _semantic_type_context(all_types: dict[str, Any], ranked_types: list[dict[str, Any]]) -> list[dict[str, Any]]:
    names = [str(item.get("name")) for item in ranked_types if isinstance(item, dict) and item.get("name")]
    if not names:
        names = list(all_types)[:80]
    return [
        {
            "id": name,
            "description_ko": (all_types.get(name) or {}).get("description_ko"),
            "aliases": (all_types.get(name) or {}).get("aliases", []),
            "entity": (all_types.get(name) or {}).get("entity"),
        }
        for name in names
        if isinstance(all_types.get(name), dict)
    ]


def _capability_context(capabilities: dict[str, Any], candidate_names: set[str]) -> list[dict[str, Any]]:
    names = candidate_names or set(capabilities)
    return [
        {"id": name, **capability}
        for name, capability in capabilities.items()
        if name in names and isinstance(capability, dict)
    ]


def _expand_downstream_capabilities(capabilities: dict[str, Any], candidate_names: set[str]) -> set[str]:
    if not candidate_names:
        return candidate_names
    expanded = set(candidate_names)
    produced_types = set()
    for name in candidate_names:
        capability = capabilities.get(name)
        if isinstance(capability, dict):
            produced_types.update(str(value) for value in capability.get("produces", []) if value)
    for name, capability in capabilities.items():
        if not isinstance(capability, dict) or name in expanded:
            continue
        consumes = {str(value) for value in capability.get("consumes", []) if value}
        if produced_types & consumes:
            expanded.add(str(name))
    return expanded


def _operation_context(operation_contracts: dict[str, Any], candidate_capabilities: set[str]) -> dict[str, Any]:
    if not isinstance(operation_contracts, dict):
        return {}
    selected = {}
    for operation_id, contract in operation_contracts.items():
        if not isinstance(contract, dict):
            continue
        if candidate_capabilities and contract.get("capability") not in candidate_capabilities:
            continue
        selected[str(operation_id)] = {
            "capability": contract.get("capability"),
            "provider": contract.get("provider"),
            "resource_id": contract.get("resource_id"),
            "method": contract.get("method"),
            "path": contract.get("path"),
            "description_ko": contract.get("description_ko"),
            "selectors": contract.get("selectors", {}),
            "request": contract.get("request", {}),
            "response": contract.get("response", {}),
        }
    return selected


def _field_mapping_context(field_mappings: dict[str, Any], operation_ids: set[str]) -> dict[str, Any]:
    selected = {}
    for mapping_id, mapping in field_mappings.items():
        if not isinstance(mapping, dict):
            continue
        operation_id = str(mapping.get("operation_id") or "")
        if operation_id not in operation_ids and not any(operation_id.endswith(".*") and candidate.startswith(operation_id[:-1]) for candidate in operation_ids):
            continue
        selected[str(mapping_id)] = {
            "operation_id": operation_id,
            "direction": mapping.get("direction"),
            "field_name": mapping.get("field_name"),
            "semantic_type": mapping.get("semantic_type"),
        }
    return selected
