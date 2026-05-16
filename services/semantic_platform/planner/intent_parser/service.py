from __future__ import annotations

from typing import Any

from services.semantic_platform.planner.intent_parser.llm import parse_intent_with_llm


def parse_intent(
    query: str,
    runtime_context: dict[str, Any],
    manual_intent: dict[str, Any] | None = None,
) -> dict[str, Any]:
    fallback = catalog_metadata_fallback_intent(query, runtime_context)
    candidates = _candidate_context(runtime_context)
    llm_intent = parse_intent_with_llm(query, candidates, fallback, manual_intent)
    if llm_intent:
        return _sanitize_intent(query, llm_intent, runtime_context, "llm_first_intent_parser")
    return fallback


def catalog_metadata_fallback_intent(query: str, runtime_context: dict[str, Any]) -> dict[str, Any]:
    semantic_types = _names(runtime_context.get("semantic_types", []))
    entities = _names(runtime_context.get("entities", []))
    return {
        "query": query,
        "language": "unknown",
        "entities": entities,
        "semantic_types": semantic_types,
        "capabilities": _names(runtime_context.get("capabilities", [])),
        "semantic_arguments": {},
        "filters": [],
        "metrics": [],
        "constraints": [],
        "confidence": 0.3 if semantic_types or entities else 0.0,
        "parser": {
            "name": "catalog_metadata_fallback_parser",
            "llm_enabled": False,
        },
    }


def _sanitize_intent(
    query: str,
    intent: dict[str, Any],
    runtime_context: dict[str, Any],
    parser_name: str,
) -> dict[str, Any]:
    semantic_types = _ordered_values(intent.get("semantic_types", []))
    entities = _ordered_values(intent.get("entities", []))
    capabilities = _ordered_values(intent.get("capabilities", []))
    semantic_arguments = _semantic_arguments(intent.get("semantic_arguments", {}))
    filters = [
        item
        for item in intent.get("filters", [])
        if isinstance(item, dict)
    ]
    metrics = [str(item) for item in intent.get("metrics", []) if item]
    constraints = [item for item in intent.get("constraints", []) if isinstance(item, dict)]
    return {
        "query": query,
        "language": str(intent.get("language") or "unknown"),
        "entities": entities,
        "semantic_types": semantic_types,
        "capabilities": capabilities,
        "semantic_arguments": semantic_arguments,
        "filters": filters,
        "metrics": metrics,
        "constraints": constraints,
        "confidence": float(intent.get("confidence") or 0.7),
        "parser": {
            **(intent.get("parser") if isinstance(intent.get("parser"), dict) else {}),
            "name": parser_name,
        },
    }


def _candidate_context(runtime_context: dict[str, Any]) -> dict[str, Any]:
    return {
        "semantic_types": [
            {
                "name": item.get("name"),
                "description_ko": item.get("description_ko"),
                "entity": item.get("entity"),
                "aliases": item.get("aliases", []),
            }
            for item in runtime_context.get("semantic_types", [])
            if isinstance(item, dict)
        ],
        "entities": [
            {
                "name": item.get("name"),
                "description_ko": item.get("description_ko"),
                "aliases": item.get("aliases", []),
            }
            for item in runtime_context.get("entities", [])
            if isinstance(item, dict)
        ],
        "capabilities": [
            {
                "name": item.get("name"),
                "consumes": item.get("consumes", []),
                "produces": item.get("produces", []),
                "entities": item.get("entities", []),
                "relations": item.get("relations", []),
                "join_keys": item.get("join_keys", []),
                "description_ko": item.get("description_ko"),
            }
            for item in runtime_context.get("capabilities", [])
            if isinstance(item, dict)
        ],
    }


def _names(items: list[Any]) -> list[str]:
    return [str(item["name"]) for item in items if isinstance(item, dict) and item.get("name")]


def _allowed_semantic_types(runtime_context: dict[str, Any]) -> set[str]:
    names = set(_names(runtime_context.get("semantic_types", [])))
    for capability in runtime_context.get("capabilities", []):
        if not isinstance(capability, dict):
            continue
        names.update(str(item) for item in capability.get("consumes", []) if item)
        names.update(str(item) for item in capability.get("produces", []) if item)
    return names


def _ordered_values(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    result = []
    for value in values:
        text = str(value)
        if text and text not in result:
            result.append(text)
    return result


def _semantic_arguments(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    return {
        str(key): item
        for key, item in value.items()
        if str(key) and item not in (None, "")
    }
