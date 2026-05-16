from __future__ import annotations

import json
import os
from typing import Any
from urllib import request
from urllib.error import HTTPError, URLError

from services.semantic_platform.ingestion.catalog.context_loader import _compact_catalog_context
from services.semantic_platform.ingestion.chunking.source_chunking import _compact_source_chunks_for_prompt
from services.semantic_platform.ingestion.graphs.source_ingestion.config import (
    LLM_MODES,
    SOURCE_LLM_API_URL,
    SOURCE_LLM_MAX_TEXT_CHARS,
    SOURCE_LLM_MODEL,
    SOURCE_LLM_TIMEOUT_SECONDS,
)
from services.semantic_platform.ingestion.graphs.source_ingestion.state import SourceGraphState

def analyze_source_with_llm(state: SourceGraphState) -> SourceGraphState:
    llm_mode = _llm_mode()
    if llm_mode == "disabled":
        _set_empty_llm_result(state, "llm_disabled")
        return state
    if llm_mode == "codex_manual":
        parsed = state.get("manual_llm_response")
        if not parsed:
            _set_empty_llm_result(state, "codex_manual_required")
            return state
        if not isinstance(parsed, dict):
            _set_empty_llm_result(state, "codex_manual_invalid_shape")
            return state
        _set_parsed_llm_result(state, parsed, result_source="codex_manual_payload")
        return state

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        _set_empty_llm_result(state, "missing_OPENAI_API_KEY_for_openai_mode")
        return state

    parsed = _call_source_llm(
        api_key=api_key,
        provider=state["provider"],
        document_id=state["document_id"],
        source_sha256=state["sha256"],
        extracted_text=state["extracted_text"],
        source_chunks=state.get("source_chunks", []),
        catalog_context=state["catalog_context"],
    )
    if not parsed:
        _set_empty_llm_result(state, "llm_call_failed")
        return state

    _set_parsed_llm_result(state, parsed, result_source="openai")
    return state


def _set_parsed_llm_result(state: SourceGraphState, parsed: dict[str, Any], result_source: str) -> None:
    state["structured_spec"] = _with_source_identity(
        parsed.get("structured_spec", {}),
        state,
    )
    state["semantic_platform_proposal"] = _with_source_identity(
        parsed.get("semantic_platform_proposal", {}),
        state,
        target="semantic_platform",
    )
    state["execution_contract_proposal"] = _with_source_identity(
        parsed.get("execution_contract_proposal", {}),
        state,
        target="semantic_platform.execution_contracts",
    )
    _enrich_catalog_changes(state)
    state["messages"].append(
        f"analyze_source_with_llm:ok:{result_source}:"
        f"operations={len(state['structured_spec'].get('operations', []))},"
        f"semantic_types={len(state['semantic_platform_proposal'].get('changes', {}).get('semantic_types', {}))},"
        f"field_mappings={len(state['execution_contract_proposal'].get('changes', {}).get('operation_field_mappings', {}))}"
    )


def _set_empty_llm_result(state: SourceGraphState, reason: str) -> None:
    base = {
        "document_id": state["document_id"],
        "provider": state["provider"],
        "source_path": state["source_path"],
        "source_sha256": state["sha256"],
        "chunks_path": state.get("chunks_path"),
    }
    state["structured_spec"] = {**base, "operations": [], "status": "not_generated", "reason": reason}
    state["semantic_platform_proposal"] = {
        "target": "semantic_platform",
        **base,
        "changes": {
            "semantic_types": {},
            "entities": {},
            "relations": {},
            "capabilities": {},
            "resources": {},
            "crosswalks": {},
        },
        "status": "not_generated",
        "reason": reason,
    }
    state["execution_contract_proposal"] = {
        "target": "semantic_platform.execution_contracts",
        **base,
        "changes": {"capability_implementations": {}, "operation_field_mappings": {}},
        "status": "not_generated",
        "reason": reason,
    }
    state["messages"].append(f"analyze_source_with_llm:skipped:{reason}")


def _call_source_llm(
    api_key: str,
    provider: str,
    document_id: str,
    source_sha256: str,
    extracted_text: str,
    source_chunks: list[dict[str, Any]],
    catalog_context: dict[str, Any],
) -> dict[str, Any] | None:
    prompt_payload = {
        "task": "Analyze a public API specification and generate reviewable proposals.",
        "provider_hint": provider if provider != "unknown" else None,
        "document_id": document_id,
        "source_sha256": source_sha256,
        "rules": [
            "Return JSON only.",
            "Do not rely on the source file name to identify the provider.",
            "Infer provider candidates from document content only, such as title, base URL, service name, organization names, and official metadata.",
            "If the provider is uncertain, use provider='unknown' and include provider_candidates with evidence.",
            "Read the extracted source text and infer operations, request fields, response fields, endpoints, methods, auth, pagination, and examples.",
            "Use catalog_context to reuse existing semantic types and capabilities when appropriate.",
            "Create new semantic type candidates only when the catalog has no suitable existing semantic type.",
            "semantic_platform_proposal must contain provider-neutral semantics only: semantic_types, entities if needed, relations if needed, capabilities.",
            "execution_contract_proposal may contain approved-contract candidates for runtime execution: resource ids, operation ids, provider ids, paths, methods, raw operation fields, field mappings, auth/pagination hints.",
            "pubdata_mcp consumes reviewed execution contracts later; do not ask pubdata_mcp to own proposals or catalog mutations.",
            "Do not directly apply changes. Mark proposals pending_review.",
        ],
        "catalog_context": _compact_catalog_context(catalog_context),
        "source_chunks": _compact_source_chunks_for_prompt(source_chunks),
        "extracted_text": extracted_text[:SOURCE_LLM_MAX_TEXT_CHARS],
        "required_json_shape": {
            "structured_spec": {
                "document_id": document_id,
                "provider": "provider id or unknown",
                "provider_candidates": [{"id": "string", "name_ko": "string", "evidence": "string"}],
                "base_url": "string|null",
                "operations": [
                    {
                        "operation_id": "provider-neutral or documented operation id",
                        "title_ko": "string",
                        "method": "GET|POST|PUT|DELETE|UNKNOWN",
                        "path": "string|null",
                        "request_fields": [
                            {
                                "name": "raw field name",
                                "location": "query|path|header|body|response|unknown",
                                "required": True,
                                "type": "string|number|integer|boolean|array|object|unknown",
                                "description_ko": "string",
                                "example": "string|null",
                            }
                        ],
                        "response_fields": [
                            {
                                "name": "raw field name",
                                "path": "response path if known",
                                "type": "string|number|integer|boolean|array|object|unknown",
                                "description_ko": "string",
                                "example": "string|null",
                            }
                        ],
                    }
                ],
            },
            "semantic_platform_proposal": {
                "target": "semantic_platform",
                "changes": {
                    "semantic_types": {
                        "semantic_type_id": {
                            "description_ko": "string",
                            "entity": "Organization|Person|Location|Contract|Document|Measurement|Identifier|Event|unknown",
                            "aliases": ["string"],
                            "existing": True,
                            "evidence": {"raw_fields": ["string"], "source_text": "short evidence"},
                        }
                    },
                    "capabilities": {
                        "capability_id": {
                            "consumes": ["semantic_type_id"],
                            "produces": ["semantic_type_id"],
                            "entities": ["Organization"],
                            "relations": ["relation ids"],
                            "join_keys": ["semantic_type_id"],
                            "description_ko": "string",
                            "evidence": {"operation_ids": ["string"], "source_text": "short evidence"},
                        }
                    },
                    "entities": {
                        "entity_id": {
                            "semantic_types": ["semantic_type_id"],
                            "capabilities": ["capability_id"],
                            "description_ko": "string",
                        }
                    },
                    "relations": {
                        "relation_id": {
                            "source": "entity_id",
                            "predicate": "HAS|IDENTIFIED_BY|LOCATED_IN|string",
                            "target": "entity_id",
                            "capabilities": ["capability_id"],
                        }
                    },
                    "resources": {
                        "resource_id": {
                            "provider": "provider id or unknown",
                            "base_url": "string|null",
                            "source_path": "string",
                            "operations": [{"operation_id": "string", "method": "string", "path": "string|null"}],
                            "capabilities": ["capability_id"],
                        }
                    },
                    "crosswalks": {
                        "crosswalk_id": {
                            "source": "operation_id.direction.raw_field",
                            "target": "semantic_type_id",
                            "relation": "maps_to",
                            "resource_id": "string",
                            "capability": "capability_id",
                        }
                    },
                },
                "status": "pending_review",
            },
            "execution_contract_proposal": {
                "target": "semantic_platform.execution_contracts",
                "changes": {
                    "capability_implementations": {
                        "capability_id": {
                            "operation_id": "globally unique operation id, e.g. nts.business_registration.status",
                            "resource_id": "API service/resource id, e.g. nts.business_registration",
                            "provider": "provider id or unknown",
                            "path": "string|null",
                            "method": "GET|POST|UNKNOWN",
                            "auth": "string|null",
                            "pagination": "object|null",
                            "status": "proposed",
                        }
                    },
                    "operation_field_mappings": {
                        "operation_id.direction.raw_field": {
                            "capability": "capability_id",
                            "operation_id": "globally unique operation id",
                            "resource_id": "API service/resource id",
                            "provider": "provider id or unknown",
                            "field_name": "raw field",
                            "direction": "request|response",
                            "semantic_type": "semantic_type_id",
                            "evidence": {"description_ko": "string", "source_text": "short evidence"},
                        }
                    },
                },
                "status": "pending_review",
            },
        },
    }
    payload = {
        "model": SOURCE_LLM_MODEL,
        "temperature": 0,
        "response_format": {"type": "json_object"},
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are a semantic catalog mapper for public-data API specs. "
                    "Your job is to produce reviewable proposals, not to apply changes. "
                    "Keep planning semantics provider-neutral. Put provider dialect in semantic_platform execution-contract proposals "
                    "so pubdata_mcp can consume reviewed contracts at runtime."
                ),
            },
            {"role": "user", "content": json.dumps(prompt_payload, ensure_ascii=False)},
        ],
    }
    try:
        body = json.dumps(payload).encode("utf-8")
        http_request = request.Request(
            SOURCE_LLM_API_URL,
            data=body,
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            method="POST",
        )
        with request.urlopen(http_request, timeout=SOURCE_LLM_TIMEOUT_SECONDS) as response:
            response_body = response.read().decode("utf-8")
        document = json.loads(response_body)
        content = document["choices"][0]["message"]["content"]
        parsed = json.loads(content)
    except (HTTPError, URLError, TimeoutError, KeyError, IndexError, json.JSONDecodeError, ValueError):
        return None
    return parsed if isinstance(parsed, dict) else None


def _enrich_catalog_changes(state: SourceGraphState) -> None:
    semantic_proposal = state.get("semantic_platform_proposal", {})
    execution_proposal = state.get("execution_contract_proposal", {})
    semantic_changes = semantic_proposal.setdefault("changes", {})
    execution_changes = execution_proposal.setdefault("changes", {})

    semantic_types = semantic_changes.setdefault("semantic_types", {})
    capabilities = semantic_changes.setdefault("capabilities", {})
    if not semantic_changes.get("entities"):
        semantic_changes["entities"] = _derive_entities(semantic_types, capabilities)
    if not semantic_changes.get("relations"):
        semantic_changes["relations"] = _derive_relations(capabilities)
    if not semantic_changes.get("resources"):
        semantic_changes["resources"] = _derive_resources(
            state=state,
            capabilities=capabilities,
            capability_implementations=execution_changes.get("capability_implementations", {}),
        )
    operation_field_mappings = execution_changes.get("operation_field_mappings") or execution_changes.get(
        "provider_field_mappings",
        {},
    )
    if not semantic_changes.get("crosswalks"):
        semantic_changes["crosswalks"] = _derive_crosswalks(operation_field_mappings)


def _derive_entities(semantic_types: dict[str, Any], capabilities: dict[str, Any]) -> dict[str, Any]:
    entities: dict[str, dict[str, Any]] = {}
    for semantic_type, payload in semantic_types.items():
        if not isinstance(payload, dict):
            continue
        entity = str(payload.get("entity") or "").strip()
        if not entity or entity.lower() == "unknown":
            continue
        row = entities.setdefault(entity, {"semantic_types": [], "capabilities": [], "identifiers": []})
        row["semantic_types"].append(semantic_type)
        if entity in set(payload.get("identifies", []) or []) or payload.get("join_priority") == "high":
            row["identifiers"].append(semantic_type)
    for capability, payload in capabilities.items():
        if not isinstance(payload, dict):
            continue
        for entity in payload.get("entities", []) or []:
            entity_name = str(entity).strip()
            if not entity_name:
                continue
            row = entities.setdefault(entity_name, {"semantic_types": [], "capabilities": [], "identifiers": []})
            row["capabilities"].append(capability)
    return {
        name: {
            "description_ko": f"{name} runtime entity inferred from public API source semantics.",
            "semantic_types": sorted(set(value["semantic_types"])),
            "capabilities": sorted(set(value["capabilities"])),
            "identifiers": sorted(set(value["identifiers"])),
        }
        for name, value in sorted(entities.items())
    }


def _derive_relations(capabilities: dict[str, Any]) -> dict[str, Any]:
    relations: dict[str, dict[str, Any]] = {}
    for capability, payload in capabilities.items():
        if not isinstance(payload, dict):
            continue
        for relation_id in payload.get("relations", []) or []:
            relation_name = str(relation_id).strip()
            if not relation_name:
                continue
            row = relations.setdefault(relation_name, {**_parse_relation_id(relation_name), "capabilities": []})
            row["capabilities"].append(capability)
    return {
        name: {**value, "capabilities": sorted(set(value.get("capabilities", [])))}
        for name, value in sorted(relations.items())
    }


def _parse_relation_id(relation_id: str) -> dict[str, str]:
    known_predicates = ("IDENTIFIED_BY", "LOCATED_IN", "MEASURED_BY", "OBSERVED_AT", "HAS", "USES", "REQUIRES")
    for predicate in known_predicates:
        marker = f"_{predicate}_"
        if marker in relation_id:
            source, target = relation_id.split(marker, 1)
            return {"source": source, "predicate": predicate, "target": target}
    parts = relation_id.split("_")
    if len(parts) >= 3:
        return {"source": parts[0], "predicate": "_".join(parts[1:-1]), "target": parts[-1]}
    return {"source": "unknown", "predicate": relation_id, "target": "unknown"}


def _derive_resources(
    state: SourceGraphState,
    capabilities: dict[str, Any],
    capability_implementations: dict[str, Any],
) -> dict[str, Any]:
    resources: dict[str, dict[str, Any]] = {}
    operation_lookup = {
        str(operation.get("operation_id")): operation
        for operation in state.get("structured_spec", {}).get("operations", [])
        if isinstance(operation, dict) and operation.get("operation_id")
    }
    for capability, proposed in capability_implementations.items():
        items = proposed if isinstance(proposed, list) else [proposed]
        for item in items:
            if not isinstance(item, dict):
                continue
            resource_id = str(item.get("resource_id") or item.get("provider") or state.get("document_id"))
            operation_id = str(item.get("operation_id") or "")
            operation = operation_lookup.get(operation_id, {})
            row = resources.setdefault(
                resource_id,
                {
                    "provider": item.get("provider") or state.get("provider") or "unknown",
                    "base_url": state.get("structured_spec", {}).get("base_url"),
                    "source_path": state.get("source_path"),
                    "source_sha256": state.get("sha256"),
                    "capabilities": [],
                    "operations": [],
                },
            )
            row["capabilities"].append(str(capability))
            row["operations"].append(
                {
                    "operation_id": operation_id,
                    "method": item.get("method") or operation.get("method") or "UNKNOWN",
                    "path": item.get("path") or operation.get("path"),
                    "title_ko": operation.get("title_ko"),
                }
            )
    for capability, payload in capabilities.items():
        if not isinstance(payload, dict):
            continue
        if capability not in capability_implementations:
            resource_id = state.get("document_id", "unknown")
            row = resources.setdefault(
                resource_id,
                {
                    "provider": state.get("provider") or "unknown",
                    "base_url": state.get("structured_spec", {}).get("base_url"),
                    "source_path": state.get("source_path"),
                    "source_sha256": state.get("sha256"),
                    "capabilities": [],
                    "operations": [],
                },
            )
            row["capabilities"].append(str(capability))
    return {
        name: {
            **value,
            "capabilities": sorted(set(value["capabilities"])),
            "operations": _unique_operations(value["operations"]),
        }
        for name, value in sorted(resources.items())
    }


def _unique_operations(operations: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen = set()
    rows = []
    for operation in operations:
        operation_id = operation.get("operation_id")
        if operation_id in seen:
            continue
        seen.add(operation_id)
        rows.append(operation)
    return rows


def _derive_crosswalks(operation_field_mappings: dict[str, Any]) -> dict[str, Any]:
    crosswalks: dict[str, Any] = {}
    for mapping_id, payload in operation_field_mappings.items():
        if not isinstance(payload, dict):
            continue
        semantic_type = payload.get("semantic_type")
        if not semantic_type:
            continue
        source = ".".join(
            str(value)
            for value in (payload.get("operation_id"), payload.get("direction"), payload.get("field_name"))
            if value
        )
        crosswalks[str(mapping_id)] = {
            "source": source or str(mapping_id),
            "target": semantic_type,
            "relation": "maps_to",
            "resource_id": payload.get("resource_id"),
            "capability": payload.get("capability"),
            "provider": payload.get("provider"),
            "field_name": payload.get("field_name"),
            "direction": payload.get("direction"),
        }
    return crosswalks


def _llm_mode() -> str:
    mode = os.getenv("SEMANTIC_PLATFORM_LLM_MODE") or os.getenv("LLM_MODE") or "disabled"
    normalized = mode.strip().lower()
    return normalized if normalized in LLM_MODES else "disabled"


def _with_source_identity(
    value: Any,
    state: SourceGraphState,
    target: str | None = None,
) -> dict[str, Any]:
    document = value if isinstance(value, dict) else {}
    if target:
        document["target"] = target
    document["document_id"] = state["document_id"]
    document["provider"] = state["provider"]
    document["source_path"] = state["source_path"]
    document["source_sha256"] = state["sha256"]
    document.setdefault("status", "pending_review")
    return document
