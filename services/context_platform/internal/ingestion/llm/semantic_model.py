from __future__ import annotations

from typing import Any


def build_manual_semantic_model_request(
    *,
    run_id: str,
    source: dict[str, Any],
    operations: list[dict[str, Any]],
    fields: list[dict[str, Any]],
    semantic_types: list[dict[str, Any]],
    canonical_class_slot_usages: list[dict[str, Any]],
    retrieved_candidates: list[dict[str, Any]],
) -> dict[str, Any]:
    operation_index = {str(item.get("id") or ""): item for item in operations}
    candidates_by_field = {
        str(item.get("field_id") or ""): item.get("registry_candidates") or []
        for item in retrieved_candidates
        if isinstance(item, dict)
    }
    return {
        "type": "semantic_model_drafting",
        "run_id": run_id,
        "source": {
            "id": source.get("id"),
            "name": source.get("name"),
            "source_type": source.get("source_type"),
            "provider": source.get("provider"),
        },
        "instructions": [
            "Review source evidence clusters and decide whether each field should reuse an existing semantic type or propose a new one.",
            "Use semantic_type_id only when an existing registry concept is clearly the right meaning.",
            "When no reliable existing concept fits, leave semantic_type_id null and provide semantic_type_name as a proposed new concept name.",
            "Also propose canonical class and canonical class-slot usage names for semantic-model authoring.",
        ],
        "registry_snapshot": {
            "semantic_types": [
                {
                    "id": item.get("id"),
                    "name": item.get("name"),
                    "aliases": item.get("aliases") or [],
                    "description": item.get("description") or "",
                }
                for item in semantic_types
            ],
            "canonical_class_slot_usages": [
                {
                    "id": item.get("id"),
                    "name": item.get("name"),
                    "class_id": item.get("class_id"),
                    "semantic_type_id": item.get("semantic_type_id"),
                    "description": item.get("description") or "",
                }
                for item in canonical_class_slot_usages
            ],
        },
        "fields": [
            {
                "field_id": field.get("id"),
                "operation_id": field.get("operation_id"),
                "operation_name": operation_index.get(str(field.get("operation_id") or ""), {}).get("name"),
                "raw_name": field.get("raw_name"),
                "field_path": field.get("field_path"),
                "scope": field.get("scope"),
                "data_type": field.get("data_type"),
                "description": field.get("description") or "",
                "evidence": field.get("evidence") or [],
                "registry_candidates": candidates_by_field.get(str(field.get("id") or ""), []),
            }
            for field in fields
        ],
        "response_contract": {
            "suggestions": [
                {
                    "field_id": "string",
                    "operation_id": "string",
                    "field_path": "string",
                    "raw_name": "string",
                    "semantic_type_id": "string|null",
                    "semantic_type_name": "string",
                    "proposed_canonical_class_name": "string",
                    "proposed_canonical_class_slot_usage_name": "string",
                    "confidence": "float",
                    "rationale": "string",
                    "evidence_refs": ["list of evidence ref objects"],
                    "status": "matched_existing|proposed_new",
                }
            ]
        },
    }


def normalize_manual_semantic_model_response(payload: dict[str, Any]) -> dict[str, Any]:
    suggestions = payload.get("suggestions") if isinstance(payload.get("suggestions"), list) else []
    normalized: list[dict[str, Any]] = []
    for item in suggestions:
        if not isinstance(item, dict):
            continue
        normalized.append(
            {
                "field_id": str(item.get("field_id") or ""),
                "operation_id": str(item.get("operation_id") or ""),
                "field_path": str(item.get("field_path") or ""),
                "raw_name": str(item.get("raw_name") or ""),
                "semantic_type_id": str(item.get("semantic_type_id") or "") or None,
                "semantic_type_name": str(item.get("semantic_type_name") or ""),
                "proposed_canonical_class_name": str(item.get("proposed_canonical_class_name") or item.get("proposed_canonical_entity_name") or ""),
                "proposed_canonical_class_slot_usage_name": str(item.get("proposed_canonical_class_slot_usage_name") or ""),
                "confidence": float(item.get("confidence", 0.0)),
                "rationale": str(item.get("rationale") or "manual semantic model response"),
                "evidence_refs": item.get("evidence_refs") if isinstance(item.get("evidence_refs"), list) else [],
                "registry_candidates": item.get("registry_candidates") if isinstance(item.get("registry_candidates"), list) else [],
                "status": str(item.get("status") or ("matched_existing" if item.get("semantic_type_id") else "proposed_new")),
            }
        )
    matched_existing_count = sum(1 for item in normalized if item["status"] == "matched_existing")
    proposed_new_count = sum(1 for item in normalized if item["status"] != "matched_existing")
    return {
        "llm_mode": "agent_manual",
        "engine": "agent_manual_pending",
        "matched_existing_count": matched_existing_count,
        "proposed_new_count": proposed_new_count,
        "suggestions": normalized,
    }
