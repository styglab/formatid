from __future__ import annotations

from typing import Any


def build_manual_semantic_mapping_request(
    *,
    run_id: str,
    source: dict[str, Any],
    fields: list[dict[str, Any]],
    semantic_types: list[dict[str, Any]],
    canonical_attributes: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "type": "semantic_mapping",
        "run_id": run_id,
        "source": {
            "id": source.get("id"),
            "name": source.get("name"),
            "source_type": source.get("source_type"),
            "provider": source.get("provider"),
        },
        "instructions": [
            "Review extracted source fields and propose semantic mappings.",
            "Return only field-level mapping suggestions backed by source evidence.",
            "Use semantic_type_id only when it exists in the provided registry snapshot.",
            "Leave semantic_type_id null when no reliable match exists.",
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
            "canonical_attributes": [
                {
                    "id": item.get("id"),
                    "name": item.get("name"),
                    "entity_id": item.get("entity_id"),
                    "description": item.get("description") or "",
                }
                for item in canonical_attributes
            ],
        },
        "fields": [
            {
                "field_id": field.get("id"),
                "operation_id": field.get("operation_id"),
                "raw_name": field.get("raw_name"),
                "field_path": field.get("field_path"),
                "scope": field.get("scope"),
                "data_type": field.get("data_type"),
                "description": field.get("description") or "",
                "evidence": field.get("evidence") or [],
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
                    "semantic_type_name": "string|null",
                    "canonical_attribute_id": "string|null",
                    "mapping_type": "exact|transform|composite|enum|reference",
                    "mapping_kind": "field_semantic",
                    "confidence": "float",
                    "rationale": "string",
                    "evidence_refs": ["list of evidence ref objects"],
                    "status": "matched|unresolved",
                }
            ]
        },
    }


def normalize_manual_semantic_mapping_response(payload: dict[str, Any]) -> dict[str, Any]:
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
                "semantic_type_name": str(item.get("semantic_type_name") or "") or None,
                "canonical_attribute_id": str(item.get("canonical_attribute_id") or "") or None,
                "mapping_type": str(item.get("mapping_type") or "exact"),
                "mapping_kind": str(item.get("mapping_kind") or "field_semantic"),
                "confidence": float(item.get("confidence", 0.0)),
                "rationale": str(item.get("rationale") or "manual semantic mapping response"),
                "evidence_refs": item.get("evidence_refs") if isinstance(item.get("evidence_refs"), list) else [],
                "status": str(item.get("status") or ("matched" if item.get("semantic_type_id") else "unresolved")),
            }
        )
    matched_count = sum(1 for item in normalized if item["status"] == "matched")
    unresolved_count = sum(1 for item in normalized if item["status"] != "matched")
    return {
        "llm_mode": "codex_manual",
        "engine": "codex_manual_llm",
        "matched_count": matched_count,
        "unresolved_count": unresolved_count,
        "suggestions": normalized,
    }
