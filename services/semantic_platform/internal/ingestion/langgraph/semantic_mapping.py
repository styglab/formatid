from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from services.semantic_platform.internal.ingestion.llm import (
    normalize_manual_semantic_mapping_response,
)
from services.semantic_platform.internal.ingestion.langgraph.common import resolve_llm_mode


@dataclass
class SemanticMappingSuggestion:
    field_id: str
    operation_id: str
    field_path: str
    raw_name: str
    semantic_type_id: str | None
    semantic_type_name: str | None
    canonical_attribute_id: str | None
    mapping_type: str
    mapping_kind: str
    confidence: float
    rationale: str
    evidence_refs: list[dict[str, Any]]
    status: str
    depends_on_proposal_ids: list[str]
    resolution_basis: str
    dependency_status: str
    review_impact: list[str]


def generate_semantic_mapping_drafts(
    *,
    fields: list[dict[str, Any]],
    semantic_types: list[dict[str, Any]],
    canonical_attributes: list[dict[str, Any]],
    existing_mappings: list[dict[str, Any]],
    manual_llm_response: dict[str, Any] | None = None,
) -> dict[str, Any]:
    llm_mode = resolve_llm_mode()
    if llm_mode == "codex_manual":
        if not isinstance(manual_llm_response, dict):
            return {
                "llm_mode": llm_mode,
                "engine": "codex_manual_pending",
                "status": "waiting_manual_llm",
                "matched_count": 0,
                "unresolved_count": 0,
                "suggestions": [],
            }
        return normalize_manual_semantic_mapping_response(manual_llm_response)
    semantic_index = _build_semantic_type_index(semantic_types)
    canonical_index = _build_canonical_attribute_index(canonical_attributes)
    mapped_field_ids = {str(item.get("field_id") or "") for item in existing_mappings if item.get("field_id")}

    suggestions: list[SemanticMappingSuggestion] = []
    for field in fields:
        field_id = str(field.get("id") or "")
        if field_id in mapped_field_ids:
            continue
        candidate_names = _candidate_names_for_field(field)
        semantic_type_id = None
        semantic_type_name = None
        canonical_attribute_id = None
        canonical_match: dict[str, Any] | None = None
        confidence = 0.41
        rationale = "No semantic type match found in current registry. Manual review required."
        status = "unresolved"
        for name in candidate_names:
            match = semantic_index.get(name)
            if match is None:
                continue
            semantic_type_id = str(match.get("id") or "")
            semantic_type_name = str(match.get("name") or "")
            canonical_match = canonical_index.get(_normalize_name(semantic_type_name))
            canonical_attribute_id = str((canonical_match or {}).get("id") or "") or None
            confidence = 0.84 if llm_mode != "disabled" else 0.72
            rationale = f"Matched field `{field.get('raw_name')}` to semantic type `{semantic_type_name}` using normalized alias/name heuristics."
            status = "matched"
            break

        depends_on_proposal_ids: list[str] = []
        resolution_basis = "missing"
        dependency_status = "blocked"
        review_impact = ["blocks_mapping"]
        if semantic_type_id:
            semantic_match = semantic_index.get(_normalize_name(semantic_type_name or ""))
            semantic_pending_proposal_id = str((semantic_match or {}).get("pending_proposal_id") or "")
            semantic_status = str((semantic_match or {}).get("status") or "")
            canonical_pending_proposal_id = str((canonical_match or {}).get("pending_proposal_id") or "") if canonical_match else ""
            canonical_status = str((canonical_match or {}).get("status") or "") if canonical_match else ""
            if semantic_pending_proposal_id:
                depends_on_proposal_ids.append(semantic_pending_proposal_id)
            if canonical_pending_proposal_id:
                depends_on_proposal_ids.append(canonical_pending_proposal_id)
            if semantic_status == "approved" and canonical_status == "approved" and canonical_attribute_id:
                resolution_basis = "approved"
                dependency_status = "ready"
                review_impact = []
            elif canonical_attribute_id:
                resolution_basis = "proposed" if depends_on_proposal_ids else "missing"
                dependency_status = "blocked"
                review_impact = ["blocks_mapping", "blocks_binding"] if depends_on_proposal_ids else ["blocks_mapping"]
            else:
                resolution_basis = "missing"
                dependency_status = "blocked"
                review_impact = ["blocks_mapping"]
        suggestions.append(
            SemanticMappingSuggestion(
                field_id=field_id,
                operation_id=str(field.get("operation_id") or ""),
                field_path=str(field.get("field_path") or field.get("raw_name") or ""),
                raw_name=str(field.get("raw_name") or ""),
                semantic_type_id=semantic_type_id,
                semantic_type_name=semantic_type_name,
                canonical_attribute_id=canonical_attribute_id,
                mapping_type="exact",
                mapping_kind="field_semantic",
                confidence=confidence,
                rationale=rationale,
                evidence_refs=_field_evidence_refs(field),
                status=status,
                depends_on_proposal_ids=depends_on_proposal_ids,
                resolution_basis=resolution_basis,
                dependency_status=dependency_status,
                review_impact=review_impact,
            )
        )
    matched = [item for item in suggestions if item.status == "matched"]
    unresolved = [item for item in suggestions if item.status != "matched"]
    return {
        "llm_mode": llm_mode,
        "engine": "heuristic_semantic_mapping_graph",
        "matched_count": len(matched),
        "unresolved_count": len(unresolved),
        "suggestions": [_serialize_suggestion(item) for item in suggestions],
    }


def _build_semantic_type_index(semantic_types: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    for semantic_type in semantic_types:
        names = [str(semantic_type.get("name") or "")]
        aliases = semantic_type.get("aliases") if isinstance(semantic_type.get("aliases"), list) else []
        names.extend(str(alias) for alias in aliases)
        for name in names:
            normalized = _normalize_name(name)
            if normalized:
                index.setdefault(normalized, semantic_type)
    return index


def _build_canonical_attribute_index(canonical_attributes: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    for attribute in canonical_attributes:
        normalized = _normalize_name(str(attribute.get("name") or ""))
        if normalized:
            index.setdefault(normalized, attribute)
    return index


def _candidate_names_for_field(field: dict[str, Any]) -> list[str]:
    raw_name = str(field.get("raw_name") or "")
    field_path = str(field.get("field_path") or "")
    base_names = [raw_name, field_path.split(".")[-1] if field_path else ""]
    lowered = raw_name.lower()
    if lowered.endswith("nm") or "name" in lowered:
        base_names.extend(["name", "title"])
    if lowered.endswith("id") or lowered.endswith("no") or "code" in lowered:
        base_names.extend(["id", "code", "identifier"])
    if lowered.endswith("dt") or "date" in lowered:
        base_names.extend(["date", "datetime"])
    if lowered.endswith("div") or "type" in lowered:
        base_names.extend(["type", "category", "control"])
    return [_normalize_name(item) for item in base_names if _normalize_name(item)]


def _field_evidence_refs(field: dict[str, Any]) -> list[dict[str, Any]]:
    evidence = field.get("evidence") if isinstance(field.get("evidence"), list) else []
    if evidence:
        return evidence
    return [{"kind": "field_path", "field_path": str(field.get("field_path") or "")}]


def _normalize_name(value: str) -> str:
    return "".join(ch for ch in value.lower() if ch.isalnum())


def _serialize_suggestion(suggestion: SemanticMappingSuggestion) -> dict[str, Any]:
    return {
        "field_id": suggestion.field_id,
        "operation_id": suggestion.operation_id,
        "field_path": suggestion.field_path,
        "raw_name": suggestion.raw_name,
        "semantic_type_id": suggestion.semantic_type_id,
        "semantic_type_name": suggestion.semantic_type_name,
        "canonical_attribute_id": suggestion.canonical_attribute_id,
        "mapping_type": suggestion.mapping_type,
        "mapping_kind": suggestion.mapping_kind,
        "confidence": suggestion.confidence,
        "rationale": suggestion.rationale,
        "evidence_refs": suggestion.evidence_refs,
        "status": suggestion.status,
        "depends_on_proposal_ids": suggestion.depends_on_proposal_ids,
        "resolution_basis": suggestion.resolution_basis,
        "dependency_status": suggestion.dependency_status,
        "review_impact": suggestion.review_impact,
    }
