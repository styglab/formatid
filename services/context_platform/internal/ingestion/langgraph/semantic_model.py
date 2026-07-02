from __future__ import annotations

from dataclasses import dataclass
import hashlib
import re
from typing import Any

from services.context_platform.internal.ingestion.langgraph.common import resolve_llm_mode
from services.context_platform.internal.ingestion.llm.semantic_model import (
    normalize_manual_semantic_model_response,
)
from services.context_platform.internal.ingestion.retrieval import (
    sync_semantic_type_registry,
    search_semantic_type_candidates,
)


@dataclass
class SemanticModelSuggestion:
    field_id: str
    operation_id: str
    field_path: str
    raw_name: str
    semantic_type_id: str | None
    semantic_type_name: str | None
    proposed_canonical_class_name: str | None
    proposed_canonical_class_slot_usage_name: str | None
    confidence: float
    rationale: str
    evidence_refs: list[dict[str, Any]]
    cluster_summary: str
    registry_candidates: list[dict[str, Any]]
    status: str


def generate_semantic_model_drafts(
    *,
    source: dict[str, Any],
    operations: list[dict[str, Any]],
    fields: list[dict[str, Any]],
    semantic_types: list[dict[str, Any]],
    canonical_class_slot_usages: list[dict[str, Any]],
    manual_llm_response: dict[str, Any] | None = None,
) -> dict[str, Any]:
    llm_mode = resolve_llm_mode()
    operation_index = {str(item.get("id") or ""): item for item in operations}
    canonical_class_slot_usage_by_semantic = {
        str(item.get("semantic_type_id") or ""): item
        for item in canonical_class_slot_usages
        if str(item.get("semantic_type_id") or "")
    }

    suggestions: list[SemanticModelSuggestion] = []
    if semantic_types:
        sync_semantic_type_registry(semantic_types)
    for field in fields:
        query_text = _build_field_cluster_text(source=source, operation=operation_index.get(str(field.get("operation_id") or "")), field=field)
        candidates = search_semantic_type_candidates(
            query_text=query_text,
            semantic_types=semantic_types,
            limit=5,
            sync_registry=False,
        )
        top = candidates[0] if candidates else None
        matched_existing = bool(top and float(top.get("score") or 0.0) >= _reuse_threshold())
        semantic_type_id = str(top.get("semantic_type_id") or "") if matched_existing and top else ""
        semantic_type_name = str(top.get("semantic_type_name") or "") if matched_existing and top else ""
        canonical_class_slot_usage = canonical_class_slot_usage_by_semantic.get(semantic_type_id) if semantic_type_id else None
        proposed_class, proposed_attribute = _derive_canonical_names(
            semantic_type_name if semantic_type_name else _derive_semantic_type_name(field)
        )
        confidence = float(top.get("score") or 0.0) if top else 0.0
        suggestions.append(
            SemanticModelSuggestion(
                field_id=str(field.get("id") or ""),
                operation_id=str(field.get("operation_id") or ""),
                field_path=str(field.get("field_path") or field.get("raw_name") or ""),
                raw_name=str(field.get("raw_name") or ""),
                semantic_type_id=semantic_type_id or None,
                semantic_type_name=semantic_type_name or _derive_semantic_type_name(field),
                proposed_canonical_class_name=str((canonical_class_slot_usage or {}).get("class_name") or (canonical_class_slot_usage or {}).get("entity_name") or proposed_class),
                proposed_canonical_class_slot_usage_name=str((canonical_class_slot_usage or {}).get("name") or proposed_attribute),
                confidence=max(confidence, 0.55 if matched_existing else 0.46),
                rationale=_build_rationale(field=field, top_candidate=top, matched_existing=matched_existing),
                evidence_refs=_field_evidence_refs(field),
                cluster_summary=_build_cluster_summary(source=source, operation=operation_index.get(str(field.get("operation_id") or "")), field=field),
                registry_candidates=candidates,
                status="matched_existing" if matched_existing else "proposed_new",
            )
        )

    matched_existing_count = sum(1 for item in suggestions if item.status == "matched_existing")
    proposed_new_count = sum(1 for item in suggestions if item.status != "matched_existing")
    if llm_mode == "agent_manual":
        if not isinstance(manual_llm_response, dict):
            return {
                "llm_mode": llm_mode,
                "engine": "agent_manual_pending",
                "status": "waiting_manual_llm",
                "matched_existing_count": matched_existing_count,
                "proposed_new_count": proposed_new_count,
                "suggestions": [_serialize_suggestion(item) for item in suggestions],
            }
        return normalize_manual_semantic_model_response(manual_llm_response)
    return {
        "llm_mode": llm_mode,
        "engine": "retrieval_first_semantic_model_graph",
        "matched_existing_count": matched_existing_count,
        "proposed_new_count": proposed_new_count,
        "suggestions": [_serialize_suggestion(item) for item in suggestions],
    }


def _build_field_cluster_text(*, source: dict[str, Any], operation: dict[str, Any] | None, field: dict[str, Any]) -> str:
    evidence = field.get("evidence") if isinstance(field.get("evidence"), list) else []
    evidence_lines = []
    for item in evidence[:5]:
        if not isinstance(item, dict):
            continue
        evidence_lines.append(" ".join(str(value) for value in item.values() if value))
    parts = [
        f"source: {source.get('name') or ''}",
        f"source_type: {source.get('source_type') or ''}",
        f"provider: {source.get('provider') or ''}",
        f"operation_name: {(operation or {}).get('name') or ''}",
        f"operation_description: {(operation or {}).get('description') or ''}",
        f"field_path: {field.get('field_path') or ''}",
        f"raw_name: {field.get('raw_name') or ''}",
        f"scope: {field.get('scope') or ''}",
        f"data_type: {field.get('data_type') or ''}",
        f"description: {field.get('description') or ''}",
        "evidence: " + " | ".join(line for line in evidence_lines if line),
    ]
    return "\n".join(part for part in parts if part.strip())


def _build_cluster_summary(*, source: dict[str, Any], operation: dict[str, Any] | None, field: dict[str, Any]) -> str:
    parts = [
        str(source.get("source_type") or ""),
        str((operation or {}).get("name") or ""),
        str(field.get("field_path") or field.get("raw_name") or ""),
        str(field.get("description") or ""),
    ]
    return " | ".join(part for part in parts if part.strip())


def _derive_semantic_type_name(field: dict[str, Any]) -> str:
    base = str(field.get("raw_name") or field.get("field_path") or "UNCLASSIFIED_FIELD")
    return _to_snake_identifier(base, fallback_prefix="semantic_field")


def _derive_canonical_names(semantic_type_name: str) -> tuple[str, str]:
    parts = [part for part in semantic_type_name.split("_") if part]
    if not parts:
        return ("record", "value")
    if len(parts) == 1:
        return ("record", parts[0].lower())
    class_name = "_".join(parts[:-1]).lower() or "record"
    attribute = parts[-1].lower()
    return (class_name, attribute)


def _to_snake_identifier(value: str, *, fallback_prefix: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "_", value.strip().lower()).strip("_")
    normalized = re.sub(r"_+", "_", normalized)
    if normalized and normalized[0].isdigit():
        normalized = f"{fallback_prefix}_{normalized}"
    if normalized:
        return normalized[:80]
    suffix = hashlib.md5(value.encode("utf-8")).hexdigest()[:8]
    return f"{fallback_prefix}_{suffix}"


def _field_evidence_refs(field: dict[str, Any]) -> list[dict[str, Any]]:
    evidence = field.get("evidence") if isinstance(field.get("evidence"), list) else []
    if evidence:
        return evidence
    return [{"kind": "field_path", "field_path": str(field.get("field_path") or "")}]


def _reuse_threshold() -> float:
    return 0.78


def _build_rationale(*, field: dict[str, Any], top_candidate: dict[str, Any] | None, matched_existing: bool) -> str:
    raw_name = str(field.get("raw_name") or field.get("field_path") or "")
    if matched_existing and top_candidate:
        return (
            f"Retrieved existing semantic type `{top_candidate.get('semantic_type_name')}` "
            f"for field `{raw_name}` using semantic-registry similarity search."
        )
    if top_candidate:
        return (
            f"Nearest semantic type candidate for `{raw_name}` was `{top_candidate.get('semantic_type_name')}`, "
            "but similarity was not strong enough to reuse without proposing a new class-slot usage."
        )
    return f"No reliable canonical slot candidate was found for `{raw_name}`. Propose a new class-slot usage."


def _serialize_suggestion(suggestion: SemanticModelSuggestion) -> dict[str, Any]:
    top_candidate = suggestion.registry_candidates[0] if suggestion.registry_candidates else None
    return {
        "field_id": suggestion.field_id,
        "operation_id": suggestion.operation_id,
        "field_path": suggestion.field_path,
        "raw_name": suggestion.raw_name,
        "semantic_type_id": suggestion.semantic_type_id,
        "semantic_type_name": suggestion.semantic_type_name,
        "proposed_canonical_class_name": suggestion.proposed_canonical_class_name,
        "proposed_canonical_class_slot_usage_name": suggestion.proposed_canonical_class_slot_usage_name,
        "confidence": suggestion.confidence,
        "rationale": suggestion.rationale,
        "evidence_refs": suggestion.evidence_refs,
        "cluster_summary": suggestion.cluster_summary,
        "registry_candidates": suggestion.registry_candidates,
        "top_registry_candidate": top_candidate,
        "status": suggestion.status,
    }
