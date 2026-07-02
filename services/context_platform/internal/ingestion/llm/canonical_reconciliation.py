from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class CanonicalProposalModel(BaseModel):
    class_name: str
    slot_name: str
    datatype: str
    description: str
    aliases: list[str]
    identity_role: str = ""


class CanonicalReconciliationDecisionModel(BaseModel):
    source_kind: str
    source_parameter_id: str | None = None
    source_field_id: str | None = None
    field_path: str
    raw_name: str
    decision: str
    canonical_class_slot_id: str | None = None
    canonical_class_id: str | None = None
    proposed_canonical: CanonicalProposalModel
    linkml_fragment: dict[str, Any] = {}
    confidence: float
    rationale: str


class CanonicalRelationSuggestionModel(BaseModel):
    decision: str
    source_class_id: str | None = None
    source_class_name: str
    target_class_id: str | None = None
    target_class_name: str
    relation_type: str
    forward_label: str = ""
    reverse_label: str = ""
    description: str = ""
    cardinality: str = ""
    required: bool = False
    metadata: dict[str, Any] = {}
    confidence: float
    rationale: str
    evidence_refs: list[dict[str, Any]] = []


class CanonicalReconciliationResponseModel(BaseModel):
    decisions: list[CanonicalReconciliationDecisionModel]
    relation_suggestions: list[CanonicalRelationSuggestionModel] = []


def normalize_manual_canonical_reconciliation_response(payload: dict[str, Any]) -> dict[str, Any]:
    decisions = payload.get("decisions") if isinstance(payload.get("decisions"), list) else []
    if not decisions and isinstance(payload.get("meaning_decisions"), list):
        decisions = payload.get("meaning_decisions") or []
    if not decisions and isinstance(payload.get("concept_decisions"), list):
        decisions = payload.get("concept_decisions") or []
    if not decisions and isinstance(payload.get("representation_decisions"), list):
        decisions = payload.get("representation_decisions") or []
    concept_decisions = _normalize_named_decisions(payload, "concept_decisions")
    representation_decisions = _normalize_named_decisions(payload, "representation_decisions")
    representation_schema_decisions = _normalize_named_decisions(payload, "representation_schema_decisions")
    value_domain_decisions = _normalize_named_decisions(payload, "value_domain_decisions")
    normalized: list[dict[str, Any]] = []
    for item in decisions:
        if not isinstance(item, dict):
            continue
        proposed = item.get("proposed_canonical") if isinstance(item.get("proposed_canonical"), dict) else {}
        if not proposed and isinstance(item.get("proposed_representation"), dict):
            proposed = item.get("proposed_representation") or {}
        if not proposed and isinstance(item.get("canonical_representation"), dict):
            proposed = item.get("canonical_representation") or {}
        if not proposed and isinstance(item.get("representation"), dict):
            proposed = item.get("representation") or {}
        concept = item.get("concept") if isinstance(item.get("concept"), dict) else {}
        representation = item.get("proposed_representation") if isinstance(item.get("proposed_representation"), dict) else {}
        if not representation and isinstance(item.get("canonical_representation"), dict):
            representation = item.get("canonical_representation") or {}
        if not representation and isinstance(item.get("representation"), dict):
            representation = item.get("representation") or {}
        schema = item.get("representation_schema") if isinstance(item.get("representation_schema"), dict) else {}
        if not schema and isinstance(item.get("proposed_schema"), dict):
            schema = item.get("proposed_schema") or {}
        aliases = proposed.get("aliases") if isinstance(proposed.get("aliases"), list) else []
        carrier_object = str(proposed.get("carrier_object_type") or proposed.get("object_type") or proposed.get("class_name") or proposed.get("entity_name") or "")
        value_property = str(proposed.get("value_property") or proposed.get("property_type") or proposed.get("slot_name") or proposed.get("attribute_name") or "")
        datatype = str(schema.get("datatype") or proposed.get("datatype") or "string")
        normalized.append(
            {
                "source_kind": str(item.get("source_kind") or ""),
                "source_parameter_id": str(item.get("source_parameter_id") or "") or None,
                "source_field_id": str(item.get("source_field_id") or "") or None,
                "source_operation_id": str(item.get("source_operation_id") or "") or None,
                "source_operation_key": str(item.get("source_operation_key") or "") or None,
                "operation_key": str(item.get("operation_key") or "") or None,
                "operation_name": str(item.get("operation_name") or "") or None,
                "operation_path": str(item.get("operation_path") or item.get("path") or "") or None,
                "field_path": str(item.get("field_path") or ""),
                "raw_name": str(item.get("raw_name") or ""),
                "decision": str(item.get("decision") or "create"),
                "canonical_class_slot_id": str(item.get("representation_id") or item.get("canonical_class_slot_id") or "") or None,
                "canonical_class_id": str(item.get("canonical_class_id") or "") or None,
                "concept_key": str(item.get("concept_key") or concept.get("stable_key") or concept.get("concept_key") or "") or None,
                "representation_key": str(item.get("representation_key") or representation.get("stable_key") or representation.get("representation_key") or "") or None,
                "representation_schema_key": str(item.get("representation_schema_key") or schema.get("stable_key") or schema.get("schema_key") or "") or None,
                "value_domain_key": str(item.get("value_domain_key") or schema.get("value_domain_key") or "") or None,
                "concept": concept,
                "proposed_representation": representation,
                "representation_schema": schema,
                "proposed_canonical": {
                    "class_name": carrier_object,
                    "slot_name": value_property,
                    "datatype": datatype,
                    "description": str(proposed.get("description") or ""),
                    "aliases": [str(alias) for alias in aliases if str(alias)],
                    "identity_role": str(proposed.get("identity_role") or ""),
                },
                "linkml_fragment": item.get("linkml_fragment") if isinstance(item.get("linkml_fragment"), dict) else {},
                "confidence": float(item.get("confidence") or 0.0),
                "rationale": str(item.get("rationale") or ""),
            }
        )
    relation_suggestions = payload.get("relation_suggestions") if isinstance(payload.get("relation_suggestions"), list) else []
    if not relation_suggestions and isinstance(payload.get("concept_relations"), list):
        relation_suggestions = payload.get("concept_relations") or []
    if not relation_suggestions and isinstance(payload.get("link_type_suggestions"), list):
        relation_suggestions = payload.get("link_type_suggestions") or []
    normalized_relations: list[dict[str, Any]] = []
    for item in relation_suggestions:
        if not isinstance(item, dict):
            continue
        metadata = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
        evidence_refs = item.get("evidence_refs") if isinstance(item.get("evidence_refs"), list) else []
        normalized_relations.append(
            {
                "decision": str(item.get("decision") or "propose_relation"),
                "source_class_id": str(item.get("source_class_id") or "") or None,
                "source_class_name": str(item.get("source_class_name") or ""),
                "target_class_id": str(item.get("target_class_id") or "") or None,
                "target_class_name": str(item.get("target_class_name") or ""),
                "relation_type": str(item.get("relation_type") or item.get("relation_name") or ""),
                "forward_label": str(item.get("forward_label") or ""),
                "reverse_label": str(item.get("reverse_label") or ""),
                "description": str(item.get("description") or ""),
                "cardinality": str(item.get("cardinality") or ""),
                "required": bool(item.get("required", False)),
                "metadata": metadata,
                "confidence": float(item.get("confidence") or 0.0),
                "rationale": str(item.get("rationale") or ""),
                "evidence_refs": [ref for ref in evidence_refs if isinstance(ref, dict)],
            }
        )
    return {
        "llm_mode": "agent_manual",
        "engine": "agent_manual_meaning_resolution_graph",
        "decisions": normalized,
        "meaning_decisions": normalized,
        "concept_decisions": concept_decisions,
        "representation_decisions": representation_decisions,
        "representation_schema_decisions": representation_schema_decisions,
        "value_domain_decisions": value_domain_decisions,
        "relation_suggestions": normalized_relations,
    }


def _normalize_named_decisions(payload: dict[str, Any], key: str) -> list[dict[str, Any]]:
    values = payload.get(key) if isinstance(payload.get(key), list) else []
    normalized: list[dict[str, Any]] = []
    for item in values:
        if isinstance(item, dict):
            normalized.append(dict(item))
    return normalized


def generate_openai_canonical_reconciliation_response(request: dict[str, Any]) -> dict[str, Any]:
    raise RuntimeError(
        "OpenAI-backed Context Platform ingestion is no longer supported; "
        "run agent_manual mode and provide an explicit agent response artifact"
    )
