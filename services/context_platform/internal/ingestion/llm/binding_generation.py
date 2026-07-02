from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class BindingCanonicalRefModel(BaseModel):
    class_name: str
    slot_name: str


class BindingTransformSpecModel(BaseModel):
    type: str
    rule_id: str = ""
    params: dict[str, Any] = {}


class BindingSuggestionModel(BaseModel):
    source_kind: str
    source_parameter_id: str | None = None
    source_field_id: str | None = None
    field_path: str
    raw_name: str
    decision: str
    canonical_class_slot_id: str | None = None
    canonical_ref: BindingCanonicalRefModel
    direction: str
    binding_type: str
    transform_spec: BindingTransformSpecModel
    normalization_rule: dict[str, Any] = {}
    enum_mapping: dict[str, Any] = {}
    depends_on_canonical_decision: bool
    confidence: float
    rationale: str
    evidence_refs: list[dict[str, Any]] = []


class BindingGenerationResponseModel(BaseModel):
    suggestions: list[BindingSuggestionModel]


def normalize_manual_binding_generation_response(payload: dict[str, Any]) -> dict[str, Any]:
    suggestions: list[dict[str, Any]] = []
    for key in ("field_bindings", "context_bindings", "parameter_bindings"):
        if isinstance(payload.get(key), list):
            for item in payload.get(key) or []:
                if isinstance(item, dict):
                    suggestions.append({**item, "binding_kind": _binding_kind_for_key(key)})
    if not suggestions:
        suggestions = payload.get("suggestions") if isinstance(payload.get("suggestions"), list) else []
    if not suggestions and isinstance(payload.get("resolution_suggestions"), list):
        suggestions = payload.get("resolution_suggestions") or []
    transform_rules = [item for item in payload.get("transform_rules", []) if isinstance(item, dict)] if isinstance(payload.get("transform_rules"), list) else []
    normalized: list[dict[str, Any]] = []
    for item in suggestions:
        if not isinstance(item, dict):
            continue
        canonical_ref = item.get("canonical_ref") if isinstance(item.get("canonical_ref"), dict) else {}
        if not canonical_ref and isinstance(item.get("representation_ref"), dict):
            canonical_ref = item.get("representation_ref") or {}
        representation = item.get("representation") if isinstance(item.get("representation"), dict) else {}
        transform_spec = item.get("transform_spec") if isinstance(item.get("transform_spec"), dict) else {}
        context_key = str(item.get("context_key") or item.get("fills_context_key") or "")
        fills_property = str(item.get("fills_property") or item.get("fills_property_type") or item.get("value_property") or "")
        normalized.append(
            {
                "binding_kind": str(item.get("binding_kind") or _infer_binding_kind(item)),
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
                "decision": str(item.get("decision") or "bind"),
                "canonical_class_slot_id": str(item.get("representation_id") or item.get("canonical_class_slot_id") or "") or None,
                "representation_id": str(item.get("representation_id") or item.get("canonical_class_slot_id") or "") or None,
                "representation_key": str(item.get("representation_key") or representation.get("stable_key") or representation.get("representation_key") or "") or None,
                "representation_schema_key": str(item.get("representation_schema_key") or item.get("schema_key") or "") or None,
                "concept_key": str(item.get("concept_key") or representation.get("concept_key") or "") or None,
                "required_concept_key": str(item.get("required_concept_key") or item.get("required_concept") or "") or None,
                "context_key": context_key or None,
                "fills_property": fills_property or None,
                "canonical_ref": {
                    "class_name": str(canonical_ref.get("class_name") or canonical_ref.get("entity_name") or canonical_ref.get("carrier_object_type") or ""),
                    "slot_name": str(canonical_ref.get("slot_name") or canonical_ref.get("attribute_name") or canonical_ref.get("value_property") or ""),
                },
                "direction": str(item.get("direction") or "output"),
                "binding_type": str(item.get("binding_type") or "exact"),
                "transform_spec": {
                    "type": str(transform_spec.get("type") or "none"),
                    **({"rule_id": str(transform_spec.get("rule_id") or "")} if transform_spec.get("rule_id") else {}),
                    "params": transform_spec.get("params") if isinstance(transform_spec.get("params"), dict) else {},
                },
                "normalization_rule": item.get("normalization_rule") if isinstance(item.get("normalization_rule"), dict) else {},
                "enum_mapping": item.get("enum_mapping") if isinstance(item.get("enum_mapping"), dict) else {},
                "depends_on_canonical_decision": bool(item.get("depends_on_canonical_decision")),
                "confidence": float(item.get("confidence") or 0.0),
                "rationale": str(item.get("rationale") or ""),
                "evidence_refs": item.get("evidence_refs") if isinstance(item.get("evidence_refs"), list) else [],
            }
        )
    return {
        "llm_mode": "agent_manual",
        "engine": "agent_manual_resolution_generation_graph",
        "suggestions": normalized,
        "resolution_suggestions": normalized,
        "field_bindings": [item for item in normalized if item.get("binding_kind") == "field"],
        "context_bindings": [item for item in normalized if item.get("binding_kind") == "context"],
        "parameter_bindings": [item for item in normalized if item.get("binding_kind") == "parameter"],
        "transform_rules": transform_rules,
    }


def _binding_kind_for_key(key: str) -> str:
    return {"field_bindings": "field", "context_bindings": "context", "parameter_bindings": "parameter"}[key]


def _infer_binding_kind(item: dict[str, Any]) -> str:
    if item.get("context_key") or item.get("fills_context_key"):
        return "context"
    if str(item.get("source_kind") or "") == "parameter":
        return "parameter"
    return "field"


def generate_openai_binding_generation_response(request: dict[str, Any]) -> dict[str, Any]:
    raise RuntimeError(
        "OpenAI-backed Context Platform ingestion is no longer supported; "
        "run agent_manual mode and provide an explicit agent response artifact"
    )
