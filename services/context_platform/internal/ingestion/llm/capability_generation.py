from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class CapabilityRefModel(BaseModel):
    class_name: str
    slot_name: str


class CapabilityPayloadModel(BaseModel):
    capability_key: str
    namespace: str = "public"
    name: str
    description: str
    intent_spec: dict[str, Any]
    metadata: dict[str, Any] = {}


class CapabilityInputModel(BaseModel):
    canonical_class_slot_id: str | None = None
    representation_id: str | None = None
    concept_key: str | None = None
    representation_key: str | None = None
    representation_schema_key: str | None = None
    canonical_ref: CapabilityRefModel
    required: bool = True
    input_order: int = 100
    source_parameter_id: str | None = None
    binding_ref: dict[str, Any] = {}
    depends_on_binding: bool = True
    depends_on_canonical_decision: bool = False


class CapabilityOutputModel(BaseModel):
    canonical_class_slot_id: str | None = None
    representation_id: str | None = None
    concept_key: str | None = None
    representation_key: str | None = None
    representation_schema_key: str | None = None
    output_key: str | None = None
    canonical_ref: CapabilityRefModel
    output_order: int = 100
    source_field_id: str | None = None
    binding_ref: dict[str, Any] = {}
    depends_on_binding: bool = True
    depends_on_canonical_decision: bool = False


class CapabilityOperationLinkModel(BaseModel):
    source_operation_id: str
    priority: int = 100
    binding_spec: dict[str, Any]


class CapabilitySuggestionModel(BaseModel):
    decision: str
    source_operation_id: str | None = None
    capability: CapabilityPayloadModel | dict[str, Any]
    inputs: list[CapabilityInputModel] = []
    outputs: list[CapabilityOutputModel] = []
    operation_link: CapabilityOperationLinkModel | dict[str, Any] = {}
    confidence: float
    rationale: str
    evidence_refs: list[dict[str, Any]] = []


class CapabilityGenerationResponseModel(BaseModel):
    suggestions: list[CapabilitySuggestionModel]


def normalize_manual_capability_generation_response(payload: dict[str, Any]) -> dict[str, Any]:
    suggestions = payload.get("suggestions") if isinstance(payload.get("suggestions"), list) else []
    if not suggestions and isinstance(payload.get("capability_contracts"), list):
        suggestions = payload.get("capability_contracts") or []
    if not suggestions and isinstance(payload.get("capabilities"), list):
        suggestions = payload.get("capabilities") or []
    normalized: list[dict[str, Any]] = []
    for item in suggestions:
        if not isinstance(item, dict):
            continue
        normalized.append(
            {
                "decision": str(item.get("decision") or "propose_capability"),
                "source_operation_id": str(item.get("source_operation_id") or "") or None,
                "operation_key": str(item.get("operation_key") or "") or None,
                "operation_name": str(item.get("operation_name") or "") or None,
                "path": str(item.get("path") or "") or None,
                "operation": item.get("operation") if isinstance(item.get("operation"), dict) else {},
                "capability": item.get("capability") if isinstance(item.get("capability"), dict) else {},
                "inputs": [_normalize_capability_io(value) for value in item.get("inputs", []) if isinstance(value, dict)]
                if isinstance(item.get("inputs"), list)
                else [],
                "outputs": [_normalize_capability_io(value) for value in item.get("outputs", []) if isinstance(value, dict)]
                if isinstance(item.get("outputs"), list)
                else [],
                "operation_link": item.get("operation_link") if isinstance(item.get("operation_link"), dict) else {},
                "confidence": float(item.get("confidence") or 0.0),
                "rationale": str(item.get("rationale") or ""),
                "evidence_refs": item.get("evidence_refs") if isinstance(item.get("evidence_refs"), list) else [],
            }
        )
    return {
        "llm_mode": "agent_manual",
        "engine": "agent_manual_capability_generation_graph",
        "suggestions": normalized,
        "capability_contracts": normalized,
    }


def _normalize_capability_io(item: dict[str, Any]) -> dict[str, Any]:
    canonical_ref = item.get("canonical_ref") if isinstance(item.get("canonical_ref"), dict) else {}
    if not canonical_ref and isinstance(item.get("representation_ref"), dict):
        canonical_ref = item.get("representation_ref") or {}
    return {
        **item,
        "concept_key": str(item.get("concept_key") or "") or None,
        "representation_id": str(item.get("representation_id") or item.get("canonical_class_slot_id") or "") or None,
        "representation_key": str(item.get("representation_key") or "") or None,
        "representation_schema_key": str(item.get("representation_schema_key") or item.get("schema_key") or "") or None,
        "output_key": str(item.get("output_key") or "") or None,
        "canonical_ref": {
            "class_name": str(canonical_ref.get("class_name") or canonical_ref.get("entity_name") or canonical_ref.get("carrier_object_type") or ""),
            "slot_name": str(canonical_ref.get("slot_name") or canonical_ref.get("attribute_name") or canonical_ref.get("value_property") or ""),
        },
    }


def generate_openai_capability_generation_response(request: dict[str, Any]) -> dict[str, Any]:
    raise RuntimeError(
        "OpenAI-backed Context Platform ingestion is no longer supported; "
        "run agent_manual mode and provide an explicit agent response artifact"
    )
