from __future__ import annotations

import re
from collections import Counter
from typing import Any

from services.context_platform.internal.ingestion.langgraph.common import resolve_llm_mode

CAPABILITY_DECISION_TYPES = {"propose_capability", "skip_capability", "conflict"}
NON_BUSINESS_CLASS_NAMES = {
    "api_response",
    "apiresponse",
    "envelope",
    "record",
    "request",
    "request_context",
    "requestcontext",
    "response",
    "response_envelope",
    "responseenvelope",
    "result",
}


def build_capability_generation(
    *,
    run_id: str,
    source: dict[str, Any],
    document: dict[str, Any],
    operations: list[dict[str, Any]],
    canonical_reconciliation: dict[str, Any],
    binding_generation: dict[str, Any],
    llm_mode: str | None = None,
    manual_llm_response: dict[str, Any] | None = None,
) -> dict[str, Any]:
    request = build_manual_capability_generation_request(
        run_id=run_id,
        source=source,
        document=document,
        operations=operations,
        canonical_reconciliation=canonical_reconciliation,
        binding_generation=binding_generation,
    )
    resolved_llm_mode = _resolve_llm_mode(llm_mode)
    if resolved_llm_mode == "agent_manual" and isinstance(manual_llm_response, dict):
        suggestions = capability_suggestions_from_manual_response(
            request["operation_contexts"],
            manual_llm_response,
            allow_heuristic_propose=False,
        )
        engine = "agent_manual_capability_generation"
    else:
        suggestions = skip_capabilities_without_llm(request["operation_contexts"])
        engine = "no_llm_capability_generation"
    return build_capability_generation_payload(
        source=source,
        document=document,
        suggestions=suggestions,
        llm_mode=resolved_llm_mode,
        engine=engine,
    )


def build_manual_capability_generation_request(
    *,
    run_id: str,
    source: dict[str, Any],
    document: dict[str, Any],
    operations: list[dict[str, Any]],
    canonical_reconciliation: dict[str, Any],
    binding_generation: dict[str, Any],
) -> dict[str, Any]:
    operation_contexts = collect_operation_contexts(
        source=source,
        document=document,
        operations=operations,
        binding_generation=binding_generation,
    )
    return {
        "type": "capability_generation",
        "legacy_type": "capability_contracting",
        "run_id": run_id,
        "source": {
            "id": source.get("id"),
            "name": source.get("name"),
            "source_type": source.get("source_type"),
            "provider": source.get("provider"),
        },
        "document": {
            "id": document.get("id"),
            "name": document.get("name"),
            "document_type": document.get("document_type"),
        },
        "instructions": [
            "Generate planner-facing Capability contracts from executable source operations and reviewed resolution bindings.",
            "Do not create capabilities for documents that have no executable source operation.",
            "Capability keys must be provider-neutral and based on canonical inputs and outputs, not provider names.",
            "Use only reviewed resolution suggestions as capability signals; skipped bindings and provider controls must not become capability inputs, outputs, or operation variants.",
            "Do not infer capabilities from provider/domain raw-name keywords when bindings do not support the meaning.",
            "Use source_operations as the operation reference. Do not invent an operation registry.",
            "Include capability inputs, outputs, and a capability-step link with binding_spec.",
            "Capability inputs and outputs must reference concept_key, representation_key, and representation_schema_key when the upstream resolution provides them.",
            "Use output_key as a consumer/planner-facing name only. Do not treat output_key as a canonical property.",
            "If the operation has no meaningful output bindings, use skip_capability.",
            "Do not create generic capabilities such as record.lookup. If the output class is only a transport/container class, use skip_capability.",
            "Return exactly one suggestion for every operation_context. Missing suggestions are treated as skip_capability, not automatic capability proposals.",
        ],
        "representation_model_linkml_fragment": canonical_reconciliation.get("linkml_fragment")
        if isinstance(canonical_reconciliation.get("linkml_fragment"), dict)
        else {},
        "canonical_model_linkml_fragment": canonical_reconciliation.get("linkml_fragment")
        if isinstance(canonical_reconciliation.get("linkml_fragment"), dict)
        else {},
        "operation_contexts": operation_contexts,
        "response_contract": {
            "suggestions": [
                {
                    "decision": "propose_capability|skip_capability|conflict",
                    "source_operation_id": "string|null",
                    "capability": {
                        "capability_key": "provider_neutral.business.capability",
                        "namespace": "public",
                        "name": "Human readable capability name",
                        "description": "what the capability achieves",
                        "intent_spec": {
                            "canonical_inputs": [],
                            "canonical_outputs": [],
                            "examples": [],
                            "aliases": [],
                        },
                    },
                    "inputs": [
                        {
                            "canonical_class_slot_id": "string|null",
                            "representation_id": "string|null",
                            "concept_key": "concept.identifier...|concept.time...|...",
                            "representation_key": "repr.identifier...|repr.time...|null",
                            "representation_schema_key": "schema.identifier...|schema.time...|null",
                            "canonical_ref": {"class_name": "string", "slot_name": "string"},
                            "required": "boolean",
                            "source_parameter_id": "string|null",
                            "binding_ref": {},
                            "depends_on_binding": "boolean",
                        }
                    ],
                    "outputs": [
                        {
                            "canonical_class_slot_id": "string|null",
                            "representation_id": "string|null",
                            "concept_key": "concept.finance.revenue|concept.tax.business_registration_status|...",
                            "representation_key": "repr.finance.revenue.observation_amount|...",
                            "representation_schema_key": "schema.finance.revenue.money_amount|...",
                            "output_key": "consumer-facing key such as revenue_amount",
                            "canonical_ref": {"class_name": "string", "slot_name": "string"},
                            "source_field_id": "string|null",
                            "binding_ref": {},
                            "depends_on_binding": "boolean",
                        }
                    ],
                    "operation_link": {
                        "source_operation_id": "string",
                        "priority": "integer",
                        "binding_spec": {},
                    },
                    "confidence": "float",
                    "rationale": "string",
                    "evidence_refs": [],
                }
            ],
            "coverage": {
                "required": "Every operation_context must have one matching suggestion keyed by source_operation_id, operation_key, operation name, or path.",
                "missing_suggestion_behavior": "The ingestion runtime will skip omitted operation contexts and will not derive provider-neutral capability keys heuristically.",
            },
        },
    }


def collect_operation_contexts(
    *,
    source: dict[str, Any],
    document: dict[str, Any],
    operations: list[dict[str, Any]],
    binding_generation: dict[str, Any],
) -> list[dict[str, Any]]:
    binding_by_operation: dict[str, list[dict[str, Any]]] = {}
    for suggestion in binding_generation.get("suggestions", []):
        if not isinstance(suggestion, dict):
            continue
        operation_id = str(suggestion.get("source_operation_id") or "")
        if operation_id:
            binding_by_operation.setdefault(operation_id, []).append(suggestion)
    contexts: list[dict[str, Any]] = []
    for operation in operations:
        operation_id = str(operation.get("id") or "")
        bindings = binding_by_operation.get(operation_id, [])
        contexts.append(
            {
                "source_id": source.get("id"),
                "source_document_id": document.get("id"),
                "operation": {
                    "source_operation_id": operation_id,
                    "operation_key": operation.get("operation_key"),
                    "name": operation.get("name"),
                    "method": operation.get("method"),
                    "path": operation.get("path"),
                    "description": operation.get("description") or "",
                },
                "input_bindings": [
                    _binding_context(item)
                    for item in bindings
                    if item.get("decision") == "bind" and item.get("direction") == "input"
                ],
                "output_bindings": [
                    _binding_context(item)
                    for item in bindings
                    if item.get("decision") == "bind"
                    and item.get("direction") == "output"
                    and _binding_kind(item) == "field"
                ],
                "context_bindings": [
                    _binding_context(item)
                    for item in bindings
                    if item.get("decision") == "bind"
                    and item.get("direction") == "output"
                    and _binding_kind(item) == "context"
                ],
                "skipped_bindings": [
                    _binding_context(item)
                    for item in bindings
                    if item.get("decision") == "skip_binding"
                ],
            }
        )
    return contexts


def capability_suggestions_from_manual_response(
    operation_contexts: list[dict[str, Any]],
    payload: dict[str, Any],
    *,
    allow_heuristic_propose: bool = True,
) -> list[dict[str, Any]]:
    suggestions = payload.get("suggestions") if isinstance(payload.get("suggestions"), list) else []
    manual_index: dict[tuple[str, str], dict[str, Any]] = {}
    for item in suggestions:
        if not isinstance(item, dict):
            continue
        for key in _manual_operation_keys(item):
            manual_index[key] = item
    return [
        _suggestion_from_manual(
            context,
            _manual_for_operation_context(manual_index, context),
            allow_heuristic_propose=allow_heuristic_propose,
        )
        for context in operation_contexts
    ]


def skip_capabilities_without_llm(operation_contexts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [_skip_suggestion(context, "LLM mode is disabled; capability generation requires business review.") for context in operation_contexts]


def suggest_capability_for_operation(context: dict[str, Any]) -> dict[str, Any]:
    operation = context.get("operation") if isinstance(context.get("operation"), dict) else {}
    input_bindings = context.get("input_bindings") if isinstance(context.get("input_bindings"), list) else []
    output_bindings = context.get("output_bindings") if isinstance(context.get("output_bindings"), list) else []
    if not operation.get("source_operation_id"):
        return _skip_suggestion(context, "No executable source operation exists.")
    if not output_bindings:
        return _skip_suggestion(context, "Operation has no output bindings that can define planner-facing results.")

    key = _derive_capability_key(input_bindings, output_bindings, operation)
    if not key:
        return _skip_suggestion(context, "Operation output bindings do not expose a business canonical output class.")
    capability = {
        "capability_key": key,
        "namespace": "public",
        "name": _title_from_key(key),
        "description": _capability_description(input_bindings, output_bindings),
        "intent_spec": {
            "canonical_inputs": [_canonical_ref_for_intent(item) for item in input_bindings],
            "canonical_outputs": [_canonical_ref_for_intent(item) for item in output_bindings],
            "aliases": _capability_aliases(operation, key),
            "examples": [],
        },
        "metadata": {
            "source_document_id": context.get("source_document_id"),
            "proposal_builder": "capability_generation",
        },
    }
    return {
        "decision": "propose_capability",
        "source_operation_id": operation.get("source_operation_id"),
        "operation": operation,
        "capability": capability,
        "inputs": [_capability_input(item, index) for index, item in enumerate(input_bindings)],
        "outputs": [_capability_output(item, index) for index, item in enumerate(output_bindings)],
        "operation_link": {
            "source_operation_id": operation.get("source_operation_id"),
            "priority": 100,
            "binding_spec": {
                "inputs": [_binding_ref(item) for item in input_bindings],
                "outputs": [_binding_ref(item) for item in output_bindings],
                "contexts": [_binding_ref(item) for item in context.get("context_bindings", []) if isinstance(item, dict)],
                "skipped_controls": [_binding_ref(item) for item in context.get("skipped_bindings", []) if isinstance(item, dict)],
            },
        },
        "confidence": 0.82,
        "rationale": "Capability is derived from executable source operation inputs and outputs bound to canonical class-slot usages.",
        "evidence_refs": [{"source_operation_id": operation.get("source_operation_id")}],
        "requires_review": True,
    }


def build_capability_generation_payload(
    *,
    source: dict[str, Any],
    document: dict[str, Any],
    suggestions: list[dict[str, Any]],
    llm_mode: str,
    engine: str,
) -> dict[str, Any]:
    counts = Counter(str(item.get("decision") or "skip_capability") for item in suggestions)
    return {
        "type": "capability_generation",
        "legacy_type": "capability_contracting",
        "llm_mode": llm_mode,
        "engine": engine,
        "source_id": source.get("id"),
        "source_document_id": document.get("id"),
        "operation_count": len(suggestions),
        "decision_counts": {key: counts.get(key, 0) for key in sorted(CAPABILITY_DECISION_TYPES)},
        "suggestions": suggestions,
        "capability_contracts": suggestions,
    }


def _suggestion_from_manual(
    context: dict[str, Any],
    manual: dict[str, Any] | None,
    *,
    allow_heuristic_propose: bool = True,
) -> dict[str, Any]:
    fallback = suggest_capability_for_operation(context)
    if not isinstance(manual, dict):
        if not allow_heuristic_propose and fallback.get("decision") == "propose_capability":
            return _skip_suggestion(context, "LLM response did not include a capability suggestion for this operation.")
        return fallback
    decision = str(manual.get("decision") or fallback.get("decision") or "propose_capability")
    if decision not in CAPABILITY_DECISION_TYPES:
        decision = "propose_capability"
    capability = manual.get("capability") if isinstance(manual.get("capability"), dict) else fallback.get("capability") or {}
    operation_link = _merge_operation_link(
        fallback.get("operation_link") if isinstance(fallback.get("operation_link"), dict) else {},
        manual.get("operation_link") if isinstance(manual.get("operation_link"), dict) else {},
    )
    return {
        **fallback,
        "decision": decision,
        "source_operation_id": str(manual.get("source_operation_id") or fallback.get("source_operation_id") or ""),
        "capability": capability,
        "inputs": manual.get("inputs") if isinstance(manual.get("inputs"), list) else fallback.get("inputs") or [],
        "outputs": manual.get("outputs") if isinstance(manual.get("outputs"), list) else fallback.get("outputs") or [],
        "operation_link": operation_link,
        "confidence": round(float(manual.get("confidence", fallback.get("confidence") or 0.0)), 3),
        "rationale": str(manual.get("rationale") or fallback.get("rationale") or ""),
        "evidence_refs": manual.get("evidence_refs") if isinstance(manual.get("evidence_refs"), list) else fallback.get("evidence_refs") or [],
        "llm_decision": True,
        "requires_review": True,
    }


def _merge_operation_link(fallback: dict[str, Any], manual: dict[str, Any]) -> dict[str, Any]:
    if not manual:
        return fallback
    fallback_spec = fallback.get("binding_spec") if isinstance(fallback.get("binding_spec"), dict) else {}
    manual_spec = manual.get("binding_spec") if isinstance(manual.get("binding_spec"), dict) else {}
    binding_spec = {**fallback_spec}
    for key, value in manual_spec.items():
        if key in {"inputs", "outputs", "contexts", "skipped_controls"} and not _is_ref_list(value):
            continue
        binding_spec[key] = value
    return {
        **fallback,
        **manual,
        "binding_spec": binding_spec,
    }


def _is_ref_list(value: Any) -> bool:
    return isinstance(value, list) and all(isinstance(item, dict) for item in value)


def _manual_for_operation_context(
    manual_index: dict[tuple[str, str], dict[str, Any]],
    context: dict[str, Any],
) -> dict[str, Any] | None:
    operation = context.get("operation") if isinstance(context.get("operation"), dict) else {}
    keys = [
        ("source_operation_id", str(operation.get("source_operation_id") or "")),
        ("operation_key", str(operation.get("operation_key") or "")),
        ("operation_name", str(operation.get("name") or "")),
        ("path", str(operation.get("path") or "")),
    ]
    for key in keys:
        if key[1] and key in manual_index:
            return manual_index[key]
    return None


def _manual_operation_keys(item: dict[str, Any]) -> list[tuple[str, str]]:
    operation = item.get("operation") if isinstance(item.get("operation"), dict) else {}
    candidates = [
        ("source_operation_id", item.get("source_operation_id") or operation.get("source_operation_id")),
        ("operation_key", item.get("operation_key") or operation.get("operation_key")),
        ("operation_name", item.get("operation_name") or operation.get("name")),
        ("path", item.get("path") or operation.get("path")),
    ]
    return [(key, str(value)) for key, value in candidates if str(value or "")]


def _skip_suggestion(context: dict[str, Any], rationale: str) -> dict[str, Any]:
    operation = context.get("operation") if isinstance(context.get("operation"), dict) else {}
    return {
        "decision": "skip_capability",
        "source_operation_id": operation.get("source_operation_id"),
        "operation": operation,
        "capability": {},
        "inputs": [],
        "outputs": [],
        "operation_link": {},
        "confidence": 0.78,
        "rationale": rationale,
        "evidence_refs": [{"source_operation_id": operation.get("source_operation_id")}],
        "requires_review": True,
    }


def _binding_context(item: dict[str, Any]) -> dict[str, Any]:
    canonical_ref = item.get("canonical_ref") if isinstance(item.get("canonical_ref"), dict) else {}
    return {
        "binding_kind": item.get("binding_kind") or _binding_kind(item),
        "source_kind": item.get("source_kind"),
        "source_operation_id": item.get("source_operation_id"),
        "source_parameter_id": item.get("source_parameter_id"),
        "source_field_id": item.get("source_field_id"),
        "field_path": item.get("field_path"),
        "raw_name": item.get("raw_name"),
        "canonical_class_slot_id": item.get("canonical_class_slot_id"),
        "representation_id": item.get("representation_id") or item.get("canonical_class_slot_id"),
        "concept_key": item.get("concept_key"),
        "required_concept_key": item.get("required_concept_key"),
        "representation_key": item.get("representation_key"),
        "representation_schema_key": item.get("representation_schema_key"),
        "context_key": item.get("context_key"),
        "fills_property": item.get("fills_property"),
        "canonical_ref": {
            "class_name": canonical_ref.get("class_name") or canonical_ref.get("entity_name") or "",
            "slot_name": canonical_ref.get("slot_name") or "",
        },
        "direction": item.get("direction"),
        "binding_type": item.get("binding_type"),
        "transform_spec": item.get("transform_spec") if isinstance(item.get("transform_spec"), dict) else {},
        "normalization_rule": item.get("normalization_rule") if isinstance(item.get("normalization_rule"), dict) else {},
        "enum_mapping": item.get("enum_mapping") if isinstance(item.get("enum_mapping"), dict) else {},
        "depends_on_canonical_decision": bool(item.get("depends_on_canonical_decision")),
        "confidence": item.get("confidence"),
    }


def _binding_kind(item: dict[str, Any]) -> str:
    value = str(item.get("binding_kind") or "")
    if value in {"field", "context", "parameter"}:
        return value
    if item.get("context_key"):
        return "context"
    if str(item.get("source_kind") or "") == "parameter":
        return "parameter"
    return "field"


def _derive_capability_key(input_bindings: list[dict[str, Any]], output_bindings: list[dict[str, Any]], operation: dict[str, Any]) -> str:
    input_class = _first_class(input_bindings, exclude=NON_BUSINESS_CLASS_NAMES)
    output_class = _first_class(output_bindings, exclude=NON_BUSINESS_CLASS_NAMES)
    if not output_class:
        return ""
    action = "lookup"
    if input_class and input_class != output_class:
        base = f"{input_class}.{output_class}.{action}"
    else:
        base = f"{output_class}.{action}"
    return _to_capability_key(base) or _to_capability_key(f"{operation.get('name') or 'operation'}.{action}")


def _first_class(bindings: list[dict[str, Any]], *, exclude: set[str]) -> str:
    for item in bindings:
        canonical_ref = item.get("canonical_ref") if isinstance(item.get("canonical_ref"), dict) else {}
        class_name = str(canonical_ref.get("class_name") or canonical_ref.get("entity_name") or "")
        normalized = _normalized_class_key(class_name)
        if class_name and normalized not in exclude and normalized.replace("_", "") not in exclude:
            return class_name
    return ""


def _normalized_class_key(value: str) -> str:
    value = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", str(value or ""))
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", value)
    return re.sub(r"[^a-z0-9_]+", "", value.lower())


def _capability_description(input_bindings: list[dict[str, Any]], output_bindings: list[dict[str, Any]]) -> str:
    inputs = ", ".join(_canonical_label(item) for item in input_bindings[:3] if _canonical_label(item))
    outputs = ", ".join(_canonical_label(item) for item in output_bindings[:4] if _canonical_label(item))
    if inputs:
        return f"Lookup {outputs or 'canonical outputs'} using {inputs}."
    return f"Lookup {outputs or 'canonical outputs'}."


def _capability_aliases(operation: dict[str, Any], key: str) -> list[str]:
    aliases = [str(operation.get("name") or ""), str(operation.get("operation_key") or ""), key]
    return [item for item in dict.fromkeys(aliases) if item]


def _capability_input(item: dict[str, Any], index: int) -> dict[str, Any]:
    return {
        "canonical_class_slot_id": item.get("canonical_class_slot_id"),
        "representation_id": item.get("representation_id") or item.get("canonical_class_slot_id"),
        "concept_key": item.get("concept_key") or item.get("required_concept_key"),
        "representation_key": item.get("representation_key"),
        "representation_schema_key": item.get("representation_schema_key"),
        "canonical_ref": item.get("canonical_ref") if isinstance(item.get("canonical_ref"), dict) else {},
        "required": True,
        "input_order": (index + 1) * 10,
        "source_parameter_id": item.get("source_parameter_id"),
        "binding_ref": _binding_ref(item),
        "depends_on_binding": True,
        "depends_on_canonical_decision": bool(item.get("depends_on_canonical_decision")),
    }


def _capability_output(item: dict[str, Any], index: int) -> dict[str, Any]:
    canonical_ref = item.get("canonical_ref") if isinstance(item.get("canonical_ref"), dict) else {}
    return {
        "canonical_class_slot_id": item.get("canonical_class_slot_id"),
        "representation_id": item.get("representation_id") or item.get("canonical_class_slot_id"),
        "concept_key": item.get("concept_key"),
        "representation_key": item.get("representation_key"),
        "representation_schema_key": item.get("representation_schema_key"),
        "output_key": item.get("output_key") or _output_key(canonical_ref),
        "canonical_ref": canonical_ref,
        "output_order": (index + 1) * 10,
        "source_field_id": item.get("source_field_id"),
        "binding_ref": _binding_ref(item),
        "depends_on_binding": True,
        "depends_on_canonical_decision": bool(item.get("depends_on_canonical_decision")),
    }


def _binding_ref(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "binding_kind": item.get("binding_kind") or _binding_kind(item),
        "source_parameter_id": item.get("source_parameter_id"),
        "source_field_id": item.get("source_field_id"),
        "field_path": item.get("field_path"),
        "canonical_class_slot_id": item.get("canonical_class_slot_id"),
        "representation_id": item.get("representation_id") or item.get("canonical_class_slot_id"),
        "concept_key": item.get("concept_key"),
        "required_concept_key": item.get("required_concept_key"),
        "representation_key": item.get("representation_key"),
        "representation_schema_key": item.get("representation_schema_key"),
        "context_key": item.get("context_key"),
        "fills_property": item.get("fills_property"),
        "canonical_ref": item.get("canonical_ref") if isinstance(item.get("canonical_ref"), dict) else {},
        "transform_spec": item.get("transform_spec") if isinstance(item.get("transform_spec"), dict) else {},
        "normalization_rule": item.get("normalization_rule") if isinstance(item.get("normalization_rule"), dict) else {},
        "enum_mapping": item.get("enum_mapping") if isinstance(item.get("enum_mapping"), dict) else {},
    }


def _output_key(canonical_ref: dict[str, Any]) -> str:
    class_name = str(canonical_ref.get("class_name") or "").strip()
    slot_name = str(canonical_ref.get("slot_name") or "").strip()
    if class_name and slot_name:
        return f"{_normalized_class_key(class_name)}_{_normalized_class_key(slot_name)}"
    return _normalized_class_key(slot_name or class_name)


def _canonical_ref_for_intent(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "canonical_class_slot_id": item.get("canonical_class_slot_id"),
        "canonical_ref": item.get("canonical_ref") if isinstance(item.get("canonical_ref"), dict) else {},
    }


def _canonical_label(item: dict[str, Any]) -> str:
    canonical_ref = item.get("canonical_ref") if isinstance(item.get("canonical_ref"), dict) else {}
    class_name = str(canonical_ref.get("class_name") or canonical_ref.get("entity_name") or "")
    attribute = str(canonical_ref.get("slot_name") or "")
    return f"{class_name}.{attribute}" if class_name and attribute else attribute


def _title_from_key(value: str) -> str:
    return " ".join(part.capitalize() for part in re.split(r"[._-]+", value) if part)


def _to_capability_key(value: str) -> str:
    normalized = re.sub(r"[^a-zA-Z0-9._]+", ".", value).strip(".").lower()
    normalized = re.sub(r"\.+", ".", normalized)
    return normalized


def _resolve_llm_mode(override: str | None = None) -> str:
    return resolve_llm_mode(override)
