from __future__ import annotations

import re
from collections import Counter
from typing import Any

from services.context_platform.internal.ingestion.langgraph.common import resolve_llm_mode

BINDING_DECISION_TYPES = {"bind", "skip_binding", "conflict"}
BINDING_TYPES = {"exact", "transform", "composite", "enum", "reference"}


def build_binding_generation(
    *,
    run_id: str,
    source: dict[str, Any],
    document: dict[str, Any],
    operations: list[dict[str, Any]],
    document_fields: list[dict[str, Any]],
    canonical_reconciliation: dict[str, Any],
    llm_mode: str | None = None,
    manual_llm_response: dict[str, Any] | None = None,
) -> dict[str, Any]:
    request = build_manual_binding_generation_request(
        run_id=run_id,
        source=source,
        document=document,
        operations=operations,
        document_fields=document_fields,
        canonical_reconciliation=canonical_reconciliation,
    )
    resolved_llm_mode = _resolve_llm_mode(llm_mode)
    if resolved_llm_mode == "agent_manual" and isinstance(manual_llm_response, dict):
        suggestions = binding_suggestions_from_manual_response(request["source_terms"], manual_llm_response, allow_heuristic_bind=False)
        engine = "agent_manual_resolution_generation"
    else:
        suggestions = suggest_bindings_without_llm(request["source_terms"])
        engine = "no_llm_resolution_generation"
    return build_binding_generation_payload(
        source=source,
        document=document,
        canonical_reconciliation=canonical_reconciliation,
        suggestions=suggestions,
        llm_mode=resolved_llm_mode,
        engine=engine,
    )


def build_manual_binding_generation_request(
    *,
    run_id: str,
    source: dict[str, Any],
    document: dict[str, Any],
    operations: list[dict[str, Any]],
    document_fields: list[dict[str, Any]],
    canonical_reconciliation: dict[str, Any],
) -> dict[str, Any]:
    terms = collect_binding_terms(
        source=source,
        document=document,
        operations=operations,
        document_fields=document_fields,
        canonical_reconciliation=canonical_reconciliation,
    )
    return {
        "type": "resolution_generation",
        "legacy_type": "binding_generation",
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
            "Create reviewable resolution suggestions from source terms to CanonicalRepresentations and their context/schema slots.",
            "Include declarative transformation and normalization specs when raw source values and representation schemas differ.",
            "Do not generate executable code. Use rule ids and parameters only.",
            "Use skip_binding for provider-only controls such as service keys, pagination, or response format flags.",
            "Preserve meaning resolution decisions. If the meaning decision is skip, return skip_binding and leave canonical_ref class_name and slot_name empty.",
            "Do not infer provider meaning from raw-name keywords or fill fallback canonical refs for skipped terms.",
            "When the CanonicalRepresentation is proposed but not yet approved, keep canonical_class_slot_id/representation_id null and set depends_on_canonical_decision true.",
            "Prefer active response keys field_bindings, context_bindings, parameter_bindings, and transform_rules. The legacy suggestions key is accepted only for compatibility.",
            "Return exactly one binding decision for every source_terms item across field_bindings/context_bindings/parameter_bindings or suggestions. Missing suggestions are treated as conflict, not automatic binds.",
        ],
        "approved_rule_catalog": _approved_rule_catalog(),
        "source_terms": terms,
        "response_contract": {
            "coverage": {
                "required": "Every source_terms item must have one matching suggestion.",
                "missing_suggestion_behavior": "The ingestion runtime will mark omitted source terms as conflict and will not infer binding decisions heuristically.",
            },
            "field_bindings": [
                {
                    "source_kind": "field",
                    "source_field_id": "string|null",
                    "field_path": "string",
                    "raw_name": "string",
                    "decision": "bind|skip_binding|conflict",
                    "canonical_class_slot_id": "string|null legacy alias for representation_id",
                    "representation_id": "string|null",
                    "representation_key": "repr.namespace.meaning.carrier_property|null",
                    "representation_schema_key": "schema.namespace.meaning.format|null",
                    "concept_key": "concept.namespace.meaning|null",
                    "fills_property": "property.observed_amount|property.identifier_value|...",
                    "canonical_ref": {
                        "class_name": "legacy carrier object name",
                        "slot_name": "legacy value property name",
                    },
                    "direction": "output",
                    "binding_type": "exact|transform|composite|enum|reference",
                    "transform_spec": {
                        "type": "none|cast|normalization_rule|enum_mapping|composite",
                        "rule_id": "string",
                        "params": {},
                    },
                    "normalization_rule": {
                        "rule_id": "string",
                        "params": {},
                    },
                    "enum_mapping": {},
                    "depends_on_canonical_decision": "boolean",
                    "confidence": "float",
                    "rationale": "string",
                    "evidence_refs": ["list of evidence ref objects"],
                }
            ],
            "context_bindings": [
                {
                    "source_kind": "field",
                    "source_field_id": "string|null",
                    "field_path": "string",
                    "raw_name": "string",
                    "decision": "bind|skip_binding|conflict",
                    "representation_id": "string|null",
                    "representation_key": "repr.namespace.meaning.carrier_property|null",
                    "representation_schema_key": "schema.namespace.meaning.format|null",
                    "concept_key": "concept.namespace.context|null",
                    "context_key": "currency|fiscal_year|statement_type|subject|source|observed_date",
                    "direction": "output",
                    "binding_type": "exact|transform|enum|reference",
                    "transform_spec": {"type": "none|cast|normalization_rule|enum_mapping", "rule_id": "string", "params": {}},
                    "confidence": "float",
                    "rationale": "string",
                    "evidence_refs": ["list of evidence ref objects"],
                }
            ],
            "parameter_bindings": [
                {
                    "source_kind": "parameter",
                    "source_parameter_id": "string|null",
                    "field_path": "string",
                    "raw_name": "string",
                    "decision": "bind|skip_binding|conflict",
                    "required_concept_key": "concept.identifier.kr_corporate_registration_number|concept.time.fiscal_year|...",
                    "representation_id": "string|null",
                    "representation_key": "repr.identifier...|repr.time...|null",
                    "representation_schema_key": "schema.identifier...|schema.time...|null",
                    "direction": "input",
                    "binding_type": "exact|transform|composite|enum|reference",
                    "transform_spec": {"type": "none|cast|normalization_rule|enum_mapping|composite", "rule_id": "string", "params": {}},
                    "normalization_rule": {"rule_id": "string", "params": {}},
                    "confidence": "float",
                    "rationale": "string",
                    "evidence_refs": ["list of evidence ref objects"],
                }
            ],
            "transform_rules": [
                {
                    "rule_id": "string",
                    "rule_type": "parse|normalize|cast|enum_mapping|compose",
                    "description": "declarative transform rule only; no executable code",
                    "params": {},
                }
            ],
            "suggestions": "legacy alias for the combined field/context/parameter binding arrays."
        },
    }


def collect_binding_terms(
    *,
    source: dict[str, Any],
    document: dict[str, Any],
    operations: list[dict[str, Any]],
    document_fields: list[dict[str, Any]],
    canonical_reconciliation: dict[str, Any],
) -> list[dict[str, Any]]:
    decision_index = _canonical_decision_index(canonical_reconciliation)
    terms: list[dict[str, Any]] = []
    for operation in operations:
        operation_ref = {
            "source_operation_id": operation.get("id"),
            "operation_key": operation.get("operation_key"),
            "operation_name": operation.get("name"),
            "method": operation.get("method"),
            "path": operation.get("path"),
        }
        for parameter in operation.get("parameters", []):
            raw_name = str(parameter.get("raw_name") or parameter.get("name") or "")
            field_path = str(parameter.get("parameter_path") or f"request.{raw_name}")
            decision = _decision_for_source_record(decision_index, "parameter", parameter, operation_ref, field_path)
            terms.append(
                _source_term(
                    source=source,
                    document=document,
                    source_kind="parameter",
                    source_record=parameter,
                    operation_ref=operation_ref,
                    field_path=field_path,
                    raw_name=raw_name,
                    direction=_source_direction(decision, "input"),
                    canonical_decision=decision,
                )
            )
        for field in operation.get("fields", []):
            field_path = str(field.get("field_path") or field.get("raw_name") or "")
            decision = _decision_for_source_record(decision_index, "field", field, operation_ref, field_path)
            terms.append(
                _source_term(
                    source=source,
                    document=document,
                    source_kind="field",
                    source_record=field,
                    operation_ref=operation_ref,
                    field_path=field_path,
                    raw_name=str(field.get("raw_name") or ""),
                    direction="output",
                    canonical_decision=decision,
                )
            )
    for field in document_fields:
        field_path = str(field.get("field_path") or field.get("raw_name") or "")
        decision = _decision_for_source_record(decision_index, "field", field, {}, field_path)
        terms.append(
            _source_term(
                source=source,
                document=document,
                source_kind="field",
                source_record=field,
                operation_ref={},
                field_path=field_path,
                raw_name=str(field.get("raw_name") or ""),
                direction="output",
                canonical_decision=decision,
            )
        )
    return terms


def binding_suggestions_from_manual_response(
    terms: list[dict[str, Any]],
    payload: dict[str, Any],
    *,
    allow_heuristic_bind: bool = True,
) -> list[dict[str, Any]]:
    suggestions = payload.get("suggestions") if isinstance(payload.get("suggestions"), list) else []
    manual_index: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    manual_path_index: dict[tuple[str, str], dict[str, Any]] = {}
    term_path_counts = Counter(_term_path_key(term) for term in terms)
    for item in suggestions:
        if not isinstance(item, dict):
            continue
        for key in _manual_suggestion_keys(item):
            manual_index[key] = item
        manual_path_index[_manual_path_key(item)] = item
    return [
        _suggestion_from_manual(
            term,
            _manual_for_term(term, manual_index, manual_path_index, term_path_counts),
            allow_heuristic_bind=allow_heuristic_bind,
        )
        for term in terms
    ]


def suggest_bindings_without_llm(terms: list[dict[str, Any]]) -> list[dict[str, Any]]:
    suggestions: list[dict[str, Any]] = []
    for term in terms:
        fallback = suggest_binding_for_term(term)
        if fallback.get("decision") == "skip_binding":
            suggestions.append(fallback)
        else:
            suggestions.append(_unresolved_binding_suggestion(term, "LLM mode is disabled; binding generation requires review."))
    return suggestions


def suggest_binding_for_term(term: dict[str, Any]) -> dict[str, Any]:
    canonical = term.get("canonical") if isinstance(term.get("canonical"), dict) else {}
    direction = _valid_binding_direction(str(term.get("direction") or "output"))
    canonical_decision = term.get("canonical_decision") if isinstance(term.get("canonical_decision"), dict) else {}
    if canonical_decision.get("decision") == "conflict":
        return _unresolved_binding_suggestion(term, "Canonical reconciliation is unresolved; binding cannot be reviewed as bind.")
    if _should_skip_binding(term):
        return _base_suggestion(
            term,
            decision="skip_binding",
            direction=direction,
            binding_type="exact",
            transform_spec={"type": "none"},
            normalization_rule={},
            confidence=0.88,
            rationale="Provider control or operational parameter is not a planner-facing canonical binding.",
        )
    if not canonical.get("slot_name") and not canonical.get("canonical_class_slot_id"):
        return _base_suggestion(
            term,
            decision="conflict",
            direction=direction,
            binding_type="exact",
            transform_spec={"type": "none"},
            normalization_rule={},
            confidence=0.32,
            rationale="Canonical reconciliation did not provide a usable canonical target.",
        )
    transform_spec, normalization_rule, binding_type = infer_transformation_spec(term, canonical)
    return _base_suggestion(
        term,
        decision="bind",
        direction=direction,
        binding_type=binding_type,
        transform_spec=transform_spec,
        normalization_rule=normalization_rule,
        confidence=0.86 if not term.get("depends_on_canonical_decision") else 0.74,
        rationale="Source term can be bound to the canonical class-slot usage selected by canonical reconciliation.",
    )


def infer_transformation_spec(term: dict[str, Any], canonical: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any], str]:
    raw_name = str(term.get("raw_name") or "")
    field_path = str(term.get("field_path") or "")
    description = str(term.get("description") or "")
    data_type = str(term.get("data_type") or "").lower()
    canonical_datatype = str(canonical.get("datatype") or data_type or "string").lower()
    text = f"{raw_name} {field_path} {description} {canonical.get('slot_name') or ''}".lower()

    if "date" in canonical_datatype or "날짜" in description or re.search(r"(basdt|ymd|date)$", raw_name, flags=re.IGNORECASE):
        return (
            {"type": "normalization_rule", "rule_id": "parse_yyyymmdd_date", "params": {"output_format": "ISO_DATE"}},
            {"rule_id": "parse_yyyymmdd_date", "params": {"input_format": "YYYYMMDD", "output_format": "ISO_DATE"}},
            "transform",
        )
    if canonical_datatype in {"number", "integer", "decimal", "float", "double"} or any(token in text for token in ["amount", "amt", "금액", "수", "율"]):
        target_type = "integer" if canonical_datatype == "integer" else "decimal"
        return (
            {"type": "cast", "target_type": target_type, "null_values": ["", "-", "N/A", "null"]},
            {"rule_id": "parse_decimal", "params": {"null_values": ["", "-", "N/A", "null"]}},
            "transform",
        )
    if "currency" in text or "통화" in description or raw_name.lower() in {"curcd", "currency", "currencycode"}:
        return (
            {"type": "normalization_rule", "rule_id": "uppercase_iso_currency_code", "params": {}},
            {"rule_id": "uppercase_iso_currency_code", "params": {}},
            "transform",
        )
    if "registration_number" in str(canonical.get("slot_name") or "") or "사업자" in description or "법인등록번호" in description:
        return (
            {"type": "normalization_rule", "rule_id": "normalize_identifier_digits", "params": {"keep": "digits"}},
            {"rule_id": "normalize_identifier_digits", "params": {"keep": "digits"}},
            "transform",
        )
    return ({"type": "none"}, {}, "exact")


def build_binding_generation_payload(
    *,
    source: dict[str, Any],
    document: dict[str, Any],
    canonical_reconciliation: dict[str, Any],
    suggestions: list[dict[str, Any]],
    llm_mode: str,
    engine: str,
) -> dict[str, Any]:
    counts = Counter(str(item.get("decision") or "bind") for item in suggestions)
    return {
        "type": "resolution_generation",
        "legacy_type": "binding_generation",
        "llm_mode": llm_mode,
        "engine": engine,
        "source_id": source.get("id"),
        "source_document_id": document.get("id"),
        "meaning_resolution_engine": canonical_reconciliation.get("engine"),
        "canonical_reconciliation_engine": canonical_reconciliation.get("engine"),
        "term_count": len(suggestions),
        "decision_counts": {key: counts.get(key, 0) for key in sorted(BINDING_DECISION_TYPES)},
        "suggestions": suggestions,
        "resolution_suggestions": suggestions,
        "field_bindings": [item for item in suggestions if item.get("binding_kind") == "field"],
        "context_bindings": [item for item in suggestions if item.get("binding_kind") == "context"],
        "parameter_bindings": [item for item in suggestions if item.get("binding_kind") == "parameter"],
    }


def _source_term(
    *,
    source: dict[str, Any],
    document: dict[str, Any],
    source_kind: str,
    source_record: dict[str, Any],
    operation_ref: dict[str, Any],
    field_path: str,
    raw_name: str,
    direction: str,
    canonical_decision: dict[str, Any],
) -> dict[str, Any]:
    canonical = _canonical_ref(canonical_decision)
    return {
        "source_kind": source_kind,
        "source_id": source.get("id"),
        "source_document_id": document.get("id"),
        "source_operation_id": operation_ref.get("source_operation_id"),
        "source_parameter_id": source_record.get("id") if source_kind == "parameter" else None,
        "source_field_id": source_record.get("id") if source_kind == "field" else None,
        "field_path": field_path,
        "raw_name": raw_name,
        "source_direction": direction,
        "direction": _valid_binding_direction(direction),
        "data_type": source_record.get("data_type") or "string",
        "is_required": bool(source_record.get("is_required")),
        "description": source_record.get("description") or "",
        "operation": operation_ref,
        "canonical_decision": {
            "decision": canonical_decision.get("decision"),
            "confidence": canonical_decision.get("confidence"),
            "rationale": canonical_decision.get("rationale"),
        },
        "concept_key": canonical_decision.get("concept_key"),
        "representation_key": canonical_decision.get("representation_key"),
        "representation_schema_key": canonical_decision.get("representation_schema_key"),
        "value_domain_key": canonical_decision.get("value_domain_key"),
        "proposed_representation": canonical_decision.get("proposed_representation")
        if isinstance(canonical_decision.get("proposed_representation"), dict)
        else {},
        "representation_schema": canonical_decision.get("representation_schema")
        if isinstance(canonical_decision.get("representation_schema"), dict)
        else {},
        "canonical": canonical,
        "depends_on_canonical_decision": not bool(canonical.get("canonical_class_slot_id")),
        "evidence_refs": canonical_decision.get("evidence_refs")
        if isinstance(canonical_decision.get("evidence_refs"), list)
        else source_record.get("evidence")
        if isinstance(source_record.get("evidence"), list)
        else [{"source_document_id": document.get("id"), "field_path": field_path}],
    }


def _canonical_ref(canonical_decision: dict[str, Any]) -> dict[str, Any]:
    matched = canonical_decision.get("matched_canonical_object") if isinstance(canonical_decision.get("matched_canonical_object"), dict) else {}
    proposed = canonical_decision.get("proposed_canonical") if isinstance(canonical_decision.get("proposed_canonical"), dict) else {}
    return {
        "canonical_class_slot_id": matched.get("id") if matched.get("object_type") == "canonical_class_slot" else None,
        "canonical_class_id": (matched.get("class_id") or matched.get("id")) if matched.get("object_type") == "canonical_class" else None,
        "concept_key": canonical_decision.get("concept_key"),
        "representation_key": canonical_decision.get("representation_key"),
        "representation_schema_key": canonical_decision.get("representation_schema_key"),
        "value_domain_key": canonical_decision.get("value_domain_key"),
        "class_name": proposed.get("class_name") or proposed.get("entity_name") or matched.get("class_name") or matched.get("entity_name") or "",
        "slot_name": proposed.get("slot_name") or proposed.get("attribute_name") or matched.get("name") or "",
        "datatype": proposed.get("datatype") or "",
        "identity_role": proposed.get("identity_role") or "",
    }


def _suggestion_from_manual(
    term: dict[str, Any],
    manual: dict[str, Any] | None,
    *,
    allow_heuristic_bind: bool = True,
) -> dict[str, Any]:
    fallback = suggest_binding_for_term(term)
    canonical_decision = term.get("canonical_decision") if isinstance(term.get("canonical_decision"), dict) else {}
    if canonical_decision.get("decision") == "conflict":
        return _unresolved_binding_suggestion(term, "Canonical reconciliation is unresolved; manual binding cannot override it.")
    if not isinstance(manual, dict):
        if not allow_heuristic_bind and fallback.get("decision") != "skip_binding":
            return _unresolved_binding_suggestion(term, "LLM response did not include a binding suggestion for this source term.")
        return fallback
    decision = str(manual.get("decision") or fallback.get("decision") or "bind")
    if decision not in BINDING_DECISION_TYPES:
        decision = "bind"
    binding_type = str(manual.get("binding_type") or fallback.get("binding_type") or "exact")
    if binding_type not in BINDING_TYPES:
        binding_type = "exact"
    canonical_ref = manual.get("canonical_ref") if isinstance(manual.get("canonical_ref"), dict) else {}
    transform_spec = manual.get("transform_spec") if isinstance(manual.get("transform_spec"), dict) else fallback.get("transform_spec") or {}
    normalization_rule = manual.get("normalization_rule") if isinstance(manual.get("normalization_rule"), dict) else fallback.get("normalization_rule") or {}
    enum_mapping = manual.get("enum_mapping") if isinstance(manual.get("enum_mapping"), dict) else {}
    if decision == "skip_binding":
        canonical_class_slot_id = None
        resolved_canonical_ref = {"class_name": "", "slot_name": ""}
    else:
        canonical_class_slot_id = str(manual.get("canonical_class_slot_id") or fallback.get("canonical_class_slot_id") or "") or None
        resolved_canonical_ref = {
            "class_name": str(canonical_ref.get("class_name") or canonical_ref.get("entity_name") or fallback.get("canonical_ref", {}).get("class_name") or ""),
            "slot_name": str(canonical_ref.get("slot_name") or fallback.get("canonical_ref", {}).get("slot_name") or ""),
        }
    return {
        **fallback,
        "binding_kind": str(manual.get("binding_kind") or fallback.get("binding_kind") or _binding_kind_from_suggestion(manual)),
        "decision": decision,
        "canonical_class_slot_id": canonical_class_slot_id,
        "representation_id": str(manual.get("representation_id") or canonical_class_slot_id or "") or None,
        "representation_key": str(manual.get("representation_key") or fallback.get("representation_key") or "") or None,
        "representation_schema_key": str(manual.get("representation_schema_key") or fallback.get("representation_schema_key") or "") or None,
        "concept_key": str(manual.get("concept_key") or fallback.get("concept_key") or "") or None,
        "required_concept_key": str(manual.get("required_concept_key") or fallback.get("required_concept_key") or "") or None,
        "context_key": str(manual.get("context_key") or fallback.get("context_key") or "") or None,
        "fills_property": str(manual.get("fills_property") or fallback.get("fills_property") or "") or None,
        "canonical_ref": resolved_canonical_ref,
        "direction": _valid_binding_direction(str(manual.get("direction") or fallback.get("direction") or "output")),
        "binding_type": binding_type,
        "transform_spec": transform_spec,
        "normalization_rule": normalization_rule,
        "enum_mapping": enum_mapping,
        "depends_on_canonical_decision": bool(manual.get("depends_on_canonical_decision", fallback.get("depends_on_canonical_decision"))),
        "confidence": round(float(manual.get("confidence", fallback.get("confidence") or 0.0)), 3),
        "rationale": str(manual.get("rationale") or fallback.get("rationale") or ""),
        "evidence_refs": manual.get("evidence_refs") if isinstance(manual.get("evidence_refs"), list) else fallback.get("evidence_refs") or [],
        "llm_decision": True,
    }


def _unresolved_binding_suggestion(term: dict[str, Any], rationale: str) -> dict[str, Any]:
    suggestion = _base_suggestion(
        term,
        decision="conflict",
        direction=_valid_binding_direction(str(term.get("direction") or "output")),
        binding_type="exact",
        transform_spec={"type": "none"},
        normalization_rule={},
        confidence=0.0,
        rationale=rationale,
    )
    suggestion["canonical_class_slot_id"] = None
    suggestion["canonical_ref"] = {"class_name": "", "slot_name": ""}
    suggestion["depends_on_canonical_decision"] = False
    return suggestion


def _base_suggestion(
    term: dict[str, Any],
    *,
    decision: str,
    direction: str,
    binding_type: str,
    transform_spec: dict[str, Any],
    normalization_rule: dict[str, Any],
    confidence: float,
    rationale: str,
) -> dict[str, Any]:
    canonical = term.get("canonical") if isinstance(term.get("canonical"), dict) else {}
    canonical_ref = {"class_name": "", "slot_name": ""}
    canonical_class_slot_id = None
    if decision != "skip_binding":
        canonical_class_slot_id = canonical.get("canonical_class_slot_id")
        canonical_ref = {
            "class_name": canonical.get("class_name") or "",
            "slot_name": canonical.get("slot_name") or "",
        }
    return {
        "binding_kind": _binding_kind_for_term(term),
        "decision": decision,
        "source_kind": term.get("source_kind"),
        "source_id": term.get("source_id"),
        "source_document_id": term.get("source_document_id"),
        "source_operation_id": term.get("source_operation_id"),
        "source_parameter_id": term.get("source_parameter_id"),
        "source_field_id": term.get("source_field_id"),
        "field_path": term.get("field_path"),
        "raw_name": term.get("raw_name"),
        "canonical_class_slot_id": canonical_class_slot_id,
        "representation_id": canonical_class_slot_id,
        "representation_key": term.get("representation_key"),
        "representation_schema_key": term.get("representation_schema_key"),
        "concept_key": term.get("concept_key"),
        "required_concept_key": term.get("required_concept_key"),
        "context_key": term.get("context_key"),
        "fills_property": canonical_ref.get("slot_name") or None,
        "canonical_ref": canonical_ref,
        "direction": direction,
        "binding_type": binding_type,
        "transform_spec": transform_spec,
        "normalization_rule": normalization_rule,
        "enum_mapping": {},
        "depends_on_canonical_decision": bool(term.get("depends_on_canonical_decision")),
        "confidence": round(float(confidence), 3),
        "rationale": rationale,
        "evidence_refs": term.get("evidence_refs") if isinstance(term.get("evidence_refs"), list) else [],
        "requires_review": True,
    }


def _should_skip_binding(term: dict[str, Any]) -> bool:
    raw_name = str(term.get("raw_name") or "").lower()
    direction = str(term.get("source_direction") or term.get("direction") or "").lower()
    description = str(term.get("description") or "").lower()
    canonical_decision = term.get("canonical_decision") if isinstance(term.get("canonical_decision"), dict) else {}
    if canonical_decision.get("decision") == "skip":
        return True
    if direction == "control":
        return True
    if raw_name in {"servicekey", "pageno", "numofrows", "resulttype"}:
        return True
    return raw_name.endswith("key") and "service" in raw_name or "인증키" in description


def _binding_kind_for_term(term: dict[str, Any]) -> str:
    if str(term.get("source_kind") or "") == "parameter":
        return "parameter"
    if term.get("context_key"):
        return "context"
    return "field"


def _binding_kind_from_suggestion(item: dict[str, Any]) -> str:
    if item.get("context_key"):
        return "context"
    if str(item.get("source_kind") or "") == "parameter":
        return "parameter"
    return "field"


def _source_direction(canonical_decision: dict[str, Any], fallback: str) -> str:
    source_term = canonical_decision.get("source_term") if isinstance(canonical_decision.get("source_term"), dict) else {}
    value = str(source_term.get("direction") or fallback)
    return value if value in {"input", "output", "control"} else fallback


def _valid_binding_direction(value: str) -> str:
    return "output" if value == "output" else "input"


def _canonical_decision_index(canonical_reconciliation: dict[str, Any]) -> dict[tuple[str, str, str, str], dict[str, Any]]:
    index: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    path_counts: Counter[tuple[str, str]] = Counter()
    decisions = [item for item in canonical_reconciliation.get("decisions", []) if isinstance(item, dict)]
    for decision in decisions:
        source_term = decision.get("source_term") if isinstance(decision.get("source_term"), dict) else {}
        path_counts[(str(source_term.get("source_kind") or ""), str(source_term.get("field_path") or ""))] += 1
    for decision in decisions:
        source_term = decision.get("source_term") if isinstance(decision.get("source_term"), dict) else {}
        source_kind = str(source_term.get("source_kind") or "")
        operation_id = str(source_term.get("source_operation_id") or "")
        source_id = str(source_term.get("source_parameter_id") or source_term.get("source_field_id") or "")
        field_path = str(source_term.get("field_path") or "")
        index[(source_kind, operation_id, source_id, field_path)] = decision
        if source_id:
            index[(source_kind, "", source_id, field_path)] = decision
        if operation_id and field_path:
            index[(source_kind, operation_id, "", field_path)] = decision
        if path_counts[(source_kind, field_path)] == 1:
            index[(source_kind, "", "", field_path)] = decision
    return index


def _decision_for_source_record(
    index: dict[tuple[str, str, str, str], dict[str, Any]],
    source_kind: str,
    source_record: dict[str, Any],
    operation_ref: dict[str, Any],
    field_path: str,
) -> dict[str, Any]:
    source_id = str(source_record.get("id") or "")
    operation_id = str(operation_ref.get("source_operation_id") or "")
    for key in (
        (source_kind, operation_id, source_id, field_path),
        (source_kind, "", source_id, field_path),
        (source_kind, operation_id, "", field_path),
        (source_kind, "", "", field_path),
    ):
        decision = index.get(key)
        if decision:
            return decision
    return {}


def _term_key(term: dict[str, Any]) -> tuple[str, str, str, str]:
    return (
        str(term.get("source_kind") or ""),
        str(term.get("source_operation_id") or ""),
        str(term.get("source_parameter_id") or term.get("source_field_id") or ""),
        str(term.get("field_path") or ""),
    )


def _term_id_key(term: dict[str, Any]) -> tuple[str, str, str, str]:
    return (
        str(term.get("source_kind") or ""),
        "",
        str(term.get("source_parameter_id") or term.get("source_field_id") or ""),
        str(term.get("field_path") or ""),
    )


def _term_operation_path_key(term: dict[str, Any]) -> tuple[str, str, str, str]:
    return (
        str(term.get("source_kind") or ""),
        str(term.get("source_operation_id") or ""),
        "",
        str(term.get("field_path") or ""),
    )


def _term_path_key(term: dict[str, Any]) -> tuple[str, str]:
    return (str(term.get("source_kind") or ""), str(term.get("field_path") or ""))


def _manual_path_key(item: dict[str, Any]) -> tuple[str, str]:
    return (str(item.get("source_kind") or ""), str(item.get("field_path") or ""))


def _manual_suggestion_keys(item: dict[str, Any]) -> list[tuple[str, str, str, str]]:
    source_kind = str(item.get("source_kind") or "")
    operation_id = str(item.get("source_operation_id") or "")
    source_id = str(item.get("source_parameter_id") or item.get("source_field_id") or "")
    field_path = str(item.get("field_path") or "")
    raw_name = str(item.get("raw_name") or "")
    keys = [(
        source_kind,
        operation_id,
        source_id,
        field_path,
    )]
    if source_id:
        keys.append((source_kind, "", source_id, field_path))
    if operation_id and field_path:
        keys.append((source_kind, operation_id, "", field_path))
    if operation_id and raw_name:
        keys.append((source_kind, operation_id, source_id, f"raw:{raw_name}"))
        keys.append((source_kind, operation_id, "", f"raw:{raw_name}"))
    for operation_ref in _manual_operation_refs(item):
        keys.append((source_kind, operation_ref, source_id, field_path))
        keys.append((source_kind, operation_ref, "", field_path))
        if raw_name:
            keys.append((source_kind, operation_ref, source_id, f"raw:{raw_name}"))
            keys.append((source_kind, operation_ref, "", f"raw:{raw_name}"))
    return list(dict.fromkeys(keys))


def _manual_for_term(
    term: dict[str, Any],
    manual_index: dict[tuple[str, str, str, str], dict[str, Any]],
    manual_path_index: dict[tuple[str, str], dict[str, Any]],
    term_path_counts: Counter[tuple[str, str]],
) -> dict[str, Any] | None:
    keys = [_term_key(term), _term_id_key(term), _term_operation_path_key(term)]
    for operation_ref in _term_operation_refs(term):
        source_kind = str(term.get("source_kind") or "")
        source_id = str(term.get("source_parameter_id") or term.get("source_field_id") or "")
        field_path = str(term.get("field_path") or "")
        raw_name = str(term.get("raw_name") or "")
        keys.append((source_kind, operation_ref, source_id, field_path))
        keys.append((source_kind, operation_ref, "", field_path))
        if raw_name:
            keys.append((source_kind, operation_ref, source_id, f"raw:{raw_name}"))
            keys.append((source_kind, operation_ref, "", f"raw:{raw_name}"))
    for key in keys:
        item = manual_index.get(key)
        if item:
            return item
    path_key = _term_path_key(term)
    if term_path_counts[path_key] == 1:
        return manual_path_index.get(path_key)
    return None


def _manual_operation_refs(item: dict[str, Any]) -> list[str]:
    refs = [
        item.get("source_operation_key"),
        item.get("operation_key"),
        item.get("operation_name"),
        item.get("operation_path"),
        item.get("path"),
    ]
    return [f"opref:{str(ref)}" for ref in refs if str(ref or "").strip()]


def _term_operation_refs(term: dict[str, Any]) -> list[str]:
    operation = term.get("operation") if isinstance(term.get("operation"), dict) else {}
    refs = [
        operation.get("operation_key"),
        operation.get("operation_name"),
        operation.get("name"),
        operation.get("path"),
    ]
    normalized: list[str] = []
    for ref in refs:
        value = str(ref or "").strip()
        if not value:
            continue
        normalized.append(f"opref:{value}")
        if ":" in value:
            normalized.append(f"opref:{value.rsplit(':', 1)[-1]}")
    return list(dict.fromkeys(normalized))


def _approved_rule_catalog() -> list[dict[str, Any]]:
    return [
        {"rule_id": "parse_yyyymmdd_date", "type": "normalization_rule", "description": "Convert YYYYMMDD strings to ISO date."},
        {"rule_id": "parse_decimal", "type": "cast", "description": "Convert numeric strings to decimal values with null handling."},
        {"rule_id": "uppercase_iso_currency_code", "type": "normalization_rule", "description": "Uppercase a currency code."},
        {"rule_id": "normalize_identifier_digits", "type": "normalization_rule", "description": "Remove formatting characters and keep digits."},
    ]


def _resolve_llm_mode(override: str | None = None) -> str:
    return resolve_llm_mode(override)
