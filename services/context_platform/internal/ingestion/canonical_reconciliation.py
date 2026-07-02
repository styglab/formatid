from __future__ import annotations

import re
from collections import Counter
from difflib import SequenceMatcher
from typing import Any

from services.context_platform.internal.ingestion.langgraph.common import resolve_llm_mode
from services.context_platform.internal.storage import ContextPlatformRepository


DECISION_TYPES = {"reuse", "extend", "revise", "create", "conflict", "skip"}
RELATION_DECISION_TYPES = {"propose_relation", "skip_relation", "conflict"}
TRANSPORT_CLASS_NAMES = {
    "apiresponse",
    "api_response",
    "envelope",
    "parameter",
    "record",
    "request",
    "requestcontext",
    "request_context",
    "response",
    "responseenvelope",
    "response_envelope",
    "result",
}

CONTROL_RAW_NAMES = {
    "_type",
    "apikey",
    "api_key",
    "numofrows",
    "num_of_rows",
    "page",
    "pageno",
    "page_no",
    "pagesize",
    "page_size",
    "resulttype",
    "servicekey",
    "service_key",
}

RESPONSE_ENVELOPE_RAW_NAMES = {
    "currentcount",
    "current_count",
    "message",
    "msg",
    "numofrows",
    "num_of_rows",
    "page",
    "pageno",
    "page_no",
    "resultcode",
    "result_code",
    "resultmsg",
    "resultmessage",
    "result_message",
    "status",
    "statuscode",
    "status_code",
    "totalcount",
    "total_count",
    "totalresultcount",
    "total_result_count",
}


def build_canonical_model_reconciliation(
    repo: ContextPlatformRepository,
    *,
    source: dict[str, Any],
    document: dict[str, Any],
    operations: list[dict[str, Any]],
    document_fields: list[dict[str, Any]],
    llm_mode: str | None = None,
    manual_llm_response: dict[str, Any] | None = None,
) -> dict[str, Any]:
    context = load_canonical_context(repo)
    terms = collect_source_terms(source=source, document=document, operations=operations, document_fields=document_fields)
    resolved_llm_mode = _resolve_llm_mode(llm_mode)
    if resolved_llm_mode == "agent_manual" and isinstance(manual_llm_response, dict):
        decisions = reconcile_terms_from_manual_response(terms, context, manual_llm_response, allow_heuristic_create=False)
        relation_suggestions = relation_suggestions_from_manual_response(context, manual_llm_response)
        representation_decisions = _manual_decision_list(manual_llm_response, "representation_decisions")
        representation_schema_decisions = _manual_decision_list(manual_llm_response, "representation_schema_decisions")
        value_domain_decisions = _manual_decision_list(manual_llm_response, "value_domain_decisions")
        engine = "agent_manual_meaning_resolution"
    else:
        decisions = reconcile_terms_without_llm(terms, context)
        relation_suggestions = []
        representation_decisions = []
        representation_schema_decisions = []
        value_domain_decisions = []
        engine = "no_llm_meaning_resolution"
    counts = Counter(str(item.get("decision") or "create") for item in decisions)
    relation_counts = Counter(str(item.get("decision") or "propose_relation") for item in relation_suggestions)
    return {
        "type": "meaning_resolution",
        "legacy_type": "canonical_reconciliation",
        "llm_mode": resolved_llm_mode,
        "engine": engine,
        "source_id": source.get("id"),
        "source_document_id": document.get("id"),
        "context_summary": {
            "class_count": len(context.get("classes") or context.get("entities") or []),
            "class_slot_usage_count": len(context["class_slot_usages"]),
            "slot_count": len(context["slots"]),
            "relation_count": len(context.get("relations") or []),
        },
        "term_count": len(terms),
        "decision_counts": {key: counts.get(key, 0) for key in sorted(DECISION_TYPES)},
        "relation_decision_counts": {key: relation_counts.get(key, 0) for key in sorted(RELATION_DECISION_TYPES)},
        "linkml_fragment": build_linkml_fragment(decisions, relation_suggestions),
        "decisions": decisions,
        "meaning_decisions": decisions,
        "representation_decisions": representation_decisions,
        "representation_schema_decisions": representation_schema_decisions,
        "value_domain_decisions": value_domain_decisions,
        "relation_suggestions": relation_suggestions,
    }


def build_manual_canonical_reconciliation_request(
    *,
    run_id: str,
    source: dict[str, Any],
    document: dict[str, Any],
    operations: list[dict[str, Any]],
    document_fields: list[dict[str, Any]],
    context: dict[str, list[dict[str, Any]]],
) -> dict[str, Any]:
    terms = collect_source_terms(source=source, document=document, operations=operations, document_fields=document_fields)
    return {
        "type": "meaning_resolution",
        "legacy_type": "canonical_model_reconciliation",
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
            "Resolve each source term into a reviewable meaning decision.",
            "Treat canonical_context as the current approved Representation Model. It may include initial seed objects and later reviewed updates; do not treat any subset as a permanent foundation layer.",
            "First compare each source term with the current approved Concepts, CanonicalRepresentations, RepresentationSchemas, classes, slots, class-slot usages, and relations.",
            "Use canonical_context.classes[].class_kind to distinguish business_entity, reference_value, reference_scheme, value_object, and context_object classes.",
            "Do not create value/context objects as business entities. Reuse TimeInterval for coverage/validity periods and MonetaryAmount for monetary values when supported by evidence.",
            "For identifiers, prefer Identifier + IdentifierType + IdentifierScheme over adding country/provider-specific identifier fields directly to Company or Organization.",
            "When canonical_context.classes contains view_fields, use them as convenience matching names and planner-facing language, not as direct canonical slots to create.",
            "Reuse or extend existing Concepts, CanonicalRepresentations, or schema constraints when they preserve the source meaning without distortion.",
            "Create new representation objects only when the current approved model cannot represent the business meaning.",
            "Use decision `skip` for provider transport fields, response envelopes, pagination, service keys, result codes, and other non-business controls.",
            "Decide skip/reuse/create/extend/revise/conflict from the provided evidence and canonical context; do not rely on hard-coded provider or domain keyword rules.",
            "If a term is skipped, leave proposed_canonical/proposed_representation class_name and slot_name empty so downstream proposal stages do not reintroduce fallback canonical objects.",
            "Do not use raw provider names as canonical class-slot usage names when a clear business meaning exists.",
            "Use decision `reuse` only when a listed existing canonical class-slot usage is semantically correct.",
            "Use decision `extend` when an existing class is correct but the attribute is new.",
            "Use decision `revise` when an existing canonical object is close but needs alias, description, or constraint changes.",
            "Use decision `create` when no existing canonical class or attribute fits.",
            "Use decision `conflict` when multiple existing objects are plausible.",
            "Do not create canonical classes named request_context, api_response, response, request, parameter, or result.",
            "Do not create generic container classes such as record or envelope.",
            "When a source has contextual measures or facts, model the value and its business context from evidence instead of mirroring provider response columns.",
            "For financial or other metric-like tables, prefer Concept + CanonicalRepresentation + explicit context/schema proposals over one slot per provider metric column when the evidence supports that shape.",
            "When business classes have evidence-backed class-to-class relationships, propose them in relation_suggestions as canonical relation edges.",
            "Use relation_suggestions for class-to-class edges only. Keep scalar/value fields in decisions as class-slot usages.",
            "Do not infer relations from raw-name keywords alone; relation proposals must be grounded in source evidence and the canonical decisions in this response.",
            "Return exactly one decision for every source_terms item. Missing decisions are treated as unresolved conflicts, not automatic canonical creates.",
            "Prefer the active response keys concept_decisions, representation_decisions, representation_schema_decisions, value_domain_decisions, and relation_suggestions. The legacy decisions key is accepted only for compatibility.",
        ],
        "modeling_contract": {
            "concept": "The business meaning. It must not carry datatype, regex, enum, or provider transport constraints.",
            "canonical_representation": "A template for expressing a Concept through an ObjectType, PropertyType, fixed context, and required context.",
            "representation_schema": "The datatype, enum, regex, cardinality, examples, and validation rules for a representation/property usage.",
            "canonical_class": "A stable business entity, reference value/scheme, value object, or context object; never an API response wrapper, page shape, table shape, or transport container.",
            "canonical_slot": "A reusable structural field. Raw provider columns may be aliases, but should not dictate the model shape.",
            "relation": "A class-to-class business edge. Use relation_suggestions for entity/fact/context links instead of encoding relationships in field names.",
            "skip": "Use for authentication, pagination, format flags, result codes/messages, counts, response headers, and other non-business controls.",
            "class_kind": "business_entity classes are primary domain objects; reference_value/reference_scheme classes hold governed codes and schemes; value_object/context_object classes structure values and contexts such as money and time intervals.",
            "view_fields": "Convenience fields that make normalized patterns easier to read. They are matching hints, not direct storage slots.",
            "fact_pattern": "When rows/columns represent measured facts, model the measured value separately from the subject, concept/classification, document/evidence, and TimeInterval context.",
        },
        "anti_patterns": [
            {
                "bad": "record.result_msg",
                "reason": "result messages are provider response envelopes, not business canonical model fields.",
                "preferred": "decision=skip with empty proposed_canonical class_name and slot_name",
            },
            {
                "bad": "FinancialStatementSummary.sales_amount, FinancialStatementSummary.operating_profit_amount, FinancialStatementSummary.net_income_amount",
                "reason": "provider metric columns can become a brittle wide response-shaped class.",
                "preferred": "reuse or extend the current approved Observation, Classification/IdentifierType, Identifier/IdentifierScheme, Document, subject, and TimeInterval context patterns when the evidence supports those contexts",
            },
            {
                "bad": "api_response.result_type or request_context.service_key",
                "reason": "transport/control data belongs in source/binding metadata, not the canonical business model.",
                "preferred": "decision=skip or binding metadata/control handling",
            },
        ],
        "canonical_context": {
            "classes": [
                {
                    "id": item.get("id"),
                    "name": item.get("name"),
                    "description": item.get("description") or "",
                    "class_kind": _canonical_class_kind(item),
                    "is_a": str((item.get("metadata") or {}).get("is_a") or ""),
                    "is_abstract": bool((item.get("metadata") or {}).get("abstract")),
                    "view_fields": _canonical_class_annotation(item, "context_platform_view_fields", []),
                    "status": item.get("status"),
                }
                for item in context.get("classes") or context.get("entities", [])
            ],
            "class_slot_usages": [
                {
                    "id": item.get("id"),
                    "class_id": item.get("class_id"),
                    "class_name": item.get("class_name"),
                    "class_kind": _canonical_class_kind(item.get("class") or {}),
                    "name": item.get("name"),
                    "description": item.get("description") or "",
                    "datatype": item.get("datatype") or "string",
                    "identity_role": item.get("identity_role") or "",
                    "inherited": bool((item.get("annotations") or {}).get("context_platform_inherited")),
                    "declared_on": str((item.get("annotations") or {}).get("context_platform_declared_on") or ""),
                    "status": item.get("status"),
                }
                for item in context.get("class_slot_usages", [])
            ],
            "relations": [
                {
                    "id": item.get("id"),
                    "source_class_id": item.get("source_class_id"),
                    "source_class_name": item.get("source_class_name") or "",
                    "target_class_id": item.get("target_class_id"),
                    "target_class_name": item.get("target_class_name") or "",
                    "relation_type": item.get("relation_type"),
                    "forward_label": item.get("forward_label") or "",
                    "reverse_label": item.get("reverse_label") or "",
                    "status": item.get("status"),
                }
                for item in context.get("relations", [])
            ],
        },
        "source_terms": [
            {
                "source_kind": item.get("source_kind"),
                "source_operation_id": item.get("source_operation_id"),
                "source_parameter_id": item.get("source_parameter_id"),
                "source_field_id": item.get("source_field_id"),
                "direction": item.get("direction"),
                "field_path": item.get("field_path"),
                "raw_name": item.get("raw_name"),
                "data_type": item.get("data_type"),
                "is_required": item.get("is_required"),
                "description": item.get("description"),
                "operation": item.get("operation") or {},
                "evidence_refs": item.get("evidence_refs") or [],
            }
            for item in terms
        ],
        "response_contract": {
            "coverage": {
                "required": "Every item in source_terms must have one matching decision. Match primarily by source_parameter_id/source_field_id plus field_path; field_path-only matching is a fallback.",
                "missing_decision_behavior": "The ingestion runtime will mark omitted source terms as conflict and will not create canonical classes or slots for them.",
            },
            "decision_order": [
                "skip provider transport/control terms",
                "reuse business_entity, reference_value, reference_scheme, value_object, and context_object classes according to class_kind",
                "reuse an existing canonical class-slot usage when semantically correct",
                "extend an existing canonical class with a new reusable slot when the class is correct",
                "revise an existing canonical object when it is close but needs review",
                "create only when the current approved canonical model cannot represent the meaning",
                "conflict when multiple interpretations are plausible",
            ],
            "concept_decisions": [
                {
                    "source_kind": "parameter|field",
                    "source_parameter_id": "string|null",
                    "source_field_id": "string|null",
                    "field_path": "string",
                    "raw_name": "string",
                    "decision": "reuse|extend|revise|create|conflict|skip",
                    "concept_key": "concept.namespace.meaning",
                    "concept": {
                        "stable_key": "concept.namespace.meaning",
                        "kind": "object_concept|metric_concept|identifier_concept|status_concept|value_concept|unit_concept|time_concept|account_concept|document_concept|operation_concept",
                        "meaning_scope": "finance|tax|identifier|time|company|global",
                        "label_ko": "string",
                        "label_en": "string",
                        "definition": "meaning only; no datatype, regex, enum, or provider transport constraint",
                        "aliases": ["raw names and common aliases"],
                    },
                    "confidence": "float",
                    "rationale": "why this concept decision is correct",
                    "evidence_refs": ["list of evidence ref objects"],
                }
            ],
            "representation_decisions": [
                {
                    "source_kind": "parameter|field",
                    "source_parameter_id": "string|null",
                    "source_field_id": "string|null",
                    "field_path": "string",
                    "raw_name": "string",
                    "decision": "reuse|extend|revise|create|conflict|skip",
                    "concept_key": "concept.namespace.meaning",
                    "representation_key": "repr.namespace.meaning.carrier_property",
                    "canonical_representation": {
                        "stable_key": "repr.namespace.meaning.carrier_property",
                        "concept_key": "concept.namespace.meaning",
                        "carrier_object_type": "object.observation|object.identifier|object.company|...",
                        "value_property": "property.observed_amount|property.identifier_value|...",
                        "representation_kind": "metric_value|identifier_value|status_value|time_interval|relationship_link|document_property",
                        "fixed_context": {},
                        "required_context": ["subject", "currency", "fiscal_year", "source"],
                    },
                    "proposed_canonical": {
                        "class_name": "legacy carrier object name for compatibility",
                        "slot_name": "legacy value property name for compatibility",
                        "datatype": "legacy broad datatype",
                        "description": "representation meaning",
                        "aliases": ["raw names and common aliases"],
                        "identity_role": "identifier|measure|descriptor|status|context|",
                    },
                    "confidence": "float",
                    "rationale": "why this representation is correct",
                    "evidence_refs": ["list of evidence ref objects"],
                }
            ],
            "representation_schema_decisions": [
                {
                    "source_kind": "parameter|field",
                    "source_parameter_id": "string|null",
                    "source_field_id": "string|null",
                    "field_path": "string",
                    "raw_name": "string",
                    "decision": "reuse|extend|revise|create|conflict|skip",
                    "representation_key": "repr.namespace.meaning.carrier_property",
                    "representation_schema_key": "schema.namespace.meaning.format",
                    "representation_schema": {
                        "stable_key": "schema.namespace.meaning.format",
                        "datatype": "string|integer|decimal|boolean|date|enum|object",
                        "pattern": "regex string when applicable",
                        "value_domain_key": "value_domain.namespace.code_set|null",
                        "unit_concept_key": "concept.currency.krw|null",
                        "cardinality": "one|many",
                        "required": "boolean",
                        "minimum": "number|null",
                        "maximum": "number|null",
                        "precision": "string|null",
                        "examples": ["non-secret examples"],
                    },
                    "confidence": "float",
                    "rationale": "why this schema/validation decision is correct",
                    "evidence_refs": ["list of evidence ref objects"],
                }
            ],
            "value_domain_decisions": [
                {
                    "decision": "reuse|extend|revise|create|conflict|skip",
                    "value_domain_key": "value_domain.namespace.code_set",
                    "concept_key": "concept.namespace.status_or_value",
                    "values": [
                        {
                            "code": "source code",
                            "concept_key": "concept.namespace.value",
                            "label_ko": "string",
                            "label_en": "string",
                            "description": "string",
                        }
                    ],
                    "confidence": "float",
                    "rationale": "why this code/value domain is correct",
                    "evidence_refs": ["list of evidence ref objects"],
                }
            ],
            "decisions": "legacy alias. If provided, each item must still preserve concept_key, representation_key, representation_schema_key, proposed_representation, and representation_schema when known.",
            "relation_suggestions": [
                {
                    "decision": "propose_relation|skip_relation|conflict",
                    "source_class_id": "string|null",
                    "source_class_name": "business_source_class_name",
                    "target_class_id": "string|null",
                    "target_class_name": "business_target_class_name",
                    "relation_type": "machine_readable_predicate_name",
                    "forward_label": "human readable forward label",
                    "reverse_label": "human readable reverse label",
                    "description": "business relationship meaning",
                    "cardinality": "one_to_one|one_to_many|many_to_one|many_to_many",
                    "required": "boolean",
                    "metadata": {},
                    "confidence": "float",
                    "rationale": "why this relation is supported by evidence",
                    "evidence_refs": ["list of evidence ref objects"],
                }
            ],
        },
    }


def reconcile_terms_from_manual_response(
    terms: list[dict[str, Any]],
    context: dict[str, list[dict[str, Any]]],
    payload: dict[str, Any],
    *,
    allow_heuristic_create: bool = True,
) -> list[dict[str, Any]]:
    decisions = payload.get("decisions") if isinstance(payload.get("decisions"), list) else []
    manual_index: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    manual_path_index: dict[tuple[str, str], dict[str, Any]] = {}
    term_path_counts = Counter(_term_path_key(term) for term in terms)
    for item in decisions:
        if not isinstance(item, dict):
            continue
        for key in _manual_decision_keys(item):
            manual_index[key] = item
        manual_path_index[_manual_path_key(item)] = item
    return [
        _decision_from_manual(
            term,
            context,
            _manual_for_term(term, manual_index, manual_path_index, term_path_counts),
            allow_heuristic_create=allow_heuristic_create,
        )
        for term in terms
    ]


def reconcile_terms_without_llm(
    terms: list[dict[str, Any]],
    context: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    decisions: list[dict[str, Any]] = []
    for term in terms:
        fallback = reconcile_source_term(term, context)
        if fallback.get("decision") == "skip":
            decisions.append(fallback)
        else:
            decisions.append(_unresolved_llm_decision(term, "LLM mode is disabled; canonical business modeling requires review."))
    return decisions


def relation_suggestions_from_manual_response(
    context: dict[str, list[dict[str, Any]]],
    payload: dict[str, Any],
) -> list[dict[str, Any]]:
    suggestions = payload.get("relation_suggestions") if isinstance(payload.get("relation_suggestions"), list) else []
    normalized: list[dict[str, Any]] = []
    for item in suggestions:
        if not isinstance(item, dict):
            continue
        suggestion = _relation_suggestion_from_manual(item, context)
        if suggestion:
            normalized.append(suggestion)
    return normalized


def build_linkml_fragment(
    decisions: list[dict[str, Any]],
    relation_suggestions: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    fragment = build_linkml_fragment_from_decisions(decisions)
    _merge_linkml_fragment(fragment, build_linkml_fragment_from_relations(relation_suggestions or []))
    return fragment


def build_linkml_fragment_from_decisions(decisions: list[dict[str, Any]]) -> dict[str, Any]:
    fragment: dict[str, Any] = {
        "default_range": "string",
        "classes": {},
        "slots": {},
    }
    for decision in decisions:
        if not isinstance(decision, dict):
            continue
        decision_fragment = decision.get("linkml_fragment")
        if not isinstance(decision_fragment, dict):
            decision_fragment = build_linkml_fragment_for_decision(decision)
        _merge_linkml_fragment(fragment, decision_fragment)
    return fragment


def build_linkml_fragment_from_relations(relation_suggestions: list[dict[str, Any]]) -> dict[str, Any]:
    fragment: dict[str, Any] = {
        "classes": {},
        "slots": {},
    }
    for relation in relation_suggestions:
        if not isinstance(relation, dict):
            continue
        if str(relation.get("decision") or "") != "propose_relation":
            continue
        source_class = _linkml_class_name(str(relation.get("source_class_name") or ""))
        target_class = _linkml_class_name(str(relation.get("target_class_name") or ""))
        relation_type = _linkml_slot_name(str(relation.get("relation_type") or relation.get("forward_label") or ""))
        if not source_class or not target_class or not relation_type:
            continue
        slot_payload: dict[str, Any] = {
            "range": target_class,
            "annotations": {
                "context_platform": {
                    "object_type": "canonical_relation",
                    "decision": relation.get("decision"),
                    "relation_type": relation.get("relation_type"),
                    "source_class_name": relation.get("source_class_name"),
                    "target_class_name": relation.get("target_class_name"),
                }
            },
        }
        if relation.get("description"):
            slot_payload["description"] = str(relation.get("description") or "")
        metadata = relation.get("metadata") if isinstance(relation.get("metadata"), dict) else {}
        if relation.get("cardinality") or metadata.get("cardinality"):
            slot_payload["annotations"]["cardinality"] = str(relation.get("cardinality") or metadata.get("cardinality") or "")
        fragment["slots"][relation_type] = slot_payload
        class_payload = fragment["classes"].setdefault(source_class, {"slots": []})
        class_payload.setdefault("slots", [])
        if relation_type not in class_payload["slots"]:
            class_payload["slots"].append(relation_type)
        if relation.get("required") is not None:
            class_payload.setdefault("slot_usage", {})[relation_type] = {"required": bool(relation.get("required"))}
        fragment["classes"].setdefault(target_class, {"slots": []})
    return fragment


def build_linkml_fragment_for_decision(decision: dict[str, Any]) -> dict[str, Any]:
    if str(decision.get("decision") or "") == "skip":
        return {"classes": {}, "slots": {}}
    proposed = decision.get("proposed_canonical") if isinstance(decision.get("proposed_canonical"), dict) else {}
    matched = decision.get("matched_canonical_object") if isinstance(decision.get("matched_canonical_object"), dict) else {}
    source_term = decision.get("source_term") if isinstance(decision.get("source_term"), dict) else {}
    class_name = str(proposed.get("class_name") or proposed.get("entity_name") or matched.get("class_name") or matched.get("entity_name") or "").strip()
    slot_name = str(proposed.get("slot_name") or proposed.get("attribute_name") or matched.get("name") or "").strip()
    if not class_name or not slot_name:
        return {"classes": {}, "slots": {}}

    class_name = _linkml_class_name(class_name)
    slot_name = _linkml_slot_name(slot_name)
    description = str(proposed.get("description") or decision.get("extracted_meaning", {}).get("description") or "")
    aliases = proposed.get("aliases") if isinstance(proposed.get("aliases"), list) else []
    identity_role = str(proposed.get("identity_role") or decision.get("extracted_meaning", {}).get("identity_hint") or "")
    slot_payload: dict[str, Any] = {
        "range": _linkml_range(str(proposed.get("datatype") or "string")),
    }
    if description:
        slot_payload["description"] = description
    if aliases:
        slot_payload["aliases"] = [str(alias) for alias in aliases if str(alias)]
    annotations = {
        "context_platform": {
            "decision": decision.get("decision"),
            "source_kind": source_term.get("source_kind"),
            "source_operation_id": source_term.get("source_operation_id"),
            "source_parameter_id": source_term.get("source_parameter_id"),
            "source_field_id": source_term.get("source_field_id"),
            "field_path": source_term.get("field_path"),
            "raw_name": source_term.get("raw_name"),
        }
    }
    if identity_role:
        annotations["identity_role"] = identity_role
    slot_payload["annotations"] = annotations

    class_payload: dict[str, Any] = {"slots": [slot_name]}
    if str(matched.get("description") or "") and decision.get("decision") in {"reuse", "extend", "revise"}:
        class_payload["description"] = str(matched.get("description") or "")
    if bool(source_term.get("is_required")):
        class_payload["slot_usage"] = {slot_name: {"required": True}}

    return {
        "classes": {class_name: class_payload},
        "slots": {slot_name: slot_payload},
    }


def load_canonical_context(repo: ContextPlatformRepository) -> dict[str, list[dict[str, Any]]]:
    classes = repo.list_canonical_classes()
    slots = repo.list_canonical_slots()
    attributes = repo.list_canonical_class_slot_usages()
    relations = repo.list_canonical_relations() if hasattr(repo, "list_canonical_relations") else []
    class_by_id = {str(item.get("id") or ""): item for item in classes}
    slot_by_id = {str(item.get("id") or ""): item for item in slots}
    enriched_attributes: list[dict[str, Any]] = []
    for attribute in attributes:
        class_item = class_by_id.get(str(attribute.get("class_id") or attribute.get("entity_id") or "")) or {}
        slot = slot_by_id.get(str(attribute.get("canonical_slot_id") or "")) or {}
        enriched_attributes.append(
            {
                **attribute,
                "class": class_item,
                "slot": slot,
                "class_name": class_item.get("name") or "",
                "slot_name": slot.get("name") or "",
            }
        )
    return {"classes": classes, "slots": slots, "class_slot_usages": enriched_attributes, "relations": relations}


def _canonical_class_kind(class_item: dict[str, Any]) -> str:
    metadata = class_item.get("metadata") if isinstance(class_item.get("metadata"), dict) else {}
    annotations = metadata.get("annotations") if isinstance(metadata.get("annotations"), dict) else {}
    value = str(annotations.get("context_platform_class_kind") or "").strip()
    if value:
        return value
    if metadata.get("abstract") is True or metadata.get("abstract") == "true":
        return "abstract"
    return "business_entity"


def _canonical_class_annotation(class_item: dict[str, Any], key: str, default: Any = None) -> Any:
    metadata = class_item.get("metadata") if isinstance(class_item.get("metadata"), dict) else {}
    annotations = metadata.get("annotations") if isinstance(metadata.get("annotations"), dict) else {}
    return annotations.get(key, default)


def collect_source_terms(
    *,
    source: dict[str, Any],
    document: dict[str, Any],
    operations: list[dict[str, Any]],
    document_fields: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    terms: list[dict[str, Any]] = []
    for operation in operations:
        operation_id = str(operation.get("id") or "")
        operation_ref = {
            "source_operation_id": operation_id,
            "operation_key": operation.get("operation_key"),
            "operation_name": operation.get("name"),
            "method": operation.get("method"),
            "path": operation.get("path"),
        }
        for parameter in operation.get("parameters", []):
            name = str(parameter.get("name") or "")
            if not name:
                continue
            terms.append(
                {
                    "source_kind": "parameter",
                    "source_id": source.get("id"),
                    "source_document_id": document.get("id"),
                    "source_operation_id": operation_id,
                    "source_parameter_id": parameter.get("id"),
                    "source_field_id": None,
                    "direction": _parameter_direction(parameter),
                    "field_path": f"request.{name}",
                    "raw_name": name,
                    "data_type": parameter.get("data_type") or "string",
                    "is_required": bool(parameter.get("is_required")),
                    "description": parameter.get("description") or "",
                    "operation": operation_ref,
                    "evidence_refs": _evidence_refs(parameter, document),
                }
            )
        for field in operation.get("fields", []):
            terms.append(_field_term(source, document, field, operation_ref))
    for field in document_fields:
        terms.append(_field_term(source, document, field, {}))
    return terms


def reconcile_source_term(term: dict[str, Any], context: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    if _should_skip_canonical_term(term):
        return _with_linkml_fragment({
            "decision": "skip",
            "source_term": {
                key: term.get(key)
                for key in [
                    "source_kind",
                    "source_id",
                    "source_document_id",
                    "source_operation_id",
                    "source_parameter_id",
                    "source_field_id",
                    "direction",
                    "field_path",
                    "raw_name",
                    "data_type",
                    "is_required",
                    "description",
                ]
            },
            "operation": term.get("operation") or {},
            "extracted_meaning": {
                "display_name": _display_name(term),
                "description": term.get("description") or "",
                "identity_hint": "transport",
            },
            "matched_canonical_object": None,
            "candidate_canonical_objects": [],
            "proposed_canonical": {
                "class_name": "",
                "slot_name": "",
                "datatype": _canonical_datatype(term),
                "description": "",
                "aliases": _aliases(term),
                "identity_role": "transport",
            },
            "confidence": 0.91,
            "rationale": "Provider transport, pagination, authentication, or response envelope field is not part of the business canonical model.",
            "evidence_refs": term.get("evidence_refs") or [],
            "requires_review": True,
        })

    candidates = _rank_attribute_candidates(term, context["class_slot_usages"])
    strong = [item for item in candidates if item["score"] >= 0.88]
    medium = [item for item in candidates if 0.72 <= item["score"] < 0.88]
    proposed_class = _propose_class_name(term)
    proposed_attribute = _propose_slot_name(term)

    if len(strong) == 1:
        decision = "reuse"
        match = strong[0]
        confidence = match["score"]
        rationale = "Existing canonical class-slot usage strongly matches source field name or description."
    elif len(strong) > 1:
        decision = "conflict"
        match = strong[0]
        confidence = strong[0]["score"]
        rationale = "Multiple existing canonical class-slot usages strongly match; reviewer must choose one."
    elif len(medium) == 1:
        decision = "revise"
        match = medium[0]
        confidence = match["score"]
        rationale = "Existing canonical class-slot usage is similar but needs review, alias, or description refinement before reuse."
    elif len(medium) > 1:
        decision = "conflict"
        match = medium[0]
        confidence = match["score"]
        rationale = "Multiple existing canonical class-slot usages are plausible partial matches."
    else:
        class_match = _best_class_match(proposed_class, context.get("classes") or context.get("entities", []))
        if class_match and class_match["score"] >= 0.78:
            decision = "extend"
            match = class_match
            confidence = max(0.55, class_match["score"] * 0.8)
            rationale = "Existing canonical class appears appropriate, but no existing attribute matches this source meaning."
        else:
            decision = "create"
            match = None
            confidence = 0.46
            rationale = "No reliable existing canonical object matches this source meaning."

    matched_object = _matched_object(match)
    return _with_linkml_fragment({
        "decision": decision,
        "source_term": {
            key: term.get(key)
            for key in [
                "source_kind",
                "source_id",
                "source_document_id",
                "source_operation_id",
                "source_parameter_id",
                "source_field_id",
                "direction",
                "field_path",
                "raw_name",
                "data_type",
                "is_required",
                "description",
            ]
        },
        "operation": term.get("operation") or {},
        "extracted_meaning": {
            "display_name": _display_name(term),
            "description": term.get("description") or "",
            "identity_hint": _identity_hint(term),
        },
        "matched_canonical_object": matched_object,
        "candidate_canonical_objects": [_matched_object(item) for item in candidates[:5]],
        "proposed_canonical": {
            "class_name": (matched_object or {}).get("class_name") if decision == "extend" else proposed_class,
            "slot_name": (matched_object or {}).get("name") if decision == "reuse" else proposed_attribute,
            "datatype": _canonical_datatype(term),
            "description": _proposed_description(term),
            "aliases": _aliases(term),
            "identity_role": _identity_hint(term),
        },
        "confidence": round(float(confidence), 3),
        "rationale": rationale,
        "evidence_refs": term.get("evidence_refs") or [],
        "requires_review": decision != "reuse",
    })


def _decision_from_manual(
    term: dict[str, Any],
    context: dict[str, list[dict[str, Any]]],
    manual: dict[str, Any] | None,
    *,
    allow_heuristic_create: bool = True,
) -> dict[str, Any]:
    fallback = reconcile_source_term(term, context)
    if not isinstance(manual, dict):
        if allow_heuristic_create:
            return fallback
        if fallback.get("decision") == "skip":
            return fallback
        return _unresolved_llm_decision(term, "LLM response did not include a decision for this source term.")
    if fallback.get("decision") == "skip":
        return fallback
    decision = str(manual.get("decision") or fallback.get("decision") or "create")
    if decision not in DECISION_TYPES:
        decision = "create" if allow_heuristic_create else "conflict"
    if decision == "skip":
        proposed = manual.get("proposed_canonical") if isinstance(manual.get("proposed_canonical"), dict) else {}
        return _with_linkml_fragment({
            **fallback,
            "decision": "skip",
            "matched_canonical_object": None,
            "candidate_canonical_objects": [],
            "proposed_canonical": {
                "class_name": "",
                "slot_name": "",
                "datatype": str(proposed.get("datatype") or fallback["proposed_canonical"].get("datatype") or "string"),
                "description": "",
                "aliases": proposed.get("aliases") if isinstance(proposed.get("aliases"), list) else fallback["proposed_canonical"].get("aliases") or [],
                "identity_role": str(proposed.get("identity_role") or "transport"),
            },
            "confidence": round(float(manual.get("confidence", fallback.get("confidence") or 0.0)), 3),
            "rationale": str(manual.get("rationale") or fallback.get("rationale") or ""),
            "requires_review": True,
            "llm_decision": True,
            "concept_key": manual.get("concept_key"),
            "representation_key": manual.get("representation_key"),
            "representation_schema_key": manual.get("representation_schema_key"),
            "value_domain_key": manual.get("value_domain_key"),
            "concept": manual.get("concept") if isinstance(manual.get("concept"), dict) else {},
            "proposed_representation": manual.get("proposed_representation") if isinstance(manual.get("proposed_representation"), dict) else {},
            "representation_schema": manual.get("representation_schema") if isinstance(manual.get("representation_schema"), dict) else {},
        })
    canonical_class_slot_id = str(manual.get("canonical_class_slot_id") or "") or None
    canonical_class_id = str(manual.get("canonical_class_id") or manual.get("canonical_entity_id") or "") or None
    matched = _manual_matched_object(canonical_class_slot_id, canonical_class_id, context)
    proposed = manual.get("proposed_canonical") if isinstance(manual.get("proposed_canonical"), dict) else {}
    proposed_canonical = {
        "class_name": str(proposed.get("class_name") or proposed.get("entity_name") or (matched or {}).get("class_name") or fallback["proposed_canonical"].get("class_name") or ""),
        "slot_name": str(proposed.get("slot_name") or proposed.get("attribute_name") or (matched or {}).get("name") or fallback["proposed_canonical"].get("slot_name") or ""),
        "datatype": str(proposed.get("datatype") or fallback["proposed_canonical"].get("datatype") or "string"),
        "description": str(proposed.get("description") or fallback["proposed_canonical"].get("description") or ""),
        "aliases": proposed.get("aliases") if isinstance(proposed.get("aliases"), list) else fallback["proposed_canonical"].get("aliases") or [],
        "identity_role": str(proposed.get("identity_role") or fallback["proposed_canonical"].get("identity_role") or ""),
    }
    if _is_transport_class_name(proposed_canonical["class_name"]):
        if allow_heuristic_create:
            return fallback
        return _unresolved_llm_decision(
            term,
            f"LLM proposed transport/container class `{proposed_canonical['class_name']}`; canonical create was blocked.",
        )
    return _with_linkml_fragment({
        **fallback,
        "decision": decision,
        "matched_canonical_object": matched,
        "proposed_canonical": proposed_canonical,
        "concept_key": manual.get("concept_key"),
        "representation_key": manual.get("representation_key"),
        "representation_schema_key": manual.get("representation_schema_key"),
        "value_domain_key": manual.get("value_domain_key"),
        "concept": manual.get("concept") if isinstance(manual.get("concept"), dict) else {},
        "proposed_representation": manual.get("proposed_representation") if isinstance(manual.get("proposed_representation"), dict) else {},
        "representation_schema": manual.get("representation_schema") if isinstance(manual.get("representation_schema"), dict) else {},
        "confidence": round(float(manual.get("confidence", fallback.get("confidence") or 0.0)), 3),
        "rationale": str(manual.get("rationale") or fallback.get("rationale") or ""),
        "requires_review": decision != "reuse",
        "llm_decision": True,
    })


def _unresolved_llm_decision(term: dict[str, Any], rationale: str) -> dict[str, Any]:
    return _with_linkml_fragment({
        "decision": "conflict",
        "source_term": {
            key: term.get(key)
            for key in [
                "source_kind",
                "source_id",
                "source_document_id",
                "source_operation_id",
                "source_parameter_id",
                "source_field_id",
                "direction",
                "field_path",
                "raw_name",
                "data_type",
                "is_required",
                "description",
            ]
        },
        "operation": term.get("operation") or {},
        "extracted_meaning": {
            "display_name": _display_name(term),
            "description": term.get("description") or "",
            "identity_hint": _identity_hint(term),
        },
        "matched_canonical_object": None,
        "candidate_canonical_objects": [],
        "proposed_canonical": {
            "class_name": "",
            "slot_name": "",
            "datatype": _canonical_datatype(term),
            "description": "",
            "aliases": _aliases(term),
            "identity_role": "",
        },
        "confidence": 0.0,
        "rationale": rationale,
        "evidence_refs": term.get("evidence_refs") or [],
        "requires_review": True,
        "llm_decision": False,
        "resolution_required": True,
    })


def _field_term(
    source: dict[str, Any],
    document: dict[str, Any],
    field: dict[str, Any],
    operation_ref: dict[str, Any],
) -> dict[str, Any]:
    return {
        "source_kind": "field",
        "source_id": source.get("id"),
        "source_document_id": document.get("id"),
        "source_operation_id": operation_ref.get("source_operation_id"),
        "source_parameter_id": None,
        "source_field_id": field.get("id"),
        "direction": field.get("direction") or "output",
        "field_path": field.get("field_path") or field.get("raw_name") or "",
        "raw_name": field.get("raw_name") or "",
        "data_type": field.get("data_type") or "string",
        "is_required": bool(field.get("is_required")),
        "description": field.get("description") or "",
        "operation": operation_ref,
        "evidence_refs": _evidence_refs(field, document),
    }


def _term_key(term: dict[str, Any]) -> tuple[str, str, str, str]:
    source_kind = str(term.get("source_kind") or "")
    operation_id = str(term.get("source_operation_id") or "")
    source_id = str(term.get("source_parameter_id") or term.get("source_field_id") or "")
    field_path = str(term.get("field_path") or "")
    return (source_kind, operation_id, source_id, field_path)


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


def _manual_decision_keys(item: dict[str, Any]) -> list[tuple[str, str, str, str]]:
    source_kind = str(item.get("source_kind") or "")
    operation_id = str(item.get("source_operation_id") or "")
    source_id = str(item.get("source_parameter_id") or item.get("source_field_id") or "")
    field_path = str(item.get("field_path") or "")
    raw_name = str(item.get("raw_name") or "")
    keys = [(source_kind, operation_id, source_id, field_path)]
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


def _manual_matched_object(
    canonical_class_slot_id: str | None,
    canonical_class_id: str | None,
    context: dict[str, list[dict[str, Any]]],
) -> dict[str, Any] | None:
    if canonical_class_slot_id:
        attribute = next((item for item in context.get("class_slot_usages", []) if item.get("id") == canonical_class_slot_id), None)
        if attribute:
            return _matched_object({"score": 1.0, **attribute})
    if canonical_class_id:
        class_item = next((item for item in context.get("classes") or context.get("entities", []) if item.get("id") == canonical_class_id), None)
        if class_item:
            return _matched_object({"score": 1.0, **class_item})
    return None


def _relation_suggestion_from_manual(item: dict[str, Any], context: dict[str, list[dict[str, Any]]]) -> dict[str, Any] | None:
    decision = str(item.get("decision") or "propose_relation")
    if decision not in RELATION_DECISION_TYPES:
        decision = "conflict"
    source_class_id = str(item.get("source_class_id") or "") or None
    target_class_id = str(item.get("target_class_id") or "") or None
    source_class_name = str(item.get("source_class_name") or "").strip()
    target_class_name = str(item.get("target_class_name") or "").strip()
    if source_class_id and not source_class_name:
        source_class_name = _class_name_for_id(context, source_class_id)
    if target_class_id and not target_class_name:
        target_class_name = _class_name_for_id(context, target_class_id)
    relation_type = str(item.get("relation_type") or item.get("relation_name") or item.get("forward_label") or "").strip()
    if decision == "skip_relation":
        return {
            "decision": "skip_relation",
            "source_class_id": source_class_id,
            "source_class_name": source_class_name,
            "target_class_id": target_class_id,
            "target_class_name": target_class_name,
            "relation_type": relation_type,
            "forward_label": str(item.get("forward_label") or ""),
            "reverse_label": str(item.get("reverse_label") or ""),
            "description": "",
            "cardinality": str(item.get("cardinality") or ""),
            "required": bool(item.get("required", False)),
            "metadata": item.get("metadata") if isinstance(item.get("metadata"), dict) else {},
            "confidence": round(float(item.get("confidence") or 0.0), 3),
            "rationale": str(item.get("rationale") or ""),
            "evidence_refs": item.get("evidence_refs") if isinstance(item.get("evidence_refs"), list) else [],
        }
    if not source_class_name or not target_class_name or not relation_type:
        return None
    return {
        "decision": decision,
        "source_class_id": source_class_id,
        "source_class_name": source_class_name,
        "target_class_id": target_class_id,
        "target_class_name": target_class_name,
        "relation_type": relation_type,
        "forward_label": str(item.get("forward_label") or relation_type),
        "reverse_label": str(item.get("reverse_label") or ""),
        "description": str(item.get("description") or ""),
        "cardinality": str(item.get("cardinality") or ""),
        "required": bool(item.get("required", False)),
        "metadata": item.get("metadata") if isinstance(item.get("metadata"), dict) else {},
        "confidence": round(float(item.get("confidence") or 0.0), 3),
        "rationale": str(item.get("rationale") or ""),
        "evidence_refs": item.get("evidence_refs") if isinstance(item.get("evidence_refs"), list) else [],
        "requires_review": True,
        "llm_decision": True,
    }


def _class_name_for_id(context: dict[str, list[dict[str, Any]]], class_id: str) -> str:
    for item in context.get("classes") or context.get("entities") or []:
        if str(item.get("id") or "") == class_id:
            return str(item.get("name") or "")
    return ""


def _rank_attribute_candidates(term: dict[str, Any], attributes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ranked = []
    query = _term_text(term)
    query_norm = _normalize(query)
    raw_norm = _normalize(str(term.get("raw_name") or ""))
    for attribute in attributes:
        texts = [
            attribute.get("name"),
            attribute.get("description"),
            attribute.get("class_name"),
            attribute.get("slot_name"),
            " ".join(str(alias) for alias in _metadata_aliases(attribute)),
        ]
        candidate_text = " ".join(str(item or "") for item in texts)
        candidate_norm = _normalize(candidate_text)
        name_norm = _normalize(str(attribute.get("name") or ""))
        score = max(
            SequenceMatcher(None, query_norm, candidate_norm).ratio(),
            SequenceMatcher(None, raw_norm, name_norm).ratio(),
            _token_overlap(query_norm, candidate_norm),
        )
        if score <= 0.3:
            continue
        ranked.append({"score": score, **attribute})
    return sorted(ranked, key=lambda item: float(item.get("score") or 0.0), reverse=True)


def _best_class_match(proposed_class: str, classes: list[dict[str, Any]]) -> dict[str, Any] | None:
    ranked = []
    proposed_norm = _normalize(proposed_class)
    for class_item in classes:
        candidate_norm = _normalize(f"{class_item.get('name') or ''} {class_item.get('description') or ''}")
        score = max(
            SequenceMatcher(None, proposed_norm, _normalize(str(class_item.get("name") or ""))).ratio(),
            _token_overlap(proposed_norm, candidate_norm),
        )
        if score > 0.3:
            ranked.append({"score": score, **class_item})
    return sorted(ranked, key=lambda item: float(item.get("score") or 0.0), reverse=True)[0] if ranked else None


def _matched_object(candidate: dict[str, Any] | None) -> dict[str, Any] | None:
    if not candidate:
        return None
    object_type = "canonical_class_slot" if candidate.get("class_id") or candidate.get("entity_id") else "canonical_class"
    return {
        "object_type": object_type,
        "id": candidate.get("id"),
        "namespace": candidate.get("namespace"),
        "class_id": candidate.get("class_id") or candidate.get("entity_id"),
        "class_name": candidate.get("class_name") or candidate.get("entity_name") or candidate.get("name"),
        "name": candidate.get("name"),
        "description": candidate.get("description") or "",
        "score": round(float(candidate.get("score") or 0.0), 3),
        "status": candidate.get("status"),
        "lifecycle": candidate.get("lifecycle"),
    }


def _parameter_direction(parameter: dict[str, Any]) -> str:
    raw_name = _normalized_identifier(str(parameter.get("name") or ""))
    if raw_name in CONTROL_RAW_NAMES:
        return "control"
    return "input"


def _term_text(term: dict[str, Any]) -> str:
    return " ".join(
        str(term.get(key) or "")
        for key in ["raw_name", "field_path", "description", "data_type", "direction"]
    )


def _display_name(term: dict[str, Any]) -> str:
    description = str(term.get("description") or "").strip()
    return description.split(" - ", 1)[0].strip() if description else str(term.get("raw_name") or "")


def _propose_class_name(term: dict[str, Any]) -> str:
    _ = term
    return "record"


def _propose_slot_name(term: dict[str, Any]) -> str:
    raw_name = str(term.get("raw_name") or term.get("field_path") or "value")
    return _to_snake(raw_name)


def _canonical_datatype(term: dict[str, Any]) -> str:
    data_type = str(term.get("data_type") or "string").lower()
    if data_type in {"integer", "number", "decimal", "float", "double"}:
        return "decimal" if data_type == "number" else data_type
    return "string"


def _proposed_description(term: dict[str, Any]) -> str:
    description = str(term.get("description") or "").strip()
    return description or f"Canonical attribute proposed from source field `{term.get('raw_name')}`."


def _aliases(term: dict[str, Any]) -> list[str]:
    values = [str(term.get("raw_name") or ""), str(term.get("field_path") or "")]
    return [item for item in dict.fromkeys(values) if item]


def _identity_hint(term: dict[str, Any]) -> str:
    text = f"{term.get('raw_name') or ''} {term.get('field_path') or ''} {term.get('description') or ''}".lower()
    if str(term.get("direction") or "") == "control":
        return "transport"
    if "id" in text or "번호" in text or text.endswith("no"):
        return "identifier"
    return ""


def _should_skip_canonical_term(term: dict[str, Any]) -> bool:
    raw_name = _normalized_identifier(str(term.get("raw_name") or ""))
    field_path = _normalized_identifier(str(term.get("field_path") or ""))
    direction = str(term.get("direction") or "").lower()
    source_kind = str(term.get("source_kind") or "")
    if source_kind == "parameter" and (direction == "control" or raw_name in CONTROL_RAW_NAMES):
        return True
    if raw_name in RESPONSE_ENVELOPE_RAW_NAMES:
        return True
    envelope_tokens = {"header", "responseheader", "response_header", "pagination", "paging", "meta", "metadata"}
    if any(token in field_path for token in envelope_tokens) and raw_name in RESPONSE_ENVELOPE_RAW_NAMES | CONTROL_RAW_NAMES:
        return True
    return False


def _is_transport_class_name(value: str) -> bool:
    normalized = _normalized_identifier(value)
    return normalized in TRANSPORT_CLASS_NAMES


def _normalized_identifier(value: str) -> str:
    return re.sub(r"[^a-z0-9_]+", "", _to_snake(value).lower())


def _metadata_aliases(attribute: dict[str, Any]) -> list[str]:
    metadata = attribute.get("metadata") if isinstance(attribute.get("metadata"), dict) else {}
    aliases = metadata.get("aliases") if isinstance(metadata.get("aliases"), list) else []
    slot = attribute.get("slot") if isinstance(attribute.get("slot"), dict) else {}
    slot_metadata = slot.get("metadata") if isinstance(slot.get("metadata"), dict) else {}
    slot_aliases = slot_metadata.get("aliases") if isinstance(slot_metadata.get("aliases"), list) else []
    return [str(item) for item in [*aliases, *slot_aliases] if str(item)]


def _evidence_refs(item: dict[str, Any], document: dict[str, Any]) -> list[dict[str, Any]]:
    evidence = item.get("evidence") if isinstance(item.get("evidence"), list) else []
    if evidence:
        return evidence
    return [{"source_document_id": document.get("id"), "field_path": item.get("field_path") or item.get("name")}]


def _normalize(value: str) -> str:
    return re.sub(r"[^a-z0-9가-힣]+", " ", value.lower()).strip()


def _token_overlap(left: str, right: str) -> float:
    left_tokens = set(left.split())
    right_tokens = set(right.split())
    if not left_tokens or not right_tokens:
        return 0.0
    return len(left_tokens & right_tokens) / len(left_tokens | right_tokens)


def _to_snake(value: str) -> str:
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", value)
    normalized = re.sub(r"[^a-zA-Z0-9]+", "_", value).strip("_").lower()
    normalized = re.sub(r"_+", "_", normalized)
    if normalized and normalized[0].isdigit():
        normalized = f"field_{normalized}"
    return normalized[:80] or "value"


def _with_linkml_fragment(decision: dict[str, Any]) -> dict[str, Any]:
    return {**decision, "linkml_fragment": build_linkml_fragment_for_decision(decision)}


def _merge_linkml_fragment(target: dict[str, Any], fragment: dict[str, Any]) -> None:
    classes = fragment.get("classes") if isinstance(fragment.get("classes"), dict) else {}
    slots = fragment.get("slots") if isinstance(fragment.get("slots"), dict) else {}
    for class_name, payload in classes.items():
        if not isinstance(payload, dict):
            continue
        existing = target["classes"].setdefault(class_name, {})
        if payload.get("description") and not existing.get("description"):
            existing["description"] = payload["description"]
        existing_slots = existing.setdefault("slots", [])
        for slot_name in payload.get("slots") if isinstance(payload.get("slots"), list) else []:
            if slot_name not in existing_slots:
                existing_slots.append(slot_name)
        slot_usage = payload.get("slot_usage") if isinstance(payload.get("slot_usage"), dict) else {}
        if slot_usage:
            existing.setdefault("slot_usage", {}).update(slot_usage)
    for slot_name, payload in slots.items():
        if not isinstance(payload, dict):
            continue
        existing = target["slots"].setdefault(slot_name, {})
        for key, value in payload.items():
            if key == "aliases":
                aliases = existing.setdefault("aliases", [])
                for alias in value if isinstance(value, list) else []:
                    if alias not in aliases:
                        aliases.append(alias)
            elif key == "annotations":
                annotations = existing.setdefault("annotations", {})
                if isinstance(value, dict):
                    annotations.update(value)
            elif value and not existing.get(key):
                existing[key] = value


def _linkml_class_name(value: str) -> str:
    parts = re.split(r"[^a-zA-Z0-9]+", value)
    name = "".join(part[:1].upper() + part[1:] for part in parts if part)
    if name and name[0].isdigit():
        name = f"Class{name}"
    return name or "Record"


def _linkml_slot_name(value: str) -> str:
    return _to_snake(value)


def _linkml_range(datatype: str) -> str:
    normalized = str(datatype or "string").lower()
    mapping = {
        "str": "string",
        "string": "string",
        "text": "string",
        "integer": "integer",
        "int": "integer",
        "number": "decimal",
        "decimal": "decimal",
        "float": "float",
        "double": "double",
        "boolean": "boolean",
        "bool": "boolean",
        "date": "date",
        "datetime": "datetime",
    }
    return mapping.get(normalized, "string")


def _resolve_llm_mode(override: str | None = None) -> str:
    return resolve_llm_mode(override)


def _manual_decision_list(payload: dict[str, Any], key: str) -> list[dict[str, Any]]:
    values = payload.get(key)
    if not isinstance(values, list):
        return []
    return [dict(item) for item in values if isinstance(item, dict)]
