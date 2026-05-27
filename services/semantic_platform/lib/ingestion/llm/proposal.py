from __future__ import annotations

import json
import os
import re
from typing import Any
from urllib import request
from urllib.error import HTTPError, URLError

from services.semantic_platform.lib.ingestion.llm.validation import validate_llm_analysis as _validate_llm_analysis
from services.semantic_platform.lib.ingestion.llm.runtime import active_llm_mode, openai_api_key
from services.semantic_platform.lib.ingestion.state import SourceGraphState


def llm_propose_capability_catalog(state: SourceGraphState) -> SourceGraphState:
    manual = state.get("manual_llm_response") or {}
    if _legacy_items(manual):
        analysis: dict[str, Any] = {}
    elif manual:
        analysis = manual
    else:
        analysis = _call_capability_llm(state) or {}
        if _llm_mode() == "openai" and state.get("api_sections"):
            _require_executable_catalog_items(analysis)
    analysis = _normalize_llm_analysis(analysis)
    _validate_llm_analysis(analysis)
    return {
        **state,
        "analysis": analysis,
        "capability_analysis": {
            "semantic_types": _list(analysis.get("semantic_types")),
            "entities": _list(analysis.get("entities")),
            "entity_identifiers": _list(analysis.get("entity_identifiers")),
            "capabilities": _list(analysis.get("capabilities")),
            "capability_entity_links": _list(analysis.get("capability_entity_links")),
            "capability_dependencies": _list(analysis.get("capability_dependencies")),
            "semantic_join_rules": _list(analysis.get("semantic_join_rules")),
            "planning_examples": _list(analysis.get("planning_examples")),
        },
    }


def llm_propose_execution_catalog(state: SourceGraphState) -> SourceGraphState:
    analysis = state.get("analysis", {})
    operation_fields = _list(analysis.get("operation_fields"))
    if not operation_fields:
        operation_fields = _operation_fields_from_analysis(analysis, state)
    operation_contracts = _operation_contracts_with_source_auth(
        _list(analysis.get("operation_contracts")),
        state,
    )
    field_mappings = _field_mappings_with_operation_fields(
        _list(analysis.get("field_mappings")),
        operation_fields,
    )
    execution_analysis = {
        "resources": _list(analysis.get("resources")),
        "operations": _list(analysis.get("operations")),
        "operation_fields": operation_fields,
        "semantic_types": _list(analysis.get("semantic_types")),
        "entities": _list(analysis.get("entities")),
        "entity_identifiers": _list(analysis.get("entity_identifiers")),
        "capabilities": _list(analysis.get("capabilities")),
        "capability_entity_links": _list(analysis.get("capability_entity_links")),
        "capability_dependencies": _list(analysis.get("capability_dependencies")),
        "semantic_join_rules": _list(analysis.get("semantic_join_rules")),
        "planning_examples": _list(analysis.get("planning_examples")),
        "operation_contracts": operation_contracts,
        "operation_variants": _list(analysis.get("operation_variants")),
        "field_mappings": field_mappings,
        "capability_implementations": _list(analysis.get("capability_implementations")),
    }
    filtered_analysis = _filter_executable_capabilities(
        _filter_analysis_by_passed_endpoints(state, execution_analysis)
    )
    return {
        **state,
        "execution_analysis": execution_analysis,
        **filtered_analysis,
    }


def operation_variant_candidates(state: SourceGraphState) -> list[dict[str, Any]]:
    evidence_section_ids = _evidence_section_ids_for_llm(state)
    evidence = state.get("structured_evidence", {})
    tables_by_section: dict[str, list[dict[str, Any]]] = {}
    for table in evidence.get("field_table_candidates", []):
        section_id = str(table.get("section_id") or "")
        if section_id in evidence_section_ids:
            tables_by_section.setdefault(section_id, []).append(table)
    controls_by_section: dict[str, list[dict[str, Any]]] = {}
    for control in evidence.get("control_field_candidates", []):
        section_id = str(control.get("section_id") or "")
        values = control.get("values") if isinstance(control.get("values"), list) else []
        if section_id in evidence_section_ids and values and (control.get("field_name") or len(values) >= 2):
            controls_by_section.setdefault(section_id, []).append(control)

    candidates = []
    for section in _evidence_sections_for_llm(state):
        section_id = str(section.get("id") or "")
        if section_id not in evidence_section_ids:
            continue
        request_fields = _request_fields_from_tables(tables_by_section.get(section_id, []))
        controls = []
        seen_controls: set[tuple[str, str]] = set()
        for control in controls_by_section.get(section_id, []):
            control_name = _control_field_name(control)
            values_key = json.dumps(control.get("values", []), ensure_ascii=False, sort_keys=True)
            control_key = (control_name, values_key)
            if control_key in seen_controls:
                continue
            seen_controls.add(control_key)
            related_request_fields = _related_request_fields(control_name, control, request_fields)
            controls.append(
                {
                    "raw_name": control_name,
                    "label": control.get("label"),
                    "sample": control.get("sample"),
                    "description": control.get("description"),
                    "text": control.get("text"),
                    "values": control.get("values", []),
                    "evidence": control.get("evidence", {}),
                    "related_request_fields": related_request_fields,
                    "variant_generation_hint": _variant_generation_hint(control_name, control, related_request_fields),
                }
            )
        if controls:
            candidates.append(
                {
                    "section_id": section_id,
                    "operation_name": section.get("operation_name"),
                    "method": section.get("method"),
                    "path": section.get("path"),
                    "controls": controls,
                    "request_fields": request_fields[:80],
                }
            )
    return candidates[:80]


def _operation_contracts_with_source_auth(
    contracts: list[dict[str, Any]],
    state: SourceGraphState,
) -> list[dict[str, Any]]:
    source_document = state.get("source_document", {})
    metadata = source_document.get("metadata") if isinstance(source_document.get("metadata"), dict) else {}
    api_key_env = str(metadata.get("api_key_env") or "").strip()
    if not api_key_env:
        return contracts
    enriched = []
    for contract in contracts:
        item = dict(contract)
        auth = dict(item.get("auth", {}) if isinstance(item.get("auth"), dict) else {})
        if auth:
            auth["env_names"] = [api_key_env]
            item["auth"] = auth
        enriched.append(item)
    return enriched


def _filter_analysis_by_passed_endpoints(
    state: SourceGraphState,
    analysis: dict[str, list[dict[str, Any]]],
) -> dict[str, list[dict[str, Any]]]:
    passed_checks = [check for check in state.get("endpoint_candidate_checks", []) if check.get("status") == "passed"]
    passed_paths = {str(check.get("path") or "") for check in passed_checks}
    passed_operations = {str(check.get("operation_name") or "") for check in passed_checks}
    if not passed_paths and not passed_operations:
        return analysis

    operations = [
        operation
        for operation in analysis["operations"]
        if _operation_matches_passed_endpoint(operation, passed_paths, passed_operations)
    ]
    operation_ids = {str(operation.get("operation_id") or "") for operation in operations}
    operation_fields = [
        field for field in analysis.get("operation_fields", []) if str(field.get("operation_id") or "") in operation_ids
    ]
    operation_contracts = [
        contract for contract in analysis["operation_contracts"] if str(contract.get("operation_id") or "") in operation_ids
    ]
    operation_variants = [
        variant for variant in analysis["operation_variants"] if str(variant.get("operation_id") or "") in operation_ids
    ]
    capability_ids = {
        str(contract.get("capability_id") or contract.get("capability") or "")
        for contract in operation_contracts
    } | {
        str(variant.get("capability_id") or variant.get("capability") or "")
        for variant in operation_variants
    }
    capabilities = [
        capability for capability in analysis["capabilities"] if str(capability.get("id") or "") in capability_ids
    ]
    capability_entity_links = [
        link for link in analysis.get("capability_entity_links", []) if str(link.get("capability_id") or "") in capability_ids
    ]
    capability_dependencies = [
        dependency for dependency in analysis.get("capability_dependencies", []) if str(dependency.get("capability_id") or "") in capability_ids
    ]
    entity_ids = {str(link.get("entity_id") or "") for link in capability_entity_links if link.get("entity_id")}
    semantic_type_ids = {
        str(value)
        for capability in capabilities
        for value in [*_list_values(capability.get("inputs")), *_list_values(capability.get("outputs"))]
    }
    semantic_type_ids.update(str(link.get("semantic_type_id") or "") for link in capability_entity_links if link.get("semantic_type_id"))
    semantic_type_ids.update(str(dependency.get("semantic_type_id") or "") for dependency in capability_dependencies if dependency.get("semantic_type_id"))
    entity_identifiers = [
        identifier
        for identifier in analysis.get("entity_identifiers", [])
        if str(identifier.get("entity_id") or "") in entity_ids
        or str(identifier.get("semantic_type_id") or "") in semantic_type_ids
    ]
    entity_ids.update(str(identifier.get("entity_id") or "") for identifier in entity_identifiers if identifier.get("entity_id"))
    entities = [entity for entity in analysis.get("entities", []) if str(entity.get("id") or "") in entity_ids]
    semantic_join_rules = [
        rule
        for rule in analysis.get("semantic_join_rules", [])
        if str(rule.get("from_entity_id") or "") in entity_ids
        or str(rule.get("to_entity_id") or "") in entity_ids
        or str(rule.get("from_semantic_type_id") or "") in semantic_type_ids
        or str(rule.get("to_semantic_type_id") or "") in semantic_type_ids
    ]
    planning_examples = [
        example
        for example in analysis.get("planning_examples", [])
        if set(str(value) for value in _list_values(example.get("expected_capability_ids"))) & capability_ids
    ]
    field_mappings = [
        mapping for mapping in analysis["field_mappings"] if str(mapping.get("operation_id") or "") in operation_ids
    ]
    capability_implementations = [
        implementation
        for implementation in analysis["capability_implementations"]
        if str(implementation.get("operation_id") or "") in operation_ids
        and str(implementation.get("capability_id") or "") in capability_ids
    ]
    return {
        **analysis,
        "operations": operations,
        "operation_fields": operation_fields,
        "entities": entities,
        "entity_identifiers": entity_identifiers,
        "capabilities": capabilities,
        "capability_entity_links": capability_entity_links,
        "capability_dependencies": capability_dependencies,
        "operation_contracts": operation_contracts,
        "operation_variants": operation_variants,
        "field_mappings": field_mappings,
        "semantic_join_rules": semantic_join_rules,
        "planning_examples": planning_examples,
        "capability_implementations": capability_implementations,
    }


def _operation_matches_passed_endpoint(
    operation: dict[str, Any],
    passed_paths: set[str],
    passed_operations: set[str],
) -> bool:
    path = str(operation.get("path") or "")
    operation_name = str(operation.get("operation_name") or "")
    operation_id = str(operation.get("operation_id") or "")
    suffix = operation_id.rsplit(".", 1)[-1]
    return (
        path in passed_paths
        or operation_name in passed_operations
        or suffix in passed_operations
        or f"/{suffix}" in passed_paths
    )


def _filter_executable_capabilities(analysis: dict[str, list[dict[str, Any]]]) -> dict[str, list[dict[str, Any]]]:
    request_fields_by_operation = {
        str(contract.get("operation_id") or ""): _contract_request_raw_fields(contract.get("request"))
        for contract in analysis.get("operation_contracts", [])
    }
    operation_variants = [
        variant
        for variant in analysis.get("operation_variants", [])
        if _variant_fixed_raw_arguments_are_declared(variant, request_fields_by_operation)
    ]
    executable_capability_ids = {
        str(contract.get("capability_id") or contract.get("capability") or "")
        for contract in analysis.get("operation_contracts", [])
        if contract.get("capability_id") or contract.get("capability")
    }
    executable_capability_ids.update(
        str(implementation.get("capability_id") or "")
        for implementation in analysis.get("capability_implementations", [])
        if implementation.get("capability_id")
    )
    executable_capability_ids.update(
        str(variant.get("capability_id") or "")
        for variant in operation_variants
        if variant.get("capability_id")
    )
    executable_capability_ids.discard("")
    capabilities = [
        capability
        for capability in analysis.get("capabilities", [])
        if str(capability.get("id") or "") in executable_capability_ids
    ]
    capability_ids = {str(capability.get("id") or "") for capability in capabilities}
    operation_variants = [
        variant for variant in operation_variants if str(variant.get("capability_id") or "") in capability_ids
    ]
    capability_implementations = [
        implementation
        for implementation in analysis.get("capability_implementations", [])
        if str(implementation.get("capability_id") or "") in capability_ids
    ]
    operation_contracts = [
        contract
        for contract in analysis.get("operation_contracts", [])
        if str(contract.get("capability_id") or contract.get("capability") or "") in capability_ids
    ]
    operation_ids = {
        str(item.get("operation_id") or "")
        for item in [*operation_contracts, *operation_variants, *capability_implementations]
        if item.get("operation_id")
    }
    operations = [
        operation for operation in analysis.get("operations", []) if str(operation.get("operation_id") or "") in operation_ids
    ]
    operation_fields = [
        field for field in analysis.get("operation_fields", []) if str(field.get("operation_id") or "") in operation_ids
    ]
    field_mappings = [
        mapping for mapping in analysis.get("field_mappings", []) if str(mapping.get("operation_id") or "") in operation_ids
    ]
    capability_entity_links = [
        link for link in analysis.get("capability_entity_links", []) if str(link.get("capability_id") or "") in capability_ids
    ]
    capability_dependencies = [
        dependency
        for dependency in analysis.get("capability_dependencies", [])
        if str(dependency.get("capability_id") or "") in capability_ids
        and str(dependency.get("depends_on_capability_id") or "") in capability_ids
    ]
    entity_ids = {str(link.get("entity_id") or "") for link in capability_entity_links if link.get("entity_id")}
    semantic_type_ids = {
        str(value)
        for capability in capabilities
        for value in [*_list_values(capability.get("inputs")), *_list_values(capability.get("outputs"))]
    }
    semantic_type_ids.update(str(link.get("semantic_type_id") or "") for link in capability_entity_links if link.get("semantic_type_id"))
    semantic_type_ids.update(str(mapping.get("semantic_type_id") or "") for mapping in field_mappings if mapping.get("semantic_type_id"))
    semantic_type_ids.discard("")
    entity_identifiers = [
        identifier
        for identifier in analysis.get("entity_identifiers", [])
        if str(identifier.get("entity_id") or "") in entity_ids
        or str(identifier.get("semantic_type_id") or "") in semantic_type_ids
    ]
    entity_ids.update(str(identifier.get("entity_id") or "") for identifier in entity_identifiers if identifier.get("entity_id"))
    entities = [entity for entity in analysis.get("entities", []) if str(entity.get("id") or "") in entity_ids]
    semantic_types = [item for item in analysis.get("semantic_types", []) if str(item.get("id") or "") in semantic_type_ids]
    semantic_join_rules = [
        rule
        for rule in analysis.get("semantic_join_rules", [])
        if str(rule.get("from_entity_id") or "") in entity_ids
        or str(rule.get("to_entity_id") or "") in entity_ids
        or str(rule.get("from_semantic_type_id") or "") in semantic_type_ids
        or str(rule.get("to_semantic_type_id") or "") in semantic_type_ids
    ]
    planning_examples = [
        example
        for example in analysis.get("planning_examples", [])
        if set(str(value) for value in _list_values(example.get("expected_capability_ids"))) & capability_ids
    ]
    resource_ids = {str(operation.get("resource_id") or "") for operation in operations if operation.get("resource_id")}
    resources = [resource for resource in analysis.get("resources", []) if str(resource.get("id") or "") in resource_ids]
    return {
        **analysis,
        "resources": resources,
        "operations": operations,
        "operation_fields": operation_fields,
        "semantic_types": semantic_types,
        "entities": entities,
        "entity_identifiers": entity_identifiers,
        "capabilities": capabilities,
        "capability_entity_links": capability_entity_links,
        "capability_dependencies": capability_dependencies,
        "operation_contracts": operation_contracts,
        "operation_variants": operation_variants,
        "field_mappings": field_mappings,
        "semantic_join_rules": semantic_join_rules,
        "planning_examples": planning_examples,
        "capability_implementations": capability_implementations,
    }


def _variant_fixed_raw_arguments_are_declared(
    variant: dict[str, Any],
    request_fields_by_operation: dict[str, set[str]],
) -> bool:
    fixed_raw_arguments = variant.get("fixed_raw_arguments")
    if not isinstance(fixed_raw_arguments, dict) or not fixed_raw_arguments:
        return True
    request_fields = request_fields_by_operation.get(str(variant.get("operation_id") or ""), set())
    return set(str(key) for key in fixed_raw_arguments) <= request_fields


def _contract_request_raw_fields(request_contract: Any) -> set[str]:
    if not isinstance(request_contract, dict):
        return set()
    raw_fields: set[str] = set()
    for fields in request_contract.values():
        if isinstance(fields, dict):
            raw_fields.update(str(key) for key in fields)
    return raw_fields


def _call_capability_llm(state: SourceGraphState) -> dict[str, Any] | None:
    mode = _llm_mode()
    if mode != "openai":
        return None
    api_key = openai_api_key()
    if not api_key:
        return None
    sections = [section for section in state.get("api_sections", []) if isinstance(section, dict)]
    batch_size = _llm_operation_batch_size()
    if len(sections) > batch_size:
        analyses = []
        batches = [sections[index : index + batch_size] for index in range(0, len(sections), batch_size)]
        for index, batch in enumerate(batches, start=1):
            _emit_llm_progress(
                state,
                {
                    "phase": "llm_operation_batch",
                    "status": "running",
                    "batch_index": index,
                    "batch_count": len(batches),
                    "operation_names": [str(section.get("operation_name") or section.get("path") or "") for section in batch],
                },
            )
            analysis = _call_capability_llm_once(_state_for_api_sections(state, batch), api_key)
            if isinstance(analysis, dict):
                analyses.append(analysis)
            _emit_llm_progress(
                state,
                {
                    "phase": "llm_operation_batch",
                    "status": "completed",
                    "batch_index": index,
                    "batch_count": len(batches),
                    "operation_count": len(_list((analysis or {}).get("operations"))) if isinstance(analysis, dict) else 0,
                    "capability_count": len(_list((analysis or {}).get("capabilities"))) if isinstance(analysis, dict) else 0,
                },
            )
        return _merge_llm_analyses([analysis for analysis in analyses if isinstance(analysis, dict)])
    _emit_llm_progress(
        state,
        {
            "phase": "llm_operation_batch",
            "status": "running",
            "batch_index": 1,
            "batch_count": 1,
            "operation_names": [str(section.get("operation_name") or section.get("path") or "") for section in sections],
        },
    )
    return _call_capability_llm_once(state, api_key)


def _call_capability_llm_once(state: SourceGraphState, api_key: str) -> dict[str, Any] | None:
    payload = {
        "model": os.getenv("SEMANTIC_PLATFORM_SOURCE_LLM_MODEL", os.getenv("SEMANTIC_PLATFORM_LLM_MODEL", "gpt-4.1-mini")),
        "temperature": 0,
        "response_format": {"type": "json_object"},
        "messages": [
            {
                "role": "system",
                "content": (
                    "You create semantic catalog proposals from public API specification evidence. "
                    "Infer executable semantic resources, operations, contracts, and variants from api_sections, "
                    "field tables, examples, source_text_excerpt, and source evidence. "
                    "Endpoint probe results are verification evidence only; do not use missing or failed pre-probes "
                    "as the reason to omit a capability when the source document defines it. "
                    "The api_sections array is the authoritative operation list detected from the source. "
                    "Use source_text_excerpt to find shared service metadata such as provider, service URL, "
                    "base URL, authentication style, response format, and operation list context. "
                    "Create at least one resource for executable API sections, with base_url copied from "
                    "source evidence when present. "
                    "For every api_section with an operation_name and path, create an operation and operation_contract "
                    "unless the section is explicitly marked non-executable by the source evidence. "
                    "Create planner-facing capabilities for those executable operations; do not return only "
                    "semantic_types or entities when api_sections are present. "
                    "Return JSON only with keys: resources, operations, semantic_types, entities, "
                    "entity_identifiers, capabilities, capability_entity_links, capability_dependencies, "
                    "operation_fields, operation_contracts, operation_variants, field_mappings, "
                    "semantic_join_rules, planning_examples, capability_implementations. "
                    "Keep capability ids provider-neutral and describe what a planner can do. "
                    "Model canonical business concepts as entities, entity identifiers, semantic join rules, "
                    "and capability dependencies instead of hiding joins in endpoint descriptions. "
                    "Do not leave entities, entity_identifiers, capability_entity_links, or planning_examples empty "
                    "when the source has identifiable business objects, identifiers, or planner examples. "
                    "Create reusable entities such as Business, Contract, Organization, FinancialStatement, "
                    "ExchangeRate, and Currency when supported by the source evidence. "
                    "Each capability should link to at least one relevant entity with role input, output, or subject. "
                    "Each identifier semantic type should be attached to the entity it identifies. "
                    "Create semantic_join_rules only for reusable identifier equivalence or join paths "
                    "that can support multi-step planning across capabilities. "
                    "Create planning_examples that map realistic user questions to expected capability, "
                    "operation, variant, and semantic argument ids. "
                    "Treat provider control fields as operation-scoped semantics, never global rules. "
                    "If one physical endpoint changes meaning by a control field value, create separate "
                    "operation_variants and separate planner-facing capabilities for those meanings. "
                    "Keep operation_contracts at the physical operation level; when variants have distinct "
                    "capabilities, put the selected capability_id on operation_variants and "
                    "capability_implementations rather than forcing one operation_contract capability. "
                    "For example, if the evidence says a query divider value changes required request fields "
                    "or search basis, the variants must carry fixed_raw_arguments and planner-visible "
                    "fixed_semantic_arguments instead of hiding that choice in executor defaults. "
                    "When operation_variant_candidates contains a control with two or more values, either "
                    "create one variant per semantically distinct value or leave only one variant only if "
                    "the evidence clearly says the values are transport-only and not user-visible. "
                    "For each variant created from a control value, set fixed_raw_arguments to the exact "
                    "raw provider value and set verification.sample_semantic_arguments or default request "
                    "fields so the variant can be live-verified independently. "
                    "Every operation_contract must be executable by a generic contract interpreter. "
                    "Declare auth as auth.in, auth.parameter, and auth.env_names. "
                    "Declare request defaults, required fields, semantic_type, transform, and format on request fields. "
                    "Declare response.items_path, optional response.count_path, response.success, response.error, "
                    "and response.fields with explicit raw paths mapped to semantic_type. "
                    "Do not rely on runtime/provider conventions for item roots, pagination names, success codes, "
                    "response formats, or default query parameters; put those facts in the contract. "
                    "Preserve evidence references from section_id/block_id where possible."
                ),
            },
            {
                "role": "user",
                "content": json.dumps(_capability_llm_context(state), ensure_ascii=False, default=str),
            },
        ],
    }
    http_request = request.Request(
        os.getenv("SEMANTIC_PLATFORM_LLM_API_URL", "https://api.openai.com/v1/chat/completions"),
        data=json.dumps(payload).encode("utf-8"),
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with request.urlopen(http_request, timeout=float(os.getenv("SEMANTIC_PLATFORM_LLM_TIMEOUT_SECONDS", "180"))) as response:
            body = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:1000]
        raise ValueError(f"openai LLM request failed: HTTP {exc.code}: {_redact_secret_text(detail)}") from exc
    except (URLError, TimeoutError) as exc:
        raise ValueError(f"openai LLM request failed: {type(exc).__name__}: {exc}") from exc
    try:
        content = body["choices"][0]["message"]["content"]
        parsed = json.loads(content)
    except (KeyError, IndexError, TypeError, json.JSONDecodeError, ValueError) as exc:
        raise ValueError(f"openai LLM response was not valid catalog JSON: {type(exc).__name__}: {exc}") from exc
    return parsed if isinstance(parsed, dict) else None


def _llm_operation_batch_size() -> int:
    try:
        return max(1, int(os.getenv("SEMANTIC_PLATFORM_SOURCE_LLM_OPERATION_BATCH_SIZE", "1")))
    except ValueError:
        return 1


def _emit_llm_progress(state: SourceGraphState, event: dict[str, Any]) -> None:
    state["llm_progress"] = {**(state.get("llm_progress") or {}), **event}
    callback = state.get("progress_callback")
    if callable(callback):
        callback({"step": "llm_propose_capability_catalog", **event})


def _state_for_api_sections(state: SourceGraphState, sections: list[dict[str, Any]]) -> SourceGraphState:
    section_ids = {str(section.get("id") or "") for section in sections}
    evidence = state.get("structured_evidence", {})
    return {
        **state,
        "api_sections": sections,
        "verified_api_sections": [
            section
            for section in state.get("verified_api_sections", [])
            if str(section.get("id") or "") in section_ids
        ],
        "endpoint_candidate_checks": [
            check
            for check in state.get("endpoint_candidate_checks", [])
            if str(check.get("section_id") or "") in section_ids
        ],
        "structured_evidence": {
            **evidence,
            "operation_candidates": [
                item
                for item in evidence.get("operation_candidates", [])
                if str(item.get("section_id") or "") in section_ids
            ],
            "field_table_candidates": [
                item
                for item in evidence.get("field_table_candidates", [])
                if str(item.get("section_id") or "") in section_ids
            ],
            "example_candidates": [
                item
                for item in evidence.get("example_candidates", [])
                if str(item.get("section_id") or "") in section_ids
            ],
            "control_field_candidates": [
                item
                for item in evidence.get("control_field_candidates", [])
                if str(item.get("section_id") or "") in section_ids
            ],
        },
    }


def _merge_llm_analyses(analyses: list[dict[str, Any]]) -> dict[str, Any]:
    keys = (
        "resources",
        "operations",
        "operation_fields",
        "semantic_types",
        "entities",
        "entity_identifiers",
        "capabilities",
        "capability_entity_links",
        "capability_dependencies",
        "operation_contracts",
        "operation_variants",
        "field_mappings",
        "semantic_join_rules",
        "planning_examples",
        "capability_implementations",
    )
    merged: dict[str, Any] = {key: [] for key in keys}
    for analysis in analyses:
        for key in keys:
            merged[key].extend(_list(analysis.get(key)))
    for key in keys:
        merged[key] = _dedupe_payloads(merged[key])
    return merged


def _normalize_llm_analysis(analysis: dict[str, Any]) -> dict[str, Any]:
    normalized = {**analysis}
    entity_ids = {str(item.get("id") or "") for item in _list(analysis.get("entities"))}
    entity_ids.discard("")
    semantic_type_ids = {str(item.get("id") or "") for item in _list(analysis.get("semantic_types"))}
    semantic_type_ids.discard("")
    normalized["operations"] = _normalize_operation_ids(_list(analysis.get("operations")))
    normalized["operation_contracts"] = [
        _normalize_operation_contract_fields(contract)
        for contract in _normalize_operation_ids(_list(analysis.get("operation_contracts")))
    ]
    normalized["operation_variants"] = _normalize_operation_ids(_list(analysis.get("operation_variants")))
    normalized["entity_identifiers"] = _drop_missing_keys(
        _normalize_semantic_type_aliases(_list(analysis.get("entity_identifiers")), "semantic_type_id"),
        ("entity_id", "semantic_type_id"),
    )
    normalized["entity_identifiers"] = _drop_unknown_refs(
        normalized["entity_identifiers"],
        {"entity_id": entity_ids, "semantic_type_id": semantic_type_ids},
    )
    normalized["capability_entity_links"] = _drop_missing_keys(
        _normalize_semantic_type_aliases(_list(analysis.get("capability_entity_links")), "semantic_type_id"),
        ("capability_id", "entity_id"),
    )
    normalized["capability_entity_links"] = _drop_unknown_refs(
        normalized["capability_entity_links"],
        {"entity_id": entity_ids, "semantic_type_id": semantic_type_ids},
        allow_empty={"semantic_type_id"},
    )
    normalized["capability_dependencies"] = _drop_missing_keys(
        _list(analysis.get("capability_dependencies")),
        ("capability_id", "depends_on_capability_id"),
    )
    normalized["field_mappings"] = _drop_missing_keys(
        _normalize_semantic_type_aliases(_list(analysis.get("field_mappings")), "semantic_type_id"),
        ("operation_id", "semantic_type_id"),
    )
    normalized["field_mappings"] = _drop_unknown_refs(
        normalized["field_mappings"],
        {"semantic_type_id": semantic_type_ids},
    )
    normalized["semantic_join_rules"] = [
        item
        for item in (_normalize_join_rule(item) for item in _list(analysis.get("semantic_join_rules")))
        if item.get("from_semantic_type_id") and item.get("to_semantic_type_id")
    ]
    normalized["semantic_join_rules"] = _drop_unknown_refs(
        normalized["semantic_join_rules"],
        {
            "from_entity_id": entity_ids,
            "to_entity_id": entity_ids,
            "from_semantic_type_id": semantic_type_ids,
            "to_semantic_type_id": semantic_type_ids,
        },
        allow_empty={"from_entity_id", "to_entity_id"},
    )
    normalized["planning_examples"] = _drop_missing_keys(
        _list(analysis.get("planning_examples")),
        ("id", "question"),
    )
    normalized["capability_implementations"] = _drop_missing_keys(
        _list(analysis.get("capability_implementations")),
        ("operation_id", "capability_id"),
    )
    return normalized


def _normalize_operation_ids(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized = []
    for item in items:
        payload = dict(item)
        if not payload.get("operation_id"):
            payload["operation_id"] = payload.get("id") or payload.get("operation") or payload.get("name")
        normalized.append(payload)
    return normalized


def _normalize_operation_contract_fields(contract: dict[str, Any]) -> dict[str, Any]:
    payload = dict(contract)
    request_contract = payload.get("request")
    if isinstance(request_contract, dict):
        payload["request"] = {
            section: _normalize_contract_field_specs(fields, drop_missing_semantic_type=False)
            for section, fields in request_contract.items()
        }
    response_contract = payload.get("response")
    if isinstance(response_contract, dict):
        response = dict(response_contract)
        if isinstance(response.get("fields"), dict):
            response["fields"] = _normalize_contract_field_specs(response["fields"], drop_missing_semantic_type=True)
        payload["response"] = response
    return payload


def _normalize_contract_field_specs(fields: Any, *, drop_missing_semantic_type: bool) -> dict[str, Any]:
    if not isinstance(fields, dict):
        return {}
    normalized: dict[str, Any] = {}
    for name, spec in fields.items():
        field_spec = dict(spec) if isinstance(spec, dict) else {}
        if not field_spec.get("semantic_type"):
            field_spec["semantic_type"] = (
                field_spec.get("semantic_type_id")
                or field_spec.get("semanticType")
                or field_spec.get("semanticTypeId")
            )
        if drop_missing_semantic_type and not field_spec.get("semantic_type"):
            continue
        normalized[str(name)] = field_spec
    return normalized


def _drop_missing_keys(items: list[dict[str, Any]], keys: tuple[str, ...]) -> list[dict[str, Any]]:
    return [item for item in items if all(item.get(key) for key in keys)]


def _drop_unknown_refs(
    items: list[dict[str, Any]],
    allowed_by_key: dict[str, set[str]],
    *,
    allow_empty: set[str] | None = None,
) -> list[dict[str, Any]]:
    allow_empty = allow_empty or set()
    filtered = []
    for item in items:
        keep = True
        for key, allowed_values in allowed_by_key.items():
            value = str(item.get(key) or "")
            if not value and key in allow_empty:
                continue
            if not allowed_values:
                if value:
                    keep = False
                    break
                continue
            if value and value not in allowed_values:
                keep = False
                break
        if keep:
            filtered.append(item)
    return filtered


def _normalize_semantic_type_aliases(items: list[dict[str, Any]], target_key: str) -> list[dict[str, Any]]:
    normalized = []
    for item in items:
        payload = dict(item)
        if not payload.get(target_key):
            payload[target_key] = (
                payload.get("semantic_type")
                or payload.get("semanticType")
                or payload.get("semantic_type_name")
            )
        normalized.append(payload)
    return normalized


def _normalize_join_rule(item: dict[str, Any]) -> dict[str, Any]:
    payload = dict(item)
    if not payload.get("from_semantic_type_id"):
        payload["from_semantic_type_id"] = payload.get("from_semantic_type") or payload.get("fromSemanticType")
    if not payload.get("to_semantic_type_id"):
        payload["to_semantic_type_id"] = payload.get("to_semantic_type") or payload.get("toSemanticType")
    return payload


def _dedupe_payloads(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen = set()
    deduped = []
    for item in items:
        key = (
            item.get("id")
            or item.get("operation_id")
            or item.get("variant_id")
            or json.dumps(item, ensure_ascii=False, sort_keys=True, default=str)
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def _analysis_has_catalog_items(analysis: dict[str, Any]) -> bool:
    return any(
        _list(analysis.get(key))
        for key in (
            "resources",
            "operations",
            "semantic_types",
            "entities",
            "capabilities",
            "operation_contracts",
            "operation_variants",
            "field_mappings",
            "capability_implementations",
        )
    )


def _require_executable_catalog_items(analysis: dict[str, Any]) -> None:
    missing = [
        key
        for key in ("resources", "operations", "capabilities", "operation_contracts")
        if not _list(analysis.get(key))
    ]
    if missing:
        raise ValueError(
            "openai LLM returned no executable semantic catalog items for detected API sections: "
            + ", ".join(missing)
        )


def _redact_secret_text(text: str) -> str:
    return re.sub(r"sk-[A-Za-z0-9_-]+", "***", text)


def _capability_llm_context(state: SourceGraphState) -> dict[str, Any]:
    evidence_section_ids = _evidence_section_ids_for_llm(state)
    evidence = state.get("structured_evidence", {})
    return {
        "source_document": state.get("source_document", {}),
        "source_text_excerpt": _source_text_excerpt(state),
        "catalog_context": state.get("catalog_context", {}),
        "api_sections": [
            section
            for section in _evidence_sections_for_llm(state)
            if str(section.get("id") or "") in evidence_section_ids
        ],
        "verified_api_sections": [
            section
            for section in state.get("verified_api_sections", [])
            if str(section.get("id") or "") in evidence_section_ids
        ],
        "endpoint_candidate_checks": [
            check
            for check in state.get("endpoint_candidate_checks", [])
            if str(check.get("section_id") or "") in evidence_section_ids
        ],
        "field_table_candidates": [
            table
            for table in evidence.get("field_table_candidates", [])
            if str(table.get("section_id") or "") in evidence_section_ids
        ][:120],
        "example_candidates": [
            example
            for example in evidence.get("example_candidates", [])
            if str(example.get("section_id") or "") in evidence_section_ids
        ][:80],
        "control_field_candidates": [
            control
            for control in evidence.get("control_field_candidates", [])
            if str(control.get("section_id") or "") in evidence_section_ids
        ][:120],
        "operation_variant_candidates": operation_variant_candidates(state),
        "semantic_graph_schema": {
            "entity_identifier": {
                "id": "stable id",
                "entity_id": "declared entity id",
                "semantic_type_id": "declared semantic type id; do not use semantic_type here",
                "identifier_role": "primary|alternate|external when known",
            },
            "capability_entity_link": {
                "capability_id": "declared capability id",
                "entity_id": "declared entity id",
                "semantic_type_id": "optional declared semantic type id",
                "role": "input|output|subject|context",
            },
            "field_mapping": {
                "operation_id": "declared operation id",
                "raw_name": "provider field/path",
                "semantic_type_id": "declared semantic type id; do not use semantic_type here",
                "direction": "request|response",
            },
        },
        "operation_contract_schema": {
            "auth": {
                "type": "api_key",
                "in": "query|header",
                "parameter": "raw auth parameter name from the source document",
                "env_names": ["manifest-provided env name, filled by graph when available"],
            },
            "request": {
                "query|body|path|header": {
                    "raw_field_name": {
                        "semantic_type": "canonical semantic type id",
                        "required": False,
                        "default": "declared provider default if the document defines one",
                        "transform": "date_start|date_end when mapping a semantic range",
                        "format": "provider date/number/string format when specified",
                    }
                }
            },
            "operation_variant": {
                "variant_id": "stable id",
                "operation_id": "operation id",
                "capability_id": "planner-facing capability id",
                "fixed_raw_arguments": {"raw_control_field": "raw provider value"},
                "fixed_semantic_arguments": {"semantic_control": "planner-visible value"},
                "verification": {
                    "safe_to_call": True,
                    "sample_semantic_arguments": "minimum semantic arguments needed for live verification",
                },
            },
            "response": {
                "items_path": "JSON path to the returned item array/object, or a list of candidate paths",
                "count_path": "optional JSON path to total row count",
                "success": {"path": "JSON path", "equals": "success code", "message_path": "optional message path"},
                "error": {"code_path": "JSON path", "not_equals": "success code", "message_path": "optional message path"},
                "fields": {
                    "raw response path such as response.body.items.item[].field": {
                        "semantic_type": "canonical semantic type id"
                    }
                },
            },
        },
        "instructions": {
            "variant_policy": (
                "Create one operation_variant per distinct control value when that value changes the "
                "semantic meaning, required request arguments, search basis, or response interpretation."
            ),
            "capability_policy": (
                "Create planner-facing capabilities at the variant level when users would ask for those "
                "meanings differently. A physical endpoint is not necessarily one capability."
            ),
            "executor_boundary": (
                "Executors must not guess provider control values, auth names, response item locations, "
                "pagination fields, success/error conventions, or response-format defaults. Put those "
                "facts in operation_contract request/response/auth or operation_variant fixed arguments."
            ),
        },
    }


def _source_text_excerpt(state: SourceGraphState) -> str:
    try:
        limit = int(os.getenv("SEMANTIC_PLATFORM_SOURCE_LLM_CONTEXT_TEXT_CHARS", "16000"))
    except ValueError:
        limit = 16000
    text = str(state.get("extracted_text") or "")
    return text[: max(0, limit)]


def _evidence_sections_for_llm(state: SourceGraphState) -> list[dict[str, Any]]:
    verified_sections = [section for section in state.get("verified_api_sections", []) if isinstance(section, dict)]
    if verified_sections:
        return verified_sections
    return [section for section in state.get("api_sections", []) if isinstance(section, dict)]


def _evidence_section_ids_for_llm(state: SourceGraphState) -> set[str]:
    return {str(section.get("id") or "") for section in _evidence_sections_for_llm(state)}


def _operation_fields_from_analysis(analysis: dict[str, Any], state: SourceGraphState) -> list[dict[str, Any]]:
    source_document_id = state.get("source_document", {}).get("id")
    section_by_operation = {
        str(section.get("operation_name") or ""): section
        for section in _evidence_sections_for_llm(state)
        if section.get("operation_name")
    }
    fields: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for contract in _list(analysis.get("operation_contracts")):
        operation_id = str(contract.get("operation_id") or "")
        if not operation_id:
            continue
        operation_name = operation_id.rsplit(".", 1)[-1]
        section = section_by_operation.get(operation_name, {})
        for direction, contract_key in (("request", "request"), ("response", "response")):
            for raw_name, field_contract in _contract_fields(contract.get(contract_key)).items():
                key = (operation_id, direction, raw_name)
                if key in seen:
                    continue
                seen.add(key)
                field = field_contract if isinstance(field_contract, dict) else {}
                fields.append(
                    {
                        "id": f"operation_field.{operation_id}.{direction}.{_field_id(raw_name)}",
                        "operation_id": operation_id,
                        "direction": direction,
                        "raw_name": raw_name,
                        "location": _operation_field_location(contract.get(contract_key), raw_name, direction),
                        "path": field.get("path"),
                        "label_ko": field.get("label_ko") or field.get("label"),
                        "description_ko": field.get("description_ko") or field.get("description"),
                        "example": field.get("example"),
                        "type_hint": field.get("type") or field.get("type_hint"),
                        "unit_hint": field.get("unit_hint"),
                        "required": field.get("required"),
                        "status": "approved",
                        "source_document_id": source_document_id,
                        "source_chunk_id": section.get("id"),
                        "evidence": {
                            "type": "operation_contract_field",
                            "source_document_id": source_document_id,
                            "source_chunk_id": section.get("id"),
                            "operation_id": operation_id,
                            "semantic_type": field.get("semantic_type"),
                        },
                    }
                )
    return fields


def _operation_field_location(contract_part: Any, raw_name: str, direction: str) -> str:
    if direction != "request":
        return "body"
    if isinstance(contract_part, dict):
        for location in ("query", "body", "path", "header"):
            fields = contract_part.get(location)
            if isinstance(fields, dict) and raw_name in fields:
                return location
    return "body"


def _field_mappings_with_operation_fields(
    mappings: list[dict[str, Any]],
    operation_fields: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    field_ids = {
        (
            str(field.get("operation_id") or ""),
            str(field.get("direction") or ""),
            str(field.get("raw_name") or ""),
        ): field.get("id")
        for field in operation_fields
    }
    enriched = []
    for mapping in mappings:
        if not isinstance(mapping, dict):
            continue
        key = (
            str(mapping.get("operation_id") or ""),
            str(mapping.get("direction") or ""),
            str(mapping.get("raw_name") or mapping.get("field_name") or ""),
        )
        if not mapping.get("operation_field_id") and field_ids.get(key):
            mapping = {**mapping, "operation_field_id": field_ids[key]}
        enriched.append(mapping)
    return enriched


def _contract_fields(contract_part: Any) -> dict[str, dict[str, Any]]:
    if not isinstance(contract_part, dict):
        return {}
    request_sections = ("query", "body", "path", "header")
    if any(isinstance(contract_part.get(section), dict) for section in request_sections):
        fields: dict[str, dict[str, Any]] = {}
        for section in request_sections:
            section_fields = contract_part.get(section)
            if not isinstance(section_fields, dict):
                continue
            for key, value in section_fields.items():
                if isinstance(value, dict):
                    fields[str(key)] = value
        return fields
    if isinstance(contract_part.get("fields"), dict):
        return {str(key): value for key, value in contract_part["fields"].items() if isinstance(value, dict)}
    return {str(key): value for key, value in contract_part.items() if isinstance(value, dict) and "semantic_type" in value}


def _field_id(raw_name: str) -> str:
    return re.sub(r"[^0-9A-Za-z가-힣._-]+", "_", raw_name).strip("_").lower()[:80] or "field"


def _request_fields_from_tables(tables: list[dict[str, Any]]) -> list[dict[str, Any]]:
    fields: list[dict[str, Any]] = []
    seen: set[str] = set()
    for table in tables:
        if table.get("direction_hint") not in {"request", "unknown"}:
            continue
        for row_index, cells in enumerate(table.get("rows", [])):
            if not isinstance(cells, list) or len(cells) < 2:
                continue
            raw_name = str(cells[0] or "").strip()
            if not raw_name or raw_name.lower() in {"name", "항목명(영문)", "항목명", "parameter", "param"}:
                continue
            if not _looks_like_field_name(raw_name):
                continue
            key = raw_name.lower()
            if key in seen:
                continue
            seen.add(key)
            fields.append(
                {
                    "raw_name": raw_name,
                    "label": str(cells[1] or "").strip() if len(cells) > 1 else "",
                    "required_hint": str(cells[3] or "").strip() if len(cells) > 3 else "",
                    "sample": str(cells[4] or "").strip() if len(cells) > 4 else "",
                    "description": str(cells[-1] or "").strip(),
                    "evidence": table.get("evidence", {}),
                }
            )
    return fields


def _looks_like_field_name(value: str) -> bool:
    if len(value) > 80 or value.isdigit() or any(char.isspace() for char in value):
        return False
    return bool(re.search(r"[A-Za-z_가-힣]", value))


def _control_field_name(control: dict[str, Any]) -> str:
    field_name = str(control.get("field_name") or "").strip()
    if field_name:
        return field_name
    text = str(control.get("text") or "")
    if text.strip().startswith("|"):
        cells = [cell.strip() for cell in text.strip().strip("|").split("|")]
        if cells and cells[0]:
            return cells[0]
    match = re.search(r"\b[A-Za-z_][A-Za-z0-9_]{1,60}\b", text)
    return match.group(0) if match else "control_field"


def _variant_generation_hint(
    control_name: str,
    control: dict[str, Any],
    related_request_fields: list[dict[str, Any]],
) -> dict[str, Any]:
    values = control.get("values") if isinstance(control.get("values"), list) else []
    return {
        "should_review_for_variants": len(values) >= 2 or bool(related_request_fields),
        "control_raw_name": control_name,
        "fixed_raw_argument_template": {control_name: "<selected raw value>"},
        "evidence_summary": (
            "Values may change semantic meaning, required request fields, search basis, or response interpretation. "
            "The LLM must decide from evidence; runtime code must not infer provider choices."
        ),
    }


def _related_request_fields(
    control_name: str,
    control: dict[str, Any],
    request_fields: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    values = control.get("values") if isinstance(control.get("values"), list) else []
    needles = {control_name}
    for value in values:
        if isinstance(value, dict):
            label = str(value.get("label") or "")
            if len(label) > 1:
                needles.add(label)
    related = []
    for field in request_fields:
        text = " ".join(
            str(field.get(key) or "")
            for key in ("raw_name", "label", "required_hint", "sample", "description")
        )
        if any(needle and needle in text for needle in needles):
            related.append(field)
    return related[:40]


def _llm_mode() -> str:
    return active_llm_mode()


def _legacy_items(payload: dict[str, Any]) -> bool:
    return isinstance(payload.get("items"), list)


def _list(value: Any) -> list[dict[str, Any]]:
    return [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []


def _list_values(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []
