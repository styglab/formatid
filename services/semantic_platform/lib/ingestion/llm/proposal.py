from __future__ import annotations

import json
import os
import re
from typing import Any
from urllib import request
from urllib.error import HTTPError, URLError

from services.semantic_platform.lib.ingestion.llm.validation import validate_llm_analysis as _validate_llm_analysis
from services.semantic_platform.lib.ingestion.state import SourceGraphState


def llm_propose_capability_catalog(state: SourceGraphState) -> SourceGraphState:
    manual = state.get("manual_llm_response") or {}
    if _legacy_items(manual):
        analysis: dict[str, Any] = {}
    elif manual:
        analysis = manual
    else:
        analysis = _call_capability_llm(state) or {}
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
    return {
        **state,
        "execution_analysis": {
            "resources": _list(analysis.get("resources")),
            "operations": _list(analysis.get("operations")),
            "operation_fields": operation_fields,
            "operation_contracts": operation_contracts,
            "operation_variants": _list(analysis.get("operation_variants")),
            "field_mappings": field_mappings,
            "capability_implementations": _list(analysis.get("capability_implementations")),
        },
        **_filter_analysis_by_passed_endpoints(
            state,
            {
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
            },
        ),
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
        if section_id in evidence_section_ids and values:
            controls_by_section.setdefault(section_id, []).append(control)

    candidates = []
    for section in _evidence_sections_for_llm(state):
        section_id = str(section.get("id") or "")
        if section_id not in evidence_section_ids:
            continue
        request_fields = _request_fields_from_tables(tables_by_section.get(section_id, []))
        controls = []
        for control in controls_by_section.get(section_id, []):
            control_name = _control_field_name(control)
            controls.append(
                {
                    "raw_name": control_name,
                    "text": control.get("text"),
                    "values": control.get("values", []),
                    "evidence": control.get("evidence", {}),
                    "related_request_fields": _related_request_fields(control_name, control, request_fields),
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


def _call_capability_llm(state: SourceGraphState) -> dict[str, Any] | None:
    mode = _llm_mode()
    if mode != "openai":
        return None
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return None
    payload = {
        "model": os.getenv("SEMANTIC_PLATFORM_SOURCE_LLM_MODEL", os.getenv("SEMANTIC_PLATFORM_LLM_MODEL", "gpt-4.1-mini")),
        "temperature": 0,
        "response_format": {"type": "json_object"},
        "messages": [
            {
                "role": "system",
                "content": (
                    "You create semantic catalog proposals from public API specification evidence. "
                    "Use only verified_api_sections whose endpoint probe status passed. "
                    "Do not create capabilities for failed or inconclusive endpoints. "
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
                "content": json.dumps(_capability_llm_context(state), ensure_ascii=False),
            },
        ],
    }
    try:
        http_request = request.Request(
            os.getenv("SEMANTIC_PLATFORM_LLM_API_URL", "https://api.openai.com/v1/chat/completions"),
            data=json.dumps(payload).encode("utf-8"),
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            method="POST",
        )
        with request.urlopen(http_request, timeout=float(os.getenv("SEMANTIC_PLATFORM_LLM_TIMEOUT_SECONDS", "60"))) as response:
            body = json.loads(response.read().decode("utf-8"))
        content = body["choices"][0]["message"]["content"]
        parsed = json.loads(content)
    except (HTTPError, URLError, TimeoutError, KeyError, IndexError, json.JSONDecodeError, ValueError):
        return None
    return parsed if isinstance(parsed, dict) else None


def _capability_llm_context(state: SourceGraphState) -> dict[str, Any]:
    evidence_section_ids = _evidence_section_ids_for_llm(state)
    evidence = state.get("structured_evidence", {})
    return {
        "source_document": state.get("source_document", {}),
        "catalog_context": state.get("catalog_context", {}),
        "verified_api_sections": [
            section
            for section in _evidence_sections_for_llm(state)
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


def _evidence_sections_for_llm(state: SourceGraphState) -> list[dict[str, Any]]:
    verified_sections = [section for section in state.get("verified_api_sections", []) if isinstance(section, dict)]
    if verified_sections:
        return verified_sections
    if state.get("manual_llm_response") or _llm_mode() == "codex_manual":
        return [section for section in state.get("api_sections", []) if isinstance(section, dict)]
    return verified_sections


def _evidence_section_ids_for_llm(state: SourceGraphState) -> set[str]:
    return {str(section.get("id") or "") for section in _evidence_sections_for_llm(state)}


def _operation_fields_from_analysis(analysis: dict[str, Any], state: SourceGraphState) -> list[dict[str, Any]]:
    source_document_id = state.get("source_document", {}).get("id")
    section_by_operation = {
        str(section.get("operation_name") or ""): section
        for section in state.get("verified_api_sections", [])
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
                        "location": "query" if direction == "request" else "body",
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
    text = str(control.get("text") or "")
    if text.strip().startswith("|"):
        cells = [cell.strip() for cell in text.strip().strip("|").split("|")]
        if cells and cells[0]:
            return cells[0]
    match = re.search(r"\b[A-Za-z_][A-Za-z0-9_]{1,60}\b", text)
    return match.group(0) if match else "control_field"


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
    mode = os.getenv("SEMANTIC_PLATFORM_LLM_MODE") or os.getenv("LLM_MODE") or "disabled"
    normalized = mode.strip().lower()
    return normalized if normalized in {"disabled", "codex_manual", "openai"} else "disabled"


def _legacy_items(payload: dict[str, Any]) -> bool:
    return isinstance(payload.get("items"), list)


def _list(value: Any) -> list[dict[str, Any]]:
    return [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []


def _list_values(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []
