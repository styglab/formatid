from __future__ import annotations

import os
from typing import Any

from services.semantic_platform.lib.ingestion.llm.runtime import active_llm_mode
from services.semantic_platform.lib.ingestion.state import SourceGraphState


def build_review_proposal(
    state: SourceGraphState,
    *,
    graph_node_names: list[str],
    operation_variant_candidates: list[dict[str, Any]],
) -> SourceGraphState:
    manual = state.get("manual_llm_response") or {}
    if _legacy_items(manual):
        proposal, items = _legacy_review_proposal(state, manual, graph_node_names, operation_variant_candidates)
        proposals = [proposal]
        item_groups = [items]
    else:
        proposals, item_groups = _capability_review_proposals(state, graph_node_names, operation_variant_candidates)
        proposal = proposals[0] if proposals else _empty_review_proposal(state, graph_node_names, operation_variant_candidates)
        items = [item for group in item_groups for item in group]
    return {
        **state,
        "proposal": proposal,
        "proposal_items": items,
        "proposals": proposals,
        "proposal_item_groups": item_groups,
    }


def _legacy_review_proposal(
    state: SourceGraphState,
    payload: dict[str, Any],
    graph_node_names: list[str],
    operation_variant_candidates: list[dict[str, Any]],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    source_document = state["source_document"]
    proposal_id = payload.get("proposal_id") or f"proposal.{source_document['id']}.manual"
    proposal = _proposal_envelope(state, proposal_id, payload.get("summary"), payload, graph_node_names, operation_variant_candidates)
    items = []
    for index, item in enumerate(payload.get("items", [])):
        if not isinstance(item, dict) or not item.get("item_type"):
            continue
        item_payload = item.get("payload", {}) if isinstance(item.get("payload"), dict) else {}
        items.append(
            {
                "id": item.get("id") or f"{proposal_id}.item.{index:04d}",
                "item_type": item["item_type"],
                "target_id": item.get("target_id") or _target_id(item_payload),
                "action": item.get("action", "upsert"),
                "status": "pending_review",
                "payload": item_payload,
                "evidence": item.get("evidence", {}),
            }
        )
    return proposal, items


def _capability_review_proposals(
    state: SourceGraphState,
    graph_node_names: list[str],
    operation_variant_candidates: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[list[dict[str, Any]]]]:
    proposals: list[dict[str, Any]] = []
    item_groups: list[list[dict[str, Any]]] = []
    for capability in state.get("capabilities", []):
        if not isinstance(capability, dict):
            continue
        capability_id = str(capability.get("id") or "")
        if not capability_id:
            continue
        proposal, items = _capability_review_proposal(state, capability_id, capability, graph_node_names, operation_variant_candidates)
        proposals.append(proposal)
        item_groups.append(items)
    return proposals, item_groups


def _capability_review_proposal(
    state: SourceGraphState,
    capability_id: str,
    capability: dict[str, Any],
    graph_node_names: list[str],
    operation_variant_candidates: list[dict[str, Any]],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    source_document = state["source_document"]
    closure = _capability_catalog_closure(state, capability_id, capability)
    variants = closure["operation_variants"]
    implementations = closure["capability_implementations"]
    operations = closure["operations"]
    operation_contracts = closure["operation_contracts"]
    operation_fields = closure["operation_fields"]
    field_mappings = closure["field_mappings"]
    resources = closure["resources"]
    semantic_types = closure["semantic_types"]
    entities = closure["entities"]
    entity_identifiers = closure["entity_identifiers"]
    capability_entity_links = closure["capability_entity_links"]
    capability_dependencies = closure["capability_dependencies"]
    semantic_join_rules = closure["semantic_join_rules"]
    planning_examples = closure["planning_examples"]

    proposal_id = f"proposal.{source_document['id']}.{capability_id}.review"
    trace = _capability_trace(state, capability_id, operation_contracts, variants)
    capability_payload = _with_trace(capability, trace)
    summary = {
        "capability_id": capability_id,
        "operation_count": len(operations),
        "operation_field_count": len(operation_fields),
        "resource_count": len(resources),
        "semantic_type_count": len(semantic_types),
        "entity_count": len(entities),
        "entity_identifier_count": len(entity_identifiers),
        "semantic_join_rule_count": len(semantic_join_rules),
        "planning_example_count": len(planning_examples),
        "operation_contract_count": len(operation_contracts),
        "operation_variant_count": len(variants),
        "field_mapping_count": len(field_mappings),
        "capability_implementation_count": len(implementations),
    }
    raw = {
        "capability": capability_payload,
        "resources": resources,
        "operations": operations,
        "operation_fields": operation_fields,
        "semantic_types": semantic_types,
        "entities": entities,
        "entity_identifiers": entity_identifiers,
        "capability_entity_links": capability_entity_links,
        "capability_dependencies": capability_dependencies,
        "semantic_join_rules": semantic_join_rules,
        "planning_examples": planning_examples,
        "operation_contracts": operation_contracts,
        "operation_variants": variants,
        "field_mappings": field_mappings,
        "capability_implementations": implementations,
        "trace": trace,
    }
    proposal = _proposal_envelope(
        state,
        proposal_id,
        summary,
        raw,
        graph_node_names,
        operation_variant_candidates,
        kind="capability_ingestion",
    )
    verification_by_variant = {
        str(item.get("variant_id") or ""): item
        for item in state.get("verification_results", [])
        if isinstance(item, dict)
    }
    items: list[dict[str, Any]] = []
    grouped_items: list[tuple[str, list[dict[str, Any]]]] = [
        ("resource", resources),
        ("semantic_type", semantic_types),
        ("entity", entities),
        ("entity_identifier", entity_identifiers),
        ("capability", [capability_payload]),
        ("capability_entity_link", capability_entity_links),
        ("capability_dependency", capability_dependencies),
        ("operation", operations),
        ("operation_field", operation_fields),
        ("operation_contract", operation_contracts),
        ("operation_variant", variants),
        ("field_mapping", field_mappings),
        ("semantic_join_rule", semantic_join_rules),
        ("planning_example", planning_examples),
        ("capability_implementation", implementations),
    ]
    for item_type, payloads in grouped_items:
        for index, payload in enumerate(payloads):
            if not isinstance(payload, dict):
                continue
            item_payload = _with_source_provenance(payload, state)
            evidence = dict(item_payload.get("evidence", {}) if isinstance(item_payload.get("evidence"), dict) else {})
            evidence.setdefault("source_document_id", source_document["id"])
            evidence.setdefault("proposal_capability_id", capability_id)
            if item_type == "operation_variant":
                evidence["verification"] = verification_by_variant.get(str(item_payload.get("variant_id") or ""), {})
            items.append(
                {
                    "id": f"{proposal_id}.{item_type}.{index:04d}",
                    "item_type": item_type,
                    "target_id": _target_id(item_payload),
                    "action": "upsert",
                    "status": "pending_review",
                    "payload": item_payload,
                    "evidence": evidence,
                }
            )
    return proposal, items


def _capability_catalog_closure(
    state: SourceGraphState,
    capability_id: str,
    capability: dict[str, Any],
) -> dict[str, list[dict[str, Any]]]:
    variants = [
        item for item in state.get("operation_variants", [])
        if str(item.get("capability_id") or item.get("capability") or "") == capability_id
    ]
    implementations = [
        item for item in state.get("capability_implementations", [])
        if str(item.get("capability_id") or "") == capability_id
    ]
    operation_ids = {
        str(item.get("operation_id") or "")
        for item in [*variants, *implementations]
        if item.get("operation_id")
    }
    operation_ids.update(
        str(item.get("operation_id") or "")
        for item in state.get("operation_contracts", [])
        if str(item.get("capability_id") or item.get("capability") or "") == capability_id
        and item.get("operation_id")
    )
    operations = [item for item in state.get("operations", []) if str(item.get("operation_id") or "") in operation_ids]
    operation_contracts = [
        item for item in state.get("operation_contracts", [])
        if str(item.get("operation_id") or "") in operation_ids
        and str(item.get("capability_id") or item.get("capability") or capability_id) == capability_id
    ]
    operation_by_id = {str(item.get("operation_id") or ""): item for item in operations}
    resource_by_id = {str(item.get("id") or ""): item for item in state.get("resources", [])}
    operation_contracts = [
        _contract_with_operation_trace(item, operation_by_id, resource_by_id)
        for item in operation_contracts
    ]
    semantic_type_ids = set(str(value) for value in _list_values(capability.get("inputs")))
    semantic_type_ids.update(str(value) for value in _list_values(capability.get("outputs")))
    for contract in operation_contracts:
        semantic_type_ids.update(_semantic_types_from_contract(contract.get("request")))
        semantic_type_ids.update(_semantic_types_from_contract(contract.get("response")))
    field_mappings = [
        item for item in state.get("field_mappings", [])
        if str(item.get("operation_id") or "") in operation_ids
        and str(item.get("semantic_type_id") or "") in semantic_type_ids
    ]
    semantic_type_ids.update(str(item.get("semantic_type_id") or "") for item in field_mappings if item.get("semantic_type_id"))
    capability_entity_links = [
        item for item in state.get("capability_entity_links", [])
        if str(item.get("capability_id") or "") == capability_id
    ]
    capability_dependencies = [
        item for item in state.get("capability_dependencies", [])
        if str(item.get("capability_id") or "") == capability_id
    ]
    semantic_type_ids.update(str(item.get("semantic_type_id") or "") for item in capability_entity_links if item.get("semantic_type_id"))
    semantic_type_ids.update(str(item.get("semantic_type_id") or "") for item in capability_dependencies if item.get("semantic_type_id"))
    entity_ids = {
        str(item.get("entity_id") or "")
        for item in capability_entity_links
        if item.get("entity_id")
    }
    entity_identifiers = [
        item for item in state.get("entity_identifiers", [])
        if str(item.get("entity_id") or "") in entity_ids
        or str(item.get("semantic_type_id") or "") in semantic_type_ids
    ]
    entity_ids.update(str(item.get("entity_id") or "") for item in entity_identifiers if item.get("entity_id"))
    semantic_type_ids.update(str(item.get("semantic_type_id") or "") for item in entity_identifiers if item.get("semantic_type_id"))
    semantic_join_rules = [
        item for item in state.get("semantic_join_rules", [])
        if str(item.get("from_semantic_type_id") or "") in semantic_type_ids
        or str(item.get("to_semantic_type_id") or "") in semantic_type_ids
        or str(item.get("from_entity_id") or "") in entity_ids
        or str(item.get("to_entity_id") or "") in entity_ids
    ]
    planning_examples = [
        item for item in state.get("planning_examples", [])
        if capability_id in {str(value) for value in _list_values(item.get("expected_capability_ids"))}
    ]
    operation_field_ids = {
        str(item.get("operation_field_id") or "")
        for item in field_mappings
        if item.get("operation_field_id")
    }
    operation_field_raw_keys = {
        (
            str(item.get("operation_id") or ""),
            str(item.get("direction") or ""),
            str(item.get("raw_name") or ""),
        )
        for item in field_mappings
    }
    operation_fields = [
        item for item in state.get("operation_fields", [])
        if str(item.get("operation_id") or "") in operation_ids
        and (
            str(item.get("id") or "") in operation_field_ids
            or (
                str(item.get("operation_id") or ""),
                str(item.get("direction") or ""),
                str(item.get("raw_name") or ""),
            )
            in operation_field_raw_keys
            or _operation_field_semantic_type(item) in semantic_type_ids
        )
    ]
    resource_ids = {
        str(item.get("resource_id") or "")
        for item in [*operations, *operation_contracts]
        if item.get("resource_id")
    }
    resources = [item for item in state.get("resources", []) if str(item.get("id") or "") in resource_ids]
    entities = [item for item in state.get("entities", []) if str(item.get("id") or "") in entity_ids]
    semantic_types = [item for item in state.get("semantic_types", []) if str(item.get("id") or "") in semantic_type_ids]
    return {
        "resources": resources,
        "operations": operations,
        "operation_fields": operation_fields,
        "semantic_types": semantic_types,
        "entities": entities,
        "entity_identifiers": entity_identifiers,
        "operation_contracts": operation_contracts,
        "operation_variants": variants,
        "field_mappings": field_mappings,
        "semantic_join_rules": semantic_join_rules,
        "capability_entity_links": capability_entity_links,
        "capability_dependencies": capability_dependencies,
        "planning_examples": planning_examples,
        "capability_implementations": implementations,
    }


def _contract_with_operation_trace(
    contract: dict[str, Any],
    operation_by_id: dict[str, dict[str, Any]],
    resource_by_id: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    operation = operation_by_id.get(str(contract.get("operation_id") or ""), {})
    resource = resource_by_id.get(str(contract.get("resource_id") or operation.get("resource_id") or ""), {})
    enriched = dict(contract)
    for key in ("method", "path", "operation_name"):
        if not enriched.get(key) and operation.get(key):
            enriched[key] = operation.get(key)
    if not enriched.get("provider"):
        enriched["provider"] = resource.get("provider") or operation.get("provider")
    if not enriched.get("base_url") and resource.get("base_url"):
        enriched["base_url"] = resource.get("base_url")
    return enriched


def _operation_field_semantic_type(item: dict[str, Any]) -> str:
    evidence = item.get("evidence") if isinstance(item.get("evidence"), dict) else {}
    return str(evidence.get("semantic_type") or item.get("semantic_type_id") or "")


def _empty_review_proposal(
    state: SourceGraphState,
    graph_node_names: list[str],
    operation_variant_candidates: list[dict[str, Any]],
) -> dict[str, Any]:
    source_document = state["source_document"]
    return _proposal_envelope(
        state,
        f"proposal.{source_document['id']}.empty",
        {"capability_count": 0},
        {},
        graph_node_names,
        operation_variant_candidates,
        kind="source_ingestion",
    )


def _capability_trace(
    state: SourceGraphState,
    capability_id: str,
    operation_contracts: list[dict[str, Any]],
    variants: list[dict[str, Any]],
) -> dict[str, Any]:
    source_document = state["source_document"]
    endpoints = []
    for contract in operation_contracts:
        endpoints.append(
            {
                "operation_id": contract.get("operation_id"),
                "provider": contract.get("provider"),
                "resource_id": contract.get("resource_id"),
                "method": contract.get("method"),
                "path": contract.get("path"),
            }
        )
    variant_ids = [str(item.get("variant_id")) for item in variants if item.get("variant_id")]
    section_ids = sorted(
        {
            str((item.get("evidence") or {}).get("section_id") or item.get("source_chunk_id") or "")
            for item in [*operation_contracts, *variants]
            if isinstance(item, dict)
            and ((item.get("evidence") or {}).get("section_id") or item.get("source_chunk_id"))
        }
    )
    return {
        "capability_id": capability_id,
        "source_document_id": source_document["id"],
        "source_file_name": source_document.get("file_name"),
        "source_path": source_document.get("path"),
        "source_section_ids": section_ids,
        "operation_ids": [str(item.get("operation_id")) for item in operation_contracts if item.get("operation_id")],
        "variant_ids": variant_ids,
        "endpoints": endpoints,
        "evidence_snapshot_id": f"evidence.{source_document['id']}.latest",
    }


def _with_trace(payload: dict[str, Any], trace: dict[str, Any]) -> dict[str, Any]:
    enriched = dict(payload)
    provenance = dict(enriched.get("provenance", {}) if isinstance(enriched.get("provenance"), dict) else {})
    provenance.update(
        {
            "source_document_id": trace.get("source_document_id"),
            "source_file_name": trace.get("source_file_name"),
            "source_path": trace.get("source_path"),
            "source_section_ids": trace.get("source_section_ids", []),
            "operation_ids": trace.get("operation_ids", []),
            "variant_ids": trace.get("variant_ids", []),
            "endpoints": trace.get("endpoints", []),
            "evidence_snapshot_id": trace.get("evidence_snapshot_id"),
        }
    )
    enriched["provenance"] = provenance
    return enriched


def _with_source_provenance(payload: dict[str, Any], state: SourceGraphState) -> dict[str, Any]:
    enriched = dict(payload)
    source_document = state["source_document"]
    enriched["source_document_id"] = source_document["id"]
    provenance = dict(enriched.get("provenance", {}) if isinstance(enriched.get("provenance"), dict) else {})
    provenance["source_document_id"] = source_document["id"]
    provenance["source_file_name"] = source_document.get("file_name")
    provenance["source_path"] = source_document.get("path")
    provenance["evidence_snapshot_id"] = f"evidence.{source_document['id']}.latest"
    enriched["provenance"] = provenance
    return enriched


def _proposal_envelope(
    state: SourceGraphState,
    proposal_id: str,
    summary: dict[str, Any] | None,
    raw: dict[str, Any],
    graph_node_names: list[str],
    operation_variant_candidates: list[dict[str, Any]],
    kind: str = "source_ingestion",
) -> dict[str, Any]:
    return {
        "id": proposal_id,
        "source_document_id": state["source_document"]["id"],
        "kind": kind,
        "status": "pending_review",
        "created_by": _proposal_creator(state),
        "payload": {
            "mode": _proposal_mode(state),
            "graph": graph_node_names,
            "source_document": state["source_document"],
            "summary": summary or {},
            "raw": raw,
            "structured_evidence": state.get("structured_evidence", {}),
            "catalog_context": state.get("catalog_context", {}),
            "operation_variant_candidates": operation_variant_candidates,
            "api_section_count": len(state.get("api_sections", [])),
            "block_count": len(state.get("document_blocks", [])),
        },
    }


def _proposal_mode(state: SourceGraphState) -> str:
    if state.get("manual_llm_response"):
        return "codex_manual"
    return _llm_mode()


def _proposal_creator(state: SourceGraphState) -> str:
    if state.get("manual_llm_response"):
        return "codex_manual_llm"
    mode = _llm_mode()
    return "openai_llm" if mode == "openai" else "llm_disabled"


def _llm_mode() -> str:
    return active_llm_mode()


def _legacy_items(payload: dict[str, Any]) -> bool:
    return isinstance(payload.get("items"), list)


def _target_id(payload: dict[str, Any]) -> str | None:
    return payload.get("id") or payload.get("variant_id") or payload.get("operation_id")


def _list_values(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _semantic_types_from_contract(contract_part: Any) -> list[str]:
    found: list[str] = []

    def walk(value: Any) -> None:
        if isinstance(value, dict):
            semantic_type = value.get("semantic_type")
            if semantic_type and str(semantic_type) not in found:
                found.append(str(semantic_type))
            for child in value.values():
                walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)

    walk(contract_part)
    return found
