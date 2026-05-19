from __future__ import annotations

import argparse
import hashlib
import json
import mimetypes
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TypedDict
from urllib import parse, request
from urllib.error import HTTPError, URLError
from xml.etree import ElementTree

from services.semantic_platform.ingestion.evidence import (
    detect_api_sections,
    extract_blocks,
    extract_structured_evidence,
    sections_to_chunks,
)
from services.semantic_platform.ingestion.extraction import compact_text, extract_text
from services.semantic_platform.storage import SemanticCatalogRepository


INGESTION_GRAPH_VERSION = "2026-05-19.capability-closure-v2"
INGESTION_PROMPT_VERSION = "2026-05-19.contract-interpreter-v2"


class SourceGraphState(TypedDict, total=False):
    source_path: str
    source_bytes: bytes
    source_document: dict[str, Any]
    extracted_text: str
    document_blocks: list[dict[str, Any]]
    api_sections: list[dict[str, Any]]
    structured_evidence: dict[str, Any]
    catalog_context: dict[str, Any]
    endpoint_candidate_checks: list[dict[str, Any]]
    verified_api_sections: list[dict[str, Any]]
    chunks: list[dict[str, Any]]
    manual_llm_response: dict[str, Any] | None
    analysis: dict[str, Any]
    capability_analysis: dict[str, Any]
    execution_analysis: dict[str, Any]
    resources: list[dict[str, Any]]
    operations: list[dict[str, Any]]
    operation_fields: list[dict[str, Any]]
    semantic_types: list[dict[str, Any]]
    capabilities: list[dict[str, Any]]
    operation_contracts: list[dict[str, Any]]
    operation_variants: list[dict[str, Any]]
    field_mappings: list[dict[str, Any]]
    capability_implementations: list[dict[str, Any]]
    verification_results: list[dict[str, Any]]
    proposal: dict[str, Any]
    proposal_items: list[dict[str, Any]]
    proposals: list[dict[str, Any]]
    proposal_item_groups: list[list[dict[str, Any]]]
    stored_proposal: dict[str, Any]
    stored_proposals: list[dict[str, Any]]
    apply_result: dict[str, Any] | None
    apply_results: list[dict[str, Any]]
    capability_document_result: dict[str, Any] | None
    embedding_result: dict[str, Any] | None


def run_source_ingestion(
    source_path: str | Path,
    *,
    manual_llm_response: dict[str, Any] | None = None,
    apply: bool = False,
    force: bool = False,
) -> dict[str, Any]:
    state: SourceGraphState = {
        "source_path": str(source_path),
        "manual_llm_response": manual_llm_response,
    }
    state = read_source(state)
    repo = SemanticCatalogRepository()
    document = state["source_document"]
    ingestion_status = repo.source_ingestion_status(document["id"], document["sha256"])
    if not force and ingestion_status.get("processed"):
        return {
            "source_document_id": document["id"],
            "source_path": document["path"],
            "sha256": document["sha256"],
            "skipped": True,
            "reason": "source_already_ingested",
            "ingestion_status": ingestion_status,
            "proposal_ids": [],
            "proposal_id": None,
            "proposal_count": int(ingestion_status.get("proposal_count") or 0),
            "proposal_item_count": 0,
            "applied": False,
            "apply_result": None,
            "capability_document_result": None,
            "embedding_result": None,
        }
    for node in SOURCE_INGESTION_GRAPH[1:]:
        state = node(state)
    chunks = state["chunks"]
    repo.upsert_source_document(document)
    repo.replace_chunks(document["id"], chunks)
    snapshot_path = write_evidence_snapshot(state)
    repo.upsert_evidence_snapshot(
        {
            "id": f"evidence.{document['id']}.latest",
            "source_document_id": document["id"],
            "snapshot_type": "api_spec_evidence",
            "payload": _evidence_snapshot_payload(state),
            "file_path": snapshot_path,
        }
    )
    proposals = state.get("proposals") or [state["proposal"]]
    item_groups = state.get("proposal_item_groups") or [state["proposal_items"]]
    stored_proposals = []
    apply_results = []
    for proposal, items in zip(proposals, item_groups):
        stored_proposals.append(repo.create_proposal(proposal, items))
        _record_variant_endpoint_checks(repo, state, proposal["id"], items)
        if apply:
            apply_results.append(repo.apply_proposal(proposal["id"]))
    apply_result = {
        "proposal_count": len(apply_results),
        "results": apply_results,
    } if apply_results else None
    applied_capability_ids = _applied_capability_ids(apply_results)
    capability_document_result = (
        repo.rebuild_capability_documents(capability_ids=applied_capability_ids)
        if apply_results
        else None
    )
    embedding_result = (
        repo.embed_capability_documents(
            force=True,
            capability_ids=applied_capability_ids,
            limit=max(len(applied_capability_ids), 1),
        )
        if capability_document_result
        else None
    )
    state["stored_proposal"] = stored_proposals[0] if stored_proposals else {}
    state["stored_proposals"] = stored_proposals
    state["apply_result"] = apply_result
    state["apply_results"] = apply_results
    state["capability_document_result"] = capability_document_result
    state["embedding_result"] = embedding_result
    return {
        "source_document_id": document["id"],
        "proposal_ids": [proposal["id"] for proposal in proposals],
        "proposal_id": proposals[0]["id"] if proposals else None,
        "chunk_count": len(chunks),
        "proposal_count": len(proposals),
        "proposal_item_count": sum(len(items) for items in item_groups),
        "operation_count": len(state.get("operations", [])),
        "operation_field_count": len(state.get("operation_fields", [])),
        "capability_count": len(state.get("capabilities", [])),
        "operation_contract_count": len(state.get("operation_contracts", [])),
        "operation_variant_count": len(state.get("operation_variants", [])),
        "verification_count": len(state.get("verification_results", [])),
        "api_section_count": len(state.get("api_sections", [])),
        "verified_api_section_count": len(state.get("verified_api_sections", [])),
        "field_table_candidate_count": len(state.get("structured_evidence", {}).get("field_table_candidates", [])),
        "example_candidate_count": len(state.get("structured_evidence", {}).get("example_candidates", [])),
        "endpoint_candidate_check_count": len(state.get("endpoint_candidate_checks", [])),
        "evidence_snapshot_path": snapshot_path,
        "applied": bool(apply_result),
        "apply_result": apply_result,
        "capability_document_result": capability_document_result,
        "embedding_result": embedding_result,
    }


def _applied_capability_ids(apply_results: list[dict[str, Any]]) -> list[str]:
    capability_ids = []
    for result in apply_results:
        for item in result.get("applied", []) if isinstance(result, dict) else []:
            if isinstance(item, dict) and item.get("item_type") == "capability" and item.get("target_id"):
                capability_ids.append(str(item["target_id"]))
    return list(dict.fromkeys(capability_ids))


def read_source(state: SourceGraphState) -> SourceGraphState:
    source = Path(state["source_path"])
    data = source.read_bytes()
    sha256 = hashlib.sha256(data).hexdigest()
    source_metadata = _source_metadata(source)
    document_id = _source_document_id(source, sha256, source_metadata)
    metadata = {
        "file_name": source.name,
        "ingestion_graph_version": INGESTION_GRAPH_VERSION,
        "ingestion_prompt_version": INGESTION_PROMPT_VERSION,
        "embedding_model": os.getenv("SEMANTIC_PLATFORM_EMBEDDING_MODEL", "BGE-m3-ko"),
        **source_metadata,
    }
    return {
        **state,
        "source_bytes": data,
        "source_document": {
            "id": document_id,
            "path": str(source),
            "file_name": source.name,
            "sha256": sha256,
            "mime_type": mimetypes.guess_type(source.name)[0],
            "size_bytes": len(data),
            "metadata": metadata,
        },
    }


def extract_text_node(state: SourceGraphState) -> SourceGraphState:
    limit = int(os.getenv("SEMANTIC_PLATFORM_SOURCE_LLM_MAX_TEXT_CHARS", "40000"))
    return {**state, "extracted_text": compact_text(extract_text(state["source_path"]), limit)}


def extract_blocks_node(state: SourceGraphState) -> SourceGraphState:
    return {**state, "document_blocks": extract_blocks(state["extracted_text"])}


def detect_api_sections_node(state: SourceGraphState) -> SourceGraphState:
    sections = detect_api_sections(state["document_blocks"], state["source_document"]["id"])
    return {
        **state,
        "api_sections": sections,
        "chunks": sections_to_chunks(sections, state["source_document"]["id"]),
    }


def extract_structured_evidence_node(state: SourceGraphState) -> SourceGraphState:
    return {
        **state,
        "structured_evidence": extract_structured_evidence(state["document_blocks"], state["api_sections"]),
    }


def load_catalog_context(state: SourceGraphState) -> SourceGraphState:
    repo = SemanticCatalogRepository()
    catalog = repo.catalog()
    proposals = repo.proposals().get("proposals", [])[:20]
    return {
        **state,
        "catalog_context": {
            "semantic_types": _catalog_values(catalog.get("semantic_types", {}), limit=120),
            "capabilities": _catalog_values(catalog.get("capabilities", {}), limit=120),
            "operation_contracts_summary": _contract_summary(catalog.get("operation_contracts", {})),
            "operation_variants_summary": _variant_summary(catalog.get("operation_variants", {})),
            "recent_proposals": [
                {
                    "id": proposal.get("id"),
                    "kind": proposal.get("kind"),
                    "status": proposal.get("status"),
                    "source_document_id": proposal.get("source_document_id"),
                    "created_at": proposal.get("created_at"),
                }
                for proposal in proposals
            ],
            "naming_policy": {
                "capability_ids": "provider-neutral action names such as search_contracts, not provider-prefixed ids",
                "operation_ids": "provider/resource operation identifiers may reflect physical endpoints",
                "variants": "provider control values and endpoint-specific meanings belong in operation_variant rows",
            },
        },
    }


def verify_endpoint_candidates(state: SourceGraphState) -> SourceGraphState:
    base_url = _candidate_base_url(state)
    checks = []
    for section in state.get("api_sections", []):
        if not section.get("path"):
            checks.append(_endpoint_candidate_not_run(section, "path_not_found"))
            continue
        if not base_url:
            checks.append(_endpoint_candidate_not_run(section, "base_url_not_found"))
            continue
        checks.append(_probe_endpoint_candidate(section, base_url, state))
    passed = {str(check.get("section_id") or "") for check in checks if check.get("status") == "passed"}
    return {
        **state,
        "endpoint_candidate_checks": checks,
        "verified_api_sections": [
            section for section in state.get("api_sections", []) if str(section.get("id") or "") in passed
        ],
    }


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
            "capabilities": _list(analysis.get("capabilities")),
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
                "capabilities": _list(analysis.get("capabilities")),
                "operation_contracts": operation_contracts,
                "operation_variants": _list(analysis.get("operation_variants")),
                "field_mappings": field_mappings,
                "capability_implementations": _list(analysis.get("capability_implementations")),
            },
        ),
    }


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


def verify_capabilities(state: SourceGraphState) -> SourceGraphState:
    manual = state.get("manual_llm_response") or {}
    provided_results = manual.get("verification_results")
    if isinstance(provided_results, list):
        return {**state, "verification_results": [item for item in provided_results if isinstance(item, dict)]}
    resources = {str(item.get("id") or ""): item for item in state.get("resources", [])}
    contracts = {str(item.get("operation_id") or ""): item for item in state.get("operation_contracts", [])}
    results = []
    for variant in state.get("operation_variants", []):
        verification = variant.get("verification") if isinstance(variant.get("verification"), dict) else {}
        if not verification.get("safe_to_call"):
            results.append(_verification_not_run(variant, "verification_not_marked_safe_to_call"))
            continue
        sample_arguments = verification.get("sample_semantic_arguments")
        if not isinstance(sample_arguments, dict):
            results.append(_verification_not_run(variant, "verification_sample_not_supplied"))
            continue
        contract = contracts.get(str(variant.get("operation_id") or ""))
        if not contract:
            results.append(_verification_not_run(variant, "operation_contract_not_found"))
            continue
        resource = resources.get(str(contract.get("resource_id") or ""))
        results.append(_verify_http_variant(variant, contract, resource, sample_arguments))
    return {**state, "verification_results": results}


def keep_passed_verified_capabilities(state: SourceGraphState) -> SourceGraphState:
    variants = state.get("operation_variants", [])
    if not variants:
        return state
    passed_variant_ids = {
        str(result.get("variant_id") or "")
        for result in state.get("verification_results", [])
        if result.get("status") == "passed" and result.get("variant_id")
    }
    if not passed_variant_ids:
        return {
            **state,
            "capabilities": [],
            "operation_fields": [],
            "operation_contracts": [],
            "operation_variants": [],
            "field_mappings": [],
            "capability_implementations": [],
        }

    kept_variants = [variant for variant in variants if str(variant.get("variant_id") or "") in passed_variant_ids]
    kept_operation_ids = {str(variant.get("operation_id") or "") for variant in kept_variants}
    kept_capability_ids = {str(variant.get("capability_id") or variant.get("capability") or "") for variant in kept_variants}
    return {
        **state,
        "operations": [
            operation
            for operation in state.get("operations", [])
            if str(operation.get("operation_id") or "") in kept_operation_ids
        ],
        "capabilities": [
            capability
            for capability in state.get("capabilities", [])
            if str(capability.get("id") or "") in kept_capability_ids
        ],
        "operation_fields": [
            field
            for field in state.get("operation_fields", [])
            if str(field.get("operation_id") or "") in kept_operation_ids
        ],
        "operation_contracts": [
            contract
            for contract in state.get("operation_contracts", [])
            if str(contract.get("operation_id") or "") in kept_operation_ids
        ],
        "operation_variants": kept_variants,
        "field_mappings": [
            mapping
            for mapping in state.get("field_mappings", [])
            if str(mapping.get("operation_id") or "") in kept_operation_ids
        ],
        "capability_implementations": [
            implementation
            for implementation in state.get("capability_implementations", [])
            if str(implementation.get("variant_id") or "") in passed_variant_ids
            and str(implementation.get("capability_id") or "") in kept_capability_ids
        ],
    }


def build_review_proposal(state: SourceGraphState) -> SourceGraphState:
    manual = state.get("manual_llm_response") or {}
    if _legacy_items(manual):
        proposal, items = _legacy_review_proposal(state, manual)
        proposals = [proposal]
        item_groups = [items]
    else:
        proposals, item_groups = _capability_review_proposals(state)
        proposal = proposals[0] if proposals else _empty_review_proposal(state)
        items = [item for group in item_groups for item in group]
    return {
        **state,
        "proposal": proposal,
        "proposal_items": items,
        "proposals": proposals,
        "proposal_item_groups": item_groups,
    }


def write_evidence_snapshot(state: SourceGraphState) -> str:
    output_dir = Path(os.getenv("SEMANTIC_PLATFORM_EVIDENCE_DIR", "/tmp/semantic_platform/evidence"))
    output_dir.mkdir(parents=True, exist_ok=True)
    document_id = state["source_document"]["id"]
    path = output_dir / f"{document_id}.api_spec_evidence.json"
    path.write_text(json.dumps(_evidence_snapshot_payload(state), ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    return str(path)


def _evidence_snapshot_payload(state: SourceGraphState) -> dict[str, Any]:
    return {
        "source_document": state.get("source_document", {}),
        "graph": [node.__name__ for node in SOURCE_INGESTION_GRAPH],
        "api_sections": [
            {
                "section_id": section.get("id"),
                "section_index": section.get("section_index"),
                "operation_name": section.get("operation_name"),
                "method": section.get("method"),
                "path": section.get("path"),
                "title": section.get("title"),
                "score": section.get("score"),
                "evidence": section.get("evidence", {}),
            }
            for section in state.get("api_sections", [])
        ],
        "verified_api_sections": [
            {
                "section_id": section.get("id"),
                "section_index": section.get("section_index"),
                "operation_name": section.get("operation_name"),
                "method": section.get("method"),
                "path": section.get("path"),
                "title": section.get("title"),
                "score": section.get("score"),
                "evidence": section.get("evidence", {}),
            }
            for section in state.get("verified_api_sections", [])
        ],
        "structured_evidence": state.get("structured_evidence", {}),
        "operation_variant_candidates": _operation_variant_candidates(state),
        "analysis_summary": {
            "resources": [_target_id(item) for item in state.get("resources", [])],
            "operations": [_target_id(item) for item in state.get("operations", [])],
            "operation_fields": [_target_id(item) for item in state.get("operation_fields", [])],
            "capabilities": [_target_id(item) for item in state.get("capabilities", [])],
            "operation_contracts": [_target_id(item) for item in state.get("operation_contracts", [])],
            "operation_variants": [_target_id(item) for item in state.get("operation_variants", [])],
        },
        "verification_results": state.get("verification_results", []),
        "endpoint_candidate_checks": state.get("endpoint_candidate_checks", []),
    }


def _legacy_review_proposal(
    state: SourceGraphState,
    payload: dict[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    source_document = state["source_document"]
    proposal_id = payload.get("proposal_id") or f"proposal.{source_document['id']}.manual"
    proposal = _proposal_envelope(state, proposal_id, payload.get("summary"), payload)
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


def _validate_llm_analysis(analysis: dict[str, Any]) -> None:
    if not analysis:
        return
    required_lists = (
        "resources",
        "operations",
        "semantic_types",
        "capabilities",
        "operation_contracts",
        "operation_variants",
        "field_mappings",
        "capability_implementations",
    )
    for key in required_lists:
        if key in analysis and not isinstance(analysis.get(key), list):
            raise ValueError(f"llm analysis field must be a list: {key}")
    resources = {str(item.get("id") or "") for item in _list(analysis.get("resources"))}
    operations = {str(item.get("operation_id") or "") for item in _list(analysis.get("operations"))}
    semantic_types = {str(item.get("id") or "") for item in _list(analysis.get("semantic_types"))}
    capabilities = {str(item.get("id") or "") for item in _list(analysis.get("capabilities"))}
    resources.discard("")
    operations.discard("")
    semantic_types.discard("")
    capabilities.discard("")
    for item in _list(analysis.get("operations")):
        _require(item, "operation_id", "operation")
        resource_id = str(item.get("resource_id") or "")
        if resource_id and resource_id not in resources:
            raise ValueError(f"operation references unknown resource: {item.get('operation_id')} -> {resource_id}")
    for item in _list(analysis.get("operation_contracts")):
        operation_id = _require(item, "operation_id", "operation_contract")
        if operation_id not in operations:
            raise ValueError(f"contract references unknown operation: {operation_id}")
        capability_id = str(item.get("capability_id") or item.get("capability") or "")
        if capability_id and capability_id not in capabilities:
            raise ValueError(f"contract references unknown capability: {operation_id} -> {capability_id}")
        _validate_operation_contract_runtime_schema(item)
    for item in _list(analysis.get("operation_variants")):
        operation_id = _require(item, "operation_id", "operation_variant")
        capability_id = _require(item, "capability_id", "operation_variant")
        _require(item, "variant_id", "operation_variant")
        if operation_id not in operations:
            raise ValueError(f"variant references unknown operation: {operation_id}")
        if capability_id not in capabilities:
            raise ValueError(f"variant references unknown capability: {capability_id}")
    for item in _list(analysis.get("capability_implementations")):
        operation_id = _require(item, "operation_id", "capability_implementation")
        capability_id = _require(item, "capability_id", "capability_implementation")
        if operation_id not in operations:
            raise ValueError(f"implementation references unknown operation: {operation_id}")
        if capability_id not in capabilities:
            raise ValueError(f"implementation references unknown capability: {capability_id}")
    for item in _list(analysis.get("field_mappings")):
        operation_id = _require(item, "operation_id", "field_mapping")
        semantic_type_id = _require(item, "semantic_type_id", "field_mapping")
        if operation_id not in operations:
            raise ValueError(f"field mapping references unknown operation: {operation_id}")
        if semantic_type_id not in semantic_types:
            raise ValueError(f"field mapping references unknown semantic type: {semantic_type_id}")
    for item in _list(analysis.get("capabilities")):
        capability_id = _require(item, "id", "capability")
        for semantic_type_id in [*_list_values(item.get("inputs")), *_list_values(item.get("outputs"))]:
            if str(semantic_type_id) not in semantic_types:
                raise ValueError(f"capability references unknown semantic type: {capability_id} -> {semantic_type_id}")


def _require(item: dict[str, Any], key: str, context: str) -> str:
    value = str(item.get(key) or "")
    if not value:
        raise ValueError(f"{context} missing required field: {key}")
    return value


def _validate_operation_contract_runtime_schema(contract: dict[str, Any]) -> None:
    operation_id = str(contract.get("operation_id") or "")
    response = contract.get("response") if isinstance(contract.get("response"), dict) else {}
    items_path = response.get("items_path")
    if not _contract_path_list(items_path):
        raise ValueError(f"operation_contract response.items_path required: {operation_id}")
    fields = response.get("fields")
    if not isinstance(fields, dict):
        raise ValueError(f"operation_contract response.fields required: {operation_id}")
    for field_name, field_contract in fields.items():
        if not isinstance(field_contract, dict) or not field_contract.get("semantic_type"):
            raise ValueError(f"operation_contract response field missing semantic_type: {operation_id}.{field_name}")
    auth = contract.get("auth") if isinstance(contract.get("auth"), dict) else {}
    if auth:
        env_names = auth.get("env_names")
        if env_names is not None and not isinstance(env_names, list):
            raise ValueError(f"operation_contract auth.env_names must be a list: {operation_id}")
        if not str(auth.get("parameter") or ""):
            raise ValueError(f"operation_contract auth.parameter required when auth is declared: {operation_id}")
    for key in ("success", "error"):
        condition = response.get(key)
        if condition is not None and not isinstance(condition, dict):
            raise ValueError(f"operation_contract response.{key} must be an object: {operation_id}")
    success = response.get("success") if isinstance(response.get("success"), dict) else {}
    if success and not str(success.get("path") or ""):
        raise ValueError(f"operation_contract response.success.path required: {operation_id}")
    error = response.get("error") if isinstance(response.get("error"), dict) else {}
    if error and not str(error.get("code_path") or ""):
        raise ValueError(f"operation_contract response.error.code_path required: {operation_id}")


def _contract_path_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if str(item or "").strip()]
    if str(value or "").strip():
        return [str(value)]
    return []


def _capability_review_proposals(state: SourceGraphState) -> tuple[list[dict[str, Any]], list[list[dict[str, Any]]]]:
    proposals: list[dict[str, Any]] = []
    item_groups: list[list[dict[str, Any]]] = []
    for capability in state.get("capabilities", []):
        if not isinstance(capability, dict):
            continue
        capability_id = str(capability.get("id") or "")
        if not capability_id:
            continue
        proposal, items = _capability_review_proposal(state, capability_id, capability)
        proposals.append(proposal)
        item_groups.append(items)
    return proposals, item_groups


def _capability_review_proposal(
    state: SourceGraphState,
    capability_id: str,
    capability: dict[str, Any],
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

    proposal_id = f"proposal.{source_document['id']}.{capability_id}.review"
    trace = _capability_trace(state, capability_id, operation_contracts, variants)
    capability_payload = _with_trace(capability, trace)
    summary = {
        "capability_id": capability_id,
        "operation_count": len(operations),
        "operation_field_count": len(operation_fields),
        "resource_count": len(resources),
        "semantic_type_count": len(semantic_types),
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
        "operation_contracts": operation_contracts,
        "operation_variants": variants,
        "field_mappings": field_mappings,
        "capability_implementations": implementations,
        "trace": trace,
    }
    proposal = _proposal_envelope(state, proposal_id, summary, raw, kind="capability_ingestion")
    verification_by_variant = {
        str(item.get("variant_id") or ""): item
        for item in state.get("verification_results", [])
        if isinstance(item, dict)
    }
    items: list[dict[str, Any]] = []
    grouped_items: list[tuple[str, list[dict[str, Any]]]] = [
        ("resource", resources),
        ("semantic_type", semantic_types),
        ("capability", [capability_payload]),
        ("operation", operations),
        ("operation_field", operation_fields),
        ("operation_contract", operation_contracts),
        ("operation_variant", variants),
        ("field_mapping", field_mappings),
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
    semantic_types = [item for item in state.get("semantic_types", []) if str(item.get("id") or "") in semantic_type_ids]
    return {
        "resources": resources,
        "operations": operations,
        "operation_fields": operation_fields,
        "semantic_types": semantic_types,
        "operation_contracts": operation_contracts,
        "operation_variants": variants,
        "field_mappings": field_mappings,
        "capability_implementations": implementations,
    }


def _operation_field_semantic_type(item: dict[str, Any]) -> str:
    evidence = item.get("evidence") if isinstance(item.get("evidence"), dict) else {}
    return str(evidence.get("semantic_type") or item.get("semantic_type_id") or "")


def _empty_review_proposal(state: SourceGraphState) -> dict[str, Any]:
    source_document = state["source_document"]
    return _proposal_envelope(
        state,
        f"proposal.{source_document['id']}.empty",
        {"capability_count": 0},
        {},
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


def _list_values(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _proposal_envelope(
    state: SourceGraphState,
    proposal_id: str,
    summary: dict[str, Any] | None,
    raw: dict[str, Any],
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
            "graph": [node.__name__ for node in SOURCE_INGESTION_GRAPH],
            "source_document": state["source_document"],
            "summary": summary or {},
            "raw": raw,
            "structured_evidence": state.get("structured_evidence", {}),
            "catalog_context": state.get("catalog_context", {}),
            "operation_variant_candidates": _operation_variant_candidates(state),
            "api_section_count": len(state.get("api_sections", [])),
            "block_count": len(state.get("document_blocks", [])),
        },
    }


SOURCE_INGESTION_GRAPH = [
    read_source,
    extract_text_node,
    extract_blocks_node,
    detect_api_sections_node,
    extract_structured_evidence_node,
    load_catalog_context,
    verify_endpoint_candidates,
    llm_propose_capability_catalog,
    llm_propose_execution_catalog,
    verify_capabilities,
    keep_passed_verified_capabilities,
    build_review_proposal,
]


def _proposal_mode(state: SourceGraphState) -> str:
    if state.get("manual_llm_response"):
        return "codex_manual"
    return _llm_mode()


def _proposal_creator(state: SourceGraphState) -> str:
    if state.get("manual_llm_response"):
        return "codex_manual_llm"
    mode = _llm_mode()
    return "openai_llm" if mode == "openai" else "llm_disabled"


def _verification_not_run(variant: dict[str, Any], reason: str) -> dict[str, Any]:
    return {
        "variant_id": variant.get("variant_id"),
        "operation_id": variant.get("operation_id"),
        "capability_id": variant.get("capability_id") or variant.get("capability"),
        "status": "not_run",
        "reason": reason,
    }


def _candidate_base_url(state: SourceGraphState) -> str | None:
    manual = state.get("manual_llm_response") or {}
    for resource in _list(manual.get("resources")):
        base_url = resource.get("base_url")
        if base_url:
            return str(base_url)
    urls = []
    for example in state.get("structured_evidence", {}).get("example_candidates", []):
        text = str(example.get("text") or "")
        for match in re.finditer(r"https?://[^\s|<>\"]+", text):
            urls.append(match.group(0))
    if not urls:
        return None
    return _base_url_from_examples(urls)


def _base_url_from_examples(urls: list[str]) -> str | None:
    prefixes: list[str] = []
    for url in urls:
        parsed = parse.urlparse(url)
        parts = [part for part in parsed.path.split("/") if part]
        if len(parts) >= 3:
            prefixes.append(f"{parsed.scheme}://{parsed.netloc}/" + "/".join(parts[:3]))
        elif parsed.scheme and parsed.netloc:
            prefixes.append(f"{parsed.scheme}://{parsed.netloc}")
    if not prefixes:
        return None
    return max(set(prefixes), key=prefixes.count)


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
        "capabilities": capabilities,
        "operation_contracts": operation_contracts,
        "operation_variants": operation_variants,
        "field_mappings": field_mappings,
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


def _endpoint_candidate_not_run(section: dict[str, Any], reason: str) -> dict[str, Any]:
    return {
        "section_id": section.get("id"),
        "operation_name": section.get("operation_name"),
        "method": section.get("method"),
        "path": section.get("path"),
        "status": "not_run",
        "reason": reason,
    }


def _probe_endpoint_candidate(section: dict[str, Any], base_url: str, state: SourceGraphState) -> dict[str, Any]:
    method = str(section.get("method") or "GET").upper()
    if method not in {"GET", "POST"}:
        return _endpoint_candidate_not_run(section, "unsupported_probe_method")
    raw_arguments = _candidate_probe_arguments(state)
    raw_body = _candidate_probe_body(section) if method == "POST" else {}
    url = _join_url(base_url, str(section.get("path") or ""))
    full_url = f"{url}?{parse.urlencode(raw_arguments, doseq=True)}"
    started = datetime.now(timezone.utc)
    try:
        http_request: str | request.Request
        if method == "POST":
            http_request = request.Request(
                full_url,
                data=json.dumps(raw_body, ensure_ascii=False).encode("utf-8"),
                headers={"Content-Type": "application/json", "Accept": "application/json"},
                method="POST",
            )
        else:
            http_request = full_url
        with request.urlopen(http_request, timeout=_candidate_probe_timeout_seconds()) as response:
            body = response.read()
            content_type = response.headers.get("content-type", "")
        provider_status, provider_message, status = _candidate_probe_status(body, content_type)
        return {
            "section_id": section.get("id"),
            "operation_name": section.get("operation_name"),
            "method": method,
            "path": section.get("path"),
            "status": status,
            "provider_status": provider_status,
            "message": provider_message,
            "request": {"url": url, "method": method, "arguments": _redact(raw_arguments), "body": _redact(raw_body)},
            "response_sample": body[:2000].decode("utf-8", errors="ignore"),
            "checked_at": started.isoformat(),
        }
    except HTTPError as exc:
        if exc.code in {401, 403}:
            return _endpoint_candidate_inconclusive(
                section,
                "auth_required",
                str(exc),
                url,
                raw_arguments,
                started,
                raw_body,
            )
        return _endpoint_candidate_failed(section, "http_error", str(exc), url, raw_arguments, started, raw_body)
    except TimeoutError as exc:
        return _endpoint_candidate_inconclusive(section, "timeout", str(exc), url, raw_arguments, started, raw_body)
    except URLError as exc:
        if "timed out" in str(exc).lower():
            return _endpoint_candidate_inconclusive(section, "timeout", str(exc), url, raw_arguments, started, raw_body)
        return _endpoint_candidate_failed(section, "transport_error", str(exc), url, raw_arguments, started, raw_body)
    except ValueError as exc:
        return _endpoint_candidate_failed(section, "transport_error", str(exc), url, raw_arguments, started, raw_body)


def _candidate_probe_status(body: bytes, content_type: str) -> tuple[str, str, str]:
    text = body.decode("utf-8", errors="ignore")
    stripped = text.lstrip()
    if not stripped:
        return "empty", "empty response body", "failed"
    if "json" in content_type.lower() or stripped.startswith("{") or stripped.startswith("["):
        try:
            json.loads(text)
        except json.JSONDecodeError as exc:
            return "invalid_json", f"unparseable response body: {exc}", "failed"
        return "http_success", "probe returned parseable JSON", "passed"
    if stripped.startswith("<"):
        try:
            ElementTree.fromstring(body)
        except ElementTree.ParseError as exc:
            return "invalid_xml", f"unparseable response body: {exc}", "failed"
        return "http_success", "probe returned parseable XML", "passed"
    return "http_success", "probe returned a non-empty response body", "passed"


def _candidate_probe_arguments(state: SourceGraphState) -> dict[str, Any]:
    try:
        configured = json.loads(os.getenv("SEMANTIC_PLATFORM_CANDIDATE_PROBE_ARGUMENTS", "{}"))
    except json.JSONDecodeError:
        configured = {}
    arguments = configured if isinstance(configured, dict) else {}
    key = _candidate_service_key(state)
    if key:
        parameter = _candidate_service_key_parameter(state)
        arguments[parameter] = key
    return arguments


def _candidate_service_key(state: SourceGraphState) -> str | None:
    for name in _candidate_source_env_names(state, "SEMANTIC_PLATFORM_CANDIDATE_SERVICE_KEY"):
        value = os.getenv(name)
        if value:
            return value
    return os.getenv("SEMANTIC_PLATFORM_CANDIDATE_SERVICE_KEY")


def _candidate_service_key_parameter(state: SourceGraphState) -> str:
    for name in _candidate_source_env_names(state, "SEMANTIC_PLATFORM_CANDIDATE_SERVICE_KEY_PARAMETER"):
        value = os.getenv(name)
        if value:
            return value
    return os.getenv("SEMANTIC_PLATFORM_CANDIDATE_SERVICE_KEY_PARAMETER", "ServiceKey")


def _candidate_source_env_names(state: SourceGraphState, prefix: str) -> list[str]:
    document = state.get("source_document", {})
    sha = str(document.get("sha256") or "")
    source_id = str(document.get("id") or "")
    names = []
    if sha:
        names.append(f"{prefix}_{sha[:8].upper()}")
    match = re.match(r"source\.([0-9a-fA-F]{8})\.", source_id)
    if match:
        names.append(f"{prefix}_{match.group(1).upper()}")
    return list(dict.fromkeys(names))


def _candidate_probe_body(section: dict[str, Any]) -> dict[str, Any]:
    bodies = _json_env("SEMANTIC_PLATFORM_CANDIDATE_PROBE_BODIES", {})
    if isinstance(bodies, dict):
        operation_name = str(section.get("operation_name") or "")
        path = str(section.get("path") or "")
        for key in (operation_name, path):
            value = bodies.get(key)
            if isinstance(value, dict):
                return value
    body = _json_env("SEMANTIC_PLATFORM_CANDIDATE_PROBE_JSON_BODY", {})
    return body if isinstance(body, dict) else {}


def _json_env(name: str, default: Any) -> Any:
    try:
        return json.loads(os.getenv(name, ""))
    except (TypeError, json.JSONDecodeError):
        return default


def _candidate_probe_timeout_seconds() -> float:
    try:
        return float(os.getenv("SEMANTIC_PLATFORM_CANDIDATE_PROBE_TIMEOUT_SECONDS", "5"))
    except ValueError:
        return 5.0


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
                    "Return JSON only with keys: resources, operations, semantic_types, capabilities, "
                    "operation_fields, operation_contracts, operation_variants, field_mappings, "
                    "capability_implementations. "
                    "Keep capability ids provider-neutral and describe what a planner can do. "
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
        "operation_variant_candidates": _operation_variant_candidates(state),
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


def _operation_variant_candidates(state: SourceGraphState) -> list[dict[str, Any]]:
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


def _evidence_sections_for_llm(state: SourceGraphState) -> list[dict[str, Any]]:
    verified_sections = [section for section in state.get("verified_api_sections", []) if isinstance(section, dict)]
    if verified_sections:
        return verified_sections
    if state.get("manual_llm_response") or _llm_mode() == "codex_manual":
        return [section for section in state.get("api_sections", []) if isinstance(section, dict)]
    return verified_sections


def _evidence_section_ids_for_llm(state: SourceGraphState) -> set[str]:
    return {str(section.get("id") or "") for section in _evidence_sections_for_llm(state)}


def _catalog_values(values: Any, limit: int) -> list[dict[str, Any]]:
    if isinstance(values, dict):
        items = values.values()
    elif isinstance(values, list):
        items = values
    else:
        items = []
    return [dict(item) for item in items if isinstance(item, dict)][:limit]


def _contract_summary(values: Any) -> list[dict[str, Any]]:
    return [
        {
            "operation_id": item.get("operation_id"),
            "capability_id": item.get("capability_id"),
            "provider": item.get("provider"),
            "resource_id": item.get("resource_id"),
            "method": item.get("method"),
            "path": item.get("path"),
            "request_semantic_types": _semantic_types_from_contract(item.get("request")),
            "response_semantic_types": _semantic_types_from_contract(item.get("response")),
        }
        for item in _catalog_values(values, limit=200)
    ]


def _variant_summary(values: Any) -> list[dict[str, Any]]:
    return [
        {
            "variant_id": item.get("variant_id"),
            "operation_id": item.get("operation_id"),
            "capability_id": item.get("capability_id"),
            "fixed_semantic_arguments": item.get("fixed_semantic_arguments") or {},
            "fixed_raw_arguments": item.get("fixed_raw_arguments") or {},
            "verification_status": (item.get("verification") or {}).get("status")
            if isinstance(item.get("verification"), dict)
            else None,
        }
        for item in _catalog_values(values, limit=240)
    ]


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


def _record_variant_endpoint_checks(
    repo: SemanticCatalogRepository,
    state: SourceGraphState,
    proposal_id: str,
    proposal_items: list[dict[str, Any]] | None = None,
) -> None:
    item_by_variant = {
        str(item.get("payload", {}).get("variant_id") or ""): item
        for item in (proposal_items or state.get("proposal_items", []))
        if item.get("item_type") == "operation_variant" and isinstance(item.get("payload"), dict)
    }
    if not item_by_variant:
        return
    for result in state.get("verification_results", []):
        if not isinstance(result, dict) or not result.get("operation_id"):
            continue
        variant_id = str(result.get("variant_id") or "")
        if variant_id not in item_by_variant:
            continue
        proposal_item = item_by_variant.get(variant_id, {})
        check_id = f"endpoint_check.{proposal_id}.{variant_id or result.get('operation_id')}"
        try:
            repo.record_endpoint_check(
                {
                    "id": check_id,
                    "operation_id": result["operation_id"],
                    "variant_id": result.get("variant_id"),
                    "capability_id": result.get("capability_id"),
                    "proposal_id": proposal_id,
                    "proposal_item_id": proposal_item.get("id"),
                    "check_type": "variant_verification",
                    "status": result.get("status", "unknown"),
                    "request_payload": result.get("request", {}),
                    "response_sample": _json_sample(result.get("response_sample")),
                    "normalized_sample": {},
                    "error_message": result.get("message") or result.get("reason"),
                    "executor": "semantic_platform_ingestion",
                    "duration_ms": result.get("duration_ms"),
                }
            )
        except Exception:
            continue


def _json_sample(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, list):
        return {"items": value[:20]}
    if value is None:
        return {}
    return {"text": str(value)[:4000]}


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


def _endpoint_candidate_inconclusive(
    section: dict[str, Any],
    reason: str,
    message: str,
    url: str,
    raw_arguments: dict[str, Any],
    checked_at: datetime,
    raw_body: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "section_id": section.get("id"),
        "operation_name": section.get("operation_name"),
        "method": section.get("method"),
        "path": section.get("path"),
        "status": "inconclusive",
        "reason": reason,
        "message": message,
        "request": {
            "url": url,
            "method": section.get("method"),
            "arguments": _redact(raw_arguments),
            "body": _redact(raw_body or {}),
        },
        "checked_at": checked_at.isoformat(),
    }


def _endpoint_candidate_failed(
    section: dict[str, Any],
    reason: str,
    message: str,
    url: str,
    raw_arguments: dict[str, Any],
    checked_at: datetime,
    raw_body: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "section_id": section.get("id"),
        "operation_name": section.get("operation_name"),
        "method": section.get("method"),
        "path": section.get("path"),
        "status": "failed",
        "reason": reason,
        "message": message,
        "request": {
            "url": url,
            "method": section.get("method"),
            "arguments": _redact(raw_arguments),
            "body": _redact(raw_body or {}),
        },
        "checked_at": checked_at.isoformat(),
    }


def _verify_http_variant(
    variant: dict[str, Any],
    contract: dict[str, Any],
    resource: dict[str, Any] | None,
    sample_semantic_arguments: dict[str, Any],
) -> dict[str, Any]:
    started = datetime.now(timezone.utc)
    if not isinstance(resource, dict) or not resource.get("base_url"):
        return {**_verification_not_run(variant, "resource_base_url_not_found"), "checked_at": started.isoformat()}
    method = str(contract.get("method") or "GET").upper()
    if method not in {"GET", "POST"}:
        return {**_verification_not_run(variant, "unsupported_verification_method"), "checked_at": started.isoformat()}
    request_parts = _raw_arguments_by_contract_section(sample_semantic_arguments, contract)
    raw_arguments = {**(variant.get("fixed_raw_arguments") or {}), **request_parts["query"]}
    raw_arguments = _with_auth(raw_arguments, contract)
    raw_body = request_parts["body"]
    raw_headers = {str(key): str(value) for key, value in request_parts["header"].items()}
    url = _join_url(str(resource.get("base_url") or ""), str(contract.get("path") or ""))
    full_url = f"{url}?{parse.urlencode(raw_arguments, doseq=True)}"
    try:
        http_request: str | request.Request
        if method == "POST":
            headers = {"Content-Type": "application/json", "Accept": "application/json", **raw_headers}
            http_request = request.Request(
                full_url,
                data=json.dumps(raw_body, ensure_ascii=False).encode("utf-8"),
                headers=headers,
                method="POST",
            )
        else:
            http_request = full_url
        with request.urlopen(http_request, timeout=15) as response:
            body = response.read()
            content_type = response.headers.get("content-type", "")
        status, provider_message, result_status = _provider_status(body, content_type, contract)
        return {
            "variant_id": variant.get("variant_id"),
            "operation_id": variant.get("operation_id"),
            "capability_id": variant.get("capability_id") or variant.get("capability"),
            "status": result_status,
            "provider_status": status,
            "message": provider_message,
            "request": {
                "url": url,
                "method": method,
                "arguments": _redact(raw_arguments),
                "body": _redact(raw_body),
                "headers": _redact(raw_headers),
            },
            "response_sample": body[:4000].decode("utf-8", errors="ignore"),
            "checked_at": started.isoformat(),
        }
    except HTTPError as exc:
        return _verification_failed(variant, "http_error", str(exc), raw_arguments, url, started, method, raw_body, raw_headers)
    except (TimeoutError, URLError, ValueError) as exc:
        return _verification_failed(variant, "transport_error", str(exc), raw_arguments, url, started, method, raw_body, raw_headers)


def _raw_arguments_from_contract(semantic_arguments: dict[str, Any], contract: dict[str, Any]) -> dict[str, Any]:
    parts = _raw_arguments_by_contract_section(semantic_arguments, contract)
    merged: dict[str, Any] = {}
    for values in parts.values():
        merged.update(values)
    return merged


def _raw_arguments_by_contract_section(
    semantic_arguments: dict[str, Any],
    contract: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    request_contract = contract.get("request") if isinstance(contract.get("request"), dict) else {}
    raw_arguments: dict[str, dict[str, Any]] = {"query": {}, "body": {}, "path": {}, "header": {}}
    for section in ("query", "body", "path", "header"):
        fields = request_contract.get(section)
        if not isinstance(fields, dict):
            continue
        for raw_name, field in fields.items():
            if not isinstance(field, dict):
                continue
            semantic_type = str(field.get("semantic_type") or "")
            if semantic_type in semantic_arguments:
                raw_arguments[section][str(raw_name)] = _contract_value(
                    semantic_arguments[semantic_type],
                    str(field.get("transform") or ""),
                )
            elif "default" in field:
                raw_arguments[section][str(raw_name)] = field.get("default")
    return {
        section: {key: value for key, value in values.items() if value not in (None, "")}
        for section, values in raw_arguments.items()
    }


def _contract_value(value: Any, transform: str) -> Any:
    if isinstance(value, dict):
        if transform == "date_start":
            return value.get("from") or value.get("start")
        if transform == "date_end":
            return value.get("to") or value.get("end")
    return value


def _with_auth(raw_arguments: dict[str, Any], contract: dict[str, Any]) -> dict[str, Any]:
    arguments = dict(raw_arguments)
    auth = contract.get("auth") if isinstance(contract.get("auth"), dict) else {}
    parameter = str(auth.get("parameter") or "ServiceKey")
    if parameter not in arguments:
        key = _api_key(auth)
        if key:
            arguments[parameter] = key
    return arguments


def _api_key(auth: dict[str, Any]) -> str | None:
    names = auth.get("env_names") if isinstance(auth.get("env_names"), list) else []
    for name in [str(value) for value in names]:
        value = os.getenv(name)
        if value:
            return value
    return None


def _join_url(base_url: str, path: str) -> str:
    return base_url.rstrip("/") + "/" + path.lstrip("/")


def _provider_status(body: bytes, content_type: str, contract: dict[str, Any]) -> tuple[str, str, str]:
    text = body.decode("utf-8", errors="ignore")
    stripped = text.lstrip()
    if not stripped:
        return "unknown", "empty response body", "failed"
    if "json" in content_type.lower() or stripped.startswith("{") or stripped.startswith("["):
        payload = json.loads(text)
        return _provider_status_from_contract(payload, contract)
    try:
        root = ElementTree.fromstring(body)
    except ElementTree.ParseError as exc:
        return "unknown", f"unparseable response body: {exc}", "failed"
    return "OK", root.findtext(".//resultMsg") or root.findtext(".//returnAuthMsg") or "", "passed"


def _provider_status_from_contract(payload: Any, contract: dict[str, Any]) -> tuple[str, str, str]:
    response = contract.get("response") if isinstance(contract.get("response"), dict) else {}
    error = response.get("error") if isinstance(response.get("error"), dict) else {}
    success = response.get("success") if isinstance(response.get("success"), dict) else {}
    if error:
        code = _first_path_value(payload, str(error.get("code_path") or ""))
        message = _first_path_value(payload, str(error.get("message_path") or ""))
        if "equals" in error and str(code) == str(error.get("equals")):
            return str(code), str(message or "declared error condition matched"), "failed"
        if "not_equals" in error and str(code) != str(error.get("not_equals")):
            return str(code), str(message or "declared error condition matched"), "failed"
    if success:
        code = _first_path_value(payload, str(success.get("path") or ""))
        message = _first_path_value(payload, str(success.get("message_path") or ""))
        if "equals" in success:
            passed = str(code) == str(success.get("equals"))
            return str(code), str(message or ""), "passed" if passed else "failed"
        allowed = success.get("in")
        if isinstance(allowed, list):
            passed = str(code) in {str(item) for item in allowed}
            return str(code), str(message or ""), "passed" if passed else "failed"
    return "OK", "", "passed"


def _first_path_value(payload: Any, path: str) -> Any:
    if not path:
        return None
    values = _path_values(payload, path)
    return values[0] if values else None


def _path_values(value: Any, path: str) -> list[Any]:
    current = [value]
    for raw_part in path.split("."):
        part = raw_part.strip()
        if not part:
            continue
        array_part = part == "[]" or part.endswith("[]")
        key = "" if part == "[]" else part.removesuffix("[]")
        next_values: list[Any] = []
        for item in current:
            if key:
                if not isinstance(item, dict) or key not in item:
                    continue
                selected = item.get(key)
            else:
                selected = item
            if array_part:
                if isinstance(selected, list):
                    next_values.extend(selected)
                elif selected not in (None, ""):
                    next_values.append(selected)
            else:
                next_values.append(selected)
        current = next_values
        if not current:
            return []
    return current


def _verification_failed(
    variant: dict[str, Any],
    reason: str,
    message: str,
    raw_arguments: dict[str, Any],
    url: str,
    checked_at: datetime,
    method: str = "GET",
    raw_body: dict[str, Any] | None = None,
    raw_headers: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "variant_id": variant.get("variant_id"),
        "operation_id": variant.get("operation_id"),
        "capability_id": variant.get("capability_id") or variant.get("capability"),
        "status": "failed",
        "reason": reason,
        "message": message,
        "request": {
            "url": url,
            "method": method,
            "arguments": _redact(raw_arguments),
            "body": _redact(raw_body or {}),
            "headers": _redact(raw_headers or {}),
        },
        "checked_at": checked_at.isoformat(),
    }


def _redact(arguments: dict[str, Any]) -> dict[str, Any]:
    redacted = {}
    for key, value in arguments.items():
        if isinstance(value, dict):
            redacted[key] = _redact(value)
        elif isinstance(value, list):
            redacted[key] = [_redact(item) if isinstance(item, dict) else item for item in value]
        else:
            redacted[key] = "***" if key.lower() in {"servicekey", "service_key", "key", "authorization"} else value
    return redacted


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest public API specification into semantic_platform Postgres catalog.")
    parser.add_argument("--source", required=True)
    parser.add_argument("--manual-llm-response", default=None)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    manual = _load_manual(args.manual_llm_response)
    result = run_source_ingestion(args.source, manual_llm_response=manual, apply=args.apply, force=args.force)
    print(json.dumps(_ingestion_result_summary(result), ensure_ascii=False, indent=2, default=str))


def _ingestion_result_summary(result: dict[str, Any]) -> dict[str, Any]:
    apply_result = result.get("apply_result") if isinstance(result.get("apply_result"), dict) else {}
    embedding_result = result.get("embedding_result") if isinstance(result.get("embedding_result"), dict) else {}
    return {
        "source_document_id": result.get("source_document_id"),
        "proposal_ids": result.get("proposal_ids", []),
        "proposal_count": result.get("proposal_count"),
        "proposal_item_count": result.get("proposal_item_count"),
        "chunk_count": result.get("chunk_count"),
        "operation_count": result.get("operation_count"),
        "operation_contract_count": result.get("operation_contract_count"),
        "operation_variant_count": result.get("operation_variant_count"),
        "capability_count": result.get("capability_count"),
        "verification_count": result.get("verification_count"),
        "api_section_count": result.get("api_section_count"),
        "verified_api_section_count": result.get("verified_api_section_count"),
        "endpoint_candidate_check_count": result.get("endpoint_candidate_check_count"),
        "evidence_snapshot_path": result.get("evidence_snapshot_path"),
        "applied": result.get("applied"),
        "apply_result_count": apply_result.get("proposal_count"),
        "embedding": {
            "status": embedding_result.get("status"),
            "embedded_changed_count": embedding_result.get("embedded_count"),
            "total_capability_document_count": embedding_result.get("total_count"),
            "embedding_model": embedding_result.get("embedding_model"),
            "embedding_provider": embedding_result.get("embedding_provider"),
            "vector_status": embedding_result.get("vector_status"),
        } if embedding_result else None,
    }


def _load_manual(path: str | None) -> dict[str, Any] | None:
    if not path:
        return None
    document = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(document, dict):
        raise ValueError("--manual-llm-response must point to a JSON object")
    return document


def _legacy_items(payload: dict[str, Any]) -> bool:
    return isinstance(payload.get("items"), list)


def _source_document_id(source: Path, sha256: str, metadata: dict[str, Any]) -> str:
    provider = _slug_optional(metadata.get("provider"))
    source_key = _slug_optional(metadata.get("source_key") or metadata.get("key"))
    version = _slug_optional(metadata.get("version"))
    if provider and source_key:
        parts = ["source", provider, source_key]
        if version:
            parts.append(version)
        return ".".join(parts)
    return f"source.{sha256[:8]}.{_slug(source.stem)}"


def _source_metadata(source: Path) -> dict[str, Any]:
    metadata: dict[str, Any] = {}
    metadata.update(_load_source_manifest_metadata(source))
    metadata.update(_load_source_sidecar_metadata(source))
    return metadata


def _load_source_manifest_metadata(source: Path) -> dict[str, Any]:
    manifest_path = os.getenv("SEMANTIC_PLATFORM_SOURCE_MANIFEST")
    candidates = [Path(manifest_path)] if manifest_path else [source.parent / "manifest.json", source.parent / "sources.json"]
    source_resolved = source.resolve()
    for candidate in candidates:
        if not candidate.exists():
            continue
        try:
            payload = json.loads(candidate.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        entries = payload.get("sources") if isinstance(payload, dict) else payload
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if not isinstance(entry, dict) or not entry.get("path"):
                continue
            entry_path = Path(str(entry["path"]))
            if not entry_path.is_absolute():
                entry_path = candidate.parent / entry_path
            try:
                if entry_path.resolve() == source_resolved:
                    return {key: value for key, value in entry.items() if key != "path"}
            except OSError:
                continue
    return {}


def _load_source_sidecar_metadata(source: Path) -> dict[str, Any]:
    candidates = [
        source.with_suffix(source.suffix + ".source.json"),
        source.with_suffix(".source.json"),
    ]
    for candidate in candidates:
        if not candidate.exists():
            continue
        try:
            payload = json.loads(candidate.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        return payload if isinstance(payload, dict) else {}
    return {}


def _list(value: Any) -> list[dict[str, Any]]:
    return [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []


def _target_id(payload: dict[str, Any]) -> str | None:
    return payload.get("id") or payload.get("variant_id") or payload.get("operation_id")


def _slug(value: str) -> str:
    import re

    slug = re.sub(r"[^0-9A-Za-z가-힣._-]+", "_", value).strip("_").lower()
    return slug[:80] or "document"


def _slug_optional(value: Any) -> str:
    if value is None or str(value).strip() == "":
        return ""
    return _slug(str(value))


if __name__ == "__main__":
    main()
