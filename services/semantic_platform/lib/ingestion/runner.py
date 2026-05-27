from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from services.semantic_platform.lib.ingestion.evidence_snapshot import write_evidence_snapshot
from services.semantic_platform.lib.ingestion.graph_runtime import CompiledStateGraph
from services.semantic_platform.lib.ingestion.state import SourceGraphState
from services.semantic_platform.lib.storage import SemanticCatalogRepository


def run_source_ingestion(
    source_path: str | Path,
    *,
    graph: CompiledStateGraph,
    manual_llm_response: dict[str, Any] | None = None,
    apply: bool = False,
    force: bool = False,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    ordered_nodes = graph.ordered_nodes
    if not ordered_nodes:
        raise ValueError("ingestion graph must contain at least one node")
    state: SourceGraphState = {
        "source_path": str(source_path),
        "manual_llm_response": manual_llm_response,
        "progress_callback": progress_callback,
    }
    state = ordered_nodes[0](state)
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

    for node in ordered_nodes[1:]:
        _emit_progress(progress_callback, {"step": node.__name__, "status": "running"})
        state = node(state)

    chunks = state["chunks"]
    repo.upsert_source_document(document)
    repo.replace_chunks(document["id"], chunks)
    graph_node_names = graph.node_names
    snapshot_path, snapshot_payload = write_evidence_snapshot(state, graph_node_names=graph_node_names)
    repo.upsert_evidence_snapshot(
        {
            "id": f"evidence.{document['id']}.latest",
            "source_document_id": document["id"],
            "snapshot_type": "api_spec_evidence",
            "payload": snapshot_payload,
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
    catalog_version_result = (
        repo.create_catalog_version(
            reason="source_ingestion_direct_apply",
            proposal_id=proposals[0]["id"] if proposals else None,
            created_by="ingestion",
            metadata={"source_document_id": document["id"]},
        )
        if apply_results
        else None
    )
    state["stored_proposal"] = stored_proposals[0] if stored_proposals else {}
    state["stored_proposals"] = stored_proposals
    state["apply_result"] = apply_result
    state["apply_results"] = apply_results
    state["capability_document_result"] = capability_document_result
    state["embedding_result"] = embedding_result
    state["catalog_version_result"] = catalog_version_result
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
        "catalog_version_result": catalog_version_result,
        "llm_progress": state.get("llm_progress", {}),
    }


def _emit_progress(callback: Callable[[dict[str, Any]], None] | None, event: dict[str, Any]) -> None:
    if callback is not None:
        callback(event)


def _applied_capability_ids(apply_results: list[dict[str, Any]]) -> list[str]:
    capability_ids = []
    for result in apply_results:
        for item in result.get("applied", []) if isinstance(result, dict) else []:
            if isinstance(item, dict) and item.get("item_type") == "capability" and item.get("target_id"):
                capability_ids.append(str(item["target_id"]))
    return list(dict.fromkeys(capability_ids))


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
