from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from services.semantic_platform.lib.ingestion.llm.validation import (
    contract_path_list as _contract_path_list,
    validate_llm_analysis as _validate_llm_analysis,
    validate_operation_contract_runtime_schema as _validate_operation_contract_runtime_schema,
)
from services.semantic_platform.lib.ingestion.graph_runtime import END, START, StateGraph
from services.semantic_platform.lib.ingestion.nodes import (
    detect_api_sections_node,
    extract_blocks_node,
    extract_structured_evidence_node,
    extract_text_node,
    build_review_proposal,
    keep_passed_verified_capabilities,
    load_catalog_context,
    llm_propose_capability_catalog,
    llm_propose_execution_catalog,
    read_source,
    verify_capabilities,
    verify_endpoint_candidates,
)
from services.semantic_platform.lib.ingestion.runner import run_source_ingestion as _run_source_ingestion
from services.semantic_platform.lib.ingestion.state import SourceGraphState


def run_source_ingestion(
    source_path: str | Path,
    *,
    manual_llm_response: dict[str, Any] | None = None,
    apply: bool = False,
    force: bool = False,
) -> dict[str, Any]:
    return _run_source_ingestion(
        source_path,
        graph=SOURCE_INGESTION_GRAPH,
        manual_llm_response=manual_llm_response,
        apply=apply,
        force=force,
    )


def build_source_ingestion_graph():
    graph = StateGraph()
    graph.add_node("read_source", read_source)
    graph.add_node("extract_text", extract_text_node)
    graph.add_node("extract_blocks", extract_blocks_node)
    graph.add_node("detect_api_sections", detect_api_sections_node)
    graph.add_node("extract_structured_evidence", extract_structured_evidence_node)
    graph.add_node("load_catalog_context", load_catalog_context)
    graph.add_node("verify_endpoint_candidates", verify_endpoint_candidates)
    graph.add_node("llm_propose_capability_catalog", llm_propose_capability_catalog)
    graph.add_node("llm_propose_execution_catalog", llm_propose_execution_catalog)
    graph.add_node("verify_capabilities", verify_capabilities)
    graph.add_node("keep_passed_verified_capabilities", keep_passed_verified_capabilities)
    graph.add_node("build_review_proposal", build_review_proposal)

    graph.add_edge(START, "read_source")
    graph.add_edge("read_source", "extract_text")
    graph.add_edge("extract_text", "extract_blocks")
    graph.add_edge("extract_blocks", "detect_api_sections")
    graph.add_edge("detect_api_sections", "extract_structured_evidence")
    graph.add_edge("extract_structured_evidence", "load_catalog_context")
    graph.add_edge("load_catalog_context", "verify_endpoint_candidates")
    graph.add_edge("verify_endpoint_candidates", "llm_propose_capability_catalog")
    graph.add_edge("llm_propose_capability_catalog", "llm_propose_execution_catalog")
    graph.add_edge("llm_propose_execution_catalog", "verify_capabilities")
    graph.add_edge("verify_capabilities", "keep_passed_verified_capabilities")
    graph.add_edge("keep_passed_verified_capabilities", "build_review_proposal")
    graph.add_edge("build_review_proposal", END)
    return graph.compile()


SOURCE_INGESTION_GRAPH = build_source_ingestion_graph()


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


if __name__ == "__main__":
    main()
