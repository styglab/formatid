from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from services.semantic_platform.lib.ingestion.llm.proposal import operation_variant_candidates
from services.semantic_platform.lib.ingestion.state import SourceGraphState


def write_evidence_snapshot(
    state: SourceGraphState,
    *,
    graph_node_names: list[str],
) -> tuple[str, dict[str, Any]]:
    output_dir = Path(os.getenv("SEMANTIC_PLATFORM_EVIDENCE_DIR", "/tmp/semantic_platform/evidence"))
    output_dir.mkdir(parents=True, exist_ok=True)
    document_id = state["source_document"]["id"]
    path = output_dir / f"{document_id}.api_spec_evidence.json"
    payload = evidence_snapshot_payload(state, graph_node_names=graph_node_names)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    return str(path), payload


def evidence_snapshot_payload(
    state: SourceGraphState,
    *,
    graph_node_names: list[str],
) -> dict[str, Any]:
    return {
        "source_document": state.get("source_document", {}),
        "graph": graph_node_names,
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
        "operation_variant_candidates": operation_variant_candidates(state),
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


def _target_id(payload: dict[str, Any]) -> str | None:
    return payload.get("id") or payload.get("variant_id") or payload.get("operation_id")
