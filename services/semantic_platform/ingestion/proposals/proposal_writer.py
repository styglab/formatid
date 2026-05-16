from __future__ import annotations

import json
from pathlib import Path

from services.semantic_platform.ingestion.chunking.source_chunking import _summarize_source_chunks
from services.semantic_platform.ingestion.graphs.source_ingestion.config import SOURCE_LLM_MODEL
from services.semantic_platform.ingestion.graphs.source_ingestion.state import SourceGraphState
from services.semantic_platform.ingestion.llm.source_proposal_analyzer import _llm_mode

def write_proposals(state: SourceGraphState) -> SourceGraphState:
    output_dir = Path(state["output_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{state['document_id']}.llm_graph_proposal.json"
    payload = {
        "graph": "semantic_platform.ingestion.source_graph",
        "mode": "llm_proposal",
        "llm_mode": _llm_mode(),
        "model": SOURCE_LLM_MODEL,
        "structured_spec": state.get("structured_spec", {}),
        "catalog_context_summary": {
            "semantic_type_count": len(state.get("catalog_context", {}).get("semantic_types", {})),
            "capability_count": len(state.get("catalog_context", {}).get("capabilities", {})),
        },
        "source_chunks": {
            "path": state.get("chunks_path"),
            "count": len(state.get("source_chunks", [])),
            "summary": _summarize_source_chunks(state.get("source_chunks", [])),
        },
        "semantic_platform_proposal": state.get("semantic_platform_proposal", {}),
        "execution_contract_proposal": state.get("execution_contract_proposal", {}),
        "messages": state["messages"],
    }
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    state["proposal_path"] = str(output_path)
    state["messages"].append(f"write_proposals:{output_path}")
    return state


def apply_if_requested(state: SourceGraphState) -> SourceGraphState:
    if not state.get("apply"):
        state["applied_changes"] = []
        state["messages"].append("apply_skipped:review_required")
        return state
    state["applied_changes"] = [
        {
            "status": "blocked",
            "reason": "LLM source graph writes proposals only. Apply requires review gate.",
        }
    ]
    return state
