from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

try:
    from langgraph.graph import END, StateGraph
except ImportError:  # pragma: no cover - local fallback when langgraph is not installed.
    END = "__end__"
    StateGraph = None

from services.semantic_platform.ingestion.graphs.source_ingestion.config import (
    DEFAULT_CHUNKS_OUTPUT_DIR,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_SOURCE,
    SOURCE_COMMIT_MODES,
)
from services.semantic_platform.ingestion.graphs.source_ingestion.nodes import (
    analyze_source_with_llm,
    apply_catalog_changes,
    apply_if_requested,
    extract_text,
    load_catalog_context,
    read_source,
    split_source_chunks,
    write_proposals,
    write_source_chunks,
)
from services.semantic_platform.ingestion.graphs.source_ingestion.state import SourceGraphState


def run_source_graph(
    source_path: str | Path = DEFAULT_SOURCE,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    chunks_output_dir: str | Path = DEFAULT_CHUNKS_OUTPUT_DIR,
    apply: bool = False,
    commit_mode: str = "proposal",
    provider: str | None = None,
    manual_llm_response: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if apply:
        commit_mode = "direct_apply"
    if commit_mode not in SOURCE_COMMIT_MODES:
        raise ValueError(f"commit_mode must be one of {sorted(SOURCE_COMMIT_MODES)}")
    initial_state: SourceGraphState = {
        "source_path": str(source_path),
        "output_dir": str(output_dir),
        "chunks_output_dir": str(chunks_output_dir),
        "apply": apply,
        "commit_mode": commit_mode,
        "provider_hint": provider,
        "messages": [],
    }
    if isinstance(manual_llm_response, dict):
        initial_state["manual_llm_response"] = manual_llm_response
    if StateGraph is None:
        return _run_sequential(initial_state)
    return _build_graph(commit_mode).invoke(initial_state)


def _build_graph(commit_mode: str) -> Any:
    graph = StateGraph(SourceGraphState)
    graph.add_node("read_source", read_source)
    graph.add_node("extract_text", extract_text)
    graph.add_node("split_source_chunks", split_source_chunks)
    graph.add_node("write_source_chunks", write_source_chunks)
    graph.add_node("load_catalog_context", load_catalog_context)
    graph.add_node("analyze_source_with_llm", analyze_source_with_llm)
    if commit_mode == "direct_apply":
        graph.add_node("apply_catalog_changes", apply_catalog_changes)
        commit_node = "apply_catalog_changes"
    else:
        graph.add_node("write_proposals", write_proposals)
        graph.add_node("apply_if_requested", apply_if_requested)
        commit_node = "write_proposals"
    graph.set_entry_point("read_source")
    graph.add_edge("read_source", "extract_text")
    graph.add_edge("extract_text", "split_source_chunks")
    graph.add_edge("split_source_chunks", "write_source_chunks")
    graph.add_edge("write_source_chunks", "load_catalog_context")
    graph.add_edge("load_catalog_context", "analyze_source_with_llm")
    graph.add_edge("analyze_source_with_llm", commit_node)
    if commit_mode == "direct_apply":
        graph.add_edge("apply_catalog_changes", END)
    else:
        graph.add_edge("write_proposals", "apply_if_requested")
        graph.add_edge("apply_if_requested", END)
    return graph.compile()


def _run_sequential(state: SourceGraphState) -> SourceGraphState:
    common_nodes = (
        read_source,
        extract_text,
        split_source_chunks,
        write_source_chunks,
        load_catalog_context,
        analyze_source_with_llm,
    )
    commit_nodes = (
        (apply_catalog_changes,)
        if state.get("commit_mode") == "direct_apply"
        else (write_proposals, apply_if_requested)
    )
    for node in (*common_nodes, *commit_nodes):
        state = node(state)
    state["messages"].append("langgraph_not_installed_used_sequential_fallback")
    return state


def _load_manual_llm_response(path: str | None) -> dict[str, Any] | None:
    if not path:
        return None
    document = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(document, dict):
        raise ValueError("--manual-llm-response must point to a JSON object")
    return document


def main() -> None:
    parser = argparse.ArgumentParser(description="Run LLM source ingestion graph.")
    parser.add_argument("--source", default=str(DEFAULT_SOURCE))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--chunks-output-dir", default=str(DEFAULT_CHUNKS_OUTPUT_DIR))
    parser.add_argument("--provider", default=None, help="Optional provider hint. Leave empty when unknown.")
    parser.add_argument(
        "--manual-llm-response",
        default=None,
        help="Path to an explicit JSON LLM response payload used only when LLM_MODE=codex_manual.",
    )
    parser.add_argument(
        "--commit-mode",
        choices=sorted(SOURCE_COMMIT_MODES),
        default="proposal",
        help="proposal writes a review artifact; direct_apply updates catalog files directly.",
    )
    parser.add_argument("--apply", action="store_true", help="Backward-compatible alias for --commit-mode direct_apply.")
    args = parser.parse_args()
    manual_llm_response = _load_manual_llm_response(args.manual_llm_response)
    result = run_source_graph(
        source_path=args.source,
        output_dir=args.output_dir,
        chunks_output_dir=args.chunks_output_dir,
        apply=args.apply,
        commit_mode=args.commit_mode,
        provider=args.provider,
        manual_llm_response=manual_llm_response,
    )
    print(
        json.dumps(
            {
                "proposal_path": result.get("proposal_path"),
                "chunks_path": result.get("chunks_path"),
                "commit_mode": result.get("commit_mode"),
                "messages": result.get("messages", []),
                "applied_changes": result.get("applied_changes", []),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
if __name__ == "__main__":
    main()
