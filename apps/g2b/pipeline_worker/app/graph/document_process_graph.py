from __future__ import annotations

from typing import Any

from core.runtime.graph_runtime import run_tracked_node

from apps.g2b.pipeline_worker.app.contracts.document_process import DocumentProcessOutput, G2bDocumentProcessState

GRAPH_NAME = "document_process_graph"
PROGRESS_TOTAL = 1


def build_g2b_document_process_graph():
    from langgraph.graph import END, START, StateGraph

    graph = StateGraph(G2bDocumentProcessState)
    graph.add_node("document", _document_node)
    graph.add_edge(START, "document")
    graph.add_edge("document", END)
    return graph.compile()


def build_g2b_document_process_initial_state() -> dict[str, Any]:
    return {"completed_nodes": []}


async def _document_node(state: G2bDocumentProcessState) -> G2bDocumentProcessState:
    async def work() -> DocumentProcessOutput:
        params = state.get("params", {})
        return {
            "document_id": params.get("document_id"),
            "status": "skipped",
            "skip_reason": "not_implemented",
            "completed_nodes": ["document"],
        }

    return await run_tracked_node(
        state,
        graph_name=GRAPH_NAME,
        node_name="document",
        progress_total=PROGRESS_TOTAL,
        work=work,
        input_summary={"document_id": state.get("params", {}).get("document_id")},
        output_summary=lambda output: {
            "document_id": output.get("document_id"),
            "status": output.get("status"),
            "skip_reason": output.get("skip_reason"),
        },
    )
