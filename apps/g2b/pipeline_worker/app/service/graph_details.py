from __future__ import annotations

from typing import Any

from core.runtime.graph_runtime import TriggeredGraphRequest


def build_graph_result_details(result: dict[str, Any]) -> dict[str, Any]:
    return {
        "completed_nodes": result.get("completed_nodes", []),
        "document_id": result.get("document_id"),
        "status": result.get("status"),
        "skip_reason": result.get("skip_reason"),
    }


def build_triggered_run_name(request: TriggeredGraphRequest) -> str:
    return f"g2b_pipeline_{request.graph_name}_triggered"
