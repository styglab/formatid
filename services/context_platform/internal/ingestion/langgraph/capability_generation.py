from __future__ import annotations

from dataclasses import dataclass
from typing import Any, TypedDict

from langgraph.graph import END, StateGraph

from services.context_platform.internal.ingestion.capability_generation import build_capability_generation_payload
from services.context_platform.internal.ingestion.capability_generation import build_manual_capability_generation_request
from services.context_platform.internal.ingestion.capability_generation import capability_suggestions_from_manual_response
from services.context_platform.internal.ingestion.capability_generation import skip_capabilities_without_llm
from services.context_platform.internal.ingestion.langgraph.common import resolve_llm_mode
from services.context_platform.internal.ingestion.llm.capability_generation import (
    normalize_manual_capability_generation_response,
)


class CapabilityGenerationState(TypedDict, total=False):
    run_id: str
    source: dict[str, Any]
    document: dict[str, Any]
    operations: list[dict[str, Any]]
    canonical_reconciliation: dict[str, Any]
    binding_generation: dict[str, Any]
    llm_mode: str
    manual_llm_response: dict[str, Any]
    manual_llm_request: dict[str, Any]
    suggestions: list[dict[str, Any]]
    status: str
    engine: str
    error: str
    payload: dict[str, Any]


@dataclass
class CapabilityGenerationResult:
    status: str
    engine: str
    llm_mode: str
    payload: dict[str, Any] | None = None
    manual_llm_request: dict[str, Any] | None = None
    error: str | None = None


def run_capability_generation_graph(
    *,
    run_id: str,
    source: dict[str, Any],
    document: dict[str, Any],
    operations: list[dict[str, Any]],
    canonical_reconciliation: dict[str, Any],
    binding_generation: dict[str, Any],
    llm_mode: str | None = None,
    manual_llm_response: dict[str, Any] | None = None,
) -> CapabilityGenerationResult:
    state: CapabilityGenerationState = {
        "run_id": run_id,
        "source": source,
        "document": document,
        "operations": operations,
        "canonical_reconciliation": canonical_reconciliation,
        "binding_generation": binding_generation,
        "llm_mode": resolve_llm_mode(llm_mode),
    }
    if isinstance(manual_llm_response, dict):
        state["manual_llm_response"] = manual_llm_response
    result = _build_graph().invoke(state)
    return CapabilityGenerationResult(
        status=str(result.get("status") or "ready"),
        engine=str(result.get("engine") or "heuristic_capability_generation_graph"),
        llm_mode=str(result.get("llm_mode") or "disabled"),
        payload=result.get("payload") if isinstance(result.get("payload"), dict) else None,
        manual_llm_request=_drop_internal_request_keys(result.get("manual_llm_request")),
        error=str(result.get("error") or "") or None,
    )


def _build_graph():
    graph = StateGraph(CapabilityGenerationState)
    graph.add_node("prepare_request", _prepare_request)
    graph.add_node("generate_suggestions", _generate_suggestions)
    graph.add_node("build_payload", _build_payload)
    graph.set_entry_point("prepare_request")
    graph.add_edge("prepare_request", "generate_suggestions")
    graph.add_edge("generate_suggestions", "build_payload")
    graph.add_edge("build_payload", END)
    return graph.compile()


def _prepare_request(state: CapabilityGenerationState) -> CapabilityGenerationState:
    state["manual_llm_request"] = build_manual_capability_generation_request(
        run_id=state.get("run_id") or "",
        source=state["source"],
        document=state["document"],
        operations=state["operations"],
        canonical_reconciliation=state["canonical_reconciliation"],
        binding_generation=state["binding_generation"],
    )
    return state


def _generate_suggestions(state: CapabilityGenerationState) -> CapabilityGenerationState:
    llm_mode = str(state.get("llm_mode") or "disabled")
    request = state.get("manual_llm_request") or {}
    operation_contexts = request.get("operation_contexts") if isinstance(request.get("operation_contexts"), list) else []
    manual_llm_response = state.get("manual_llm_response")

    if llm_mode == "agent_manual":
        if not isinstance(manual_llm_response, dict):
            state["status"] = "waiting_manual_llm"
            state["engine"] = "agent_manual_pending_capability_generation_graph"
            state["suggestions"] = []
            return state
        normalized = normalize_manual_capability_generation_response(manual_llm_response)
        state["suggestions"] = capability_suggestions_from_manual_response(
            operation_contexts,
            normalized,
            allow_heuristic_propose=False,
        )
        state["status"] = "ready"
        state["engine"] = "agent_manual_capability_generation_graph"
        state["manual_llm_request"] = {}
        return state

    state["suggestions"] = skip_capabilities_without_llm(operation_contexts)
    state["status"] = "ready"
    state["engine"] = "no_llm_capability_generation_graph"
    state["manual_llm_request"] = {}
    return state


def _build_payload(state: CapabilityGenerationState) -> CapabilityGenerationState:
    if str(state.get("status") or "") != "ready":
        state["payload"] = {}
        return state
    state["payload"] = build_capability_generation_payload(
        source=state["source"],
        document=state["document"],
        suggestions=state.get("suggestions") or [],
        llm_mode=state.get("llm_mode") or "disabled",
        engine=state.get("engine") or "heuristic_capability_generation_graph",
    )
    return state


def _drop_internal_request_keys(payload: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return payload
    return {key: value for key, value in payload.items() if not key.startswith("_")}
