from __future__ import annotations

from dataclasses import dataclass
from typing import Any, TypedDict

from langgraph.graph import END, StateGraph

from services.context_platform.internal.ingestion.binding_generation import build_binding_generation_payload
from services.context_platform.internal.ingestion.binding_generation import build_manual_binding_generation_request
from services.context_platform.internal.ingestion.binding_generation import binding_suggestions_from_manual_response
from services.context_platform.internal.ingestion.binding_generation import suggest_bindings_without_llm
from services.context_platform.internal.ingestion.langgraph.common import resolve_llm_mode
from services.context_platform.internal.ingestion.llm.binding_generation import (
    normalize_manual_binding_generation_response,
)


class BindingGenerationState(TypedDict, total=False):
    run_id: str
    source: dict[str, Any]
    document: dict[str, Any]
    operations: list[dict[str, Any]]
    document_fields: list[dict[str, Any]]
    canonical_reconciliation: dict[str, Any]
    llm_mode: str
    manual_llm_response: dict[str, Any]
    manual_llm_request: dict[str, Any]
    suggestions: list[dict[str, Any]]
    status: str
    engine: str
    error: str
    payload: dict[str, Any]


@dataclass
class BindingGenerationResult:
    status: str
    engine: str
    llm_mode: str
    payload: dict[str, Any] | None = None
    manual_llm_request: dict[str, Any] | None = None
    error: str | None = None


def run_binding_generation_graph(
    *,
    run_id: str,
    source: dict[str, Any],
    document: dict[str, Any],
    operations: list[dict[str, Any]],
    document_fields: list[dict[str, Any]],
    canonical_reconciliation: dict[str, Any],
    llm_mode: str | None = None,
    manual_llm_response: dict[str, Any] | None = None,
) -> BindingGenerationResult:
    state: BindingGenerationState = {
        "run_id": run_id,
        "source": source,
        "document": document,
        "operations": operations,
        "document_fields": document_fields,
        "canonical_reconciliation": canonical_reconciliation,
        "llm_mode": resolve_llm_mode(llm_mode),
    }
    if isinstance(manual_llm_response, dict):
        state["manual_llm_response"] = manual_llm_response
    result = _build_graph().invoke(state)
    return BindingGenerationResult(
        status=str(result.get("status") or "ready"),
        engine=str(result.get("engine") or "heuristic_resolution_generation_graph"),
        llm_mode=str(result.get("llm_mode") or "disabled"),
        payload=result.get("payload") if isinstance(result.get("payload"), dict) else None,
        manual_llm_request=_drop_internal_request_keys(result.get("manual_llm_request")),
        error=str(result.get("error") or "") or None,
    )


def _build_graph():
    graph = StateGraph(BindingGenerationState)
    graph.add_node("prepare_request", _prepare_request)
    graph.add_node("generate_suggestions", _generate_suggestions)
    graph.add_node("build_payload", _build_payload)
    graph.set_entry_point("prepare_request")
    graph.add_edge("prepare_request", "generate_suggestions")
    graph.add_edge("generate_suggestions", "build_payload")
    graph.add_edge("build_payload", END)
    return graph.compile()


def _prepare_request(state: BindingGenerationState) -> BindingGenerationState:
    state["manual_llm_request"] = build_manual_binding_generation_request(
        run_id=state.get("run_id") or "",
        source=state["source"],
        document=state["document"],
        operations=state["operations"],
        document_fields=state["document_fields"],
        canonical_reconciliation=state["canonical_reconciliation"],
    )
    return state


def _generate_suggestions(state: BindingGenerationState) -> BindingGenerationState:
    llm_mode = str(state.get("llm_mode") or "disabled")
    request = state.get("manual_llm_request") or {}
    terms = request.get("source_terms") if isinstance(request.get("source_terms"), list) else []
    manual_llm_response = state.get("manual_llm_response")

    if llm_mode == "agent_manual":
        if not isinstance(manual_llm_response, dict):
            state["status"] = "waiting_manual_llm"
            state["engine"] = "agent_manual_pending_resolution_generation_graph"
            state["suggestions"] = []
            return state
        normalized = normalize_manual_binding_generation_response(manual_llm_response)
        state["suggestions"] = binding_suggestions_from_manual_response(terms, normalized, allow_heuristic_bind=False)
        state["status"] = "ready"
        state["engine"] = "agent_manual_resolution_generation_graph"
        state["manual_llm_request"] = {}
        return state

    state["suggestions"] = suggest_bindings_without_llm(terms)
    state["status"] = "ready"
    state["engine"] = "no_llm_resolution_generation_graph"
    state["manual_llm_request"] = {}
    return state


def _build_payload(state: BindingGenerationState) -> BindingGenerationState:
    if str(state.get("status") or "") != "ready":
        state["payload"] = {}
        return state
    state["payload"] = build_binding_generation_payload(
        source=state["source"],
        document=state["document"],
        canonical_reconciliation=state["canonical_reconciliation"],
        suggestions=state.get("suggestions") or [],
        llm_mode=state.get("llm_mode") or "disabled",
        engine=state.get("engine") or "heuristic_resolution_generation_graph",
    )
    return state


def _drop_internal_request_keys(payload: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return payload
    return {key: value for key, value in payload.items() if not key.startswith("_")}
