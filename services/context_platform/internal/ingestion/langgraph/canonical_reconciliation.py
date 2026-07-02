from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from typing import Any, TypedDict

from langgraph.graph import END, StateGraph

from services.context_platform.internal.ingestion.canonical_reconciliation import DECISION_TYPES
from services.context_platform.internal.ingestion.canonical_reconciliation import RELATION_DECISION_TYPES
from services.context_platform.internal.ingestion.canonical_reconciliation import build_linkml_fragment
from services.context_platform.internal.ingestion.canonical_reconciliation import build_manual_canonical_reconciliation_request
from services.context_platform.internal.ingestion.canonical_reconciliation import collect_source_terms
from services.context_platform.internal.ingestion.canonical_reconciliation import load_canonical_context
from services.context_platform.internal.ingestion.canonical_reconciliation import reconcile_terms_without_llm
from services.context_platform.internal.ingestion.canonical_reconciliation import relation_suggestions_from_manual_response
from services.context_platform.internal.ingestion.canonical_reconciliation import reconcile_terms_from_manual_response
from services.context_platform.internal.ingestion.langgraph.common import resolve_llm_mode
from services.context_platform.internal.ingestion.llm.canonical_reconciliation import (
    normalize_manual_canonical_reconciliation_response,
)
from services.context_platform.internal.storage import ContextPlatformRepository


class CanonicalReconciliationState(TypedDict, total=False):
    repo: ContextPlatformRepository
    run_id: str
    source: dict[str, Any]
    document: dict[str, Any]
    operations: list[dict[str, Any]]
    document_fields: list[dict[str, Any]]
    llm_mode: str
    context: dict[str, list[dict[str, Any]]]
    terms: list[dict[str, Any]]
    manual_llm_response: dict[str, Any]
    manual_llm_request: dict[str, Any]
    decisions: list[dict[str, Any]]
    concept_decisions: list[dict[str, Any]]
    representation_decisions: list[dict[str, Any]]
    representation_schema_decisions: list[dict[str, Any]]
    value_domain_decisions: list[dict[str, Any]]
    relation_suggestions: list[dict[str, Any]]
    status: str
    engine: str
    error: str
    payload: dict[str, Any]


@dataclass
class CanonicalReconciliationResult:
    status: str
    engine: str
    llm_mode: str
    payload: dict[str, Any] | None = None
    manual_llm_request: dict[str, Any] | None = None
    error: str | None = None


def run_canonical_reconciliation_graph(
    repo: ContextPlatformRepository,
    *,
    run_id: str,
    source: dict[str, Any],
    document: dict[str, Any],
    operations: list[dict[str, Any]],
    document_fields: list[dict[str, Any]],
    llm_mode: str | None = None,
    manual_llm_response: dict[str, Any] | None = None,
) -> CanonicalReconciliationResult:
    state: CanonicalReconciliationState = {
        "repo": repo,
        "run_id": run_id,
        "source": source,
        "document": document,
        "operations": operations,
        "document_fields": document_fields,
        "llm_mode": resolve_llm_mode(llm_mode),
    }
    if isinstance(manual_llm_response, dict):
        state["manual_llm_response"] = manual_llm_response
    result = _build_graph().invoke(state)
    return CanonicalReconciliationResult(
        status=str(result.get("status") or "ready"),
        engine=str(result.get("engine") or "heuristic_meaning_resolution_graph"),
        llm_mode=str(result.get("llm_mode") or "disabled"),
        payload=result.get("payload") if isinstance(result.get("payload"), dict) else None,
        manual_llm_request=_drop_internal_request_keys(result.get("manual_llm_request")),
        error=str(result.get("error") or "") or None,
    )


def _build_graph():
    graph = StateGraph(CanonicalReconciliationState)
    graph.add_node("load_context", _load_context)
    graph.add_node("reconcile_terms", _reconcile_terms)
    graph.add_node("build_payload", _build_payload)
    graph.set_entry_point("load_context")
    graph.add_edge("load_context", "reconcile_terms")
    graph.add_edge("reconcile_terms", "build_payload")
    graph.add_edge("build_payload", END)
    return graph.compile()


def _load_context(state: CanonicalReconciliationState) -> CanonicalReconciliationState:
    context = load_canonical_context(state["repo"])
    terms = collect_source_terms(
        source=state["source"],
        document=state["document"],
        operations=state["operations"],
        document_fields=state["document_fields"],
    )
    state["context"] = context
    state["terms"] = terms
    state["manual_llm_request"] = build_manual_canonical_reconciliation_request(
        run_id=state.get("run_id") or "",
        source=state["source"],
        document=state["document"],
        operations=state["operations"],
        document_fields=state["document_fields"],
        context=context,
    )
    return state


def _reconcile_terms(state: CanonicalReconciliationState) -> CanonicalReconciliationState:
    llm_mode = str(state.get("llm_mode") or "disabled")
    context = state.get("context") or {"classes": [], "slots": [], "class_slot_usages": []}
    terms = state.get("terms") or []
    manual_llm_response = state.get("manual_llm_response")

    if llm_mode == "agent_manual":
        if not isinstance(manual_llm_response, dict):
            state["status"] = "waiting_manual_llm"
            state["engine"] = "agent_manual_pending_meaning_resolution_graph"
            state["decisions"] = []
            return state
        normalized = normalize_manual_canonical_reconciliation_response(manual_llm_response)
        state["decisions"] = reconcile_terms_from_manual_response(terms, context, normalized, allow_heuristic_create=False)
        state["concept_decisions"] = normalized.get("concept_decisions") or []
        state["representation_decisions"] = normalized.get("representation_decisions") or []
        state["representation_schema_decisions"] = normalized.get("representation_schema_decisions") or []
        state["value_domain_decisions"] = normalized.get("value_domain_decisions") or []
        state["relation_suggestions"] = relation_suggestions_from_manual_response(context, normalized)
        state["status"] = "ready"
        state["engine"] = "agent_manual_meaning_resolution_graph"
        state["manual_llm_request"] = {}
        return state

    state["decisions"] = reconcile_terms_without_llm(terms, context)
    state["concept_decisions"] = []
    state["representation_decisions"] = []
    state["representation_schema_decisions"] = []
    state["value_domain_decisions"] = []
    state["relation_suggestions"] = []
    state["status"] = "ready"
    state["engine"] = "no_llm_meaning_resolution_graph"
    state["manual_llm_request"] = {}
    return state


def _build_payload(state: CanonicalReconciliationState) -> CanonicalReconciliationState:
    if str(state.get("status") or "") != "ready":
        state["payload"] = {}
        return state

    decisions = state.get("decisions") or []
    relation_suggestions = state.get("relation_suggestions") or []
    context = state.get("context") or {"classes": [], "slots": [], "class_slot_usages": []}
    counts = Counter(str(item.get("decision") or "create") for item in decisions)
    relation_counts = Counter(str(item.get("decision") or "propose_relation") for item in relation_suggestions)
    state["payload"] = {
        "llm_mode": state.get("llm_mode") or "disabled",
        "type": "meaning_resolution",
        "legacy_type": "canonical_reconciliation",
        "engine": state.get("engine") or "heuristic_meaning_resolution_graph",
        "source_id": state.get("source", {}).get("id"),
        "source_document_id": state.get("document", {}).get("id"),
        "context_summary": {
            "class_count": len(context.get("classes") or context.get("entities", [])),
            "class_slot_usage_count": len(context.get("class_slot_usages", [])),
            "slot_count": len(context.get("slots", [])),
            "relation_count": len(context.get("relations", [])),
        },
        "term_count": len(state.get("terms") or []),
        "decision_counts": {key: counts.get(key, 0) for key in sorted(DECISION_TYPES)},
        "relation_decision_counts": {key: relation_counts.get(key, 0) for key in sorted(RELATION_DECISION_TYPES)},
        "linkml_fragment": build_linkml_fragment(decisions, relation_suggestions),
        "decisions": decisions,
        "meaning_decisions": decisions,
        "concept_decisions": state.get("concept_decisions") or [],
        "representation_decisions": state.get("representation_decisions") or [],
        "representation_schema_decisions": state.get("representation_schema_decisions") or [],
        "value_domain_decisions": state.get("value_domain_decisions") or [],
        "relation_suggestions": relation_suggestions,
    }
    return state


def _drop_internal_request_keys(payload: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return payload
    return {key: value for key, value in payload.items() if not key.startswith("_")}
