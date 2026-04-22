from __future__ import annotations

import operator
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Annotated, Any, TypedDict


class SummaryGraphState(TypedDict, total=False):
    job_id: str
    bucket: str
    object_key: str
    callback_url: str | None
    graph: dict[str, Any]

    extract_payload: dict[str, Any]
    llm_payload: dict[str, Any]
    result: dict[str, Any]

    completed_nodes: Annotated[list[str], operator.add]


SummaryStep = Callable[[SummaryGraphState], Awaitable[dict[str, Any]]]


@dataclass(frozen=True)
class SummaryDagSteps:
    extract_text: SummaryStep
    serve_llm: SummaryStep
    load_result: SummaryStep


def build_summary_graph(steps: SummaryDagSteps):
    from langgraph.graph import END, START, StateGraph

    graph = StateGraph(SummaryGraphState)
    graph.add_node("extract_text", _extract_text_node(steps))
    graph.add_node("serve_llm", _serve_llm_node(steps))
    graph.add_node("load_result", _load_result_node(steps))

    graph.add_edge(START, "extract_text")
    graph.add_edge("extract_text", "serve_llm")
    graph.add_edge("serve_llm", "load_result")
    graph.add_edge("load_result", END)
    return graph.compile()


async def run_summary_dag(
    *,
    job_id: str,
    bucket: str,
    object_key: str,
    callback_url: str | None,
    steps: SummaryDagSteps,
) -> dict[str, Any]:
    graph = build_summary_graph(steps)
    initial_state: SummaryGraphState = {
        "job_id": job_id,
        "bucket": bucket,
        "object_key": object_key,
        "callback_url": callback_url,
        "graph": build_summary_graph_definition(),
        "completed_nodes": [],
    }
    return await graph.ainvoke(initial_state)


def build_summary_graph_definition() -> dict[str, Any]:
    return {
        "engine": "langgraph",
        "nodes": ["extract_text", "serve_llm", "load_result"],
        "edges": [
            ["START", "extract_text"],
            ["extract_text", "serve_llm"],
            ["serve_llm", "load_result"],
            ["load_result", "END"],
        ],
    }


def _extract_text_node(steps: SummaryDagSteps):
    async def node(state: SummaryGraphState) -> SummaryGraphState:
        result = await steps.extract_text(state)
        return {**result, "completed_nodes": ["extract_text"]}

    return node


def _serve_llm_node(steps: SummaryDagSteps):
    async def node(state: SummaryGraphState) -> SummaryGraphState:
        result = await steps.serve_llm(state)
        return {**result, "completed_nodes": ["serve_llm"]}

    return node


def _load_result_node(steps: SummaryDagSteps):
    async def node(state: SummaryGraphState) -> SummaryGraphState:
        result = await steps.load_result(state)
        return {**result, "completed_nodes": ["load_result"]}

    return node
