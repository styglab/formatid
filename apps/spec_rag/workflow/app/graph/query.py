from __future__ import annotations

from typing import Any

from apps.spec_rag.workflow.app.contracts.state import SpecQueryState


GRAPH_NAME = "spec_query_graph"
QUEUE_NAME = "spec-rag:query"


def build_spec_query_graph(*, checkpointer: Any | None = None):
    from langgraph.graph import END, START, StateGraph

    graph = StateGraph(SpecQueryState)
    graph.add_node("embed_query", _planned_node("embed_query"))
    graph.add_node("retrieve_context", _planned_node("retrieve_context"))
    graph.add_node("build_prompt", _planned_node("build_prompt"))
    graph.add_node("generate_answer", _planned_answer_node)
    graph.add_edge(START, "embed_query")
    graph.add_edge("embed_query", "retrieve_context")
    graph.add_edge("retrieve_context", "build_prompt")
    graph.add_edge("build_prompt", "generate_answer")
    graph.add_edge("generate_answer", END)
    if checkpointer is None:
        return graph.compile()
    return graph.compile(checkpointer=checkpointer)


def _planned_node(name: str):
    async def run(state: SpecQueryState) -> SpecQueryState:
        tasks = dict(state.get("tasks") or {})
        tasks[name] = {"status": "planned"}
        return {"tasks": tasks}

    return run


async def _planned_answer_node(state: SpecQueryState) -> SpecQueryState:
    tasks = dict(state.get("tasks") or {})
    tasks["generate_answer"] = {"status": "planned"}
    return {
        "status": "planned",
        "answer": "spec_query_graph is defined, but query retrieval and answer tasks are not implemented yet.",
        "tasks": tasks,
    }
