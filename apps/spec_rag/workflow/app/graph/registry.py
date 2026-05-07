from __future__ import annotations

import os
from typing import Any

from core.runtime.graph_runtime.context import GraphRunContext
from core.runtime.graph_runtime.registry import GraphDefinition, GraphRegistry

from apps.spec_rag.workflow.app.graph.indexing import GRAPH_NAME as INDEX_GRAPH_NAME
from apps.spec_rag.workflow.app.graph.indexing import build_spec_indexing_graph
from apps.spec_rag.workflow.app.graph.query import GRAPH_NAME as QUERY_GRAPH_NAME
from apps.spec_rag.workflow.app.graph.query import build_spec_query_graph


async def _run_index_graph(context: GraphRunContext) -> dict[str, Any]:
    return await _run_langgraph(context=context, build_graph=build_spec_indexing_graph)


async def _run_query_graph(context: GraphRunContext) -> dict[str, Any]:
    return await _run_langgraph(context=context, build_graph=build_spec_query_graph)


async def _run_langgraph(*, context: GraphRunContext, build_graph) -> dict[str, Any]:
    if context.graph_checkpointer is None:
        raise RuntimeError("spec_rag LangGraph execution requires graph_checkpointer")
    graph = build_graph(checkpointer=context.graph_checkpointer)
    config = {"configurable": {"thread_id": context.thread_id or context.run_id}}
    if context.resume_value is not None:
        from langgraph.types import Command

        return await graph.ainvoke(Command(resume=context.resume_value), config=config)
    return await graph.ainvoke(_initial_state(context), config=config)


def _initial_state(context: GraphRunContext) -> dict[str, Any]:
    return {
        "params": context.params,
        "checkpoint_database_url": context.checkpoint_store._database_url,
        "redis_url": os.getenv("SERVICE_REDIS_URL", os.getenv("WORKER_REDIS_URL", "redis://redis:6379/0")),
        "graph_run_id": context.run_id,
        "correlation_id": context.correlation_id,
        "resource_key": context.resource_key,
        "tasks": {},
    }


GRAPH_REGISTRY = GraphRegistry(
    [
        GraphDefinition(
            name=INDEX_GRAPH_NAME,
            description="Parse an uploaded spec, chunk it, and build dense/sparse indexes.",
            run=_run_index_graph,
        ),
        GraphDefinition(
            name=QUERY_GRAPH_NAME,
            description="Embed a user query, retrieve spec context, and generate an answer.",
            run=_run_query_graph,
        ),
    ]
)
