from __future__ import annotations

import os
from typing import Any

from apps.spec_rag.workflow.app.contracts.state import SpecIndexingState
from core.runtime.task_runtime.enqueue import enqueue_task


GRAPH_NAME = "spec_indexing_graph"
QUEUE_NAME = "spec-rag:index"
VECTOR_COLLECTION = "spec_rag"
SPARSE_INDEX = "spec_rag"
EMBEDDING_DIMENSIONS = 16


def build_spec_indexing_graph(*, checkpointer: Any | None = None):
    from langgraph.graph import END, START, StateGraph
    from langgraph.types import interrupt

    graph = StateGraph(SpecIndexingState)
    graph.add_node("parse_dispatch", _parse_dispatch_node)
    graph.add_node("parse_resume", _resume_task_node_factory(task_key="parse_task_id", result_key="parse", interrupt_fn=interrupt))
    graph.add_node("chunk_dispatch", _chunk_dispatch_node)
    graph.add_node("chunk_resume", _resume_task_node_factory(task_key="chunk_task_id", result_key="chunk", interrupt_fn=interrupt))
    graph.add_node("index_dispatch", _index_dispatch_node)
    graph.add_node("index_resume", _index_resume_node_factory(interrupt_fn=interrupt))
    graph.add_node("finalize", _finalize_node)
    graph.add_edge(START, "parse_dispatch")
    graph.add_edge("parse_dispatch", "parse_resume")
    graph.add_edge("parse_resume", "chunk_dispatch")
    graph.add_edge("chunk_dispatch", "chunk_resume")
    graph.add_edge("chunk_resume", "index_dispatch")
    graph.add_edge("index_dispatch", "index_resume")
    graph.add_edge("index_resume", "finalize")
    graph.add_edge("finalize", END)
    if checkpointer is None:
        return graph.compile()
    return graph.compile(checkpointer=checkpointer)


async def _parse_dispatch_node(state: SpecIndexingState) -> SpecIndexingState:
    resource_key = _resource_key(state)
    message = await enqueue_task(
        redis_url=state["redis_url"],
        task_name="parse.document.run",
        payload={
            "source": _source(state),
            "target": {
                "bucket_env": "S3_BUCKET",
                "object_key": _parsed_object_key(resource_key),
                "endpoint_env": "S3_ENDPOINT",
                "access_key_env": "S3_ACCESS_KEY",
                "secret_key_env": "S3_SECRET_KEY",
                "secure_env": "S3_SECURE",
                "resource_key": resource_key,
                "metadata_target": {
                    "schema": "spec_rag",
                    "table": "parsed_documents",
                    "database_url_env": "POSTGRES_DATABASE_URL",
                },
            },
        },
        correlation_id=state.get("correlation_id") or state.get("graph_run_id"),
        resource_key=resource_key,
    )
    return {"parse_task_id": message.task_id, "status": "parse_queued", "resource_key": resource_key}


async def _chunk_dispatch_node(state: SpecIndexingState) -> SpecIndexingState:
    resource_key = _resource_key(state)
    message = await enqueue_task(
        redis_url=state["redis_url"],
        task_name="chunk.document.run",
        payload={
            "source": {
                "type": "object_storage",
                "bucket_env": "S3_BUCKET",
                "object_key": _parsed_object_key(resource_key),
                "endpoint_env": "S3_ENDPOINT",
                "access_key_env": "S3_ACCESS_KEY",
                "secret_key_env": "S3_SECRET_KEY",
                "secure_env": "S3_SECURE",
                "resource_key": resource_key,
            },
            "target": {"schema": "spec_rag", "table": "chunks", "key_value": resource_key},
            "options": {"chunk_size_chars": 1600, "overlap_chars": 200},
        },
        correlation_id=state.get("correlation_id") or state.get("graph_run_id"),
        resource_key=resource_key,
    )
    return {"chunk_task_id": message.task_id, "status": "chunk_queued", "resource_key": resource_key}


async def _index_dispatch_node(state: SpecIndexingState) -> SpecIndexingState:
    resource_key = _resource_key(state)
    dense_message = await enqueue_task(
        redis_url=state["redis_url"],
        task_name="index.dense.upsert",
        payload={
            "source": {
                "schema": "spec_rag",
                "table": "chunks",
                "key_value": resource_key,
                "key_column": "resource_key",
                "text_column": "chunk_text",
                "order_column": "chunk_index",
            },
            "target": {
                "collection": _vector_collection(state),
                "resource_key": resource_key,
                "endpoint_env": "SPEC_RAG_VECTOR_DB_ENDPOINT",
                "api_key_env": "QDRANT_API_KEY",
            },
            "request": {
                "dimensions": _embedding_dimensions(state),
                "embedding_endpoint_env": "SPEC_RAG_EMBEDDING_ENDPOINT",
                "embedding_model_env": "SPEC_RAG_EMBEDDING_MODEL",
                "embedding_model": "mock-embedding",
            },
        },
        correlation_id=state.get("correlation_id") or state.get("graph_run_id"),
        resource_key=resource_key,
    )
    sparse_message = await enqueue_task(
        redis_url=state["redis_url"],
        task_name="index.sparse.upsert",
        payload={
            "source": {
                "schema": "spec_rag",
                "table": "chunks",
                "key_value": resource_key,
                "key_column": "resource_key",
                "text_column": "chunk_text",
                "order_column": "chunk_index",
            },
            "target": {
                "index": _sparse_index(state),
                "resource_key": resource_key,
                "endpoint_env": "OPENSEARCH_ENDPOINT",
            },
        },
        correlation_id=state.get("correlation_id") or state.get("graph_run_id"),
        resource_key=resource_key,
    )
    return {
        "index_dense_task_id": dense_message.task_id,
        "index_sparse_task_id": sparse_message.task_id,
        "status": "index_queued",
    }


def _resume_task_node_factory(*, task_key: str, result_key: str, interrupt_fn):
    async def run(state: SpecIndexingState) -> SpecIndexingState:
        return await _resume_task_node(state, task_key=task_key, result_key=result_key, interrupt_fn=interrupt_fn)

    return run


async def _resume_task_node(
    state: SpecIndexingState,
    *,
    task_key: str,
    result_key: str,
    interrupt_fn,
) -> SpecIndexingState:
    task_id = state[task_key]
    resume_payload = interrupt_fn({"kind": "task_result", "task_id": task_id})
    status = resume_payload.get("status") if isinstance(resume_payload, dict) else None
    tasks = dict(state.get("tasks") or {})
    tasks[result_key] = {"task_id": task_id, "status": status}
    if status != "succeeded":
        raise RuntimeError(f"task {task_id} finished with status {status}")
    return {"tasks": tasks, "status": f"{result_key}_succeeded"}


def _index_resume_node_factory(*, interrupt_fn):
    async def run(state: SpecIndexingState) -> SpecIndexingState:
        tasks = dict(state.get("tasks") or {})
        pending = _pending_index_task(state=state, tasks=tasks)
        while pending is not None:
            task_key, result_key = pending
            task_id = state[task_key]
            resume_payload = interrupt_fn({"kind": "task_result", "task_id": task_id})
            status = resume_payload.get("status") if isinstance(resume_payload, dict) else None
            resumed_task_id = resume_payload.get("task_id") if isinstance(resume_payload, dict) else None
            if resumed_task_id != task_id:
                raise RuntimeError(f"resume payload task_id mismatch: expected={task_id} actual={resumed_task_id}")
            tasks[result_key] = {"task_id": task_id, "status": status}
            if status != "succeeded":
                raise RuntimeError(f"task {task_id} finished with status {status}")
            pending = _pending_index_task(state=state, tasks=tasks)
        return {"tasks": tasks, "status": "index_succeeded"}

    return run


def _pending_index_task(
    *,
    state: SpecIndexingState,
    tasks: dict[str, dict[str, Any]],
) -> tuple[str, str] | None:
    for task_key, result_key in (
        ("index_dense_task_id", "index_dense"),
        ("index_sparse_task_id", "index_sparse"),
    ):
        task_id = state[task_key]
        current = tasks.get(result_key) or {}
        if current.get("task_id") != task_id or current.get("status") != "succeeded":
            return task_key, result_key
    return None


async def _finalize_node(state: SpecIndexingState) -> SpecIndexingState:
    resource_key = _resource_key(state)
    return {
        "status": "completed",
        "resource_key": resource_key,
        "vector_collection": _vector_collection(state),
        "sparse_index": _sparse_index(state),
    }


def _resource_key(state: SpecIndexingState) -> str:
    return str((state.get("params") or {}).get("resource_key") or state.get("resource_key"))


def _source(state: SpecIndexingState) -> dict[str, Any]:
    return dict((state.get("params") or {}).get("source") or {})


def _parsed_object_key(resource_key: str) -> str:
    return f"parsed/{resource_key}/text.txt"


def _embedding_dimensions(state: SpecIndexingState) -> int:
    params = state.get("params") or {}
    value = (
        state.get("embedding_dimensions")
        or params.get("embedding_dimensions")
        or os.getenv("SPEC_RAG_EMBEDDING_DIMENSIONS")
        or EMBEDDING_DIMENSIONS
    )
    return int(value)


def _vector_collection(state: SpecIndexingState) -> str:
    params = state.get("params") or {}
    return str(params.get("vector_collection") or os.getenv("SPEC_RAG_VECTOR_COLLECTION", VECTOR_COLLECTION))


def _sparse_index(state: SpecIndexingState) -> str:
    params = state.get("params") or {}
    return str(params.get("sparse_index") or os.getenv("SPEC_RAG_SPARSE_INDEX", SPARSE_INDEX))
