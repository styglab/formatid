from __future__ import annotations

from typing import Any, TypedDict


class SpecIndexingState(TypedDict, total=False):
    params: dict[str, Any]
    checkpoint_database_url: str
    redis_url: str
    graph_run_id: str
    correlation_id: str
    resource_key: str
    parse_task_id: str
    chunk_task_id: str
    index_dense_task_id: str
    index_sparse_task_id: str
    embedding_dimensions: int
    tasks: dict[str, dict[str, Any]]
    vector_collection: str
    sparse_index: str
    status: str


class SpecQueryState(TypedDict, total=False):
    params: dict[str, Any]
    query: str
    retrieved_context: list[dict[str, Any]]
    answer: str
    tasks: dict[str, dict[str, Any]]
    status: str
