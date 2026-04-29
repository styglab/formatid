from __future__ import annotations

import asyncio
import os

from apps.spec_rag.workflow.app.graph.registry import GRAPH_REGISTRY
from core.runtime.app_service.runtime.config import get_settings
from core.runtime.app_service.runtime.core import AppServiceRuntime
from core.runtime.app_service.runtime.run_store import ServiceRunStore
from core.runtime.graph_runtime.checkpointer import AsyncGraphCheckpointer
from core.runtime.graph_runtime.queue import TriggeredGraphQueue
from core.runtime.graph_runtime.triggers.triggered import consume_triggered_graphs
from core.runtime.runtime_db.checkpoints import PostgresCheckpointStore


async def main_async() -> None:
    settings = get_settings()
    runtime = AppServiceRuntime(settings=settings, logger_name="spec_rag.workflow")
    checkpoint_store = PostgresCheckpointStore(database_url=settings.checkpoint_database_url)
    run_store = ServiceRunStore(database_url=settings.checkpoint_database_url)
    graph_checkpointer = AsyncGraphCheckpointer(
        database_url=settings.checkpoint_database_url,
        schema_name=os.getenv("LANGGRAPH_CHECKPOINT_SCHEMA", "langgraph_checkpoints"),
    )
    queue = TriggeredGraphQueue(redis_url=settings.redis_url, queue_name=_graph_queue_name())
    runtime.add_close_callback(queue.close)
    runtime.add_close_callback(graph_checkpointer.close)
    runtime.add_close_callback(run_store.close)
    runtime.add_close_callback(checkpoint_store.close)

    await runtime.start()
    await graph_checkpointer.start()
    try:
        await consume_triggered_graphs(
            registry=GRAPH_REGISTRY,
            runtime=runtime,
            queue=queue,
            checkpoint_store=checkpoint_store,
            run_store=run_store,
            graph_checkpointer=graph_checkpointer,
            resume_queue_name=_graph_queue_name(),
            max_attempts=_max_attempts(),
            result_details=_result_details,
        )
    finally:
        await runtime.close()


def _result_details(result: dict) -> dict:
    return {
        "status": result.get("status"),
        "resource_key": result.get("resource_key"),
        "answer": result.get("answer"),
        "vector_collection": result.get("vector_collection"),
        "sparse_index": result.get("sparse_index"),
        "tasks": result.get("tasks", {}),
    }


def _graph_queue_name() -> str:
    return os.getenv("SPEC_RAG_INDEX_QUEUE", "spec-rag:index")


def _max_attempts() -> int:
    return int(os.getenv("SPEC_RAG_INDEX_MAX_ATTEMPTS", "3"))


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
