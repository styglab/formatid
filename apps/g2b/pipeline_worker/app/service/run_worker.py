from __future__ import annotations

import asyncio
import logging
import os

from core.runtime.app_service.runtime.config import get_settings as get_service_settings
from core.runtime.app_service.runtime.core import AppServiceRuntime
from core.runtime.app_service.runtime.cron import env_bool, env_int
from core.runtime.app_service.runtime.logger import get_logger, log_event
from core.runtime.app_service.runtime.run_store import ServiceRunStore
from core.runtime.graph_runtime import TriggeredGraphQueue
from core.runtime.graph_runtime.triggers import consume_triggered_graphs
from core.runtime.runtime_db.checkpoints import PostgresCheckpointStore

from apps.g2b.pipeline_worker.app.graph.registry import GRAPH_REGISTRY
from apps.g2b.pipeline_worker.app.service.graph_details import build_graph_result_details, build_triggered_run_name


logger = get_logger("g2b.pipeline_worker")


async def run_pipeline_worker() -> None:
    service_settings = get_service_settings()
    checkpoint_store = PostgresCheckpointStore(database_url=service_settings.checkpoint_database_url)
    run_store = ServiceRunStore(database_url=service_settings.checkpoint_database_url)
    runtime = AppServiceRuntime(settings=service_settings, logger_name="g2b.pipeline_worker")
    runtime.add_close_callback(checkpoint_store.close)
    runtime.add_close_callback(run_store.close)

    queue = TriggeredGraphQueue(
        redis_url=service_settings.redis_url,
        queue_name=_document_process_queue_name(),
    )
    runtime.add_close_callback(queue.close)

    await runtime.start()
    try:
        if not _document_process_triggered_enabled():
            log_event(
                runtime.logger,
                logging.INFO,
                "g2b_pipeline_worker_idle_triggered_disabled",
                service_name=runtime.settings.app_name,
                triggered_graphs=[],
            )
            await runtime.wait_for_shutdown()
            return

        log_event(
            runtime.logger,
            logging.INFO,
            "g2b_pipeline_worker_started",
            service_name=runtime.settings.app_name,
            triggered_graphs=_triggered_graphs_summary(),
        )
        await consume_triggered_graphs(
            registry=GRAPH_REGISTRY,
            runtime=runtime,
            queue=queue,
            checkpoint_store=checkpoint_store,
            run_store=run_store,
            max_attempts=_document_process_max_attempts(),
            run_name_builder=build_triggered_run_name,
            result_details=build_graph_result_details,
        )
    finally:
        await runtime.close()


def _triggered_graphs_summary() -> list[dict[str, object]]:
    if not _document_process_triggered_enabled():
        return []
    return [
        {
            "graph_name": "document_process_graph",
            "trigger": "triggered",
            "queue": _document_process_queue_name(),
        }
    ]


def _document_process_triggered_enabled() -> bool:
    return env_bool("G2B_DOCUMENT_PROCESS_GRAPH_TRIGGERED_ENABLED", True)


def _document_process_queue_name() -> str:
    return os.getenv("G2B_DOCUMENT_PROCESS_GRAPH_QUEUE", "g2b:pipeline:document-process")


def _document_process_max_attempts() -> int:
    return env_int("G2B_DOCUMENT_PROCESS_GRAPH_MAX_ATTEMPTS", 3)


def main() -> None:
    asyncio.run(run_pipeline_worker())


if __name__ == "__main__":
    main()
