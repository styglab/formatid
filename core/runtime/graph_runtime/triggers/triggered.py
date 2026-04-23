from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable

from core.runtime.app_service.runtime.core import AppServiceRuntime
from core.runtime.app_service.runtime.logger import log_event
from core.runtime.app_service.runtime.run_store import ServiceRunStore
from core.runtime.graph_runtime.queue import TriggeredGraphQueue, TriggeredGraphRequest
from core.runtime.graph_runtime.registry import GraphRegistry
from core.runtime.graph_runtime.runner import GraphResultDetails, run_registered_graph
from core.runtime.runtime_db.checkpoints import PostgresCheckpointStore


RunNameBuilder = Callable[[TriggeredGraphRequest], str]


async def consume_triggered_graphs(
    *,
    registry: GraphRegistry,
    runtime: AppServiceRuntime,
    queue: TriggeredGraphQueue,
    checkpoint_store: PostgresCheckpointStore,
    run_store: ServiceRunStore,
    max_attempts: int = 3,
    run_name_builder: RunNameBuilder | None = None,
    result_details: GraphResultDetails | None = None,
) -> None:
    await queue.start()
    log_event(
        runtime.logger,
        logging.INFO,
        "graph_runtime_triggered_consumer_started",
        service_name=runtime.settings.app_name,
        queue_name=queue.queue_name,
    )
    while not runtime.shutdown_event.is_set():
        request = await queue.get()
        if request is None:
            continue
        await _run_triggered_request(
            registry=registry,
            runtime=runtime,
            queue=queue,
            checkpoint_store=checkpoint_store,
            run_store=run_store,
            request=request,
            max_attempts=max_attempts,
            run_name_builder=run_name_builder,
            result_details=result_details,
        )


async def _run_triggered_request(
    *,
    registry: GraphRegistry,
    runtime: AppServiceRuntime,
    queue: TriggeredGraphQueue,
    checkpoint_store: PostgresCheckpointStore,
    run_store: ServiceRunStore,
    request: TriggeredGraphRequest,
    max_attempts: int,
    run_name_builder: RunNameBuilder | None,
    result_details: GraphResultDetails | None,
) -> None:
    try:
        result = await run_registered_graph(
            registry=registry,
            runtime=runtime,
            graph_name=request.graph_name,
            trigger="triggered",
            checkpoint_store=checkpoint_store,
            run_store=run_store,
            run_name=_run_name(request=request, run_name_builder=run_name_builder),
            run_id=request.run_id,
            params=request.params,
            trigger_config={
                "queue": queue.queue_name,
                "attempts": request.attempts,
                "requested_by": request.requested_by,
                "requested_at": request.requested_at,
                "lock_enabled": runtime.settings.service_lock_enabled,
                "lock_ttl_seconds": runtime.settings.service_lock_ttl_seconds,
            },
            lock_enabled=runtime.settings.service_lock_enabled,
            lock_ttl_seconds=runtime.settings.service_lock_ttl_seconds,
            result_details=result_details,
        )
        if result is None:
            await asyncio.sleep(1)
            await queue.requeue(request)
    except Exception as exc:
        if request.attempts + 1 < max_attempts:
            await queue.requeue(request.next_attempt())
            return
        await queue.push_dlq(
            request,
            error={"type": type(exc).__name__, "message": str(exc)},
        )


def _run_name(*, request: TriggeredGraphRequest, run_name_builder: RunNameBuilder | None) -> str:
    if run_name_builder is not None:
        return run_name_builder(request)
    return f"{request.graph_name}_triggered"
