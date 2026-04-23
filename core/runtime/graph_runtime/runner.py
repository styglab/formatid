from __future__ import annotations

import logging
import time
from uuid import uuid4
from collections.abc import Callable
from typing import Any

from core.runtime.app_service.runtime.core import AppServiceRuntime
from core.runtime.app_service.runtime.locks import RedisLock
from core.runtime.app_service.runtime.logger import log_event
from core.runtime.app_service.runtime.run_store import ServiceRunStore
from core.runtime.graph_runtime.context import GraphRunContext, GraphTrigger
from core.runtime.graph_runtime.registry import GraphRegistry
from core.runtime.graph_runtime.state_store import GraphRunStore
from core.runtime.runtime_db.checkpoints import PostgresCheckpointStore
from core.runtime.time import now


GraphResultDetails = Callable[[dict[str, Any]], dict[str, Any]]


async def run_registered_graph(
    *,
    registry: GraphRegistry,
    runtime: AppServiceRuntime,
    graph_name: str,
    trigger: GraphTrigger,
    checkpoint_store: PostgresCheckpointStore,
    run_store: ServiceRunStore,
    run_name: str,
    run_id: str | None = None,
    params: dict[str, Any] | None = None,
    trigger_config: dict[str, Any] | None = None,
    lock_enabled: bool = True,
    lock_ttl_seconds: int = 300,
    lock_key: str | None = None,
    result_details: GraphResultDetails | None = None,
    graph_run_store: GraphRunStore | None = None,
) -> dict[str, Any] | None:
    started_at = now()
    monotonic_started_at = time.perf_counter()
    trigger_details = trigger_config or {}
    resolved_run_id = run_id or uuid4().hex
    owned_graph_run_store = graph_run_store is None
    graph_run_store = graph_run_store or GraphRunStore(database_url=runtime.settings.checkpoint_database_url)
    try:
        if lock_enabled:
            resolved_lock_key = lock_key or f"graph_runtime:lock:{runtime.settings.app_name}:{graph_name}:{trigger}"
            async with RedisLock(
                redis_url=runtime.settings.redis_url,
                key=resolved_lock_key,
                ttl_seconds=lock_ttl_seconds,
            ) as lock:
                if not lock.acquired:
                    await _record_skipped(
                        run_store=run_store,
                        graph_run_store=graph_run_store,
                        runtime=runtime,
                        run_name=run_name,
                        graph_name=graph_name,
                        trigger=trigger,
                        run_id=resolved_run_id,
                        params=params or {},
                        trigger_config=trigger_details,
                        lock_key=resolved_lock_key,
                        started_at=started_at,
                        monotonic_started_at=monotonic_started_at,
                    )
                    return None
                return await _execute_and_record(
                    registry=registry,
                    runtime=runtime,
                    graph_name=graph_name,
                    trigger=trigger,
                    checkpoint_store=checkpoint_store,
                    run_store=run_store,
                    graph_run_store=graph_run_store,
                    run_name=run_name,
                    run_id=resolved_run_id,
                    params=params or {},
                    trigger_config=trigger_details,
                    lock_acquired=True,
                    started_at=started_at,
                    monotonic_started_at=monotonic_started_at,
                    result_details=result_details,
                )

        return await _execute_and_record(
            registry=registry,
            runtime=runtime,
            graph_name=graph_name,
            trigger=trigger,
            checkpoint_store=checkpoint_store,
            run_store=run_store,
            graph_run_store=graph_run_store,
            run_name=run_name,
            run_id=resolved_run_id,
            params=params or {},
            trigger_config=trigger_details,
            lock_acquired=None,
            started_at=started_at,
            monotonic_started_at=monotonic_started_at,
            result_details=result_details,
        )
    finally:
        if owned_graph_run_store:
            await graph_run_store.close()


async def _execute_and_record(
    *,
    registry: GraphRegistry,
    runtime: AppServiceRuntime,
    graph_name: str,
    trigger: GraphTrigger,
    checkpoint_store: PostgresCheckpointStore,
    run_store: ServiceRunStore,
    graph_run_store: GraphRunStore,
    run_name: str,
    run_id: str,
    params: dict[str, Any],
    trigger_config: dict[str, Any],
    lock_acquired: bool | None,
    started_at,
    monotonic_started_at: float,
    result_details: GraphResultDetails | None,
) -> dict[str, Any]:
    graph = registry.get(graph_name)
    context = GraphRunContext(
        graph_name=graph_name,
        trigger=trigger,
        checkpoint_store=checkpoint_store,
        run_store=run_store,
        graph_run_store=graph_run_store,
        run_id=run_id,
        params=params,
        trigger_config=trigger_config,
    )
    await graph_run_store.mark_running(
        run_id=run_id,
        service_name=runtime.settings.app_name,
        graph_name=graph_name,
        trigger_type=trigger,
        params=params,
        started_at=started_at,
    )
    log_event(
        runtime.logger,
        logging.INFO,
        "graph_runtime_graph_started",
        service_name=runtime.settings.app_name,
        graph_name=graph_name,
        trigger=trigger,
        run_id=run_id,
    )
    try:
        result = await graph.run(context)
    except Exception as exc:
        finished_at = now()
        error = {"type": type(exc).__name__, "message": str(exc)}
        await graph_run_store.mark_failed(run_id=run_id, error=error, finished_at=finished_at)
        await run_store.record(
            service_name=runtime.settings.app_name,
            run_name=run_name,
            status="failed",
            error=error,
            trigger_type=trigger,
            trigger_config=trigger_config,
            correlation_id=run_id,
            resource_key=graph_name,
            lock_acquired=lock_acquired,
            started_at=started_at,
            finished_at=finished_at,
            duration_ms=round((time.perf_counter() - monotonic_started_at) * 1000, 3),
        )
        runtime.logger.exception(
            "graph_runtime_graph_failed",
            extra={
                "extra_fields": {
                    "event": "graph_runtime_graph_failed",
                    "service_name": runtime.settings.app_name,
                    "graph_name": graph_name,
                    "trigger": trigger,
                    "run_id": run_id,
                }
            },
        )
        raise

    finished_at = now()
    details = result_details(result) if result_details is not None else _default_result_details(result)
    await graph_run_store.mark_succeeded(run_id=run_id, result=details, finished_at=finished_at)
    await run_store.record(
        service_name=runtime.settings.app_name,
        run_name=run_name,
        status="succeeded",
        details=details,
        trigger_type=trigger,
        trigger_config=trigger_config,
        correlation_id=run_id,
        resource_key=graph_name,
        lock_acquired=lock_acquired,
        started_at=started_at,
        finished_at=finished_at,
        duration_ms=round((time.perf_counter() - monotonic_started_at) * 1000, 3),
    )
    log_event(
        runtime.logger,
        logging.INFO,
        "graph_runtime_graph_completed",
        service_name=runtime.settings.app_name,
        graph_name=graph_name,
        trigger=trigger,
        run_id=run_id,
        **details,
    )
    return result


async def _record_skipped(
    *,
    run_store: ServiceRunStore,
    graph_run_store: GraphRunStore,
    runtime: AppServiceRuntime,
    run_name: str,
    graph_name: str,
    trigger: GraphTrigger,
    run_id: str,
    params: dict[str, Any],
    trigger_config: dict[str, Any],
    lock_key: str,
    started_at,
    monotonic_started_at: float,
) -> None:
    finished_at = now()
    await graph_run_store.mark_skipped(
        run_id=run_id,
        service_name=runtime.settings.app_name,
        graph_name=graph_name,
        trigger_type=trigger,
        params=params,
        result={"skip_reason": "lock_not_acquired", "lock_key": lock_key},
        started_at=started_at,
        finished_at=finished_at,
    )
    await run_store.record(
        service_name=runtime.settings.app_name,
        run_name=run_name,
        status="skipped",
        skip_reason="lock_not_acquired",
        details={"lock_key": lock_key},
        trigger_type=trigger,
        trigger_config=trigger_config,
        correlation_id=run_id,
        resource_key=graph_name,
        lock_acquired=False,
        started_at=started_at,
        finished_at=finished_at,
        duration_ms=round((time.perf_counter() - monotonic_started_at) * 1000, 3),
    )
    log_event(
        runtime.logger,
        logging.INFO,
        "graph_runtime_graph_skipped_lock_not_acquired",
        service_name=runtime.settings.app_name,
        graph_name=graph_name,
        trigger=trigger,
        run_id=run_id,
        lock_key=lock_key,
    )


def _default_result_details(result: dict[str, Any]) -> dict[str, Any]:
    return {
        "completed_nodes": result.get("completed_nodes", []),
        "status": result.get("status"),
        "skip_reason": result.get("skip_reason"),
    }
