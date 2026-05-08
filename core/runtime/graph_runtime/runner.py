from __future__ import annotations

import logging
import time
from uuid import uuid4
from collections.abc import Callable
from typing import Any

from core.contracts.execution.identity import normalize_execution_identity
from core.runtime.app_service.runtime.core import AppServiceRuntime
from core.runtime.app_service.runtime.locks import RedisLock
from core.runtime.app_service.runtime.logger import log_event
from core.runtime.app_service.runtime.run_store import ServiceRunStore
from core.runtime.graph_runtime.checkpointer import AsyncGraphCheckpointer
from core.runtime.graph_runtime.context import GraphRunContext, GraphTrigger
from core.runtime.graph_runtime.resume import enqueue_graph_resumes_for_task
from core.runtime.graph_runtime.registry import GraphRegistry
from core.runtime.graph_runtime.state_store import GraphRunStore
from core.runtime.runtime_db.checkpoints import PostgresCheckpointStore
from core.runtime.runtime_db.connection import connect
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
    graph_checkpointer: AsyncGraphCheckpointer | None = None,
    resume_value: Any | None = None,
    resume_queue_name: str | None = None,
    request_id: str | None = None,
    correlation_id: str | None = None,
    resource_key: str | None = None,
    session_id: str | None = None,
) -> dict[str, Any] | None:
    started_at = now()
    monotonic_started_at = time.perf_counter()
    trigger_details = trigger_config or {}
    resolved_run_id = run_id or uuid4().hex
    resolved_params = dict(params or {})
    runtime_params = dict(resolved_params.get("__runtime") or {})
    identity = _resolve_execution_identity(
        run_id=resolved_run_id,
        graph_name=graph_name,
        runtime_params=runtime_params,
        request_id=request_id,
        correlation_id=correlation_id,
        resource_key=resource_key,
        session_id=session_id,
    )
    runtime_params["identity"] = identity
    if resume_queue_name:
        runtime_params["resume_queue"] = resume_queue_name
    resolved_params["__runtime"] = runtime_params
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
                        params=resolved_params,
                        trigger_config=trigger_details,
                        identity=identity,
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
                    params=resolved_params,
                    trigger_config=trigger_details,
                    identity=identity,
                    lock_acquired=True,
                    started_at=started_at,
                    monotonic_started_at=monotonic_started_at,
                    result_details=result_details,
                    graph_checkpointer=graph_checkpointer,
                    resume_value=resume_value,
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
            params=resolved_params,
            trigger_config=trigger_details,
            identity=identity,
            lock_acquired=None,
            started_at=started_at,
            monotonic_started_at=monotonic_started_at,
            result_details=result_details,
            graph_checkpointer=graph_checkpointer,
            resume_value=resume_value,
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
    identity: dict[str, str],
    lock_acquired: bool | None,
    started_at,
    monotonic_started_at: float,
    result_details: GraphResultDetails | None,
    graph_checkpointer: AsyncGraphCheckpointer | None,
    resume_value: Any | None,
) -> dict[str, Any]:
    graph = registry.get(graph_name)
    if resume_value is not None:
        existing_run = await graph_run_store.get_run(run_id)
        if existing_run is not None and existing_run.get("status") in {"succeeded", "failed"}:
            result = existing_run.get("result")
            return dict(result) if isinstance(result, dict) else {}
    context = GraphRunContext(
        graph_name=graph_name,
        trigger=trigger,
        checkpoint_store=checkpoint_store,
        run_store=run_store,
        graph_run_store=graph_run_store,
        run_id=run_id,
        thread_id=run_id,
        request_id=identity.get("request_id"),
        correlation_id=identity.get("correlation_id"),
        resource_key=identity.get("resource_key"),
        session_id=identity.get("session_id"),
        resume_value=resume_value,
        graph_checkpointer=graph_checkpointer.get() if graph_checkpointer is not None else None,
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
        **identity,
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
            correlation_id=identity.get("correlation_id"),
            resource_key=identity.get("resource_key"),
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
                    **identity,
                }
            },
        )
        raise

    if _is_interrupted(result):
        details = _interrupt_details(result)
        await graph_run_store.mark_suspended(run_id=run_id, result=details)
        await _enqueue_resume_if_task_already_finished(
            runtime=runtime,
            graph_run_store=graph_run_store,
            details=details,
        )
        await run_store.record(
            service_name=runtime.settings.app_name,
            run_name=run_name,
            status="suspended",
            details=details,
            trigger_type=trigger,
            trigger_config=trigger_config,
            correlation_id=identity.get("correlation_id"),
            resource_key=identity.get("resource_key"),
            lock_acquired=lock_acquired,
            started_at=started_at,
            finished_at=None,
            duration_ms=round((time.perf_counter() - monotonic_started_at) * 1000, 3),
        )
        log_event(
            runtime.logger,
            logging.INFO,
            "graph_runtime_graph_suspended",
            service_name=runtime.settings.app_name,
            graph_name=graph_name,
            trigger=trigger,
            **identity,
            **details,
        )
        return result

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
        correlation_id=identity.get("correlation_id"),
        resource_key=identity.get("resource_key"),
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
        **identity,
        **details,
    )
    return result


def _is_interrupted(result: dict[str, Any]) -> bool:
    return bool(result.get("__interrupt__"))


def _interrupt_details(result: dict[str, Any]) -> dict[str, Any]:
    interrupt = result["__interrupt__"][0]
    value = getattr(interrupt, "value", None)
    if isinstance(value, dict):
        return {"interrupt": value}
    if isinstance(interrupt, dict) and isinstance(interrupt.get("value"), dict):
        return {"interrupt": dict(interrupt["value"])}
    return {"interrupt": {"value": str(value if value is not None else interrupt)}}


async def _enqueue_resume_if_task_already_finished(
    *,
    runtime: AppServiceRuntime,
    graph_run_store: GraphRunStore,
    details: dict[str, Any],
) -> None:
    interrupt = details.get("interrupt")
    if not isinstance(interrupt, dict):
        return
    task_id = interrupt.get("task_id")
    if not isinstance(task_id, str) or not task_id:
        return
    status = await _get_terminal_task_status(database_url=runtime.settings.checkpoint_database_url, task_id=task_id)
    if status is None:
        return
    resume_count = await enqueue_graph_resumes_for_task(
        redis_url=runtime.settings.redis_url,
        graph_run_store=graph_run_store,
        task_id=task_id,
        resume_value={"task_id": task_id, "status": status},
        requested_by=f"graph-runtime:{runtime.settings.app_name}",
    )
    if resume_count:
        log_event(
            runtime.logger,
            logging.INFO,
            "graph_runtime_resume_enqueued_for_finished_task",
            service_name=runtime.settings.app_name,
            task_id=task_id,
            status=status,
            resume_count=resume_count,
        )


async def _get_terminal_task_status(*, database_url: str, task_id: str) -> str | None:
    conn = await connect(database_url)
    try:
        async with conn.cursor() as cursor:
            await cursor.execute(
                """
                SELECT status
                FROM task_executions
                WHERE task_id = %s
                  AND status IN ('succeeded', 'failed', 'interrupted', 'dead_lettered')
                """,
                (task_id,),
            )
            row = await cursor.fetchone()
    finally:
        await conn.close()
    if row is None:
        return None
    return str(row[0])


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
    identity: dict[str, str],
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
        correlation_id=identity.get("correlation_id"),
        resource_key=identity.get("resource_key"),
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
        **identity,
        lock_key=lock_key,
    )


def _resolve_execution_identity(
    *,
    run_id: str,
    graph_name: str,
    runtime_params: dict[str, Any],
    request_id: str | None,
    correlation_id: str | None,
    resource_key: str | None,
    session_id: str | None,
) -> dict[str, str]:
    runtime_identity = runtime_params.get("identity")
    if not isinstance(runtime_identity, dict):
        runtime_identity = {}
    return normalize_execution_identity(
        runtime_identity,
        request_id=request_id,
        correlation_id=correlation_id or runtime_identity.get("correlation_id") or run_id,
        run_id=run_id,
        thread_id=run_id,
        resource_key=resource_key or runtime_identity.get("resource_key") or graph_name,
        session_id=session_id or runtime_identity.get("session_id"),
    )


def _default_result_details(result: dict[str, Any]) -> dict[str, Any]:
    return {
        "completed_nodes": result.get("completed_nodes", []),
        "status": result.get("status"),
        "skip_reason": result.get("skip_reason"),
    }
