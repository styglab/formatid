from __future__ import annotations

from typing import Any

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from core.runtime.app_service.runtime.core import AppServiceRuntime
from core.runtime.app_service.runtime.run_store import ServiceRunStore
from core.runtime.graph_runtime.checkpointer import AsyncGraphCheckpointer
from core.runtime.graph_runtime.registry import GraphRegistry
from core.runtime.graph_runtime.runner import GraphResultDetails, run_registered_graph
from core.runtime.runtime_db.checkpoints import PostgresCheckpointStore
from core.runtime.time import get_timezone


def add_scheduled_graph_job(
    *,
    scheduler: AsyncIOScheduler,
    registry: GraphRegistry,
    runtime: AppServiceRuntime,
    checkpoint_store: PostgresCheckpointStore,
    run_store: ServiceRunStore,
    graph_checkpointer: AsyncGraphCheckpointer | None,
    resume_queue_name: str | None,
    graph_name: str,
    run_name: str,
    schedule: str,
    coalesce: bool = True,
    max_instances: int = 1,
    misfire_grace_seconds: int = 30,
    lock_enabled: bool = True,
    lock_ttl_seconds: int = 300,
    result_details: GraphResultDetails | None = None,
    trigger_config: dict[str, Any] | None = None,
) -> None:
    scheduler.add_job(
        _run_scheduled_graph,
        trigger=CronTrigger.from_crontab(schedule, timezone=get_timezone()),
        id=run_name,
        name=run_name,
        kwargs={
            "registry": registry,
            "runtime": runtime,
            "checkpoint_store": checkpoint_store,
            "run_store": run_store,
            "graph_checkpointer": graph_checkpointer,
            "resume_queue_name": resume_queue_name,
            "graph_name": graph_name,
            "run_name": run_name,
            "trigger_config": {
                "schedule": schedule,
                "coalesce": coalesce,
                "max_instances": max_instances,
                "misfire_grace_seconds": misfire_grace_seconds,
                "lock_enabled": lock_enabled,
                "lock_ttl_seconds": lock_ttl_seconds,
                **(trigger_config or {}),
            },
            "lock_enabled": lock_enabled,
            "lock_ttl_seconds": lock_ttl_seconds,
            "result_details": result_details,
        },
        coalesce=coalesce,
        max_instances=max_instances,
        misfire_grace_time=misfire_grace_seconds,
        replace_existing=True,
    )


async def _run_scheduled_graph(
    *,
    registry: GraphRegistry,
    runtime: AppServiceRuntime,
    checkpoint_store: PostgresCheckpointStore,
    run_store: ServiceRunStore,
    graph_checkpointer: AsyncGraphCheckpointer | None,
    resume_queue_name: str | None,
    graph_name: str,
    run_name: str,
    trigger_config: dict[str, Any],
    lock_enabled: bool,
    lock_ttl_seconds: int,
    result_details: GraphResultDetails | None,
) -> None:
    await run_registered_graph(
        registry=registry,
        runtime=runtime,
        graph_name=graph_name,
        trigger="scheduled",
        checkpoint_store=checkpoint_store,
        run_store=run_store,
        graph_checkpointer=graph_checkpointer,
        resume_queue_name=resume_queue_name,
        run_name=run_name,
        trigger_config=trigger_config,
        lock_enabled=lock_enabled,
        lock_ttl_seconds=lock_ttl_seconds,
        result_details=result_details,
    )
