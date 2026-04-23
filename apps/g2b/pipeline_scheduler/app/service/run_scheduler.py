from __future__ import annotations

import asyncio
import logging
import os

from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED, JobExecutionEvent
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from core.runtime.app_service.runtime.config import get_settings as get_service_settings
from core.runtime.app_service.runtime.core import AppServiceRuntime
from core.runtime.app_service.runtime.cron import env_bool, env_cron, env_int
from core.runtime.app_service.runtime.logger import get_logger, log_event
from core.runtime.app_service.runtime.run_store import ServiceRunStore
from core.runtime.graph_runtime.triggers import add_scheduled_graph_job
from core.runtime.runtime_db.checkpoints import PostgresCheckpointStore
from core.runtime.time import get_timezone

from apps.g2b.pipeline_scheduler.app.graph.registry import GRAPH_REGISTRY
from apps.g2b.pipeline_scheduler.app.service.graph_details import build_graph_result_details


logger = get_logger("g2b.pipeline_scheduler.scheduler")


async def run_pipeline_scheduler() -> None:
    service_settings = get_service_settings()
    checkpoint_store = PostgresCheckpointStore(database_url=service_settings.checkpoint_database_url)
    run_store = ServiceRunStore(database_url=service_settings.checkpoint_database_url)
    runtime = AppServiceRuntime(settings=service_settings, logger_name="g2b.pipeline_scheduler.scheduler")
    runtime.add_close_callback(checkpoint_store.close)
    runtime.add_close_callback(run_store.close)

    scheduler = _build_scheduler(
        runtime=runtime,
        checkpoint_store=checkpoint_store,
        run_store=run_store,
    )
    await runtime.start()
    try:
        scheduler.start()
        log_event(
            runtime.logger,
            logging.INFO,
            "g2b_pipeline_scheduler_started",
            service_name=runtime.settings.app_name,
            scheduled_graphs=_scheduled_graphs_summary(),
        )
        await runtime.wait_for_shutdown()
    finally:
        if scheduler.running:
            scheduler.shutdown(wait=False)
        await runtime.close()


def _build_scheduler(
    *,
    runtime: AppServiceRuntime,
    checkpoint_store: PostgresCheckpointStore,
    run_store: ServiceRunStore,
) -> AsyncIOScheduler:
    scheduler = AsyncIOScheduler(timezone=get_timezone())
    scheduler.add_listener(_log_scheduled_job_event, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
    if _ingest_scheduled_enabled():
        add_scheduled_graph_job(
            scheduler=scheduler,
            registry=GRAPH_REGISTRY,
            runtime=runtime,
            checkpoint_store=checkpoint_store,
            run_store=run_store,
            graph_name="ingest_graph",
            run_name="g2b_pipeline_ingest_graph_scheduled",
            schedule=_ingest_schedule(),
            coalesce=_ingest_schedule_coalesce(),
            max_instances=_ingest_schedule_max_instances(),
            misfire_grace_seconds=_ingest_schedule_misfire_grace_seconds(),
            lock_enabled=runtime.settings.service_lock_enabled,
            lock_ttl_seconds=runtime.settings.service_lock_ttl_seconds,
            result_details=build_graph_result_details,
        )
    return scheduler


def _log_scheduled_job_event(event: JobExecutionEvent) -> None:
    if event.exception is not None:
        log_event(
            logger,
            logging.ERROR,
            "g2b_pipeline_scheduled_job_failed",
            job_id=event.job_id,
            error=repr(event.exception),
        )
        return
    log_event(
        logger,
        logging.DEBUG,
        "g2b_pipeline_scheduled_job_succeeded",
        job_id=event.job_id,
    )


def _scheduled_graphs_summary() -> list[dict[str, object]]:
    if not _ingest_scheduled_enabled():
        return []
    return [{"graph_name": "ingest_graph", "trigger": "scheduled", "schedule": _ingest_schedule()}]


def _ingest_scheduled_enabled() -> bool:
    return env_bool("G2B_INGEST_GRAPH_SCHEDULED_ENABLED", True)


def _ingest_schedule() -> str:
    return _env_cron("G2B_INGEST_GRAPH_SCHEDULE", "G2B_INGEST_SERVICE_CRON", "* * * * *")


def _ingest_schedule_misfire_grace_seconds() -> int:
    return _env_int(
        "G2B_INGEST_GRAPH_MISFIRE_GRACE_SECONDS",
        "G2B_INGEST_SERVICE_MISFIRE_GRACE_SECONDS",
        30,
    )


def _ingest_schedule_max_instances() -> int:
    return _env_int("G2B_INGEST_GRAPH_MAX_INSTANCES", "G2B_INGEST_SERVICE_MAX_INSTANCES", 1)


def _ingest_schedule_coalesce() -> bool:
    return _env_bool("G2B_INGEST_GRAPH_COALESCE", "G2B_INGEST_SERVICE_COALESCE", True)


def _env_cron(primary: str, fallback: str, default: str) -> str:
    return os.getenv(primary) or env_cron(fallback, default)


def _env_int(primary: str, fallback: str, default: int) -> int:
    return int(os.getenv(primary) or os.getenv(fallback) or str(default))


def _env_bool(primary: str, fallback: str, default: bool) -> bool:
    raw = os.getenv(primary)
    if raw is None:
        return env_bool(fallback, default)
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def main() -> None:
    asyncio.run(run_pipeline_scheduler())


if __name__ == "__main__":
    main()
