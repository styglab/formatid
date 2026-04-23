from __future__ import annotations

import logging
import os
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED, JobExecutionEvent
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from core.runtime.app_service.runtime.core import AppServiceRuntime
from core.runtime.app_service.runtime.locks import RedisLock
from core.runtime.app_service.runtime.logger import log_event
from core.runtime.app_service.runtime.run_store import ServiceRunStore
from core.runtime.time import get_timezone


CronHandler = Callable[..., Awaitable[None]]


@dataclass(frozen=True)
class CronJob:
    name: str
    cron: str
    handler: CronHandler
    kwargs: dict[str, Any] = field(default_factory=dict)
    coalesce: bool = True
    max_instances: int = 1
    misfire_grace_seconds: int = 30
    lock_enabled: bool = True
    lock_ttl_seconds: int = 90
    lock_key: str | None = None


class CronServiceRunner:
    def __init__(self, *, runtime: AppServiceRuntime, jobs: list[CronJob]) -> None:
        self._runtime = runtime
        self._jobs = jobs
        self._scheduler: AsyncIOScheduler | None = None
        self._run_store = ServiceRunStore(database_url=runtime.settings.checkpoint_database_url)
        self._runtime.add_close_callback(self._run_store.close)

    async def run(self) -> None:
        await self._runtime.start()
        self._scheduler = self._build_scheduler()
        try:
            self._scheduler.start()
            log_event(
                self._runtime.logger,
                logging.INFO,
                "cron_service_started",
                service_name=self._runtime.settings.app_name,
                jobs=[{"name": job.name, "cron": job.cron} for job in self._jobs],
            )
            await self._runtime.wait_for_shutdown()
        finally:
            if self._scheduler is not None and self._scheduler.running:
                self._scheduler.shutdown(wait=False)
            await self._runtime.close()

    def _build_scheduler(self) -> AsyncIOScheduler:
        scheduler = AsyncIOScheduler(timezone=get_timezone())
        scheduler.add_listener(self._log_job_event, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
        for job in self._jobs:
            scheduler.add_job(
                _run_cron_job,
                trigger=CronTrigger.from_crontab(job.cron, timezone=get_timezone()),
                id=job.name,
                name=job.name,
                kwargs={"job": job, "runtime": self._runtime},
                coalesce=job.coalesce,
                max_instances=job.max_instances,
                misfire_grace_time=job.misfire_grace_seconds,
                replace_existing=True,
            )
        return scheduler

    def _log_job_event(self, event: JobExecutionEvent) -> None:
        if event.exception is not None:
            log_event(
                self._runtime.logger,
                logging.ERROR,
                "cron_job_failed",
                service_name=self._runtime.settings.app_name,
                job_id=event.job_id,
                error=repr(event.exception),
            )
            return
        log_event(
            self._runtime.logger,
            logging.DEBUG,
            "cron_job_succeeded",
            service_name=self._runtime.settings.app_name,
            job_id=event.job_id,
        )


async def _run_cron_job(*, job: CronJob, runtime: AppServiceRuntime) -> None:
    run_store = ServiceRunStore(database_url=runtime.settings.checkpoint_database_url)
    started_at = get_timezone_now()
    monotonic_started_at = time.perf_counter()
    trigger_config = {
        "cron": job.cron,
        "coalesce": job.coalesce,
        "max_instances": job.max_instances,
        "misfire_grace_seconds": job.misfire_grace_seconds,
        "lock_enabled": job.lock_enabled,
        "lock_ttl_seconds": job.lock_ttl_seconds,
    }
    try:
        if job.lock_enabled:
            lock_key = job.lock_key or f"app_service:lock:{runtime.settings.app_name}:{job.name}"
            async with RedisLock(
                redis_url=runtime.settings.redis_url,
                key=lock_key,
                ttl_seconds=job.lock_ttl_seconds,
            ) as lock:
                if not lock.acquired:
                    finished_at = get_timezone_now()
                    await run_store.record(
                        service_name=runtime.settings.app_name,
                        run_name=job.name,
                        status="skipped",
                        skip_reason="lock_not_acquired",
                        details={"lock_key": lock_key},
                        trigger_type="cron",
                        trigger_config=trigger_config,
                        lock_acquired=False,
                        started_at=started_at,
                        finished_at=finished_at,
                        duration_ms=round((time.perf_counter() - monotonic_started_at) * 1000, 3),
                    )
                    log_event(
                        runtime.logger,
                        logging.INFO,
                        "cron_job_skipped_lock_not_acquired",
                        service_name=runtime.settings.app_name,
                        job_name=job.name,
                        lock_key=lock_key,
                    )
                    return
                await _execute_and_record_cron_job(
                    job=job,
                    runtime=runtime,
                    run_store=run_store,
                    started_at=started_at,
                    monotonic_started_at=monotonic_started_at,
                    trigger_config=trigger_config,
                    lock_acquired=True,
                )
                return

        await _execute_and_record_cron_job(
            job=job,
            runtime=runtime,
            run_store=run_store,
            started_at=started_at,
            monotonic_started_at=monotonic_started_at,
            trigger_config=trigger_config,
            lock_acquired=None,
        )
    finally:
        await run_store.close()


async def _execute_and_record_cron_job(
    *,
    job: CronJob,
    runtime: AppServiceRuntime,
    run_store: ServiceRunStore,
    started_at,
    monotonic_started_at: float,
    trigger_config: dict[str, Any],
    lock_acquired: bool | None,
) -> None:
    try:
        await job.handler(**job.kwargs)
    except Exception:
        finished_at = get_timezone_now()
        await run_store.record(
            service_name=runtime.settings.app_name,
            run_name=job.name,
            status="failed",
            error={"type": "UnhandledCronJobError"},
            trigger_type="cron",
            trigger_config=trigger_config,
            lock_acquired=lock_acquired,
            started_at=started_at,
            finished_at=finished_at,
            duration_ms=round((time.perf_counter() - monotonic_started_at) * 1000, 3),
        )
        runtime.logger.exception(
            "cron_job_unhandled_error",
            extra={
                "extra_fields": {
                    "event": "cron_job_unhandled_error",
                    "service_name": runtime.settings.app_name,
                    "job_name": job.name,
                }
            },
        )
        raise
    finished_at = get_timezone_now()
    await run_store.record(
        service_name=runtime.settings.app_name,
        run_name=job.name,
        status="succeeded",
        trigger_type="cron",
        trigger_config=trigger_config,
        lock_acquired=lock_acquired,
        started_at=started_at,
        finished_at=finished_at,
        duration_ms=round((time.perf_counter() - monotonic_started_at) * 1000, 3),
    )


def get_timezone_now():
    from core.runtime.time import now

    return now()


def env_cron(name: str, default: str = "* * * * *") -> str:
    return os.getenv(name, default)


def env_bool(name: str, default: bool = True) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def env_int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))
