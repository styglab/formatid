import asyncio
from importlib import import_module
import logging
import os
import signal
import socket
from datetime import datetime, timedelta
from typing import Any

from services.scheduler.runtime.checkpoints import build_checkpoint_store
from services.scheduler.runtime.config import get_settings
from services.scheduler.runtime.locks import SchedulerLockStore
from services.scheduler.runtime.logger import configure_logging, get_logger, log_event
from services.scheduler.runtime.run_store import ScheduleRunStore
from shared.schedule_catalog import list_schedule_definitions
from shared.scheduler_health.store import SchedulerHeartbeatStore
from shared.tasking.enqueue import enqueue_task
from shared.time import get_timezone, iso_now, now


logger = get_logger("scheduler.main")


def _install_signal_handlers(*, shutdown_event: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()

    def request_shutdown(signal_name: str) -> None:
        if shutdown_event.is_set():
            return
        log_event(logger, logging.INFO, "scheduler_shutdown_requested", signal=signal_name)
        shutdown_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, request_shutdown, sig.name)
        except NotImplementedError:
            pass


async def run_scheduler() -> None:
    settings = get_settings()
    configure_logging(level=settings.log_level)
    shutdown_event = asyncio.Event()
    _install_signal_handlers(shutdown_event=shutdown_event)

    checkpoint_store = build_checkpoint_store()
    schedule_run_store = ScheduleRunStore(database_url=settings.checkpoint_database_url)
    lock_store = SchedulerLockStore(redis_url=settings.redis_url) if settings.schedule_lock_enabled else None
    lock_owner = _lock_owner()
    heartbeat_store = SchedulerHeartbeatStore(
        redis_url=settings.redis_url,
        ttl_seconds=settings.scheduler_heartbeat_ttl,
    )
    heartbeat_task = asyncio.create_task(
        _run_heartbeat_loop(
            store=heartbeat_store,
            scheduler_id=lock_owner,
            shutdown_event=shutdown_event,
        )
    )

    schedules = [definition for definition in list_schedule_definitions() if definition.enabled]
    next_run_at = await _load_next_run_times(
        schedules=schedules,
        checkpoint_store=checkpoint_store,
    )

    job_lock = asyncio.Lock()
    scheduler = _build_apscheduler()
    for definition in schedules:
        scheduler.add_job(
            _run_schedule_job,
            trigger="interval",
            seconds=definition.interval_seconds,
            id=definition.name,
            name=definition.name,
            args=[
                definition,
                checkpoint_store,
                schedule_run_store,
                lock_store,
                lock_owner,
                job_lock,
            ],
            next_run_time=next_run_at[definition.name],
            misfire_grace_time=definition.misfire_grace_seconds,
            coalesce=definition.coalesce,
            max_instances=definition.max_instances,
            replace_existing=True,
        )

    scheduler.start()
    log_event(
        logger,
        logging.INFO,
        "scheduler_started",
        schedule_count=len(schedules),
        schedule_names=[definition.name for definition in schedules],
        scheduler_engine="apscheduler",
    )

    try:
        await shutdown_event.wait()
    finally:
        shutdown_event.set()
        scheduler.shutdown(wait=False)
        heartbeat_task.cancel()
        try:
            await heartbeat_task
        except asyncio.CancelledError:
            pass
        await checkpoint_store.close()
        await schedule_run_store.close()
        await heartbeat_store.close()
        if lock_store is not None:
            await lock_store.close()

    log_event(logger, logging.INFO, "scheduler_shutdown_completed")


async def _run_schedule_job(
    definition,
    checkpoint_store,
    schedule_run_store: ScheduleRunStore,
    lock_store: SchedulerLockStore | None,
    lock_owner: str,
    job_lock: asyncio.Lock,
) -> None:
    async with job_lock:
        await _run_schedule_job_locked(
            definition,
            checkpoint_store,
            schedule_run_store,
            lock_store,
            lock_owner,
        )


async def _run_schedule_job_locked(
    definition,
    checkpoint_store,
    schedule_run_store: ScheduleRunStore,
    lock_store: SchedulerLockStore | None,
    lock_owner: str,
) -> None:
    settings = get_settings()
    lock_acquired = False

    try:
        if lock_store is not None and definition.lock_enabled:
            lock_ttl_seconds = _lock_ttl_seconds(
                definition=definition,
                ttl_buffer_seconds=settings.schedule_lock_ttl_buffer_seconds,
            )
            lock_acquired = await lock_store.acquire(
                schedule_name=definition.name,
                owner=lock_owner,
                ttl_seconds=lock_ttl_seconds,
            )
            if not lock_acquired:
                log_event(
                    logger,
                    logging.INFO,
                    "schedule_lock_skipped",
                    schedule_name=definition.name,
                    lock_ttl_seconds=lock_ttl_seconds,
                )
                await _record_schedule_run(
                    store=schedule_run_store,
                    schedule_name=definition.name,
                    queue_name=definition.queue_name,
                    task_name=definition.task_name,
                    status="skipped",
                    skip_reason="lock_not_acquired",
                    details={"lock_ttl_seconds": lock_ttl_seconds},
                )
                return

        due = await _is_schedule_due(definition=definition, checkpoint_store=checkpoint_store)
        if not due:
            await _record_schedule_run(
                store=schedule_run_store,
                schedule_name=definition.name,
                queue_name=definition.queue_name,
                task_name=definition.task_name,
                status="skipped",
                skip_reason="checkpoint_not_due",
            )
            return

        payload = await _build_schedule_payload(
            definition=definition,
            checkpoint_store=checkpoint_store,
        )
        if payload is None:
            await _record_schedule_run(
                store=schedule_run_store,
                schedule_name=definition.name,
                queue_name=definition.queue_name,
                task_name=definition.task_name,
                status="skipped",
                skip_reason="payload_factory_returned_none",
            )
            return

        message = await enqueue_task(
            redis_url=settings.redis_url,
            queue_name=definition.queue_name,
            task_name=definition.task_name,
            payload=payload,
            status_ttl=settings.task_status_ttl,
        )
        await checkpoint_store.set(
            _checkpoint_name(definition.name),
            {
                "last_enqueued_at": iso_now(),
                "last_task_id": message.task_id,
                "queue_name": definition.queue_name,
                "task_name": definition.task_name,
                "payload": payload,
            },
        )
        await _record_schedule_run(
            store=schedule_run_store,
            schedule_name=definition.name,
            queue_name=definition.queue_name,
            task_name=definition.task_name,
            task_id=message.task_id,
            status="enqueued",
            payload=payload,
        )
        log_event(
            logger,
            logging.INFO,
            "schedule_enqueued",
            schedule_name=definition.name,
            queue_name=definition.queue_name,
            task_name=definition.task_name,
            task_id=message.task_id,
            payload=payload,
        )
    except Exception as exc:
        await _record_schedule_run(
            store=schedule_run_store,
            schedule_name=definition.name,
            queue_name=definition.queue_name,
            task_name=definition.task_name,
            status="failed",
            skip_reason="exception",
            details={"error_type": type(exc).__name__, "error_message": str(exc)},
        )
        log_event(
            logger,
            logging.ERROR,
            "schedule_failed",
            schedule_name=definition.name,
            queue_name=definition.queue_name,
            task_name=definition.task_name,
            error_type=type(exc).__name__,
            error_message=str(exc),
        )
    finally:
        if lock_store is not None and lock_acquired:
            await lock_store.release(schedule_name=definition.name, owner=lock_owner)


def _build_apscheduler():
    from apscheduler.schedulers.asyncio import AsyncIOScheduler

    return AsyncIOScheduler(timezone=get_timezone())


async def _load_next_run_times(*, schedules, checkpoint_store) -> dict[str, datetime]:
    current_time = now()
    next_run_at: dict[str, datetime] = {}

    for definition in schedules:
        checkpoint = await checkpoint_store.get(_checkpoint_name(definition.name))
        if checkpoint is None:
            next_run_at[definition.name] = (
                current_time if definition.run_immediately else current_time + timedelta(seconds=definition.interval_seconds)
            )
            continue

        last_enqueued_at_raw = checkpoint.get("value", {}).get("last_enqueued_at")
        if not isinstance(last_enqueued_at_raw, str):
            next_run_at[definition.name] = current_time
            continue

        due_at = datetime.fromisoformat(last_enqueued_at_raw) + timedelta(seconds=definition.interval_seconds)
        next_run_at[definition.name] = current_time if due_at <= current_time else due_at

    return next_run_at


async def _is_schedule_due(*, definition, checkpoint_store) -> bool:
    checkpoint = await checkpoint_store.get(_checkpoint_name(definition.name))
    if checkpoint is None:
        return True

    last_enqueued_at_raw = checkpoint.get("value", {}).get("last_enqueued_at")
    if not isinstance(last_enqueued_at_raw, str):
        return True

    due_at = datetime.fromisoformat(last_enqueued_at_raw) + timedelta(seconds=definition.interval_seconds)
    return due_at <= now()


async def _build_schedule_payload(*, definition, checkpoint_store) -> dict[str, Any] | None:
    if definition.payload_factory is None:
        return dict(definition.payload)

    payload_factory = _load_payload_factory(definition.payload_factory)
    return await payload_factory(definition=definition, checkpoint_store=checkpoint_store)


async def _record_schedule_run(
    *,
    store: ScheduleRunStore,
    schedule_name: str,
    status: str,
    queue_name: str | None = None,
    task_name: str | None = None,
    task_id: str | None = None,
    payload: dict[str, Any] | None = None,
    skip_reason: str | None = None,
    details: dict[str, Any] | None = None,
) -> None:
    try:
        await store.record(
            schedule_name=schedule_name,
            status=status,
            queue_name=queue_name,
            task_name=task_name,
            task_id=task_id,
            payload=payload,
            skip_reason=skip_reason,
            details=details,
        )
    except Exception as exc:
        log_event(
            logger,
            logging.WARNING,
            "schedule_run_record_failed",
            schedule_name=schedule_name,
            status=status,
            error_type=type(exc).__name__,
            error_message=str(exc),
        )


async def _run_heartbeat_loop(
    *,
    store: SchedulerHeartbeatStore,
    scheduler_id: str,
    shutdown_event: asyncio.Event,
) -> None:
    settings = get_settings()
    while not shutdown_event.is_set():
        await store.publish(scheduler_id=scheduler_id, app_name=settings.app_name)
        log_event(
            logger,
            logging.DEBUG,
            "scheduler_heartbeat_published",
            scheduler_id=scheduler_id,
            app_name=settings.app_name,
        )
        try:
            await asyncio.wait_for(
                shutdown_event.wait(),
                timeout=settings.scheduler_heartbeat_interval,
            )
        except asyncio.TimeoutError:
            continue


def _load_payload_factory(factory_path: str):
    module_path, _, attr_name = factory_path.rpartition(".")
    if not module_path or not attr_name:
        raise RuntimeError(f"invalid payload_factory path: {factory_path}")
    module = import_module(module_path)
    factory = getattr(module, attr_name)
    if not callable(factory):
        raise RuntimeError(f"payload_factory is not callable: {factory_path}")
    return factory


def _checkpoint_name(schedule_name: str) -> str:
    return f"schedule:{schedule_name}"


def _lock_ttl_seconds(*, definition, ttl_buffer_seconds: int) -> int:
    if definition.lock_ttl_seconds is not None:
        return int(definition.lock_ttl_seconds)
    return max(1, int(definition.interval_seconds) + ttl_buffer_seconds)


def _lock_owner() -> str:
    return f"{socket.gethostname()}:{os.getpid()}"


def main() -> None:
    asyncio.run(run_scheduler())


if __name__ == "__main__":
    main()
