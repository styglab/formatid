import asyncio
from importlib import import_module
import logging
import signal
from datetime import datetime, timedelta
from typing import Any

from services.scheduler.runtime.checkpoints import build_checkpoint_store
from services.scheduler.runtime.config import get_settings
from services.scheduler.runtime.logger import configure_logging, get_logger, log_event
from shared.schedule_catalog import list_schedule_definitions
from shared.tasking.enqueue import enqueue_task
from shared.time import iso_now, now


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

    schedules = [definition for definition in list_schedule_definitions() if definition.enabled]
    next_run_at = await _load_next_run_times(
        schedules=schedules,
        checkpoint_store=checkpoint_store,
    )

    log_event(
        logger,
        logging.INFO,
        "scheduler_started",
        schedule_count=len(schedules),
        schedule_names=[definition.name for definition in schedules],
    )

    try:
        while not shutdown_event.is_set():
            current_time = now()
            for definition in schedules:
                if current_time < next_run_at[definition.name]:
                    continue
                payload = await _build_schedule_payload(
                    definition=definition,
                    checkpoint_store=checkpoint_store,
                )
                if payload is None:
                    next_run_at[definition.name] = now() + timedelta(seconds=definition.interval_seconds)
                    continue
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
                next_run_at[definition.name] = now() + timedelta(seconds=definition.interval_seconds)

            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=settings.poll_interval_seconds)
            except asyncio.TimeoutError:
                continue
    finally:
        await checkpoint_store.close()

    log_event(logger, logging.INFO, "scheduler_shutdown_completed")


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


async def _build_schedule_payload(*, definition, checkpoint_store) -> dict[str, Any] | None:
    if definition.payload_factory is None:
        return dict(definition.payload)

    payload_factory = _load_payload_factory(definition.payload_factory)
    return await payload_factory(definition=definition, checkpoint_store=checkpoint_store)


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


def main() -> None:
    asyncio.run(run_scheduler())


if __name__ == "__main__":
    main()
