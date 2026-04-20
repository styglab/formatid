import argparse
import asyncio
import logging
import os
import signal
import sys
import traceback
from pathlib import Path
from socket import gethostname

def find_project_root() -> Path:
    current = Path(__file__).resolve()
    for parent in current.parents:
        if (parent / "domains").exists() and (parent / "shared").exists():
            return parent
    raise RuntimeError("project root not found")


PROJECT_ROOT = find_project_root()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.worker.runtime.config import get_settings
from services.worker.runtime.executor import execute_task
from services.worker.runtime.logger import configure_logging, get_logger, log_event
from services.worker.runtime.queue import RedisTaskQueue, TaskQueue
from services.worker.runtime.retry_policy import (
    TaskPolicy,
    build_task_policy,
    decide_failure_action,
)
from services.worker.runtime.task_loader import load_task_modules
from shared.tasking.catalog import list_task_names_for_queue
from shared.tasking.errors import (
    WorkerTaskNotAllowedError,
)
from shared.postgres_url import get_checkpoint_database_url
from shared.tasking.execution_store import PostgresTaskExecutionStore
from shared.tasking.registry import registry
from shared.tasking.routing import validate_task_route
from shared.tasking.status_store import TaskStatusStore
from shared.tasking.validation import validate_task_payload
from shared.worker_health.store import WorkerHeartbeatStore

logger = get_logger("worker.main")


def build_queue() -> TaskQueue:
    settings = get_settings()
    return RedisTaskQueue(
        redis_url=settings.redis_url,
        queue_name=settings.worker_queue_name,
    )


def build_heartbeat_store() -> WorkerHeartbeatStore:
    settings = get_settings()
    return WorkerHeartbeatStore(
        redis_url=settings.redis_url,
        ttl_seconds=settings.worker_heartbeat_ttl,
    )


def build_status_store() -> TaskStatusStore:
    settings = get_settings()
    return TaskStatusStore(
        redis_url=settings.redis_url,
        ttl_seconds=settings.task_status_ttl,
    )


def build_execution_store() -> PostgresTaskExecutionStore:
    return PostgresTaskExecutionStore(database_url=get_checkpoint_database_url(host_default="postgres"))


def build_worker_id(*, queue_name: str) -> str:
    return f"{queue_name}:{gethostname()}:{os.getpid()}"


def build_retry_queue(*, queue_name: str) -> RedisTaskQueue:
    settings = get_settings()
    return RedisTaskQueue(redis_url=settings.redis_url, queue_name=queue_name)


def build_dlq_queue_name(*, queue_name: str) -> str:
    settings = get_settings()
    return f"{queue_name}:{settings.task_dlq_suffix}"


def get_task_policy(task_name: str) -> TaskPolicy:
    settings = get_settings()
    return build_task_policy(
        task_name=task_name,
        default_max_retries=settings.task_max_retries,
        default_backoff_seconds=settings.task_retry_delay_seconds,
        default_timeout_seconds=settings.task_timeout_seconds,
    )


async def requeue_message(*, message, queue_name: str) -> None:
    queue = build_retry_queue(queue_name=queue_name)
    try:
        await queue.put(message)
    finally:
        await queue.close()


async def record_execution_history(
    *,
    execution_store: PostgresTaskExecutionStore,
    status_document: dict,
    worker_id: str,
) -> None:
    try:
        await execution_store.upsert(status_document)
    except Exception as exc:
        log_event(
            logger,
            logging.WARNING,
            "task_execution_history_record_failed",
            worker_id=worker_id,
            task_id=status_document.get("task_id"),
            task_name=status_document.get("task_name"),
            status=status_document.get("status"),
            error_type=type(exc).__name__,
            error_message=str(exc),
        )


async def run_heartbeat_loop(*, store: WorkerHeartbeatStore, shutdown_event: asyncio.Event) -> None:
    settings = get_settings()
    worker_id = build_worker_id(queue_name=settings.worker_queue_name)

    while not shutdown_event.is_set():
        await store.publish(
            queue_name=settings.worker_queue_name,
            app_name=settings.app_name,
            worker_id=worker_id,
        )
        log_event(
            logger,
            logging.DEBUG,
            "worker_heartbeat_published",
            worker_id=worker_id,
            queue_name=settings.worker_queue_name,
            app_name=settings.app_name,
        )
        try:
            await asyncio.wait_for(
                shutdown_event.wait(),
                timeout=settings.worker_heartbeat_interval,
            )
        except asyncio.TimeoutError:
            continue


def _build_error_payload(exc: BaseException) -> dict[str, str]:
    return {
        "type": type(exc).__name__,
        "message": str(exc),
        "traceback": traceback.format_exc(),
    }


def _install_signal_handlers(
    *,
    shutdown_event: asyncio.Event,
    worker_id: str,
    queue_name: str,
) -> None:
    loop = asyncio.get_running_loop()

    def request_shutdown(signal_name: str) -> None:
        if shutdown_event.is_set():
            return
        log_event(
            logger,
            logging.INFO,
            "worker_shutdown_requested",
            worker_id=worker_id,
            queue_name=queue_name,
            signal=signal_name,
        )
        shutdown_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, request_shutdown, sig.name)
        except NotImplementedError:
            pass


async def _wait_for_next_message(
    *,
    queue: TaskQueue,
    timeout: int,
    shutdown_event: asyncio.Event,
) -> tuple[object | None, bool]:
    queue_task = asyncio.create_task(queue.get(timeout=timeout))
    shutdown_task = asyncio.create_task(shutdown_event.wait())
    done, pending = await asyncio.wait(
        {queue_task, shutdown_task},
        return_when=asyncio.FIRST_COMPLETED,
    )

    for task in pending:
        task.cancel()
    for task in pending:
        try:
            await task
        except asyncio.CancelledError:
            pass

    if shutdown_task in done:
        return None, True
    return queue_task.result(), False


async def _process_message(
    *,
    message,
    worker_id: str,
    allowed_task_names: set[str],
    status_store: TaskStatusStore,
    execution_store: PostgresTaskExecutionStore,
    raise_on_error: bool,
) -> None:
    try:
        log_event(
            logger,
            logging.INFO,
            "task_received",
            worker_id=worker_id,
            task_id=message.task_id,
            queue_name=message.queue_name,
            task_name=message.task_name,
            attempts=message.attempts,
        )
        validate_task_route(
            queue_name=message.queue_name,
            task_name=message.task_name,
        )
        if message.task_name not in allowed_task_names:
            raise WorkerTaskNotAllowedError(
                f"task_name={message.task_name} is not allowed for worker queue={get_settings().worker_queue_name}"
            )
        message.payload = validate_task_payload(task_name=message.task_name, payload=message.payload)
        task_policy = get_task_policy(message.task_name)
        status_document = await status_store.mark_running(
            message,
            worker_id=worker_id,
            policy_snapshot=task_policy.to_snapshot(),
        )
        await record_execution_history(
            execution_store=execution_store,
            status_document=status_document,
            worker_id=worker_id,
        )
        result = await asyncio.wait_for(execute_task(message), timeout=task_policy.timeout_seconds)
        status_document = await status_store.mark_succeeded(
            message,
            result,
            worker_id=worker_id,
        )
        await record_execution_history(
            execution_store=execution_store,
            status_document=status_document,
            worker_id=worker_id,
        )
    except asyncio.CancelledError as exc:
        task_policy = get_task_policy(message.task_name)
        error_payload = _build_error_payload(exc)
        log_event(
            logger,
            logging.ERROR,
            "task_interrupted",
            worker_id=worker_id,
            task_id=message.task_id,
            queue_name=message.queue_name,
            task_name=message.task_name,
            attempts=message.attempts,
            error_type=error_payload["type"],
            error_message="task interrupted during worker shutdown",
        )
        status_document = await status_store.mark_interrupted(
            message,
            worker_id=worker_id,
            policy_snapshot=task_policy.to_snapshot(),
            error=error_payload,
        )
        await record_execution_history(
            execution_store=execution_store,
            status_document=status_document,
            worker_id=worker_id,
        )
        return
    except Exception as exc:
        task_policy = get_task_policy(message.task_name)
        error_payload = _build_error_payload(exc)
        decision = decide_failure_action(attempts=message.attempts, policy=task_policy, exc=exc)
        policy_snapshot = task_policy.to_snapshot()

        if decision.action == "retry":
            retry_message = message.__class__(
                queue_name=message.queue_name,
                task_name=message.task_name,
                payload=message.payload,
                attempts=decision.next_attempts,
                task_id=message.task_id,
                enqueued_at=message.enqueued_at,
            )
            if task_policy.backoff_seconds > 0:
                await asyncio.sleep(task_policy.backoff_seconds)
            await requeue_message(message=retry_message, queue_name=message.queue_name)
            log_event(
                logger,
                logging.WARNING,
                "task_retrying",
                worker_id=worker_id,
                task_id=message.task_id,
                queue_name=message.queue_name,
                task_name=message.task_name,
                attempts=decision.next_attempts,
                max_retries=task_policy.max_retries,
                backoff_seconds=task_policy.backoff_seconds,
                error_type=error_payload["type"],
                error_message=error_payload["message"],
            )
            status_document = await status_store.mark_retrying(
                message,
                worker_id=worker_id,
                next_attempts=decision.next_attempts,
                max_retries=task_policy.max_retries,
                policy_snapshot=policy_snapshot,
                error=error_payload,
            )
            await record_execution_history(
                execution_store=execution_store,
                status_document=status_document,
                worker_id=worker_id,
            )
        elif decision.action == "dead_letter":
            dlq_message = message.__class__(
                queue_name=message.queue_name,
                task_name=message.task_name,
                payload=message.payload,
                attempts=decision.terminal_attempts,
                task_id=message.task_id,
                enqueued_at=message.enqueued_at,
            )
            dlq_queue_name = build_dlq_queue_name(queue_name=message.queue_name)
            await requeue_message(message=dlq_message, queue_name=dlq_queue_name)
            log_event(
                logger,
                logging.ERROR,
                "task_dead_lettered",
                worker_id=worker_id,
                task_id=message.task_id,
                queue_name=message.queue_name,
                dlq_queue_name=dlq_queue_name,
                task_name=message.task_name,
                attempts=decision.terminal_attempts,
                max_retries=task_policy.max_retries,
                error_type=error_payload["type"],
                error_message=error_payload["message"],
            )
            status_document = await status_store.mark_dead_lettered(
                dlq_message,
                worker_id=worker_id,
                dlq_queue_name=dlq_queue_name,
                max_retries=task_policy.max_retries,
                policy_snapshot=policy_snapshot,
                error=error_payload,
            )
            await record_execution_history(
                execution_store=execution_store,
                status_document=status_document,
                worker_id=worker_id,
            )
        else:
            log_event(
                logger,
                logging.ERROR,
                "task_failed",
                worker_id=worker_id,
                task_id=message.task_id,
                queue_name=message.queue_name,
                task_name=message.task_name,
                attempts=message.attempts,
                error_type=error_payload["type"],
                error_message=error_payload["message"],
                dlq_enabled=task_policy.dlq_enabled,
            )
            status_document = await status_store.mark_failed(
                message,
                error_payload,
                worker_id=worker_id,
                policy_snapshot=policy_snapshot,
            )
            await record_execution_history(
                execution_store=execution_store,
                status_document=status_document,
                worker_id=worker_id,
            )
        log_event(
            logger,
            logging.ERROR,
            "task_exception",
            worker_id=worker_id,
            task_id=message.task_id,
            queue_name=message.queue_name,
            task_name=message.task_name,
            attempts=message.attempts,
            error_type=error_payload["type"],
            error_message=error_payload["message"],
            traceback=error_payload["traceback"],
        )
        if raise_on_error:
            raise


async def run_worker(*, once: bool) -> None:
    settings = get_settings()
    queue = build_queue()
    heartbeat_store = build_heartbeat_store()
    status_store = build_status_store()
    execution_store = build_execution_store()
    shutdown_event = asyncio.Event()
    allowed_task_names = set(list_task_names_for_queue(settings.worker_queue_name))

    try:
        worker_id = build_worker_id(queue_name=settings.worker_queue_name)
        _install_signal_handlers(
            shutdown_event=shutdown_event,
            worker_id=worker_id,
            queue_name=settings.worker_queue_name,
        )
        heartbeat_task = asyncio.create_task(
            run_heartbeat_loop(store=heartbeat_store, shutdown_event=shutdown_event)
        )
        log_event(
            logger,
            logging.INFO,
            "worker_started",
            worker_id=worker_id,
            queue_name=settings.worker_queue_name,
            registered_tasks=registry.task_names(),
            allowed_tasks=sorted(allowed_task_names),
        )

        while True:
            if shutdown_event.is_set():
                log_event(
                    logger,
                    logging.INFO,
                    "worker_stopping_accept_new_tasks",
                    worker_id=worker_id,
                    queue_name=settings.worker_queue_name,
                )
                break

            message, shutdown_requested = await _wait_for_next_message(
                queue=queue,
                timeout=settings.redis_block_timeout,
                shutdown_event=shutdown_event,
            )
            if shutdown_requested:
                log_event(
                    logger,
                    logging.INFO,
                    "worker_stopping_accept_new_tasks",
                    worker_id=worker_id,
                    queue_name=settings.worker_queue_name,
                )
                break

            if message is None:
                if once:
                    log_event(
                        logger,
                        logging.INFO,
                        "worker_queue_empty",
                        worker_id=worker_id,
                        queue_name=settings.worker_queue_name,
                    )
                    return
                continue

            if message.queue_name != settings.worker_queue_name:
                log_event(
                    logger,
                    logging.WARNING,
                    "task_skipped_wrong_queue",
                    worker_id=worker_id,
                    task_id=message.task_id,
                    expected_queue_name=settings.worker_queue_name,
                    actual_queue_name=message.queue_name,
                    task_name=message.task_name,
                )
                continue

            processing_task = asyncio.create_task(
                _process_message(
                    message=message,
                    worker_id=worker_id,
                    allowed_task_names=allowed_task_names,
                    status_store=status_store,
                    execution_store=execution_store,
                    raise_on_error=once,
                )
            )
            shutdown_wait_task = asyncio.create_task(shutdown_event.wait())
            done, pending = await asyncio.wait(
                {processing_task, shutdown_wait_task},
                return_when=asyncio.FIRST_COMPLETED,
            )

            if shutdown_wait_task in done and not processing_task.done():
                log_event(
                    logger,
                    logging.INFO,
                    "worker_shutdown_waiting_inflight_task",
                    worker_id=worker_id,
                    queue_name=settings.worker_queue_name,
                    task_id=message.task_id,
                    task_name=message.task_name,
                    shutdown_timeout_seconds=settings.worker_shutdown_timeout_seconds,
                )
                try:
                    await asyncio.wait_for(
                        asyncio.shield(processing_task),
                        timeout=settings.worker_shutdown_timeout_seconds,
                    )
                except asyncio.TimeoutError:
                    log_event(
                        logger,
                        logging.ERROR,
                        "worker_shutdown_timeout",
                        worker_id=worker_id,
                        queue_name=settings.worker_queue_name,
                        task_id=message.task_id,
                        task_name=message.task_name,
                        shutdown_timeout_seconds=settings.worker_shutdown_timeout_seconds,
                    )
                    processing_task.cancel()
                    try:
                        await processing_task
                    except asyncio.CancelledError:
                        pass
                break

            shutdown_wait_task.cancel()
            try:
                await shutdown_wait_task
            except asyncio.CancelledError:
                pass

            if processing_task.exception() is not None:
                raise processing_task.exception()
    finally:
        shutdown_event.set()
        heartbeat_task.cancel()
        try:
            await heartbeat_task
        except asyncio.CancelledError:
            pass
        log_event(
            logger,
            logging.INFO,
            "worker_shutdown_completed",
            worker_id=build_worker_id(queue_name=settings.worker_queue_name),
            queue_name=settings.worker_queue_name,
        )
        await status_store.close()
        await execution_store.close()
        await heartbeat_store.close()
        await queue.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a Redis-backed worker")
    parser.add_argument(
        "--once",
        action="store_true",
        help="exit after the queue becomes empty",
    )
    return parser.parse_args()


def main() -> None:
    configure_logging()
    settings = get_settings()
    load_task_modules(queue_name=settings.worker_queue_name)
    args = parse_args()
    asyncio.run(run_worker(once=args.once))


if __name__ == "__main__":
    main()
