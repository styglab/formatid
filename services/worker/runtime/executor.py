import logging
import inspect
import time

from services.worker.runtime.dispatcher import get_handler
from services.worker.runtime.logger import get_logger, log_event
from services.task_runtime.context import TaskContext
from shared.tasking.schemas import TaskMessage, TaskResult

logger = get_logger("worker.executor")


async def execute_task(message: TaskMessage, context: TaskContext | None = None) -> TaskResult:
    handler = get_handler(message.task_name)
    started_at = time.perf_counter()

    log_event(
        logger,
        logging.INFO,
        "task_started",
        task_id=message.task_id,
        queue_name=message.queue_name,
        task_name=message.task_name,
        attempts=message.attempts,
    )
    if context is not None and _accepts_context(handler):
        result = await handler(message, context)
    else:
        result = await handler(message)
    duration_ms = round((time.perf_counter() - started_at) * 1000, 3)
    log_event(
        logger,
        logging.INFO,
        "task_finished",
        task_id=result.task_id,
        queue_name=message.queue_name,
        task_name=result.task_name,
        status=result.status,
        attempts=message.attempts,
        duration_ms=duration_ms,
    )
    return result


def _accepts_context(handler) -> bool:
    try:
        return len(inspect.signature(handler).parameters) >= 2
    except (TypeError, ValueError):
        return False
