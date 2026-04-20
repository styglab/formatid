import logging
import time

from services.worker.runtime.dispatcher import get_handler
from services.worker.runtime.logger import get_logger, log_event
from shared.tasking.schemas import TaskMessage, TaskResult

logger = get_logger("worker.executor")


async def execute_task(message: TaskMessage) -> TaskResult:
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
