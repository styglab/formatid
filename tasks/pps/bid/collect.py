from shared.tasking.registry import task
from shared.tasking.schemas import TaskMessage, TaskResult


@task("pps.bid.collect")
async def collect_bid(message: TaskMessage) -> TaskResult:
    return TaskResult(
        task_id=message.task_id,
        task_name=message.task_name,
        status="queued",
        output={
            "message": "bid queue wiring is ready",
            "queue_name": message.queue_name,
            "payload": message.payload,
        },
    )
