from shared.tasking.registry import task
from shared.tasking.schemas import TaskMessage, TaskResult


@task("pps.attachment.download")
async def download_attachment(message: TaskMessage) -> TaskResult:
    return TaskResult(
        task_id=message.task_id,
        task_name=message.task_name,
        status="queued",
        output={
            "message": "attachment queue wiring is ready",
            "queue_name": message.queue_name,
            "payload": message.payload,
        },
    )
