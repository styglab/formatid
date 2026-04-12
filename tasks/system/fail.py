from shared.tasking.registry import task
from shared.tasking.schemas import TaskMessage, TaskResult


@task("system.test.fail")
async def fail_task(message: TaskMessage) -> TaskResult:
    raise RuntimeError(f"intentional failure for retry test: {message.payload}")
