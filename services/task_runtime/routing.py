from services.task_runtime.catalog import get_task_definition
from shared.tasking.errors import InvalidTaskRouteError


def expected_queue_name(task_name: str) -> str:
    return get_task_definition(task_name).queue_name


def validate_task_route(*, queue_name: str, task_name: str) -> None:
    expected = expected_queue_name(task_name)
    if queue_name != expected:
        raise InvalidTaskRouteError(
            f"invalid task route: task_name={task_name} must use queue_name={expected}, got {queue_name}"
        )
