from collections.abc import Awaitable, Callable

from shared.tasking.errors import UnknownTaskError
from shared.tasking.schemas import TaskMessage, TaskResult

TaskHandler = Callable[..., Awaitable[TaskResult]]


class TaskRegistry:
    def __init__(self) -> None:
        self._handlers: dict[str, TaskHandler] = {}

    def register(self, task_name: str, handler: TaskHandler) -> None:
        if task_name in self._handlers:
            raise ValueError(f"task already registered: {task_name}")
        self._handlers[task_name] = handler

    def get(self, task_name: str) -> TaskHandler:
        try:
            return self._handlers[task_name]
        except KeyError as exc:
            raise UnknownTaskError(f"unknown task: {task_name}") from exc

    def task_names(self) -> list[str]:
        return sorted(self._handlers)


registry = TaskRegistry()


def task(task_name: str) -> Callable[[TaskHandler], TaskHandler]:
    def decorator(func: TaskHandler) -> TaskHandler:
        registry.register(task_name, func)
        return func

    return decorator
