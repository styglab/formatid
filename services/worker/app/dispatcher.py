from shared.tasking.registry import TaskHandler, registry


def get_handler(task_name: str) -> TaskHandler:
    return registry.get(task_name)
