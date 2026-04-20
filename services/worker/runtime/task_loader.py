from importlib import import_module

from shared.tasking.catalog import list_task_module_paths_for_queue


def load_task_modules(*, queue_name: str) -> None:
    for module_path in list_task_module_paths_for_queue(queue_name):
        import_module(module_path)
