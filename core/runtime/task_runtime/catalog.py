from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from core.catalog.app_catalog import iter_task_manifest_paths, load_json_file
from core.catalog.queue_catalog import get_queue_definition, list_queue_definitions
from core.runtime.task_runtime.errors import UnknownTaskRoutingError


PROJECT_ROOT = Path(__file__).resolve().parents[3]


@dataclass(frozen=True)
class TaskDefinition:
    task_name: str
    queue: str
    service_name: str
    queue_name: str
    module_path: str
    payload_schema: str | None = None
    output_schema: str | None = None
    max_retries: int | None = None
    retryable: bool = True
    backoff_seconds: int | None = None
    timeout_seconds: int | None = None
    dlq_enabled: bool = True
    dlq_requeue_limit: int | None = None
    dlq_requeue_keep_attempts: bool = False


@lru_cache(maxsize=1)
def _load_task_catalog() -> dict[str, TaskDefinition]:
    task_catalog: dict[str, TaskDefinition] = {}
    for path in iter_task_manifest_paths():
        raw_catalog = load_json_file(path)
        if not isinstance(raw_catalog, dict):
            raise RuntimeError(f"invalid task catalog format: {path}")
        service_name = _load_service_name(path)

        for task_name, payload in raw_catalog.items():
            if not isinstance(payload, dict):
                raise RuntimeError(f"invalid task definition for {task_name}: expected object")

            queue = payload.get("queue")
            if queue is None:
                queue = _queue_id_from_legacy_queue_name(payload["queue_name"])
            queue_definition = get_queue_definition(queue)
            definition = TaskDefinition(
                task_name=payload.get("task_name", task_name),
                queue=queue,
                service_name=payload.get("service_name", queue_definition.worker_service or service_name),
                queue_name=queue_definition.queue_name,
                module_path=payload["module_path"],
                payload_schema=payload.get("payload_schema"),
                output_schema=payload.get("output_schema"),
                max_retries=payload.get("max_retries"),
                retryable=payload.get("retryable", True),
                backoff_seconds=payload.get("backoff_seconds"),
                timeout_seconds=payload.get("timeout_seconds"),
                dlq_enabled=payload.get("dlq_enabled", True),
                dlq_requeue_limit=payload.get("dlq_requeue_limit"),
                dlq_requeue_keep_attempts=payload.get("dlq_requeue_keep_attempts", False),
            )
            if definition.task_name in task_catalog:
                raise RuntimeError(f"duplicate task_name in app manifests: {definition.task_name}")
            task_catalog[definition.task_name] = definition

    return task_catalog


def _load_service_name(path: Path) -> str:
    app_manifest = path.parent / "app.json"
    if app_manifest.exists():
        payload = load_json_file(app_manifest)
        if isinstance(payload, dict) and isinstance(payload.get("app"), str):
            return payload["app"]
    return path.parent.parent.name


def _queue_id_from_legacy_queue_name(queue_name: str) -> str:
    for definition in list_queue_definitions():
        if definition.queue_name == queue_name:
            return definition.queue
    raise RuntimeError(f"task manifest references unknown queue_name: {queue_name}")


def get_task_definition(task_name: str) -> TaskDefinition:
    try:
        return _load_task_catalog()[task_name]
    except KeyError as exc:
        raise UnknownTaskRoutingError(f"unknown task routing: {task_name}") from exc


def list_task_definitions() -> tuple[TaskDefinition, ...]:
    return tuple(_load_task_catalog().values())


def list_queue_names() -> tuple[str, ...]:
    return tuple(definition.queue_name for definition in list_queue_definitions())


def list_task_names_for_queue(queue_name: str) -> tuple[str, ...]:
    task_names = [
        definition.task_name
        for definition in _load_task_catalog().values()
        if definition.queue_name == queue_name
    ]
    return tuple(task_names)


def list_task_module_paths_for_queue(queue_name: str) -> tuple[str, ...]:
    seen: set[str] = set()
    module_paths: list[str] = []

    for definition in _load_task_catalog().values():
        if definition.queue_name != queue_name:
            continue
        if definition.module_path in seen:
            continue
        seen.add(definition.module_path)
        module_paths.append(definition.module_path)

    return tuple(module_paths)
