from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any

from core.catalog.app_catalog import iter_queue_manifest_paths, load_json_file


@dataclass(frozen=True)
class QueueDefinition:
    queue: str
    queue_name: str
    capability: str
    kind: str
    worker_service: str
    description: str | None = None
    policies: dict[str, Any] = field(default_factory=dict)


@lru_cache(maxsize=1)
def _load_queue_catalog() -> dict[str, QueueDefinition]:
    catalog: dict[str, QueueDefinition] = {}
    queue_names: set[str] = set()

    for path in iter_queue_manifest_paths():
        raw_catalog = load_json_file(path)
        if not isinstance(raw_catalog, dict):
            raise RuntimeError(f"invalid queue catalog format: {path}")

        for queue, payload in raw_catalog.items():
            if not isinstance(payload, dict):
                raise RuntimeError(f"invalid queue definition for {queue}: expected object")
            definition = QueueDefinition(
                queue=payload.get("queue", queue),
                queue_name=payload["queue_name"],
                capability=payload["capability"],
                kind=payload["kind"],
                worker_service=payload["worker_service"],
                description=payload.get("description"),
                policies=dict(payload.get("policies", {})),
            )
            if definition.queue in catalog:
                raise RuntimeError(f"duplicate queue id in queue catalog: {definition.queue}")
            if definition.queue_name in queue_names:
                raise RuntimeError(f"duplicate queue_name in queue catalog: {definition.queue_name}")
            catalog[definition.queue] = definition
            queue_names.add(definition.queue_name)

    return catalog


def get_queue_definition(queue: str) -> QueueDefinition:
    try:
        return _load_queue_catalog()[queue]
    except KeyError as exc:
        raise RuntimeError(f"unknown queue id: {queue}") from exc


def get_queue_definition_by_name(queue_name: str) -> QueueDefinition:
    for definition in _load_queue_catalog().values():
        if definition.queue_name == queue_name:
            return definition
    raise RuntimeError(f"unknown queue_name: {queue_name}")


def list_queue_definitions() -> tuple[QueueDefinition, ...]:
    return tuple(_load_queue_catalog().values())


def list_queue_names() -> tuple[str, ...]:
    return tuple(definition.queue_name for definition in _load_queue_catalog().values())
