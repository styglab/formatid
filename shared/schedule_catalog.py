from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

from shared.domain_catalog import iter_schedule_manifest_paths, load_json_file


PROJECT_ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class ScheduleDefinition:
    name: str
    queue_name: str
    task_name: str
    interval_seconds: int
    payload: dict[str, Any]
    payload_factory: str | None = None
    run_immediately: bool = False
    enabled: bool = True
    lock_enabled: bool = True
    lock_ttl_seconds: int | None = None
    misfire_grace_seconds: int = 30
    coalesce: bool = True
    max_instances: int = 1


@lru_cache(maxsize=1)
def _load_schedule_catalog() -> tuple[ScheduleDefinition, ...]:
    definitions: list[ScheduleDefinition] = []
    names: set[str] = set()

    for path in iter_schedule_manifest_paths():
        payload = load_json_file(path)
        definition = ScheduleDefinition(
            name=payload["name"],
            queue_name=payload["queue_name"],
            task_name=payload["task_name"],
            interval_seconds=int(payload["interval_seconds"]),
            payload=dict(payload.get("payload", {})),
            payload_factory=payload.get("payload_factory"),
            run_immediately=bool(payload.get("run_immediately", False)),
            enabled=bool(payload.get("enabled", True)),
            lock_enabled=bool(payload.get("lock_enabled", True)),
            lock_ttl_seconds=(
                int(payload["lock_ttl_seconds"])
                if payload.get("lock_ttl_seconds") is not None
                else None
            ),
            misfire_grace_seconds=int(payload.get("misfire_grace_seconds", 30)),
            coalesce=bool(payload.get("coalesce", True)),
            max_instances=int(payload.get("max_instances", 1)),
        )
        if definition.name in names:
            raise RuntimeError(f"duplicate schedule name: {definition.name}")
        names.add(definition.name)
        definitions.append(definition)

    return tuple(definitions)


def list_schedule_definitions() -> tuple[ScheduleDefinition, ...]:
    return _load_schedule_catalog()
