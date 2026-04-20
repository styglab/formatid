import json
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCHEDULE_MANIFESTS_DIR = PROJECT_ROOT / "infra" / "schedules"


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


@lru_cache(maxsize=1)
def _load_schedule_catalog() -> tuple[ScheduleDefinition, ...]:
    definitions: list[ScheduleDefinition] = []
    names: set[str] = set()

    for path in sorted(SCHEDULE_MANIFESTS_DIR.glob("*.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        definition = ScheduleDefinition(
            name=payload["name"],
            queue_name=payload["queue_name"],
            task_name=payload["task_name"],
            interval_seconds=int(payload["interval_seconds"]),
            payload=dict(payload.get("payload", {})),
            payload_factory=payload.get("payload_factory"),
            run_immediately=bool(payload.get("run_immediately", False)),
            enabled=bool(payload.get("enabled", True)),
        )
        if definition.name in names:
            raise RuntimeError(f"duplicate schedule name: {definition.name}")
        names.add(definition.name)
        definitions.append(definition)

    return tuple(definitions)


def list_schedule_definitions() -> tuple[ScheduleDefinition, ...]:
    return _load_schedule_catalog()
