import json
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SERVICE_MANIFESTS_DIR = PROJECT_ROOT / "infra" / "worker_services"


@dataclass(frozen=True)
class WorkerServiceDefinition:
    service_name: str
    queue_name: str
    dockerfile: str
    env_files: tuple[str, ...]
    replicas: int = 1


@lru_cache(maxsize=1)
def _load_service_catalog() -> tuple[WorkerServiceDefinition, ...]:
    definitions: list[WorkerServiceDefinition] = []
    service_names: set[str] = set()
    queue_names: set[str] = set()

    for path in sorted(SERVICE_MANIFESTS_DIR.glob("*.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        definition = WorkerServiceDefinition(
            service_name=payload["service_name"],
            queue_name=payload["queue_name"],
            dockerfile=payload["dockerfile"],
            env_files=tuple(payload.get("env_files", [])),
            replicas=int(payload.get("replicas", 1)),
        )
        if definition.service_name in service_names:
            raise RuntimeError(f"duplicate service_name in service catalog: {definition.service_name}")
        if definition.queue_name in queue_names:
            raise RuntimeError(f"duplicate queue_name in service catalog: {definition.queue_name}")
        service_names.add(definition.service_name)
        queue_names.add(definition.queue_name)
        definitions.append(definition)

    return tuple(definitions)


def list_service_definitions() -> tuple[WorkerServiceDefinition, ...]:
    return _load_service_catalog()


def list_worker_queue_names() -> tuple[str, ...]:
    return tuple(definition.queue_name for definition in _load_service_catalog())


def get_expected_workers() -> dict[str, int]:
    return {
        definition.queue_name: definition.replicas
        for definition in _load_service_catalog()
    }
