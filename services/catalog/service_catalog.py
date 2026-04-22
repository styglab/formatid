from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from services.catalog.app_catalog import iter_worker_manifest_paths, list_app_worker_env_files, load_json_file


PROJECT_ROOT = Path(__file__).resolve().parents[2]


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
    app_env_files = list_app_worker_env_files()

    for path in iter_worker_manifest_paths():
        raw_payload = load_json_file(path)
        payloads = raw_payload if isinstance(raw_payload, list) else [raw_payload]
        for payload in payloads:
            if not isinstance(payload, dict):
                raise RuntimeError(f"invalid worker service manifest format: {path}")
            definition = WorkerServiceDefinition(
                service_name=payload["service_name"],
                queue_name=payload["queue_name"],
                dockerfile=payload["dockerfile"],
                env_files=(
                    *tuple(payload.get("env_files", [])),
                    *app_env_files.get(payload["service_name"], ()),
                ),
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
