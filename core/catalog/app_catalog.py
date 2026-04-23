from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable


PROJECT_ROOT = Path(__file__).resolve().parents[2]
APPS_DIR = PROJECT_ROOT / "apps"
SERVICES_DIR = PROJECT_ROOT / "services"
REQUIRED_PLATFORM_SERVICES = ("postgres", "redis", "runtime-api", "runtime-dashboard")


@lru_cache(maxsize=1)
def list_app_manifest_dirs() -> tuple[Path, ...]:
    if not APPS_DIR.exists():
        return ()
    return tuple(
        path
        for path in sorted(APPS_DIR.rglob("manifests"))
        if path.is_dir() and (path / "app.json").exists()
    )


@lru_cache(maxsize=1)
def list_runtime_manifest_dirs() -> tuple[Path, ...]:
    if not SERVICES_DIR.exists():
        return ()
    return tuple(
        path / "manifests"
        for path in sorted(SERVICES_DIR.iterdir())
        if path.is_dir() and (path / "manifests").is_dir()
    )


def list_task_manifest_dirs() -> tuple[Path, ...]:
    return (*list_app_manifest_dirs(), *list_runtime_manifest_dirs())


def list_worker_manifest_dirs() -> tuple[Path, ...]:
    if not SERVICES_DIR.exists():
        return ()
    return tuple(
        path / "manifests"
        for path in sorted(SERVICES_DIR.iterdir())
        if path.is_dir() and (path / "manifests").is_dir()
    )


def load_json_file(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def iter_task_manifest_paths() -> Iterable[Path]:
    for manifests_dir in list_task_manifest_dirs():
        path = manifests_dir / "tasks.json"
        if path.exists():
            yield path


def iter_queue_manifest_paths() -> Iterable[Path]:
    for manifests_dir in list_runtime_manifest_dirs():
        path = manifests_dir / "queues.json"
        if path.exists():
            yield path


def iter_app_manifest_paths() -> Iterable[Path]:
    for manifests_dir in list_app_manifest_dirs():
        path = manifests_dir / "app.json"
        if path.exists():
            yield path


def list_required_platform_services() -> tuple[str, ...]:
    required = list(REQUIRED_PLATFORM_SERVICES)
    seen = set(required)
    for payload in _iter_app_manifest_payloads():
        for service_name in _requires_list(payload, "platform_services"):
            if service_name not in seen:
                required.append(service_name)
                seen.add(service_name)
    return tuple(required)


def list_required_workers() -> tuple[str, ...]:
    workers: list[str] = []
    seen: set[str] = set()
    for payload in _iter_app_manifest_payloads():
        for queue in _requires_list(payload, "queues"):
            worker_service = _worker_service_for_queue(queue)
            if worker_service is not None and worker_service not in seen:
                workers.append(worker_service)
                seen.add(worker_service)
        for service_name in _requires_list(payload, "workers"):
            if service_name not in seen:
                workers.append(service_name)
                seen.add(service_name)
    return tuple(workers)


def _worker_service_for_queue(queue: str) -> str | None:
    from core.catalog.queue_catalog import get_queue_definition

    try:
        return get_queue_definition(queue).worker_service
    except RuntimeError:
        return None


def iter_worker_manifest_paths() -> Iterable[Path]:
    for manifests_dir in list_worker_manifest_dirs():
        for path in sorted(manifests_dir.glob("*worker.json")):
            yield path
        workers_manifest = manifests_dir / "workers.json"
        if workers_manifest.exists():
            yield workers_manifest
        workers_dir = manifests_dir / "workers"
        if workers_dir.exists():
            yield from sorted(workers_dir.glob("*.json"))


def iter_app_service_manifest_paths() -> Iterable[Path]:
    for manifests_dir in list_app_manifest_dirs():
        services_dir = manifests_dir / "services"
        if services_dir.exists():
            yield from sorted(services_dir.glob("*.json"))


def list_app_worker_env_files() -> dict[str, tuple[str, ...]]:
    env_files_by_service: dict[str, list[str]] = {}
    for manifests_dir in list_app_manifest_dirs():
        app_manifest = manifests_dir / "app.json"
        if not app_manifest.exists():
            continue
        payload = load_json_file(app_manifest)
        if not isinstance(payload, dict):
            raise RuntimeError(f"invalid app manifest format: {app_manifest}")
        worker_env_files = payload.get("worker_env_files", {})
        if not isinstance(worker_env_files, dict):
            continue
        for service_name, env_files in worker_env_files.items():
            if not isinstance(service_name, str) or not isinstance(env_files, list):
                continue
            env_files_by_service.setdefault(service_name, []).extend(
                str(env_file)
                for env_file in env_files
                if isinstance(env_file, str)
            )
    return {
        service_name: tuple(env_files)
        for service_name, env_files in env_files_by_service.items()
    }


def _iter_app_manifest_payloads() -> Iterable[dict[str, Any]]:
    for path in iter_app_manifest_paths():
        payload = load_json_file(path)
        if isinstance(payload, dict):
            yield payload


def _requires_list(payload: dict[str, Any], key: str) -> tuple[str, ...]:
    requires = payload.get("requires", {})
    if not isinstance(requires, dict):
        return ()
    values = requires.get(key, [])
    if not isinstance(values, list):
        return ()
    return tuple(value for value in values if isinstance(value, str))
