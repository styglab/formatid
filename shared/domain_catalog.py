from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DOMAINS_DIR = PROJECT_ROOT / "domains"


@lru_cache(maxsize=1)
def list_domain_manifest_dirs() -> tuple[Path, ...]:
    if not DOMAINS_DIR.exists():
        return ()
    return tuple(
        path / "manifests"
        for path in sorted(DOMAINS_DIR.iterdir())
        if path.is_dir() and (path / "manifests").is_dir()
    )


def load_json_file(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def iter_task_manifest_paths() -> Iterable[Path]:
    for manifests_dir in list_domain_manifest_dirs():
        path = manifests_dir / "tasks.json"
        if path.exists():
            yield path


def iter_domain_manifest_paths() -> Iterable[Path]:
    for manifests_dir in list_domain_manifest_dirs():
        path = manifests_dir / "domain.json"
        if path.exists():
            yield path


def iter_worker_manifest_paths() -> Iterable[Path]:
    for manifests_dir in list_domain_manifest_dirs():
        for path in sorted(manifests_dir.glob("*worker.json")):
            yield path
        workers_manifest = manifests_dir / "workers.json"
        if workers_manifest.exists():
            yield workers_manifest
        workers_dir = manifests_dir / "workers"
        if workers_dir.exists():
            yield from sorted(workers_dir.glob("*.json"))


def iter_schedule_manifest_paths() -> Iterable[Path]:
    for manifests_dir in list_domain_manifest_dirs():
        schedules_dir = manifests_dir / "schedules"
        if schedules_dir.exists():
            yield from sorted(schedules_dir.glob("*.json"))


def list_domain_scheduler_env_files() -> tuple[str, ...]:
    env_files: list[str] = []
    for manifests_dir in list_domain_manifest_dirs():
        domain_manifest = manifests_dir / "domain.json"
        if not domain_manifest.exists():
            continue
        payload = load_json_file(domain_manifest)
        if not isinstance(payload, dict):
            raise RuntimeError(f"invalid domain manifest format: {domain_manifest}")
        for env_file in payload.get("scheduler_env_files", []):
            env_files.append(str(env_file))
    return tuple(env_files)
