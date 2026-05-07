import json
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from core.catalog.app_catalog import list_required_platform_services

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CORE_DIR = PROJECT_ROOT / "core"
SERVICES_DIR = PROJECT_ROOT / "services"


@dataclass(frozen=True)
class HealthcheckDefinition:
    test: tuple[str, ...]
    interval: str
    timeout: str
    retries: int


@dataclass(frozen=True)
class PlatformServiceDefinition:
    service_name: str
    service_type: str = "platform"
    image: str | None = None
    dockerfile: str | None = None
    command: tuple[str, ...] = ()
    build_context: str = ".."
    user: str | None = None
    restart: str | None = None
    env_files: tuple[str, ...] = ()
    ports: tuple[str, ...] = ()
    volumes: tuple[str, ...] = ()
    depends_on_service_healthy: tuple[str, ...] = ()
    healthcheck: HealthcheckDefinition | None = None


@lru_cache(maxsize=1)
def _load_platform_service_catalog() -> tuple[PlatformServiceDefinition, ...]:
    definitions: list[PlatformServiceDefinition] = []
    service_names: set[str] = set()

    for path in _iter_platform_service_manifest_paths():
        payload = json.loads(path.read_text(encoding="utf-8"))
        healthcheck_payload = payload.get("healthcheck")
        healthcheck = None
        if healthcheck_payload is not None:
            healthcheck = HealthcheckDefinition(
                test=tuple(healthcheck_payload["test"]),
                interval=healthcheck_payload["interval"],
                timeout=healthcheck_payload["timeout"],
                retries=int(healthcheck_payload["retries"]),
            )

        definition = PlatformServiceDefinition(
            service_name=payload["service_name"],
            service_type=payload.get("service_type", "platform"),
            image=payload.get("image"),
            dockerfile=payload.get("dockerfile"),
            command=tuple(payload.get("command", [])),
            build_context=payload.get("build_context", ".."),
            user=payload.get("user"),
            restart=payload.get("restart"),
            env_files=tuple(payload.get("env_files", [])),
            ports=tuple(payload.get("ports", [])),
            volumes=tuple(payload.get("volumes", [])),
            depends_on_service_healthy=tuple(payload.get("depends_on_service_healthy", [])),
            healthcheck=healthcheck,
        )
        if definition.service_name in service_names:
            raise RuntimeError(f"duplicate platform service_name: {definition.service_name}")
        if definition.image is None and definition.dockerfile is None:
            raise RuntimeError(
                f"platform service must define either image or dockerfile: {definition.service_name}"
            )
        service_names.add(definition.service_name)
        definitions.append(definition)

    return tuple(definitions)


def _iter_platform_service_manifest_paths() -> tuple[Path, ...]:
    core_manifests = sorted(CORE_DIR.glob("**/manifests/*.json"))
    service_manifests = sorted(_iter_service_manifest_paths())
    return tuple((*core_manifests, *service_manifests))


def _iter_service_manifest_paths() -> tuple[Path, ...]:
    if not SERVICES_DIR.exists():
        return ()
    paths: list[Path] = []
    for service_dir in sorted(SERVICES_DIR.iterdir()):
        manifests_dir = service_dir / "manifests"
        if not manifests_dir.is_dir():
            continue
        if _has_worker_or_task_manifests(manifests_dir):
            continue
        paths.extend(sorted(manifests_dir.glob("*.json")))
    return tuple(paths)


def _has_worker_or_task_manifests(manifests_dir: Path) -> bool:
    return any(
        (
            (manifests_dir / "tasks.json").exists(),
            (manifests_dir / "queues.json").exists(),
            (manifests_dir / "workers.json").exists(),
            (manifests_dir / "workers").exists(),
        )
    )


def list_platform_service_definitions() -> tuple[PlatformServiceDefinition, ...]:
    return _load_platform_service_catalog()


def list_active_platform_service_definitions() -> tuple[PlatformServiceDefinition, ...]:
    required_services = set(list_required_platform_services())
    return tuple(
        definition
        for definition in _load_platform_service_catalog()
        if definition.service_name in required_services
    )
