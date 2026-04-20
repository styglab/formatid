import json
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PLATFORM_SERVICE_MANIFESTS_DIR = PROJECT_ROOT / "infra" / "platform_services"


@dataclass(frozen=True)
class HealthcheckDefinition:
    test: tuple[str, ...]
    interval: str
    timeout: str
    retries: int


@dataclass(frozen=True)
class PlatformServiceDefinition:
    service_name: str
    image: str | None = None
    dockerfile: str | None = None
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

    for path in sorted(PLATFORM_SERVICE_MANIFESTS_DIR.glob("*.json")):
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
            image=payload.get("image"),
            dockerfile=payload.get("dockerfile"),
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


def list_platform_service_definitions() -> tuple[PlatformServiceDefinition, ...]:
    return _load_platform_service_catalog()
