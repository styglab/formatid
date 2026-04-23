from functools import lru_cache

from core.catalog.app_catalog import iter_app_service_manifest_paths, load_json_file
from core.catalog.platform_service_catalog import HealthcheckDefinition, PlatformServiceDefinition


@lru_cache(maxsize=1)
def _load_app_service_catalog() -> tuple[PlatformServiceDefinition, ...]:
    definitions: list[PlatformServiceDefinition] = []
    service_names: set[str] = set()

    for path in iter_app_service_manifest_paths():
        payload = load_json_file(path)
        if not isinstance(payload, dict):
            raise RuntimeError(f"invalid app service manifest format: {path}")

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
            service_type=payload.get("service_type", "service"),
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
            raise RuntimeError(f"duplicate app service_name: {definition.service_name}")
        if definition.image is None and definition.dockerfile is None:
            raise RuntimeError(
                f"app service must define either image or dockerfile: {definition.service_name}"
            )
        service_names.add(definition.service_name)
        definitions.append(definition)

    return tuple(definitions)


def list_app_service_definitions() -> tuple[PlatformServiceDefinition, ...]:
    return _load_app_service_catalog()
