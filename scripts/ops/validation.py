from __future__ import annotations

import re

from core.catalog.app_catalog import iter_app_manifest_paths, load_json_file
from core.catalog.registry import CatalogRegistry
from scripts.ops.common import COMPOSE_FILE, PROJECT_ROOT


APP_SERVICE_TYPES = {"cron", "api", "consumer", "service"}
SERVICE_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9]*(?:-[a-z0-9]+)*$")


def validate_config() -> dict:
    from scripts.generate_compose import render_compose

    errors: list[str] = []
    warnings: list[str] = []

    try:
        registry = CatalogRegistry.load()
    except (KeyError, TypeError, ValueError, RuntimeError) as exc:
        return {
            "valid": False,
            "errors": [f"catalog load failed: {exc}"],
            "warnings": warnings,
            "summary": {},
        }

    platform_service_definitions = registry.active_platform_services
    available_platform_service_definitions = registry.available_platform_services
    app_service_definitions = registry.app_services
    active_platform_service_names = {definition.service_name for definition in platform_service_definitions}
    available_platform_service_names = {
        definition.service_name for definition in available_platform_service_definitions
    }
    required_platform_service_names = set(registry.required_platform_services)

    for service_name in sorted(required_platform_service_names - available_platform_service_names):
        errors.append(
            "required platform service is not defined: "
            f"service_name={service_name} available={sorted(available_platform_service_names)}"
        )

    for path in iter_app_manifest_paths():
        payload = load_json_file(path)
        if not isinstance(payload, dict):
            errors.append(f"app manifest must be an object: path={path}")
            continue
        app_name = payload.get("app")
        if not app_name:
            errors.append(f"app manifest must define app: path={path}")
        elif app_name != _app_name_from_manifest_path(path):
            errors.append(
                f"app manifest app must match directory path: path={path} app={app_name} directory={_app_name_from_manifest_path(path)}"
            )
        profiles = payload.get("profiles", [])
        if not isinstance(profiles, list) or not all(isinstance(value, str) for value in profiles):
            errors.append(f"app manifest profiles must be a list of strings: app={app_name}")
        requires = payload.get("requires", {})
        if requires is not None and not isinstance(requires, dict):
            errors.append(f"app manifest requires must be an object: app={app_name}")
        elif isinstance(requires, dict):
            allowed_keys = {"platform_services"}
            for key in sorted(set(requires) - allowed_keys):
                errors.append(f"app manifest requires has unsupported key: app={app_name} key={key}")
            platform_services = requires.get("platform_services", [])
            if not isinstance(platform_services, list) or not all(isinstance(value, str) for value in platform_services):
                errors.append(f"app manifest requires.platform_services must be a list of strings: app={app_name}")
            else:
                for service_name in sorted(set(platform_services) - available_platform_service_names):
                    errors.append(
                        "app manifest requires unknown platform service: "
                        f"app={app_name} service_name={service_name} available={sorted(available_platform_service_names)}"
                    )
        nginx_routes = payload.get("nginx_routes", [])
        if not isinstance(nginx_routes, list):
            errors.append(f"app nginx_routes must be a list: app={app_name}")
        else:
            for route in nginx_routes:
                if not isinstance(route, dict):
                    errors.append(f"app nginx_routes item must be an object: app={app_name}")
                    continue
                upstream_service = route.get("upstream_service")
                if upstream_service is not None and not _matches(SERVICE_NAME_PATTERN, upstream_service):
                    errors.append(
                        f"app nginx_routes.upstream_service must be kebab-case service name: app={app_name}"
                    )
                upstream_port = route.get("upstream_port")
                if upstream_port is not None and (not isinstance(upstream_port, int) or upstream_port <= 0):
                    errors.append(f"app nginx_routes.upstream_port must be a positive integer: app={app_name}")

    for definition in (*available_platform_service_definitions, *app_service_definitions):
        if not _matches(SERVICE_NAME_PATTERN, definition.service_name):
            errors.append(f"service_name must be kebab-case: service_name={definition.service_name}")
        if definition.service_type not in APP_SERVICE_TYPES and definition.service_type not in {
            "platform",
            "database",
            "cache",
            "object_storage",
            "vector_db",
        }:
            warnings.append(
                f"service_type is not one of known values: service_name={definition.service_name} service_type={definition.service_type}"
            )
        for env_file in definition.env_files:
            if not (PROJECT_ROOT / env_file).exists():
                errors.append(f"service env_file does not exist: service_name={definition.service_name} env_file={env_file}")
        if definition.dockerfile is not None and not (PROJECT_ROOT / definition.dockerfile).exists():
            errors.append(
                f"service dockerfile does not exist: service_name={definition.service_name} dockerfile={definition.dockerfile}"
            )
        for dependency in definition.depends_on_service_healthy:
            if dependency not in active_platform_service_names and dependency not in {
                service.service_name for service in app_service_definitions
            }:
                errors.append(
                    f"service depends_on_service_healthy references inactive service: service_name={definition.service_name} dependency={dependency}"
                )

    compose_in_sync = COMPOSE_FILE.exists() and COMPOSE_FILE.read_text(encoding="utf-8") == render_compose()
    if not compose_in_sync:
        errors.append("generated compose is out of sync: run `python3 scripts/generate_compose.py`")

    return {
        "valid": not errors,
        "errors": errors,
        "warnings": warnings,
        "summary": {
            "required_platform_services": len(platform_service_definitions),
            "available_platform_services": len(available_platform_service_definitions),
            "app_services": len(app_service_definitions),
            "compose_in_sync": compose_in_sync,
        },
    }


def _app_name_from_manifest_path(path) -> str:
    app_dir = path.parent.parent
    services_dir = PROJECT_ROOT / "services"
    if app_dir.is_relative_to(services_dir):
        return ".".join(app_dir.relative_to(services_dir).parts)
    return ".".join(app_dir.relative_to(PROJECT_ROOT / "apps").parts)


def _matches(pattern: re.Pattern[str], value: object) -> bool:
    return isinstance(value, str) and bool(pattern.fullmatch(value))
