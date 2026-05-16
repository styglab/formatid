from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable


PROJECT_ROOT = Path(__file__).resolve().parents[2]
APPS_DIR = PROJECT_ROOT / "apps"
SERVICES_DIR = PROJECT_ROOT / "services"
REQUIRED_PLATFORM_SERVICES = (
    "postgres",
    "redis",
    "platform-api",
    "platform-dashboard",
    "nginx",
)


@lru_cache(maxsize=1)
def list_app_manifest_dirs() -> tuple[Path, ...]:
    app_dirs = []
    if APPS_DIR.exists():
        app_dirs.extend(
            path
            for path in sorted(APPS_DIR.rglob("manifests"))
            if path.is_dir() and (path / "app.json").exists() and _is_app_manifest_enabled(path / "app.json")
        )
    if SERVICES_DIR.exists():
        app_dirs.extend(
            path / "manifests"
            for path in sorted(SERVICES_DIR.iterdir())
            if path.is_dir()
            and (path / "manifests" / "app.json").exists()
            and _is_app_manifest_enabled(path / "manifests" / "app.json")
        )
    return tuple(
        path
        for path in app_dirs
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


def load_json_file(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def iter_app_manifest_paths() -> Iterable[Path]:
    for manifests_dir in list_app_manifest_dirs():
        path = manifests_dir / "app.json"
        if path.exists():
            yield path


def iter_app_service_manifest_paths() -> Iterable[Path]:
    for manifests_dir in list_app_manifest_dirs():
        services_dir = manifests_dir / "services"
        if services_dir.exists():
            yield from sorted(services_dir.glob("*.json"))


def list_required_platform_services() -> tuple[str, ...]:
    required = list(REQUIRED_PLATFORM_SERVICES)
    seen = set(required)
    for payload in _iter_app_manifest_payloads():
        for service_name in _expand_platform_services(_requires_list(payload, "platform_services")):
            if service_name not in seen:
                required.append(service_name)
                seen.add(service_name)
    return tuple(required)


def list_app_required_platform_service_profiles() -> dict[str, tuple[str, ...]]:
    profiles_by_service: dict[str, list[str]] = {}
    for payload in _iter_app_manifest_payloads():
        profiles = _profiles_list(payload)
        if not profiles:
            continue
        for service_name in _expand_platform_services(_requires_list(payload, "platform_services")):
            profiles_by_service.setdefault(service_name, []).extend(profiles)
    return {
        service_name: tuple(dict.fromkeys(profiles))
        for service_name, profiles in profiles_by_service.items()
    }


def list_app_nginx_route_sources() -> tuple[tuple[str, str], ...]:
    routes: list[tuple[str, str]] = []
    for payload in _iter_app_manifest_payloads():
        app_name = payload.get("app", "app")
        for route in payload.get("nginx_routes", []):
            if not isinstance(route, dict):
                continue
            source = route.get("source")
            if isinstance(source, str):
                routes.append((str(app_name), source))
    return tuple(dict.fromkeys(routes))


def list_app_nginx_route_definitions() -> tuple[dict[str, Any], ...]:
    routes: list[dict[str, Any]] = []
    for payload in _iter_app_manifest_payloads():
        app_name = str(payload.get("app", "app"))
        for route in payload.get("nginx_routes", []):
            if not isinstance(route, dict) or "source" in route:
                continue
            route_definition = dict(route)
            route_definition["app"] = app_name
            routes.append(route_definition)
    return tuple(routes)


def _is_app_manifest_enabled(path: Path) -> bool:
    payload = load_json_file(path)
    if not isinstance(payload, dict):
        return True
    return payload.get("enabled", True) is not False


def _expand_platform_services(service_names: Iterable[str]) -> tuple[str, ...]:
    dependencies = _platform_service_dependencies()
    expanded: list[str] = []
    seen: set[str] = set()

    def visit(service_name: str) -> None:
        if service_name in seen:
            return
        seen.add(service_name)
        expanded.append(service_name)
        for dependency in dependencies.get(service_name, ()):
            visit(dependency)

    for service_name in service_names:
        visit(service_name)
    return tuple(expanded)


@lru_cache(maxsize=1)
def _platform_service_dependencies() -> dict[str, tuple[str, ...]]:
    dependencies: dict[str, tuple[str, ...]] = {}
    for path in _iter_platform_service_manifest_paths():
        payload = load_json_file(path)
        if not isinstance(payload, dict):
            continue
        service_name = payload.get("service_name")
        values = payload.get("depends_on_service_healthy", [])
        if isinstance(service_name, str) and isinstance(values, list) and all(isinstance(value, str) for value in values):
            dependencies[service_name] = tuple(values)
    return dependencies


def _iter_platform_service_manifest_paths() -> tuple[Path, ...]:
    service_manifests: list[Path] = []
    for manifests_dir in list_runtime_manifest_dirs():
        service_manifests.extend(sorted(manifests_dir.glob("*.json")))
    return tuple(service_manifests)


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


def _profiles_list(payload: dict[str, Any]) -> tuple[str, ...]:
    values = payload.get("profiles", [])
    if not isinstance(values, list):
        return ()
    return tuple(value for value in values if isinstance(value, str))
