from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache

from services.catalog.app_catalog import iter_app_manifest_paths, load_json_file


@dataclass(frozen=True)
class AppDashboardDefinition:
    app: str
    summary: str
    env_files: tuple[str, ...] = ()


@lru_cache(maxsize=1)
def _load_app_dashboard_catalog() -> dict[str, AppDashboardDefinition]:
    definitions: dict[str, AppDashboardDefinition] = {}
    for path in iter_app_manifest_paths():
        payload = load_json_file(path)
        if not isinstance(payload, dict):
            continue
        dashboard = payload.get("dashboard")
        if not isinstance(dashboard, dict):
            continue
        app = payload.get("app")
        summary = dashboard.get("summary")
        if not isinstance(app, str) or not isinstance(summary, str):
            continue
        definitions[app] = AppDashboardDefinition(
            app=app,
            summary=summary,
            env_files=tuple(str(env_file) for env_file in dashboard.get("env_files", [])),
        )
    return definitions


def list_app_dashboard_definitions() -> tuple[AppDashboardDefinition, ...]:
    return tuple(_load_app_dashboard_catalog().values())


def get_app_dashboard_definition(app: str) -> AppDashboardDefinition | None:
    return _load_app_dashboard_catalog().get(app)
