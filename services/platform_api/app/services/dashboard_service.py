from __future__ import annotations

import importlib
import os
from pathlib import Path
from typing import Any

from core.catalog.app_dashboard_catalog import (
    get_app_dashboard_definition,
    list_app_dashboard_definitions,
)
from core.runtime.runtime_db.connection import connect
from core.runtime.runtime_db.schema import ensure_service_runs_table
from core.runtime.time import iso_now
from services.platform_api.app.config import get_settings
from services.platform_api.app.services.health_service import build_health_summary, get_app_services_health_report


async def build_dashboard_summary() -> dict[str, Any]:
    return {
        "evaluated_at": iso_now(),
        "health": (await build_health_summary()).model_dump(),
        "app_services": await get_app_services_health_report(),
        "service_runs": await list_dashboard_service_runs(),
    }


async def list_dashboard_service_runs() -> list[dict[str, Any]]:
    last_runs = await _fetch_last_service_runs()
    return [
        {
            "name": name,
            "enabled": True,
            "next_run_at": None,
            "last_run": run,
        }
        for name, run in sorted(last_runs.items())
    ]


async def list_app_dashboard_summaries() -> list[dict[str, Any]]:
    return [await build_app_dashboard_summary(definition.app) for definition in list_app_dashboard_definitions()]


async def build_app_dashboard_summary(app_name: str) -> dict[str, Any]:
    definition = get_app_dashboard_definition(app_name)
    if definition is None:
        return {
            "app": app_name,
            "error": "app dashboard is not registered",
        }
    _load_env_files(definition.env_files)
    module_path, _, function_name = definition.summary.rpartition(".")
    if not module_path or not function_name:
        return {
            "app": app_name,
            "error": f"invalid dashboard summary provider: {definition.summary}",
        }
    module = importlib.import_module(module_path)
    build_app_summary = getattr(module, function_name)
    settings = get_settings()
    try:
        return await build_app_summary(
            redis_url=settings.redis_url,
            checkpoint_database_url=settings.checkpoint_database_url,
        )
    except Exception as exc:
        return {
            "app": app_name,
            "status": "degraded",
            "error": f"{type(exc).__name__}: {exc}",
            "metrics": [],
            "sections": [],
        }


def _load_env_files(env_files: tuple[str, ...]) -> None:
    project_root = Path(__file__).resolve().parents[4]
    for env_file in env_files:
        path = project_root / env_file
        if not path.exists():
            continue
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip("'\""))


async def _fetch_last_service_runs() -> dict[str, dict[str, Any]]:
    conn = await connect(get_settings().checkpoint_database_url)
    try:
        await ensure_service_runs_table(conn)
        async with conn.cursor() as cursor:
            await cursor.execute(
                """
                SELECT DISTINCT ON (run_name)
                    run_name, id, status, skip_reason, payload, details, created_at,
                    duration_ms, error, trigger_type, lock_acquired
                FROM service_runs
                ORDER BY run_name, created_at DESC
                """
            )
            rows = await cursor.fetchall()
    finally:
        await conn.close()
    return {
        run_name: {
            "id": run_id,
            "status": status,
            "skip_reason": skip_reason,
            "payload": payload,
            "details": details,
            "created_at": created_at.isoformat(),
            "duration_ms": None if duration_ms is None else float(duration_ms),
            "error": error,
            "trigger_type": trigger_type,
            "lock_acquired": lock_acquired,
        }
        for (
            run_name,
            run_id,
            status,
            skip_reason,
            payload,
            details,
            created_at,
            duration_ms,
            error,
            trigger_type,
            lock_acquired,
        ) in rows
    }
