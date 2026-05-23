from __future__ import annotations

import importlib
import json
import os
import socket
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

from core.catalog.app_dashboard_catalog import (
    get_app_dashboard_definition,
    list_app_dashboard_definitions,
)
from core.catalog.app_service_catalog import list_app_service_definitions
from core.catalog.platform_service_catalog import list_active_platform_service_definitions
from core.runtime.runtime_db.connection import connect
from core.runtime.runtime_db.schema import ensure_service_runs_table
from core.runtime.time import iso_now
from services.platform_api.app.config import get_settings
from services.platform_api.app.services.health_service import build_health_summary, get_app_services_health_report


async def build_dashboard_summary() -> dict[str, Any]:
    return {
        "evaluated_at": iso_now(),
        "health": (await build_health_summary()).model_dump(),
        "service_health": await build_service_health_checks(),
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


async def build_service_health_checks() -> list[dict[str, Any]]:
    checks = []
    for definition in list_active_platform_service_definitions():
        checks.append({**await _check_service_health(definition.service_name), "scope": "platform"})
    for definition in list_app_service_definitions():
        checks.append({**await _check_service_health(definition.service_name), "scope": "app"})
    return checks


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


async def _check_service_health(service_name: str) -> dict[str, Any]:
    if service_name == "platform-api":
        return _service_check(
            service_name,
            status="healthy",
            kind="http",
            address="http://platform-api:8000",
            role="Platform API",
            detail="self",
        )
    if service_name == "redis":
        redis = (await build_health_summary()).redis
        return _service_check(
            service_name,
            status="healthy" if redis.ok else "down",
            kind="redis",
            address="redis:6379",
            role="Queue/cache",
            detail=redis.error,
        )
    if service_name == "postgres":
        return await _check_tcp_service(service_name, "postgres", 5432)
    if service_name == "prefect-postgres":
        return await _check_tcp_service(service_name, "prefect-postgres", 5432)
    if service_name == "prefect-redis":
        return await _check_tcp_service(service_name, "prefect-redis", 6379)
    if service_name == "prefect-services":
        prefect_server = await _check_http_service(service_name, "http://prefect-server:4200/api/health")
        return {
            **prefect_server,
            "status": "healthy" if prefect_server["status"] == "healthy" else "down",
            "kind": "prefect",
            "address": "prefect-server:4200",
            "role": "Prefect background services",
            "detail": "Uses Prefect API control plane",
        }
    url_by_service = {
        "nginx": "http://nginx/health/ready",
        "platform-dashboard": "http://platform-dashboard/",
        "prefect-server": "http://prefect-server:4200/api/health",
        "pubdata-mcp": "http://pubdata-mcp:8000/health/ready",
    }
    url = url_by_service.get(service_name)
    if url is not None:
        return await _check_http_service(service_name, url)

    return {
        "service": service_name,
        "status": "unmonitored",
        "kind": "process",
        "address": "-",
        "role": _service_role(service_name),
        "detail": "no health endpoint configured",
    }


async def _check_http_service(service_name: str, url: str) -> dict[str, Any]:
    def check() -> dict[str, Any]:
        try:
            with urlopen(url, timeout=2) as response:
                return {
                    "service": service_name,
                    "status": "healthy" if 200 <= response.status < 400 else "down",
                    "kind": "http",
                    "address": _service_address(service_name, url),
                    "role": _service_role(service_name),
                    "detail": url,
                }
        except Exception as exc:
            return {
                "service": service_name,
                "status": "down",
                "kind": "http",
                "address": _service_address(service_name, url),
                "role": _service_role(service_name),
                "detail": f"{url}: {type(exc).__name__}: {exc}",
            }

    import asyncio

    return await asyncio.to_thread(check)


async def _check_tcp_service(service_name: str, host: str, port: int) -> dict[str, Any]:
    def check() -> dict[str, Any]:
        try:
            with socket.create_connection((host, port), timeout=2):
                return {
                    "service": service_name,
                    "status": "healthy",
                    "kind": "tcp",
                    "address": f"{host}:{port}",
                    "role": _service_role(service_name),
                    "detail": f"{host}:{port}",
                }
        except Exception as exc:
            return {
                "service": service_name,
                "status": "down",
                "kind": "tcp",
                "address": f"{host}:{port}",
                "role": _service_role(service_name),
                "detail": f"{host}:{port}: {type(exc).__name__}: {exc}",
            }

    import asyncio

    return await asyncio.to_thread(check)


async def _check_prefect_worker(service_name: str, work_pool_name: str) -> dict[str, Any]:
    def check() -> dict[str, Any]:
        url = f"http://prefect-server:4200/api/work_pools/{work_pool_name}/workers/filter"
        try:
            request = Request(
                url,
                data=json.dumps({}).encode("utf-8"),
                headers={"Content-Type": "application/json"},
            )
            with urlopen(request, timeout=3) as response:
                workers = json.loads(response.read().decode("utf-8"))
        except Exception as exc:
            return {
                "service": service_name,
                "status": "down",
                "kind": "prefect-worker",
                "address": f"work pool: {work_pool_name}",
                "role": "Prefect worker",
                "detail": f"{type(exc).__name__}: {exc}",
            }
        online_workers = [worker for worker in workers if worker.get("status") == "ONLINE"]
        return {
            "service": service_name,
            "status": "healthy" if online_workers else "down",
            "kind": "prefect-worker",
            "address": f"work pool: {work_pool_name}",
            "role": "Prefect worker",
            "detail": f"{len(online_workers)} online",
        }

    import asyncio

    return await asyncio.to_thread(check)


def _service_check(
    service_name: str,
    *,
    status: str,
    kind: str,
    address: str,
    role: str,
    detail: str | None = None,
) -> dict[str, Any]:
    return {
        "service": service_name,
        "status": status,
        "kind": kind,
        "address": address,
        "role": role,
        "detail": detail,
    }


def _service_address(service_name: str, url: str) -> str:
    address_by_service = {
        "nginx": "http://nginx",
        "platform-dashboard": "http://platform-dashboard",
        "prefect-server": "http://prefect-server:4200",
        "pubdata-mcp": "http://pubdata-mcp:8000",
    }
    return address_by_service.get(service_name, url)


def _service_role(service_name: str) -> str:
    role_by_service = {
        "nginx": "Reverse proxy",
        "platform-api": "Platform API",
        "platform-dashboard": "Dashboard UI",
        "postgres": "Primary database",
        "redis": "Queue/cache",
        "prefect-server": "Prefect API/UI",
        "prefect-postgres": "Prefect database",
        "prefect-redis": "Prefect queue/cache",
        "prefect-services": "Prefect background services",
        "pubdata-mcp": "MCP server",
    }
    return role_by_service.get(service_name, "Service")
