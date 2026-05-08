from __future__ import annotations

from typing import Literal
from typing import Any

from services.runtime_api.app.config import get_settings
from services.runtime_api.app.services.health_service import (
    get_app_services_health_report,
    get_workers_health_report,
)
from core.runtime.runtime_db.connection import connect
from core.runtime.runtime_db.schema import ensure_service_logs_table


async def list_log_sources() -> list[dict[str, Any]]:
    sources: dict[tuple[str, str, str | None], dict[str, Any]] = {}

    try:
        app_report = await get_app_services_health_report()
        for service in app_report.get("services", []):
            service_name = service.get("app_name")
            if service_name:
                sources[(service_name, "service", None)] = {
                    "service_name": service_name,
                    "source_type": "service",
                    "status": service.get("health_status"),
                    "last_seen_at": service.get("updated_at"),
                }
    except Exception:
        pass

    try:
        worker_report = await get_workers_health_report()
        for workers in worker_report.get("workers", {}).values():
            for worker in workers:
                service_name = worker.get("app_name")
                worker_id = worker.get("worker_id")
                if service_name:
                    sources[(service_name, "worker", worker_id)] = {
                        "service_name": service_name,
                        "source_type": "worker",
                        "status": worker.get("health_status"),
                        "worker_id": worker_id,
                        "queue_name": worker.get("queue_name"),
                        "last_seen_at": worker.get("updated_at"),
                    }
    except Exception:
        pass

    for row in await _list_logged_sources():
        service_name = row["service_name"]
        worker_id = row.get("worker_id")
        source_type = "worker" if worker_id else "service"
        key = (service_name, source_type, worker_id)
        sources.setdefault(
            key,
            {
                "service_name": service_name,
                "source_type": source_type,
                "worker_id": worker_id,
                "status": None,
                "last_seen_at": row["last_seen_at"],
            },
        )

    return sorted(
        sources.values(),
        key=lambda item: (item["source_type"], item["service_name"], item.get("worker_id") or ""),
    )


async def list_service_logs(
    *,
    limit: int = 200,
    service_name: str | None = None,
    worker_id: str | None = None,
    level: str | None = None,
    event_name: str | None = None,
    request_id: str | None = None,
    run_name: str | None = None,
    task_id: str | None = None,
    correlation_id: str | None = None,
    after_id: int | None = None,
    before_id: int | None = None,
    sort: Literal["asc", "desc"] = "desc",
) -> list[dict[str, Any]]:
    settings = get_settings()
    from psycopg.rows import dict_row

    conn = await connect(settings.checkpoint_database_url)
    try:
        await ensure_service_logs_table(conn)
        conditions = []
        params: list[object] = []
        if service_name is not None:
            conditions.append("service_name = %s")
            params.append(service_name)
        if worker_id is not None:
            conditions.append("worker_id = %s")
            params.append(worker_id)
        if level is not None:
            conditions.append("level = %s")
            params.append(level.lower())
        if event_name is not None:
            conditions.append("event_name = %s")
            params.append(event_name)
        if request_id is not None:
            conditions.append("request_id = %s")
            params.append(request_id)
        if run_name is not None:
            conditions.append("run_name = %s")
            params.append(run_name)
        if task_id is not None:
            conditions.append("task_id = %s")
            params.append(task_id)
        if correlation_id is not None:
            conditions.append("correlation_id = %s")
            params.append(correlation_id)
        if after_id is not None:
            conditions.append("id > %s")
            params.append(after_id)
        if before_id is not None:
            conditions.append("id < %s")
            params.append(before_id)
        where_clause = "" if not conditions else "WHERE " + " AND ".join(conditions)
        params.append(limit)
        async with conn.cursor(row_factory=dict_row) as cursor:
            await cursor.execute(
                f"""
                SELECT id, service_name, worker_id, level, event_name, message,
                       logger_name, request_id, run_name, task_id, correlation_id,
                       resource_key, details, created_at
                FROM service_logs
                {where_clause}
                ORDER BY id {"ASC" if sort == "asc" else "DESC"}
                LIMIT %s
                """,
                tuple(params),
            )
            rows = await cursor.fetchall()
    finally:
        await conn.close()
    return [_serialize_row(row) for row in rows]


async def _list_logged_sources() -> list[dict[str, Any]]:
    settings = get_settings()
    from psycopg.rows import dict_row

    conn = await connect(settings.checkpoint_database_url)
    try:
        await ensure_service_logs_table(conn)
        async with conn.cursor(row_factory=dict_row) as cursor:
            await cursor.execute(
                """
                SELECT service_name, worker_id, MAX(created_at) AS last_seen_at
                FROM service_logs
                GROUP BY service_name, worker_id
                ORDER BY service_name, worker_id
                """
            )
            rows = await cursor.fetchall()
    finally:
        await conn.close()
    return [_serialize_row(row) for row in rows]


def _serialize_row(row: dict[str, Any]) -> dict[str, Any]:
    serialized: dict[str, Any] = {}
    for key, value in dict(row).items():
        serialized[key] = value.isoformat() if hasattr(value, "isoformat") else value
    return serialized
