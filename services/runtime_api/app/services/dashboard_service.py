from __future__ import annotations

import json
import importlib
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from redis.asyncio import Redis

from services.runtime_api.app.config import get_settings
from services.runtime_api.app.services.health_service import (
    build_health_summary,
    get_app_services_health_report,
    get_workers_health_report,
)
from core.runtime.runtime_db.connection import connect
from core.runtime.runtime_db.schema import ensure_service_runs_table, ensure_task_executions_table
from core.catalog.app_dashboard_catalog import (
    get_app_dashboard_definition,
    list_app_dashboard_definitions,
)
from core.catalog.service_catalog import list_worker_queue_names
from core.runtime.time import iso_now, now
from core.runtime.task_runtime.queue_control import queue_pause_key


async def build_dashboard_summary() -> dict[str, Any]:
    settings = get_settings()
    health = await build_health_summary()
    worker_report = await get_workers_health_report()
    app_services_report = await get_app_services_health_report()
    queue_report = await _build_queue_report(settings.redis_url)
    service_runs = await list_dashboard_service_runs()
    task_counts = await get_task_status_counts(hours=24)
    recent_failures = await list_recent_failures(limit=10)
    recent_tasks = await list_recent_tasks(limit=20)

    return {
        "evaluated_at": iso_now(),
        "health": health.model_dump(),
        "app_services": app_services_report,
        "workers": worker_report,
        "queues": queue_report,
        "service_runs": service_runs,
        "tasks": {
            "last_24h": task_counts,
            "recent_failures": recent_failures,
            "recent": recent_tasks,
        },
    }


async def list_dashboard_service_runs() -> list[dict[str, Any]]:
    last_runs = await _fetch_last_service_runs()
    return [
        {
            "name": name,
            "enabled": True,
            "queue_name": run.get("queue_name"),
            "task_name": run.get("task_name"),
            "next_run_at": None,
            "last_run": run,
        }
        for name, run in sorted(last_runs.items())
    ]


async def get_task_status_counts(*, hours: int) -> dict[str, int]:
    if hours < 1:
        raise ValueError("hours must be >= 1")
    cutoff_at = now() - timedelta(hours=hours)
    conn = await connect(get_settings().checkpoint_database_url)
    try:
        await ensure_task_executions_table(conn)
        async with conn.cursor() as cursor:
            await cursor.execute(
                """
                SELECT status, COUNT(*)
                FROM task_executions
                WHERE updated_at >= %s
                GROUP BY status
                """,
                (cutoff_at,),
            )
            rows = await cursor.fetchall()
    finally:
        await conn.close()
    return {status: int(count) for status, count in rows}


async def list_task_trends(*, hours: int = 24) -> list[dict[str, Any]]:
    if hours < 1:
        raise ValueError("hours must be >= 1")
    cutoff_at = now() - timedelta(hours=hours)
    conn = await connect(get_settings().checkpoint_database_url)
    try:
        await ensure_task_executions_table(conn)
        async with conn.cursor() as cursor:
            await cursor.execute(
                """
                SELECT
                    date_trunc('hour', updated_at) AS bucket,
                    status,
                    COUNT(*) AS count
                FROM task_executions
                WHERE updated_at >= %s
                GROUP BY bucket, status
                ORDER BY bucket ASC, status ASC
                """,
                (cutoff_at,),
            )
            rows = await cursor.fetchall()
    finally:
        await conn.close()
    return [
        {
            "bucket": bucket.isoformat(),
            "status": status,
            "count": int(count),
        }
        for bucket, status, count in rows
    ]


async def list_recent_failures(*, limit: int = 50) -> list[dict[str, Any]]:
    conn = await connect(get_settings().checkpoint_database_url)
    try:
        await ensure_task_executions_table(conn)
        async with conn.cursor() as cursor:
            await cursor.execute(
                """
                SELECT task_id, queue_name, task_name, status, attempts, worker_id,
                       error, updated_at
                FROM task_executions
                WHERE status IN ('failed', 'dead_lettered', 'interrupted')
                ORDER BY updated_at DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = await cursor.fetchall()
    finally:
        await conn.close()
    return [
        {
            "task_id": task_id,
            "queue_name": queue_name,
            "task_name": task_name,
            "status": status,
            "attempts": attempts,
            "worker_id": worker_id,
            "error": error,
            "updated_at": updated_at.isoformat(),
        }
        for task_id, queue_name, task_name, status, attempts, worker_id, error, updated_at in rows
    ]


async def list_recent_tasks(*, limit: int = 50) -> list[dict[str, Any]]:
    conn = await connect(get_settings().checkpoint_database_url)
    try:
        await ensure_task_executions_table(conn)
        async with conn.cursor() as cursor:
            await cursor.execute(
                """
                SELECT task_id, queue_name, task_name, status, attempts, worker_id,
                       enqueued_at, started_at, finished_at, duration_ms, error, updated_at
                FROM task_executions
                ORDER BY updated_at DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = await cursor.fetchall()
    finally:
        await conn.close()
    return [
        {
            "task_id": task_id,
            "queue_name": queue_name,
            "task_name": task_name,
            "status": status,
            "attempts": attempts,
            "worker_id": worker_id,
            "enqueued_at": None if enqueued_at is None else enqueued_at.isoformat(),
            "started_at": None if started_at is None else started_at.isoformat(),
            "finished_at": None if finished_at is None else finished_at.isoformat(),
            "duration_ms": duration_ms,
            "error": error,
            "updated_at": updated_at.isoformat(),
        }
        for (
            task_id,
            queue_name,
            task_name,
            status,
            attempts,
            worker_id,
            enqueued_at,
            started_at,
            finished_at,
            duration_ms,
            error,
            updated_at,
        ) in rows
    ]


async def list_task_duration_stats(*, hours: int = 24) -> list[dict[str, Any]]:
    cutoff_at = now() - timedelta(hours=hours)
    conn = await connect(get_settings().checkpoint_database_url)
    try:
        await ensure_task_executions_table(conn)
        async with conn.cursor() as cursor:
            await cursor.execute(
                """
                SELECT
                    task_name,
                    COUNT(*) AS count,
                    AVG(duration_ms) AS avg_duration_ms,
                    percentile_cont(0.95) WITHIN GROUP (ORDER BY duration_ms) AS p95_duration_ms
                FROM task_executions
                WHERE finished_at >= %s
                  AND duration_ms IS NOT NULL
                GROUP BY task_name
                ORDER BY task_name ASC
                """,
                (cutoff_at,),
            )
            rows = await cursor.fetchall()
    finally:
        await conn.close()
    return [
        {
            "task_name": task_name,
            "count": int(count),
            "avg_duration_ms": None if avg_duration_ms is None else float(avg_duration_ms),
            "p95_duration_ms": None if p95_duration_ms is None else float(p95_duration_ms),
        }
        for task_name, count, avg_duration_ms, p95_duration_ms in rows
    ]


async def list_app_dashboard_summaries() -> list[dict[str, Any]]:
    summaries = []
    for definition in list_app_dashboard_definitions():
        summaries.append(await build_app_dashboard_summary(definition.app))
    return summaries


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
    return await build_app_summary(
        redis_url=settings.redis_url,
        checkpoint_database_url=settings.checkpoint_database_url,
    )


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


async def _build_queue_report(redis_url: str) -> dict[str, dict[str, Any]]:
    queue_names = list(list_worker_queue_names())
    redis = Redis.from_url(redis_url, decode_responses=True)
    try:
        report = {}
        for queue_name in queue_names:
            dlq_queue_name = f"{queue_name}:dlq"
            oldest = await _peek_oldest_queue_message(redis, queue_name)
            oldest_dlq = await _peek_oldest_queue_message(redis, dlq_queue_name)
            pause = await _get_queue_pause(redis, queue_name)
            report[queue_name] = {
                "size": int(await redis.llen(queue_name)),
                "dlq_size": int(await redis.llen(dlq_queue_name)),
                "paused": pause is not None,
                "pause": pause,
                "oldest_enqueued_at": oldest["enqueued_at"],
                "oldest_age_seconds": oldest["age_seconds"],
                "oldest_task_name": oldest["task_name"],
                "oldest_dlq_enqueued_at": oldest_dlq["enqueued_at"],
                "oldest_dlq_age_seconds": oldest_dlq["age_seconds"],
                "oldest_dlq_task_name": oldest_dlq["task_name"],
            }
        return report
    finally:
        await redis.aclose()


async def _peek_oldest_queue_message(redis: Redis, queue_name: str) -> dict[str, Any]:
    raw_message = await redis.lindex(queue_name, 0)
    if raw_message is None:
        return {"enqueued_at": None, "age_seconds": None, "task_name": None}
    try:
        payload = json.loads(raw_message)
    except json.JSONDecodeError:
        return {"enqueued_at": None, "age_seconds": None, "task_name": None}
    enqueued_at = payload.get("enqueued_at")
    if not isinstance(enqueued_at, str):
        return {"enqueued_at": None, "age_seconds": None, "task_name": payload.get("task_name")}
    try:
        enqueued = datetime.fromisoformat(enqueued_at)
    except ValueError:
        return {"enqueued_at": enqueued_at, "age_seconds": None, "task_name": payload.get("task_name")}
    return {
        "enqueued_at": enqueued.isoformat(),
        "age_seconds": round(max((now() - enqueued).total_seconds(), 0.0), 3),
        "task_name": payload.get("task_name"),
    }


async def _get_queue_pause(redis: Redis, queue_name: str) -> dict[str, Any] | None:
    raw = await redis.get(queue_pause_key(queue_name))
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {"queue_name": queue_name, "reason": "invalid_pause_payload"}


async def _fetch_last_service_runs() -> dict[str, dict[str, Any]]:
    conn = await connect(get_settings().checkpoint_database_url)
    try:
        await ensure_service_runs_table(conn)
        async with conn.cursor() as cursor:
            await cursor.execute(
                """
                SELECT DISTINCT ON (run_name)
                    run_name, id, task_id, queue_name, task_name, status,
                    skip_reason, payload, details, created_at, duration_ms, error,
                    trigger_type, lock_acquired
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
            "task_id": task_id,
            "queue_name": queue_name,
            "task_name": task_name,
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
            task_id,
            queue_name,
            task_name,
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
