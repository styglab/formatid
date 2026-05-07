from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from core.runtime.runtime_db.connection import connect
from core.runtime.runtime_db.schema import (
    ensure_service_events_table,
    ensure_service_logs_table,
    ensure_service_requests_table,
    ensure_service_runs_table,
    ensure_task_execution_events_table,
    ensure_task_executions_table,
)
from core.runtime.time import now


@dataclass(frozen=True)
class ObservabilityPruneResult:
    retention_days: int
    cutoff_at: str
    deleted_service_runs: int
    deleted_service_requests: int
    deleted_service_events: int
    deleted_service_logs: int
    deleted_task_executions: int
    deleted_task_execution_events: int

    def to_dict(self) -> dict:
        return {
            "retention_days": self.retention_days,
            "cutoff_at": self.cutoff_at,
            "deleted_service_runs": self.deleted_service_runs,
            "deleted_service_requests": self.deleted_service_requests,
            "deleted_service_events": self.deleted_service_events,
            "deleted_service_logs": self.deleted_service_logs,
            "deleted_task_executions": self.deleted_task_executions,
            "deleted_task_execution_events": self.deleted_task_execution_events,
        }


async def prune_observability(*, database_url: str, retention_days: int) -> ObservabilityPruneResult:
    if retention_days < 1:
        raise ValueError("retention_days must be >= 1")

    cutoff_at = now() - timedelta(days=retention_days)
    conn = await connect(database_url)
    try:
        await ensure_service_runs_table(conn)
        await ensure_service_requests_table(conn)
        await ensure_service_events_table(conn)
        await ensure_service_logs_table(conn)
        await ensure_task_executions_table(conn)
        await ensure_task_execution_events_table(conn)
        async with conn.cursor() as cursor:
            await cursor.execute(
                "DELETE FROM service_runs WHERE created_at < %s",
                (cutoff_at,),
            )
            deleted_service_runs = cursor.rowcount
            await cursor.execute(
                "DELETE FROM service_requests WHERE created_at < %s",
                (cutoff_at,),
            )
            deleted_service_requests = cursor.rowcount
            await cursor.execute(
                "DELETE FROM service_events WHERE created_at < %s",
                (cutoff_at,),
            )
            deleted_service_events = cursor.rowcount
            await cursor.execute(
                "DELETE FROM service_logs WHERE created_at < %s",
                (cutoff_at,),
            )
            deleted_service_logs = cursor.rowcount
            await cursor.execute(
                "DELETE FROM task_executions WHERE updated_at < %s",
                (cutoff_at,),
            )
            deleted_task_executions = cursor.rowcount
            await cursor.execute(
                "DELETE FROM task_execution_events WHERE created_at < %s",
                (cutoff_at,),
            )
            deleted_task_execution_events = cursor.rowcount
        await conn.commit()
    finally:
        await conn.close()

    return ObservabilityPruneResult(
        retention_days=retention_days,
        cutoff_at=cutoff_at.isoformat(),
        deleted_service_runs=max(deleted_service_runs, 0),
        deleted_service_requests=max(deleted_service_requests, 0),
        deleted_service_events=max(deleted_service_events, 0),
        deleted_service_logs=max(deleted_service_logs, 0),
        deleted_task_executions=max(deleted_task_executions, 0),
        deleted_task_execution_events=max(deleted_task_execution_events, 0),
    )
