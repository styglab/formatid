from __future__ import annotations

from typing import Any


async def ensure_service_runs_table(conn: Any) -> None:
    async with conn.cursor() as cursor:
        await cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS service_runs (
                id BIGSERIAL PRIMARY KEY,
                service_name TEXT,
                run_name TEXT NOT NULL,
                status TEXT NOT NULL,
                skip_reason TEXT,
                payload JSONB NOT NULL DEFAULT '{}'::jsonb,
                details JSONB NOT NULL DEFAULT '{}'::jsonb,
                error JSONB,
                trigger_type TEXT,
                trigger_config JSONB NOT NULL DEFAULT '{}'::jsonb,
                correlation_id TEXT,
                resource_key TEXT,
                lock_acquired BOOLEAN,
                started_at TIMESTAMPTZ,
                finished_at TIMESTAMPTZ,
                duration_ms DOUBLE PRECISION,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        await cursor.execute(
            """
            ALTER TABLE service_runs
                ADD COLUMN IF NOT EXISTS service_name TEXT,
                ADD COLUMN IF NOT EXISTS error JSONB,
                ADD COLUMN IF NOT EXISTS trigger_type TEXT,
                ADD COLUMN IF NOT EXISTS trigger_config JSONB NOT NULL DEFAULT '{}'::jsonb,
                ADD COLUMN IF NOT EXISTS correlation_id TEXT,
                ADD COLUMN IF NOT EXISTS resource_key TEXT,
                ADD COLUMN IF NOT EXISTS lock_acquired BOOLEAN,
                ADD COLUMN IF NOT EXISTS started_at TIMESTAMPTZ,
                ADD COLUMN IF NOT EXISTS finished_at TIMESTAMPTZ,
                ADD COLUMN IF NOT EXISTS duration_ms DOUBLE PRECISION
            """
        )
        await cursor.execute(
            """
            ALTER TABLE service_runs
                DROP COLUMN IF EXISTS task_id,
                DROP COLUMN IF EXISTS queue_name,
                DROP COLUMN IF EXISTS task_name
            """
        )
        await cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_service_runs_service_created_at
                ON service_runs (service_name, created_at DESC)
            """
        )
        await cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_service_runs_name_created_at
                ON service_runs (run_name, created_at DESC)
            """
        )
        await cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_service_runs_status_created_at
                ON service_runs (status, created_at DESC)
            """
        )
    await conn.commit()

async def ensure_service_logs_table(conn: Any) -> None:
    async with conn.cursor() as cursor:
        await cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS service_logs (
                id BIGSERIAL PRIMARY KEY,
                service_name TEXT NOT NULL,
                level TEXT NOT NULL,
                event_name TEXT,
                message TEXT NOT NULL,
                logger_name TEXT,
                request_id TEXT,
                run_name TEXT,
                correlation_id TEXT,
                resource_key TEXT,
                details JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        await cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_service_logs_service_created_at
                ON service_logs (service_name, created_at DESC)
            """
        )
        await cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_service_logs_event_created_at
                ON service_logs (event_name, created_at DESC)
            """
        )
        await cursor.execute("DROP INDEX IF EXISTS idx_service_logs_task_id")
        await cursor.execute(
            """
            ALTER TABLE service_logs
                DROP COLUMN IF EXISTS worker_id,
                DROP COLUMN IF EXISTS task_id
            """
        )
    await conn.commit()


async def ensure_service_requests_table(conn: Any) -> None:
    async with conn.cursor() as cursor:
        await cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS service_requests (
                id BIGSERIAL PRIMARY KEY,
                service_name TEXT NOT NULL,
                request_id TEXT NOT NULL,
                method TEXT,
                path TEXT,
                correlation_id TEXT,
                resource_key TEXT,
                status TEXT NOT NULL,
                payload JSONB NOT NULL DEFAULT '{}'::jsonb,
                result JSONB,
                error JSONB,
                duration_ms DOUBLE PRECISION,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        await cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_service_requests_service_created_at
                ON service_requests (service_name, created_at DESC)
            """
        )
        await cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_service_requests_request_id
                ON service_requests (request_id)
            """
        )
        await cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_service_requests_status_created_at
                ON service_requests (status, created_at DESC)
            """
        )
    await conn.commit()


async def ensure_service_events_table(conn: Any) -> None:
    async with conn.cursor() as cursor:
        await cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS service_events (
                id BIGSERIAL PRIMARY KEY,
                service_name TEXT NOT NULL,
                event_name TEXT NOT NULL,
                request_id TEXT,
                run_name TEXT,
                correlation_id TEXT,
                resource_key TEXT,
                details JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        await cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_service_events_service_created_at
                ON service_events (service_name, created_at DESC)
            """
        )
        await cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_service_events_request_created_at
                ON service_events (request_id, created_at DESC)
            """
        )
        await cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_service_events_event_created_at
                ON service_events (event_name, created_at DESC)
            """
        )
    await conn.commit()
