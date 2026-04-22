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
                task_id TEXT,
                queue_name TEXT,
                task_name TEXT,
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


async def ensure_task_executions_table(conn: Any) -> None:
    async with conn.cursor() as cursor:
        await cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS task_executions (
                task_id TEXT PRIMARY KEY,
                queue_name TEXT NOT NULL,
                service_name TEXT,
                task_name TEXT NOT NULL,
                dedupe_key TEXT,
                correlation_id TEXT,
                resource_key TEXT,
                status TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                worker_id TEXT,
                enqueued_at TIMESTAMPTZ,
                started_at TIMESTAMPTZ,
                finished_at TIMESTAMPTZ,
                duration_ms DOUBLE PRECISION,
                last_heartbeat_at TIMESTAMPTZ,
                lease_expires_at TIMESTAMPTZ,
                payload JSONB NOT NULL DEFAULT '{}'::jsonb,
                result JSONB,
                error JSONB,
                status_document JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        await cursor.execute(
            """
            ALTER TABLE task_executions
                ADD COLUMN IF NOT EXISTS service_name TEXT
            """
        )
        await cursor.execute(
            """
            ALTER TABLE task_executions
                ADD COLUMN IF NOT EXISTS dedupe_key TEXT,
                ADD COLUMN IF NOT EXISTS correlation_id TEXT,
                ADD COLUMN IF NOT EXISTS resource_key TEXT,
                ADD COLUMN IF NOT EXISTS last_heartbeat_at TIMESTAMPTZ,
                ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ
            """
        )
        await cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_task_executions_service_updated_at
                ON task_executions (service_name, updated_at DESC)
            """
        )
        await cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_task_executions_dedupe_key
                ON task_executions (dedupe_key)
            """
        )
        await cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_task_executions_resource_updated_at
                ON task_executions (resource_key, updated_at DESC)
            """
        )
        await cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_task_executions_running_lease
                ON task_executions (status, lease_expires_at)
            """
        )
        await cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_task_executions_status_updated_at
                ON task_executions (status, updated_at DESC)
            """
        )
        await cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_task_executions_task_name_updated_at
                ON task_executions (task_name, updated_at DESC)
            """
        )
    await conn.commit()


async def ensure_task_execution_events_table(conn: Any) -> None:
    async with conn.cursor() as cursor:
        await cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS task_execution_events (
                id BIGSERIAL PRIMARY KEY,
                task_id TEXT NOT NULL,
                queue_name TEXT NOT NULL,
                service_name TEXT,
                task_name TEXT NOT NULL,
                status TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                worker_id TEXT,
                error JSONB,
                details JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        await cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_task_execution_events_task_created_at
                ON task_execution_events (task_id, created_at DESC)
            """
        )
        await cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_task_execution_events_service_created_at
                ON task_execution_events (service_name, created_at DESC)
            """
        )
    await conn.commit()


async def ensure_external_api_quota_blocks_table(conn: Any) -> None:
    async with conn.cursor() as cursor:
        await cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS external_api_quota_blocks (
                id BIGSERIAL PRIMARY KEY,
                app TEXT NOT NULL,
                provider TEXT NOT NULL,
                api_name TEXT NOT NULL,
                reason TEXT NOT NULL,
                blocked_until TIMESTAMPTZ NOT NULL,
                detail JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (app, provider, api_name)
            )
            """
        )
        await cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_external_api_quota_blocks_until
                ON external_api_quota_blocks (blocked_until DESC)
            """
        )
    await conn.commit()
