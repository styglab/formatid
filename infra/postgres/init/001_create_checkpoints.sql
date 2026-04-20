CREATE TABLE IF NOT EXISTS checkpoints (
    name TEXT PRIMARY KEY,
    value JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS schedule_runs (
    id BIGSERIAL PRIMARY KEY,
    schedule_name TEXT NOT NULL,
    task_id TEXT,
    queue_name TEXT,
    task_name TEXT,
    status TEXT NOT NULL,
    skip_reason TEXT,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_schedule_runs_schedule_created_at
    ON schedule_runs (schedule_name, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_schedule_runs_status_created_at
    ON schedule_runs (status, created_at DESC);

CREATE TABLE IF NOT EXISTS task_executions (
    task_id TEXT PRIMARY KEY,
    queue_name TEXT NOT NULL,
    task_name TEXT NOT NULL,
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    worker_id TEXT,
    enqueued_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    duration_ms DOUBLE PRECISION,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    result JSONB,
    error JSONB,
    status_document JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_task_executions_status_updated_at
    ON task_executions (status, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_task_executions_task_name_updated_at
    ON task_executions (task_name, updated_at DESC);
