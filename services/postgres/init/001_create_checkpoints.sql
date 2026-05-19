CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS checkpoints (
    name TEXT PRIMARY KEY,
    value JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

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
);

CREATE INDEX IF NOT EXISTS idx_service_runs_service_created_at
    ON service_runs (service_name, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_service_runs_name_created_at
    ON service_runs (run_name, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_service_runs_status_created_at
    ON service_runs (status, created_at DESC);

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
);

CREATE INDEX IF NOT EXISTS idx_service_logs_service_created_at
    ON service_logs (service_name, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_service_logs_event_created_at
    ON service_logs (event_name, created_at DESC);

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
);

CREATE INDEX IF NOT EXISTS idx_service_requests_service_created_at
    ON service_requests (service_name, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_service_requests_request_id
    ON service_requests (request_id);

CREATE INDEX IF NOT EXISTS idx_service_requests_status_created_at
    ON service_requests (status, created_at DESC);

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
);

CREATE INDEX IF NOT EXISTS idx_service_events_service_created_at
    ON service_events (service_name, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_service_events_request_created_at
    ON service_events (request_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_service_events_event_created_at
    ON service_events (event_name, created_at DESC);

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
);

CREATE INDEX IF NOT EXISTS idx_external_api_quota_blocks_until
    ON external_api_quota_blocks (blocked_until DESC);
