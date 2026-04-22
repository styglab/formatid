CREATE SCHEMA IF NOT EXISTS summary;

CREATE TABLE IF NOT EXISTS summary.jobs (
    job_id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    bucket TEXT NOT NULL,
    object_key TEXT NOT NULL,
    callback_url TEXT,
    error JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS summary.extracted_texts (
    job_id TEXT PRIMARY KEY REFERENCES summary.jobs(job_id) ON DELETE CASCADE,
    bucket TEXT NOT NULL,
    object_key TEXT NOT NULL,
    text TEXT NOT NULL,
    char_count INTEGER NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS summary.results (
    job_id TEXT PRIMARY KEY REFERENCES summary.jobs(job_id) ON DELETE CASCADE,
    summary_text TEXT NOT NULL,
    model TEXT NOT NULL,
    prompt_version TEXT NOT NULL,
    raw_result JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS summary.job_events (
    id BIGSERIAL PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES summary.jobs(job_id) ON DELETE CASCADE,
    event_name TEXT NOT NULL,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_summary_jobs_status_updated_at
    ON summary.jobs (status, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_summary_job_events_job_created_at
    ON summary.job_events (job_id, created_at DESC);
