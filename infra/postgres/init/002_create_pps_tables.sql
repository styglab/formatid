CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.pps_bid_notices (
    bid_ntce_no TEXT NOT NULL,
    bid_ntce_ord TEXT NOT NULL,
    bid_ntce_nm TEXT,
    ntce_instt_nm TEXT,
    bid_ntce_dt TIMESTAMPTZ,
    bid_begin_dt TIMESTAMPTZ,
    bid_clse_dt TIMESTAMPTZ,
    openg_dt TIMESTAMPTZ,
    rgst_dt TIMESTAMPTZ,
    chg_dt TIMESTAMPTZ,
    raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (bid_ntce_no, bid_ntce_ord)
);

CREATE TABLE IF NOT EXISTS raw.pps_bid_attachments (
    bid_ntce_no TEXT NOT NULL,
    bid_ntce_ord TEXT NOT NULL,
    attachment_type TEXT NOT NULL,
    attachment_index INTEGER NOT NULL,
    source_url TEXT NOT NULL,
    file_name TEXT NOT NULL,
    storage_bucket TEXT,
    storage_key TEXT,
    download_status TEXT NOT NULL,
    raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (bid_ntce_no, bid_ntce_ord, attachment_type, attachment_index)
);

CREATE TABLE IF NOT EXISTS raw.pps_bid_result_participants (
    id BIGSERIAL PRIMARY KEY,
    bid_ntce_no TEXT NOT NULL,
    bid_ntce_ord TEXT NOT NULL,
    openg_rank TEXT,
    prcbdr_bizno TEXT,
    prcbdr_nm TEXT,
    bidprc_amt TEXT,
    bidprcrt TEXT,
    raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.pps_bid_result_winners (
    id BIGSERIAL PRIMARY KEY,
    bid_ntce_no TEXT NOT NULL,
    bid_ntce_ord TEXT NOT NULL,
    bidwinnr_bizno TEXT,
    bidwinnr_nm TEXT,
    sucsfbid_amt TEXT,
    sucsfbid_rate TEXT,
    raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.pps_task_states (
    id BIGSERIAL PRIMARY KEY,
    job_type TEXT NOT NULL,
    job_key TEXT NOT NULL,
    status TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    last_enqueued_at TIMESTAMPTZ,
    last_started_at TIMESTAMPTZ,
    last_completed_at TIMESTAMPTZ,
    last_failed_at TIMESTAMPTZ,
    last_error JSONB,
    retry_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (job_type, job_key)
);
