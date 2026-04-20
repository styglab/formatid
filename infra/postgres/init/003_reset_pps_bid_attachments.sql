CREATE SCHEMA IF NOT EXISTS raw;

DROP TABLE IF EXISTS raw.pps_bid_attachments;

CREATE TABLE raw.pps_bid_attachments (
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
