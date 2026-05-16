from __future__ import annotations

import os
from datetime import datetime
from typing import Any

from apps.g2b.pipeline.app.steps.bid_notices import BASE_URLS, compute_realtime_window_value
from apps.g2b.pipeline.app.steps.common import G2BIngestWindow
from apps.g2b.pipeline.app.tasks.bid_notices import (
    fetch_category,
    normalize_raw_notices,
    normalize_raw_notices_once,
    write_records,
    write_records_value,
)
from apps.g2b.pipeline.app.tasks.license_limits import (
    fetch_license_limits,
    normalize_license_limits,
    normalize_license_limits_once,
    write_license_limits,
)
from apps.g2b.pipeline.app.tasks.participation_regions import (
    fetch_participation_regions,
    normalize_participation_regions,
    normalize_participation_regions_once,
    write_participation_regions,
)
from apps.g2b.pipeline.app.service.lock import (
    g2b_bid_ingest_run_lock,
    skipped_by_running_ingest,
)


def run_g2b_bid_initial_ingest(
    begin: str = "202605040000",
    end: str = "202605042359",
    *,
    use_prefect_tasks: bool = False,
) -> dict[str, Any]:
    window_dict = {"begin": begin, "end": end}
    with g2b_bid_ingest_run_lock() as acquired:
        if not acquired:
            return skipped_by_running_ingest(flow="g2b-bid-initial-ingest", window=window_dict)
        return run_ingest_window(
            flow_name="g2b-bid-initial-ingest",
            window=G2BIngestWindow(begin=begin, end=end),
            use_prefect_tasks=use_prefect_tasks,
        )


def run_g2b_bid_5min_ingest_once(now: datetime | None = None) -> dict[str, Any]:
    lookback_minutes = int(
        os.getenv(
            "G2B_BID_5MIN_LOOKBACK_MINUTES",
            os.getenv("G2B_INGEST_REALTIME_LOOKBACK_MINUTES", "180"),
        )
    )
    window = compute_realtime_window_value(now, lookback_minutes=lookback_minutes)
    return process_5min_window(window, use_prefect_tasks=False)


def process_5min_window(window: dict[str, str], *, use_prefect_tasks: bool) -> dict[str, Any]:
    with g2b_bid_ingest_run_lock() as acquired:
        if not acquired:
            return skipped_by_running_ingest(flow="g2b-bid-5min-ingest", window=window)
        return run_ingest_window(
            flow_name="g2b-bid-5min-ingest",
            window=G2BIngestWindow(begin=window["begin"], end=window["end"]),
            use_prefect_tasks=use_prefect_tasks,
        )


def run_ingest_window(
    *,
    flow_name: str,
    window: G2BIngestWindow,
    use_prefect_tasks: bool,
) -> dict[str, Any]:
    if use_prefect_tasks:
        raise ValueError("Prefect task orchestration belongs in app.flows")

    raw_counts = ingest_notice_raw(window)
    license_raw_count = ingest_license_limits(window)
    participation_region_raw_count = ingest_participation_regions(window)
    normalized_result = normalize_notices(window)
    normalized_license_result = normalize_license_limits_stage(window)
    normalized_participation_region_result = normalize_participation_regions_stage(window)
    return build_ingest_result(
        flow_name=flow_name,
        window=window,
        raw_counts=raw_counts,
        license_raw_count=license_raw_count,
        participation_region_raw_count=participation_region_raw_count,
        normalized_result=normalized_result,
        normalized_license_result=normalized_license_result,
        normalized_participation_region_result=normalized_participation_region_result,
    )


def build_ingest_result(
    *,
    flow_name: str,
    window: G2BIngestWindow,
    raw_counts: dict[str, int],
    license_raw_count: int,
    participation_region_raw_count: int,
    normalized_result: dict[str, Any],
    normalized_license_result: dict[str, Any],
    normalized_participation_region_result: dict[str, Any],
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "window": {"begin": window.begin, "end": window.end},
        "raw": {
            "counts": raw_counts,
            "total": sum(raw_counts.values()),
        },
        "normalized": normalized_result,
        "license_limits": {
            "raw": {"count": license_raw_count},
            "normalized": normalized_license_result,
        },
        "participation_regions": {
            "raw": {"count": participation_region_raw_count},
            "normalized": normalized_participation_region_result,
        },
    }
    if flow_name == "g2b-bid-initial-ingest":
        result["raw"].update(
            {
                "schema": os.getenv("G2B_INGEST_SCHEMA", "g2b"),
                "table": os.getenv("G2B_INGEST_TABLE", "bid_public_notice_raw"),
            }
        )
        result["license_limits"]["raw"].update(
            {
                "schema": os.getenv("G2B_INGEST_SCHEMA", "g2b"),
                "table": os.getenv("G2B_LICENSE_LIMIT_RAW_TABLE", "bid_public_notice_license_limit_raw"),
            }
        )
        result["participation_regions"]["raw"].update(
            {
                "schema": os.getenv("G2B_INGEST_SCHEMA", "g2b"),
                "table": os.getenv("G2B_PARTICIPATION_REGION_RAW_TABLE", "bid_public_notice_participation_region_raw"),
            }
        )
    else:
        window_dict = {"begin": window.begin, "end": window.end}
        result["raw"]["window"] = window_dict
        result["license_limits"]["raw"]["window"] = window_dict
        result["participation_regions"]["raw"]["window"] = window_dict
    return result


def ingest_notice_raw(window: G2BIngestWindow) -> dict[str, int]:
    raw_counts: dict[str, int] = {}
    for category in BASE_URLS:
        records = fetch_category.fn(category, window)
        raw_counts[category] = write_records_value(category=category, window=window, records=records)
    return raw_counts


def ingest_license_limits(window: G2BIngestWindow) -> int:
    records = fetch_license_limits.fn(window)
    return write_license_limits.fn(window, records)


def ingest_participation_regions(window: G2BIngestWindow) -> int:
    records = fetch_participation_regions.fn(window)
    return write_participation_regions.fn(window, records)


def normalize_notices(window: G2BIngestWindow) -> dict[str, Any]:
    return normalize_raw_notices_once(window_begin=window.begin, window_end=window.end)


def normalize_license_limits_stage(window: G2BIngestWindow) -> dict[str, Any]:
    return normalize_license_limits_once(window_begin=window.begin, window_end=window.end)


def normalize_participation_regions_stage(window: G2BIngestWindow) -> dict[str, Any]:
    return normalize_participation_regions_once(window_begin=window.begin, window_end=window.end)


_ingest_notice_raw = ingest_notice_raw
