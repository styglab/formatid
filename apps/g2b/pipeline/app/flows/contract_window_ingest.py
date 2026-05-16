from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Any

from apps.g2b.pipeline.app.service.lock import (
    g2b_contract_ingest_run_lock,
    skipped_by_running_ingest,
)
from apps.g2b.pipeline.app.steps.common import G2BIngestWindow
from apps.g2b.pipeline.app.steps.contracts import CONTRACT_URLS
from apps.g2b.pipeline.app.tasks.contracts import (
    fetch_contract_category,
    normalize_contracts,
    write_contract_records,
)


def run_contract_window_ingest(
    *,
    flow_name: str,
    begin: str,
    end: str,
) -> dict[str, Any]:
    windows = split_daily_windows(begin=begin, end=end)
    lock_name = os.getenv("G2B_CONTRACT_INGEST_RUN_LOCK_NAME", "g2b_contract_ingest_run")
    with g2b_contract_ingest_run_lock() as acquired:
        if not acquired:
            return skipped_by_running_ingest(
                flow=flow_name,
                window={"begin": begin, "end": end},
                lock_name=lock_name,
            )

        results = []
        total_raw_counts = {category: 0 for category in CONTRACT_URLS}
        for window in windows:
            result = ingest_contract_window(window)
            raw_counts = result["contracts"]["raw"]["counts"]
            for category, count in raw_counts.items():
                total_raw_counts[category] += int(count)
            results.append(result)

    return {
        "flow": flow_name,
        "window": {"begin": begin, "end": end},
        "chunk_count": len(windows),
        "raw_counts": total_raw_counts,
        "raw_total": sum(total_raw_counts.values()),
        "chunks": results,
    }


def ingest_contract_window(window: G2BIngestWindow) -> dict[str, Any]:
    fetch_futures = {
        category: fetch_contract_category.submit(category, window)
        for category in CONTRACT_URLS
    }
    raw_counts = {
        category: int(write_contract_records.submit(category, window, fetch_future).result())
        for category, fetch_future in fetch_futures.items()
    }
    normalized_result = normalize_contracts.submit(window_begin=window.begin, window_end=window.end).result()
    return build_contract_ingest_result(
        window=window,
        raw_counts=raw_counts,
        normalized_result=normalized_result,
    )


def build_contract_ingest_result(
    *,
    window: G2BIngestWindow,
    raw_counts: dict[str, int],
    normalized_result: dict[str, Any],
) -> dict[str, Any]:
    return {
        "window": {"begin": window.begin, "end": window.end},
        "contracts": {
            "raw": {
                "counts": raw_counts,
                "total": sum(raw_counts.values()),
                "window": {"begin": window.begin, "end": window.end},
            },
            "normalized": normalized_result,
        },
    }


def split_daily_windows(*, begin: str, end: str) -> list[G2BIngestWindow]:
    begin_dt = datetime.strptime(begin, "%Y%m%d%H%M")
    end_dt = datetime.strptime(end, "%Y%m%d%H%M")
    if end_dt < begin_dt:
        raise ValueError(f"end must be greater than or equal to begin: begin={begin}, end={end}")

    windows = []
    cursor = begin_dt
    while cursor.date() <= end_dt.date():
        day_end = datetime.combine(cursor.date(), datetime.max.time()).replace(second=0, microsecond=0)
        window_end = min(day_end, end_dt)
        windows.append(
            G2BIngestWindow(
                begin=cursor.strftime("%Y%m%d%H%M"),
                end=window_end.strftime("%Y%m%d%H%M"),
            )
        )
        cursor = datetime.combine(cursor.date() + timedelta(days=1), datetime.min.time())
    return windows
