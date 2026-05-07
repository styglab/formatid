from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from prefect import flow

from apps.shared.data_pipeline.app.prefect_tasks.g2b_bid import (
    compute_due_windows,
    fetch_category,
    get_last_succeeded_window_begin,
    mark_window_failed,
    mark_window_running,
    mark_window_succeeded,
    normalize_raw_notices,
    normalize_raw_notices_once,
    write_records,
    write_records_value,
)
from apps.shared.data_pipeline.app.steps.g2b_bid import (
    BASE_URLS,
    G2BIngestWindow,
    compute_due_hourly_windows,
)


@flow(name="g2b-bid-hourly-ingest")
def g2b_bid_hourly_ingest() -> dict[str, Any]:
    last_succeeded_begin = get_last_succeeded_window_begin()
    windows = compute_due_windows(last_succeeded_begin)
    results = []
    for window in windows:
        results.append(_process_window(window, use_prefect_tasks=True))
    return {
        "last_succeeded_begin": last_succeeded_begin,
        "window_count": len(windows),
        "results": results,
    }


def _process_window(window: dict[str, str], *, use_prefect_tasks: bool) -> dict[str, Any]:
    ingest_window = G2BIngestWindow(begin=window["begin"], end=window["end"])
    if use_prefect_tasks:
        mark_window_running(ingest_window)
    else:
        mark_window_running.fn(ingest_window)

    raw_counts: dict[str, int] = {}
    try:
        if use_prefect_tasks:
            raw_counts = _ingest_window_raw_parallel(ingest_window)
        else:
            raw_counts = _ingest_window_raw_sync(ingest_window)
        if use_prefect_tasks:
            normalized_result = normalize_raw_notices(
                window_begin=window["begin"],
                window_end=window["end"],
            )
            mark_window_succeeded(
                ingest_window,
                raw_count=sum(raw_counts.values()),
                normalized_count=normalized_result["target"]["count"],
            )
        else:
            normalized_result = normalize_raw_notices_once(
                window_begin=window["begin"],
                window_end=window["end"],
            )
            mark_window_succeeded.fn(
                ingest_window,
                raw_count=sum(raw_counts.values()),
                normalized_count=normalized_result["target"]["count"],
            )
        return {
            "window": window,
            "raw": {
                "window": window,
                "counts": raw_counts,
                "total": sum(raw_counts.values()),
            },
            "normalized": normalized_result,
        }
    except Exception as exc:
        if use_prefect_tasks:
            mark_window_failed(ingest_window, str(exc))
        else:
            mark_window_failed.fn(ingest_window, str(exc))
        raise


def _ingest_window_raw_parallel(window: G2BIngestWindow) -> dict[str, int]:
    fetch_futures = {
        category: fetch_category.submit(category, window)
        for category in BASE_URLS
    }
    write_futures = {
        category: write_records.submit(category, window, fetch_future)
        for category, fetch_future in fetch_futures.items()
    }
    return {
        category: int(write_future.result())
        for category, write_future in write_futures.items()
    }


def _ingest_window_raw_sync(window: G2BIngestWindow) -> dict[str, int]:
    raw_counts: dict[str, int] = {}
    for category in BASE_URLS:
        records = fetch_category.fn(category, window)
        raw_counts[category] = write_records_value(category=category, window=window, records=records)
    return raw_counts


def run_g2b_bid_hourly_ingest_once(now: datetime | None = None) -> dict[str, Any]:
    last_succeeded_begin = get_last_succeeded_window_begin.fn()
    windows = compute_due_hourly_windows(last_succeeded_begin=last_succeeded_begin, now=now)
    results = [_process_window(window, use_prefect_tasks=False) for window in windows]
    return {
        "last_succeeded_begin": last_succeeded_begin,
        "window_count": len(windows),
        "results": results,
    }



if __name__ == "__main__":
    print(json.dumps(run_g2b_bid_hourly_ingest_once(), ensure_ascii=False))
