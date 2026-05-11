from __future__ import annotations

from typing import Any

from prefect import flow

from apps.g2b.pipeline.app.service.lock import g2b_success_bid_ingest_run_lock
from apps.g2b.pipeline.app.steps.common import G2BIngestWindow
from apps.g2b.pipeline.app.steps.success_bids import SUCCESS_BID_URLS
from apps.g2b.pipeline.app.tasks.success_bids import (
    fetch_success_bid_category,
    normalize_success_bids,
    write_success_bid_records,
)


class G2BSuccessBidIngestLockNotAcquired(RuntimeError):
    pass


@flow(name="g2b-success-bid-initial-ingest")
def g2b_success_bid_initial_ingest(
    begin: str = "202605010000",
    end: str = "202605012359",
) -> dict[str, Any]:
    window = G2BIngestWindow(begin=begin, end=end)
    with g2b_success_bid_ingest_run_lock() as acquired:
        if not acquired:
            raise G2BSuccessBidIngestLockNotAcquired(
                f"another g2b success bid ingest flow is already running for window={{'begin': {begin}, 'end': {end}}}"
            )

        fetch_futures = {
            category: fetch_success_bid_category.submit(category, window)
            for category in SUCCESS_BID_URLS
        }
        raw_counts = {
            category: int(write_success_bid_records.submit(category, window, fetch_future).result())
            for category, fetch_future in fetch_futures.items()
        }
        normalized_result = normalize_success_bids.submit(window_begin=begin, window_end=end).result()

    return _build_success_bid_ingest_result(window=window, raw_counts=raw_counts, normalized_result=normalized_result)


def _build_success_bid_ingest_result(
    *,
    window: G2BIngestWindow,
    raw_counts: dict[str, int],
    normalized_result: dict[str, Any],
) -> dict[str, Any]:
    return {
        "window": {"begin": window.begin, "end": window.end},
        "success_bids": {
            "raw": {
                "counts": raw_counts,
                "total": sum(raw_counts.values()),
                "window": {"begin": window.begin, "end": window.end},
            },
            "normalized": normalized_result,
        },
    }
