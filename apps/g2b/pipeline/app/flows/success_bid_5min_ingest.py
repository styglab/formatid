from __future__ import annotations

import os
from typing import Any

from prefect import flow

from apps.g2b.pipeline.app.flows.success_bid_initial_ingest import _build_success_bid_ingest_result
from apps.g2b.pipeline.app.service.lock import (
    g2b_success_bid_ingest_run_lock,
    skipped_by_running_ingest,
)
from apps.g2b.pipeline.app.steps.bid_notices import compute_realtime_window_value
from apps.g2b.pipeline.app.steps.common import G2BIngestWindow
from apps.g2b.pipeline.app.steps.success_bids import SUCCESS_BID_URLS
from apps.g2b.pipeline.app.tasks.companies import sync_award_companies
from apps.g2b.pipeline.app.tasks.success_bids import (
    fetch_success_bid_category,
    normalize_success_bids,
    write_success_bid_records,
)


@flow(name="g2b-success-bid-5min-ingest")
def g2b_success_bid_5min_ingest() -> dict[str, Any]:
    lookback_minutes = int(
        os.getenv(
            "G2B_SUCCESS_BID_5MIN_LOOKBACK_MINUTES",
            os.getenv("G2B_SUCCESS_BID_REALTIME_LOOKBACK_MINUTES", "180"),
        )
    )
    window_dict = compute_realtime_window_value(lookback_minutes=lookback_minutes)
    window = G2BIngestWindow(begin=window_dict["begin"], end=window_dict["end"])
    lock_name = os.getenv("G2B_SUCCESS_BID_INGEST_RUN_LOCK_NAME", "g2b_success_bid_ingest_run")
    with g2b_success_bid_ingest_run_lock() as acquired:
        if not acquired:
            return skipped_by_running_ingest(
                flow="g2b-success-bid-5min-ingest",
                window=window_dict,
                lock_name=lock_name,
            )

        fetch_futures = {
            category: fetch_success_bid_category.submit(category, window)
            for category in SUCCESS_BID_URLS
        }
        raw_counts = {
            category: int(write_success_bid_records.submit(category, window, fetch_future).result())
            for category, fetch_future in fetch_futures.items()
        }
        normalized_result = normalize_success_bids.submit(window_begin=window.begin, window_end=window.end).result()
        companies_result = sync_award_companies.submit(window_begin=window.begin, window_end=window.end).result()

    return _build_success_bid_ingest_result(
        window=window,
        raw_counts=raw_counts,
        normalized_result=normalized_result,
        companies_result=companies_result,
    )
