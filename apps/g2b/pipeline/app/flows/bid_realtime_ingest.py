from __future__ import annotations

import os
from typing import Any

from prefect import flow

from apps.g2b.pipeline.app.service.ingest import (
    build_ingest_result,
)
from apps.g2b.pipeline.app.service.lock import (
    g2b_bid_ingest_run_lock,
    skipped_by_running_ingest,
)
from apps.g2b.pipeline.app.steps.bid_notices import BASE_URLS, compute_realtime_window_value
from apps.g2b.pipeline.app.steps.common import G2BIngestWindow
from apps.g2b.pipeline.app.tasks.bid_notices import fetch_category, normalize_raw_notices, write_records
from apps.g2b.pipeline.app.tasks.license_limits import (
    fetch_license_limits,
    normalize_license_limits,
    write_license_limits,
)
from apps.g2b.pipeline.app.tasks.participation_regions import (
    fetch_participation_regions,
    normalize_participation_regions,
    write_participation_regions,
)


@flow(name="g2b-bid-realtime-ingest")
def g2b_bid_realtime_ingest() -> dict[str, Any]:
    lookback_minutes = int(os.getenv("G2B_INGEST_REALTIME_LOOKBACK_MINUTES", "90"))
    window_dict = compute_realtime_window_value(lookback_minutes=lookback_minutes)
    window = G2BIngestWindow(begin=window_dict["begin"], end=window_dict["end"])
    with g2b_bid_ingest_run_lock() as acquired:
        if not acquired:
            return skipped_by_running_ingest(flow="g2b-bid-realtime-ingest", window=window_dict)

        fetch_futures = {
            category: fetch_category.submit(category, window)
            for category in BASE_URLS
        }
        raw_counts = {
            category: int(write_records.submit(category, window, fetch_future).result())
            for category, fetch_future in fetch_futures.items()
        }

        license_records = fetch_license_limits.submit(window)
        license_raw_count = int(write_license_limits.submit(window, license_records).result())

        participation_region_records = fetch_participation_regions.submit(window)
        participation_region_raw_count = int(
            write_participation_regions.submit(window, participation_region_records).result()
        )

        normalized_result = normalize_raw_notices.submit(
            window_begin=window.begin,
            window_end=window.end,
        ).result()
        normalized_license_result = normalize_license_limits.submit(
            window_begin=window.begin,
            window_end=window.end,
        ).result()
        normalized_participation_region_result = normalize_participation_regions.submit(
            window_begin=window.begin,
            window_end=window.end,
        ).result()

    return build_ingest_result(
        flow_name="g2b-bid-realtime-ingest",
        window=window,
        raw_counts=raw_counts,
        license_raw_count=license_raw_count,
        participation_region_raw_count=participation_region_raw_count,
        normalized_result=normalized_result,
        normalized_license_result=normalized_license_result,
        normalized_participation_region_result=normalized_participation_region_result,
    )
