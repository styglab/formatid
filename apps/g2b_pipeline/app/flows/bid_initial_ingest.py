from __future__ import annotations

from typing import Any

from prefect import flow

from apps.g2b_pipeline.app.service.ingest import (
    build_ingest_result,
)
from apps.g2b_pipeline.app.service.lock import (
    g2b_bid_ingest_run_lock,
)
from apps.g2b_pipeline.app.steps.bid_notices import BASE_URLS
from apps.g2b_pipeline.app.steps.common import G2BIngestWindow
from apps.g2b_pipeline.app.tasks.bid_notices import fetch_category, normalize_raw_notices, write_records
from apps.g2b_pipeline.app.tasks.license_limits import (
    fetch_license_limits,
    normalize_license_limits,
    write_license_limits,
)
from apps.g2b_pipeline.app.tasks.participation_regions import (
    fetch_participation_regions,
    normalize_participation_regions,
    write_participation_regions,
)


class G2BIngestLockNotAcquired(RuntimeError):
    pass


@flow(name="g2b-bid-initial-ingest")
def g2b_bid_initial_ingest(
    begin: str = "202605040000",
    end: str = "202605042359",
) -> dict[str, Any]:
    window = G2BIngestWindow(begin=begin, end=end)
    window_dict = {"begin": begin, "end": end}
    with g2b_bid_ingest_run_lock() as acquired:
        if not acquired:
            raise G2BIngestLockNotAcquired(
                f"another g2b bid ingest flow is already running for window={window_dict}"
            )

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

        normalized_result = normalize_raw_notices.submit(window_begin=begin, window_end=end).result()
        normalized_license_result = normalize_license_limits.submit(window_begin=begin, window_end=end).result()
        normalized_participation_region_result = normalize_participation_regions.submit(
            window_begin=begin,
            window_end=end,
        ).result()

    return build_ingest_result(
        flow_name="g2b-bid-initial-ingest",
        window=window,
        raw_counts=raw_counts,
        license_raw_count=license_raw_count,
        participation_region_raw_count=participation_region_raw_count,
        normalized_result=normalized_result,
        normalized_license_result=normalized_license_result,
        normalized_participation_region_result=normalized_participation_region_result,
    )
