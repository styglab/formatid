from __future__ import annotations

import json
import os
from typing import Any

from prefect import flow

from apps.shared.data_pipeline.app.prefect_tasks.g2b_bid import fetch_category, write_records
from apps.shared.data_pipeline.app.steps.g2b_bid import BASE_URLS, G2BIngestWindow


@flow(name="g2b-bid-initial-ingest")
def g2b_bid_initial_ingest(
    begin: str = "202605040000",
    end: str = "202605042359",
) -> dict[str, Any]:
    return run_g2b_bid_initial_ingest(begin=begin, end=end, use_prefect_tasks=True)


def run_g2b_bid_initial_ingest(
    begin: str = "202605040000",
    end: str = "202605042359",
    *,
    use_prefect_tasks: bool = False,
) -> dict[str, Any]:
    window = G2BIngestWindow(begin=begin, end=end)
    summary: dict[str, int] = {}
    for category in BASE_URLS:
        if use_prefect_tasks:
            records = fetch_category(category, window)
            summary[category] = write_records(category, window, records)
        else:
            records = fetch_category.fn(category, window)
            summary[category] = write_records.fn(category, window, records)
    return {
        "window": {"begin": begin, "end": end},
        "schema": os.getenv("G2B_INGEST_SCHEMA", "g2b"),
        "table": os.getenv("G2B_INGEST_TABLE", "bid_public_notice_raw"),
        "counts": summary,
        "total": sum(summary.values()),
    }


if __name__ == "__main__":
    print(json.dumps(run_g2b_bid_initial_ingest(), ensure_ascii=False))
