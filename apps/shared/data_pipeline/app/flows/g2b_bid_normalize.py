from __future__ import annotations

from typing import Any

from prefect import flow

from apps.shared.data_pipeline.app.prefect_tasks.g2b_bid import normalize_raw_notices
from apps.shared.data_pipeline.app.prefect_tasks.g2b_bid import normalize_raw_notices_once


@flow(name="g2b-bid-normalize")
def g2b_bid_normalize() -> dict[str, Any]:
    return normalize_raw_notices()


if __name__ == "__main__":
    print(normalize_raw_notices_once())
