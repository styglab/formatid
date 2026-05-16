from __future__ import annotations

import os
from typing import Any

from prefect import flow

from apps.g2b.pipeline.app.flows.contract_window_ingest import run_contract_window_ingest
from apps.g2b.pipeline.app.steps.bid_notices import compute_realtime_window_value


@flow(name="g2b-contract-hourly-ingest")
def g2b_contract_hourly_ingest() -> dict[str, Any]:
    lookback_minutes = int(
        os.getenv(
            "G2B_CONTRACT_HOURLY_LOOKBACK_MINUTES",
            os.getenv("G2B_CONTRACT_REALTIME_LOOKBACK_MINUTES", "360"),
        )
    )
    window_dict = compute_realtime_window_value(lookback_minutes=lookback_minutes)
    return run_contract_window_ingest(
        flow_name="g2b-contract-hourly-ingest",
        begin=window_dict["begin"],
        end=window_dict["end"],
    )
