from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from prefect import flow

from apps.g2b.pipeline.app.flows.contract_window_ingest import run_contract_window_ingest
from apps.g2b.pipeline.app.steps.common import G2B_TIMEZONE


@flow(name="g2b-contract-daily-reconcile")
def g2b_contract_daily_reconcile(target_date: str | None = None) -> dict[str, Any]:
    reconcile_date = (
        datetime.strptime(target_date, "%Y%m%d").date()
        if target_date
        else (datetime.now(G2B_TIMEZONE).date() - timedelta(days=1))
    )
    day = reconcile_date.strftime("%Y%m%d")
    return run_contract_window_ingest(
        flow_name="g2b-contract-daily-reconcile",
        begin=f"{day}0000",
        end=f"{day}2359",
    )
