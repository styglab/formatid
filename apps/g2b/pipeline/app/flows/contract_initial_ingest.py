from __future__ import annotations

from typing import Any

from prefect import flow

from apps.g2b.pipeline.app.flows.contract_window_ingest import run_contract_window_ingest


@flow(name="g2b-contract-initial-ingest")
def g2b_contract_initial_ingest(
    begin: str = "202605010000",
    end: str = "202605012359",
) -> dict[str, Any]:
    return run_contract_window_ingest(
        flow_name="g2b-contract-initial-ingest",
        begin=begin,
        end=end,
    )
