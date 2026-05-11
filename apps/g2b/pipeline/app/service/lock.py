from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Iterator

from apps.g2b.pipeline.app.repositories.common import ingest_run_lock


@contextmanager
def g2b_bid_ingest_run_lock() -> Iterator[bool]:
    database_url = os.getenv("G2B_INGEST_DATABASE_URL")
    if not database_url:
        yield True
        return

    lock_name = os.getenv("G2B_INGEST_RUN_LOCK_NAME", "g2b_bid_ingest_run")
    with ingest_run_lock(database_url=database_url, lock_name=lock_name) as acquired:
        yield acquired


@contextmanager
def g2b_success_bid_ingest_run_lock() -> Iterator[bool]:
    database_url = os.getenv("G2B_INGEST_DATABASE_URL")
    if not database_url:
        yield True
        return

    lock_name = os.getenv("G2B_SUCCESS_BID_INGEST_RUN_LOCK_NAME", "g2b_success_bid_ingest_run")
    with ingest_run_lock(database_url=database_url, lock_name=lock_name) as acquired:
        yield acquired


def skipped_by_running_ingest(
    *,
    flow: str,
    window: dict[str, str],
    lock_name: str | None = None,
) -> dict[str, object]:
    return {
        "flow": flow,
        "window": window,
        "skipped": True,
        "reason": "another g2b bid ingest flow is already running",
        "lock_name": lock_name or os.getenv("G2B_INGEST_RUN_LOCK_NAME", "g2b_bid_ingest_run"),
    }
