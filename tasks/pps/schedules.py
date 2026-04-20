from __future__ import annotations

import math
import os
from datetime import timedelta
from typing import Any

from shared.queue.redis import RedisTaskQueue
from shared.time import now
from tasks.pps.config import get_settings
from tasks.pps.job_keys import build_bid_list_page_job_key, build_bid_list_window_job_key
from tasks.pps.quota import get_quota_block
from tasks.pps.state import PpsTaskStateStore


def _get_mode() -> str:
    return os.getenv("PPS_SCHEDULER_MODE", "auto").strip().lower()


def _get_backfill_start() -> str:
    value = os.getenv("PPS_BACKFILL_START", "").strip()
    if not value:
        raise RuntimeError("PPS_BACKFILL_START is required for PPS scheduler")
    return value


def _get_window_minutes() -> int:
    return int(os.getenv("PPS_WINDOW_MINUTES", "1440"))


def _get_incremental_lookback_minutes() -> int:
    return int(os.getenv("PPS_INCREMENTAL_LOOKBACK_MINUTES", "120"))


def _get_bid_queue_target() -> int:
    return int(os.getenv("PPS_BID_QUEUE_TARGET", "10"))


async def build_bid_list_payload(*, definition, checkpoint_store) -> dict[str, Any] | None:
    settings = get_settings()
    if await get_quota_block(redis_url=settings.redis_url) is not None:
        return None

    if await _is_bid_queue_full(queue_name=definition.queue_name):
        return None

    checkpoint = await checkpoint_store.get(f"schedule:{definition.name}")
    database_url = os.getenv(
        "PPS_DATABASE_URL",
        os.getenv("CHECKPOINT_DATABASE_URL", "postgresql://formatid:formatid@postgres:5432/formatid"),
    )
    state_store = PpsTaskStateStore(database_url=database_url)
    try:
        if checkpoint is not None:
            last_payload = checkpoint.get("value", {}).get("payload", {})
            if isinstance(last_payload, dict):
                inqry_bgn_dt = str(last_payload.get("inqryBgnDt", ""))
                inqry_end_dt = str(last_payload.get("inqryEndDt", ""))
                if "pageNo" not in last_payload:
                    last_window_state = await state_store.get_state(
                        job_type="bid_list_window",
                        job_key=build_bid_list_window_job_key(
                            inqry_bgn_dt=inqry_bgn_dt,
                            inqry_end_dt=inqry_end_dt,
                        ),
                    )
                    if last_window_state is not None and last_window_state.get("status") == "failed":
                        return {
                            "inqryBgnDt": inqry_bgn_dt,
                            "inqryEndDt": inqry_end_dt,
                            "pageNo": 1,
                        }
                    if last_window_state is not None and last_window_state.get("status") in {"queued", "running"}:
                        return None
                    if last_window_state is None:
                        return None
                else:
                    page_no = int(last_payload.get("pageNo", 1))
                    last_state = await state_store.get_state(
                        job_type="bid_list_page",
                        job_key=build_bid_list_page_job_key(
                            inqry_bgn_dt=inqry_bgn_dt,
                            inqry_end_dt=inqry_end_dt,
                            page_no=page_no,
                        ),
                    )
                    if last_state is None:
                        return None
                    if last_state.get("status") == "failed":
                        return {
                            "inqryBgnDt": inqry_bgn_dt,
                            "inqryEndDt": inqry_end_dt,
                            "pageNo": page_no,
                        }
                    if last_state.get("status") in {"queued", "running"}:
                        return None

                    active_window = await checkpoint_store.get(
                        _bid_list_window_checkpoint_name(
                            inqry_bgn_dt=inqry_bgn_dt,
                            inqry_end_dt=inqry_end_dt,
                        )
                    )
                    active_value = active_window.get("value", {}) if active_window is not None else {}
                    if isinstance(active_value, dict) and active_value.get("status") != "succeeded":
                        next_page_no = active_value.get("next_page_no")
                        if next_page_no is not None:
                            return {
                                "inqryBgnDt": inqry_bgn_dt,
                                "inqryEndDt": inqry_end_dt,
                                "pageNo": int(next_page_no),
                            }

        mode = _get_mode()
        if mode == "incremental":
            return _build_incremental_window()
        if mode == "backfill":
            return _build_backfill_window(checkpoint=checkpoint)

        backfill_payload = _build_backfill_window(checkpoint=checkpoint)
        if _window_end_to_epoch_minutes(backfill_payload["inqryEndDt"]) < _now_epoch_minutes():
            return backfill_payload
        return _build_incremental_window()
    finally:
        await state_store.close()


async def _is_bid_queue_full(*, queue_name: str) -> bool:
    target = _get_bid_queue_target()
    if target <= 0:
        return False

    settings = get_settings()
    queue = RedisTaskQueue(redis_url=settings.redis_url, queue_name=queue_name)
    try:
        return await queue.size() >= target
    finally:
        await queue.close()


def _build_backfill_window(*, checkpoint: dict[str, Any] | None) -> dict[str, Any]:
    window_minutes = _get_window_minutes()
    if checkpoint is None:
        start = _parse_yyyymmddhhmm(_get_backfill_start())
    else:
        payload = checkpoint.get("value", {}).get("payload", {})
        last_end = _parse_yyyymmddhhmm(str(payload.get("inqryEndDt", _get_backfill_start())))
        start = last_end + timedelta(minutes=1)
    end = start + timedelta(minutes=window_minutes) - timedelta(minutes=1)
    return {
        "inqryBgnDt": start.strftime("%Y%m%d%H%M"),
        "inqryEndDt": end.strftime("%Y%m%d%H%M"),
        "pageNo": 1,
    }


def _build_incremental_window() -> dict[str, Any]:
    current = now().replace(second=0, microsecond=0)
    lookback = timedelta(minutes=_get_incremental_lookback_minutes())
    start = current - lookback
    return {
        "inqryBgnDt": start.strftime("%Y%m%d%H%M"),
        "inqryEndDt": current.strftime("%Y%m%d%H%M"),
        "pageNo": 1,
    }


def _parse_yyyymmddhhmm(value: str):
    from datetime import datetime

    return datetime.strptime(value, "%Y%m%d%H%M").replace(tzinfo=now().tzinfo)


def _window_end_to_epoch_minutes(value: str) -> int:
    return math.floor(_parse_yyyymmddhhmm(value).timestamp() / 60)


def _now_epoch_minutes() -> int:
    return math.floor(now().timestamp() / 60)


def _bid_list_window_checkpoint_name(*, inqry_bgn_dt: str, inqry_end_dt: str) -> str:
    return f"pps:bid_list_window:{inqry_bgn_dt}:{inqry_end_dt}"
