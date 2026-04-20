from __future__ import annotations

import json
from datetime import timedelta
from typing import Any

from shared.time import iso_now, now


PPS_QUOTA_BLOCK_KEY = "pps:api:quota_blocked_until"


async def get_quota_block(*, redis_url: str) -> dict[str, Any] | None:
    from redis.asyncio import Redis

    redis = Redis.from_url(redis_url, decode_responses=True)
    try:
        raw_value = await redis.get(PPS_QUOTA_BLOCK_KEY)
    finally:
        await redis.aclose()
    if raw_value is None:
        return None
    try:
        value = json.loads(raw_value)
    except json.JSONDecodeError:
        return {
            "reason": "daily_quota_exceeded",
            "message": raw_value,
        }
    return value if isinstance(value, dict) else None


async def mark_quota_blocked(
    *,
    redis_url: str,
    error_detail: dict[str, Any],
) -> dict[str, Any]:
    from redis.asyncio import Redis

    blocked_until = _next_quota_reset_buffer()
    payload = {
        "reason": "daily_quota_exceeded",
        "blocked_until": blocked_until.isoformat(),
        "created_at": iso_now(),
        **error_detail,
    }
    ttl_seconds = max(1, int((blocked_until - now()).total_seconds()))

    redis = Redis.from_url(redis_url, decode_responses=True)
    try:
        await redis.set(PPS_QUOTA_BLOCK_KEY, json.dumps(payload), ex=ttl_seconds)
    finally:
        await redis.aclose()
    return payload


def build_quota_error_detail(
    *,
    message: str = "daily PPS API quota exceeded",
    result_code: Any = None,
    result_msg: Any = None,
    blocked_until: Any = None,
) -> dict[str, Any]:
    detail = {
        "type": "PpsDailyQuotaExceededError",
        "message": message,
        "reason": "daily_quota_exceeded",
    }
    if result_code is not None:
        detail["result_code"] = result_code
    if result_msg is not None:
        detail["result_msg"] = result_msg
    if blocked_until is not None:
        detail["blocked_until"] = blocked_until
    return detail


def build_quota_skipped_output(
    *,
    payload: dict[str, Any],
    quota_block: dict[str, Any],
) -> dict[str, Any]:
    return {
        **payload,
        "skipped": True,
        "skipped_reason": "daily_quota_exceeded",
        "quota_block": quota_block,
    }


def _next_quota_reset_buffer():
    current = now()
    tomorrow = current.date() + timedelta(days=1)
    return current.replace(
        year=tomorrow.year,
        month=tomorrow.month,
        day=tomorrow.day,
        hour=0,
        minute=5,
        second=0,
        microsecond=0,
    )
