from __future__ import annotations

import json
from datetime import timedelta
from typing import Any

from core.runtime.runtime_db.connection import connect
from core.runtime.runtime_db.schema import ensure_external_api_quota_blocks_table
from core.runtime.time import iso_now, now


G2B_INGEST_QUOTA_BLOCK_KEY = "g2b_ingest:api:quota_blocked_until"
G2B_INGEST_QUOTA_APP = "g2b_ingest"
G2B_INGEST_QUOTA_PROVIDER = "data.go.kr"
G2B_INGEST_QUOTA_API_NAME = "g2b-pipeline-openapi"


async def get_quota_block(*, redis_url: str, database_url: str | None = None) -> dict[str, Any] | None:
    if database_url:
        persistent_block = await _get_persistent_quota_block(database_url=database_url)
        if persistent_block is not None:
            await _cache_quota_block(redis_url=redis_url, block=persistent_block)
            return persistent_block

    from redis.asyncio import Redis

    redis = Redis.from_url(redis_url, decode_responses=True)
    try:
        raw_value = await redis.get(G2B_INGEST_QUOTA_BLOCK_KEY)
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
    database_url: str | None = None,
) -> dict[str, Any]:
    blocked_until = _next_quota_reset_buffer()
    payload = {
        "reason": "daily_quota_exceeded",
        "blocked_until": blocked_until.isoformat(),
        "created_at": iso_now(),
        **error_detail,
    }
    if database_url:
        await _upsert_persistent_quota_block(database_url=database_url, block=payload)
    await _cache_quota_block(redis_url=redis_url, block=payload)
    return payload


async def _cache_quota_block(*, redis_url: str, block: dict[str, Any]) -> None:
    from redis.asyncio import Redis

    redis = Redis.from_url(redis_url, decode_responses=True)
    try:
        blocked_until = block.get("blocked_until")
        ttl_seconds = max(1, int((_parse_blocked_until(blocked_until) - now()).total_seconds()))
        await redis.set(G2B_INGEST_QUOTA_BLOCK_KEY, json.dumps(block), ex=ttl_seconds)
    finally:
        await redis.aclose()


async def _get_persistent_quota_block(*, database_url: str) -> dict[str, Any] | None:
    conn = await connect(database_url)
    try:
        await ensure_external_api_quota_blocks_table(conn)
        async with conn.cursor() as cursor:
            await cursor.execute(
                """
                SELECT reason, blocked_until, detail, updated_at
                FROM external_api_quota_blocks
                WHERE app = %s
                  AND provider = %s
                  AND api_name = %s
                  AND blocked_until > NOW()
                """,
                (G2B_INGEST_QUOTA_APP, G2B_INGEST_QUOTA_PROVIDER, G2B_INGEST_QUOTA_API_NAME),
            )
            row = await cursor.fetchone()
    finally:
        await conn.close()
    if row is None:
        return None
    reason, blocked_until, detail, updated_at = row
    payload = dict(detail or {})
    payload.setdefault("reason", reason)
    payload["blocked_until"] = blocked_until.isoformat()
    payload["updated_at"] = updated_at.isoformat()
    return payload


async def _upsert_persistent_quota_block(*, database_url: str, block: dict[str, Any]) -> None:
    conn = await connect(database_url)
    try:
        await ensure_external_api_quota_blocks_table(conn)
        async with conn.cursor() as cursor:
            await cursor.execute(
                """
                INSERT INTO external_api_quota_blocks (
                    app, provider, api_name, reason, blocked_until, detail
                )
                VALUES (%s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (app, provider, api_name)
                DO UPDATE SET
                    reason = EXCLUDED.reason,
                    blocked_until = EXCLUDED.blocked_until,
                    detail = EXCLUDED.detail,
                    updated_at = NOW()
                """,
                (
                    G2B_INGEST_QUOTA_APP,
                    G2B_INGEST_QUOTA_PROVIDER,
                    G2B_INGEST_QUOTA_API_NAME,
                    block.get("reason", "daily_quota_exceeded"),
                    block["blocked_until"],
                    json.dumps(block),
                ),
            )
        await conn.commit()
    finally:
        await conn.close()


def build_quota_error_detail(
    *,
    message: str = "daily G2B Ingest API quota exceeded",
    result_code: Any = None,
    result_msg: Any = None,
    blocked_until: Any = None,
) -> dict[str, Any]:
    detail = {
        "type": "G2bIngestDailyQuotaExceededError",
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


def _parse_blocked_until(value: Any):
    from datetime import datetime

    if hasattr(value, "timestamp"):
        return value
    return datetime.fromisoformat(str(value))
