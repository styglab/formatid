from __future__ import annotations

import json
from typing import Any

from redis.asyncio import Redis

from core.runtime.time import iso_now


def queue_pause_key(queue_name: str) -> str:
    return f"task:queue_pause:{queue_name}"


async def pause_queue(
    *,
    redis_url: str,
    queue_name: str,
    reason: str = "manual",
    ttl_seconds: int | None = None,
) -> dict[str, Any]:
    redis = Redis.from_url(redis_url, decode_responses=True)
    payload = {
        "queue_name": queue_name,
        "reason": reason,
        "paused_at": iso_now(),
    }
    try:
        await redis.set(queue_pause_key(queue_name), json.dumps(payload), ex=ttl_seconds)
    finally:
        await redis.aclose()
    return payload


async def resume_queue(*, redis_url: str, queue_name: str) -> bool:
    redis = Redis.from_url(redis_url, decode_responses=True)
    try:
        return bool(await redis.delete(queue_pause_key(queue_name)))
    finally:
        await redis.aclose()


async def get_queue_pause(*, redis_url: str, queue_name: str) -> dict[str, Any] | None:
    redis = Redis.from_url(redis_url, decode_responses=True)
    try:
        raw = await redis.get(queue_pause_key(queue_name))
    finally:
        await redis.aclose()
    if raw is None:
        return None
    return json.loads(raw)


async def is_queue_paused(*, redis_url: str, queue_name: str) -> bool:
    return await get_queue_pause(redis_url=redis_url, queue_name=queue_name) is not None
