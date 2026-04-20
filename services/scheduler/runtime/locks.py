from __future__ import annotations


class SchedulerLockStore:
    def __init__(self, *, redis_url: str) -> None:
        from redis.asyncio import Redis

        self._redis = Redis.from_url(redis_url, decode_responses=True)

    async def acquire(self, *, schedule_name: str, owner: str, ttl_seconds: int) -> bool:
        acquired = await self._redis.set(
            _lock_key(schedule_name),
            owner,
            nx=True,
            ex=ttl_seconds,
        )
        return bool(acquired)

    async def release(self, *, schedule_name: str, owner: str) -> bool:
        released = await self._redis.eval(
            """
            if redis.call("get", KEYS[1]) == ARGV[1] then
                return redis.call("del", KEYS[1])
            end
            return 0
            """,
            1,
            _lock_key(schedule_name),
            owner,
        )
        return bool(released)

    async def close(self) -> None:
        await self._redis.aclose()


def _lock_key(schedule_name: str) -> str:
    return f"scheduler:lock:{schedule_name}"
