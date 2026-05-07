from __future__ import annotations

import secrets


class RedisLock:
    def __init__(self, *, redis_url: str, key: str, ttl_seconds: int) -> None:
        self._redis_url = redis_url
        self._key = key
        self._ttl_seconds = ttl_seconds
        self._token = secrets.token_urlsafe(24)
        self._redis = None
        self.acquired = False

    async def __aenter__(self) -> "RedisLock":
        from redis.asyncio import Redis

        self._redis = Redis.from_url(self._redis_url, decode_responses=True)
        self.acquired = bool(
            await self._redis.set(
                self._key,
                self._token,
                ex=self._ttl_seconds,
                nx=True,
            )
        )
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._redis is None:
            return
        if self.acquired:
            await self._redis.eval(
                """
                if redis.call("get", KEYS[1]) == ARGV[1] then
                    return redis.call("del", KEYS[1])
                end
                return 0
                """,
                1,
                self._key,
                self._token,
            )
        await self._redis.aclose()
