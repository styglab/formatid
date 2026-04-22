from __future__ import annotations

import json
import os
import socket
from typing import Any

from shared.time import iso_now


class ServiceHeartbeatStore:
    def __init__(self, *, redis_url: str, ttl_seconds: int, key_prefix: str = "service:heartbeat") -> None:
        from redis.asyncio import Redis

        self._redis = Redis.from_url(redis_url, decode_responses=True)
        self.ttl_seconds = ttl_seconds
        self.key_prefix = key_prefix

    async def publish(self, *, service_id: str, app_name: str) -> None:
        payload = {
            "service_id": service_id,
            "app_name": app_name,
            "hostname": socket.gethostname(),
            "pid": os.getpid(),
            "status": "healthy",
            "updated_at": iso_now(),
        }
        await self._redis.set(self.build_key(service_id), json.dumps(payload), ex=self.ttl_seconds)

    async def list_services(self) -> list[dict[str, Any]]:
        keys = await self._redis.keys(f"{self.key_prefix}:*")
        services: list[dict[str, Any]] = []
        for key in keys:
            raw = await self._redis.get(key)
            if raw is None:
                continue
            services.append(json.loads(raw))
        return services

    async def close(self) -> None:
        await self._redis.aclose()

    def build_key(self, service_id: str) -> str:
        return f"{self.key_prefix}:{service_id}"
