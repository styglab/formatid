import json
import os
from socket import gethostname

from core.runtime.time import iso_now


class WorkerHeartbeatStore:
    def __init__(self, *, redis_url: str, ttl_seconds: int, key_prefix: str = "worker:heartbeat") -> None:
        from redis.asyncio import Redis

        self._redis = Redis.from_url(redis_url, decode_responses=True)
        self.ttl_seconds = ttl_seconds
        self.key_prefix = key_prefix

    def build_key(self, *, queue_name: str, worker_id: str) -> str:
        return f"{self.key_prefix}:{queue_name}:{worker_id}"

    async def publish(
        self,
        *,
        queue_name: str,
        app_name: str,
        worker_id: str,
    ) -> None:
        payload = {
            "worker_id": worker_id,
            "app_name": app_name,
            "queue_name": queue_name,
            "hostname": gethostname(),
            "pid": os.getpid(),
            "status": "healthy",
            "updated_at": iso_now(),
        }
        await self._redis.set(
            self.build_key(queue_name=queue_name, worker_id=worker_id),
            json.dumps(payload),
            ex=self.ttl_seconds,
        )

    async def list_workers(self) -> list[dict]:
        workers: list[dict] = []
        async for key in self._redis.scan_iter(match=f"{self.key_prefix}:*"):
            raw = await self._redis.get(key)
            if raw is None:
                continue
            workers.append(json.loads(raw))
        workers.sort(key=lambda item: (item["queue_name"], item["worker_id"]))
        return workers

    async def close(self) -> None:
        await self._redis.aclose()
