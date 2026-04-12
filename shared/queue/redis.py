import json
from typing import Protocol

from shared.tasking.schemas import TaskMessage


class TaskQueue(Protocol):
    async def put(self, message: TaskMessage) -> None:
        ...

    async def get(self, *, timeout: int | float | None = None) -> TaskMessage | None:
        ...

    async def size(self) -> int:
        ...

    async def close(self) -> None:
        ...


class RedisTaskQueue:
    def __init__(self, *, redis_url: str, queue_name: str) -> None:
        from redis.asyncio import Redis

        self.queue_name = queue_name
        self._redis = Redis.from_url(redis_url, decode_responses=True)

    async def put(self, message: TaskMessage) -> None:
        await self._redis.rpush(self.queue_name, json.dumps(message.to_dict()))

    async def get(self, *, timeout: int | float | None = None) -> TaskMessage | None:
        block_timeout = 0 if timeout is None else int(timeout)
        response = await self._redis.blpop(self.queue_name, timeout=block_timeout)
        if response is None:
            return None

        _, raw_message = response
        return TaskMessage.from_dict(json.loads(raw_message))

    async def size(self) -> int:
        return int(await self._redis.llen(self.queue_name))

    async def close(self) -> None:
        await self._redis.aclose()
