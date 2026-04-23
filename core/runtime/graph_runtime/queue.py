from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any
from uuid import uuid4

from core.runtime.time import now


@dataclass(frozen=True)
class TriggeredGraphRequest:
    graph_name: str
    params: dict[str, Any] = field(default_factory=dict)
    run_id: str = field(default_factory=lambda: uuid4().hex)
    requested_by: str | None = None
    requested_at: str = field(default_factory=lambda: now().isoformat())
    attempts: int = 0

    @classmethod
    def from_json(cls, raw: str) -> "TriggeredGraphRequest":
        data = json.loads(raw)
        return cls(
            graph_name=data["graph_name"],
            params=data.get("params", {}),
            run_id=data.get("run_id") or uuid4().hex,
            requested_by=data.get("requested_by"),
            requested_at=data.get("requested_at") or now().isoformat(),
            attempts=int(data.get("attempts", 0)),
        )

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=True)

    def next_attempt(self) -> "TriggeredGraphRequest":
        return TriggeredGraphRequest(
            graph_name=self.graph_name,
            params=self.params,
            run_id=self.run_id,
            requested_by=self.requested_by,
            requested_at=self.requested_at,
            attempts=self.attempts + 1,
        )

    def requested_datetime(self) -> datetime | None:
        try:
            return datetime.fromisoformat(self.requested_at)
        except ValueError:
            return None


class TriggeredGraphQueue:
    def __init__(self, *, redis_url: str, queue_name: str, timeout_seconds: int = 5) -> None:
        self._redis_url = redis_url
        self._queue_name = queue_name
        self._timeout_seconds = timeout_seconds
        self._redis = None

    @property
    def queue_name(self) -> str:
        return self._queue_name

    @property
    def dlq_name(self) -> str:
        return f"{self._queue_name}:dlq"

    async def start(self) -> None:
        from redis.asyncio import Redis

        self._redis = Redis.from_url(self._redis_url, decode_responses=True)

    async def get(self) -> TriggeredGraphRequest | None:
        if self._redis is None:
            await self.start()
        item = await self._redis.brpop(self._queue_name, timeout=self._timeout_seconds)
        if item is None:
            return None
        _, raw = item
        return TriggeredGraphRequest.from_json(raw)

    async def requeue(self, request: TriggeredGraphRequest) -> None:
        await self.enqueue(request)

    async def enqueue(self, request: TriggeredGraphRequest) -> None:
        if self._redis is None:
            await self.start()
        await self._redis.lpush(self._queue_name, request.to_json())

    async def push_dlq(self, request: TriggeredGraphRequest, *, error: dict[str, Any]) -> None:
        if self._redis is None:
            await self.start()
        payload = {
            "request": asdict(request),
            "error": error,
            "failed_at": now().isoformat(),
        }
        await self._redis.lpush(self.dlq_name, json.dumps(payload, ensure_ascii=True))

    async def close(self) -> None:
        if self._redis is not None:
            await self._redis.aclose()
            self._redis = None
