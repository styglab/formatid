from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from core.runtime.task_runtime.schemas import TaskMessage


@dataclass(slots=True)
class TaskContext:
    message: TaskMessage
    service_name: str | None
    worker_id: str
    deadline_at: datetime | None
    execution_store: Any

    @property
    def task_id(self) -> str:
        return self.message.task_id

    @property
    def queue_name(self) -> str:
        return self.message.queue_name

    @property
    def task_name(self) -> str:
        return self.message.task_name

    @property
    def attempt(self) -> int:
        return self.message.attempts

    @property
    def dedupe_key(self) -> str | None:
        return self.message.dedupe_key

    @property
    def correlation_id(self) -> str | None:
        return self.message.correlation_id

    @property
    def resource_key(self) -> str | None:
        return self.message.resource_key

    async def heartbeat(self) -> None:
        await self.execution_store.refresh_lease(
            task_id=self.task_id,
            lease_expires_at=None if self.deadline_at is None else self.deadline_at.isoformat(),
        )

    async def event(
        self,
        status: str,
        *,
        details: dict[str, Any] | None = None,
        error: dict[str, Any] | None = None,
    ) -> None:
        await self.execution_store.record_event(
            task_id=self.task_id,
            queue_name=self.queue_name,
            service_name=self.service_name,
            task_name=self.task_name,
            status=status,
            attempts=self.attempt,
            worker_id=self.worker_id,
            error=error,
            details={
                "dedupe_key": self.dedupe_key,
                "correlation_id": self.correlation_id,
                "resource_key": self.resource_key,
                **(details or {}),
            },
        )
