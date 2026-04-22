from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any
from uuid import uuid4

from shared.time import now


@dataclass(slots=True)
class TaskMessage:
    queue_name: str
    task_name: str
    payload: dict[str, Any] = field(default_factory=dict)
    attempts: int = 0
    task_id: str = field(default_factory=lambda: str(uuid4()))
    enqueued_at: datetime = field(default_factory=now)
    dedupe_key: str | None = None
    correlation_id: str | None = None
    resource_key: str | None = None

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["enqueued_at"] = self.enqueued_at.isoformat()
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TaskMessage":
        payload = dict(data)
        enqueued_at = payload.get("enqueued_at")
        if isinstance(enqueued_at, str):
            payload["enqueued_at"] = datetime.fromisoformat(enqueued_at)
        return cls(**payload)


@dataclass(slots=True)
class TaskResult:
    task_id: str
    task_name: str
    status: str
    output: dict[str, Any] = field(default_factory=dict)
