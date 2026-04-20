from typing import Any

from pydantic import BaseModel


class CheckpointEntry(BaseModel):
    name: str
    value: dict[str, Any]
    created_at: str
    updated_at: str


class CheckpointListResponse(BaseModel):
    checkpoints: list[CheckpointEntry]
