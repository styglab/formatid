from __future__ import annotations

from typing import Any, Literal, TypedDict


ToolCallStatus = Literal["queued", "running", "succeeded", "failed"]


class ToolCall(TypedDict, total=False):
    tool_name: str
    arguments: dict[str, Any]
    correlation_id: str
    resource_key: str


class ToolResult(TypedDict, total=False):
    tool_name: str
    status: ToolCallStatus
    output: dict[str, Any]
    error: dict[str, Any]
    artifact: dict[str, Any]
