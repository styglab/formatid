from __future__ import annotations

from typing import Any, Literal, TypedDict


GraphInterruptKind = Literal["task_completion", "tool_result", "human_input", "external_event"]


class GraphInterruptPayload(TypedDict, total=False):
    kind: GraphInterruptKind
    task_id: str
    message: str
    metadata: dict[str, Any]


class GraphResumePayload(TypedDict, total=False):
    task_id: str
    status: str
    artifact: dict[str, Any]
    output: dict[str, Any]
    metadata: dict[str, Any]
