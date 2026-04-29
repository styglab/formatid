from __future__ import annotations

from typing import TypedDict


class ExecutionIdentity(TypedDict, total=False):
    request_id: str
    correlation_id: str
    run_id: str
    thread_id: str
    task_id: str
    resource_key: str
    artifact_id: str
    session_id: str


def normalize_execution_identity(identity: ExecutionIdentity | None = None, /, **overrides: str | None) -> ExecutionIdentity:
    normalized: ExecutionIdentity = {}
    payload: dict[str, str | None] = {}
    if identity is not None:
        payload.update(identity)
    payload.update(overrides)
    for key in (
        "request_id",
        "correlation_id",
        "run_id",
        "thread_id",
        "task_id",
        "resource_key",
        "artifact_id",
        "session_id",
    ):
        value = payload.get(key)
        if isinstance(value, str) and value:
            normalized[key] = value
    return normalized
