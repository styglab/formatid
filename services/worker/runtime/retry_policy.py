from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from shared.tasking.catalog import get_task_definition
from shared.tasking.errors import (
    InvalidTaskPayloadError,
    InvalidTaskRouteError,
    UnknownTaskError,
    UnknownTaskRoutingError,
    WorkerTaskNotAllowedError,
)


FailureAction = Literal["retry", "dead_letter", "fail"]


@dataclass(frozen=True)
class TaskPolicy:
    max_retries: int
    payload_schema: str | None
    retryable: bool
    backoff_seconds: int
    timeout_seconds: int
    dlq_enabled: bool

    def to_snapshot(self) -> dict[str, int | bool | str | None]:
        return {
            "max_retries": self.max_retries,
            "payload_schema": self.payload_schema,
            "retryable": self.retryable,
            "backoff_seconds": self.backoff_seconds,
            "timeout_seconds": self.timeout_seconds,
            "dlq_enabled": self.dlq_enabled,
        }


@dataclass(frozen=True)
class FailureDecision:
    action: FailureAction
    next_attempts: int
    terminal_attempts: int
    retryable: bool


NON_RETRYABLE_ERRORS = (
    InvalidTaskPayloadError,
    InvalidTaskRouteError,
    UnknownTaskRoutingError,
    UnknownTaskError,
    WorkerTaskNotAllowedError,
)


def build_task_policy(
    *,
    task_name: str,
    default_max_retries: int,
    default_backoff_seconds: int,
    default_timeout_seconds: int,
) -> TaskPolicy:
    definition = get_task_definition(task_name)
    return TaskPolicy(
        max_retries=default_max_retries if definition.max_retries is None else definition.max_retries,
        payload_schema=definition.payload_schema,
        retryable=definition.retryable,
        backoff_seconds=(
            default_backoff_seconds
            if definition.backoff_seconds is None
            else definition.backoff_seconds
        ),
        timeout_seconds=(
            default_timeout_seconds
            if definition.timeout_seconds is None
            else definition.timeout_seconds
        ),
        dlq_enabled=definition.dlq_enabled,
    )


def is_retryable_error(exc: Exception) -> bool:
    return not isinstance(exc, NON_RETRYABLE_ERRORS)


def decide_failure_action(*, attempts: int, policy: TaskPolicy, exc: Exception) -> FailureDecision:
    next_attempts = attempts + 1
    retryable = policy.retryable and is_retryable_error(exc)
    if retryable and next_attempts <= policy.max_retries:
        return FailureDecision(
            action="retry",
            next_attempts=next_attempts,
            terminal_attempts=next_attempts,
            retryable=retryable,
        )
    if policy.dlq_enabled:
        return FailureDecision(
            action="dead_letter",
            next_attempts=next_attempts,
            terminal_attempts=next_attempts if retryable else attempts,
            retryable=retryable,
        )
    return FailureDecision(
        action="fail",
        next_attempts=next_attempts,
        terminal_attempts=attempts,
        retryable=retryable,
    )
