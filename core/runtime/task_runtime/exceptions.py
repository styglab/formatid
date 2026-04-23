from __future__ import annotations

from datetime import datetime


class TaskRuntimeError(Exception):
    retryable: bool | None = None


class RetryableTaskError(TaskRuntimeError):
    retryable = True


class NonRetryableTaskError(TaskRuntimeError):
    retryable = False


class BlockedTaskError(RetryableTaskError):
    def __init__(self, message: str, *, reason: str = "blocked", blocked_until: datetime | None = None) -> None:
        super().__init__(message)
        self.reason = reason
        self.blocked_until = blocked_until
