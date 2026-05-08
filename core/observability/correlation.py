from __future__ import annotations

from typing import Any

from core.contracts.execution.identity import ExecutionIdentity, normalize_execution_identity


def build_correlation_details(
    *,
    details: dict[str, Any] | None = None,
    identity: ExecutionIdentity | None = None,
    **identity_overrides: str | None,
) -> dict[str, Any]:
    merged = dict(details or {})
    execution_identity = normalize_execution_identity(identity, **identity_overrides)
    if execution_identity:
        merged["execution_identity"] = execution_identity
    return merged
