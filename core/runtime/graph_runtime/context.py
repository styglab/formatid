from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

from core.contracts.execution.identity import ExecutionIdentity, normalize_execution_identity
from core.runtime.app_service.runtime.run_store import ServiceRunStore
from core.runtime.graph_runtime.state_store import GraphRunStore
from core.runtime.runtime_db.checkpoints import PostgresCheckpointStore


GraphTrigger = Literal["scheduled", "triggered"]


@dataclass(frozen=True)
class GraphRunContext:
    graph_name: str
    trigger: GraphTrigger
    checkpoint_store: PostgresCheckpointStore
    run_store: ServiceRunStore
    graph_run_store: GraphRunStore
    run_id: str
    thread_id: str | None = None
    correlation_id: str | None = None
    resource_key: str | None = None
    request_id: str | None = None
    session_id: str | None = None
    resume_value: Any | None = None
    graph_checkpointer: Any | None = None
    params: dict[str, Any] = field(default_factory=dict)
    trigger_config: dict[str, Any] = field(default_factory=dict)

    @property
    def execution_identity(self) -> ExecutionIdentity:
        return normalize_execution_identity(
            request_id=self.request_id,
            correlation_id=self.correlation_id,
            run_id=self.run_id,
            thread_id=self.thread_id or self.run_id,
            resource_key=self.resource_key,
            session_id=self.session_id,
        )
