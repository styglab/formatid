from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

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
    params: dict[str, Any] = field(default_factory=dict)
    trigger_config: dict[str, Any] = field(default_factory=dict)
