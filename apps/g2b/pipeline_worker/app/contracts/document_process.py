from __future__ import annotations

import operator
from typing import Annotated, Any, TypedDict

from core.runtime.app_service.runtime.run_store import ServiceRunStore
from core.runtime.graph_runtime import GraphRunStore
from core.runtime.runtime_db.checkpoints import PostgresCheckpointStore


class DocumentProcessInput(TypedDict, total=False):
    document_id: str


class DocumentProcessOutput(TypedDict, total=False):
    document_id: str | None
    status: str
    skip_reason: str | None
    completed_nodes: list[str]


class G2bDocumentProcessState(TypedDict, total=False):
    checkpoint_store: PostgresCheckpointStore
    run_store: ServiceRunStore
    graph_run_store: GraphRunStore
    graph_run_id: str
    params: dict[str, Any]
    document_id: str | None
    status: str
    skip_reason: str | None
    completed_nodes: Annotated[list[str], operator.add]
