from __future__ import annotations

from typing import TypedDict

from core.runtime.app_service.runtime.run_store import ServiceRunStore
from core.runtime.graph_runtime import GraphRunStore
from core.runtime.runtime_db.checkpoints import PostgresCheckpointStore


class Notice(TypedDict):
    bid_ntce_no: str
    bid_ntce_ord: str


class BranchResult(TypedDict, total=False):
    bid_ntce_no: str
    bid_ntce_ord: str
    status: str
    candidate_count: int
    enqueued_count: int
    skip_reason: str | None


class G2bIngestState(TypedDict, total=False):
    checkpoint_store: PostgresCheckpointStore
    run_store: ServiceRunStore
    graph_run_store: GraphRunStore
    graph_run_id: str

    notices: list[Notice]
    attachment_candidates: list[Notice]
    participant_candidates: list[Notice]
    winner_candidates: list[Notice]


class NoticesStepOutput(TypedDict):
    notices: list[Notice]
    attachment_candidates: list[Notice]
    participant_candidates: list[Notice]
    winner_candidates: list[Notice]


BranchStepOutput = list[BranchResult]
