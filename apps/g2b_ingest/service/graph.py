from __future__ import annotations

import operator
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Annotated, Any, TypedDict

from services.app_service.runtime.run_store import ServiceRunStore
from shared.checkpoints.postgres import PostgresCheckpointStore


class Notice(TypedDict):
    bid_ntce_no: str
    bid_ntce_ord: str


class Attachment(TypedDict, total=False):
    bid_ntce_no: str
    bid_ntce_ord: str
    status: str
    candidate_count: int
    enqueued_count: int
    skip_reason: str | None


class Participant(TypedDict, total=False):
    bid_ntce_no: str
    status: str
    candidate_count: int
    enqueued_count: int
    skip_reason: str | None


class Winner(TypedDict, total=False):
    bid_ntce_no: str
    status: str
    candidate_count: int
    enqueued_count: int
    skip_reason: str | None


class G2bIngestState(TypedDict, total=False):
    checkpoint_store: PostgresCheckpointStore
    run_store: ServiceRunStore
    graph: dict[str, Any]

    notices: list[Notice]
    attachment_candidates: list[Notice]
    participant_candidates: list[Notice]
    winner_candidates: list[Notice]

    attachments: Annotated[list[Attachment], operator.add]
    participants: Annotated[list[Participant], operator.add]
    winners: Annotated[list[Winner], operator.add]

    completed_nodes: Annotated[list[str], operator.add]


NoticeStep = Callable[
    [PostgresCheckpointStore, ServiceRunStore],
    Awaitable[dict[str, Any]],
]
BranchStep = Callable[
    [ServiceRunStore, list[Notice]],
    Awaitable[list[dict[str, Any]]],
]


@dataclass(frozen=True)
class G2bIngestDagSteps:
    ingest_bid_notices: NoticeStep
    ingest_bid_attachments: BranchStep
    ingest_bid_result_participants: BranchStep
    ingest_bid_result_winners: BranchStep


def build_g2b_ingest_graph(steps: G2bIngestDagSteps):
    from langgraph.graph import END, START, StateGraph

    graph = StateGraph(G2bIngestState)
    graph.add_node("notices", _notices_node(steps))
    graph.add_node("attachments", _attachments_node(steps))
    graph.add_node("participants", _participants_node(steps))
    graph.add_node("winners", _winners_node(steps))

    graph.add_edge(START, "notices")
    graph.add_edge("notices", "attachments")
    graph.add_edge("notices", "participants")
    graph.add_edge("notices", "winners")
    graph.add_edge("attachments", END)
    graph.add_edge("participants", END)
    graph.add_edge("winners", END)
    return graph.compile()


async def run_g2b_ingest_dag(
    *,
    checkpoint_store: PostgresCheckpointStore,
    run_store: ServiceRunStore,
    steps: G2bIngestDagSteps,
) -> dict[str, Any]:
    graph = build_g2b_ingest_graph(steps)
    initial_state = {
        "checkpoint_store": checkpoint_store,
        "run_store": run_store,
        "graph": build_g2b_ingest_graph_definition(),
        "completed_nodes": [],
        "attachments": [],
        "participants": [],
        "winners": [],
    }
    return await graph.ainvoke(initial_state)


def build_g2b_ingest_graph_definition() -> dict[str, Any]:
    return {
        "engine": "langgraph",
        "nodes": ["notices", "attachments", "participants", "winners"],
        "edges": [
            ["START", "notices"],
            ["notices", "attachments"],
            ["notices", "participants"],
            ["notices", "winners"],
            ["attachments", "END"],
            ["participants", "END"],
            ["winners", "END"],
        ],
    }


def _notices_node(steps: G2bIngestDagSteps):
    async def node(state: G2bIngestState) -> G2bIngestState:
        result = await steps.ingest_bid_notices(state["checkpoint_store"], state["run_store"])
        notices = result["notices"]
        return {
            "notices": notices,
            "attachment_candidates": result.get("attachment_candidates", notices),
            "participant_candidates": result.get("participant_candidates", notices),
            "winner_candidates": result.get("winner_candidates", notices),
            "completed_nodes": ["notices"],
        }

    return node


def _attachments_node(steps: G2bIngestDagSteps):
    async def node(state: G2bIngestState) -> G2bIngestState:
        result = await steps.ingest_bid_attachments(
            state["run_store"],
            state.get("attachment_candidates", []),
        )
        return {
            "attachments": result,
            "completed_nodes": ["attachments"],
        }

    return node


def _participants_node(steps: G2bIngestDagSteps):
    async def node(state: G2bIngestState) -> G2bIngestState:
        result = await steps.ingest_bid_result_participants(
            state["run_store"],
            state.get("participant_candidates", []),
        )
        return {
            "participants": result,
            "completed_nodes": ["participants"],
        }

    return node


def _winners_node(steps: G2bIngestDagSteps):
    async def node(state: G2bIngestState) -> G2bIngestState:
        result = await steps.ingest_bid_result_winners(
            state["run_store"],
            state.get("winner_candidates", []),
        )
        return {
            "winners": result,
            "completed_nodes": ["winners"],
        }

    return node
