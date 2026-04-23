from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from core.runtime.app_service.runtime.run_store import ServiceRunStore
from core.runtime.graph_runtime import run_tracked_node
from core.runtime.runtime_db.checkpoints import PostgresCheckpointStore

from apps.g2b.pipeline_scheduler.app.contracts.ingest import BranchStepOutput, G2bIngestState, Notice, NoticesStepOutput

GRAPH_NAME = "ingest_graph"
PROGRESS_TOTAL = 4


NoticeStep = Callable[
    [PostgresCheckpointStore, ServiceRunStore],
    Awaitable[NoticesStepOutput],
]
BranchStep = Callable[
    [ServiceRunStore, list[Notice]],
    Awaitable[BranchStepOutput],
]


@dataclass(frozen=True)
class G2bIngestGraphSteps:
    ingest_bid_notices: NoticeStep
    ingest_bid_attachments: BranchStep
    ingest_bid_result_participants: BranchStep
    ingest_bid_result_winners: BranchStep


def build_g2b_ingest_graph(steps: G2bIngestGraphSteps):
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


def build_g2b_ingest_initial_state() -> dict[str, Any]:
    return {}


def _notices_node(steps: G2bIngestGraphSteps):
    async def node(state: G2bIngestState) -> G2bIngestState:
        async def work() -> NoticesStepOutput:
            return await steps.ingest_bid_notices(state["checkpoint_store"], state["run_store"])

        result = await run_tracked_node(
            state,
            graph_name=GRAPH_NAME,
            node_name="notices",
            progress_total=PROGRESS_TOTAL,
            work=work,
            output_summary=lambda output: {
                "notices": len(output["notices"]),
                "attachment_candidates": len(output["attachment_candidates"]),
                "participant_candidates": len(output["participant_candidates"]),
                "winner_candidates": len(output["winner_candidates"]),
            },
        )
        notices = result["notices"]
        return {
            "notices": notices,
            "attachment_candidates": result.get("attachment_candidates", notices),
            "participant_candidates": result.get("participant_candidates", notices),
            "winner_candidates": result.get("winner_candidates", notices),
        }

    return node


def _attachments_node(steps: G2bIngestGraphSteps):
    async def node(state: G2bIngestState) -> G2bIngestState:
        candidates = state.get("attachment_candidates", [])

        async def work() -> BranchStepOutput:
            return await steps.ingest_bid_attachments(state["run_store"], candidates)

        await run_tracked_node(
            state,
            graph_name=GRAPH_NAME,
            node_name="attachments",
            progress_total=PROGRESS_TOTAL,
            work=work,
            input_summary={"candidate_count": len(candidates)},
            output_summary=lambda output: {"result_count": len(output)},
        )
        return {}

    return node


def _participants_node(steps: G2bIngestGraphSteps):
    async def node(state: G2bIngestState) -> G2bIngestState:
        candidates = state.get("participant_candidates", [])

        async def work() -> BranchStepOutput:
            return await steps.ingest_bid_result_participants(state["run_store"], candidates)

        await run_tracked_node(
            state,
            graph_name=GRAPH_NAME,
            node_name="participants",
            progress_total=PROGRESS_TOTAL,
            work=work,
            input_summary={"candidate_count": len(candidates)},
            output_summary=lambda output: {"result_count": len(output)},
        )
        return {}

    return node


def _winners_node(steps: G2bIngestGraphSteps):
    async def node(state: G2bIngestState) -> G2bIngestState:
        candidates = state.get("winner_candidates", [])

        async def work() -> BranchStepOutput:
            return await steps.ingest_bid_result_winners(state["run_store"], candidates)

        await run_tracked_node(
            state,
            graph_name=GRAPH_NAME,
            node_name="winners",
            progress_total=PROGRESS_TOTAL,
            work=work,
            input_summary={"candidate_count": len(candidates)},
            output_summary=lambda output: {"result_count": len(output)},
        )
        return {}

    return node
