from __future__ import annotations

from core.runtime.app_service.runtime.run_store import ServiceRunStore
from core.runtime.runtime_db.checkpoints import PostgresCheckpointStore

from apps.g2b.pipeline_scheduler.app.contracts.ingest import BranchStepOutput, Notice, NoticesStepOutput
from apps.g2b.pipeline_scheduler.app.steps.bid_list_ingest import enqueue_bid_list_if_due
from apps.g2b.pipeline_scheduler.app.steps.downstream import build_downstream_plan, enqueue_downstream_with_own_run_store
from apps.g2b.pipeline_scheduler.app.graph.ingest_graph import G2bIngestGraphSteps
from apps.g2b.pipeline_scheduler.app.steps.normalizer import normalize_generic_api_ingest, normalize_generic_file_ingest


def build_g2b_ingest_graph_steps() -> G2bIngestGraphSteps:
    return G2bIngestGraphSteps(
        ingest_bid_notices=ingest_bid_notices_step,
        ingest_bid_attachments=ingest_bid_attachments_step,
        ingest_bid_result_participants=ingest_bid_result_participants_step,
        ingest_bid_result_winners=ingest_bid_result_winners_step,
    )


async def ingest_bid_notices_step(
    checkpoint_store: PostgresCheckpointStore,
    run_store: ServiceRunStore,
) -> NoticesStepOutput:
    await enqueue_bid_list_if_due(checkpoint_store=checkpoint_store, run_store=run_store)
    await normalize_generic_api_ingest(sources={"g2b_ingest_bid_list"})
    return await build_downstream_plan()


async def ingest_bid_attachments_step(
    run_store: ServiceRunStore,
    candidates: list[Notice],
) -> BranchStepOutput:
    await normalize_generic_file_ingest()
    return await enqueue_downstream_with_own_run_store(
        job_type="attachment",
        candidates=candidates,
    )


async def ingest_bid_result_participants_step(
    run_store: ServiceRunStore,
    candidates: list[Notice],
) -> BranchStepOutput:
    await normalize_generic_api_ingest(sources={"g2b_ingest_participants"})
    return await enqueue_downstream_with_own_run_store(
        job_type="participants",
        candidates=candidates,
    )


async def ingest_bid_result_winners_step(
    run_store: ServiceRunStore,
    candidates: list[Notice],
) -> BranchStepOutput:
    await normalize_generic_api_ingest(sources={"g2b_ingest_winners"})
    return await enqueue_downstream_with_own_run_store(
        job_type="winners",
        candidates=candidates,
    )
