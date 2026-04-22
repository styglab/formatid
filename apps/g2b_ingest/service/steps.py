from __future__ import annotations

from typing import Any

from services.app_service.runtime.run_store import ServiceRunStore
from shared.checkpoints.postgres import PostgresCheckpointStore

from apps.g2b_ingest.service.bid_list_ingest import enqueue_bid_list_if_due
from apps.g2b_ingest.service.downstream import build_downstream_plan, enqueue_downstream_with_own_run_store
from apps.g2b_ingest.service.graph import G2bIngestDagSteps, Notice
from apps.g2b_ingest.service.normalizer import normalize_generic_api_ingest, normalize_generic_file_ingest


def build_g2b_ingest_dag_steps() -> G2bIngestDagSteps:
    return G2bIngestDagSteps(
        ingest_bid_notices=ingest_bid_notices_step,
        ingest_bid_attachments=ingest_bid_attachments_step,
        ingest_bid_result_participants=ingest_bid_result_participants_step,
        ingest_bid_result_winners=ingest_bid_result_winners_step,
    )


async def ingest_bid_notices_step(
    checkpoint_store: PostgresCheckpointStore,
    run_store: ServiceRunStore,
) -> dict[str, Any]:
    await enqueue_bid_list_if_due(checkpoint_store=checkpoint_store, run_store=run_store)
    await normalize_generic_api_ingest(sources={"g2b_ingest_bid_list"})
    return await build_downstream_plan()


async def ingest_bid_attachments_step(
    run_store: ServiceRunStore,
    candidates: list[Notice],
) -> list[dict[str, Any]]:
    await normalize_generic_file_ingest()
    return await enqueue_downstream_with_own_run_store(
        job_type="attachment",
        candidates=candidates,
    )


async def ingest_bid_result_participants_step(
    run_store: ServiceRunStore,
    candidates: list[Notice],
) -> list[dict[str, Any]]:
    await normalize_generic_api_ingest(sources={"g2b_ingest_participants"})
    return await enqueue_downstream_with_own_run_store(
        job_type="participants",
        candidates=candidates,
    )


async def ingest_bid_result_winners_step(
    run_store: ServiceRunStore,
    candidates: list[Notice],
) -> list[dict[str, Any]]:
    await normalize_generic_api_ingest(sources={"g2b_ingest_winners"})
    return await enqueue_downstream_with_own_run_store(
        job_type="winners",
        candidates=candidates,
    )
