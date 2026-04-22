from __future__ import annotations

import asyncio
import logging

from services.app_service.runtime.config import get_settings as get_service_settings
from services.app_service.runtime.core import AppServiceRuntime
from services.app_service.runtime.cron import CronJob, CronServiceRunner, env_bool, env_cron, env_int
from services.app_service.runtime.logger import get_logger, log_event
from services.app_service.runtime.run_store import ServiceRunStore
from shared.checkpoints.postgres import PostgresCheckpointStore

from apps.g2b_ingest.service.graph import run_g2b_ingest_dag
from apps.g2b_ingest.service.steps import build_g2b_ingest_dag_steps


logger = get_logger("g2b_ingest.service")


async def run_g2b_ingest_service() -> None:
    service_settings = get_service_settings()
    checkpoint_store = PostgresCheckpointStore(database_url=service_settings.checkpoint_database_url)
    run_store = ServiceRunStore(database_url=service_settings.checkpoint_database_url)
    runtime = AppServiceRuntime(settings=service_settings, logger_name="g2b_ingest.service")
    runtime.add_close_callback(checkpoint_store.close)
    runtime.add_close_callback(run_store.close)
    runner = CronServiceRunner(
        runtime=runtime,
        jobs=[
            CronJob(
                name="g2b_ingest_collect",
                cron=_job_cron(),
                handler=_run_scheduled_once,
                kwargs={"checkpoint_store": checkpoint_store, "run_store": run_store},
                coalesce=_job_coalesce(),
                max_instances=_job_max_instances(),
                misfire_grace_seconds=_job_misfire_grace_seconds(),
                lock_enabled=service_settings.service_lock_enabled,
                lock_ttl_seconds=service_settings.service_lock_ttl_seconds,
            )
        ],
    )
    await runner.run()


async def _run_scheduled_once(*, checkpoint_store: PostgresCheckpointStore, run_store: ServiceRunStore) -> None:
    try:
        await _run_once(checkpoint_store=checkpoint_store, run_store=run_store)
    except Exception:
        logger.exception(
            "g2b_ingest_scheduled_run_failed",
            extra={"extra_fields": {"event": "g2b_ingest_scheduled_run_failed"}},
        )
        raise


async def _run_once(*, checkpoint_store: PostgresCheckpointStore, run_store: ServiceRunStore) -> None:
    graph_state = await run_g2b_ingest_dag(
        checkpoint_store=checkpoint_store,
        run_store=run_store,
        steps=build_g2b_ingest_dag_steps(),
    )
    log_event(
        logger,
        logging.INFO,
        "g2b_ingest_graph_completed",
        completed_nodes=graph_state.get("completed_nodes", []),
        graph=graph_state.get("graph"),
        notices=len(graph_state.get("notices", [])),
        attachment_candidates=len(graph_state.get("attachment_candidates", [])),
        participant_candidates=len(graph_state.get("participant_candidates", [])),
        winner_candidates=len(graph_state.get("winner_candidates", [])),
        attachments=graph_state.get("attachments", []),
        participants=graph_state.get("participants", []),
        winners=graph_state.get("winners", []),
    )


def _job_cron() -> str:
    return env_cron("G2B_INGEST_SERVICE_CRON", "* * * * *")


def _job_misfire_grace_seconds() -> int:
    return env_int("G2B_INGEST_SERVICE_MISFIRE_GRACE_SECONDS", 30)


def _job_max_instances() -> int:
    return env_int("G2B_INGEST_SERVICE_MAX_INSTANCES", 1)


def _job_coalesce() -> bool:
    return env_bool("G2B_INGEST_SERVICE_COALESCE", True)


def main() -> None:
    asyncio.run(run_g2b_ingest_service())


if __name__ == "__main__":
    main()
