from __future__ import annotations

import os

from prefect.client.schemas.objects import ConcurrencyLimitStrategy
from prefect.deployments.runner import ConcurrencyLimitConfig

from services.semantic_platform.worker.flows.source_ingestion import semantic_platform_source_ingestion


def _process_job_variables() -> dict[str, object]:
    return {
        "working_dir": os.getenv("PREFECT_FLOW_WORKING_DIR", "/app"),
        "env": {
            "PREFECT_HOME": os.getenv("PREFECT_HOME", "/tmp/prefect"),
            "LLM_MODE": os.getenv("LLM_MODE", "disabled"),
            "SEMANTIC_PLATFORM_LLM_MODE": os.getenv("SEMANTIC_PLATFORM_LLM_MODE", os.getenv("LLM_MODE", "disabled")),
        },
    }


def deploy_source_ingestion() -> None:
    deployment = semantic_platform_source_ingestion.to_deployment(
        name="manual",
        work_pool_name=os.getenv("PREFECT_WORK_POOL", "semantic-platform-worker-pool"),
        concurrency_limit=ConcurrencyLimitConfig(
            limit=1,
            collision_strategy=ConcurrencyLimitStrategy.CANCEL_NEW,
        ),
        job_variables=_process_job_variables(),
    )
    deployment.apply()


def deploy_all() -> None:
    deploy_source_ingestion()


if __name__ == "__main__":
    deploy_all()
