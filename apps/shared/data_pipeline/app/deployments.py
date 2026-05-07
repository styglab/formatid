from __future__ import annotations

import os

from prefect.schedules import Cron

from apps.shared.data_pipeline.app.flows.healthcheck import data_pipeline_healthcheck
from apps.shared.data_pipeline.app.flows.g2b_bid_hourly_ingest import g2b_bid_hourly_ingest
from apps.shared.data_pipeline.app.flows.spec_rag_indexing import spec_rag_indexing


def deploy_healthcheck() -> None:
    deployment = data_pipeline_healthcheck.to_deployment(
        name="every-5-minutes",
        work_pool_name=os.getenv("PREFECT_WORK_POOL", "shared-data-pipeline-pool"),
        schedule=Cron("*/5 * * * *", timezone=os.getenv("APP_TIMEZONE", "Asia/Seoul")),
    )
    deployment.apply()


def deploy_spec_rag_indexing() -> None:
    deployment = spec_rag_indexing.to_deployment(
        name="hourly",
        work_pool_name=os.getenv("PREFECT_WORK_POOL", "shared-data-pipeline-pool"),
        schedule=Cron("0 * * * *", timezone=os.getenv("APP_TIMEZONE", "Asia/Seoul")),
    )
    deployment.apply()


def deploy_g2b_bid_hourly_ingest() -> None:
    deployment = g2b_bid_hourly_ingest.to_deployment(
        name="hourly",
        work_pool_name=os.getenv("PREFECT_WORK_POOL", "shared-data-pipeline-pool"),
        schedule=Cron("0 * * * *", timezone=os.getenv("APP_TIMEZONE", "Asia/Seoul")),
    )
    deployment.apply()


def deploy_all() -> None:
    deploy_healthcheck()
    deploy_spec_rag_indexing()
    deploy_g2b_bid_hourly_ingest()



if __name__ == "__main__":
    deploy_all()
