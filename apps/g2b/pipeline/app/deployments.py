from __future__ import annotations

import os

from prefect.client.schemas.objects import ConcurrencyLimitStrategy
from prefect.deployments.runner import ConcurrencyLimitConfig
from prefect.schedules import Cron

from apps.g2b.pipeline.app.flows.bid_initial_ingest import g2b_bid_initial_ingest
from apps.g2b.pipeline.app.flows.bid_5min_ingest import g2b_bid_5min_ingest
from apps.g2b.pipeline.app.flows.contract_initial_ingest import g2b_contract_initial_ingest
from apps.g2b.pipeline.app.flows.contract_daily_reconcile import g2b_contract_daily_reconcile
from apps.g2b.pipeline.app.flows.contract_hourly_ingest import g2b_contract_hourly_ingest
from apps.g2b.pipeline.app.flows.success_bid_initial_ingest import g2b_success_bid_initial_ingest
from apps.g2b.pipeline.app.flows.success_bid_5min_ingest import g2b_success_bid_5min_ingest


def _process_job_variables() -> dict[str, object]:
    return {
        "working_dir": os.getenv("PREFECT_FLOW_WORKING_DIR", "/app"),
        "env": {
            "PREFECT_HOME": os.getenv("PREFECT_HOME", "/tmp/prefect"),
        },
    }


def deploy_g2b_bid_initial_ingest() -> None:
    deployment = g2b_bid_initial_ingest.to_deployment(
        name="manual",
        work_pool_name=os.getenv("PREFECT_WORK_POOL", "g2b-pipeline-pool"),
        concurrency_limit=ConcurrencyLimitConfig(
            limit=1,
            collision_strategy=ConcurrencyLimitStrategy.CANCEL_NEW,
        ),
        job_variables=_process_job_variables(),
    )
    deployment.apply()


def deploy_g2b_bid_5min_ingest() -> None:
    interval_minutes = int(
        os.getenv(
            "G2B_BID_5MIN_INTERVAL_MINUTES",
            os.getenv("G2B_INGEST_REALTIME_INTERVAL_MINUTES", "5"),
        )
    )
    deployment = g2b_bid_5min_ingest.to_deployment(
        name="every-5-minutes",
        work_pool_name=os.getenv("PREFECT_WORK_POOL", "g2b-pipeline-pool"),
        schedule=Cron(f"*/{interval_minutes} * * * *", timezone=os.getenv("APP_TIMEZONE", "Asia/Seoul")),
        concurrency_limit=ConcurrencyLimitConfig(
            limit=1,
            collision_strategy=ConcurrencyLimitStrategy.CANCEL_NEW,
        ),
        job_variables=_process_job_variables(),
    )
    deployment.apply()


def deploy_g2b_success_bid_initial_ingest() -> None:
    deployment = g2b_success_bid_initial_ingest.to_deployment(
        name="manual",
        work_pool_name=os.getenv("PREFECT_WORK_POOL", "g2b-pipeline-pool"),
        concurrency_limit=ConcurrencyLimitConfig(
            limit=1,
            collision_strategy=ConcurrencyLimitStrategy.CANCEL_NEW,
        ),
        job_variables=_process_job_variables(),
    )
    deployment.apply()


def deploy_g2b_success_bid_5min_ingest() -> None:
    interval_minutes = int(
        os.getenv(
            "G2B_SUCCESS_BID_5MIN_INTERVAL_MINUTES",
            os.getenv("G2B_SUCCESS_BID_REALTIME_INTERVAL_MINUTES", "5"),
        )
    )
    deployment = g2b_success_bid_5min_ingest.to_deployment(
        name="every-5-minutes",
        work_pool_name=os.getenv("PREFECT_WORK_POOL", "g2b-pipeline-pool"),
        schedule=Cron(f"*/{interval_minutes} * * * *", timezone=os.getenv("APP_TIMEZONE", "Asia/Seoul")),
        concurrency_limit=ConcurrencyLimitConfig(
            limit=1,
            collision_strategy=ConcurrencyLimitStrategy.CANCEL_NEW,
        ),
        job_variables=_process_job_variables(),
    )
    deployment.apply()


def deploy_g2b_contract_initial_ingest() -> None:
    deployment = g2b_contract_initial_ingest.to_deployment(
        name="manual",
        work_pool_name=os.getenv("PREFECT_WORK_POOL", "g2b-pipeline-pool"),
        concurrency_limit=ConcurrencyLimitConfig(
            limit=1,
            collision_strategy=ConcurrencyLimitStrategy.CANCEL_NEW,
        ),
        job_variables=_process_job_variables(),
    )
    deployment.apply()


def deploy_g2b_contract_hourly_ingest() -> None:
    interval_minutes = int(
        os.getenv(
            "G2B_CONTRACT_HOURLY_INTERVAL_MINUTES",
            os.getenv("G2B_CONTRACT_REALTIME_INTERVAL_MINUTES", "60"),
        )
    )
    cron_expression = "0 * * * *" if interval_minutes == 60 else f"*/{interval_minutes} * * * *"
    deployment = g2b_contract_hourly_ingest.to_deployment(
        name=f"every-{interval_minutes}-minutes",
        work_pool_name=os.getenv("PREFECT_WORK_POOL", "g2b-pipeline-pool"),
        schedule=Cron(cron_expression, timezone=os.getenv("APP_TIMEZONE", "Asia/Seoul")),
        concurrency_limit=ConcurrencyLimitConfig(
            limit=1,
            collision_strategy=ConcurrencyLimitStrategy.CANCEL_NEW,
        ),
        job_variables=_process_job_variables(),
    )
    deployment.apply()


def deploy_g2b_contract_daily_reconcile() -> None:
    deployment = g2b_contract_daily_reconcile.to_deployment(
        name="daily-00-30-kst",
        work_pool_name=os.getenv("PREFECT_WORK_POOL", "g2b-pipeline-pool"),
        schedule=Cron("30 0 * * *", timezone=os.getenv("APP_TIMEZONE", "Asia/Seoul")),
        concurrency_limit=ConcurrencyLimitConfig(
            limit=1,
            collision_strategy=ConcurrencyLimitStrategy.CANCEL_NEW,
        ),
        job_variables=_process_job_variables(),
    )
    deployment.apply()


def deploy_all() -> None:
    deploy_g2b_bid_initial_ingest()
    deploy_g2b_bid_5min_ingest()
    deploy_g2b_success_bid_initial_ingest()
    deploy_g2b_success_bid_5min_ingest()
    deploy_g2b_contract_initial_ingest()
    deploy_g2b_contract_hourly_ingest()
    deploy_g2b_contract_daily_reconcile()


if __name__ == "__main__":
    deploy_all()
