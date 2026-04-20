from redis.asyncio import Redis

from services.api.app.config import get_settings
from services.api.app.schemas.health import HealthResponse, ReadinessResponse, RedisHealth
from shared.scheduler_health.health import build_scheduler_health_report
from shared.scheduler_health.store import SchedulerHeartbeatStore
from shared.service_catalog import get_expected_workers, list_worker_queue_names
from shared.time import iso_now
from shared.worker_health.health import build_health_report
from shared.worker_health.store import WorkerHeartbeatStore


async def get_workers_health_report() -> dict:
    settings = get_settings()
    queue_names = list(list_worker_queue_names())
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    heartbeat_store = WorkerHeartbeatStore(
        redis_url=settings.redis_url,
        ttl_seconds=settings.worker_heartbeat_ttl,
    )

    try:
        queue_sizes = {
            queue_name: int(await redis.llen(queue_name))
            for queue_name in queue_names
        }
        workers = await heartbeat_store.list_workers()
    finally:
        await heartbeat_store.close()
        await redis.aclose()

    report = build_health_report(
        queue_names=queue_names,
        workers=workers,
        queue_sizes=queue_sizes,
        heartbeat_interval_seconds=settings.worker_heartbeat_interval,
        heartbeat_ttl_seconds=settings.worker_heartbeat_ttl,
        expected_workers=get_expected_workers(),
    )
    report["redis_url"] = settings.redis_url
    return report


async def get_scheduler_health_report() -> dict:
    settings = get_settings()
    heartbeat_store = SchedulerHeartbeatStore(
        redis_url=settings.redis_url,
        ttl_seconds=settings.scheduler_heartbeat_ttl,
    )
    try:
        schedulers = await heartbeat_store.list_schedulers()
    finally:
        await heartbeat_store.close()

    report = build_scheduler_health_report(
        schedulers=schedulers,
        heartbeat_interval_seconds=settings.scheduler_heartbeat_interval,
        heartbeat_ttl_seconds=settings.scheduler_heartbeat_ttl,
    )
    report["redis_url"] = settings.redis_url
    return report


async def build_health_summary() -> HealthResponse:
    settings = get_settings()
    readiness = await build_readiness()
    redis_status = readiness.redis

    if readiness.status == "not_ready":
        return HealthResponse(
            status="down",
            evaluated_at=readiness.evaluated_at,
            redis=redis_status,
            services=readiness.services,
            scheduler=readiness.scheduler,
        )

    return HealthResponse(
        status=_summarize_status(
            [service.status for service in readiness.services.values()]
            + ([] if readiness.scheduler is None else [readiness.scheduler.status])
        ),
        evaluated_at=readiness.evaluated_at,
        redis=redis_status,
        services=readiness.services,
        scheduler=readiness.scheduler,
    )


async def build_readiness() -> ReadinessResponse:
    settings = get_settings()

    try:
        worker_report = await get_workers_health_report()
        scheduler_report = await get_scheduler_health_report()
    except Exception as exc:
        return ReadinessResponse(
            status="not_ready",
            evaluated_at=iso_now(),
            redis=RedisHealth(ok=False, url=settings.redis_url, error=str(exc)),
            services={},
            scheduler=None,
        )

    services = {
        queue_name: {
            "status": details["status"],
            "queue_size": details["size"],
            "workers": details["observed_workers"],
        }
        for queue_name, details in worker_report["queues"].items()
    }
    scheduler = {
        "status": scheduler_report["status"],
        "schedulers": scheduler_report["scheduler_count"],
    }

    return ReadinessResponse(
        status="ready",
        evaluated_at=worker_report["evaluated_at"],
        redis=RedisHealth(ok=True, url=settings.redis_url, error=None),
        services=services,
        scheduler=scheduler,
    )


def _summarize_status(statuses: list[str]) -> str:
    if not statuses:
        return "down"
    if any(status == "down" for status in statuses):
        return "down"
    if any(status == "degraded" for status in statuses):
        return "degraded"
    return "healthy"
