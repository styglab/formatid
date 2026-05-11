from redis.asyncio import Redis

from core.runtime.app_service.runtime.health.health import build_service_health_report
from core.runtime.app_service.runtime.health.store import ServiceHeartbeatStore
from core.runtime.time import iso_now
from services.platform_api.app.config import get_settings
from services.platform_api.app.schemas.health import HealthResponse, ReadinessResponse, RedisHealth


async def get_app_services_health_report() -> dict:
    settings = get_settings()
    heartbeat_store = ServiceHeartbeatStore(
        redis_url=settings.redis_url,
        ttl_seconds=settings.service_heartbeat_ttl,
    )
    try:
        app_services = await heartbeat_store.list_services()
    finally:
        await heartbeat_store.close()

    report = build_service_health_report(
        services=app_services,
        heartbeat_interval_seconds=settings.service_heartbeat_interval,
        heartbeat_ttl_seconds=settings.service_heartbeat_ttl,
    )
    report["redis_url"] = settings.redis_url
    return report


async def build_health_summary() -> HealthResponse:
    readiness = await build_readiness()
    if readiness.status == "not_ready":
        return HealthResponse(
            status="down",
            evaluated_at=readiness.evaluated_at,
            redis=readiness.redis,
            app_services=readiness.app_services,
        )
    return HealthResponse(
        status=(
            "healthy"
            if readiness.app_services is None or readiness.app_services.status == "not_configured"
            else readiness.app_services.status
        ),
        evaluated_at=readiness.evaluated_at,
        redis=readiness.redis,
        app_services=readiness.app_services,
    )


async def build_readiness() -> ReadinessResponse:
    settings = get_settings()
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    try:
        await redis.ping()
        app_service_report = await get_app_services_health_report()
    except Exception as exc:
        return ReadinessResponse(
            status="not_ready",
            evaluated_at=iso_now(),
            redis=RedisHealth(ok=False, url=settings.redis_url, error=str(exc)),
            app_services=None,
        )
    finally:
        await redis.aclose()

    return ReadinessResponse(
        status="ready",
        evaluated_at=app_service_report["evaluated_at"],
        redis=RedisHealth(ok=True, url=settings.redis_url, error=None),
        app_services={
            "status": app_service_report["status"],
            "services": app_service_report["service_count"],
        },
    )
