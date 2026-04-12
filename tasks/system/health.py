import os

from redis.asyncio import Redis

from shared.tasking.registry import task
from shared.tasking.schemas import TaskMessage, TaskResult
from shared.worker_health.health import DEFAULT_EXPECTED_WORKERS, build_health_report
from shared.worker_health.store import WorkerHeartbeatStore


@task("system.health.check")
async def health_check(message: TaskMessage) -> TaskResult:
    redis_url = os.getenv("WORKER_REDIS_URL", "redis://localhost:6379/0")
    heartbeat_interval = int(os.getenv("WORKER_HEARTBEAT_INTERVAL", "10"))
    heartbeat_ttl = int(os.getenv("WORKER_HEARTBEAT_TTL", "30"))
    redis = Redis.from_url(redis_url, decode_responses=True)
    heartbeat_store = WorkerHeartbeatStore(redis_url=redis_url, ttl_seconds=heartbeat_ttl)
    queue_names = list(DEFAULT_EXPECTED_WORKERS)

    try:
        redis_ok = bool(await redis.ping())
        queue_sizes = {
            queue_name: int(await redis.llen(queue_name))
            for queue_name in queue_names
        }
        workers = await heartbeat_store.list_workers()
    finally:
        await heartbeat_store.close()
        await redis.aclose()

    health_report = build_health_report(
        queue_names=queue_names,
        workers=workers,
        queue_sizes=queue_sizes,
        heartbeat_interval_seconds=heartbeat_interval,
        heartbeat_ttl_seconds=heartbeat_ttl,
    )

    return TaskResult(
        task_id=message.task_id,
        task_name=message.task_name,
        status="succeeded",
        output={
            "redis": {
                "url": redis_url,
                "ok": redis_ok,
                "queue_sizes": queue_sizes,
            },
            "health": health_report,
            "payload": message.payload,
        },
    )
