from __future__ import annotations

import os

from core.catalog.service_catalog import get_expected_workers
from core.runtime.worker.runtime.health.health import build_health_report
from scripts.ops.common import get_redis_url


async def check_workers(queue_names: list[str]) -> dict:
    from redis.asyncio import Redis
    from core.runtime.worker.runtime.health.store import WorkerHeartbeatStore

    redis_url = get_redis_url()
    heartbeat_interval = int(os.getenv("WORKER_HEARTBEAT_INTERVAL", "10"))
    heartbeat_ttl = int(os.getenv("WORKER_HEARTBEAT_TTL", "30"))
    redis = Redis.from_url(redis_url, decode_responses=True)
    heartbeat_store = WorkerHeartbeatStore(redis_url=redis_url, ttl_seconds=heartbeat_ttl)

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
        heartbeat_interval_seconds=heartbeat_interval,
        heartbeat_ttl_seconds=heartbeat_ttl,
        expected_workers=get_expected_workers(),
    )
    report["redis_url"] = redis_url
    return report


def build_workers_summary(report: dict) -> dict:
    services_summary: dict[str, dict] = {}
    for queue_name, payload in report.get("queues", {}).items():
        services_summary[queue_name] = {
            "status": payload.get("status"),
            "queue_size": payload.get("size"),
            "workers": payload.get("observed_workers"),
        }
    return {
        "evaluated_at": report.get("evaluated_at"),
        "services": services_summary,
    }


def render_workers_table(report: dict) -> str:
    headers = ["SERVICE", "STATUS", "QUEUE_SIZE", "WORKERS", "HEALTHY", "STALE", "DOWN"]
    rows = []
    for queue_name, payload in report.get("queues", {}).items():
        rows.append(
            [
                queue_name,
                str(payload.get("status", "")),
                str(payload.get("size", "")),
                str(payload.get("observed_workers", "")),
                str(payload.get("healthy_workers", "")),
                str(payload.get("stale_workers", "")),
                str(payload.get("down_workers", "")),
            ]
        )

    widths = [
        max(len(headers[index]), *(len(row[index]) for row in rows)) if rows else len(headers[index])
        for index in range(len(headers))
    ]

    def format_row(row: list[str]) -> str:
        return "  ".join(value.ljust(widths[index]) for index, value in enumerate(row))

    lines = [format_row(headers)]
    lines.extend(format_row(row) for row in rows)
    return "\n".join(lines)
