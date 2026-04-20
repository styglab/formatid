from datetime import datetime
from typing import Any

from shared.service_catalog import get_expected_workers
from shared.time import now


def build_health_report(
    *,
    queue_names: list[str],
    workers: list[dict[str, Any]],
    queue_sizes: dict[str, int],
    heartbeat_interval_seconds: int,
    heartbeat_ttl_seconds: int,
    expected_workers: dict[str, int] | None = None,
) -> dict[str, Any]:
    expected_workers = expected_workers or get_expected_workers()
    current_time = now()
    healthy_threshold_seconds = heartbeat_interval_seconds * 2

    workers_by_queue: dict[str, list[dict[str, Any]]] = {queue_name: [] for queue_name in queue_names}
    for worker in workers:
        queue_name = worker["queue_name"]
        enriched_worker = dict(worker)
        age_seconds = _worker_age_seconds(worker, now=current_time)
        enriched_worker["age_seconds"] = age_seconds
        enriched_worker["health_status"] = classify_worker_status(
            age_seconds=age_seconds,
            healthy_threshold_seconds=healthy_threshold_seconds,
            heartbeat_ttl_seconds=heartbeat_ttl_seconds,
        )
        workers_by_queue.setdefault(queue_name, []).append(enriched_worker)

    queues: dict[str, dict[str, Any]] = {}
    for queue_name in queue_names:
        queue_workers = workers_by_queue.get(queue_name, [])
        observed_workers = len(queue_workers)
        expected_count = expected_workers.get(queue_name, 1)
        healthy_count = sum(1 for worker in queue_workers if worker["health_status"] == "healthy")
        stale_count = sum(1 for worker in queue_workers if worker["health_status"] == "stale")
        down_count = max(expected_count - observed_workers, 0)
        queue_status = classify_queue_status(
            expected_workers=expected_count,
            observed_workers=observed_workers,
            healthy_workers=healthy_count,
            stale_workers=stale_count,
        )
        queues[queue_name] = {
            "size": queue_sizes.get(queue_name, 0),
            "status": queue_status,
            "expected_workers": expected_count,
            "observed_workers": observed_workers,
            "healthy_workers": healthy_count,
            "stale_workers": stale_count,
            "down_workers": down_count,
        }

    return {
        "evaluated_at": current_time.isoformat(),
        "policy": {
            "heartbeat_interval_seconds": heartbeat_interval_seconds,
            "heartbeat_ttl_seconds": heartbeat_ttl_seconds,
            "healthy_threshold_seconds": healthy_threshold_seconds,
        },
        "queues": queues,
        "workers": workers_by_queue,
    }


def classify_worker_status(
    *,
    age_seconds: float,
    healthy_threshold_seconds: int,
    heartbeat_ttl_seconds: int,
) -> str:
    if age_seconds <= healthy_threshold_seconds:
        return "healthy"
    if age_seconds <= heartbeat_ttl_seconds:
        return "stale"
    return "down"


def classify_queue_status(
    *,
    expected_workers: int,
    observed_workers: int,
    healthy_workers: int,
    stale_workers: int,
) -> str:
    if expected_workers > 0 and observed_workers == 0:
        return "down"
    if observed_workers < expected_workers:
        return "degraded"
    if stale_workers > 0:
        return "degraded"
    if healthy_workers >= expected_workers:
        return "healthy"
    return "degraded"


def _worker_age_seconds(worker: dict[str, Any], *, now: datetime) -> float:
    updated_at = datetime.fromisoformat(worker["updated_at"])
    return max((now - updated_at).total_seconds(), 0.0)
