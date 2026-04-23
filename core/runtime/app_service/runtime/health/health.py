from __future__ import annotations

from datetime import datetime
from typing import Any

from core.runtime.time import iso_now, now


def build_service_health_report(
    *,
    services: list[dict[str, Any]],
    heartbeat_interval_seconds: int,
    heartbeat_ttl_seconds: int,
) -> dict[str, Any]:
    healthy_threshold_seconds = heartbeat_interval_seconds * 2
    entries = []
    for service in services:
        age_seconds = _age_seconds(service.get("updated_at"))
        entry = dict(service)
        entry["age_seconds"] = age_seconds
        entry["health_status"] = _classify(
            age_seconds=age_seconds,
            healthy_threshold_seconds=healthy_threshold_seconds,
            heartbeat_ttl_seconds=heartbeat_ttl_seconds,
        )
        entries.append(entry)

    healthy_count = sum(1 for entry in entries if entry["health_status"] == "healthy")
    stale_count = sum(1 for entry in entries if entry["health_status"] == "stale")
    down_count = sum(1 for entry in entries if entry["health_status"] == "down")

    if healthy_count > 0:
        status = "healthy"
    elif stale_count > 0:
        status = "degraded"
    else:
        status = "down"

    return {
        "evaluated_at": iso_now(),
        "status": status,
        "policy": {
            "heartbeat_interval_seconds": heartbeat_interval_seconds,
            "heartbeat_ttl_seconds": heartbeat_ttl_seconds,
            "healthy_threshold_seconds": healthy_threshold_seconds,
        },
        "service_count": len(entries),
        "healthy_services": healthy_count,
        "stale_services": stale_count,
        "down_services": down_count,
        "services": entries,
    }


def _age_seconds(updated_at: str | None) -> float:
    if not updated_at:
        return float("inf")
    try:
        updated = datetime.fromisoformat(updated_at)
    except ValueError:
        return float("inf")
    return round(max((now() - updated).total_seconds(), 0.0), 3)


def _classify(
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
