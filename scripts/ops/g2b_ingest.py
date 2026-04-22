from __future__ import annotations

from scripts.ops.compose import compose


G2B_INGEST_SERVICES = ("g2b-ingest", "ingest-api-worker", "ingest-file-worker")


def start_g2b_ingest() -> dict:
    output = compose("up", "-d", *G2B_INGEST_SERVICES)
    return {"status": "started", "services": list(G2B_INGEST_SERVICES), "output": output}


def stop_g2b_ingest() -> dict:
    output = compose("stop", *G2B_INGEST_SERVICES)
    return {"status": "stopped", "services": list(G2B_INGEST_SERVICES), "output": output}


def g2b_ingest_status() -> dict:
    output = compose("ps", *G2B_INGEST_SERVICES, check=False)
    return {"services": list(G2B_INGEST_SERVICES), "output": output}


def reset_g2b_ingest_checkpoint(*, start: str | None = None) -> dict:
    sql = "DELETE FROM checkpoints WHERE name LIKE 'g2b_ingest:%' OR name = 'service:g2b_ingest_bid_list_collect';"
    output = compose("exec", "-T", "postgres", "psql", "-U", "postgres", "-d", "postgres", "-c", sql)
    return {"status": "checkpoint_reset", "start": start, "output": output}


def unblock_g2b_ingest_quota() -> dict:
    sql = (
        "DELETE FROM external_api_quota_blocks "
        "WHERE app = 'g2b_ingest' AND provider = 'data.go.kr' AND api_name = 'g2b-ingest-openapi';"
    )
    pg_output = compose("exec", "-T", "postgres", "psql", "-U", "postgres", "-d", "postgres", "-c", sql)
    redis_output = compose("exec", "-T", "redis", "redis-cli", "DEL", "g2b_ingest:api:quota_blocked_until", check=False)
    return {"status": "quota_unblocked", "postgres": pg_output, "redis": redis_output}
