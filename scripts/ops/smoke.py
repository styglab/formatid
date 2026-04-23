from __future__ import annotations

import json
import time

from scripts.ops.common import POLL_INTERVAL_SECONDS, POLL_TIMEOUT_SECONDS
from scripts.ops.compose import compose, compose_run_python


def wait_for_task_status(task_id: str, expected_status: str) -> dict:
    deadline = time.time() + POLL_TIMEOUT_SECONDS
    last_payload: dict | None = None

    while time.time() < deadline:
        output = compose_run_python("scripts/ops.py", "task", task_id)
        payload = json.loads(output or "null")
        last_payload = payload
        if payload and payload.get("status") == expected_status:
            return payload
        time.sleep(POLL_INTERVAL_SECONDS)

    raise RuntimeError(
        f"task status did not reach {expected_status} within timeout; last payload={json.dumps(last_payload, indent=2)}"
    )


def wait_for_task_requeue_count(task_id: str, expected_requeue_count: int) -> dict:
    deadline = time.time() + POLL_TIMEOUT_SECONDS
    last_payload: dict | None = None

    while time.time() < deadline:
        output = compose_run_python("scripts/ops.py", "task", task_id)
        payload = json.loads(output or "null")
        last_payload = payload
        if payload and payload.get("dlq_requeue_count") == expected_requeue_count:
            return payload
        time.sleep(POLL_INTERVAL_SECONDS)

    raise RuntimeError(
        "task requeue count did not reach "
        f"{expected_requeue_count} within timeout; last payload={json.dumps(last_payload, indent=2)}"
    )


def wait_for_worker_heartbeats() -> dict:
    deadline = time.time() + POLL_TIMEOUT_SECONDS
    last_payload: dict | None = None

    while time.time() < deadline:
        output = compose_run_python("scripts/ops.py", "workers")
        payload = json.loads(output)
        last_payload = payload
        workers = payload.get("workers", {})
        if all(workers.get(queue_name) for queue_name in ("ingest:api", "ingest:file")):
            return payload
        time.sleep(POLL_INTERVAL_SECONDS)

    raise RuntimeError(
        f"worker heartbeats did not appear within timeout; last payload={json.dumps(last_payload, indent=2)}"
    )


def wait_for_dlq_message(queue_name: str, expected_task_id: str) -> dict:
    deadline = time.time() + POLL_TIMEOUT_SECONDS
    last_payload: dict | None = None

    while time.time() < deadline:
        output = compose_run_python("scripts/ops.py", "dlq", "--queues", queue_name, "--limit", "10")
        payload = json.loads(output)
        last_payload = payload
        queue_payload = payload["queues"][queue_name]
        task_ids = {message["task_id"] for message in queue_payload["messages"]}
        if expected_task_id in task_ids:
            return queue_payload
        time.sleep(POLL_INTERVAL_SECONDS)

    raise RuntimeError(
        f"task_id={expected_task_id} did not appear in DLQ within timeout; last payload={json.dumps(last_payload, indent=2)}"
    )


def enqueue_for_smoke(queue_name: str, task_name: str, payload: dict) -> dict:
    output = compose_run_python(
        "scripts/ops.py",
        "enqueue",
        task_name,
        "--queue-name",
        queue_name,
        "--payload",
        json.dumps(payload),
    )
    return json.loads(output)


def requeue_dlq_for_smoke(queue_name: str, task_id: str, *, force: bool = False) -> dict:
    args = [
        "scripts/ops.py",
        "requeue-dlq",
        queue_name,
        "--task-id",
        task_id,
    ]
    if force:
        args.append("--force")
    output = compose_run_python(*args)
    return json.loads(output)


def run_compose_smoke_test() -> dict:
    compose("down", "-v", "--remove-orphans", check=False)

    try:
        compose(
            "build",
            "ingest-api-worker",
            "ingest-file-worker",
        )
        compose(
            "up",
            "-d",
            "redis",
            "ingest-api-worker",
            "ingest-file-worker",
        )

        heartbeat_report = wait_for_worker_heartbeats()

        return {
            "heartbeats": heartbeat_report["workers"],
        }
    finally:
        compose("down", "-v", "--remove-orphans", check=False)
