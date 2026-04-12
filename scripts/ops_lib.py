import argparse
import asyncio
import json
import os
import subprocess
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from shared.tasking.catalog import (
    get_task_definition,
    list_queue_names,
)
from shared.tasking.schemas import TaskMessage
from shared.time import iso_now
from shared.worker_health.health import DEFAULT_EXPECTED_WORKERS, build_health_report

COMPOSE_FILE = PROJECT_ROOT / "infra" / "docker-compose.yml"
POLL_TIMEOUT_SECONDS = 30
POLL_INTERVAL_SECONDS = 1


def get_redis_url() -> str:
    return os.getenv("WORKER_REDIS_URL", "redis://localhost:6379/0")


def get_dlq_suffix() -> str:
    return os.getenv("TASK_DLQ_SUFFIX", "dlq")


def build_dlq_queue_name(queue_name: str) -> str:
    return f"{queue_name}:{get_dlq_suffix()}"


def parse_json_object(raw: str) -> dict:
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"invalid JSON payload: {exc}") from exc

    if not isinstance(payload, dict):
        raise SystemExit("payload must be a JSON object")
    return payload


def print_json(payload: object) -> None:
    print(json.dumps(payload, indent=2))


async def enqueue(queue_name: str, task_name: str, payload: dict, attempts: int) -> TaskMessage:
    from shared.queue.redis import RedisTaskQueue
    from shared.tasking.routing import validate_task_route
    from shared.tasking.status_store import TaskStatusStore

    redis_url = get_redis_url()
    validate_task_route(queue_name=queue_name, task_name=task_name)
    queue = RedisTaskQueue(redis_url=redis_url, queue_name=queue_name)
    status_store = TaskStatusStore(redis_url=redis_url)
    message = TaskMessage(
        queue_name=queue_name,
        task_name=task_name,
        payload=payload,
        attempts=attempts,
    )

    try:
        await queue.put(message)
        definition = get_task_definition(task_name)
        await status_store.mark_queued(
            message,
            policy_snapshot={
                "queue_name": definition.queue_name,
                "max_retries": definition.max_retries,
                "retryable": definition.retryable,
                "backoff_seconds": definition.backoff_seconds,
                "timeout_seconds": definition.timeout_seconds,
                "dlq_enabled": definition.dlq_enabled,
                "dlq_requeue_limit": definition.dlq_requeue_limit,
                "dlq_requeue_keep_attempts": definition.dlq_requeue_keep_attempts,
            },
        )
    finally:
        await status_store.close()
        await queue.close()

    return message


async def check_workers(queue_names: list[str]) -> dict:
    from redis.asyncio import Redis
    from shared.worker_health.store import WorkerHeartbeatStore

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
    )
    report["redis_url"] = redis_url
    return report


async def fetch_task(task_id: str) -> dict | None:
    from shared.tasking.status_store import TaskStatusStore

    store = TaskStatusStore(redis_url=get_redis_url())
    try:
        return await store.get(task_id)
    finally:
        await store.close()


async def inspect_dlq(queue_names: list[str], *, limit: int) -> dict:
    from redis.asyncio import Redis
    from shared.tasking.status_store import TaskStatusStore

    redis_url = get_redis_url()
    redis = Redis.from_url(redis_url, decode_responses=True)
    status_store = TaskStatusStore(redis_url=redis_url)

    try:
        report: dict[str, dict] = {}
        for queue_name in queue_names:
            dlq_queue_name = build_dlq_queue_name(queue_name)
            raw_messages = await redis.lrange(dlq_queue_name, 0, max(limit - 1, -1))
            messages = []
            for raw_message in raw_messages:
                message = TaskMessage.from_dict(json.loads(raw_message))
                status = await status_store.get(message.task_id)
                messages.append(
                    {
                        **message.to_dict(),
                        "dlq_requeue_count": 0 if status is None else status.get("dlq_requeue_count", 0),
                        "last_error": None if status is None else status.get("last_error"),
                    }
                )
            report[queue_name] = {
                "dlq_queue_name": dlq_queue_name,
                "size": int(await redis.llen(dlq_queue_name)),
                "messages": messages,
            }
        return {
            "redis_url": redis_url,
            "queues": report,
        }
    finally:
        await status_store.close()
        await redis.aclose()


async def requeue_dlq_messages(
    *,
    queue_name: str,
    task_id: str | None,
    count: int,
    keep_attempts: bool,
    force: bool,
) -> dict:
    from redis.asyncio import Redis
    from shared.tasking.status_store import TaskStatusStore

    if count < 1:
        raise ValueError("count must be >= 1")

    redis_url = get_redis_url()
    redis = Redis.from_url(redis_url, decode_responses=True)
    status_store = TaskStatusStore(redis_url=redis_url)
    dlq_queue_name = build_dlq_queue_name(queue_name)
    requeued_messages: list[dict] = []
    skipped_messages: list[dict] = []

    try:
        if task_id is not None:
            raw_messages = await redis.lrange(dlq_queue_name, 0, -1)
            for raw_message in raw_messages:
                message = TaskMessage.from_dict(json.loads(raw_message))
                if message.task_id != task_id:
                    continue
                maybe_requeued = await _maybe_requeue_message(
                    redis=redis,
                    status_store=status_store,
                    message=message,
                    queue_name=queue_name,
                    dlq_queue_name=dlq_queue_name,
                    keep_attempts=keep_attempts,
                    force=force,
                )
                if "skipped" in maybe_requeued:
                    skipped_messages.append(maybe_requeued)
                    break
                await redis.lrem(dlq_queue_name, 1, raw_message)
                requeued_message = maybe_requeued["message"]
                requeued_messages.append(requeued_message.to_dict())
                break
        else:
            for _ in range(count):
                raw_message = await redis.lpop(dlq_queue_name)
                if raw_message is None:
                    break
                message = TaskMessage.from_dict(json.loads(raw_message))
                maybe_requeued = await _maybe_requeue_message(
                    redis=redis,
                    status_store=status_store,
                    message=message,
                    queue_name=queue_name,
                    dlq_queue_name=dlq_queue_name,
                    keep_attempts=keep_attempts,
                    force=force,
                )
                if "skipped" in maybe_requeued:
                    skipped_messages.append(maybe_requeued)
                    await redis.rpush(dlq_queue_name, raw_message)
                    continue
                requeued_message = maybe_requeued["message"]
                requeued_messages.append(requeued_message.to_dict())

        return {
            "redis_url": redis_url,
            "queue_name": queue_name,
            "dlq_queue_name": dlq_queue_name,
            "requeued_count": len(requeued_messages),
            "skipped_count": len(skipped_messages),
            "messages": requeued_messages,
            "skipped_messages": skipped_messages,
        }
    finally:
        await status_store.close()
        await redis.aclose()


async def _maybe_requeue_message(
    *,
    redis,
    status_store,
    message: TaskMessage,
    queue_name: str,
    dlq_queue_name: str,
    keep_attempts: bool,
    force: bool,
) -> dict:
    definition = get_task_definition(message.task_name)
    preserve_attempts = keep_attempts or definition.dlq_requeue_keep_attempts
    policy_snapshot = {
        "queue_name": definition.queue_name,
        "max_retries": definition.max_retries,
        "retryable": definition.retryable,
        "backoff_seconds": definition.backoff_seconds,
        "timeout_seconds": definition.timeout_seconds,
        "dlq_enabled": definition.dlq_enabled,
        "dlq_requeue_limit": definition.dlq_requeue_limit,
        "dlq_requeue_keep_attempts": definition.dlq_requeue_keep_attempts,
    }
    current_status = await status_store.get(message.task_id) or {}
    current_requeue_count = int(current_status.get("dlq_requeue_count", 0))
    requeue_limit = definition.dlq_requeue_limit

    if not force and requeue_limit is not None and current_requeue_count >= requeue_limit:
        return {
            "skipped": True,
            "task_id": message.task_id,
            "task_name": message.task_name,
            "queue_name": queue_name,
            "reason": "dlq_requeue_limit_exceeded",
            "dlq_requeue_count": current_requeue_count,
            "dlq_requeue_limit": requeue_limit,
        }

    requeued_message = TaskMessage(
        queue_name=queue_name,
        task_name=message.task_name,
        payload=message.payload,
        attempts=message.attempts if preserve_attempts else 0,
        task_id=message.task_id,
    )
    requeue_entry = {
        "requeued_at": iso_now(),
        "from_queue_name": dlq_queue_name,
        "to_queue_name": queue_name,
        "preserved_attempts": preserve_attempts,
        "requeue_number": current_requeue_count + 1,
        "forced": force,
    }
    await status_store.mark_requeued_from_dlq(
        requeued_message,
        queue_name=queue_name,
        dlq_queue_name=dlq_queue_name,
        requeue_entry=requeue_entry,
        policy_snapshot=policy_snapshot,
    )
    await redis.rpush(queue_name, json.dumps(requeued_message.to_dict()))
    return {"message": requeued_message}


def run_command(*args: str, check: bool = True) -> str:
    completed = subprocess.run(
        args,
        cwd=PROJECT_ROOT,
        text=True,
        capture_output=True,
    )
    if check and completed.returncode != 0:
        raise RuntimeError(
            f"command failed: {' '.join(args)}\nstdout:\n{completed.stdout}\nstderr:\n{completed.stderr}"
        )
    return completed.stdout.strip()


def compose(*args: str, check: bool = True) -> str:
    return run_command("docker", "compose", "-f", str(COMPOSE_FILE), *args, check=check)


def compose_run_python(*python_args: str) -> str:
    return compose(
        "run",
        "--rm",
        "--no-deps",
        "system-health-worker",
        "python",
        *python_args,
    )


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
        if all(workers.get(queue_name) for queue_name in ("system:health", "pps:bid", "pps:attachment")):
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
        queue_name,
        task_name,
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
            "system-health-worker",
            "pps-bid-worker",
            "pps-attachment-worker",
        )
        compose(
            "up",
            "-d",
            "redis",
            "system-health-worker",
            "pps-bid-worker",
            "pps-attachment-worker",
        )

        heartbeat_report = wait_for_worker_heartbeats()

        health_task = enqueue_for_smoke(
            "system:health",
            "system.health.check",
            {"source": "compose-smoke-test"},
        )
        fail_task = enqueue_for_smoke(
            "system:health",
            "system.test.fail",
            {"source": "compose-smoke-test"},
        )

        health_status = wait_for_task_status(health_task["task_id"], "succeeded")
        fail_status = wait_for_task_status(fail_task["task_id"], "dead_lettered")
        dlq_payload = wait_for_dlq_message("system:health", fail_task["task_id"])

        dlq_requeues: list[dict] = []
        for expected_requeue_count in (1, 2, 3):
            dlq_before_requeue = wait_for_dlq_message("system:health", fail_task["task_id"])
            requeue_result = requeue_dlq_for_smoke("system:health", fail_task["task_id"])
            recycled_status = wait_for_task_status(fail_task["task_id"], "dead_lettered")
            requeue_count_status = wait_for_task_requeue_count(
                fail_task["task_id"],
                expected_requeue_count,
            )
            dlq_after_requeue = wait_for_dlq_message("system:health", fail_task["task_id"])
            dlq_requeues.append(
                {
                    "dlq_before_requeue": dlq_before_requeue,
                    "requeue_result": requeue_result,
                    "status_after_requeue": recycled_status,
                    "status_with_requeue_count": requeue_count_status,
                    "dlq_after_requeue": dlq_after_requeue,
                }
            )

        limit_dlq_payload = wait_for_dlq_message("system:health", fail_task["task_id"])
        requeue_limit_result = requeue_dlq_for_smoke("system:health", fail_task["task_id"])
        forced_requeue_dlq_payload = wait_for_dlq_message("system:health", fail_task["task_id"])
        forced_requeue_result = requeue_dlq_for_smoke("system:health", fail_task["task_id"], force=True)
        forced_dead_lettered_status = wait_for_task_status(fail_task["task_id"], "dead_lettered")
        forced_requeue_count_status = wait_for_task_requeue_count(fail_task["task_id"], 4)
        final_dlq_payload = wait_for_dlq_message("system:health", fail_task["task_id"])

        return {
            "heartbeats": heartbeat_report["workers"],
            "health_task": health_status,
            "failed_task": fail_status,
            "dlq": dlq_payload,
            "dlq_requeues": dlq_requeues,
            "dlq_before_requeue_limit_check": limit_dlq_payload,
            "dlq_requeue_limit_result": requeue_limit_result,
            "dlq_before_forced_requeue": forced_requeue_dlq_payload,
            "forced_dlq_requeue_result": forced_requeue_result,
            "forced_dead_lettered_status": forced_dead_lettered_status,
            "forced_requeue_count_status": forced_requeue_count_status,
            "final_dlq": final_dlq_payload,
        }
    finally:
        compose("down", "-v", "--remove-orphans", check=False)


def build_ops_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Operational CLI for worker runtime")
    subparsers = parser.add_subparsers(dest="command", required=True)

    enqueue_parser = subparsers.add_parser("enqueue", help="enqueue a task")
    enqueue_parser.add_argument("queue_name", help="target Redis queue")
    enqueue_parser.add_argument("task_name", help="registered task name")
    enqueue_parser.add_argument(
        "--payload",
        default="{}",
        help='JSON object payload, for example: --payload \'{"source":"cli"}\'',
    )
    enqueue_parser.add_argument("--attempts", type=int, default=0, help="initial attempts count")

    workers_parser = subparsers.add_parser("workers", help="inspect worker heartbeats and queue health")
    workers_parser.add_argument(
        "--queues",
        nargs="*",
        default=list(DEFAULT_EXPECTED_WORKERS),
        help="queue names to inspect",
    )
    workers_parser.add_argument(
        "--verbose",
        action="store_true",
        help="include per-worker heartbeat details",
    )
    workers_parser.add_argument(
        "--format",
        choices=("json", "table"),
        default="json",
        help="output format",
    )

    task_parser = subparsers.add_parser("task", help="inspect stored task lifecycle status")
    task_parser.add_argument("task_id", help="task id to inspect")

    dlq_parser = subparsers.add_parser("dlq", help="inspect DLQ messages by queue")
    dlq_parser.add_argument(
        "--queues",
        nargs="*",
        default=list(list_queue_names()),
        help="source queue names to inspect before applying the DLQ suffix",
    )
    dlq_parser.add_argument("--limit", type=int, default=20, help="maximum messages to preview per DLQ queue")

    requeue_parser = subparsers.add_parser("requeue-dlq", help="requeue messages from DLQ back to source queue")
    requeue_parser.add_argument("queue_name", choices=list(list_queue_names()), help="source queue name")
    requeue_parser.add_argument("--task-id", help="requeue only the matching task id from the DLQ")
    requeue_parser.add_argument(
        "--count",
        type=int,
        default=1,
        help="number of DLQ messages to requeue when --task-id is not provided",
    )
    requeue_parser.add_argument(
        "--keep-attempts",
        action="store_true",
        help="preserve attempts instead of resetting them to zero",
    )
    requeue_parser.add_argument(
        "--force",
        action="store_true",
        help="bypass catalog-based DLQ requeue limits",
    )

    subparsers.add_parser("smoke", help="run docker compose smoke test")
    return parser


def run_ops_command(args: argparse.Namespace) -> object | None:
    if args.command == "enqueue":
        payload = parse_json_object(args.payload)
        message = asyncio.run(enqueue(args.queue_name, args.task_name, payload, args.attempts))
        return {
            "queue_name": message.queue_name,
            "task_id": message.task_id,
            "task_name": message.task_name,
        }

    if args.command == "workers":
        return asyncio.run(check_workers(args.queues))

    if args.command == "task":
        return asyncio.run(fetch_task(args.task_id))

    if args.command == "dlq":
        return asyncio.run(inspect_dlq(args.queues, limit=args.limit))

    if args.command == "requeue-dlq":
        return asyncio.run(
            requeue_dlq_messages(
                queue_name=args.queue_name,
                task_id=args.task_id,
                count=args.count,
                keep_attempts=args.keep_attempts,
                force=args.force,
            )
        )

    if args.command == "smoke":
        return run_compose_smoke_test()

    raise SystemExit(f"unknown command: {args.command}")


def build_workers_summary(report: dict) -> dict:
    queues_summary: dict[str, dict] = {}
    for queue_name, payload in report.get("queues", {}).items():
        queues_summary[queue_name] = {
            "status": payload.get("status"),
            "size": payload.get("size"),
            "workers": payload.get("observed_workers"),
        }
    return {
        "evaluated_at": report.get("evaluated_at"),
        "queues": queues_summary,
    }


def render_workers_table(report: dict) -> str:
    headers = ["QUEUE", "STATUS", "SIZE", "WORKERS", "HEALTHY", "STALE", "DOWN"]
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
