from __future__ import annotations

import argparse
import asyncio

from core.catalog.service_catalog import list_worker_queue_names
from core.runtime.task_runtime.catalog import list_queue_names
from scripts.ops.boundaries import lint_boundaries
from scripts.ops.catalog import inspect_catalog
from scripts.ops.check_all import check_all
from scripts.ops.checkpoints import fetch_checkpoints
from scripts.ops.common import parse_json_object
from scripts.ops.dlq import inspect_dlq, requeue_dlq_messages
from scripts.ops.health import check_workers
from scripts.ops.observability import prune_observability_data
from scripts.ops.g2b_pipeline import (
    g2b_pipeline_status,
    reset_g2b_pipeline_checkpoint,
    start_g2b_pipeline,
    stop_g2b_pipeline,
    unblock_g2b_pipeline_quota,
)
from scripts.ops.smoke import run_compose_smoke_test
from scripts.ops.tasks import enqueue, fetch_task
from scripts.ops.validation import validate_config


def build_ops_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Operational CLI for worker runtime")
    subparsers = parser.add_subparsers(dest="command", required=True)

    enqueue_parser = subparsers.add_parser("enqueue", help="enqueue a task")
    enqueue_parser.add_argument("task_name", help="registered task name")
    enqueue_parser.add_argument("--queue-name", choices=list(list_queue_names()), help="optional route assertion")
    enqueue_parser.add_argument(
        "--payload",
        default="{}",
        help='JSON object payload, for example: --payload \'{"source":"cli"}\'',
    )
    enqueue_parser.add_argument("--attempts", type=int, default=0, help="initial attempts count")
    enqueue_parser.add_argument("--dedupe-key", help="skip enqueue when the same task dedupe key already exists")
    enqueue_parser.add_argument("--correlation-id", help="cross-task trace id")
    enqueue_parser.add_argument("--resource-key", help="app resource key for filtering")

    workers_parser = subparsers.add_parser("workers", help="inspect worker heartbeats and queue health")
    workers_parser.add_argument(
        "--queues",
        nargs="*",
        default=list(list_worker_queue_names()),
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

    checkpoints_parser = subparsers.add_parser("checkpoints", help="inspect checkpoints stored in postgres")
    checkpoints_parser.add_argument("name", nargs="?", help="checkpoint name to inspect")

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

    queue_parser = subparsers.add_parser("queue", help="pause, resume, or inspect a queue")
    queue_subparsers = queue_parser.add_subparsers(dest="queue_command", required=True)
    queue_pause_parser = queue_subparsers.add_parser("pause", help="pause worker consumption for a queue")
    queue_pause_parser.add_argument("queue_name", choices=list(list_queue_names()))
    queue_pause_parser.add_argument("--reason", default="manual")
    queue_pause_parser.add_argument("--ttl-seconds", type=int)
    queue_resume_parser = queue_subparsers.add_parser("resume", help="resume worker consumption for a queue")
    queue_resume_parser.add_argument("queue_name", choices=list(list_queue_names()))
    queue_status_parser = queue_subparsers.add_parser("status", help="inspect queue pause state")
    queue_status_parser.add_argument("queue_name", choices=list(list_queue_names()))

    prune_parser = subparsers.add_parser(
        "prune-observability",
        help="delete old service_runs and task_executions from internal postgres",
    )
    prune_parser.add_argument(
        "--days",
        type=int,
        help="retention days; defaults to OBSERVABILITY_RETENTION_DAYS or 30",
    )

    g2b_pipeline_parser = subparsers.add_parser("g2b_pipeline", help="operate G2B pipeline services")
    g2b_pipeline_subparsers = g2b_pipeline_parser.add_subparsers(dest="g2b_pipeline_command", required=True)
    g2b_pipeline_subparsers.add_parser("start", help="start G2B pipeline service and workers")
    g2b_pipeline_subparsers.add_parser("stop", help="stop G2B pipeline service and workers")
    g2b_pipeline_subparsers.add_parser("status", help="show G2B pipeline service status")
    reset_parser = g2b_pipeline_subparsers.add_parser("reset-checkpoint", help="delete G2B pipeline service checkpoints")
    reset_parser.add_argument("--from", dest="start", help="document the intended restart start date")
    g2b_pipeline_subparsers.add_parser("unblock-quota", help="clear G2B pipeline quota block from internal stores")

    subparsers.add_parser("validate-config", help="validate task catalog, worker service manifests, and generated compose")
    subparsers.add_parser("lint-boundaries", help="validate service/core layer boundary rules")
    subparsers.add_parser("check-all", help="run compose, config, boundary, compile, and docker compose checks")
    subparsers.add_parser("catalog", help="list available platform services and worker services")
    subparsers.add_parser("smoke", help="run docker compose smoke test")
    return parser


def run_ops_command(args: argparse.Namespace) -> object | None:
    if args.command == "enqueue":
        payload = parse_json_object(args.payload)
        message = asyncio.run(
            enqueue(
                args.task_name,
                payload,
                args.attempts,
                queue_name=args.queue_name,
                dedupe_key=args.dedupe_key,
                correlation_id=args.correlation_id,
                resource_key=args.resource_key,
            )
        )
        return {
            "queue_name": message.queue_name,
            "task_id": message.task_id,
            "task_name": message.task_name,
            "dedupe_key": message.dedupe_key,
            "correlation_id": message.correlation_id,
            "resource_key": message.resource_key,
        }

    if args.command == "workers":
        return asyncio.run(check_workers(args.queues))

    if args.command == "task":
        return asyncio.run(fetch_task(args.task_id))

    if args.command == "checkpoints":
        return asyncio.run(fetch_checkpoints(args.name))

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

    if args.command == "queue":
        from core.runtime.task_runtime.queue_control import get_queue_pause, pause_queue, resume_queue
        from scripts.ops.common import get_redis_url

        if args.queue_command == "pause":
            return asyncio.run(
                pause_queue(
                    redis_url=get_redis_url(),
                    queue_name=args.queue_name,
                    reason=args.reason,
                    ttl_seconds=args.ttl_seconds,
                )
            )
        if args.queue_command == "resume":
            return {"resumed": asyncio.run(resume_queue(redis_url=get_redis_url(), queue_name=args.queue_name))}
        if args.queue_command == "status":
            return asyncio.run(get_queue_pause(redis_url=get_redis_url(), queue_name=args.queue_name))

    if args.command == "prune-observability":
        return asyncio.run(prune_observability_data(days=args.days))

    if args.command == "g2b_pipeline":
        if args.g2b_pipeline_command == "start":
            return start_g2b_pipeline()
        if args.g2b_pipeline_command == "stop":
            return stop_g2b_pipeline()
        if args.g2b_pipeline_command == "status":
            return g2b_pipeline_status()
        if args.g2b_pipeline_command == "reset-checkpoint":
            return reset_g2b_pipeline_checkpoint(start=args.start)
        if args.g2b_pipeline_command == "unblock-quota":
            return unblock_g2b_pipeline_quota()

    if args.command == "validate-config":
        return validate_config()

    if args.command == "lint-boundaries":
        return lint_boundaries()

    if args.command == "check-all":
        return check_all()

    if args.command == "catalog":
        return inspect_catalog()

    if args.command == "smoke":
        return run_compose_smoke_test()

    raise SystemExit(f"unknown command: {args.command}")
