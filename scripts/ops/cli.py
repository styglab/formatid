from __future__ import annotations

import argparse
import asyncio

from shared.service_catalog import list_worker_queue_names
from shared.tasking.catalog import list_queue_names
from scripts.ops.checkpoints import fetch_checkpoints
from scripts.ops.common import parse_json_object
from scripts.ops.dlq import inspect_dlq, requeue_dlq_messages
from scripts.ops.health import check_workers
from scripts.ops.smoke import run_compose_smoke_test
from scripts.ops.tasks import enqueue, fetch_task
from scripts.ops.validation import validate_config


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

    checkpoints_parser = subparsers.add_parser("checkpoints", help="inspect scheduler checkpoints stored in postgres")
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

    subparsers.add_parser("validate-config", help="validate task catalog, worker service manifests, and generated compose")
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

    if args.command == "validate-config":
        return validate_config()

    if args.command == "smoke":
        return run_compose_smoke_test()

    raise SystemExit(f"unknown command: {args.command}")
