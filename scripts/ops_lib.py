import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.ops.checkpoints import fetch_checkpoints
from scripts.ops.cli import build_ops_parser, run_ops_command
from scripts.ops.common import (
    build_dlq_queue_name,
    get_dlq_suffix,
    get_redis_url,
    parse_json_object,
    print_json,
    run_command,
)
from scripts.ops.compose import compose, compose_run_python
from scripts.ops.dlq import inspect_dlq, requeue_dlq_messages
from scripts.ops.health import build_workers_summary, check_workers, render_workers_table
from scripts.ops.observability import get_observability_retention_days, prune_observability_data
from scripts.ops.smoke import (
    enqueue_for_smoke,
    requeue_dlq_for_smoke,
    run_compose_smoke_test,
    wait_for_dlq_message,
    wait_for_task_requeue_count,
    wait_for_task_status,
    wait_for_worker_heartbeats,
)
from scripts.ops.tasks import enqueue, fetch_task
from scripts.ops.validation import validate_config


__all__ = [
    "build_dlq_queue_name",
    "build_ops_parser",
    "build_workers_summary",
    "check_workers",
    "compose",
    "compose_run_python",
    "enqueue",
    "enqueue_for_smoke",
    "fetch_checkpoints",
    "fetch_task",
    "get_dlq_suffix",
    "get_redis_url",
    "get_observability_retention_days",
    "inspect_dlq",
    "parse_json_object",
    "print_json",
    "render_workers_table",
    "requeue_dlq_for_smoke",
    "requeue_dlq_messages",
    "prune_observability_data",
    "run_command",
    "run_compose_smoke_test",
    "run_ops_command",
    "validate_config",
    "wait_for_dlq_message",
    "wait_for_task_requeue_count",
    "wait_for_task_status",
    "wait_for_worker_heartbeats",
]
