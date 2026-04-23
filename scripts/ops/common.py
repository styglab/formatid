from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

COMPOSE_FILE = PROJECT_ROOT / "deploy" / "compose" / "docker-compose.yml"
COMPOSE_ENV_FILE = PROJECT_ROOT / "deploy" / "compose" / "env" / "compose.env"
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
