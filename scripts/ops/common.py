from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

COMPOSE_FILE = PROJECT_ROOT / "deploy" / "compose" / "docker-compose.yml"
COMPOSE_ENV_FILE = PROJECT_ROOT / "deploy" / "compose" / "env" / "compose.env"


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
