from __future__ import annotations

from scripts.ops.common import COMPOSE_ENV_FILE, COMPOSE_FILE, run_command


def compose(*args: str, check: bool = True) -> str:
    return run_command(
        "docker",
        "compose",
        "--env-file",
        str(COMPOSE_ENV_FILE),
        "-f",
        str(COMPOSE_FILE),
        *args,
        check=check,
    )


def compose_run_python(*python_args: str) -> str:
    return compose(
        "run",
        "--rm",
        "--no-deps",
        "ingest-api-worker",
        "python",
        *python_args,
    )
