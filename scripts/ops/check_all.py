from __future__ import annotations

import py_compile
from pathlib import Path

from scripts.generate_compose import check_compose
from scripts.ops.boundaries import lint_boundaries
from scripts.ops.common import COMPOSE_ENV_FILE, COMPOSE_FILE, PROJECT_ROOT, run_command
from scripts.ops.validation import validate_config


def check_all() -> dict:
    checks: list[dict[str, object]] = []
    checks.append(_check_compose_sync())
    checks.append(_check_validate_config())
    checks.append(_check_boundaries())
    checks.append(_check_compileall())
    checks.append(_check_docker_compose_config())

    return {
        "valid": all(bool(check["valid"]) for check in checks),
        "checks": checks,
    }


def _check_compose_sync() -> dict[str, object]:
    valid = check_compose()
    return {
        "name": "generate-compose-check",
        "valid": valid,
        "errors": [] if valid else ["generated compose is out of sync: run `python3 scripts/generate_compose.py`"],
    }


def _check_validate_config() -> dict[str, object]:
    result = validate_config()
    return {
        "name": "validate-config",
        "valid": bool(result["valid"]),
        "errors": result["errors"],
        "warnings": result["warnings"],
    }


def _check_boundaries() -> dict[str, object]:
    result = lint_boundaries()
    return {
        "name": "lint-boundaries",
        "valid": bool(result["valid"]),
        "errors": result["findings"],
    }


def _check_compileall() -> dict[str, object]:
    errors: list[str] = []
    for root_name in ("apps", "services", "core", "scripts"):
        root = PROJECT_ROOT / root_name
        if not root.exists():
            continue
        for path in sorted(root.rglob("*.py")):
            if "__pycache__" in path.parts:
                continue
            try:
                py_compile.compile(str(path), doraise=True)
            except py_compile.PyCompileError as exc:
                errors.append(str(exc))
    return {
        "name": "compileall",
        "valid": not errors,
        "errors": errors,
    }


def _check_docker_compose_config() -> dict[str, object]:
    try:
        run_command(
            "docker",
            "compose",
            "--env-file",
            str(COMPOSE_ENV_FILE),
            "-f",
            str(COMPOSE_FILE),
            "config",
        )
    except RuntimeError as exc:
        return {
            "name": "docker-compose-config",
            "valid": False,
            "errors": [str(exc)],
        }
    return {
        "name": "docker-compose-config",
        "valid": True,
        "errors": [],
    }
