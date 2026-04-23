from __future__ import annotations

import ast
from pathlib import Path

from scripts.ops.common import PROJECT_ROOT


SERVICE_FORBIDDEN_SUBSTRINGS = (
    "G2B_INGEST",
    "PUBLIC_API_KEY",
    "g2b_ingest",
    "g2b_summary",
    "procurement",
)
SERVICE_FORBIDDEN_CALLS = ("enqueue_task",)
CORE_FORBIDDEN_SUBSTRINGS = (
    "G2B_INGEST",
    "PUBLIC_API_KEY",
    "g2b_ingest",
    "g2b_summary",
    "procurement",
)


def lint_boundaries() -> dict:
    findings: list[dict[str, object]] = []
    findings.extend(_scan_python_tree(PROJECT_ROOT / "services", SERVICE_FORBIDDEN_SUBSTRINGS, SERVICE_FORBIDDEN_CALLS))
    findings.extend(_scan_python_tree(PROJECT_ROOT / "core", CORE_FORBIDDEN_SUBSTRINGS, ()))
    return {
        "valid": not findings,
        "findings": findings,
        "summary": {
            "finding_count": len(findings),
        },
    }


def _scan_python_tree(
    root: Path,
    forbidden_substrings: tuple[str, ...],
    forbidden_calls: tuple[str, ...],
) -> list[dict[str, object]]:
    if not root.exists():
        return []
    findings: list[dict[str, object]] = []
    for path in sorted(root.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        text = path.read_text(encoding="utf-8")
        relative_path = path.relative_to(PROJECT_ROOT).as_posix()
        for line_number, line in enumerate(text.splitlines(), start=1):
            for token in forbidden_substrings:
                if token in line:
                    findings.append(
                        {
                            "path": relative_path,
                            "line": line_number,
                            "rule": "forbidden-app-specific-token",
                            "token": token,
                        }
                    )
        if forbidden_calls:
            findings.extend(_scan_for_forbidden_calls(path, relative_path, forbidden_calls))
    return findings


def _scan_for_forbidden_calls(
    path: Path,
    relative_path: str,
    forbidden_calls: tuple[str, ...],
) -> list[dict[str, object]]:
    findings: list[dict[str, object]] = []
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except SyntaxError as exc:
        return [
            {
                "path": relative_path,
                "line": exc.lineno or 1,
                "rule": "python-syntax-error",
                "message": exc.msg,
            }
        ]

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        name = _call_name(node.func)
        if name in forbidden_calls:
            findings.append(
                {
                    "path": relative_path,
                    "line": getattr(node, "lineno", 1),
                    "rule": "forbidden-service-call",
                    "call": name,
                }
            )
    return findings


def _call_name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return None
