from __future__ import annotations

import argparse
import asyncio

from scripts.ops.boundaries import lint_boundaries
from scripts.ops.catalog import inspect_catalog
from scripts.ops.check_all import check_all
from scripts.ops.checkpoints import fetch_checkpoints
from scripts.ops.g2b_pipeline import (
    g2b_pipeline_status,
    reset_g2b_pipeline_checkpoint,
    start_g2b_pipeline,
    stop_g2b_pipeline,
    unblock_g2b_pipeline_quota,
)
from scripts.ops.observability import prune_observability_data
from scripts.ops.smoke import run_compose_smoke_test
from scripts.ops.validation import validate_config


def build_ops_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Operational CLI for platform services and app runtimes")
    subparsers = parser.add_subparsers(dest="command", required=True)

    checkpoints_parser = subparsers.add_parser("checkpoints", help="inspect checkpoints stored in postgres")
    checkpoints_parser.add_argument("name", nargs="?", help="checkpoint name to inspect")

    prune_parser = subparsers.add_parser(
        "prune-observability",
        help="delete old service observability rows from postgres",
    )
    prune_parser.add_argument("--days", type=int)

    g2b_pipeline_parser = subparsers.add_parser("g2b_pipeline", help="operate G2B pipeline service")
    g2b_pipeline_subparsers = g2b_pipeline_parser.add_subparsers(dest="g2b_pipeline_command", required=True)
    g2b_pipeline_subparsers.add_parser("start", help="start G2B pipeline app service")
    g2b_pipeline_subparsers.add_parser("stop", help="stop G2B pipeline app service")
    g2b_pipeline_subparsers.add_parser("status", help="show G2B pipeline service status")
    reset_parser = g2b_pipeline_subparsers.add_parser("reset-checkpoint", help="delete G2B pipeline service checkpoints")
    reset_parser.add_argument("--from", dest="start", help="document the intended restart start date")
    g2b_pipeline_subparsers.add_parser("unblock-quota", help="clear G2B pipeline quota block from internal stores")

    subparsers.add_parser("validate-config", help="validate manifests and generated compose")
    subparsers.add_parser("lint-boundaries", help="validate service/core layer boundary rules")
    subparsers.add_parser("check-all", help="run compose, config, boundary, compile, and docker compose checks")
    subparsers.add_parser("catalog", help="list available platform and app services")
    subparsers.add_parser("smoke", help="run docker compose smoke test")
    return parser


def run_ops_command(args: argparse.Namespace) -> object | None:
    if args.command == "checkpoints":
        return asyncio.run(fetch_checkpoints(args.name))

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
