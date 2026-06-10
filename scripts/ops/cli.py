from __future__ import annotations

import argparse
import asyncio

from scripts.ops.boundaries import lint_boundaries
from scripts.ops.catalog import inspect_catalog
from scripts.ops.check_all import check_all
from scripts.ops.checkpoints import fetch_checkpoints
from scripts.ops.semantic_layer import reset_semantic_layer, seed_semantic_registry
from scripts.ops.observability import prune_observability_data
from scripts.ops.smoke import run_smoke_test
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

    subparsers.add_parser("validate-config", help="validate manifests and generated compose")
    subparsers.add_parser("lint-boundaries", help="validate service/core layer boundary rules")
    subparsers.add_parser("check-all", help="run compose, config, boundary, compile, and docker compose checks")
    subparsers.add_parser("catalog", help="list available platform and app services")
    subparsers.add_parser("smoke", help="run docker compose smoke test")
    semantic_layer_parser = subparsers.add_parser(
        "semantic-layer",
        help="operate semantic layer catalog",
    )
    semantic_layer_subparsers = semantic_layer_parser.add_subparsers(
        dest="semantic_layer_command",
        required=True,
    )
    semantic_layer_subparsers.add_parser("reset", help="clear semantic layer catalog data")
    semantic_layer_subparsers.add_parser("seed-registry", help="seed core semantic type registry")
    return parser


def run_ops_command(args: argparse.Namespace) -> object | None:
    if args.command == "checkpoints":
        return asyncio.run(fetch_checkpoints(args.name))

    if args.command == "prune-observability":
        return asyncio.run(prune_observability_data(days=args.days))

    if args.command == "validate-config":
        return validate_config()
    if args.command == "lint-boundaries":
        return lint_boundaries()
    if args.command == "check-all":
        return check_all()
    if args.command == "catalog":
        return inspect_catalog()
    if args.command == "smoke":
        return run_smoke_test()
    if args.command == "semantic-layer":
        if args.semantic_layer_command == "reset":
            return reset_semantic_layer()
        if args.semantic_layer_command == "seed-registry":
            return seed_semantic_registry()
    raise SystemExit(f"unknown command: {args.command}")
