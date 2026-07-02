from __future__ import annotations

import argparse
import asyncio

from scripts.ops.boundaries import lint_boundaries
from scripts.ops.catalog import inspect_catalog
from scripts.ops.check_all import check_all
from scripts.ops.checkpoints import fetch_checkpoints
from scripts.ops.context_platform import (
    draft_context_platform_source_contract,
    ingest_context_platform_source,
    ingest_queued_context_platform_source,
    reset_context_platform,
    seed_context_platform,
)
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
    context_platform_parser = subparsers.add_parser(
        "context-platform",
        help="operate Context Platform catalog",
    )
    context_platform_subparsers = context_platform_parser.add_subparsers(
        dest="context_platform_command",
        required=True,
    )
    context_platform_subparsers.add_parser("reset", help="clear Context Platform catalog data")
    context_platform_subparsers.add_parser("seed-registry", help="apply baseline Context Platform seed graph")
    ingest_source_parser = context_platform_subparsers.add_parser(
        "ingest-source",
        help="upload a source document and run ingestion through proposal bundle creation",
    )
    ingest_source_parser.add_argument("source_path", help="local path to an API document, schema, sample, or field list")
    ingest_source_parser.add_argument("--name", default="", help="source display name; defaults to file stem")
    ingest_source_parser.add_argument("--provider", default="", help="source provider or owner")
    ingest_source_parser.add_argument("--source-type", default="api", help="source type, default: api")
    ingest_source_parser.add_argument("--document-type", default="api_document", help="document type, default: api_document")
    ingest_source_parser.add_argument("--description", default="", help="source description")
    ingest_source_parser.add_argument(
        "--llm-mode",
        choices=["env", "disabled", "agent_manual", "manual", "codex_manual", "openai"],
        default="env",
        help="deprecated alias for --agent-mode; openai is no longer supported",
    )
    ingest_source_parser.add_argument(
        "--agent-mode",
        choices=["env", "disabled", "manual", "agent_manual"],
        default="",
        help="agent mode for this run; use manual to wait for or consume an explicit agent response artifact",
    )
    ingest_source_parser.add_argument(
        "--manual-llm-response",
        default="",
        help="deprecated alias for --agent-response",
    )
    ingest_source_parser.add_argument(
        "--agent-response",
        default="",
        help="optional JSON file containing the agent response artifact",
    )
    ingest_queued_parser = context_platform_subparsers.add_parser(
        "ingest-queued-source",
        help="run agent/manual ingestion for a source document already queued from the dashboard",
    )
    ingest_queued_parser.add_argument("run_id", help="onboarding run id created by Source Intake upload")
    ingest_queued_parser.add_argument(
        "--llm-mode",
        choices=["env", "disabled", "agent_manual", "manual", "codex_manual", "openai"],
        default="env",
        help="deprecated alias for --agent-mode; openai is no longer supported",
    )
    ingest_queued_parser.add_argument(
        "--agent-mode",
        choices=["env", "disabled", "manual", "agent_manual"],
        default="",
        help="agent mode for this run; use manual to wait for or consume an explicit agent response artifact",
    )
    ingest_queued_parser.add_argument(
        "--manual-llm-response",
        default="",
        help="deprecated alias for --agent-response",
    )
    ingest_queued_parser.add_argument(
        "--agent-response",
        default="",
        help="optional JSON file containing the agent response artifact",
    )
    draft_contract_parser = context_platform_subparsers.add_parser(
        "draft-source-contract",
        help="draft a source_structure agent response section with LangExtract in the worker",
    )
    draft_contract_parser.add_argument("source_path", help="local path to a source/API document")
    draft_contract_parser.add_argument("--output", default="", help="write the generated agent response JSON to this path")
    draft_contract_parser.add_argument("--model-id", default="", help="LangExtract model id; defaults to LANGEXTRACT_MODEL_ID or worker default")
    draft_contract_parser.add_argument("--name", default="", help="source display name; defaults to file stem")
    draft_contract_parser.add_argument("--source-type", default="api", help="source type, default: api")

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
    if args.command == "context-platform":
        if args.context_platform_command == "reset":
            return reset_context_platform()
        if args.context_platform_command == "seed-registry":
            return seed_context_platform()
        if args.context_platform_command == "ingest-source":
            return ingest_context_platform_source(
                source_path=args.source_path,
                name=args.name,
                provider=args.provider,
                source_type=args.source_type,
                document_type=args.document_type,
                description=args.description,
                llm_mode=args.agent_mode or args.llm_mode,
                manual_llm_response_path=args.agent_response or args.manual_llm_response,
            )
        if args.context_platform_command == "ingest-queued-source":
            return ingest_queued_context_platform_source(
                run_id=args.run_id,
                llm_mode=args.agent_mode or args.llm_mode,
                manual_llm_response_path=args.agent_response or args.manual_llm_response,
            )
        if args.context_platform_command == "draft-source-contract":
            return draft_context_platform_source_contract(
                source_path=args.source_path,
                output_path=args.output,
                model_id=args.model_id,
                source_name=args.name,
                source_type=args.source_type,
            )
    raise SystemExit(f"unknown command: {args.command}")
