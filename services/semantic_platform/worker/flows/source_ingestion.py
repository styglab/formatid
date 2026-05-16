from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

try:
    from prefect import flow
except ImportError:  # pragma: no cover - allows local CLI use without Prefect installed.
    def flow(*_args: Any, **_kwargs: Any) -> Any:
        def decorator(func: Any) -> Any:
            return func

        return decorator

from services.semantic_platform.worker.tasks.registry import (
    DEFAULT_REGISTRY_PATH,
    load_registry,
    mark_processed,
    mark_skipped,
    save_registry,
    should_process,
)
from services.semantic_platform.worker.tasks.run_source_graph import run_graph_for_source
from services.semantic_platform.worker.tasks.scan_sources import scan_sources, source_document


DEFAULT_SOURCES_ROOT = Path("sources")
DEFAULT_PROPOSALS_DIR = Path("sources/proposals")
DEFAULT_CHUNKS_DIR = Path("sources/chunks")


@flow(name="semantic-platform-source-ingestion")
def semantic_platform_source_ingestion(
    sources_root: str | Path = DEFAULT_SOURCES_ROOT,
    output_dir: str | Path = DEFAULT_PROPOSALS_DIR,
    chunks_output_dir: str | Path = DEFAULT_CHUNKS_DIR,
    registry_path: str | Path = DEFAULT_REGISTRY_PATH,
    source: str | Path | None = None,
    provider: str | None = None,
    commit_mode: str = "proposal",
    manual_llm_response_path: str | Path | None = None,
    force: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    if commit_mode not in {"proposal", "direct_apply"}:
        raise ValueError("commit_mode must be proposal or direct_apply")
    registry = load_registry(registry_path)
    documents = [source_document(source)] if source else scan_sources(sources_root)
    manual_llm_response = _load_manual_llm_response(manual_llm_response_path)
    if manual_llm_response is not None and not source:
        raise ValueError("manual_llm_response_path can be used only with --source")
    processed = []
    skipped = []
    for document in documents:
        if not should_process(document, registry, force):
            mark_skipped(registry, document, "unchanged")
            skipped.append({"path": document["path"], "reason": "unchanged"})
            continue
        if dry_run:
            skipped.append({"path": document["path"], "reason": "dry_run"})
            continue
        result = run_graph_for_source(
            document["path"],
            output_dir,
            chunks_output_dir,
            provider=provider,
            commit_mode=commit_mode,
            manual_llm_response=manual_llm_response,
        )
        status = "catalog_applied" if commit_mode == "direct_apply" else "proposal_written"
        mark_processed(registry, document, result, status=status)
        processed.append(
            {
                "path": document["path"],
                "sha256": document["sha256"],
                "proposal_path": result.get("proposal_path"),
                "chunks_path": result.get("chunks_path"),
                "commit_mode": result.get("commit_mode"),
                "applied_changes": result.get("applied_changes", []),
                "messages": result.get("messages", []),
            }
        )
    if not dry_run:
        save_registry(registry, registry_path)
    return {
        "sources_root": str(sources_root),
        "output_dir": str(output_dir),
        "chunks_output_dir": str(chunks_output_dir),
        "registry_path": str(registry_path),
        "commit_mode": commit_mode,
        "manual_llm_response_path": str(manual_llm_response_path) if manual_llm_response_path else None,
        "force": force,
        "dry_run": dry_run,
        "processed": processed,
        "skipped": skipped,
        "summary": {
            "document_count": len(documents),
            "processed_count": len(processed),
            "skipped_count": len(skipped),
        },
    }


run_manual_source_ingestion = semantic_platform_source_ingestion


def _load_manual_llm_response(path: str | Path | None) -> dict[str, Any] | None:
    if not path:
        return None
    document = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(document, dict):
        raise ValueError("--manual-llm-response must point to a JSON object")
    return document


def main() -> None:
    parser = argparse.ArgumentParser(description="Run manual semantic source ingestion worker.")
    parser.add_argument("--sources-root", default=str(DEFAULT_SOURCES_ROOT))
    parser.add_argument("--output-dir", default=str(DEFAULT_PROPOSALS_DIR))
    parser.add_argument("--chunks-output-dir", default=str(DEFAULT_CHUNKS_DIR))
    parser.add_argument("--registry", default=str(DEFAULT_REGISTRY_PATH))
    parser.add_argument("--source", default=None, help="Process one source file instead of scanning sources root.")
    parser.add_argument("--provider", default=None, help="Optional provider hint passed to source graph.")
    parser.add_argument(
        "--commit-mode",
        choices=["proposal", "direct_apply"],
        default="proposal",
        help="proposal writes review files; direct_apply updates catalog files directly.",
    )
    parser.add_argument(
        "--manual-llm-response",
        default=None,
        help="Path to an explicit JSON LLM response payload used only when LLM_MODE=codex_manual.",
    )
    parser.add_argument("--force", action="store_true", help="Process sources even when sha256 is unchanged.")
    parser.add_argument("--dry-run", action="store_true", help="Scan and report without writing proposals or registry.")
    args = parser.parse_args()
    result = run_manual_source_ingestion(
        sources_root=args.sources_root,
        output_dir=args.output_dir,
        chunks_output_dir=args.chunks_output_dir,
        registry_path=args.registry,
        source=args.source,
        provider=args.provider,
        commit_mode=args.commit_mode,
        manual_llm_response_path=args.manual_llm_response,
        force=args.force,
        dry_run=args.dry_run,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
