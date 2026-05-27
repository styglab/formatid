from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

try:
    from prefect import flow
except ImportError:  # pragma: no cover
    def flow(*_args: Any, **_kwargs: Any) -> Any:
        def decorator(func: Any) -> Any:
            return func

        return decorator

from services.semantic_platform.adapters.worker.tasks.run_ingestion_graph import run_ingestion_graph
from services.semantic_platform.adapters.worker.tasks.scan_sources import DEFAULT_IMPORT_ROOT, scan_sources, source_document


@flow(name="semantic-platform-source-ingestion")
def semantic_platform_source_ingestion(
    sources_root: str | Path = DEFAULT_IMPORT_ROOT,
    source: str | Path | None = None,
    commit_mode: str = "proposal",
    manual_llm_response_path: str | Path | None = None,
    llm_secret_ref: str | None = None,
    llm_mode: str | None = None,
    force: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    if commit_mode not in {"proposal", "direct_apply"}:
        raise ValueError("commit_mode must be proposal or direct_apply")
    documents = [source_document(source)] if source else scan_sources(sources_root)
    manual_llm_response = _load_manual_llm_response(manual_llm_response_path)
    if manual_llm_response is not None and not source:
        raise ValueError("manual_llm_response_path can be used only with --source")
    processed = []
    skipped = []
    for document in documents:
        if dry_run:
            skipped.append({"path": document["path"], "reason": "dry_run"})
            continue
        result = run_ingestion_graph(
            document["path"],
            commit_mode=commit_mode,
            manual_llm_response=manual_llm_response,
            llm_secret_ref=llm_secret_ref,
            llm_mode=llm_mode,
            force=force,
        )
        output = {"path": document["path"], "sha256": document["sha256"], **result}
        if result.get("skipped"):
            skipped.append(output)
        else:
            processed.append(output)
    return {
        "sources_root": str(sources_root),
        "commit_mode": commit_mode,
        "manual_llm_response_path": str(manual_llm_response_path) if manual_llm_response_path else None,
        "llm_secret_ref": llm_secret_ref,
        "llm_mode": llm_mode,
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
    parser = argparse.ArgumentParser(description="Run semantic source ingestion into Postgres.")
    parser.add_argument("--sources-root", default=str(DEFAULT_IMPORT_ROOT))
    parser.add_argument("--source", default=None)
    parser.add_argument("--commit-mode", choices=["proposal", "direct_apply"], default="proposal")
    parser.add_argument("--manual-llm-response", default=None)
    parser.add_argument("--llm-secret-ref", default=None)
    parser.add_argument("--llm-mode", default=None)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    result = run_manual_source_ingestion(
        sources_root=args.sources_root,
        source=args.source,
        commit_mode=args.commit_mode,
        manual_llm_response_path=args.manual_llm_response,
        llm_secret_ref=args.llm_secret_ref,
        llm_mode=args.llm_mode,
        force=args.force,
        dry_run=args.dry_run,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
