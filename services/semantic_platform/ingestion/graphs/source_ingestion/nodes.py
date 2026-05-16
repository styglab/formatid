from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

from services.semantic_platform.ingestion.catalog.context_loader import load_catalog_context
from services.semantic_platform.ingestion.chunking.source_chunking import _split_source_chunks
from services.semantic_platform.ingestion.extraction.text_extraction import _extract_docx_text, _extract_zip_text
from services.semantic_platform.ingestion.graphs.source_ingestion.state import SourceGraphState
from services.semantic_platform.ingestion.llm.source_proposal_analyzer import analyze_source_with_llm
from services.semantic_platform.ingestion.proposals.catalog_applier import apply_catalog_changes
from services.semantic_platform.ingestion.proposals.proposal_writer import apply_if_requested, write_proposals


def read_source(state: SourceGraphState) -> SourceGraphState:
    path = Path(state["source_path"])
    raw = path.read_bytes()
    state["sha256"] = hashlib.sha256(raw).hexdigest()
    state["raw_bytes_size"] = len(raw)
    provider_hint = state.get("provider_hint")
    state["provider"] = provider_hint if provider_hint else "unknown"
    state["document_id"] = _document_id(path, state["sha256"])
    state["messages"].append(f"read_source:{path}")
    return state


def extract_text(state: SourceGraphState) -> SourceGraphState:
    path = Path(state["source_path"])
    if path.suffix.lower() in {".md", ".txt"}:
        text = path.read_text(encoding="utf-8")
    elif path.suffix.lower() == ".docx":
        text = _extract_docx_text(path)
    elif path.suffix.lower() == ".zip":
        text = _extract_zip_text(path)
    else:
        text = path.read_bytes().decode("utf-8", errors="ignore")
    state["extracted_text"] = text
    state["messages"].append(f"extract_text:chars={len(text)}")
    return state


def split_source_chunks(state: SourceGraphState) -> SourceGraphState:
    chunks = _split_source_chunks(
        text=state["extracted_text"],
        document_id=state["document_id"],
        provider=state["provider"],
    )
    state["source_chunks"] = chunks
    state["messages"].append(
        "split_source_chunks:"
        f"chunks={len(chunks)},"
        f"operations={sum(1 for chunk in chunks if chunk.get('operation_id'))}"
    )
    return state


def write_source_chunks(state: SourceGraphState) -> SourceGraphState:
    output_dir = Path(state["chunks_output_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{state['document_id']}.chunks.jsonl"
    with output_path.open("w", encoding="utf-8") as file:
        for chunk in state.get("source_chunks", []):
            file.write(json.dumps(chunk, ensure_ascii=False, sort_keys=True) + "\n")
    state["chunks_path"] = str(output_path)
    state["messages"].append(f"write_source_chunks:{output_path}")
    return state


def _document_id(path: Path, source_sha256: str) -> str:
    stem = re.sub(r"[^0-9A-Za-z가-힣]+", "_", path.stem).strip("_").lower()
    return f"source.{source_sha256[:8]}.{stem}"
