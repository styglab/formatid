from __future__ import annotations

import hashlib
import json
import mimetypes
import os
import re
import tempfile
import threading
import traceback
import uuid
from pathlib import Path
from datetime import datetime, timezone
from typing import Any

from services.semantic_platform.lib.context import runtime_context
from services.semantic_platform.lib.ingestion.graph import run_source_ingestion
from services.semantic_platform.lib.ingestion.llm.runtime import llm_secret_context
from services.semantic_platform.lib.object_store import get_object_bytes, put_object
from services.semantic_platform.lib.storage import SemanticCatalogRepository
from services.semantic_platform.lib.storage.repository import llm_mode


def repository() -> SemanticCatalogRepository:
    return SemanticCatalogRepository()


def load_catalog() -> dict[str, Any]:
    return repository().catalog()


def load_catalog_section(section: str, limit: int = 100, offset: int = 0, q: str | None = None) -> dict[str, Any]:
    return repository().catalog_section(section=section, limit=limit, offset=offset, q=q)


def update_catalog_item(section: str, item_id: str, document: dict[str, Any]) -> dict[str, Any]:
    return repository().update_catalog_item(section, item_id, document)


def catalog_item_delete_plan(section: str, item_id: str) -> dict[str, Any]:
    return repository().catalog_item_delete_plan(section, item_id)


def delete_catalog_item(section: str, item_id: str, document: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = document or {}
    return repository().delete_catalog_item(section, item_id, mode=str(payload.get("mode") or "deprecate"))


def load_execution_contracts() -> dict[str, Any]:
    return repository().execution_contracts()


def list_capability_documents(limit: int = 100) -> dict[str, Any]:
    return repository().capability_documents(limit=limit)


def rebuild_capability_documents() -> dict[str, Any]:
    return repository().rebuild_capability_documents()


def embed_capability_documents(
    limit: int = 100,
    force: bool = False,
    capability_ids: list[str] | None = None,
    document_ids: list[str] | None = None,
) -> dict[str, Any]:
    return repository().embed_capability_documents(
        limit=limit,
        force=force,
        capability_ids=capability_ids,
        document_ids=document_ids,
    )


def retrieve_capabilities(query: str, limit: int = 10) -> dict[str, Any]:
    return repository().retrieve_capabilities(query, limit=limit)


def list_execution_graphs(limit: int = 100) -> dict[str, Any]:
    return repository().execution_graphs(limit=limit)


def list_catalog_versions(limit: int = 100, offset: int = 0) -> dict[str, Any]:
    return repository().catalog_versions(limit=limit, offset=offset)


def create_catalog_version(document: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = document or {}
    reason = str(payload.get("reason") or "manual_snapshot")
    note = str(payload.get("note") or "").strip()
    metadata = {"source": "dashboard_manual_snapshot"}
    if note:
        metadata["note"] = note
    return repository().create_catalog_version(
        reason=reason,
        created_by=str(payload.get("created_by") or "api"),
        metadata=metadata,
    )


def read_catalog_version(version_id: str) -> dict[str, Any]:
    return repository().catalog_version(version_id)


def export_catalog_version(version_id: str) -> dict[str, Any]:
    version = repository().catalog_version(version_id)["catalog_version"]
    return {
        "export_type": "semantic_platform_catalog_version",
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "catalog_version": {
            "id": version.get("id"),
            "version_number": version.get("version_number"),
            "status": version.get("status"),
            "reason": version.get("reason"),
            "proposal_id": version.get("proposal_id"),
            "snapshot_sha256": version.get("snapshot_sha256"),
            "counts": version.get("counts") or {},
            "metadata": version.get("metadata") or {},
            "created_by": version.get("created_by"),
            "created_at": version.get("created_at"),
        },
        "snapshot": version.get("snapshot") or {},
    }


def read_catalog_version_diff(version_id: str, base_version_id: str | None = None) -> dict[str, Any]:
    return repository().catalog_version_diff(version_id, base_version_id=base_version_id)


def restore_catalog_version(version_id: str) -> dict[str, Any]:
    return repository().restore_catalog_version(version_id, restored_by="api")


def upsert_execution_graph(graph: dict[str, Any]) -> dict[str, Any]:
    return repository().upsert_execution_graph(graph)


def list_planner_feedback(limit: int = 100) -> dict[str, Any]:
    return repository().planner_feedback(limit=limit)


def record_planner_feedback(feedback: dict[str, Any]) -> dict[str, Any]:
    return repository().record_planner_feedback(feedback)


def catalog_metadata() -> dict[str, Any]:
    return repository().meta()


def list_sources() -> dict[str, Any]:
    return repository().sources()


def list_ingestion_runs(limit: int = 100, source_id: str | None = None) -> dict[str, Any]:
    return repository().ingestion_runs(limit=limit, source_id=source_id)


def read_ingestion_run(run_id: str) -> dict[str, Any]:
    return repository().ingestion_run(run_id)


def start_source_ingestion(source_id: str, document: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = document or {}
    commit_mode = str(payload.get("commit_mode") or "proposal")
    if commit_mode not in {"proposal", "direct_apply"}:
        raise ValueError("commit_mode must be proposal or direct_apply")
    force = bool(payload.get("force", False))
    manual_llm_response = payload.get("manual_llm_response") if isinstance(payload.get("manual_llm_response"), dict) else None
    requested_llm_mode = str(payload.get("llm_mode") or "").strip().lower()
    if requested_llm_mode and requested_llm_mode not in {"codex_manual", "openai"}:
        raise ValueError("request llm_mode override supports codex_manual or openai only")
    llm_secret_ref = str(payload.get("llm_secret_ref") or "").strip() or None
    if manual_llm_response is not None and not _manual_llm_response_has_catalog_items(manual_llm_response):
        raise ValueError("manual_llm_response must contain at least one semantic catalog item")
    effective_llm_mode = "codex_manual" if manual_llm_response is not None else (requested_llm_mode or llm_mode())
    if requested_llm_mode == "codex_manual" and manual_llm_response is None:
        raise ValueError("manual_llm_response is required when llm_mode is codex_manual")
    llm_secret_value = _resolve_llm_secret(llm_secret_ref) if llm_secret_ref else None
    if manual_llm_response is None and effective_llm_mode != "openai":
        raise ValueError("OpenAI service mode is not ready; provide manual_llm_response for codex_manual or request llm_mode=openai")
    if manual_llm_response is None and not (llm_secret_value or os.getenv("OPENAI_API_KEY")):
        raise ValueError("OpenAI API key is not ready; select an LLM secret or configure OPENAI_API_KEY")
    requested_by = str(payload.get("requested_by") or "").strip() or None
    revision_id = str(payload.get("revision_id") or "").strip() or None
    revision_ref = repository().source_revision(source_id, revision_id)
    revision = revision_ref["revision"]
    selected_revision_id = str(revision["id"])
    run_id = f"ingestion_run.{uuid.uuid4().hex[:16]}"
    run = repository().create_ingestion_run(
        run_id=run_id,
        source_id=source_id,
        revision_id=selected_revision_id,
        commit_mode=commit_mode,
        requested_by=requested_by,
        request={
            "source_id": source_id,
            "revision_id": selected_revision_id,
            "commit_mode": commit_mode,
            "force": force,
            "llm_mode": effective_llm_mode,
            "llm_secret_ref": llm_secret_ref,
            "manual_llm_response_provided": manual_llm_response is not None,
        },
    )
    thread = threading.Thread(
        target=_run_source_ingestion_background,
        kwargs={
            "run_id": run_id,
            "source_id": source_id,
            "revision_id": selected_revision_id,
            "commit_mode": commit_mode,
            "force": force,
            "manual_llm_response": manual_llm_response,
            "llm_secret_ref": llm_secret_ref,
        },
        daemon=True,
    )
    thread.start()
    return {"ingestion_run": run}


def _resolve_llm_secret(secret_ref: str | None) -> str | None:
    if not secret_ref:
        return None
    value = repository().secret_value(secret_ref)
    if not value:
        raise ValueError(f"LLM secret not found or has no value: {secret_ref}")
    return value


def _manual_llm_response_has_catalog_items(manual_llm_response: dict[str, Any]) -> bool:
    catalog_keys = (
        "resources",
        "operations",
        "operation_fields",
        "semantic_types",
        "entities",
        "entity_identifiers",
        "capabilities",
        "capability_entity_links",
        "capability_dependencies",
        "operation_contracts",
        "operation_variants",
        "field_mappings",
        "semantic_join_rules",
        "planning_examples",
        "capability_implementations",
    )
    for key in catalog_keys:
        value = manual_llm_response.get(key)
        if isinstance(value, list) and value:
            return True
    return False


def list_secrets() -> dict[str, Any]:
    return repository().secrets()


def upsert_secret(document: dict[str, Any]) -> dict[str, Any]:
    name = str(document.get("name") or "").strip()
    provider = str(document.get("provider") or "").strip() or None
    secret_id = str(document.get("id") or "").strip()
    allow_update = bool(document.get("allow_update"))
    if not secret_id:
        raise ValueError("key is required")
    if not name:
        raise ValueError("name is required")
    if not secret_id.startswith("secret."):
        secret_id = f"secret.{secret_id}"
    return repository().upsert_secret(
        secret_id=secret_id,
        provider=provider,
        name=name,
        description=str(document.get("description") or "").strip() or None,
        secret_value=str(document.get("value") or ""),
        metadata={"secret_id_policy": "user_provided"},
        allow_update=allow_update,
    )


def delete_secret(secret_id: str) -> dict[str, Any]:
    return repository().delete_secret(secret_id)


def update_source(source_id: str, document: dict[str, Any]) -> dict[str, Any]:
    title = str(document.get("title") or "").strip()
    if not title:
        raise ValueError("title is required")
    return repository().update_source(
        source_id=source_id,
        provider=str(document.get("provider") or "").strip() or None,
        provider_name_ko=str(document.get("provider_name_ko") or "").strip() or None,
        title=title,
        auth_secret_refs=[_secret_id(item) for item in parse_list_field(document.get("auth_secret_refs")) if _secret_id(item)],
        auth_parameter_names=parse_list_field(document.get("auth_parameter_names")),
        status=str(document.get("status") or "").strip() or None,
    )


def delete_source(source_id: str) -> dict[str, Any]:
    return repository().delete_source(source_id, mode="archive")


def source_delete_plan(source_id: str) -> dict[str, Any]:
    return repository().source_delete_plan(source_id)


def delete_source_with_mode(source_id: str, document: dict[str, Any]) -> dict[str, Any]:
    return repository().delete_source(source_id, mode=str(document.get("mode") or "archive"))


def upload_source(
    *,
    file_name: str,
    content: bytes,
    provider: str | None = None,
    provider_name_ko: str | None = None,
    title: str | None = None,
    source_id: str | None = None,
    content_type: str | None = None,
    auth_secret_refs: list[str] | None = None,
    auth_parameter_names: list[str] | None = None,
    uploaded_by: str | None = None,
    allow_update: bool = False,
) -> dict[str, Any]:
    sha256 = hashlib.sha256(content).hexdigest()
    source_id_policy = "user_provided" if source_id and source_id.strip() else "system_generated"
    source_id = _source_id(
        source_id,
        sha256=sha256,
        provider=provider,
        title=title or file_name,
    )
    if not allow_update and repository().source_exists(source_id):
        raise FileExistsError(source_id)
    existing_revision = repository().source_revision_by_sha(source_id, sha256)
    if existing_revision:
        return existing_revision
    provider_slug = _slug(provider or "unknown")
    next_revision = _next_revision_hint(source_id)
    safe_file_name = _safe_file_name(file_name)
    object_key = f"raw/{provider_slug}/{source_id}/revisions/{next_revision:03d}/{safe_file_name}"
    object_ref = put_object(
        key=object_key,
        body=content,
        content_type=content_type or mimetypes.guess_type(file_name)[0],
    )
    metadata = {
        "source_id_policy": source_id_policy,
        "original_file_name": file_name,
    }
    result = repository().create_source_revision(
        source_id=source_id,
        provider=provider,
        provider_name_ko=provider_name_ko,
        title=title or file_name,
        file_name=file_name,
        content_type=content_type or mimetypes.guess_type(file_name)[0],
        size_bytes=len(content),
        sha256=sha256,
        object_uri=object_ref.uri,
        object_bucket=object_ref.bucket,
        object_key=object_ref.key,
        auth_secret_refs=[_secret_id(item) for item in (auth_secret_refs or []) if _secret_id(item)],
        auth_parameter_names=auth_parameter_names or [],
        uploaded_by=uploaded_by,
        metadata=metadata,
        allow_update=allow_update,
    )
    return {
        **result,
        "object": {"bucket": object_ref.bucket, "key": object_ref.key, "uri": object_ref.uri},
    }


def list_proposals(
    *,
    status: str | None = None,
    limit: int = 100,
    offset: int = 0,
    include_payload: bool = False,
) -> dict[str, Any]:
    return repository().proposals(
        status=status,
        limit=limit,
        offset=offset,
        include_payload=include_payload,
    )


def read_proposal(proposal_id: str) -> dict[str, Any]:
    return repository().proposal(proposal_id)


def update_proposal_item(proposal_id: str, item_id: str, document: dict[str, Any]) -> dict[str, Any]:
    payload = document.get("payload") if isinstance(document.get("payload"), dict) else None
    immutable_keys = {"evidence", "status", "action", "item_type", "target_id", "proposal_id"}
    blocked_keys = sorted(key for key in immutable_keys if key in document)
    if blocked_keys:
        raise ValueError(f"proposal item fields are read-only: {', '.join(blocked_keys)}")
    if payload is None:
        raise ValueError("payload is required")
    return repository().update_proposal_item(
        proposal_id,
        item_id,
        payload=payload,
    )


def apply_proposal(proposal_id: str) -> dict[str, Any]:
    repo = repository()
    result = repo.apply_proposal(proposal_id, reviewer="api")
    capability_ids = [
        str(item.get("target_id"))
        for item in result.get("applied", [])
        if isinstance(item, dict) and item.get("item_type") == "capability" and item.get("target_id")
    ]
    result["embeddings"] = repo.embed_capability_documents(
        limit=max(len(capability_ids), 1),
        force=True,
        capability_ids=capability_ids,
    )
    return result


def reject_proposal(proposal_id: str) -> dict[str, Any]:
    return repository().reject_proposal(proposal_id)


def sources_summary() -> dict[str, Any]:
    return {"summary": repository().meta()["counts"]}


def _run_source_ingestion_background(
    *,
    run_id: str,
    source_id: str,
    revision_id: str,
    commit_mode: str,
    force: bool,
    manual_llm_response: dict[str, Any] | None = None,
    llm_secret_ref: str | None = None,
) -> None:
    repo = repository()
    temp_path: Path | None = None
    temp_dir: tempfile.TemporaryDirectory[str] | None = None
    try:
        repo.update_ingestion_run(run_id, status="running", current_step="prepare_source", mark_started=True)
        revision_ref = repo.source_revision(source_id, revision_id)
        source = revision_ref["source"]
        revision = revision_ref["revision"]
        body = get_object_bytes(bucket=revision.get("object_bucket"), key=revision["object_key"])
        temp_dir = tempfile.TemporaryDirectory()
        file_name = str(revision.get("file_name") or "source_document")
        temp_path = Path(temp_dir.name) / file_name
        temp_path.write_bytes(body)
        temp_path.with_suffix(temp_path.suffix + ".source.json").write_text(
            json.dumps(
                {
                    "source_id": source_id,
                    "provider": source.get("provider"),
                    "provider_name_ko": source.get("provider_name_ko"),
                    "title": source.get("title"),
                    "revision_id": revision_id,
                    "auth_secret_refs": source.get("auth_secret_refs") or [],
                    "auth_parameter_names": source.get("auth_parameter_names") or [],
                    "source_registry": "semantic_platform",
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        repo.update_ingestion_run(run_id, status="running", current_step="run_graph")
        llm_secret_value = _resolve_llm_secret(llm_secret_ref) if llm_secret_ref and manual_llm_response is None else None
        def progress_callback(event: dict[str, Any]) -> None:
            step = str(event.get("step") or "run_graph")
            repo.update_ingestion_run(
                run_id,
                status="running",
                current_step=step,
                result={"progress": event},
            )

        with llm_secret_context(openai_key=llm_secret_value, mode="codex_manual" if manual_llm_response else "openai"):
            result = run_source_ingestion(
                temp_path,
                manual_llm_response=manual_llm_response,
                apply=commit_mode == "direct_apply",
                force=force,
                progress_callback=progress_callback,
            )
        repo.update_ingestion_run(
            run_id,
            status="succeeded",
            current_step="completed",
            result=result,
            mark_finished=True,
        )
    except Exception as exc:  # pragma: no cover - background safety
        repo.update_ingestion_run(
            run_id,
            status="failed",
            current_step="failed",
            error_message=f"{type(exc).__name__}: {exc}",
            result={"traceback": traceback.format_exc()[-8000:]},
            mark_finished=True,
        )
    finally:
        if temp_dir:
            try:
                temp_dir.cleanup()
            except OSError:
                pass


def parse_list_field(value: str | None) -> list[str]:
    if value is None or not value.strip():
        return []
    stripped = value.strip()
    if stripped.startswith("["):
        parsed = json.loads(stripped)
        if not isinstance(parsed, list):
            raise ValueError("expected JSON array")
        return [str(item).strip() for item in parsed if str(item).strip()]
    return [item.strip() for item in stripped.split(",") if item.strip()]


def parse_bool_field(value: str | None) -> bool:
    text = str(value or "").strip().lower()
    return text in {"1", "true", "yes", "y", "on"}


def list_evidence_snapshots(source_document_id: str | None = None, limit: int = 100) -> dict[str, Any]:
    return repository().evidence_snapshots(source_document_id=source_document_id, limit=limit)


def list_endpoint_checks(
    operation_id: str | None = None,
    variant_id: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    return repository().endpoint_checks(operation_id=operation_id, variant_id=variant_id, limit=limit)


def record_endpoint_check(check: dict[str, Any]) -> dict[str, Any]:
    return repository().record_endpoint_check(check)


def _source_id(
    source_id: str | None,
    *,
    sha256: str,
    provider: str | None = None,
    title: str | None = None,
) -> str:
    if source_id and source_id.strip():
        text = source_id.strip()
        return text if text.startswith("source.") else f"source.{text}"
    provider_slug = _slug(provider or "unknown")
    title_slug = _slug(title or "")
    if title_slug == "unknown":
        title_slug = sha256[:8]
    return f"source.{provider_slug}.{title_slug}"


def _secret_id(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return text if text.startswith("secret.") else f"secret.{text}"


def _next_revision_hint(source_id: str) -> int:
    sources = repository().sources().get("sources", [])
    for source in sources:
        if source.get("id") == source_id and source.get("revision_number"):
            return int(source["revision_number"]) + 1
    return 1


def _safe_file_name(file_name: str) -> str:
    name = file_name.strip().replace("\\", "_").replace("/", "_")
    return name or "source_document"


def _slug(value: str) -> str:
    slug = re.sub(r"[^\w.-]+", "_", value.strip().lower())
    slug = re.sub(r"_+", "_", slug).strip("_")
    return slug or "unknown"
