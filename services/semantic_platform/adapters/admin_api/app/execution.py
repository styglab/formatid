from __future__ import annotations

import hashlib
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, File, Form, HTTPException, Query, UploadFile
from pydantic import BaseModel, Field

from services.semantic_platform.adapters.admin_api.app.common import (
    paged,
    payload_dict,
)
from services.semantic_platform.adapters.worker.deployments import submit_onboarding_run
from services.semantic_platform.internal.storage import SemanticLayerRepository


router = APIRouter()
UPLOAD_ROOT = Path("/tmp/semantic_platform_uploads")


class ExecutionSourcePayload(BaseModel):
    name: str | None = Field(default=None)
    provider: str = ""
    source_type: str = "api"
    description: str = ""
    status: str = "draft"
    config: dict[str, Any] = Field(default_factory=dict)
@router.get("/api/execution-sources")
def list_execution_sources(
    query: str = Query(default=""),
    status: str = Query(default=""),
) -> list[dict[str, Any]]:
    return SemanticLayerRepository().list_execution_sources(query=query, status=status)


@router.get("/api/execution-sources/page")
def list_execution_sources_page(
    query: str = Query(default=""),
    status: str = Query(default=""),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
) -> dict[str, Any]:
    return paged(SemanticLayerRepository().list_execution_sources(query=query, status=status), page, page_size)


@router.get("/api/execution-assets")
def list_execution_assets(
    query: str = Query(default=""),
    status: str = Query(default=""),
    source_id: str = Query(default=""),
) -> list[dict[str, Any]]:
    return SemanticLayerRepository().list_execution_assets(query=query, status=status, source_id=source_id)


@router.get("/api/execution-assets/{asset_id}")
def get_execution_asset(asset_id: str) -> dict[str, Any]:
    record = SemanticLayerRepository().get_execution_asset(asset_id)
    if record is None:
        raise HTTPException(status_code=404, detail="execution asset not found")
    return record


@router.get("/api/execution-operations")
def list_execution_operations(
    query: str = Query(default=""),
    status: str = Query(default=""),
    source_id: str = Query(default=""),
    asset_id: str = Query(default=""),
) -> list[dict[str, Any]]:
    return SemanticLayerRepository().list_execution_operations(
        query=query,
        status=status,
        source_id=source_id,
        asset_id=asset_id,
    )


@router.get("/api/execution-operations/page")
def list_execution_operations_page(
    query: str = Query(default=""),
    status: str = Query(default=""),
    source_id: str = Query(default=""),
    asset_id: str = Query(default=""),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
) -> dict[str, Any]:
    return paged(
        SemanticLayerRepository().list_execution_operations(
            query=query,
            status=status,
            source_id=source_id,
            asset_id=asset_id,
        ),
        page,
        page_size,
    )


@router.get("/api/execution-operations/{operation_id}")
def get_execution_operation(operation_id: str) -> dict[str, Any]:
    record = SemanticLayerRepository().get_execution_operation(operation_id)
    if record is None:
        raise HTTPException(status_code=404, detail="execution operation not found")
    return record


@router.get("/api/operation-fields")
def list_operation_fields(
    operation_id: str = Query(default=""),
    variant_id: str = Query(default=""),
) -> list[dict[str, Any]]:
    return SemanticLayerRepository().list_operation_fields(operation_id=operation_id, variant_id=variant_id)


@router.post("/api/execution-sources")
def create_execution_source(payload: ExecutionSourcePayload) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().create_execution_source(payload_dict(payload, exclude_unset=False))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/api/execution-sources/upload")
async def upload_execution_source(
    file: UploadFile = File(...),
    name: str = Form(...),
    provider: str = Form(default=""),
    source_type: str = Form(default="api"),
    description: str = Form(default=""),
    status: str = Form(default="draft"),
    reference_uri: str = Form(default=""),
    manual_notes: str = Form(default=""),
) -> dict[str, Any]:
    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="uploaded file is empty")

    UPLOAD_ROOT.mkdir(parents=True, exist_ok=True)
    safe_name = Path(file.filename or f"upload-{uuid4().hex}").name
    stored_name = f"{uuid4().hex}-{safe_name}"
    stored_path = UPLOAD_ROOT / stored_name
    stored_path.write_bytes(content)

    preview = _text_preview(content)
    content_hash = hashlib.sha256(content).hexdigest()
    upload_metadata = {
        "filename": safe_name,
        "media_type": file.content_type or "application/octet-stream",
        "stored_path": str(stored_path),
        "size_bytes": len(content),
        "sha256": content_hash,
        "preview": preview,
        "suggestion_generation": {
            "mode": "deterministic_assist",
            "status": "ready_for_field_extraction",
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "note": "Field-level semantic and transform suggestions are generated after operation fields are extracted.",
        },
    }
    config = {
        "input_mode": "uploaded_file",
        "reference_uri": reference_uri or str(stored_path),
        "manual_notes": manual_notes,
        "upload": upload_metadata,
    }
    payload = {
        "name": name,
        "provider": provider,
        "source_type": source_type,
        "description": description or f"Uploaded source document: {safe_name}",
        "status": status,
        "config": config,
    }
    try:
        repository = SemanticLayerRepository()
        created = repository.create_execution_source(payload)
        onboarding = repository.create_onboarding_run_for_source(
            source=created.get("execution_source"),
            proposal=created.get("proposal"),
            upload_metadata=upload_metadata,
        )
        trigger = submit_onboarding_run(str((onboarding.get("onboarding_run") or {}).get("id") or ""))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return {
        "source": created.get("execution_source"),
        "proposal": created.get("proposal"),
        "onboarding_run": onboarding.get("onboarding_run"),
        "evidence_snapshot": onboarding.get("evidence_snapshot"),
        "proposal_bundle": onboarding.get("proposal_bundle"),
        "work_queue_tasks": onboarding.get("work_queue_tasks") or [],
        "prefect_trigger": trigger,
        "upload": upload_metadata,
    }


@router.get("/api/execution-sources/{source_id}")
def get_execution_source(source_id: str) -> dict[str, Any]:
    record = SemanticLayerRepository().get_execution_source(source_id)
    if record is None:
        raise HTTPException(status_code=404, detail="execution source not found")
    return record


@router.patch("/api/execution-sources/{source_id}")
def update_execution_source(source_id: str, payload: ExecutionSourcePayload) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().update_execution_source(
            source_id,
            payload_dict(payload, exclude_unset=True),
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="execution source not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.delete("/api/execution-sources/{source_id}")
def delete_execution_source(source_id: str) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().delete_execution_source(source_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="execution source not found") from exc


def _text_preview(content: bytes) -> str:
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError:
        try:
            text = content.decode("cp949")
        except UnicodeDecodeError:
            return ""
    normalized = " ".join(text.split())
    return normalized[:500]
