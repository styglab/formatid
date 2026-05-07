from __future__ import annotations

from fastapi import APIRouter, File, HTTPException, Request, UploadFile

from apps.spec_rag.api.app.domain.spec_rag.schemas import (
    SpecRagRunCreateResponse,
    SpecRagRunResponse,
)
from apps.spec_rag.api.app.domain.spec_rag.service import SpecRagService


router = APIRouter(prefix="/api/v1/spec-rag", tags=["spec-rag"])


@router.post("", response_model=SpecRagRunCreateResponse)
async def create_spec_rag(
    request: Request,
    file: UploadFile = File(...),
) -> SpecRagRunCreateResponse:
    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="uploaded file is empty")
    service = SpecRagService()
    return await service.create_run(
        filename=file.filename or "upload.txt",
        content_type=file.content_type or "application/octet-stream",
        content=content,
        request_id=getattr(request.state, "request_id", None),
        correlation_id=getattr(request.state, "correlation_id", None),
    )


@router.get("/{run_id}", response_model=SpecRagRunResponse)
async def get_spec_rag_run(run_id: str) -> SpecRagRunResponse:
    service = SpecRagService()
    run = await service.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="spec rag run not found")
    return run
