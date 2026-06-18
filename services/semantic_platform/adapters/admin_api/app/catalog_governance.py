from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from services.semantic_platform.adapters.admin_api.app.common import paged
from services.semantic_platform.internal.storage import SemanticLayerRepository


router = APIRouter()


class ReviewPayload(BaseModel):
    reviewer: str = "admin"


@router.get("/api/proposals")
def list_proposals(status: str = Query(default=""), entity_type: str = Query(default="")) -> list[dict[str, Any]]:
    proposals = SemanticLayerRepository().list_proposals(status=status)
    if entity_type:
        proposals = [item for item in proposals if item.get("entity_type") == entity_type]
    return proposals


@router.get("/api/proposals/page")
def list_proposals_page(
    status: str = Query(default=""),
    entity_type: str = Query(default=""),
    query: str = Query(default=""),
    ids: str = Query(default=""),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
) -> dict[str, Any]:
    proposals = SemanticLayerRepository().list_proposals(status=status)
    if entity_type:
        proposals = [item for item in proposals if item.get("entity_type") == entity_type]
    proposal_ids = {item.strip() for item in ids.split(",") if item.strip()}
    if proposal_ids:
        proposals = [item for item in proposals if item.get("id") in proposal_ids]
    if query:
        lowered = query.lower()
        proposals = [
            item
            for item in proposals
            if lowered in str(item.get("id", "")).lower()
            or lowered in str(item.get("title", "")).lower()
            or lowered in str(item.get("entity_type", "")).lower()
            or lowered in str(item.get("entity_id", "")).lower()
        ]
    return paged(proposals, page, page_size)


@router.post("/api/proposals/{proposal_id}/approve")
def approve_proposal(proposal_id: str, payload: ReviewPayload) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().review_proposal(
            proposal_id,
            "approved",
            reviewer=payload.reviewer,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="proposal not found") from exc


@router.post("/api/proposals/{proposal_id}/reject")
def reject_proposal(proposal_id: str, payload: ReviewPayload) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().review_proposal(
            proposal_id,
            "rejected",
            reviewer=payload.reviewer,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="proposal not found") from exc
