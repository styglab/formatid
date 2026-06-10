from __future__ import annotations

from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from services.semantic_layer.lib.context import build_runtime_context
from services.semantic_layer.lib.storage import SemanticLayerRepository


app = FastAPI(title="Semantic Layer Admin API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://127.0.0.1:8018",
        "http://localhost:8018",
    ],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


class SemanticTypePayload(BaseModel):
    name: str | None = Field(default=None)
    description: str = ""
    datatype: str = "string"
    entity_kind: str = "entity"
    parent_entity_id: str = ""
    semantic_role: str = ""
    aliases: list[str] = Field(default_factory=list)
    owners: list[str] = Field(default_factory=list)
    tags: list[str] = Field(default_factory=list)
    documentation: str = ""
    status: str = "draft"


class RelationshipPayload(BaseModel):
    source_id: str | None = None
    target_id: str
    relation_type: str


class ExecutionSourcePayload(BaseModel):
    name: str | None = Field(default=None)
    provider: str = ""
    source_type: str = "api"
    description: str = ""
    status: str = "draft"
    config: dict[str, Any] = Field(default_factory=dict)


class ReviewPayload(BaseModel):
    reviewer: str = "admin"


@app.get("/health/ready")
def ready() -> dict[str, str]:
    return {"status": "ready"}


@app.get("/context")
def context() -> dict[str, object]:
    return build_runtime_context()


@app.get("/api/overview")
def overview() -> dict[str, Any]:
    return SemanticLayerRepository().overview()


@app.get("/semantic/catalog")
def semantic_catalog() -> dict[str, object]:
    return SemanticLayerRepository().semantic_catalog()


@app.post("/semantic-types/seed")
def seed_semantic_types() -> dict[str, object]:
    return SemanticLayerRepository().seed_semantic_type_registry()


@app.get("/api/semantic-types")
def list_semantic_types(
    query: str = Query(default=""),
    status: str = Query(default=""),
) -> list[dict[str, Any]]:
    return SemanticLayerRepository().list_semantic_types(query=query, status=status)


@app.get("/api/execution-sources")
def list_execution_sources(
    query: str = Query(default=""),
    status: str = Query(default=""),
) -> list[dict[str, Any]]:
    return SemanticLayerRepository().list_execution_sources(query=query, status=status)


@app.post("/api/execution-sources")
def create_execution_source(payload: ExecutionSourcePayload) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().create_execution_source(_payload_dict(payload, exclude_unset=False))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/execution-sources/{source_id}")
def get_execution_source(source_id: str) -> dict[str, Any]:
    record = SemanticLayerRepository().get_execution_source(source_id)
    if record is None:
        raise HTTPException(status_code=404, detail="execution source not found")
    return record


@app.patch("/api/execution-sources/{source_id}")
def update_execution_source(source_id: str, payload: ExecutionSourcePayload) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().update_execution_source(
            source_id,
            _payload_dict(payload, exclude_unset=True),
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="execution source not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.delete("/api/execution-sources/{source_id}")
def delete_execution_source(source_id: str) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().delete_execution_source(source_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="execution source not found") from exc


@app.post("/api/semantic-types")
def create_semantic_type(payload: SemanticTypePayload) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().create_semantic_type(_payload_dict(payload, exclude_unset=False))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/semantic-types/{semantic_type_id}")
def get_semantic_type(semantic_type_id: str) -> dict[str, Any]:
    record = SemanticLayerRepository().get_semantic_type(semantic_type_id)
    if record is None:
        raise HTTPException(status_code=404, detail="semantic type not found")
    return record


@app.patch("/api/semantic-types/{semantic_type_id}")
def update_semantic_type(semantic_type_id: str, payload: SemanticTypePayload) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().update_semantic_type(
            semantic_type_id,
            _payload_dict(payload, exclude_unset=True),
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="semantic type not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.delete("/api/semantic-types/{semantic_type_id}")
def delete_semantic_type(semantic_type_id: str) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().delete_semantic_type(semantic_type_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="semantic type not found") from exc


@app.post("/api/semantic-types/{semantic_type_id}/relationships")
def add_relationship(semantic_type_id: str, payload: RelationshipPayload) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().add_semantic_relationship(
            semantic_type_id,
            _payload_dict(payload, exclude_unset=False),
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"semantic type not found: {exc}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.patch("/api/semantic-relationships/{relationship_id}")
def update_relationship(relationship_id: str, payload: RelationshipPayload) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().update_semantic_relationship(
            relationship_id,
            _payload_dict(payload, exclude_unset=False),
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"relationship or semantic type not found: {exc}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.delete("/api/semantic-relationships/{relationship_id}")
def delete_relationship(relationship_id: str) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().delete_semantic_relationship(relationship_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="relationship not found") from exc


@app.get("/api/proposals")
def list_proposals(
    status: str = Query(default=""),
    entity_type: str = Query(default=""),
) -> list[dict[str, Any]]:
    proposals = SemanticLayerRepository().list_proposals(status=status)
    if entity_type:
        proposals = [item for item in proposals if item.get("entity_type") == entity_type]
    return proposals


@app.get("/api/semantic-relationships")
def list_semantic_relationships(
    semantic_type_id: str = Query(default=""),
    status: str = Query(default=""),
) -> list[dict[str, Any]]:
    return SemanticLayerRepository().list_relationships(
        semantic_type_id=semantic_type_id,
        status=status,
    )


@app.post("/api/proposals/{proposal_id}/approve")
def approve_proposal(proposal_id: str, payload: ReviewPayload) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().review_proposal(
            proposal_id,
            "approved",
            reviewer=payload.reviewer,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="proposal not found") from exc


def _payload_dict(model: BaseModel, *, exclude_unset: bool) -> dict[str, Any]:
    if hasattr(model, "model_dump"):
        return model.model_dump(exclude_none=True, exclude_unset=exclude_unset)
    return model.dict(exclude_none=True, exclude_unset=exclude_unset)


@app.post("/api/proposals/{proposal_id}/reject")
def reject_proposal(proposal_id: str, payload: ReviewPayload) -> dict[str, Any]:
    try:
        return SemanticLayerRepository().review_proposal(
            proposal_id,
            "rejected",
            reviewer=payload.reviewer,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="proposal not found") from exc
