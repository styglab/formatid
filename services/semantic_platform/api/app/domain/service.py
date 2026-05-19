from __future__ import annotations

from typing import Any

from services.semantic_platform.runtime import runtime_context
from services.semantic_platform.storage import SemanticCatalogRepository


def repository() -> SemanticCatalogRepository:
    return SemanticCatalogRepository()


def load_catalog() -> dict[str, Any]:
    return repository().catalog()


def load_catalog_section(section: str, limit: int = 100, offset: int = 0, q: str | None = None) -> dict[str, Any]:
    return repository().catalog_section(section=section, limit=limit, offset=offset, q=q)


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


def list_proposals() -> dict[str, Any]:
    return repository().proposals()


def read_proposal(proposal_id: str) -> dict[str, Any]:
    return repository().proposal(proposal_id)


def apply_proposal(proposal_id: str) -> dict[str, Any]:
    repo = repository()
    result = repo.apply_proposal(proposal_id, reviewer="api")
    capability_ids = [
        str(item.get("target_id"))
        for item in result.get("applied", [])
        if isinstance(item, dict) and item.get("item_type") == "capability" and item.get("target_id")
    ]
    result["capability_documents"] = repo.rebuild_capability_documents(capability_ids=capability_ids)
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
