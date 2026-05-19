from __future__ import annotations

from typing import Any

from services.semantic_platform.planner.service import planner_context
from services.semantic_platform.storage import SemanticCatalogRepository


def runtime_context(query: str, limit: int = 12) -> dict[str, Any]:
    repository = SemanticCatalogRepository()
    catalog = repository.catalog()
    retrieved = repository.retrieve_capabilities(query, limit=limit)
    context = planner_context(catalog, limit=limit, retrieved=retrieved)
    return {
        "query": query,
        "runtime_context": context,
    }
