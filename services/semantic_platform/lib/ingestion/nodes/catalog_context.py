from __future__ import annotations

from typing import Any

from services.semantic_platform.lib.ingestion.state import SourceGraphState
from services.semantic_platform.lib.storage import SemanticCatalogRepository


def load_catalog_context(state: SourceGraphState) -> SourceGraphState:
    repo = SemanticCatalogRepository()
    catalog = repo.catalog()
    proposals = repo.proposals().get("proposals", [])[:20]
    return {
        **state,
        "catalog_context": {
            "semantic_types": _catalog_values(catalog.get("semantic_types", {}), limit=120),
            "entities": _catalog_values(catalog.get("entities", {}), limit=120),
            "entity_identifiers": _catalog_values(catalog.get("entity_identifiers", {}), limit=120),
            "semantic_join_rules": _catalog_values(catalog.get("semantic_join_rules", {}), limit=120),
            "capabilities": _catalog_values(catalog.get("capabilities", {}), limit=120),
            "capability_entity_links": _catalog_values(catalog.get("capability_entity_links", {}), limit=120),
            "capability_dependencies": _catalog_values(catalog.get("capability_dependencies", {}), limit=120),
            "planning_examples": _catalog_values(catalog.get("planning_examples", {}), limit=120),
            "operation_contracts_summary": _contract_summary(catalog.get("operation_contracts", {})),
            "operation_variants_summary": _variant_summary(catalog.get("operation_variants", {})),
            "recent_proposals": [
                {
                    "id": proposal.get("id"),
                    "kind": proposal.get("kind"),
                    "status": proposal.get("status"),
                    "source_document_id": proposal.get("source_document_id"),
                    "created_at": proposal.get("created_at"),
                }
                for proposal in proposals
            ],
            "naming_policy": {
                "capability_ids": "provider-neutral action names such as search_contracts, not provider-prefixed ids",
                "operation_ids": "provider/resource operation identifiers may reflect physical endpoints",
                "variants": "provider control values and endpoint-specific meanings belong in operation_variant rows",
            },
        },
    }


def _catalog_values(values: Any, limit: int) -> list[dict[str, Any]]:
    if isinstance(values, dict):
        items = values.values()
    elif isinstance(values, list):
        items = values
    else:
        items = []
    return [dict(item) for item in items if isinstance(item, dict)][:limit]


def _contract_summary(values: Any) -> list[dict[str, Any]]:
    return [
        {
            "operation_id": item.get("operation_id"),
            "capability_id": item.get("capability_id"),
            "provider": item.get("provider"),
            "resource_id": item.get("resource_id"),
            "method": item.get("method"),
            "path": item.get("path"),
            "request_semantic_types": _semantic_types_from_contract(item.get("request")),
            "response_semantic_types": _semantic_types_from_contract(item.get("response")),
        }
        for item in _catalog_values(values, limit=200)
    ]


def _variant_summary(values: Any) -> list[dict[str, Any]]:
    return [
        {
            "variant_id": item.get("variant_id"),
            "operation_id": item.get("operation_id"),
            "capability_id": item.get("capability_id"),
            "fixed_semantic_arguments": item.get("fixed_semantic_arguments") or {},
            "fixed_raw_arguments": item.get("fixed_raw_arguments") or {},
            "verification_status": (item.get("verification") or {}).get("status")
            if isinstance(item.get("verification"), dict)
            else None,
        }
        for item in _catalog_values(values, limit=240)
    ]


def _semantic_types_from_contract(contract_part: Any) -> list[str]:
    found: list[str] = []

    def walk(value: Any) -> None:
        if isinstance(value, dict):
            semantic_type = value.get("semantic_type")
            if semantic_type and str(semantic_type) not in found:
                found.append(str(semantic_type))
            for child in value.values():
                walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)

    walk(contract_part)
    return found
