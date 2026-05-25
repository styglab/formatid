from __future__ import annotations

from fastapi import FastAPI, HTTPException

from services.semantic_platform.adapters.api.app.gateway import (
    apply_proposal,
    catalog_metadata,
    embed_capability_documents,
    list_capability_documents,
    list_evidence_snapshots,
    list_execution_graphs,
    list_proposals,
    list_planner_feedback,
    list_sources,
    load_execution_contracts,
    load_catalog,
    load_catalog_section,
    read_proposal,
    list_endpoint_checks,
    record_endpoint_check,
    record_planner_feedback,
    reject_proposal,
    rebuild_capability_documents,
    retrieve_capabilities,
    runtime_context,
    sources_summary,
    upsert_execution_graph,
)
from services.semantic_platform.lib.planner import plan_execution


app = FastAPI(title="Semantic Platform API", version="0.1.0")


@app.get("/health/ready")
def ready() -> dict[str, str]:
    return {"status": "ready"}


@app.get("/semantic/catalog")
def read_catalog() -> dict:
    return load_catalog()


@app.get("/catalog")
def read_dashboard_catalog() -> dict:
    catalog = load_catalog()
    return {
        "semantic_types": catalog.get("semantic_types", {}),
        "entities": catalog.get("entities", {}),
        "entity_identifiers": catalog.get("entity_identifiers", {}),
        "capabilities": catalog.get("capabilities", {}),
        "capability_entity_links": catalog.get("capability_entity_links", {}),
        "capability_dependencies": catalog.get("capability_dependencies", {}),
        "planning_examples": catalog.get("planning_examples", {}),
        "resources": catalog.get("resources", {}),
        "operations": catalog.get("operations", {}),
        "operation_contracts": catalog.get("operation_contracts", {}),
        "operation_variants": catalog.get("operation_variants", {}),
        "field_mappings": catalog.get("field_mappings", {}),
        "semantic_join_rules": catalog.get("semantic_join_rules", {}),
        "capability_implementations": catalog.get("capability_implementations", []),
        "capability_documents": catalog.get("capability_documents", {}),
    }


@app.get("/catalog/sections/{section}")
def read_dashboard_catalog_section(section: str, limit: int = 100, offset: int = 0, q: str | None = None) -> dict:
    try:
        return load_catalog_section(section=section, limit=limit, offset=offset, q=q)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"unknown catalog section: {section}") from exc


@app.get("/semantic/execution/contracts")
def read_execution_contracts() -> dict:
    return load_execution_contracts()


@app.get("/semantic/capability-documents")
def read_capability_documents(limit: int = 100) -> dict:
    return list_capability_documents(limit=limit)


@app.post("/semantic/capability-documents/rebuild")
def rebuild_capability_documents_post() -> dict:
    return rebuild_capability_documents()


@app.post("/semantic/capability-documents/embed")
def embed_capability_documents_post(document: dict | None = None) -> dict:
    payload = document or {}
    return embed_capability_documents(
        limit=int(payload.get("limit", 100)),
        force=bool(payload.get("force", False)),
        capability_ids=payload.get("capability_ids") if isinstance(payload.get("capability_ids"), list) else None,
        document_ids=payload.get("document_ids") if isinstance(payload.get("document_ids"), list) else None,
    )


@app.post("/semantic/capabilities/retrieve")
def retrieve_capabilities_post(document: dict) -> dict:
    return retrieve_capabilities(str(document.get("query", "")), int(document.get("limit", 10)))


@app.get("/semantic/execution/checks")
def read_endpoint_checks(
    operation_id: str | None = None,
    variant_id: str | None = None,
    limit: int = 100,
) -> dict:
    return list_endpoint_checks(operation_id=operation_id, variant_id=variant_id, limit=limit)


@app.post("/semantic/execution/checks")
def record_endpoint_check_post(document: dict) -> dict:
    try:
        return record_endpoint_check(document)
    except KeyError as exc:
        raise HTTPException(status_code=400, detail=f"missing field: {exc}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/planner/execution-graphs")
def read_execution_graphs(limit: int = 100) -> dict:
    return list_execution_graphs(limit=limit)


@app.post("/planner/execution-graphs")
def upsert_execution_graph_post(document: dict) -> dict:
    try:
        return upsert_execution_graph(document)
    except KeyError as exc:
        raise HTTPException(status_code=400, detail=f"missing field: {exc}") from exc


@app.get("/semantic/governance/feedback")
def read_planner_feedback(limit: int = 100) -> dict:
    return list_planner_feedback(limit=limit)


@app.post("/semantic/governance/feedback")
def record_planner_feedback_post(document: dict) -> dict:
    try:
        return record_planner_feedback(document)
    except KeyError as exc:
        raise HTTPException(status_code=400, detail=f"missing field: {exc}") from exc


@app.get("/semantic/meta")
def read_meta() -> dict:
    return catalog_metadata()


@app.get("/catalog/meta")
def read_dashboard_meta() -> dict:
    return catalog_metadata()


@app.get("/sources")
def read_sources() -> dict:
    return list_sources()


@app.get("/sources/summary")
def read_sources_summary() -> dict:
    return sources_summary()


@app.get("/sources/evidence")
def read_source_evidence_snapshots(source_document_id: str | None = None, limit: int = 100) -> dict:
    return list_evidence_snapshots(source_document_id=source_document_id, limit=limit)


@app.get("/proposals")
def read_proposals() -> dict:
    return list_proposals()


@app.get("/proposals/{proposal_id}")
def read_proposal_detail(proposal_id: str) -> dict:
    try:
        return read_proposal(proposal_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail="proposal not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/proposals/{proposal_id}/apply")
def apply_proposal_post(proposal_id: str) -> dict:
    try:
        return apply_proposal(proposal_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail="proposal not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/proposals/{proposal_id}/reject")
def reject_proposal_post(proposal_id: str) -> dict:
    try:
        return reject_proposal(proposal_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail="proposal not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/planner/plan")
def plan_query_post(document: dict) -> dict:
    manual_plan = document.get("manual_plan")
    return plan_execution(
        str(document.get("query", "")),
        int(document.get("limit", 12)),
        manual_plan if isinstance(manual_plan, dict) else None,
    )


@app.post("/planner/execution-plan")
def plan_execution_post(document: dict) -> dict:
    manual_plan = document.get("manual_plan")
    return plan_execution(
        str(document.get("query", "")),
        int(document.get("limit", 12)),
        manual_plan if isinstance(manual_plan, dict) else None,
    )


@app.post("/runtime/context")
def runtime_context_post(document: dict) -> dict:
    return runtime_context(str(document.get("query", "")), int(document.get("limit", 8)))
