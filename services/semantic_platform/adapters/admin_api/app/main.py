from __future__ import annotations

import json

from fastapi import FastAPI, File, Form, HTTPException, Response, UploadFile

from services.semantic_platform.adapters.admin_api.app.gateway import (
    apply_proposal,
    catalog_item_delete_plan,
    catalog_metadata,
    create_catalog_version,
    delete_catalog_item,
    delete_secret,
    delete_source,
    delete_source_with_mode,
    embed_capability_documents,
    export_catalog_version,
    list_capability_documents,
    list_catalog_versions,
    list_evidence_snapshots,
    list_execution_graphs,
    list_ingestion_runs,
    list_proposals,
    list_planner_feedback,
    list_secrets,
    list_sources,
    load_execution_contracts,
    load_catalog,
    load_catalog_section,
    parse_bool_field,
    parse_list_field,
    read_proposal,
    read_catalog_version,
    read_catalog_version_diff,
    read_ingestion_run,
    list_endpoint_checks,
    record_endpoint_check,
    record_planner_feedback,
    reject_proposal,
    rebuild_capability_documents,
    retrieve_capabilities,
    restore_catalog_version,
    runtime_context,
    sources_summary,
    source_delete_plan,
    start_source_ingestion,
    upsert_execution_graph,
    upsert_secret,
    update_source,
    update_catalog_item,
    update_proposal_item,
    upload_source,
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


@app.patch("/catalog/sections/{section}/{item_id}")
def update_dashboard_catalog_item(section: str, item_id: str, document: dict) -> dict:
    try:
        return update_catalog_item(section, item_id, document)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"catalog section is not editable: {section}") from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail="catalog item not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/catalog/sections/{section}/{item_id}/delete-plan")
def read_dashboard_catalog_item_delete_plan(section: str, item_id: str) -> dict:
    try:
        return catalog_item_delete_plan(section, item_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"catalog section is not editable: {section}") from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail="catalog item not found") from exc


@app.post("/catalog/sections/{section}/{item_id}/delete")
def delete_dashboard_catalog_item(section: str, item_id: str, document: dict | None = None) -> dict:
    try:
        return delete_catalog_item(section, item_id, document)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"catalog section is not editable: {section}") from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail="catalog item not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


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


@app.get("/ingestion/runs")
def read_ingestion_runs(limit: int = 100, source_id: str | None = None) -> dict:
    return list_ingestion_runs(limit=limit, source_id=source_id)


@app.get("/ingestion/runs/{run_id}")
def read_ingestion_run_get(run_id: str) -> dict:
    try:
        return read_ingestion_run(run_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail="ingestion run not found") from exc


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


@app.get("/catalog/versions")
def read_catalog_versions(limit: int = 100, offset: int = 0) -> dict:
    return list_catalog_versions(limit=limit, offset=offset)


@app.post("/catalog/versions")
def create_catalog_version_snapshot(document: dict | None = None) -> dict:
    return create_catalog_version(document)


@app.get("/catalog/versions/{version_id}")
def read_catalog_version_detail(version_id: str) -> dict:
    try:
        return read_catalog_version(version_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail="catalog version not found") from exc


@app.get("/catalog/versions/{version_id}/export")
def export_catalog_version_detail(version_id: str) -> Response:
    try:
        payload = export_catalog_version(version_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail="catalog version not found") from exc
    filename = f"{version_id}.catalog-snapshot.json"
    return Response(
        content=json.dumps(payload, ensure_ascii=False, default=str, indent=2),
        media_type="application/json",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/catalog/versions/{version_id}/diff")
def read_catalog_version_diff_detail(version_id: str, base_version_id: str | None = None) -> dict:
    try:
        return read_catalog_version_diff(version_id, base_version_id=base_version_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail="catalog version not found") from exc


@app.post("/catalog/versions/{version_id}/restore")
def restore_catalog_version_detail(version_id: str) -> dict:
    try:
        return restore_catalog_version(version_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail="catalog version not found") from exc


@app.get("/sources")
def read_sources() -> dict:
    return list_sources()


@app.patch("/sources/{source_id}")
def update_source_patch(source_id: str, document: dict) -> dict:
    try:
        return update_source(source_id, document)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail="source not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.delete("/sources/{source_id}")
def delete_source_delete(source_id: str) -> dict:
    try:
        return delete_source(source_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail="source not found") from exc


@app.get("/sources/{source_id}/delete-plan")
def read_source_delete_plan(source_id: str) -> dict:
    try:
        return source_delete_plan(source_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail="source not found") from exc


@app.post("/sources/{source_id}/ingest")
def ingest_source_post(source_id: str, document: dict | None = None) -> dict:
    try:
        return start_source_ingestion(source_id, document)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/sources/{source_id}/delete")
def delete_source_post(source_id: str, document: dict) -> dict:
    try:
        return delete_source_with_mode(source_id, document)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail="source not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/secrets")
def read_secrets() -> dict:
    return list_secrets()


@app.post("/secrets")
def upsert_secret_post(document: dict) -> dict:
    try:
        return {"secret": upsert_secret(document)}
    except FileExistsError as exc:
        raise HTTPException(status_code=409, detail=f"secret key already exists: {exc}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.delete("/secrets/{secret_id}")
def delete_secret_delete(secret_id: str) -> dict:
    try:
        return delete_secret(secret_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail="secret not found") from exc


@app.post("/sources/upload")
async def upload_source_post(
    file: UploadFile = File(...),
    provider: str | None = Form(None),
    provider_name_ko: str | None = Form(None),
    title: str | None = Form(None),
    source_id: str | None = Form(None),
    auth_secret_refs: str | None = Form(None),
    auth_parameter_names: str | None = Form(None),
    uploaded_by: str | None = Form(None),
    allow_update: str | None = Form(None),
) -> dict:
    try:
        content = await file.read()
        return upload_source(
            file_name=file.filename or "source_document",
            content=content,
            provider=provider,
            provider_name_ko=provider_name_ko,
            title=title,
            source_id=source_id,
            content_type=file.content_type,
            auth_secret_refs=parse_list_field(auth_secret_refs),
            auth_parameter_names=parse_list_field(auth_parameter_names),
            uploaded_by=uploaded_by,
            allow_update=parse_bool_field(allow_update),
        )
    except FileExistsError as exc:
        raise HTTPException(status_code=409, detail=f"source key already exists: {exc}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/sources/summary")
def read_sources_summary() -> dict:
    return sources_summary()


@app.get("/sources/evidence")
def read_source_evidence_snapshots(source_document_id: str | None = None, limit: int = 100) -> dict:
    return list_evidence_snapshots(source_document_id=source_document_id, limit=limit)


@app.get("/proposals")
def read_proposals(
    status: str | None = None,
    limit: int = 100,
    offset: int = 0,
    include_payload: bool = False,
) -> dict:
    return list_proposals(
        status=status,
        limit=limit,
        offset=offset,
        include_payload=include_payload,
    )


@app.get("/proposals/{proposal_id}")
def read_proposal_detail(proposal_id: str) -> dict:
    try:
        return read_proposal(proposal_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail="proposal not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.patch("/proposals/{proposal_id}/items/{item_id}")
def update_proposal_item_patch(proposal_id: str, item_id: str, document: dict) -> dict:
    try:
        return update_proposal_item(proposal_id, item_id, document)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail="proposal item not found") from exc
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
