from __future__ import annotations

from fastapi import FastAPI, HTTPException

from services.semantic_platform.api.app.domain.service import (
    apply_proposal,
    catalog_metadata,
    find_capabilities,
    list_domains,
    list_proposals,
    list_sources,
    load_execution_contracts,
    load_catalog,
    plan_join,
    read_proposal,
    reject_proposal,
    resolve,
    runtime_context,
    sources_summary,
)
from services.semantic_platform.planner.intent_parser import parse_intent
from services.semantic_platform.planner.execution_planner import plan_execution


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
    runtime = catalog.get("runtime", {})
    return {
        "fields": runtime.get("semantic_types", {}),
        "entities": runtime.get("entities", {}),
        "relationships": runtime.get("relations", {}),
        "capabilities": runtime.get("capabilities", {}),
        "vocabulary": catalog.get("mappings", {}).get("crosswalks", {}),
        "workflows": catalog.get("resources", {}).get("resources", {}),
    }


@app.get("/semantic/execution/contracts")
def read_execution_contracts() -> dict:
    return load_execution_contracts()


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


@app.get("/semantic/domains")
def read_domains() -> dict:
    return list_domains()


@app.get("/semantic/resolve")
def resolve_get(q: str, limit: int = 10) -> dict:
    return resolve(q, limit)


@app.post("/semantic/resolve")
def resolve_post(document: dict) -> dict:
    return resolve(str(document.get("query", "")), int(document.get("limit", 10)))


@app.post("/semantic/capabilities/find")
def find_capabilities_post(document: dict) -> dict:
    properties = document.get("properties", [])
    if not isinstance(properties, list):
        properties = []
    return find_capabilities(
        entity=document.get("entity"),
        properties=[str(value) for value in properties],
        limit=int(document.get("limit", 20)),
    )


@app.post("/semantic/join/plan")
def plan_join_post(document: dict) -> dict:
    return plan_join(str(document.get("from_entity", "")), str(document.get("to_entity", "")))


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


@app.post("/planner/intent")
def parse_intent_post(document: dict) -> dict:
    query = str(document.get("query", ""))
    context = runtime_context(query, int(document.get("limit", 8)))["runtime_context"]
    manual_intent = document.get("manual_intent")
    return {
        "query": query,
        "semantic_intent": parse_intent(
            query,
            context,
            manual_intent if isinstance(manual_intent, dict) else None,
        ),
    }


@app.post("/runtime/context")
def runtime_context_post(document: dict) -> dict:
    return runtime_context(str(document.get("query", "")), int(document.get("limit", 8)))
