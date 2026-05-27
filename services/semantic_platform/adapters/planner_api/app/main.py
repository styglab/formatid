from __future__ import annotations

from fastapi import FastAPI, HTTPException

from services.semantic_platform.lib.context import runtime_context
from services.semantic_platform.lib.planner import plan_execution
from services.semantic_platform.lib.storage import SemanticCatalogRepository


app = FastAPI(title="Semantic Platform Planner API", version="0.1.0")


def repository() -> SemanticCatalogRepository:
    return SemanticCatalogRepository()


@app.get("/health/ready")
def ready() -> dict[str, str]:
    return {"status": "ready"}


@app.get("/semantic/catalog")
def read_catalog() -> dict:
    return repository().catalog()


@app.get("/semantic/execution/contracts")
def read_execution_contracts() -> dict:
    return repository().execution_contracts()


@app.post("/semantic/capabilities/retrieve")
def retrieve_capabilities_post(document: dict) -> dict:
    return repository().retrieve_capabilities(
        str(document.get("query", "")),
        limit=int(document.get("limit", 10)),
    )


@app.get("/semantic/execution/checks")
def read_endpoint_checks(
    operation_id: str | None = None,
    variant_id: str | None = None,
    limit: int = 100,
) -> dict:
    return repository().endpoint_checks(operation_id=operation_id, variant_id=variant_id, limit=limit)


@app.post("/semantic/execution/checks")
def record_endpoint_check_post(document: dict) -> dict:
    try:
        return repository().record_endpoint_check(document)
    except KeyError as exc:
        raise HTTPException(status_code=400, detail=f"missing field: {exc}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/semantic/planner/execution-plan")
def plan_execution_post(document: dict) -> dict:
    manual_plan = document.get("manual_plan")
    return plan_execution(
        str(document.get("query", "")),
        int(document.get("limit", 12)),
        manual_plan if isinstance(manual_plan, dict) else None,
    )


@app.post("/semantic/planner/runtime-context")
def runtime_context_post(document: dict) -> dict:
    return runtime_context(str(document.get("query", "")), int(document.get("limit", 8)))

