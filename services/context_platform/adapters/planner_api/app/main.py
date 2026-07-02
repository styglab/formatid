from __future__ import annotations

from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi import Query

from services.context_platform.internal.context import build_runtime_context
from services.context_platform.internal.planner import (
    create_plan,
    execute_plan as execute_plan_service,
    get_plan as get_plan_service,
    validate_plan,
)
from services.context_platform.internal.storage import ContextPlatformRepository


app = FastAPI(title="Context Platform Planner API")


@app.get("/health/ready")
def ready() -> dict[str, str]:
    return {"status": "ready"}


@app.get("/runtime-context")
def runtime_context() -> dict[str, Any]:
    return build_runtime_context()


@app.post("/planner/plan")
def planner_plan(payload: dict[str, Any]) -> dict[str, Any]:
    return create_plan(payload)


@app.post("/planner/execute")
def planner_execute(payload: dict[str, Any]) -> dict[str, Any]:
    return execute_plan_service(payload)


@app.get("/planner/plans/{plan_id}")
def planner_get_plan(plan_id: str) -> dict[str, Any]:
    plan = get_plan_service(plan_id)
    if plan is None:
        raise HTTPException(status_code=404, detail="plan not found")
    return plan


@app.post("/planner/validate")
def planner_validate(payload: dict[str, Any]) -> dict[str, Any]:
    plan = payload.get("plan") if isinstance(payload.get("plan"), dict) else payload
    approved_operation_ids = payload.get("approved_source_operation_ids")
    approved = {str(item) for item in approved_operation_ids} if isinstance(approved_operation_ids, list) else None
    return validate_plan(plan, approved_operation_ids=approved)


@app.get("/planner/capabilities")
def planner_capabilities(
    query: str = Query(default=""),
    status: str = Query(default="approved"),
) -> list[dict[str, Any]]:
    query = query if isinstance(query, str) else ""
    status = status if isinstance(status, str) else "approved"
    return ContextPlatformRepository().list_capabilities(query=query, status=status)


@app.get("/planner/capabilities/{capability_id}")
def planner_capability(capability_id: str) -> dict[str, Any]:
    capability = ContextPlatformRepository().get_capability(capability_id)
    if capability is None:
        raise HTTPException(status_code=404, detail="capability not found")
    return capability


@app.get("/planner/canonical-model")
def planner_canonical_model() -> dict[str, Any]:
    repo = ContextPlatformRepository()
    return {
        "meaning_scopes": repo.list_meaning_scopes(status="approved"),
        "concept_schemes": repo.list_concept_schemes(status="approved"),
        "concepts": repo.list_concepts(status="approved"),
        "value_domains": repo.list_value_domains(status="approved"),
        "value_domain_values": repo.list_value_domain_values(status="approved"),
        "object_types": repo.list_object_types(status="approved"),
        "property_types": repo.list_property_types(status="approved"),
        "link_types": repo.list_link_types(status="approved"),
        "canonical_representations": repo.list_canonical_representations(status="approved"),
        "representation_schemas": repo.list_representation_schemas(status="approved"),
        "canonical_types": repo.list_canonical_types(status="approved"),
        "canonical_enums": repo.list_canonical_enums(status="approved"),
        "canonical_slots": repo.list_canonical_slots(status="approved"),
        "canonical_classes": repo.list_canonical_classes(status="approved"),
        "canonical_class_slots": repo.list_canonical_class_slots(status="approved"),
        "canonical_class_slot_usages": repo.list_canonical_class_slot_usages(status="approved"),
    }


@app.get("/planner/meaning-graph")
def planner_meaning_graph() -> dict[str, Any]:
    repo = ContextPlatformRepository()
    return {
        "meaning_scopes": repo.list_meaning_scopes(status="approved"),
        "concept_schemes": repo.list_concept_schemes(status="approved"),
        "concepts": repo.list_concepts(status="approved"),
        "value_domains": repo.list_value_domains(status="approved"),
        "value_domain_values": repo.list_value_domain_values(status="approved"),
    }


@app.get("/planner/representations")
def planner_representations() -> dict[str, Any]:
    repo = ContextPlatformRepository()
    return {
        "object_types": repo.list_object_types(status="approved"),
        "property_types": repo.list_property_types(status="approved"),
        "link_types": repo.list_link_types(status="approved"),
        "canonical_representations": repo.list_canonical_representations(status="approved"),
        "representation_schemas": repo.list_representation_schemas(status="approved"),
    }


@app.get("/planner/operation-bindings")
def planner_operation_bindings(source_operation_id: str = Query(default="")) -> list[dict[str, Any]]:
    source_operation_id = source_operation_id if isinstance(source_operation_id, str) else ""
    return ContextPlatformRepository().list_bindings(source_operation_id=source_operation_id, status="approved")


@app.get("/planner/resolution-bindings")
def planner_resolution_bindings(source_operation_id: str = Query(default="")) -> dict[str, Any]:
    source_operation_id = source_operation_id if isinstance(source_operation_id, str) else ""
    repo = ContextPlatformRepository()
    return {
        "field_bindings": repo.list_field_bindings(source_operation_id=source_operation_id, status="approved"),
        "context_bindings": repo.list_context_bindings(source_operation_id=source_operation_id, status="approved"),
        "parameter_bindings": repo.list_parameter_bindings(source_operation_id=source_operation_id, status="approved"),
    }
