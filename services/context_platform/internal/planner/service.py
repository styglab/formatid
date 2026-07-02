from __future__ import annotations

import re
from typing import Any

from services.context_platform.internal.storage import ContextPlatformRepository


def build_not_found_plan(reason: str = "capability_not_found") -> dict[str, Any]:
    return {
        "plan_id": None,
        "planner": {"status": "not_found", "reason": reason},
        "selected_capability_id": None,
        "selected_source_operation_id": None,
        "canonical_inputs": {},
        "parameter_bindings": [],
        "expected_outputs": [],
        "confidence": 0,
        "requires_confirmation": False,
        "validation": {"valid": False, "errors": [{"code": reason}]},
        "execution_graph": {"type": "dag", "status": "not_found", "nodes": []},
        "errors": [{"code": reason}],
    }


def build_runtime_context_payload(repository: ContextPlatformRepository | None = None) -> dict[str, Any]:
    repo = repository or ContextPlatformRepository()
    overview = repo.overview()
    return {
        "service": "context_platform",
        "platform": "context_platform",
        "planner_reads": "approved_context_only",
        "modules": [
            "Meaning Graph",
            "Representation Model",
            "Source Graph",
            "Resolution Graph",
            "Capability Graph",
            "Execution Graph",
        ],
        "counts": overview.get("counts", {}),
    }


def load_planner_context(repository: ContextPlatformRepository | None = None) -> dict[str, Any]:
    repo = repository or ContextPlatformRepository()
    return {
        "capabilities": {item["id"]: item for item in repo.list_capabilities(status="approved")},
        "source_operations": {item["id"]: item for item in repo.list_source_operations(status="approved")},
        "bindings": repo.list_bindings(status="approved"),
        "capability_operations": repo.list_capability_operations(),
    }


def build_execution_plan(payload: dict[str, Any], repository: ContextPlatformRepository | None = None) -> dict[str, Any]:
    return create_plan(payload, repository=repository)


def create_plan(payload: dict[str, Any], repository: ContextPlatformRepository | None = None) -> dict[str, Any]:
    repo = repository or ContextPlatformRepository()
    query = _extract_query_text(payload)
    if not query:
        return build_not_found_plan("empty_query")

    capabilities = repo.list_capabilities(status="approved")
    capability_operations = repo.list_capability_operations()
    operations_by_id = {item["id"]: item for item in repo.list_source_operations(status="approved")}

    ranked = sorted(
        (
            {"capability": capability, "score": _score_text_match(query, capability)}
            for capability in capabilities
        ),
        key=lambda item: item["score"],
        reverse=True,
    )
    ranked = [item for item in ranked if item["score"] > 0]
    if not ranked:
        return build_not_found_plan()

    selected_capability = ranked[0]["capability"]
    operation_link = _select_capability_operation(selected_capability["id"], capability_operations, operations_by_id)
    if operation_link is None:
        return build_not_found_plan("source_operation_not_found")

    source_operation = operations_by_id[str(operation_link["source_operation_id"])]
    operation_bindings = repo.list_bindings(source_operation_id=source_operation["id"], status="approved")
    parameter_bindings = [
        {
            "binding_id": binding["id"],
            "source_parameter_id": binding.get("source_parameter_id"),
            "required_concept_id": binding.get("required_concept_id"),
            "representation_id": binding.get("representation_id"),
            "representation_schema_id": binding.get("representation_schema_id"),
            "canonical_class_slot_id": binding.get("canonical_class_slot_id"),
            "transform_spec": binding.get("transform_spec") or {},
            "normalization_rule": binding.get("normalization_rule") or {},
            "enum_mapping": binding.get("enum_mapping") or {},
        }
        for binding in operation_bindings
        if binding.get("direction") == "input"
    ]
    expected_outputs = [
        {
            "binding_id": binding["id"],
            "source_field_id": binding.get("source_field_id"),
            "concept_id": binding.get("required_concept_id"),
            "representation_id": binding.get("representation_id"),
            "representation_schema_id": binding.get("representation_schema_id"),
            "context_key": binding.get("context_key"),
            "canonical_class_slot_id": binding.get("canonical_class_slot_id"),
        }
        for binding in operation_bindings
        if binding.get("direction") in {"output", "output_context"}
    ]
    plan = {
        "selected_capability_id": selected_capability["id"],
        "selected_source_operation_id": source_operation["id"],
        "canonical_inputs": payload.get("canonical_inputs") if isinstance(payload.get("canonical_inputs"), dict) else {},
        "parameter_bindings": parameter_bindings,
        "expected_outputs": expected_outputs,
        "confidence": min(round(ranked[0]["score"] / 10, 2), 0.99),
        "requires_confirmation": not parameter_bindings,
        "validation": {},
        "request_payload": payload,
    }
    validation = validate_plan(plan, approved_operation_ids=set(operations_by_id))
    status = "validated" if validation["valid"] and not plan["requires_confirmation"] else "requires_confirmation"
    saved = repo.save_plan(
        {
            **plan,
            "status": status,
            "validation": validation,
            "plan_payload": {**plan, "validation": validation, "status": status},
        }
    )
    return {
        "plan_id": saved["id"],
        "planner": {"status": status, "mode": "capability_catalog_retrieval"},
        "selected_capability_id": selected_capability["id"],
        "selected_source_operation_id": source_operation["id"],
        "canonical_inputs": plan["canonical_inputs"],
        "parameter_bindings": parameter_bindings,
        "expected_outputs": expected_outputs,
        "confidence": plan["confidence"],
        "requires_confirmation": plan["requires_confirmation"],
        "validation": validation,
        "execution_graph": {
            "type": "dag",
            "status": status,
            "nodes": [
                {
                    "id": "step_1",
                    "capability_id": selected_capability["id"],
                    "source_operation_id": source_operation["id"],
                }
            ],
            "edges": [],
        },
        "errors": validation["errors"],
    }


def get_plan(plan_id: str, repository: ContextPlatformRepository | None = None) -> dict[str, Any] | None:
    repo = repository or ContextPlatformRepository()
    record = repo.get_plan(plan_id)
    if record is None:
        return None
    payload = record.get("plan_payload")
    return payload if isinstance(payload, dict) and payload else record


def execute_plan(payload: dict[str, Any], repository: ContextPlatformRepository | None = None) -> dict[str, Any]:
    repo = repository or ContextPlatformRepository()
    plan_id = str(payload.get("plan_id") or "")
    plan = repo.get_plan(plan_id) if plan_id else None
    if plan is None:
        return {"status": "error", "errors": [{"code": "plan_not_found"}]}
    validation = plan.get("validation_result") if isinstance(plan.get("validation_result"), dict) else {}
    if not validation.get("valid"):
        result = {"status": "rejected", "errors": [{"code": "plan_not_validated"}]}
        repo.create_execution(plan["id"], status="rejected", request_payload=payload, result_payload=result)
        return result
    result = {
        "status": "not_executed",
        "reason": "provider_execution_not_implemented",
        "plan_id": plan["id"],
        "selected_source_operation_id": plan.get("selected_source_operation_id"),
    }
    repo.create_execution(plan["id"], status="not_executed", request_payload=payload, result_payload=result)
    return result


def validate_plan(plan: dict[str, Any], approved_operation_ids: set[str] | None = None) -> dict[str, Any]:
    errors: list[dict[str, str]] = []
    operation_id = plan.get("selected_source_operation_id")
    if not plan.get("selected_capability_id"):
        errors.append({"code": "missing_selected_capability_id"})
    if not operation_id:
        errors.append({"code": "missing_selected_source_operation_id"})
    if approved_operation_ids is not None and operation_id and operation_id not in approved_operation_ids:
        errors.append({"code": "unapproved_source_operation_id", "source_operation_id": str(operation_id)})
    if not isinstance(plan.get("parameter_bindings"), list):
        errors.append({"code": "invalid_parameter_bindings"})
    if not isinstance(plan.get("expected_outputs"), list):
        errors.append({"code": "invalid_expected_outputs"})
    return {"valid": not errors, "errors": errors}


def _select_capability_operation(
    capability_id: str,
    capability_operations: list[dict[str, Any]],
    operations_by_id: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    candidates = [
        item
        for item in capability_operations
        if item.get("capability_id") == capability_id
        and item.get("source_operation_id") in operations_by_id
        and item.get("status") in {"approved", "published"}
    ]
    return sorted(candidates, key=lambda item: int(item.get("priority") or item.get("step_order") or 100))[0] if candidates else None


def _extract_query_text(payload: dict[str, Any]) -> str:
    for key in ("query", "question", "input_text", "text"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    user_input = payload.get("input")
    if isinstance(user_input, dict):
        return _extract_query_text(user_input)
    return ""


def _score_text_match(query: str, capability: dict[str, Any]) -> int:
    query_tokens = _tokenize(query)
    haystack = " ".join(
        str(value or "")
        for value in [
            capability.get("capability_key"),
            capability.get("name"),
            capability.get("description"),
            capability.get("intent_spec"),
        ]
    )
    overlap = query_tokens.intersection(_tokenize(haystack))
    score = len(overlap) * 3
    query_lower = query.lower()
    for key in ("capability_key", "name"):
        value = str(capability.get(key) or "").lower()
        if value and value in query_lower:
            score += 4
    return score


def _tokenize(value: str) -> set[str]:
    return {token for token in re.split(r"[^0-9A-Za-z_가-힣]+", value.lower()) if len(token) > 1}
