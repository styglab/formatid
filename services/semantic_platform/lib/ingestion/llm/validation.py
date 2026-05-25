from __future__ import annotations

from typing import Any


def validate_llm_analysis(analysis: dict[str, Any]) -> None:
    if not analysis:
        return
    required_lists = (
        "resources",
        "operations",
        "semantic_types",
        "entities",
        "entity_identifiers",
        "capabilities",
        "capability_entity_links",
        "capability_dependencies",
        "operation_contracts",
        "operation_variants",
        "field_mappings",
        "semantic_join_rules",
        "planning_examples",
        "capability_implementations",
    )
    for key in required_lists:
        if key in analysis and not isinstance(analysis.get(key), list):
            raise ValueError(f"llm analysis field must be a list: {key}")
    resources = {str(item.get("id") or "") for item in _list(analysis.get("resources"))}
    operations = {str(item.get("operation_id") or "") for item in _list(analysis.get("operations"))}
    semantic_types = {str(item.get("id") or "") for item in _list(analysis.get("semantic_types"))}
    entities = {str(item.get("id") or "") for item in _list(analysis.get("entities"))}
    capabilities = {str(item.get("id") or "") for item in _list(analysis.get("capabilities"))}
    resources.discard("")
    operations.discard("")
    semantic_types.discard("")
    entities.discard("")
    capabilities.discard("")
    for item in _list(analysis.get("entity_identifiers")):
        entity_id = _require(item, "entity_id", "entity_identifier")
        semantic_type_id = _require(item, "semantic_type_id", "entity_identifier")
        if entity_id not in entities:
            raise ValueError(f"entity identifier references unknown entity: {entity_id}")
        if semantic_type_id not in semantic_types:
            raise ValueError(f"entity identifier references unknown semantic type: {semantic_type_id}")
    for item in _list(analysis.get("capability_entity_links")):
        capability_id = _require(item, "capability_id", "capability_entity_link")
        entity_id = _require(item, "entity_id", "capability_entity_link")
        if capability_id not in capabilities:
            raise ValueError(f"capability entity link references unknown capability: {capability_id}")
        if entity_id not in entities:
            raise ValueError(f"capability entity link references unknown entity: {entity_id}")
        semantic_type_id = str(item.get("semantic_type_id") or "")
        if semantic_type_id and semantic_type_id not in semantic_types:
            raise ValueError(f"capability entity link references unknown semantic type: {semantic_type_id}")
    for item in _list(analysis.get("capability_dependencies")):
        capability_id = _require(item, "capability_id", "capability_dependency")
        depends_on = _require(item, "depends_on_capability_id", "capability_dependency")
        if capability_id not in capabilities:
            raise ValueError(f"capability dependency references unknown capability: {capability_id}")
        if depends_on not in capabilities:
            raise ValueError(f"capability dependency references unknown dependency: {depends_on}")
    for item in _list(analysis.get("semantic_join_rules")):
        from_type = _require(item, "from_semantic_type_id", "semantic_join_rule")
        to_type = _require(item, "to_semantic_type_id", "semantic_join_rule")
        if from_type not in semantic_types:
            raise ValueError(f"join rule references unknown semantic type: {from_type}")
        if to_type not in semantic_types:
            raise ValueError(f"join rule references unknown semantic type: {to_type}")
        for key in ("from_entity_id", "to_entity_id"):
            entity_id = str(item.get(key) or "")
            if entity_id and entity_id not in entities:
                raise ValueError(f"join rule references unknown entity: {entity_id}")
    for item in _list(analysis.get("planning_examples")):
        _require(item, "id", "planning_example")
        _require(item, "question", "planning_example")
        for capability_id in _list_values(item.get("expected_capability_ids")):
            if str(capability_id) not in capabilities:
                raise ValueError(f"planning example references unknown capability: {capability_id}")
    for item in _list(analysis.get("operations")):
        _require(item, "operation_id", "operation")
        resource_id = str(item.get("resource_id") or "")
        if resource_id and resource_id not in resources:
            raise ValueError(f"operation references unknown resource: {item.get('operation_id')} -> {resource_id}")
    for item in _list(analysis.get("operation_contracts")):
        operation_id = _require(item, "operation_id", "operation_contract")
        if operation_id not in operations:
            raise ValueError(f"contract references unknown operation: {operation_id}")
        capability_id = str(item.get("capability_id") or item.get("capability") or "")
        if capability_id and capability_id not in capabilities:
            raise ValueError(f"contract references unknown capability: {operation_id} -> {capability_id}")
        validate_operation_contract_runtime_schema(item)
    for item in _list(analysis.get("operation_variants")):
        operation_id = _require(item, "operation_id", "operation_variant")
        capability_id = _require(item, "capability_id", "operation_variant")
        _require(item, "variant_id", "operation_variant")
        if operation_id not in operations:
            raise ValueError(f"variant references unknown operation: {operation_id}")
        if capability_id not in capabilities:
            raise ValueError(f"variant references unknown capability: {capability_id}")
    for item in _list(analysis.get("capability_implementations")):
        operation_id = _require(item, "operation_id", "capability_implementation")
        capability_id = _require(item, "capability_id", "capability_implementation")
        if operation_id not in operations:
            raise ValueError(f"implementation references unknown operation: {operation_id}")
        if capability_id not in capabilities:
            raise ValueError(f"implementation references unknown capability: {capability_id}")
    for item in _list(analysis.get("field_mappings")):
        operation_id = _require(item, "operation_id", "field_mapping")
        semantic_type_id = _require(item, "semantic_type_id", "field_mapping")
        if operation_id not in operations:
            raise ValueError(f"field mapping references unknown operation: {operation_id}")
        if semantic_type_id not in semantic_types:
            raise ValueError(f"field mapping references unknown semantic type: {semantic_type_id}")
    for item in _list(analysis.get("capabilities")):
        capability_id = _require(item, "id", "capability")
        for semantic_type_id in [*_list_values(item.get("inputs")), *_list_values(item.get("outputs"))]:
            if str(semantic_type_id) not in semantic_types:
                raise ValueError(f"capability references unknown semantic type: {capability_id} -> {semantic_type_id}")


def validate_operation_contract_runtime_schema(contract: dict[str, Any]) -> None:
    operation_id = str(contract.get("operation_id") or "")
    response = contract.get("response") if isinstance(contract.get("response"), dict) else {}
    items_path = response.get("items_path")
    if not contract_path_list(items_path):
        raise ValueError(f"operation_contract response.items_path required: {operation_id}")
    fields = response.get("fields")
    if not isinstance(fields, dict):
        raise ValueError(f"operation_contract response.fields required: {operation_id}")
    for field_name, field_contract in fields.items():
        if not isinstance(field_contract, dict) or not field_contract.get("semantic_type"):
            raise ValueError(f"operation_contract response field missing semantic_type: {operation_id}.{field_name}")
    auth = contract.get("auth") if isinstance(contract.get("auth"), dict) else {}
    if auth:
        env_names = auth.get("env_names")
        if env_names is not None and not isinstance(env_names, list):
            raise ValueError(f"operation_contract auth.env_names must be a list: {operation_id}")
        if not str(auth.get("parameter") or ""):
            raise ValueError(f"operation_contract auth.parameter required when auth is declared: {operation_id}")
    for key in ("success", "error"):
        condition = response.get(key)
        if condition is not None and not isinstance(condition, dict):
            raise ValueError(f"operation_contract response.{key} must be an object: {operation_id}")
    success = response.get("success") if isinstance(response.get("success"), dict) else {}
    if success and not str(success.get("path") or ""):
        raise ValueError(f"operation_contract response.success.path required: {operation_id}")
    error = response.get("error") if isinstance(response.get("error"), dict) else {}
    if error and not str(error.get("code_path") or ""):
        raise ValueError(f"operation_contract response.error.code_path required: {operation_id}")


def contract_path_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if str(item or "").strip()]
    if str(value or "").strip():
        return [str(value)]
    return []


def _require(item: dict[str, Any], key: str, context: str) -> str:
    value = str(item.get(key) or "")
    if not value:
        raise ValueError(f"{context} missing required field: {key}")
    return value


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _list_values(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if value in (None, ""):
        return []
    return [value]
