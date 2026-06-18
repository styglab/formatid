from __future__ import annotations

from typing import Any


def build_capability_bindings(
    capabilities: list[dict[str, Any]],
    operations: list[dict[str, Any]],
    variants: list[dict[str, Any]],
    mappings: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    variants_by_operation: dict[str, list[dict[str, Any]]] = {}
    for variant in variants:
        variants_by_operation.setdefault(str(variant.get("operation_id") or ""), []).append(variant)

    mappings_by_operation: dict[str, list[dict[str, Any]]] = {}
    for mapping in mappings:
        mappings_by_operation.setdefault(str(mapping.get("operation_id") or ""), []).append(mapping)

    bindings: list[dict[str, Any]] = []
    for capability in capabilities:
        capability_outputs = {str(item) for item in capability.get("output_semantic_types") or [] if item}
        capability_inputs = {str(item) for item in capability.get("input_semantic_types") or [] if item}
        best_operation: dict[str, Any] | None = None
        best_overlap = -1
        for operation in operations:
            operation_mappings = mappings_by_operation.get(str(operation.get("id") or ""), [])
            operation_semantics = {str(item.get("semantic_type_id") or "") for item in operation_mappings if item.get("semantic_type_id")}
            overlap = len(capability_outputs.intersection(operation_semantics))
            overlap += len(capability_inputs.intersection(operation_semantics))
            if overlap > best_overlap:
                best_overlap = overlap
                best_operation = operation
        if best_operation is None:
            continue

        binding_variants = variants_by_operation.get(str(best_operation.get("id") or ""), [])
        denominator = max(len(capability_outputs) + len(capability_inputs), 1)
        coverage = round(best_overlap / denominator, 2) if best_overlap >= 0 else 0.0
        bindings.append(
            {
                "id": f"binding_{capability['id']}_{best_operation['id']}",
                "capability_id": capability["id"],
                "capability_key": capability.get("capability_key") or capability["id"],
                "capability_name": capability.get("name") or capability.get("capability_key") or capability["id"],
                "operation_id": best_operation["id"],
                "operation_name": best_operation.get("name") or best_operation["id"],
                "variant_ids": [variant["id"] for variant in binding_variants],
                "variant_count": len(binding_variants),
                "semantic_coverage": coverage,
                "status": "candidate" if coverage < 1 else "ready",
                "evidence": "derived from approved mapping semantic overlap",
            }
        )

    return sorted(bindings, key=lambda item: (item["status"], item["capability_name"]))


def build_execution_contract_catalog(repository: Any) -> dict[str, Any]:
    capabilities = repository.list_capabilities(status="approved")
    operations = repository.list_execution_operations(status="approved")
    variants = repository.list_operation_variants(status="approved")
    mappings = repository.list_field_mappings(status="approved")
    resources = repository.list_execution_sources(status="approved")
    bindings = build_capability_bindings(capabilities, operations, variants, mappings)

    bindings_by_capability: dict[str, list[dict[str, Any]]] = {}
    for binding in bindings:
        bindings_by_capability.setdefault(str(binding.get("capability_id") or ""), []).append(binding)

    mappings_by_operation: dict[str, list[dict[str, Any]]] = {}
    for mapping in mappings:
        mappings_by_operation.setdefault(str(mapping.get("operation_id") or ""), []).append(mapping)

    return {
        "capabilities": {item["id"]: item for item in capabilities},
        "capability_implementations": bindings_by_capability,
        "operation_field_mappings": mappings_by_operation,
        "operation_contracts": {item["id"]: item for item in operations},
        "operation_variants": {item["id"]: item for item in variants},
        "resources": {item["id"]: item for item in resources},
    }
