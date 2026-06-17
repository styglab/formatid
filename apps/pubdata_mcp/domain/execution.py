from __future__ import annotations

import os
import re
from datetime import datetime
from time import perf_counter
from typing import Any
from urllib.parse import urljoin

import requests


SENSITIVE_KEY_PARTS = (
    "authkey",
    "apikey",
    "api_key",
    "servicekey",
    "service_key",
    "authorization",
    "access_token",
    "refresh_token",
    "secret",
    "password",
)


def smoke_test_operation(
    operation_id: str,
    semantic_arguments: dict[str, Any],
    execution_contracts: dict[str, Any],
    catalog: dict[str, Any] | None = None,
    persist: bool = True,
    variant_id: str | None = None,
) -> dict[str, Any]:
    operation_variants = execution_contracts.get("operation_variants", {})
    variant = operation_variants.get(variant_id) if variant_id and isinstance(operation_variants, dict) else {}
    if variant and not operation_id:
        operation_id = str(variant.get("operation_id") or "")
    operation_contract = _operation_contract(execution_contracts.get("operation_contracts", {}), operation_id)
    if not operation_contract:
        result = _smoke_result(
            operation_id=operation_id,
            variant_id=variant_id,
            capability_id=str((variant or {}).get("capability") or ""),
            status="skipped",
            semantic_arguments=semantic_arguments,
            error_message="operation_contract_not_found",
        )
        return _persist_smoke_result(result) if persist else result
    capability = str((variant or {}).get("capability") or operation_contract.get("capability") or "")
    semantic_arguments = {**((variant or {}).get("fixed_semantic_arguments") or {}), **semantic_arguments}
    plan = {
        "query": f"smoke test {operation_id}",
        "execution_graph": {
            "type": "dag",
            "status": "planned",
            "nodes": [
                {
                    "id": "smoke",
                    "capability": capability,
                    "variant_id": variant_id,
                    "operation_id": operation_id,
                    "call": {"semantic_arguments": semantic_arguments},
                    "argument_bindings": {},
                    "post_filters": [],
                }
            ],
        },
    }
    started = perf_counter()
    execution = execute_semantic_plan(plan, execution_contracts, catalog)
    duration_ms = int((perf_counter() - started) * 1000)
    node_result = (execution.get("results") or [{}])[0]
    status = str(node_result.get("status") or "skipped")
    raw_result = node_result.get("result") if isinstance(node_result, dict) else {}
    error = raw_result.get("error") if isinstance(raw_result, dict) else None
    result = _smoke_result(
        operation_id=operation_id,
        variant_id=variant_id,
        capability_id=capability,
        status=status,
        result_status=node_result.get("result_status"),
        semantic_arguments=semantic_arguments,
        raw_arguments=(node_result.get("arguments") or {}).get("raw", {}),
        response_sample=raw_result if isinstance(raw_result, dict) else {"value": raw_result},
        normalized_sample=node_result.get("semantic_result", {}),
        error_message=error.get("message") if isinstance(error, dict) else node_result.get("reason"),
        duration_ms=duration_ms,
    )
    return _persist_smoke_result(result) if persist else result


def execute_semantic_plan(
    plan: dict[str, Any],
    execution_contracts: dict[str, Any],
    catalog: dict[str, Any] | None = None,
) -> dict[str, Any]:
    graph = plan.get("execution_graph", {})
    nodes = graph.get("nodes", []) if isinstance(graph, dict) else []
    implementations = _normalize_implementations(execution_contracts.get("capability_implementations", {}))
    field_mappings = execution_contracts.get("operation_field_mappings", {})
    operation_contracts = execution_contracts.get("operation_contracts", {})
    operation_variants = execution_contracts.get("operation_variants", {})
    catalog_resources = execution_contracts.get("resources") or (catalog or {}).get("resources", {})
    resources = catalog_resources.get("resources", {}) if isinstance(catalog_resources.get("resources"), dict) else catalog_resources
    results = []

    for node in nodes:
        if not isinstance(node, dict):
            continue
        capability = str(node.get("capability") or "")
        variant_id = str(node.get("variant_id") or "")
        variant = operation_variants.get(variant_id) if variant_id and isinstance(operation_variants, dict) else {}
        semantic_arguments = _semantic_arguments_for_node(node, results)
        semantic_arguments = {**((variant or {}).get("fixed_semantic_arguments") or {}), **semantic_arguments}
        operation_id_hint = str(node.get("operation_id") or (variant or {}).get("operation_id") or "")
        operation_contract = _operation_contract(operation_contracts, operation_id_hint)
        capability = capability or str((variant or {}).get("capability") or operation_contract.get("capability") or "")
        implementation = _select_implementation(capability, implementations, operation_id_hint, variant_id)
        if not implementation and operation_contract:
            implementation = {
                "operation_id": operation_id_hint,
                "resource_id": operation_contract.get("resource_id"),
                "provider": operation_contract.get("provider"),
                "tool": None,
                "status": "planned",
                "method": operation_contract.get("method"),
                "path": operation_contract.get("path"),
            }
        elif implementation and operation_contract:
            implementation = {
                "resource_id": operation_contract.get("resource_id"),
                "provider": operation_contract.get("provider"),
                "method": operation_contract.get("method"),
                "path": operation_contract.get("path"),
                **implementation,
            }
        if not implementation:
            results.append(_skipped(node, "implementation_not_available"))
            continue
        fixed_raw_arguments = (variant or {}).get("fixed_raw_arguments") or {}
        missing_arguments = _missing_required_semantic_arguments(semantic_arguments, operation_contract)
        if missing_arguments:
            skipped = _skipped(node, "missing_required_semantic_arguments", implementation)
            results.append(
                {
                    **skipped,
                    "missing": missing_arguments,
                    "arguments": {
                        "semantic": semantic_arguments,
                        "raw": _redact_secrets(fixed_raw_arguments),
                    },
                }
            )
            continue
        if not semantic_arguments and not fixed_raw_arguments:
            results.append(_skipped(node, "missing_semantic_arguments", implementation))
            continue

        operation_id = operation_id_hint or str(implementation.get("operation_id") or "")
        request_mappings = _operation_mappings(field_mappings, operation_id, "request")
        response_mappings = _operation_mappings(field_mappings, operation_id, "response")
        raw_arguments = _semantic_to_raw_arguments(semantic_arguments, request_mappings, operation_contract)
        raw_arguments = {**raw_arguments, **fixed_raw_arguments}
        if not raw_arguments:
            results.append(_skipped(node, "semantic_arguments_not_mappable", implementation))
            continue
        validation_errors = _validate_raw_arguments(raw_arguments, operation_contract)
        if validation_errors:
            results.append(
                {
                    **_skipped(node, "argument_validation_failed", implementation),
                    "validation_errors": validation_errors,
                    "arguments": {
                        "semantic": semantic_arguments,
                        "raw": _redact_secrets(raw_arguments),
                    },
                }
            )
            continue

        if _can_use_generic_http(implementation, resources):
            adapter = _execute_generic_http
            tool_name = "generic_http_executor"
        else:
            results.append(
                {
                    **_skipped(node, "not_executable_missing_tool_or_http_metadata", implementation),
                    "required": ["resource base_url", "operation path", "operation method"],
                }
            )
            continue

        try:
            raw_result = adapter(
                raw_arguments,
                {
                    **implementation,
                    "tool": tool_name,
                    "_resources": resources,
                    "_operation_contract": operation_contract,
                },
            )
        except (TypeError, ValueError) as exc:
            results.append(
                {
                    **_skipped(node, "tool_argument_error", implementation),
                    "message": str(exc),
                    "arguments": {
                        "semantic": semantic_arguments,
                        "raw": _redact_secrets(raw_arguments),
                    },
                }
            )
            continue
        semantic_result = _apply_post_filters(
            _normalize_response(raw_result, response_mappings, operation_contract),
            node.get("post_filters", []),
        )
        status = "error" if isinstance(raw_result, dict) and raw_result.get("error") else "executed"
        results.append(
            {
                "node": node.get("id"),
                "capability": capability,
                "status": status,
                "result_status": _result_status(status, raw_result=raw_result, semantic_result=semantic_result),
                "implementation": _implementation_summary(implementation),
                "operation_contract": _operation_contract_summary(operation_contract),
                "variant_id": variant_id or None,
                "arguments": {
                    "semantic": semantic_arguments,
                    "raw": _redact_secrets(raw_arguments),
                },
                "result": raw_result,
                "semantic_result": semantic_result,
            }
        )

    executed = [item for item in results if item.get("status") in {"executed", "error"}]
    execution = {
        "requested": True,
        "status": "executed" if executed else "not_executed",
        "message": "Executed available semantic DAG nodes with mapped arguments."
        if executed
        else "No executable semantic DAG nodes had both an implementation and mappable arguments.",
        "results": results,
    }
    execution["answer"] = _build_answer(plan.get("query", ""), results)
    return execution


def _select_implementation(
    capability: str,
    implementations: dict[str, list[dict[str, Any]]],
    operation_id_hint: str = "",
    variant_id_hint: str = "",
) -> dict[str, Any] | None:
    candidates = implementations.get(capability, [])
    if variant_id_hint:
        for item in candidates:
            if item.get("variant_id") == variant_id_hint:
                return item
    if operation_id_hint:
        for item in candidates:
            if item.get("operation_id") == operation_id_hint:
                return item
    for item in candidates:
        if item.get("status") == "available" and item.get("tool"):
            return item
    return candidates[0] if candidates else None


def _can_use_generic_http(implementation: dict[str, Any], resources: dict[str, Any]) -> bool:
    resource = resources.get(str(implementation.get("resource_id") or ""))
    if not isinstance(resource, dict):
        return False
    return bool(resource.get("base_url") and implementation.get("path") and implementation.get("method"))


def _normalize_implementations(value: Any) -> dict[str, list[dict[str, Any]]]:
    if not isinstance(value, dict):
        return {}
    normalized: dict[str, list[dict[str, Any]]] = {}
    for capability, items in value.items():
        if isinstance(items, list):
            normalized[str(capability)] = [item for item in items if isinstance(item, dict)]
        elif isinstance(items, dict):
            normalized[str(capability)] = [items]
    return normalized


def _operation_contract(operation_contracts: Any, operation_id: str) -> dict[str, Any]:
    if not operation_id or not isinstance(operation_contracts, dict):
        return {}
    contract = operation_contracts.get(operation_id)
    return contract if isinstance(contract, dict) else {}


def _operation_contract_summary(operation_contract: dict[str, Any]) -> dict[str, Any] | None:
    if not operation_contract:
        return None
    return {
        "provider": operation_contract.get("provider"),
        "resource_id": operation_contract.get("resource_id"),
        "method": operation_contract.get("method"),
        "path": operation_contract.get("path"),
    }


def _operation_mappings(
    field_mappings: dict[str, Any],
    operation_id: str,
    direction: str,
) -> list[dict[str, Any]]:
    mappings = []
    for mapping_id, mapping in field_mappings.items():
        if not isinstance(mapping, dict):
            continue
        if not _operation_id_matches(str(mapping.get("operation_id") or ""), operation_id):
            continue
        if mapping.get("direction") != direction:
            continue
        mappings.append({"mapping_id": mapping_id, **mapping})
    return mappings


def _operation_id_matches(pattern: str, operation_id: str) -> bool:
    if pattern == operation_id:
        return True
    if pattern.endswith(".*"):
        return operation_id.startswith(pattern[:-1])
    return False


def _semantic_arguments_for_node(node: dict[str, Any], previous_results: list[dict[str, Any]]) -> dict[str, Any]:
    call = node.get("call", {}) if isinstance(node.get("call"), dict) else {}
    call_arguments = call.get("semantic_arguments", {}) if isinstance(call.get("semantic_arguments"), dict) else {}
    arguments = dict(node.get("arguments", {}) if isinstance(node.get("arguments"), dict) else call_arguments)
    bindings = node.get("argument_bindings", {}) if isinstance(node.get("argument_bindings"), dict) else {}
    for semantic_type, binding in bindings.items():
        if not isinstance(binding, dict):
            continue
        if semantic_type in arguments and arguments[semantic_type] not in (None, ""):
            continue
        values = _semantic_values_from_result(
            previous_results,
            str(binding.get("from_node") or ""),
            str(binding.get("semantic_type") or semantic_type),
        )
        if binding.get("deduplicate", True):
            values = list(dict.fromkeys(values))
        if not values:
            continue
        arguments[str(semantic_type)] = values if binding.get("mode") == "batch" else values[0]
    return arguments


def _semantic_values_from_result(
    results: list[dict[str, Any]],
    node_id: str,
    semantic_type: str,
) -> list[Any]:
    values = []
    for result in results:
        if result.get("node") != node_id:
            continue
        semantic_result = result.get("semantic_result", {})
        items = semantic_result.get("items", []) if isinstance(semantic_result, dict) else []
        for item in items:
            semantic = item.get("semantic", {}) if isinstance(item, dict) else {}
            value = semantic.get(semantic_type)
            if value not in (None, ""):
                values.append(value)
    return values


def _semantic_to_raw_arguments(
    semantic_arguments: dict[str, Any],
    request_mappings: list[dict[str, Any]],
    operation_contract: dict[str, Any] | None = None,
) -> dict[str, Any]:
    raw_from_contract = _semantic_to_raw_arguments_from_contract(semantic_arguments, operation_contract or {})
    if raw_from_contract:
        return raw_from_contract
    raw_arguments = {}
    for mapping in request_mappings:
        semantic_type = str(mapping.get("semantic_type") or "")
        field_name = str(mapping.get("field_name") or "")
        if semantic_type in semantic_arguments and field_name:
            raw_arguments[field_name] = _raw_argument_value(field_name, semantic_arguments[semantic_type])
    return raw_arguments


def _semantic_to_raw_arguments_from_contract(
    semantic_arguments: dict[str, Any],
    operation_contract: dict[str, Any],
) -> dict[str, Any]:
    request_contract = operation_contract.get("request", {}) if isinstance(operation_contract.get("request"), dict) else {}
    raw_arguments = {}
    for section in ("query", "body", "path", "header"):
        fields = request_contract.get(section, {})
        if not isinstance(fields, dict):
            continue
        _collect_contract_raw_arguments(raw_arguments, fields, semantic_arguments)
    flat_fields = request_contract.get("fields", {})
    if isinstance(flat_fields, dict):
        _collect_contract_raw_arguments(raw_arguments, flat_fields, semantic_arguments)
    defaults = request_contract.get("defaults")
    if isinstance(defaults, dict):
        for field_name, value in defaults.items():
            raw_arguments.setdefault(str(field_name), value)
    return {key: value for key, value in raw_arguments.items() if value not in (None, "")}


def _collect_contract_raw_arguments(
    raw_arguments: dict[str, Any],
    fields: dict[str, Any],
    semantic_arguments: dict[str, Any],
) -> None:
    for field_name, field_contract in fields.items():
        if not isinstance(field_contract, dict):
            continue
        field = str(field_name)
        semantic_type = str(field_contract.get("semantic_type") or "")
        if semantic_type in semantic_arguments:
            raw_arguments[field] = _contract_argument_value(
                semantic_arguments[semantic_type],
                str(field_contract.get("transform") or ""),
                field_contract,
            )
        elif field in semantic_arguments:
            raw_arguments[field] = semantic_arguments[field]
        elif "default" in field_contract:
            raw_arguments[field] = field_contract.get("default")


def _missing_required_semantic_arguments(
    semantic_arguments: dict[str, Any],
    operation_contract: dict[str, Any],
) -> list[str]:
    request_contract = operation_contract.get("request", {}) if isinstance(operation_contract.get("request"), dict) else {}
    missing: list[str] = []
    for section in ("query", "body", "path", "header"):
        fields = request_contract.get(section, {})
        if not isinstance(fields, dict):
            continue
        _collect_missing_required(missing, fields, semantic_arguments)
    flat_fields = request_contract.get("fields", {})
    if isinstance(flat_fields, dict):
        _collect_missing_required(missing, flat_fields, semantic_arguments)
    return sorted(set(missing))


def _collect_missing_required(
    missing: list[str],
    fields: dict[str, Any],
    semantic_arguments: dict[str, Any],
) -> None:
    for field_contract in fields.values():
        if not isinstance(field_contract, dict) or not field_contract.get("required"):
            continue
        semantic_type = str(field_contract.get("semantic_type") or "")
        if not semantic_type:
            continue
        value = semantic_arguments.get(semantic_type)
        has_default = "default" in field_contract and field_contract.get("default") not in (None, "", [], {})
        if value in (None, "", [], {}) and not has_default:
            missing.append(semantic_type)


def _contract_argument_value(value: Any, transform: str, field_contract: dict[str, Any]) -> Any:
    enum_mapping = field_contract.get("enum_mapping")
    if isinstance(enum_mapping, dict):
        mapped = enum_mapping.get(str(value))
        if mapped not in (None, ""):
            return mapped
    transformed = _apply_declared_transform(value, field_contract.get("transform"))
    if not isinstance(transformed, dict):
        return _format_contract_argument(transformed, field_contract)
    if transform == "date_start":
        return _format_contract_argument(transformed.get("start") or transformed.get("from"), field_contract)
    if transform == "date_end":
        return _format_contract_argument(transformed.get("end") or transformed.get("to"), field_contract)
    return transformed


def _format_contract_argument(value: Any, field_contract: dict[str, Any]) -> Any:
    value_format = str(field_contract.get("format") or "")
    if value_format in {"yyyyMMddHHmm", "YYYYMMDDHHmm"}:
        digits = _digits_only(str(value))
        return digits[:12] if len(digits) >= 12 else digits
    if value_format in {"yyyyMMdd", "YYYYMMDD"}:
        digits = _digits_only(str(value))
        return digits[:8] if len(digits) >= 8 else digits
    return value


def _apply_declared_transform(value: Any, transform: Any) -> Any:
    if not transform:
        return value
    if isinstance(transform, str):
        return _apply_transform_step(value, {"name": transform})
    if isinstance(transform, list):
        current = value
        for step in transform:
            current = _apply_declared_transform(current, step)
        return current
    if isinstance(transform, dict):
        current = value
        if "strip" in transform:
            current = _strip_chars(current, transform.get("strip"))
        if transform.get("remove_whitespace"):
            current = _remove_whitespace(current)
        if transform.get("digits_only"):
            current = _digits_only(current)
        if "case" in transform:
            current = _apply_case(current, str(transform.get("case") or ""))
        if "phone_format" in transform:
            current = _phone_format(current, str(transform.get("phone_format") or ""))
        if "date_format" in transform:
            current = _date_format(current, transform.get("date_format"))
        name = transform.get("name") or transform.get("type")
        return _apply_transform_step(current, {**transform, "name": name}) if name else current
    return value


def _apply_transform_step(value: Any, transform: dict[str, Any]) -> Any:
    name = str(transform.get("name") or "").strip()
    if name in {"", "date_start", "date_end"}:
        return value
    if name in {"strip_chars", "strip"}:
        return _strip_chars(value, transform.get("chars") or transform.get("strip"))
    if name in {"remove_whitespace", "whitespace_remove"}:
        return _remove_whitespace(value)
    if name in {"digits_only", "number_digits"}:
        return _digits_only(value)
    if name == "uppercase":
        return _apply_case(value, "upper")
    if name == "lowercase":
        return _apply_case(value, "lower")
    if name == "phone_format":
        return _phone_format(value, str(transform.get("style") or transform.get("format") or ""))
    if name == "date_format":
        return _date_format(value, transform)
    return value


def _strip_chars(value: Any, chars: Any) -> Any:
    if not isinstance(value, str):
        return value
    remove = [str(item) for item in chars] if isinstance(chars, list) else list(str(chars or ""))
    result = value
    for char in remove:
        result = result.replace(char, "")
    return result


def _remove_whitespace(value: Any) -> Any:
    return re.sub(r"\s+", "", value) if isinstance(value, str) else value


def _digits_only(value: Any) -> Any:
    return re.sub(r"\D+", "", value) if isinstance(value, str) else value


def _apply_case(value: Any, case: str) -> Any:
    if not isinstance(value, str):
        return value
    if case in {"upper", "uppercase"}:
        return value.upper()
    if case in {"lower", "lowercase"}:
        return value.lower()
    return value


def _phone_format(value: Any, style: str) -> Any:
    if not isinstance(value, str):
        return value
    digits = _digits_only(value)
    if style in {"kr_mobile_hyphen", "hyphenated_kr_mobile"} and len(digits) in {10, 11} and digits.startswith("01"):
        middle = 3 if len(digits) == 10 else 4
        return f"{digits[:3]}-{digits[3:3 + middle]}-{digits[3 + middle:]}"
    if style in {"digits_only", "number_digits"}:
        return digits
    return value


def _date_format(value: Any, rule: Any) -> Any:
    if not isinstance(value, str):
        return value
    target = str(rule.get("to") or rule.get("target") or rule.get("format") or rule if isinstance(rule, dict) else rule)
    if target in {"YYYYMMDD", "%Y%m%d"}:
        digits = _digits_only(value)
        if len(digits) >= 8:
            return digits[:8]
    return value


def _validate_raw_arguments(raw_arguments: dict[str, Any], operation_contract: dict[str, Any]) -> list[dict[str, Any]]:
    request_contract = operation_contract.get("request", {}) if isinstance(operation_contract.get("request"), dict) else {}
    errors = []
    for location in ("query", "body", "path", "header"):
        fields = request_contract.get(location, {})
        if not isinstance(fields, dict):
            continue
        for field_name, field_contract in fields.items():
            if not isinstance(field_contract, dict) or field_contract.get("kind") == "auth":
                continue
            field = str(field_name)
            value = raw_arguments.get(field)
            if value in (None, ""):
                continue
            semantic_type = field_contract.get("semantic_type")
            enum_values = field_contract.get("enum")
            if isinstance(enum_values, list) and str(value) not in {str(item) for item in enum_values}:
                errors.append(_validation_error(field, semantic_type, value, "enum", enum_values))
            pattern = field_contract.get("pattern")
            if pattern and not re.fullmatch(str(pattern), str(value)):
                errors.append(_validation_error(field, semantic_type, value, "pattern", pattern))
            min_length = field_contract.get("min_length")
            if min_length is not None and len(str(value)) < int(min_length):
                errors.append(_validation_error(field, semantic_type, value, "min_length", min_length))
            max_length = field_contract.get("max_length")
            if max_length is not None and len(str(value)) > int(max_length):
                errors.append(_validation_error(field, semantic_type, value, "max_length", max_length))
    return errors


def _validation_error(field: str, semantic_type: Any, value: Any, rule: str, expected: Any) -> dict[str, Any]:
    return {
        "field": field,
        "semantic_type": semantic_type,
        "value": value,
        "rule": rule,
        "expected": expected,
        "message": f"Value for {field} failed declared {rule} validation.",
    }


def _raw_argument_value(field_name: str, value: Any) -> Any:
    if isinstance(value, dict):
        lowered = field_name.lower()
        if any(token in lowered for token in ("bgn", "start", "from")):
            return value.get("start") or value.get("from")
        if any(token in lowered for token in ("end", "to")):
            return value.get("end") or value.get("to")
    return value


def _normalize_response(
    raw_result: Any,
    response_mappings: list[dict[str, Any]],
    operation_contract: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if not isinstance(raw_result, dict):
        return {"items": []}
    response_mappings = response_mappings or _response_mappings_from_contract(operation_contract or {})
    if not response_mappings:
        return {"items": [{"semantic": {}, "raw": item} for item in _result_items(raw_result)]}
    semantic_items = []
    for item in _result_items(raw_result):
        raw_item = item.get("raw") if isinstance(item, dict) and isinstance(item.get("raw"), dict) else item
        if not isinstance(raw_item, dict):
            continue
        semantic_item = {}
        for mapping in response_mappings:
            field_name = mapping.get("field_name")
            semantic_type = mapping.get("semantic_type")
            candidate = _mapped_response_value(raw_item, str(field_name or ""))
            if candidate is not None and semantic_type:
                current = semantic_item.get(str(semantic_type))
                if current in (None, "") or candidate not in (None, ""):
                    semantic_item[str(semantic_type)] = candidate
        if semantic_item:
            semantic_items.append({"semantic": semantic_item, "raw": raw_item})
    return {
        "items": semantic_items,
        "mapping_count": len(response_mappings),
    }


def _response_mappings_from_contract(operation_contract: dict[str, Any]) -> list[dict[str, Any]]:
    response_contract = operation_contract.get("response", {}) if isinstance(operation_contract.get("response"), dict) else {}
    fields = response_contract.get("fields", {})
    if not isinstance(fields, dict):
        return []
    mappings = []
    for field_name, field_contract in fields.items():
        if not isinstance(field_contract, dict):
            continue
        semantic_type = field_contract.get("semantic_type")
        if semantic_type:
            mappings.append(
                {
                    "field_name": str(field_name),
                    "semantic_type": str(semantic_type),
                    "direction": "response",
                    "source": "operation_contract.response.fields",
                }
            )
    return mappings


def _apply_post_filters(semantic_result: dict[str, Any], post_filters: Any) -> dict[str, Any]:
    if not isinstance(post_filters, list) or not post_filters:
        return semantic_result
    items = semantic_result.get("items", []) if isinstance(semantic_result, dict) else []
    if not isinstance(items, list):
        return semantic_result
    filtered = []
    for item in items:
        semantic = item.get("semantic", {}) if isinstance(item, dict) else {}
        if all(_matches_post_filter(semantic, filter_item) for filter_item in post_filters if isinstance(filter_item, dict)):
            filtered.append(item)
    return {**semantic_result, "items": filtered, "post_filter_count": len(post_filters)}


def _matches_post_filter(semantic: dict[str, Any], filter_item: dict[str, Any]) -> bool:
    semantic_type = str(filter_item.get("semantic_type") or "")
    operator = str(filter_item.get("operator") or filter_item.get("op") or "=")
    expected = filter_item.get("value")
    actual = semantic.get(semantic_type)
    if actual in (None, ""):
        return False
    if operator in {">=", ">", "<=", "<"}:
        actual_number = _number(actual)
        expected_number = _number(expected)
        if actual_number is None or expected_number is None:
            return False
        if operator == ">=":
            return actual_number >= expected_number
        if operator == ">":
            return actual_number > expected_number
        if operator == "<=":
            return actual_number <= expected_number
        return actual_number < expected_number
    if operator in {"=", "=="}:
        return str(actual) == str(expected)
    return True


def _mapped_response_value(raw_item: dict[str, Any], field_name: str) -> Any:
    if not field_name:
        return None
    if field_name in raw_item:
        return raw_item.get(field_name)
    for path in _response_path_candidates(field_name):
        values = _path_values(raw_item, path)
        for value in values:
            if value not in (None, ""):
                return value
    return None


def _response_path_candidates(field_name: str) -> list[str]:
    normalized = field_name.replace("[*]", "[]")
    without_arrays = normalized.replace("[]", "")
    candidates = [normalized, without_arrays]
    if "." in without_arrays:
        candidates.append(without_arrays.rsplit(".", 1)[-1])
    return list(dict.fromkeys(candidate for candidate in candidates if candidate))


def _path_values(value: Any, path: str) -> list[Any]:
    current = [value]
    for raw_part in path.split("."):
        part = raw_part.strip()
        if not part:
            continue
        array_part = part == "[]" or part.endswith("[]")
        key = "" if part == "[]" else part.removesuffix("[]")
        next_values: list[Any] = []
        for item in current:
            if key:
                if not isinstance(item, dict) or key not in item:
                    continue
                selected = item.get(key)
            else:
                selected = item
            if array_part:
                if isinstance(selected, list):
                    next_values.extend(selected)
                elif selected not in (None, ""):
                    next_values.append(selected)
            else:
                next_values.append(selected)
        current = next_values
        if not current:
            return []
    return current


def _number(value: Any) -> float | None:
    try:
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None


def _result_items(raw_result: dict[str, Any]) -> list[Any]:
    items = raw_result.get("items")
    if isinstance(items, list):
        return items
    data = raw_result.get("data")
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return [data]
    return []


def _execute_generic_http(raw_arguments: dict[str, Any], implementation: dict[str, Any]) -> dict[str, Any]:
    resources = implementation.get("_resources", {})
    resource = resources.get(str(implementation.get("resource_id") or ""))
    if not isinstance(resource, dict):
        return _generic_error("missing_resource", "Resource metadata is missing.", implementation, raw_arguments)
    base_url = str(resource.get("base_url") or "")
    path = str(implementation.get("path") or "")
    method = str(implementation.get("method") or "GET").upper()
    if not base_url or not path:
        return _generic_error("missing_endpoint", "Resource base_url or implementation path is missing.", implementation, raw_arguments)

    url = _join_url(base_url, path)
    operation_contract = implementation.get("_operation_contract", {})
    params_or_body = _with_provider_auth(
        str(implementation.get("provider") or ""),
        raw_arguments,
        operation_contract if isinstance(operation_contract, dict) else {},
    )
    query_arguments, body_arguments = _split_request_arguments(params_or_body, operation_contract)
    try:
        if method == "POST":
            response = requests.post(url, params=query_arguments, json=body_arguments, timeout=20)
        else:
            response = requests.get(url, params={**query_arguments, **body_arguments}, timeout=20)
        response.raise_for_status()
        payload = response.json()
    except requests.Timeout:
        return _generic_error("timeout", "Provider API request timed out.", implementation, raw_arguments, url)
    except requests.RequestException as exc:
        return _generic_error("transport_error", str(exc), implementation, raw_arguments, url)
    except ValueError:
        return _generic_error("invalid_response", "Provider API response was not JSON.", implementation, raw_arguments, url)

    provider_error = _contract_payload_error(payload, operation_contract if isinstance(operation_contract, dict) else {})
    if provider_error:
        return {
            "error": provider_error,
            "raw": payload,
            "evidence": {
                "tool": "generic_http_executor",
                "provider": implementation.get("provider"),
                "resource_id": implementation.get("resource_id"),
                "operation_id": implementation.get("operation_id"),
                "method": method,
                "url": url,
                "request": _redact_secrets(params_or_body),
                "query": _redact_secrets(query_arguments),
                "body": _redact_secrets(body_arguments),
                "called_at": datetime.now().astimezone().isoformat(),
            },
        }

    return {
        "items": _contract_items(payload, operation_contract if isinstance(operation_contract, dict) else {}),
        "raw": payload,
        "evidence": {
            "tool": "generic_http_executor",
            "provider": implementation.get("provider"),
            "resource_id": implementation.get("resource_id"),
            "operation_id": implementation.get("operation_id"),
            "method": method,
            "url": url,
            "request": _redact_secrets(params_or_body),
            "query": _redact_secrets(query_arguments),
            "body": _redact_secrets(body_arguments),
            "called_at": datetime.now().astimezone().isoformat(),
        },
    }


def _join_url(base_url: str, path: str) -> str:
    if base_url.endswith(path):
        return base_url
    return urljoin(base_url.rstrip("/") + "/", path.lstrip("/"))


def _with_provider_auth(
    provider: str,
    raw_arguments: dict[str, Any],
    operation_contract: dict[str, Any] | None = None,
) -> dict[str, Any]:
    params = dict(raw_arguments)
    auth = operation_contract.get("auth", {}) if isinstance(operation_contract, dict) else {}
    key = _contract_api_key(auth)
    if not key:
        return params
    auth_parameter = str(auth.get("parameter") or "serviceKey")
    params.setdefault(auth_parameter, key)
    return params


def _split_request_arguments(
    arguments: dict[str, Any],
    operation_contract: dict[str, Any] | None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    if not isinstance(operation_contract, dict):
        return {}, arguments
    request_contract = operation_contract.get("request", {}) if isinstance(operation_contract.get("request"), dict) else {}
    query_fields = set((request_contract.get("query") or {}).keys()) if isinstance(request_contract.get("query"), dict) else set()
    body_fields = set((request_contract.get("body") or {}).keys()) if isinstance(request_contract.get("body"), dict) else set()
    flat_fields = request_contract.get("fields", {})
    if isinstance(flat_fields, dict):
        for field_name, field_contract in flat_fields.items():
            if not isinstance(field_contract, dict):
                continue
            location = str(field_contract.get("location") or "query").lower()
            if location == "body":
                body_fields.add(str(field_name))
            else:
                query_fields.add(str(field_name))
    auth = operation_contract.get("auth", {}) if isinstance(operation_contract.get("auth"), dict) else {}
    auth_parameter = str(auth.get("parameter") or "")
    auth_location = str(auth.get("in") or "query")
    query = {}
    body = {}
    for key, value in arguments.items():
        if key in query_fields or (key == auth_parameter and auth_location == "query"):
            query[key] = value
        elif key in body_fields:
            body[key] = value
        else:
            body[key] = value
    return query, body


def _contract_api_key(auth: dict[str, Any]) -> str | None:
    env_names = auth.get("env_names") if isinstance(auth, dict) else None
    names = [str(value) for value in env_names] if isinstance(env_names, list) else []
    for env_name in names:
        value = os.getenv(env_name)
        if value:
            return value
    return None


def _contract_items(payload: Any, operation_contract: dict[str, Any]) -> list[dict[str, Any]]:
    response_contract = operation_contract.get("response", {}) if isinstance(operation_contract.get("response"), dict) else {}
    item_paths = _contract_path_list(response_contract.get("items_path"))
    for items_path in item_paths:
        values = _path_values(payload, items_path)
        if len(values) == 1 and isinstance(values[0], list):
            values = values[0]
        items = [item for item in values if isinstance(item, dict)]
        if items:
            return items
    if item_paths:
        return []
    selectors = operation_contract.get("selectors", {}) if isinstance(operation_contract.get("selectors"), dict) else {}
    result_root = str(selectors.get("result_root") or "")
    if result_root:
        values = _path_values(payload, result_root)
        if len(values) == 1 and isinstance(values[0], list):
            values = values[0]
        return [item for item in values if isinstance(item, dict)]
    return _generic_items(payload)


def _contract_path_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if str(item or "").strip()]
    if str(value or "").strip():
        return [str(value)]
    return []


def _generic_items(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []
    body = payload.get("response", payload)
    if isinstance(body, dict):
        body = body.get("body", body)
    if isinstance(body, dict):
        items = body.get("items", body.get("item", body.get("data", body.get("list"))))
        if isinstance(items, dict) and "item" in items:
            items = items.get("item")
        if isinstance(items, list):
            return [item for item in items if isinstance(item, dict)]
        if isinstance(items, dict):
            return [items]
    return [payload]


def _contract_payload_error(payload: Any, operation_contract: dict[str, Any]) -> dict[str, Any] | None:
    response_contract = operation_contract.get("response", {}) if isinstance(operation_contract.get("response"), dict) else {}
    error_contract = response_contract.get("error") if isinstance(response_contract.get("error"), dict) else {}
    success_contract = response_contract.get("success") if isinstance(response_contract.get("success"), dict) else {}
    if error_contract:
        code = _first_path_value(payload, str(error_contract.get("code_path") or ""))
        message = _first_path_value(payload, str(error_contract.get("message_path") or ""))
        error_equals = error_contract.get("equals")
        error_not_equals = error_contract.get("not_equals")
        is_error = False
        if error_equals is not None:
            is_error = str(code) == str(error_equals)
        elif error_not_equals is not None:
            is_error = str(code) != str(error_not_equals)
        elif code not in (None, ""):
            is_error = True
        if is_error:
            return {
                "type": "provider_error",
                "message": str(message or "Provider API returned an error payload."),
                "provider_status": code,
            }
    if success_contract:
        value = _first_path_value(payload, str(success_contract.get("path") or ""))
        if "equals" in success_contract and str(value) != str(success_contract.get("equals")):
            message = _first_path_value(payload, str(success_contract.get("message_path") or ""))
            return {
                "type": "provider_error",
                "message": str(message or "Provider API did not match the declared success condition."),
                "provider_status": value,
            }
        if "in" in success_contract and isinstance(success_contract.get("in"), list):
            allowed = {str(item) for item in success_contract.get("in", [])}
            if str(value) not in allowed:
                message = _first_path_value(payload, str(success_contract.get("message_path") or ""))
                return {
                    "type": "provider_error",
                    "message": str(message or "Provider API did not match the declared success condition."),
                    "provider_status": value,
                }
    return None


def _first_path_value(payload: Any, path: str) -> Any:
    if not path:
        return None
    values = _path_values(payload, path)
    return values[0] if values else None


def _generic_error(
    error_type: str,
    message: str,
    implementation: dict[str, Any],
    raw_arguments: dict[str, Any],
    url: str | None = None,
) -> dict[str, Any]:
    return {
        "error": {
            "type": error_type,
            "message": message,
        },
        "evidence": {
            "tool": "generic_http_executor",
            "provider": implementation.get("provider"),
            "resource_id": implementation.get("resource_id"),
            "operation_id": implementation.get("operation_id"),
            "url": url,
            "request": _redact_secrets(raw_arguments),
        },
    }


def _skipped(
    node: dict[str, Any],
    reason: str,
    implementation: dict[str, Any] | None = None,
) -> dict[str, Any]:
    row = {
        "node": node.get("id"),
        "capability": node.get("capability"),
        "status": "skipped",
        "result_status": _result_status("skipped", reason=reason),
        "reason": reason,
    }
    if implementation:
        row["implementation"] = _implementation_summary(implementation)
    return row


def _result_status(
    status: str,
    *,
    raw_result: Any | None = None,
    semantic_result: dict[str, Any] | None = None,
    reason: str | None = None,
) -> str:
    if status == "executed":
        items = semantic_result.get("items", []) if isinstance(semantic_result, dict) else []
        return "executed_with_items" if items else "executed_empty"
    if status == "error":
        error = raw_result.get("error") if isinstance(raw_result, dict) else {}
        error_type = str(error.get("type") or "") if isinstance(error, dict) else ""
        if error_type == "provider_error":
            return "provider_error"
        if error_type in {"timeout", "transport_error", "invalid_response"}:
            return error_type
        return "execution_error"
    if status == "skipped":
        if reason in {
            "missing_required_semantic_arguments",
            "missing_semantic_arguments",
            "semantic_arguments_not_mappable",
            "tool_argument_error",
            "argument_validation_failed",
        }:
            return "validation_error"
        return "not_executable"
    return status


def _implementation_summary(implementation: dict[str, Any]) -> dict[str, Any]:
    return {
        "operation_id": implementation.get("operation_id"),
        "variant_id": implementation.get("variant_id"),
        "resource_id": implementation.get("resource_id"),
        "provider": implementation.get("provider"),
        "tool": implementation.get("tool"),
        "method": implementation.get("method"),
        "path": implementation.get("path"),
    }


def _build_answer(_query: str, results: list[dict[str, Any]]) -> dict[str, Any]:
    executed = [item for item in results if item.get("status") == "executed"]
    errors = [item for item in results if item.get("status") == "error"]
    skipped = [item for item in results if item.get("status") == "skipped"]
    return {
        "text": None,
        "mode": "structured_only",
        "message": "Natural-language answer generation is disabled until an LLM answer generator is configured.",
        "facts": _structured_facts(executed),
        "executed_count": len(executed),
        "error_count": len(errors),
        "skipped_count": len(skipped),
        "errors": [_error_summary(item) for item in errors],
        "skipped": [_skip_summary(item) for item in skipped],
    }


def _structured_facts(executed: list[dict[str, Any]]) -> list[dict[str, Any]]:
    facts = []
    for result in executed:
        semantic_result = result.get("semantic_result", {})
        items = semantic_result.get("items", []) if isinstance(semantic_result, dict) else []
        for item in items:
            semantic = item.get("semantic", {}) if isinstance(item, dict) else {}
            if semantic:
                facts.append(
                    {
                        "node": result.get("node"),
                        "capability": result.get("capability"),
                        "operation_id": (result.get("implementation") or {}).get("operation_id"),
                        "semantic": semantic,
                    }
                )
    return facts


def _error_summary(item: dict[str, Any]) -> dict[str, Any]:
    error = (item.get("result") or {}).get("error") if isinstance(item.get("result"), dict) else {}
    return {
        "node": item.get("node"),
        "capability": item.get("capability"),
        "operation_id": (item.get("implementation") or {}).get("operation_id"),
        "type": error.get("type") if isinstance(error, dict) else None,
        "message": error.get("message") if isinstance(error, dict) else None,
    }


def _skip_summary(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "node": item.get("node"),
        "capability": item.get("capability"),
        "operation_id": (item.get("implementation") or {}).get("operation_id"),
        "reason": item.get("reason"),
    }


def _smoke_result(
    *,
    operation_id: str,
    variant_id: str | None = None,
    capability_id: str | None = None,
    status: str,
    result_status: str | None = None,
    semantic_arguments: dict[str, Any],
    raw_arguments: dict[str, Any] | None = None,
    response_sample: dict[str, Any] | None = None,
    normalized_sample: dict[str, Any] | None = None,
    error_message: str | None = None,
    duration_ms: int | None = None,
) -> dict[str, Any]:
    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    subject = variant_id or operation_id
    return {
        "id": f"check.{subject}.{timestamp}",
        "operation_id": operation_id,
        "variant_id": variant_id,
        "capability_id": capability_id,
        "check_type": "smoke_test",
        "status": status,
        "result_status": result_status or status,
        "request_payload": {
            "semantic_arguments": semantic_arguments,
            "raw_arguments": _redact_secrets(raw_arguments or {}),
        },
        "response_sample": _compact_sample(_redact_secrets(response_sample or {})),
        "normalized_sample": _compact_sample(normalized_sample or {}),
        "error_message": error_message,
        "executor": "pubdata_mcp",
        "duration_ms": duration_ms,
    }


def _compact_sample(value: dict[str, Any], max_chars: int = 20000) -> dict[str, Any]:
    import json

    try:
        encoded = json.dumps(value, ensure_ascii=False, default=str)
    except TypeError:
        return {"value": str(value)[:max_chars]}
    if len(encoded) <= max_chars:
        return value
    return {"truncated": True, "preview": encoded[:max_chars]}


def _persist_smoke_result(result: dict[str, Any]) -> dict[str, Any]:
    from apps.pubdata_mcp.domain.catalog import record_endpoint_check

    stored = record_endpoint_check(result)
    return {**result, "stored": bool(stored), "stored_record": stored}


def _redact_secrets(value: Any) -> Any:
    if isinstance(value, dict):
        redacted = {}
        for key, item in value.items():
            key_text = str(key)
            if _is_sensitive_key(key_text):
                redacted[key] = "***REDACTED***"
            else:
                redacted[key] = _redact_secrets(item)
        return redacted
    if isinstance(value, list):
        return [_redact_secrets(item) for item in value]
    return value


def _is_sensitive_key(key: str) -> bool:
    normalized = key.replace("-", "_").lower()
    compact = normalized.replace("_", "")
    return any(part in normalized or part in compact for part in SENSITIVE_KEY_PARTS)
