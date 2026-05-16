from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Callable
from urllib.parse import urljoin

import requests

from apps.pubdata_mcp.app.providers.nts.tools import (
    check_business_status,
    validate_business_registration,
)
from apps.pubdata_mcp.app.providers.pps.tools import search_contracts
from apps.pubdata_mcp.app.providers.pps.parsers import contract_companies


ToolAdapter = Callable[[dict[str, Any], dict[str, Any]], dict[str, Any]]


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
    resources = (catalog or {}).get("resources", {}).get("resources", {})
    results = []

    for node in nodes:
        if not isinstance(node, dict):
            continue
        capability = str(node.get("capability") or "")
        semantic_arguments = _semantic_arguments_for_node(node, results)
        operation_id_hint = str(node.get("operation_id") or "")
        operation_contract = _operation_contract(operation_contracts, operation_id_hint)
        implementation = _select_implementation(capability, implementations, operation_id_hint)
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
        if not implementation:
            results.append(_skipped(node, "implementation_not_available"))
            continue
        if not semantic_arguments:
            results.append(_skipped(node, "missing_semantic_arguments", implementation))
            continue

        operation_id = operation_id_hint or str(implementation.get("operation_id") or "")
        request_mappings = _operation_mappings(field_mappings, operation_id, "request")
        response_mappings = _operation_mappings(field_mappings, operation_id, "response")
        raw_arguments = _semantic_to_raw_arguments(semantic_arguments, request_mappings, operation_contract)
        if not raw_arguments:
            results.append(_skipped(node, "semantic_arguments_not_mappable", implementation))
            continue

        tool_name = _tool_name(capability, implementation)
        adapter = TOOL_REGISTRY.get(tool_name)
        if operation_contract and _can_use_generic_http(implementation, resources):
            adapter = _execute_generic_http
            tool_name = "generic_http_executor"
        elif adapter is None and _can_use_generic_http(implementation, resources):
            adapter = _execute_generic_http
            tool_name = "generic_http_executor"
        if adapter is None:
            results.append(
                {
                    **_skipped(node, "not_executable_missing_tool_or_http_metadata", implementation),
                    "required": ["tool adapter", "or resource base_url + implementation path/method"],
                }
            )
            continue
        missing_required = _missing_required_raw_arguments(tool_name, raw_arguments)
        if missing_required:
            results.append(
                {
                    **_skipped(node, "missing_required_raw_arguments", implementation),
                    "missing": missing_required,
                    "arguments": {
                        "semantic": semantic_arguments,
                        "raw": raw_arguments,
                    },
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
                        "raw": raw_arguments,
                    },
                }
            )
            continue
        semantic_result = _apply_post_filters(
            _normalize_response(raw_result, response_mappings, operation_contract),
            node.get("post_filters", []),
        )
        results.append(
            {
                "node": node.get("id"),
                "capability": capability,
                "status": "error" if isinstance(raw_result, dict) and raw_result.get("error") else "executed",
                "implementation": _implementation_summary(implementation),
                "operation_contract": _operation_contract_summary(operation_contract),
                "arguments": {
                    "semantic": semantic_arguments,
                    "raw": raw_arguments,
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
) -> dict[str, Any] | None:
    candidates = implementations.get(capability, [])
    if operation_id_hint:
        for item in candidates:
            if item.get("operation_id") == operation_id_hint:
                return item
    for item in candidates:
        if item.get("status") == "available" and item.get("tool"):
            return item
    return candidates[0] if candidates else None


def _tool_name(capability: str, implementation: dict[str, Any]) -> str:
    explicit = implementation.get("tool")
    if explicit:
        return str(explicit)
    provider = str(implementation.get("provider") or "")
    return FALLBACK_TOOL_BY_PROVIDER_CAPABILITY.get((provider, capability), "")


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
        for field_name, field_contract in fields.items():
            if not isinstance(field_contract, dict):
                continue
            semantic_type = str(field_contract.get("semantic_type") or "")
            if semantic_type in semantic_arguments:
                raw_arguments[str(field_name)] = _contract_argument_value(
                    semantic_arguments[semantic_type],
                    str(field_contract.get("transform") or ""),
                    field_contract,
                )
            elif "default" in field_contract:
                raw_arguments[str(field_name)] = field_contract.get("default")
    return {key: value for key, value in raw_arguments.items() if value not in (None, "")}


def _contract_argument_value(value: Any, transform: str, field_contract: dict[str, Any]) -> Any:
    enum_mapping = field_contract.get("enum_mapping")
    if isinstance(enum_mapping, dict):
        mapped = enum_mapping.get(str(value))
        if mapped not in (None, ""):
            return mapped
    if not isinstance(value, dict):
        return value
    if transform == "date_start":
        return value.get("start") or value.get("from")
    if transform == "date_end":
        return value.get("end") or value.get("to")
    return value


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
    if not isinstance(raw_result, dict) or not response_mappings:
        return {"items": []}
    semantic_items = []
    for item in _result_items(raw_result):
        raw_item = item.get("raw") if isinstance(item, dict) and isinstance(item.get("raw"), dict) else item
        if not isinstance(raw_item, dict):
            continue
        semantic_item = {}
        for mapping in response_mappings:
            field_name = mapping.get("field_name")
            semantic_type = mapping.get("semantic_type")
            if field_name in raw_item and semantic_type:
                current = semantic_item.get(str(semantic_type))
                candidate = raw_item.get(field_name)
                if current in (None, "") or candidate not in (None, ""):
                    semantic_item[str(semantic_type)] = candidate
        company_items = _company_semantic_items(raw_item, semantic_item, operation_contract or {})
        if company_items:
            semantic_items.extend(company_items)
        elif semantic_item:
            semantic_items.append({"semantic": semantic_item, "raw": raw_item})
    return {
        "items": semantic_items,
        "mapping_count": len(response_mappings),
    }


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
    operator = str(filter_item.get("operator") or "=")
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


def _number(value: Any) -> float | None:
    try:
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None


def _company_semantic_items(
    raw_item: dict[str, Any],
    base_semantic: dict[str, Any],
    operation_contract: dict[str, Any],
) -> list[dict[str, Any]]:
    extractors = (
        operation_contract.get("response", {}).get("extractors", {})
        if isinstance(operation_contract.get("response"), dict)
        else {}
    )
    corp_extractor = extractors.get("corpList") if isinstance(extractors, dict) else None
    if not isinstance(corp_extractor, dict) or corp_extractor.get("parser") != "pps_contract_companies":
        return []
    produces = corp_extractor.get("produces", {})
    if not isinstance(produces, dict):
        return []
    rows = []
    for company in contract_companies(raw_item):
        if not isinstance(company, dict):
            continue
        semantic = dict(base_semantic)
        for raw_name, semantic_type in produces.items():
            value = company.get(str(raw_name))
            if value not in (None, ""):
                semantic[str(semantic_type)] = value
        if semantic:
            rows.append({"semantic": semantic, "raw": raw_item, "extracted": {"corpList": company}})
    return rows


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

    provider_error = _provider_payload_error(payload)
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
                "request": params_or_body,
                "query": query_arguments,
                "body": body_arguments,
                "called_at": datetime.now().astimezone().isoformat(),
            },
        }

    return {
        "items": _generic_items(payload),
        "raw": payload,
        "evidence": {
            "tool": "generic_http_executor",
            "provider": implementation.get("provider"),
            "resource_id": implementation.get("resource_id"),
            "operation_id": implementation.get("operation_id"),
            "method": method,
            "url": url,
            "request": params_or_body,
            "query": query_arguments,
            "body": body_arguments,
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
    key = _provider_api_key(provider)
    if not key:
        return params
    auth = operation_contract.get("auth", {}) if isinstance(operation_contract, dict) else {}
    auth_parameter = str(auth.get("parameter") or "serviceKey")
    if provider == "vworld":
        params.setdefault(auth_parameter if auth_parameter != "serviceKey" else "key", key)
        params.setdefault("format", "json")
    elif provider in {"pps", "kma", "keco", "vworld", "kasi", "exim", "fss"}:
        params.setdefault(auth_parameter, key)
        params.setdefault("type", "json")
    elif provider == "nts":
        params.setdefault(auth_parameter, key)
        params.setdefault("returnType", "JSON")
    else:
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
    auth = operation_contract.get("auth", {}) if isinstance(operation_contract.get("auth"), dict) else {}
    auth_parameter = str(auth.get("parameter") or "")
    auth_location = str(auth.get("in") or "query")
    query = {}
    body = {}
    for key, value in arguments.items():
        if key in query_fields or (key == auth_parameter and auth_location == "query") or key in {"type", "returnType"}:
            query[key] = value
        elif key in body_fields:
            body[key] = value
        else:
            body[key] = value
    return query, body


def _provider_api_key(provider: str) -> str | None:
    env_names = {
        "pps": ("PPS_PUBLIC_API_KEY", "G2B_PUBLIC_API_KEY", "PUBLIC_API_KEY"),
        "nts": ("NTS_BUSINESSMAN_API_KEY", "ODCLOUD_API_KEY", "PUBLIC_API_KEY"),
        "kma": ("KMA_PUBLIC_API_KEY", "PUBLIC_API_KEY"),
        "keco": ("KECO_PUBLIC_API_KEY", "AIRKOREA_PUBLIC_API_KEY", "PUBLIC_API_KEY"),
        "vworld": ("VWORLD_API_KEY", "PUBLIC_API_KEY"),
        "kasi": ("KASI_PUBLIC_API_KEY", "PUBLIC_API_KEY"),
        "exim": ("EXIM_API_KEY", "PUBLIC_API_KEY"),
        "fss": ("FSS_API_KEY", "PUBLIC_API_KEY"),
    }.get(provider, ("PUBLIC_API_KEY",))
    for env_name in env_names:
        value = os.getenv(env_name)
        if value:
            return value
    return None


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


def _provider_payload_error(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    response = payload.get("response")
    if isinstance(response, dict):
        status = str(response.get("status") or "").upper()
        error = response.get("error")
        if status == "ERROR" or isinstance(error, dict):
            message = ""
            if isinstance(error, dict):
                message = str(error.get("text") or error.get("message") or "")
            return {
                "type": "provider_error",
                "message": message or "Provider API returned an error payload.",
                "provider_status": response.get("status"),
                "provider_error": error,
            }
    header = payload.get("header")
    if isinstance(header, dict) and str(header.get("resultCode") or "00") not in {"00", "0", "SUCCESS"}:
        return {
            "type": "provider_error",
            "message": str(header.get("resultMsg") or "Provider API returned an error payload."),
            "provider_status": header.get("resultCode"),
            "provider_error": header,
        }
    return None


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
            "request": raw_arguments,
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
        "reason": reason,
    }
    if implementation:
        row["implementation"] = _implementation_summary(implementation)
    return row


def _implementation_summary(implementation: dict[str, Any]) -> dict[str, Any]:
    return {
        "operation_id": implementation.get("operation_id"),
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


def _missing_required_raw_arguments(tool_name: str, raw_arguments: dict[str, Any]) -> list[str]:
    required = TOOL_REQUIRED_RAW_ARGUMENTS.get(tool_name, set())
    return sorted(item for item in required if raw_arguments.get(item) in (None, ""))


def _check_nts_business_status(raw_arguments: dict[str, Any], _implementation: dict[str, Any]) -> dict[str, Any]:
    business_number = raw_arguments.get("b_no")
    if isinstance(business_number, list):
        business_numbers = [str(value) for value in business_number if value not in (None, "")]
    else:
        business_numbers = [str(business_number)]
    return check_business_status(business_numbers)


def _validate_nts_business_registration(raw_arguments: dict[str, Any], _implementation: dict[str, Any]) -> dict[str, Any]:
    required = {"b_no", "p_nm", "start_dt"}
    missing = sorted(required - set(raw_arguments))
    if missing:
        return {
            "error": {
                "type": "missing_arguments",
                "message": "Validation requires b_no, p_nm, and start_dt.",
                "missing": missing,
            }
        }
    return validate_business_registration([raw_arguments])


def _search_pps_contracts(raw_arguments: dict[str, Any], _implementation: dict[str, Any]) -> dict[str, Any]:
    return search_contracts(
        category=str(raw_arguments.get("bsnsDivNm") or "물품"),
        contract_date_from=str(raw_arguments.get("inqryBgnDt") or raw_arguments.get("cntrctDate") or ""),
        contract_date_to=str(raw_arguments.get("inqryEndDt") or raw_arguments.get("cntrctDate") or ""),
        keyword=raw_arguments.get("cntrctNm"),
        page_no=int(raw_arguments.get("pageNo") or 1),
        num_of_rows=int(raw_arguments.get("numOfRows") or 10),
    )


TOOL_REGISTRY: dict[str, ToolAdapter] = {
    "check_nts_business_status_live": _check_nts_business_status,
    "validate_nts_business_registration_live": _validate_nts_business_registration,
    "search_pps_contracts_live": _search_pps_contracts,
}

TOOL_REQUIRED_RAW_ARGUMENTS: dict[str, set[str]] = {
    "check_nts_business_status_live": {"b_no"},
    "validate_nts_business_registration_live": {"b_no", "p_nm", "start_dt"},
}

FALLBACK_TOOL_BY_PROVIDER_CAPABILITY: dict[tuple[str, str], str] = {
    ("pps", "search_procurement_contracts"): "search_pps_contracts_live",
}
