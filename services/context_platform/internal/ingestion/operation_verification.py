from __future__ import annotations

import json
import os
import re
from typing import Any
from urllib.parse import urljoin

import httpx

from services.context_platform.internal.storage import ContextPlatformRepository


SAFE_METHODS = {"GET", "HEAD"}
SECRET_NAME_PATTERN = re.compile(r"(api[-_]?key|servicekey|token|secret|password|authorization|auth)", re.IGNORECASE)


def verify_ingestion_contracts(
    repo: ContextPlatformRepository,
    *,
    run_id: str,
    source: dict[str, Any],
    document: dict[str, Any],
    operations: list[dict[str, Any]],
    binding_generation: dict[str, Any],
    capability_generation: dict[str, Any],
) -> dict[str, Any]:
    operation_checks = [
        _persist_operation_check(
            repo,
            _verify_operation(source=source, document=document, run_id=run_id, operation=operation),
        )
        for operation in operations
    ]
    operation_check_by_id = {
        str(item.get("source_operation_id") or ""): item
        for item in operation_checks
        if item.get("source_operation_id")
    }
    binding_by_operation = _bindings_by_operation(binding_generation)
    capability_checks = [
        _persist_operation_check(
            repo,
            _verify_capability(
                source=source,
                document=document,
                run_id=run_id,
                suggestion=suggestion,
                operation_check_by_id=operation_check_by_id,
                binding_by_operation=binding_by_operation,
            ),
        )
        for suggestion in capability_generation.get("suggestions", [])
        if isinstance(suggestion, dict) and suggestion.get("decision") == "propose_capability"
    ]
    checks = operation_checks + capability_checks
    return {
        "operation_checks": operation_checks,
        "capability_checks": capability_checks,
        "summary": _verification_summary(checks),
    }


def _verify_operation(
    *,
    source: dict[str, Any],
    document: dict[str, Any],
    run_id: str,
    operation: dict[str, Any],
) -> dict[str, Any]:
    method = str(operation.get("method") or "GET").upper()
    source_config = source.get("config") if isinstance(source.get("config"), dict) else {}
    base_url = _operation_base_url(source, operation)
    required_parameters = [
        parameter
        for parameter in operation.get("parameters", [])
        if isinstance(parameter, dict) and bool(parameter.get("is_required"))
    ]
    sample_values, missing_required = _sample_parameter_values(source, operation, required_parameters)
    request_parts = _build_request_parts(operation, sample_values)
    request_sample = {
        "method": method,
        "base_url": base_url,
        "path": operation.get("path"),
        "query": _redact_mapping(request_parts["query"]),
        "headers": _redact_mapping(request_parts["headers"]),
        "body": _redact_value(request_parts["body"]),
    }
    base_record = {
        "run_id": run_id,
        "source_id": source.get("id"),
        "source_document_id": document.get("id"),
        "source_operation_id": operation.get("id"),
        "check_type": "operation",
        "request_sample_redacted": request_sample,
        "field_coverage": {},
        "binding_validation": {
            "required_parameter_count": len(required_parameters),
            "missing_required_parameters": missing_required,
        },
    }
    if not _method_allowed_for_verification(source_config, operation, method):
        return {
            **base_record,
            "status": "skipped",
            "error_message": f"Method {method} is not enabled for ingestion verification.",
        }
    if not base_url:
        return {
            **base_record,
            "status": "skipped",
            "error_message": "No verification base_url is configured for this source or operation.",
        }
    if missing_required:
        return {
            **base_record,
            "status": "needs_input",
            "error_message": "Required sample parameters are missing for endpoint verification.",
        }

    url = _join_url(base_url, str(operation.get("path") or ""))
    timeout = float(os.getenv("CONTEXT_PLATFORM_ENDPOINT_CHECK_TIMEOUT_SECONDS") or "8")
    try:
        with httpx.Client(timeout=timeout, follow_redirects=False) as client:
            response = client.request(
                method,
                url,
                params=request_parts["query"],
                headers=request_parts["headers"],
                json=request_parts["body"] if request_parts["body"] not in ({}, None) else None,
            )
    except httpx.TimeoutException as exc:
        return {
            **base_record,
            "status": "failed",
            "binding_validation": {
                **base_record["binding_validation"],
                "error_category": "transient_timeout",
                "transient": True,
            },
            "error_message": str(exc),
        }
    except httpx.HTTPError as exc:
        category = "network_error"
        return {
            **base_record,
            "status": "failed",
            "binding_validation": {
                **base_record["binding_validation"],
                "error_category": category,
                "transient": True,
            },
            "error_message": str(exc),
        }

    response_ref = _response_sample_ref(response)
    coverage = _response_field_coverage(operation.get("fields", []), response)
    status = "verified" if 200 <= response.status_code < 300 and not coverage.get("missing_required_output_paths") else "failed"
    error_category = "" if status == "verified" else _response_error_category(response.status_code, coverage)
    return {
        **base_record,
        "status": status,
        "http_status": response.status_code,
        "response_sample_ref": response_ref,
        "field_coverage": coverage,
        "binding_validation": {
            **base_record["binding_validation"],
            "error_category": error_category,
            "transient": error_category in {"rate_limited", "upstream_5xx"},
        },
        "error_message": "" if status == "verified" else "Endpoint response did not satisfy expected status or required output field coverage.",
    }


def _method_allowed_for_verification(source_config: dict[str, Any], operation: dict[str, Any], method: str) -> bool:
    if method in SAFE_METHODS:
        return True
    endpoint_metadata = operation.get("endpoint_metadata") if isinstance(operation.get("endpoint_metadata"), dict) else {}
    source_verification = source_config.get("verification") if isinstance(source_config.get("verification"), dict) else {}
    endpoint_verification = endpoint_metadata.get("verification") if isinstance(endpoint_metadata.get("verification"), dict) else {}
    if endpoint_verification.get("allow_unsafe_methods") is True or source_verification.get("allow_unsafe_methods") is True:
        return True
    allowed: list[str] = []
    for config in (source_verification, endpoint_verification):
        methods = config.get("allow_methods")
        if isinstance(methods, list):
            allowed.extend(str(item).upper() for item in methods if str(item or "").strip())
    return method in set(allowed)


def _verify_capability(
    *,
    source: dict[str, Any],
    document: dict[str, Any],
    run_id: str,
    suggestion: dict[str, Any],
    operation_check_by_id: dict[str, dict[str, Any]],
    binding_by_operation: dict[str, list[dict[str, Any]]],
) -> dict[str, Any]:
    capability = suggestion.get("capability") if isinstance(suggestion.get("capability"), dict) else {}
    capability_key = str(capability.get("capability_key") or suggestion.get("source_operation_id") or "")
    source_operation_id = str(suggestion.get("source_operation_id") or "")
    bindings = binding_by_operation.get(source_operation_id, [])
    input_bindings = [item for item in bindings if item.get("decision") == "bind" and item.get("direction") == "input"]
    output_bindings = [item for item in bindings if item.get("decision") == "bind" and item.get("direction") == "output"]
    operation_check = operation_check_by_id.get(source_operation_id) or {}
    operation_status = str(operation_check.get("status") or "skipped")
    binding_validation = {
        "operation_check_status": operation_status,
        "input_binding_count": len(input_bindings),
        "output_binding_count": len(output_bindings),
        "input_bindings_ready": bool(input_bindings),
        "output_bindings_ready": bool(output_bindings),
        "operation_check_id": operation_check.get("id"),
        "error_category": (
            operation_check.get("binding_validation", {}).get("error_category")
            if isinstance(operation_check.get("binding_validation"), dict)
            else ""
        ),
        "transient": (
            bool(operation_check.get("binding_validation", {}).get("transient"))
            if isinstance(operation_check.get("binding_validation"), dict)
            else False
        ),
    }
    if not source_operation_id:
        status = "failed"
        message = "Capability has no source_operation_id."
    elif not input_bindings or not output_bindings:
        status = "failed"
        message = "Capability lacks input or output bindings."
    elif operation_status == "verified":
        status = "verified"
        message = ""
    elif operation_status == "needs_input":
        status = "needs_input"
        message = "Operation verification needs sample input before capability can be verified."
    elif operation_status == "failed":
        status = "failed"
        message = "Operation verification failed."
    else:
        status = "skipped"
        message = "Operation verification was skipped."
    return {
        "run_id": run_id,
        "source_id": source.get("id"),
        "source_document_id": document.get("id"),
        "source_operation_id": source_operation_id or None,
        "capability_key": capability_key,
        "check_type": "capability",
        "status": status,
        "http_status": operation_check.get("http_status"),
        "request_sample_redacted": operation_check.get("request_sample_redacted") if isinstance(operation_check.get("request_sample_redacted"), dict) else {},
        "response_sample_ref": operation_check.get("response_sample_ref") if isinstance(operation_check.get("response_sample_ref"), dict) else {},
        "field_coverage": operation_check.get("field_coverage") if isinstance(operation_check.get("field_coverage"), dict) else {},
        "binding_validation": binding_validation,
        "error_message": message,
    }


def _persist_operation_check(repo: ContextPlatformRepository, payload: dict[str, Any]) -> dict[str, Any]:
    return repo.create_endpoint_check(payload)


def _response_error_category(status_code: int, coverage: dict[str, Any]) -> str:
    if status_code in {401, 403}:
        return "auth_failed"
    if status_code == 429:
        return "rate_limited"
    if 500 <= status_code < 600:
        return "upstream_5xx"
    if coverage.get("missing_required_output_paths"):
        return "response_shape_mismatch"
    if status_code < 200 or status_code >= 300:
        return "http_status_failed"
    return ""


def _operation_base_url(source: dict[str, Any], operation: dict[str, Any]) -> str:
    source_config = source.get("config") if isinstance(source.get("config"), dict) else {}
    endpoint_metadata = operation.get("endpoint_metadata") if isinstance(operation.get("endpoint_metadata"), dict) else {}
    verification = source_config.get("verification") if isinstance(source_config.get("verification"), dict) else {}
    endpoint_verification = endpoint_metadata.get("verification") if isinstance(endpoint_metadata.get("verification"), dict) else {}
    for value in [
        endpoint_verification.get("base_url"),
        endpoint_metadata.get("base_url"),
        verification.get("base_url"),
        source_config.get("base_url"),
        source_config.get("api_base_url"),
    ]:
        if isinstance(value, str) and value.startswith(("http://", "https://")):
            return value.rstrip("/")
    return ""


def _sample_parameter_values(
    source: dict[str, Any],
    operation: dict[str, Any],
    required_parameters: list[dict[str, Any]],
) -> tuple[dict[str, str], list[str]]:
    values: dict[str, str] = {}
    source_config = source.get("config") if isinstance(source.get("config"), dict) else {}
    operation_samples = _operation_sample_config(source_config, operation)
    for parameter in operation.get("parameters", []):
        if not isinstance(parameter, dict):
            continue
        name = str(parameter.get("name") or parameter.get("raw_name") or "")
        if not name:
            continue
        value = _sample_value_for_parameter(parameter, operation_samples, source_config)
        if value != "":
            values[name] = value
    for name, value in _configured_secret_parameters(source_config).items():
        if name not in values and value:
            values[name] = value
    missing = [
        str(parameter.get("name") or parameter.get("raw_name") or "")
        for parameter in required_parameters
        if str(parameter.get("name") or parameter.get("raw_name") or "") not in values
    ]
    return values, missing


def _build_request_parts(operation: dict[str, Any], sample_values: dict[str, str]) -> dict[str, Any]:
    query: dict[str, Any] = {}
    headers: dict[str, Any] = {}
    body: dict[str, Any] = {}
    for parameter in operation.get("parameters", []):
        if not isinstance(parameter, dict):
            continue
        name = str(parameter.get("name") or parameter.get("raw_name") or "")
        if not name or name not in sample_values:
            continue
        value = sample_values[name]
        location = str(parameter.get("location") or "").lower()
        parameter_path = str(parameter.get("parameter_path") or "")
        if location == "header":
            headers[name] = value
        elif location == "path":
            query[name] = value
        elif location == "body":
            _set_body_value(body, parameter_path, name, value, str(parameter.get("data_type") or ""))
        else:
            query[name] = value
    return {"query": query, "headers": headers, "body": body}


def _set_body_value(body: dict[str, Any], parameter_path: str, name: str, value: Any, data_type: str) -> None:
    path = parameter_path
    for prefix in ("request.body.", "body."):
        if path.startswith(prefix):
            path = path.removeprefix(prefix)
            break
    if not path or path.startswith("request."):
        path = name
    parts = [part for part in path.split(".") if part]
    if not parts:
        parts = [name]
    _assign_body_path(body, parts, value, data_type=data_type)


def _assign_body_path(container: dict[str, Any], parts: list[str], value: Any, *, data_type: str) -> None:
    current: Any = container
    for index, part in enumerate(parts):
        is_last = index == len(parts) - 1
        is_array = part.endswith("[]")
        key = part[:-2] if is_array else part
        if not key:
            continue
        if is_last:
            if is_array or data_type.lower() == "array":
                current[key] = [value]
            else:
                current[key] = value
            return
        if is_array:
            values = current.setdefault(key, [{}])
            if not isinstance(values, list) or not values:
                values = [{}]
                current[key] = values
            if not isinstance(values[0], dict):
                values[0] = {}
            current = values[0]
        else:
            child = current.setdefault(key, {})
            if not isinstance(child, dict):
                child = {}
                current[key] = child
            current = child


def _configured_secret_parameters(source_config: dict[str, Any]) -> dict[str, str]:
    verification = source_config.get("verification") if isinstance(source_config.get("verification"), dict) else {}
    secret_env = verification.get("secret_env") if isinstance(verification.get("secret_env"), dict) else {}
    values: dict[str, str] = {}
    for parameter_name, env_name in secret_env.items():
        name = str(parameter_name or "")
        env = str(env_name or "")
        if not name or not env:
            continue
        value = os.getenv(env, "")
        if value:
            values[name] = value
    return values


def _operation_sample_config(source_config: dict[str, Any], operation: dict[str, Any]) -> dict[str, Any]:
    verification = source_config.get("verification") if isinstance(source_config.get("verification"), dict) else {}
    samples = verification.get("sample_parameters") if isinstance(verification.get("sample_parameters"), dict) else {}
    operation_keys = [
        str(operation.get("id") or ""),
        str(operation.get("operation_key") or ""),
        str(operation.get("name") or ""),
        str(operation.get("path") or ""),
        "default",
    ]
    merged: dict[str, Any] = {}
    for key in operation_keys:
        value = samples.get(key)
        if isinstance(value, dict):
            merged.update(value)
    return merged


def _sample_value_for_parameter(
    parameter: dict[str, Any],
    operation_samples: dict[str, Any],
    source_config: dict[str, Any],
) -> str:
    name = str(parameter.get("name") or parameter.get("raw_name") or "")
    if name in operation_samples and operation_samples[name] is not None:
        return str(operation_samples[name])
    metadata = parameter.get("metadata") if isinstance(parameter.get("metadata"), dict) else {}
    for key in ("sample_value", "example", "default"):
        if metadata.get(key) is not None and str(metadata.get(key)) != "":
            return str(metadata[key])
    if parameter.get("default_value") is not None and str(parameter.get("default_value")) != "":
        return str(parameter.get("default_value"))
    enum_values = parameter.get("enum_values") if isinstance(parameter.get("enum_values"), list) else []
    if enum_values:
        return str(enum_values[0])
    default_control = _default_control_sample(name)
    if default_control:
        return default_control
    if not parameter.get("is_required"):
        return ""
    if SECRET_NAME_PATTERN.search(name):
        for env_name in _secret_env_names(name, source_config):
            value = os.getenv(env_name, "")
            if value:
                return value
    return ""


def _default_control_sample(parameter_name: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "", parameter_name.lower())
    defaults = {
        "pageno": "1",
        "page": "1",
        "numofrows": "10",
        "pagesize": "10",
        "rows": "10",
        "resulttype": "json",
        "type": "json",
    }
    return defaults.get(normalized, "")


def _secret_env_names(parameter_name: str, source_config: dict[str, Any]) -> list[str]:
    verification = source_config.get("verification") if isinstance(source_config.get("verification"), dict) else {}
    secret_env = verification.get("secret_env") if isinstance(verification.get("secret_env"), dict) else {}
    names: list[str] = []
    configured = secret_env.get(parameter_name)
    if isinstance(configured, str) and configured:
        names.append(configured)
    normalized = re.sub(r"[^A-Za-z0-9]+", "_", parameter_name).upper()
    names.append(f"CONTEXT_PLATFORM_{normalized}")
    if parameter_name.lower() == "servicekey":
        names.append("CONTEXT_PLATFORM_SERVICE_KEY")
    return list(dict.fromkeys(names))


def _response_sample_ref(response: httpx.Response) -> dict[str, Any]:
    content_type = response.headers.get("content-type", "")
    max_body_chars = int(os.getenv("CONTEXT_PLATFORM_RESPONSE_PREVIEW_MAX_CHARS") or "65536")
    body = response.text[:max_body_chars] if response.text else ""
    return {
        "content_type": content_type,
        "body_preview": body,
        "body_truncated": len(response.text or "") > max_body_chars,
    }


def _response_field_coverage(fields: list[dict[str, Any]], response: httpx.Response) -> dict[str, Any]:
    expected_paths = [str(field.get("field_path") or "") for field in fields if isinstance(field, dict)]
    required_paths = [str(field.get("field_path") or "") for field in fields if isinstance(field, dict) and bool(field.get("is_required"))]
    observed_paths = _observed_response_paths(response)
    observed_suffixes = {_normalize_response_path(path) for path in observed_paths}
    matched = [path for path in expected_paths if _normalize_response_path(path) in observed_suffixes]
    missing_required = [path for path in required_paths if _normalize_response_path(path) not in observed_suffixes]
    return {
        "expected_output_paths": expected_paths,
        "observed_path_count": len(observed_paths),
        "matched_output_paths": matched,
        "missing_required_output_paths": missing_required,
    }


def _observed_response_paths(response: httpx.Response) -> list[str]:
    try:
        payload = response.json()
    except json.JSONDecodeError:
        return []
    paths: list[str] = []
    _flatten_json_paths(payload, "", paths)
    return paths


def _flatten_json_paths(value: Any, prefix: str, paths: list[str]) -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            child_prefix = f"{prefix}.{key}" if prefix else str(key)
            _flatten_json_paths(child, child_prefix, paths)
        return
    if isinstance(value, list):
        if value:
            _flatten_json_paths(value[0], f"{prefix}.item", paths)
        return
    if prefix:
        paths.append(prefix)


def _normalize_response_path(path: str) -> str:
    value = str(path or "")
    value = value.replace("[0]", ".item")
    value = value.replace("[]", ".item")
    value = re.sub(r"\.+", ".", value)
    normalized = value.strip(".").lower()
    if normalized.startswith("response.response."):
        normalized = normalized.removeprefix("response.")
    if normalized.startswith("response.body."):
        normalized = normalized.removeprefix("response.body.")
    normalized = normalized.replace(".item.item.", ".item.")
    return normalized


def _bindings_by_operation(binding_generation: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = {}
    for suggestion in binding_generation.get("suggestions", []):
        if not isinstance(suggestion, dict):
            continue
        operation_id = str(suggestion.get("source_operation_id") or "")
        if operation_id:
            result.setdefault(operation_id, []).append(suggestion)
    return result


def _verification_summary(checks: list[dict[str, Any]]) -> dict[str, Any]:
    statuses = {status: 0 for status in ["verified", "failed", "skipped", "needs_input"]}
    for check in checks:
        status = str(check.get("status") or "skipped")
        statuses[status] = statuses.get(status, 0) + 1
    return {
        "total": len(checks),
        **statuses,
    }


def _join_url(base_url: str, path: str) -> str:
    return urljoin(f"{base_url.rstrip('/')}/", path.lstrip("/"))


def _redact_mapping(values: dict[str, Any]) -> dict[str, Any]:
    return {
        key: "***REDACTED***" if SECRET_NAME_PATTERN.search(str(key)) else _redact_value(value)
        for key, value in values.items()
    }


def _redact_value(value: Any) -> Any:
    if isinstance(value, dict):
        return _redact_mapping(value)
    if isinstance(value, list):
        return [_redact_value(item) for item in value]
    return value
