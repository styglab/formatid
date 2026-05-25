from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from typing import Any
from urllib import parse, request
from urllib.error import HTTPError, URLError
from xml.etree import ElementTree

from services.semantic_platform.lib.ingestion.state import SourceGraphState


def verify_endpoint_candidates(state: SourceGraphState) -> SourceGraphState:
    base_url = _candidate_base_url(state)
    checks = []
    for section in state.get("api_sections", []):
        if not section.get("path"):
            checks.append(_endpoint_candidate_not_run(section, "path_not_found"))
            continue
        if not base_url:
            checks.append(_endpoint_candidate_not_run(section, "base_url_not_found"))
            continue
        checks.append(_probe_endpoint_candidate(section, base_url, state))
    passed = {str(check.get("section_id") or "") for check in checks if check.get("status") == "passed"}
    return {
        **state,
        "endpoint_candidate_checks": checks,
        "verified_api_sections": [
            section for section in state.get("api_sections", []) if str(section.get("id") or "") in passed
        ],
    }


def verify_capabilities(state: SourceGraphState) -> SourceGraphState:
    manual = state.get("manual_llm_response") or {}
    provided_results = manual.get("verification_results")
    if isinstance(provided_results, list):
        return {**state, "verification_results": [item for item in provided_results if isinstance(item, dict)]}
    resources = {str(item.get("id") or ""): item for item in state.get("resources", [])}
    contracts = {str(item.get("operation_id") or ""): item for item in state.get("operation_contracts", [])}
    results = []
    for variant in state.get("operation_variants", []):
        verification = variant.get("verification") if isinstance(variant.get("verification"), dict) else {}
        if not verification.get("safe_to_call"):
            results.append(_verification_not_run(variant, "verification_not_marked_safe_to_call"))
            continue
        sample_arguments = verification.get("sample_semantic_arguments")
        if not isinstance(sample_arguments, dict):
            results.append(_verification_not_run(variant, "verification_sample_not_supplied"))
            continue
        contract = contracts.get(str(variant.get("operation_id") or ""))
        if not contract:
            results.append(_verification_not_run(variant, "operation_contract_not_found"))
            continue
        resource = resources.get(str(contract.get("resource_id") or ""))
        results.append(_verify_http_variant(variant, contract, resource, sample_arguments))
    return {**state, "verification_results": results}


def _verification_not_run(variant: dict[str, Any], reason: str) -> dict[str, Any]:
    return {
        "variant_id": variant.get("variant_id"),
        "operation_id": variant.get("operation_id"),
        "capability_id": variant.get("capability_id") or variant.get("capability"),
        "status": "not_run",
        "reason": reason,
    }


def _candidate_base_url(state: SourceGraphState) -> str | None:
    manual = state.get("manual_llm_response") or {}
    for resource in _list(manual.get("resources")):
        base_url = resource.get("base_url")
        if base_url:
            return str(base_url)
    urls = []
    for example in state.get("structured_evidence", {}).get("example_candidates", []):
        text = str(example.get("text") or "")
        for match in re.finditer(r"https?://[^\s|<>\"]+", text):
            urls.append(match.group(0))
    if not urls:
        return None
    return _base_url_from_examples(urls)


def _base_url_from_examples(urls: list[str]) -> str | None:
    prefixes: list[str] = []
    for url in urls:
        parsed = parse.urlparse(url)
        parts = [part for part in parsed.path.split("/") if part]
        if len(parts) >= 3:
            prefixes.append(f"{parsed.scheme}://{parsed.netloc}/" + "/".join(parts[:3]))
        elif parsed.scheme and parsed.netloc:
            prefixes.append(f"{parsed.scheme}://{parsed.netloc}")
    if not prefixes:
        return None
    return max(set(prefixes), key=prefixes.count)


def _endpoint_candidate_not_run(section: dict[str, Any], reason: str) -> dict[str, Any]:
    return {
        "section_id": section.get("id"),
        "operation_name": section.get("operation_name"),
        "method": section.get("method"),
        "path": section.get("path"),
        "status": "not_run",
        "reason": reason,
    }


def _probe_endpoint_candidate(section: dict[str, Any], base_url: str, state: SourceGraphState) -> dict[str, Any]:
    method = str(section.get("method") or "GET").upper()
    if method not in {"GET", "POST"}:
        return _endpoint_candidate_not_run(section, "unsupported_probe_method")
    raw_arguments = _candidate_probe_arguments(state)
    raw_body = _candidate_probe_body(section) if method == "POST" else {}
    url = _join_url(base_url, str(section.get("path") or ""))
    full_url = f"{url}?{parse.urlencode(raw_arguments, doseq=True)}"
    started = datetime.now(timezone.utc)
    try:
        http_request: str | request.Request
        if method == "POST":
            http_request = request.Request(
                full_url,
                data=json.dumps(raw_body, ensure_ascii=False).encode("utf-8"),
                headers={"Content-Type": "application/json", "Accept": "application/json"},
                method="POST",
            )
        else:
            http_request = full_url
        with request.urlopen(http_request, timeout=_candidate_probe_timeout_seconds()) as response:
            body = response.read()
            content_type = response.headers.get("content-type", "")
        provider_status, provider_message, status = _candidate_probe_status(body, content_type)
        return {
            "section_id": section.get("id"),
            "operation_name": section.get("operation_name"),
            "method": method,
            "path": section.get("path"),
            "status": status,
            "provider_status": provider_status,
            "message": provider_message,
            "request": {"url": url, "method": method, "arguments": _redact(raw_arguments), "body": _redact(raw_body)},
            "response_sample": body[:2000].decode("utf-8", errors="ignore"),
            "checked_at": started.isoformat(),
        }
    except HTTPError as exc:
        if exc.code in {401, 403}:
            return _endpoint_candidate_inconclusive(section, "auth_required", str(exc), url, raw_arguments, started, raw_body)
        return _endpoint_candidate_failed(section, "http_error", str(exc), url, raw_arguments, started, raw_body)
    except TimeoutError as exc:
        return _endpoint_candidate_inconclusive(section, "timeout", str(exc), url, raw_arguments, started, raw_body)
    except URLError as exc:
        if "timed out" in str(exc).lower():
            return _endpoint_candidate_inconclusive(section, "timeout", str(exc), url, raw_arguments, started, raw_body)
        return _endpoint_candidate_failed(section, "transport_error", str(exc), url, raw_arguments, started, raw_body)
    except ValueError as exc:
        return _endpoint_candidate_failed(section, "transport_error", str(exc), url, raw_arguments, started, raw_body)


def _candidate_probe_status(body: bytes, content_type: str) -> tuple[str, str, str]:
    text = body.decode("utf-8", errors="ignore")
    stripped = text.lstrip()
    if not stripped:
        return "empty", "empty response body", "failed"
    if "json" in content_type.lower() or stripped.startswith("{") or stripped.startswith("["):
        try:
            json.loads(text)
        except json.JSONDecodeError as exc:
            return "invalid_json", f"unparseable response body: {exc}", "failed"
        return "http_success", "probe returned parseable JSON", "passed"
    if stripped.startswith("<"):
        try:
            ElementTree.fromstring(body)
        except ElementTree.ParseError as exc:
            return "invalid_xml", f"unparseable response body: {exc}", "failed"
        return "http_success", "probe returned parseable XML", "passed"
    return "http_success", "probe returned a non-empty response body", "passed"


def _candidate_probe_arguments(state: SourceGraphState) -> dict[str, Any]:
    try:
        configured = json.loads(os.getenv("SEMANTIC_PLATFORM_CANDIDATE_PROBE_ARGUMENTS", "{}"))
    except json.JSONDecodeError:
        configured = {}
    arguments = configured if isinstance(configured, dict) else {}
    key = _candidate_service_key(state)
    if key:
        arguments[_candidate_service_key_parameter(state)] = key
    return arguments


def _candidate_service_key(state: SourceGraphState) -> str | None:
    for name in _candidate_source_env_names(state, "SEMANTIC_PLATFORM_CANDIDATE_SERVICE_KEY"):
        value = os.getenv(name)
        if value:
            return value
    return os.getenv("SEMANTIC_PLATFORM_CANDIDATE_SERVICE_KEY")


def _candidate_service_key_parameter(state: SourceGraphState) -> str:
    for name in _candidate_source_env_names(state, "SEMANTIC_PLATFORM_CANDIDATE_SERVICE_KEY_PARAMETER"):
        value = os.getenv(name)
        if value:
            return value
    return os.getenv("SEMANTIC_PLATFORM_CANDIDATE_SERVICE_KEY_PARAMETER", "ServiceKey")


def _candidate_source_env_names(state: SourceGraphState, prefix: str) -> list[str]:
    document = state.get("source_document", {})
    sha = str(document.get("sha256") or "")
    source_id = str(document.get("id") or "")
    names = []
    if sha:
        names.append(f"{prefix}_{sha[:8].upper()}")
    match = re.match(r"source\.([0-9a-fA-F]{8})\.", source_id)
    if match:
        names.append(f"{prefix}_{match.group(1).upper()}")
    return list(dict.fromkeys(names))


def _candidate_probe_body(section: dict[str, Any]) -> dict[str, Any]:
    bodies = _json_env("SEMANTIC_PLATFORM_CANDIDATE_PROBE_BODIES", {})
    if isinstance(bodies, dict):
        operation_name = str(section.get("operation_name") or "")
        path = str(section.get("path") or "")
        for key in (operation_name, path):
            value = bodies.get(key)
            if isinstance(value, dict):
                return value
    body = _json_env("SEMANTIC_PLATFORM_CANDIDATE_PROBE_JSON_BODY", {})
    return body if isinstance(body, dict) else {}


def _json_env(name: str, default: Any) -> Any:
    try:
        return json.loads(os.getenv(name, ""))
    except (TypeError, json.JSONDecodeError):
        return default


def _candidate_probe_timeout_seconds() -> float:
    try:
        return float(os.getenv("SEMANTIC_PLATFORM_CANDIDATE_PROBE_TIMEOUT_SECONDS", "5"))
    except ValueError:
        return 5.0


def _endpoint_candidate_inconclusive(
    section: dict[str, Any],
    reason: str,
    message: str,
    url: str,
    raw_arguments: dict[str, Any],
    checked_at: datetime,
    raw_body: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "section_id": section.get("id"),
        "operation_name": section.get("operation_name"),
        "method": section.get("method"),
        "path": section.get("path"),
        "status": "inconclusive",
        "reason": reason,
        "message": message,
        "request": {
            "url": url,
            "method": section.get("method"),
            "arguments": _redact(raw_arguments),
            "body": _redact(raw_body or {}),
        },
        "checked_at": checked_at.isoformat(),
    }


def _endpoint_candidate_failed(
    section: dict[str, Any],
    reason: str,
    message: str,
    url: str,
    raw_arguments: dict[str, Any],
    checked_at: datetime,
    raw_body: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "section_id": section.get("id"),
        "operation_name": section.get("operation_name"),
        "method": section.get("method"),
        "path": section.get("path"),
        "status": "failed",
        "reason": reason,
        "message": message,
        "request": {
            "url": url,
            "method": section.get("method"),
            "arguments": _redact(raw_arguments),
            "body": _redact(raw_body or {}),
        },
        "checked_at": checked_at.isoformat(),
    }


def _verify_http_variant(
    variant: dict[str, Any],
    contract: dict[str, Any],
    resource: dict[str, Any] | None,
    sample_semantic_arguments: dict[str, Any],
) -> dict[str, Any]:
    started = datetime.now(timezone.utc)
    if not isinstance(resource, dict) or not resource.get("base_url"):
        return {**_verification_not_run(variant, "resource_base_url_not_found"), "checked_at": started.isoformat()}
    method = str(contract.get("method") or "GET").upper()
    if method not in {"GET", "POST"}:
        return {**_verification_not_run(variant, "unsupported_verification_method"), "checked_at": started.isoformat()}
    request_parts = _raw_arguments_by_contract_section(sample_semantic_arguments, contract)
    raw_arguments = {**(variant.get("fixed_raw_arguments") or {}), **request_parts["query"]}
    raw_arguments = _with_auth(raw_arguments, contract)
    raw_body = request_parts["body"]
    raw_headers = {str(key): str(value) for key, value in request_parts["header"].items()}
    url = _join_url(str(resource.get("base_url") or ""), str(contract.get("path") or ""))
    full_url = f"{url}?{parse.urlencode(raw_arguments, doseq=True)}"
    try:
        http_request: str | request.Request
        if method == "POST":
            headers = {"Content-Type": "application/json", "Accept": "application/json", **raw_headers}
            http_request = request.Request(
                full_url,
                data=json.dumps(raw_body, ensure_ascii=False).encode("utf-8"),
                headers=headers,
                method="POST",
            )
        else:
            http_request = full_url
        with request.urlopen(http_request, timeout=15) as response:
            body = response.read()
            content_type = response.headers.get("content-type", "")
        status, provider_message, result_status = _provider_status(body, content_type, contract)
        return {
            "variant_id": variant.get("variant_id"),
            "operation_id": variant.get("operation_id"),
            "capability_id": variant.get("capability_id") or variant.get("capability"),
            "status": result_status,
            "provider_status": status,
            "message": provider_message,
            "request": {
                "url": url,
                "method": method,
                "arguments": _redact(raw_arguments),
                "body": _redact(raw_body),
                "headers": _redact(raw_headers),
            },
            "response_sample": body[:4000].decode("utf-8", errors="ignore"),
            "checked_at": started.isoformat(),
        }
    except HTTPError as exc:
        return _verification_failed(variant, "http_error", str(exc), raw_arguments, url, started, method, raw_body, raw_headers)
    except (TimeoutError, URLError, ValueError) as exc:
        return _verification_failed(variant, "transport_error", str(exc), raw_arguments, url, started, method, raw_body, raw_headers)


def _raw_arguments_by_contract_section(
    semantic_arguments: dict[str, Any],
    contract: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    request_contract = contract.get("request") if isinstance(contract.get("request"), dict) else {}
    raw_arguments: dict[str, dict[str, Any]] = {"query": {}, "body": {}, "path": {}, "header": {}}
    for section in ("query", "body", "path", "header"):
        fields = request_contract.get(section)
        if not isinstance(fields, dict):
            continue
        for raw_name, field in fields.items():
            if not isinstance(field, dict):
                continue
            semantic_type = str(field.get("semantic_type") or "")
            if semantic_type in semantic_arguments:
                raw_arguments[section][str(raw_name)] = _contract_value(
                    semantic_arguments[semantic_type],
                    str(field.get("transform") or ""),
                )
            elif "default" in field:
                raw_arguments[section][str(raw_name)] = field.get("default")
    return {
        section: {key: value for key, value in values.items() if value not in (None, "")}
        for section, values in raw_arguments.items()
    }


def _contract_value(value: Any, transform: str) -> Any:
    if isinstance(value, dict):
        if transform == "date_start":
            return value.get("from") or value.get("start")
        if transform == "date_end":
            return value.get("to") or value.get("end")
    return value


def _with_auth(raw_arguments: dict[str, Any], contract: dict[str, Any]) -> dict[str, Any]:
    arguments = dict(raw_arguments)
    auth = contract.get("auth") if isinstance(contract.get("auth"), dict) else {}
    parameter = str(auth.get("parameter") or "ServiceKey")
    if parameter not in arguments:
        key = _api_key(auth)
        if key:
            arguments[parameter] = key
    return arguments


def _api_key(auth: dict[str, Any]) -> str | None:
    names = auth.get("env_names") if isinstance(auth.get("env_names"), list) else []
    for name in [str(value) for value in names]:
        value = os.getenv(name)
        if value:
            return value
    return None


def _join_url(base_url: str, path: str) -> str:
    return base_url.rstrip("/") + "/" + path.lstrip("/")


def _provider_status(body: bytes, content_type: str, contract: dict[str, Any]) -> tuple[str, str, str]:
    text = body.decode("utf-8", errors="ignore")
    stripped = text.lstrip()
    if not stripped:
        return "unknown", "empty response body", "failed"
    if "json" in content_type.lower() or stripped.startswith("{") or stripped.startswith("["):
        payload = json.loads(text)
        return _provider_status_from_contract(payload, contract)
    try:
        root = ElementTree.fromstring(body)
    except ElementTree.ParseError as exc:
        return "unknown", f"unparseable response body: {exc}", "failed"
    return "OK", root.findtext(".//resultMsg") or root.findtext(".//returnAuthMsg") or "", "passed"


def _provider_status_from_contract(payload: Any, contract: dict[str, Any]) -> tuple[str, str, str]:
    response = contract.get("response") if isinstance(contract.get("response"), dict) else {}
    error = response.get("error") if isinstance(response.get("error"), dict) else {}
    success = response.get("success") if isinstance(response.get("success"), dict) else {}
    if error:
        code = _first_path_value(payload, str(error.get("code_path") or ""))
        message = _first_path_value(payload, str(error.get("message_path") or ""))
        if "equals" in error and str(code) == str(error.get("equals")):
            return str(code), str(message or "declared error condition matched"), "failed"
        if "not_equals" in error and str(code) != str(error.get("not_equals")):
            return str(code), str(message or "declared error condition matched"), "failed"
    if success:
        code = _first_path_value(payload, str(success.get("path") or ""))
        message = _first_path_value(payload, str(success.get("message_path") or ""))
        if "equals" in success:
            passed = str(code) == str(success.get("equals"))
            return str(code), str(message or ""), "passed" if passed else "failed"
        allowed = success.get("in")
        if isinstance(allowed, list):
            passed = str(code) in {str(item) for item in allowed}
            return str(code), str(message or ""), "passed" if passed else "failed"
    return "OK", "", "passed"


def _first_path_value(payload: Any, path: str) -> Any:
    if not path:
        return None
    values = _path_values(payload, path)
    return values[0] if values else None


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


def _verification_failed(
    variant: dict[str, Any],
    reason: str,
    message: str,
    raw_arguments: dict[str, Any],
    url: str,
    checked_at: datetime,
    method: str = "GET",
    raw_body: dict[str, Any] | None = None,
    raw_headers: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "variant_id": variant.get("variant_id"),
        "operation_id": variant.get("operation_id"),
        "capability_id": variant.get("capability_id") or variant.get("capability"),
        "status": "failed",
        "reason": reason,
        "message": message,
        "request": {
            "url": url,
            "method": method,
            "arguments": _redact(raw_arguments),
            "body": _redact(raw_body or {}),
            "headers": _redact(raw_headers or {}),
        },
        "checked_at": checked_at.isoformat(),
    }


def _redact(arguments: dict[str, Any]) -> dict[str, Any]:
    redacted = {}
    for key, value in arguments.items():
        if isinstance(value, dict):
            redacted[key] = _redact(value)
        elif isinstance(value, list):
            redacted[key] = [_redact(item) if isinstance(item, dict) else item for item in value]
        else:
            redacted[key] = "***" if key.lower() in {"servicekey", "service_key", "key", "authorization"} else value
    return redacted


def _list(value: Any) -> list[dict[str, Any]]:
    return [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []
