from __future__ import annotations

import re
from typing import Any


_HANGUL_PATTERN = re.compile(r"[가-힣]")
_WIRE_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_WIRE_PATH_SEGMENT_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(?:\[\])?$")


def validate_source_contract(parsed: dict[str, Any]) -> list[str]:
    """Validate that executable Source Graph records keep wire names separate from labels."""
    errors: list[str] = []
    operations = parsed.get("operations") if isinstance(parsed.get("operations"), list) else []
    for operation_index, operation in enumerate(operations):
        if not isinstance(operation, dict):
            continue
        operation_ref = str(operation.get("operation_key") or operation.get("name") or f"operation[{operation_index}]")
        method = str(operation.get("method") or "")
        path = str(operation.get("path") or "")
        if not method:
            errors.append(f"{operation_ref}: executable source operation is missing method")
        if not path:
            errors.append(f"{operation_ref}: executable source operation is missing path")
        for parameter in operation.get("parameters", []):
            if isinstance(parameter, dict):
                errors.extend(_validate_parameter(operation_ref, parameter))
        for field in operation.get("response_fields", []):
            if isinstance(field, dict):
                errors.extend(_validate_field(operation_ref, field))
    return errors


def _validate_parameter(operation_ref: str, parameter: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    name = str(parameter.get("name") or "")
    raw_name = str(parameter.get("raw_name") or name)
    parameter_path = str(parameter.get("parameter_path") or "")
    label = _label(parameter)
    if not name:
        errors.append(f"{operation_ref}: source parameter is missing executable name")
    elif not _WIRE_NAME_PATTERN.fullmatch(name):
        errors.append(
            f"{operation_ref}: source parameter `{name}` is not an executable wire/API key; "
            "use the actual request key as name/raw_name"
        )
    if raw_name and not _WIRE_NAME_PATTERN.fullmatch(raw_name):
        errors.append(
            f"{operation_ref}: source parameter raw_name `{raw_name}` is not an executable wire/API key"
        )
    if _contains_hangul(name) or _contains_hangul(raw_name):
        errors.append(
            f"{operation_ref}: source parameter `{name or raw_name}` looks like a Korean label; "
            "use the wire/API key as name/raw_name and put the Korean label in metadata.label_ko"
        )
    if parameter_path and not _valid_api_path(parameter_path):
        errors.append(
            f"{operation_ref}: source parameter path `{parameter_path}` is not an executable request path"
        )
    if parameter_path and _contains_hangul(_last_path_token(parameter_path)):
        errors.append(
            f"{operation_ref}: source parameter path `{parameter_path}` contains a Korean label; "
            "use an executable path such as request.query.crno"
        )
    if label and label in {name, raw_name, _last_path_token(parameter_path)}:
        errors.append(f"{operation_ref}: source parameter label `{label}` must not be reused as the executable name/path")
    return errors


def _validate_field(operation_ref: str, field: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    raw_name = str(field.get("raw_name") or "")
    field_path = str(field.get("field_path") or "")
    label = _label(field)
    if not raw_name:
        errors.append(f"{operation_ref}: source response field is missing raw_name")
    elif not _WIRE_NAME_PATTERN.fullmatch(raw_name):
        errors.append(
            f"{operation_ref}: source response field `{raw_name}` is not an executable wire/API key"
        )
    if not field_path:
        errors.append(f"{operation_ref}: source response field `{raw_name}` is missing field_path")
    elif not _valid_api_path(field_path):
        errors.append(
            f"{operation_ref}: source response field path `{field_path}` is not an executable response path"
        )
    if _contains_hangul(raw_name):
        errors.append(
            f"{operation_ref}: source response field `{raw_name}` looks like a Korean label; "
            "use the wire/API key as raw_name and put the Korean label in metadata.label_ko"
        )
    if field_path and _contains_hangul(_last_path_token(field_path)):
        errors.append(
            f"{operation_ref}: source response field path `{field_path}` contains a Korean label; "
            "use an executable path such as response.body.items.item.enpSaleAmt"
        )
    if label and label in {raw_name, _last_path_token(field_path)}:
        errors.append(f"{operation_ref}: source response field label `{label}` must not be reused as raw_name/path")
    return errors


def _label(payload: dict[str, Any]) -> str:
    metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
    return str(payload.get("label_ko") or metadata.get("label_ko") or "")


def _contains_hangul(value: str) -> bool:
    return bool(_HANGUL_PATTERN.search(value))


def _last_path_token(value: str) -> str:
    if not value:
        return ""
    return value.replace("[", ".").replace("]", "").split(".")[-1]


def _valid_api_path(value: str) -> bool:
    if not value.startswith(("request.", "response.")):
        return False
    for segment in [part for part in value.strip(".").split(".") if part]:
        if segment in {"request", "response", "query", "body", "header", "path", "items", "item"}:
            continue
        if not _WIRE_PATH_SEGMENT_PATTERN.fullmatch(segment):
            return False
    return True
