from __future__ import annotations

import re
from datetime import datetime
from typing import Any


def build_transform_suggestion(
    mapping: dict[str, Any],
    semantic_type: dict[str, Any] | None,
    operation_field: dict[str, Any] | None,
) -> dict[str, Any]:
    samples = extract_transform_samples(mapping, operation_field)
    field_text = " ".join(
        str(value or "")
        for value in [
            mapping.get("field_path"),
            operation_field.get("raw_name") if operation_field else "",
            operation_field.get("display_name") if operation_field else "",
            operation_field.get("description") if operation_field else "",
            semantic_type.get("name") if semantic_type else "",
            semantic_type.get("description") if semantic_type else "",
            semantic_type.get("datatype") if semantic_type else "",
        ]
    ).lower()
    datatype = str((semantic_type or {}).get("datatype") or "").lower()
    suggestion = suggest_date_transform(samples, datatype, field_text)
    if suggestion is None:
        suggestion = suggest_number_transform(samples, datatype, field_text)
    if suggestion is None:
        suggestion = suggest_boolean_transform(samples, datatype, field_text)
    if suggestion is None:
        suggestion = {
            "transform_spec": {
                "kind": "identity",
                "empty_policy": "null",
                "invalid_policy": "keep",
            },
            "mapping_type": "exact",
            "mapping_kind": "direct",
            "confidence": 0.55 if samples else 0.35,
            "rationale": "No strong format mismatch was detected from the current semantic type and sample evidence.",
        }
    transform_spec = suggestion["transform_spec"]
    return {
        "mode": "deterministic_assist",
        "transform_spec": transform_spec,
        "mapping_type": suggestion["mapping_type"],
        "mapping_kind": suggestion["mapping_kind"],
        "enum_mapping": suggestion.get("enum_mapping", {}),
        "confidence": suggestion["confidence"],
        "rationale": suggestion["rationale"],
        "samples": samples,
        "preview": [preview_transform(value, transform_spec, suggestion.get("enum_mapping", {})) for value in samples[:5]],
    }


def suggest_semantic_types(operation_field: dict[str, Any], semantic_types: list[dict[str, Any]]) -> list[dict[str, Any]]:
    field_terms = tokenize_suggestion_text(
        " ".join(
            str(value or "")
            for value in [
                operation_field.get("raw_name"),
                operation_field.get("display_name"),
                operation_field.get("field_path"),
                operation_field.get("description"),
                operation_field.get("data_type"),
            ]
        )
    )
    scored: list[dict[str, Any]] = []
    for semantic_type in semantic_types:
        display = semantic_type.get("draft_snapshot") or semantic_type
        semantic_terms = tokenize_suggestion_text(
            " ".join(
                str(value or "")
                for value in [
                    display.get("name"),
                    display.get("urn"),
                    display.get("description"),
                    display.get("datatype"),
                    " ".join(display.get("aliases") or []),
                    " ".join(display.get("tags") or []),
                ]
            )
        )
        overlap = field_terms.intersection(semantic_terms)
        raw_name = str(operation_field.get("raw_name") or "").lower()
        semantic_name = str(display.get("name") or "").lower()
        boost = 0.0
        if raw_name and raw_name in semantic_name:
            boost += 0.25
        if semantic_name and semantic_name in raw_name:
            boost += 0.25
        score = min(0.98, 0.35 + (len(overlap) * 0.12) + boost)
        if overlap or boost:
            scored.append(
                {
                    "semantic_type_id": semantic_type["id"],
                    "name": display.get("name") or semantic_type["id"],
                    "datatype": display.get("datatype") or "string",
                    "description": display.get("description") or "",
                    "confidence": round(score, 2),
                    "rationale": f"Matched field evidence: {', '.join(sorted(overlap)[:5])}" if overlap else "Name similarity matched the source field.",
                }
            )
    if not scored:
        fallback = semantic_types[:3]
        return [
            {
                "semantic_type_id": item["id"],
                "name": (item.get("draft_snapshot") or item).get("name") or item["id"],
                "datatype": (item.get("draft_snapshot") or item).get("datatype") or "string",
                "description": (item.get("draft_snapshot") or item).get("description") or "",
                "confidence": 0.25,
                "rationale": "No strong semantic match was detected; review manually.",
            }
            for item in fallback
        ]
    return sorted(scored, key=lambda item: item["confidence"], reverse=True)[:5]


def tokenize_suggestion_text(value: str) -> set[str]:
    spaced = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", value)
    return {
        token.lower()
        for token in re.split(r"[^0-9A-Za-z가-힣]+", spaced)
        if len(token.strip()) >= 2
    }


def extract_transform_samples(mapping: dict[str, Any], operation_field: dict[str, Any] | None) -> list[str]:
    samples: list[str] = []
    for source in [mapping.get("evidence") or [], (operation_field or {}).get("evidence") or []]:
        if not isinstance(source, list):
            continue
        for item in source:
            if isinstance(item, dict):
                value = item.get("sample") or item.get("value") or item.get("example")
                if value is not None:
                    samples.append(str(value))
            elif item is not None:
                samples.append(str(item))
    metadata = (operation_field or {}).get("metadata") or {}
    for key in ("samples", "sample_values", "examples"):
        values = metadata.get(key)
        if isinstance(values, list):
            samples.extend(str(value) for value in values if value is not None)
    deduped: list[str] = []
    for sample in samples:
        normalized = sample.strip()
        if normalized and normalized not in deduped:
            deduped.append(normalized)
    return deduped[:10]


def suggest_date_transform(samples: list[str], datatype: str, field_text: str) -> dict[str, Any] | None:
    if "date" not in datatype and "time" not in datatype and not re.search(r"(date|datetime|dt|일자|날짜|시간)", field_text):
        return None
    sample = samples[0] if samples else ""
    compact_digits = re.sub(r"\D", "", sample)
    if len(compact_digits) == 14:
        input_format = "yyyyMMddHHmmss"
        output_format = "ISO_DATETIME"
        confidence = 0.9
    elif len(compact_digits) == 8:
        input_format = "yyyyMMdd"
        output_format = "ISO_DATE"
        confidence = 0.88
    elif re.fullmatch(r"\d{4}-\d{2}-\d{2}", sample):
        input_format = "yyyy-MM-dd"
        output_format = "ISO_DATE"
        confidence = 0.78
    else:
        input_format = "auto"
        output_format = "ISO_DATETIME" if "time" in datatype or "datetime" in field_text else "ISO_DATE"
        confidence = 0.62 if samples else 0.45
    return {
        "transform_spec": {
            "kind": "date_parse",
            "input_format": input_format,
            "output_format": output_format,
            "empty_policy": "null",
            "invalid_policy": "reject",
        },
        "mapping_type": "transform",
        "mapping_kind": "transform",
        "confidence": confidence,
        "rationale": "The target semantic type or field evidence indicates a date/time value.",
    }


def suggest_number_transform(samples: list[str], datatype: str, field_text: str) -> dict[str, Any] | None:
    numeric_target = any(token in datatype for token in ("number", "numeric", "integer", "float", "decimal", "amount"))
    numeric_target = numeric_target or bool(re.search(r"(amount|price|cost|amt|prce|금액|가격)", field_text))
    if not numeric_target:
        return None
    sample = samples[0] if samples else ""
    return {
        "transform_spec": {
            "kind": "number_parse",
            "thousands_separator": "," if "," in sample else "",
            "decimal_separator": ".",
            "empty_policy": "null",
            "invalid_policy": "reject",
        },
        "mapping_type": "transform",
        "mapping_kind": "transform",
        "confidence": 0.86 if samples else 0.5,
        "rationale": "The target semantic type or field evidence indicates a numeric value.",
    }


def suggest_boolean_transform(samples: list[str], datatype: str, field_text: str) -> dict[str, Any] | None:
    if "bool" not in datatype and not re.search(r"(flag|yn| 여부|여부)", field_text):
        return None
    values = {sample.strip() for sample in samples if sample.strip()}
    enum_mapping: dict[str, bool] = {}
    for value in values:
        lowered = value.lower()
        if lowered in {"y", "yes", "true", "1"}:
            enum_mapping[value] = True
        elif lowered in {"n", "no", "false", "0"}:
            enum_mapping[value] = False
    return {
        "transform_spec": {
            "kind": "enum_map",
            "empty_policy": "null",
            "invalid_policy": "reject",
        },
        "enum_mapping": enum_mapping,
        "mapping_type": "enum",
        "mapping_kind": "enum",
        "confidence": 0.82 if enum_mapping else 0.55,
        "rationale": "The target semantic type or field evidence indicates a boolean/flag value.",
    }


def preview_transform(value: str, transform_spec: dict[str, Any], enum_mapping: dict[str, Any]) -> dict[str, Any]:
    try:
        output: Any = apply_preview_transform(value, transform_spec, enum_mapping)
        return {"input": value, "output": output, "ok": True}
    except ValueError as exc:
        return {"input": value, "output": None, "ok": False, "error": str(exc)}


def apply_preview_transform(value: str, transform_spec: dict[str, Any], enum_mapping: dict[str, Any]) -> Any:
    kind = transform_spec.get("kind")
    if kind == "identity":
        return value
    if kind == "number_parse":
        normalized = value.strip()
        thousands_separator = str(transform_spec.get("thousands_separator") or "")
        if thousands_separator:
            normalized = normalized.replace(thousands_separator, "")
        try:
            return float(normalized) if "." in normalized else int(normalized)
        except ValueError as exc:
            raise ValueError("number_parse failed") from exc
    if kind == "enum_map":
        if value in enum_mapping:
            return enum_mapping[value]
        raise ValueError("enum value is not mapped")
    if kind == "date_parse":
        input_format = str(transform_spec.get("input_format") or "auto")
        output_format = str(transform_spec.get("output_format") or "ISO_DATE")
        parsed = parse_preview_date(value, input_format)
        return parsed.isoformat() if output_format == "ISO_DATETIME" else parsed.date().isoformat()
    return value


def parse_preview_date(value: str, input_format: str) -> datetime:
    formats = {
        "yyyyMMddHHmmss": "%Y%m%d%H%M%S",
        "yyyyMMdd": "%Y%m%d",
        "yyyy-MM-dd": "%Y-%m-%d",
    }
    candidates = list(formats.values()) if input_format == "auto" else [formats.get(input_format, input_format)]
    for candidate in candidates:
        try:
            return datetime.strptime(value.strip(), candidate)
        except ValueError:
            continue
    raise ValueError("date_parse failed")
