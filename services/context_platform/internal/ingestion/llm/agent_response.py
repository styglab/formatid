from __future__ import annotations

import re
from typing import Any

from pydantic import BaseModel


SCHEMA_CONSTRAINT_KEYS = {
    "cardinality",
    "constraints",
    "datatype",
    "default",
    "enum",
    "enum_values",
    "examples",
    "maximum",
    "minimum",
    "pattern",
    "regex",
    "required",
    "structured_pattern",
    "validation",
    "validation_rules",
}

VALID_CONCEPT_KINDS = {
    "object_concept",
    "metric_concept",
    "identifier_concept",
    "status_concept",
    "value_concept",
    "unit_concept",
    "time_concept",
    "account_concept",
    "document_concept",
    "operation_concept",
}

WIRE_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
WIRE_PATH_SEGMENT_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(?:\[\])?$")
HANGUL_PATTERN = re.compile(r"[가-힣]")
API_FIELD_PATH_PREFIXES = ("request.", "response.")
SECRET_SAMPLE_PARAMETER_PATTERN = re.compile(
    r"(api[-_]?key|servicekey|service_key|token|secret|password|authorization|auth)",
    re.IGNORECASE,
)


class AgentResponseValidationError(ValueError):
    pass


class _FlexibleModel(BaseModel):
    class Config:
        extra = "allow"


class MeaningResolutionArtifactModel(_FlexibleModel):
    concept_decisions: list[dict[str, Any]] = []
    representation_decisions: list[dict[str, Any]] = []
    representation_schema_decisions: list[dict[str, Any]] = []
    value_domain_decisions: list[dict[str, Any]] = []
    relation_suggestions: list[dict[str, Any]] = []
    decisions: list[dict[str, Any]] = []
    meaning_decisions: list[dict[str, Any]] = []


class ResolutionGenerationArtifactModel(_FlexibleModel):
    field_bindings: list[dict[str, Any]] = []
    context_bindings: list[dict[str, Any]] = []
    parameter_bindings: list[dict[str, Any]] = []
    transform_rules: list[dict[str, Any]] = []
    suggestions: list[dict[str, Any]] = []
    resolution_suggestions: list[dict[str, Any]] = []


class CapabilityGenerationArtifactModel(_FlexibleModel):
    suggestions: list[dict[str, Any]] = []
    capability_contracts: list[dict[str, Any]] = []
    capabilities: list[dict[str, Any]] = []


class AgentResponseArtifactModel(_FlexibleModel):
    operation_candidates: list[dict[str, Any]] = []
    field_candidates: list[dict[str, Any]] = []
    meaning_resolution: MeaningResolutionArtifactModel | None = None
    resolution_generation: ResolutionGenerationArtifactModel | None = None
    capability_generation: CapabilityGenerationArtifactModel | None = None
    canonical_reconciliation: MeaningResolutionArtifactModel | None = None
    binding_generation: ResolutionGenerationArtifactModel | None = None
    capability_contracting: CapabilityGenerationArtifactModel | None = None
    verification: dict[str, Any] = {}


def validate_agent_response_artifact(payload: dict[str, Any]) -> None:
    if not isinstance(payload, dict):
        raise AgentResponseValidationError("agent response must be a JSON object")
    try:
        AgentResponseArtifactModel(**payload)
    except Exception as exc:  # pragma: no cover - pydantic version dependent message
        raise AgentResponseValidationError(f"agent response schema validation failed: {exc}") from exc

    _validate_source_contract(payload)
    for active_key, legacy_key in (
        ("meaning_resolution", "canonical_reconciliation"),
        ("resolution_generation", "binding_generation"),
        ("capability_generation", "capability_contracting"),
    ):
        stage_payload = payload.get(active_key)
        if stage_payload is None:
            stage_payload = payload.get(legacy_key)
        if stage_payload is not None:
            validate_manual_stage_response(active_key, stage_payload)
    _validate_verification(payload.get("verification"))


def _validate_source_contract(payload: dict[str, Any]) -> None:
    operation_candidates = payload.get("operation_candidates") if isinstance(payload.get("operation_candidates"), list) else []
    field_candidates = _source_contract_field_candidates(payload)
    source_structure = payload.get("source_structure") if isinstance(payload.get("source_structure"), dict) else {}
    is_api_contract = bool(operation_candidates) or any(
        isinstance(item, dict) and _is_api_field_path(str(item.get("field_path") or ""))
        for item in field_candidates
    )
    if not is_api_contract:
        return
    for index, item in enumerate(field_candidates):
        if not isinstance(item, dict):
            continue
        scope = str(item.get("scope") or "output")
        if scope not in {"input", "output", "control", "transport", "envelope"}:
            raise AgentResponseValidationError(
                f"field_candidates[{index}] has invalid scope {scope!r}; use input, output, control, transport, or envelope"
            )
        raw_name = str(item.get("raw_name") or item.get("wire_name") or "")
        wire_name = str(item.get("wire_name") or raw_name)
        if not wire_name:
            raise AgentResponseValidationError(f"field_candidates[{index}] must include wire_name or raw_name")
        for key, value in (("wire_name", wire_name), ("raw_name", raw_name)):
            if value and not WIRE_NAME_PATTERN.fullmatch(value):
                raise AgentResponseValidationError(
                    f"field_candidates[{index}].{key}={value!r} is not an executable API wire key; "
                    "put Korean or display labels in label_ko/label_en"
                )
            if HANGUL_PATTERN.search(value):
                raise AgentResponseValidationError(
                    f"field_candidates[{index}].{key} contains Korean label text; "
                    "API source raw_name/wire_name must be the actual wire key"
                )
        field_path = str(item.get("field_path") or "")
        if not _is_api_field_path(field_path):
            raise AgentResponseValidationError(
                f"field_candidates[{index}].field_path must start with request. or response. for API source contracts"
            )
        _validate_wire_field_path(field_path, index)
        if wire_name not in _field_path_segments(field_path):
            raise AgentResponseValidationError(
                f"field_candidates[{index}].field_path={field_path!r} must include its wire_name {wire_name!r}"
            )


def _source_contract_field_candidates(payload: dict[str, Any]) -> list[dict[str, Any]]:
    fields: list[dict[str, Any]] = []
    if isinstance(payload.get("field_candidates"), list):
        fields.extend(item for item in payload.get("field_candidates") or [] if isinstance(item, dict))
    source_structure = payload.get("source_structure") if isinstance(payload.get("source_structure"), dict) else {}
    for key in ("field_candidates", "fields"):
        if isinstance(source_structure.get(key), list):
            fields.extend(item for item in source_structure.get(key) or [] if isinstance(item, dict))
    operations = source_structure.get("operations") if isinstance(source_structure.get("operations"), list) else []
    if not operations and isinstance(payload.get("operation_candidates"), list):
        operations = payload.get("operation_candidates") or []
    for operation in operations:
        if not isinstance(operation, dict):
            continue
        for key in ("parameters", "request_fields", "response_fields", "fields"):
            values = operation.get(key)
            if isinstance(values, list):
                fields.extend(item for item in values if isinstance(item, dict))
    return fields


def _is_api_field_path(value: str) -> bool:
    return value.startswith(API_FIELD_PATH_PREFIXES)


def _validate_wire_field_path(field_path: str, index: int) -> None:
    if HANGUL_PATTERN.search(field_path):
        raise AgentResponseValidationError(
            f"field_candidates[{index}].field_path contains Korean label text; use wire-key path segments only"
        )
    for segment in _field_path_segments(field_path):
        if segment in {"request", "response", "query", "body", "header", "path", "items", "item"}:
            continue
        if not WIRE_PATH_SEGMENT_PATTERN.fullmatch(segment):
            raise AgentResponseValidationError(
                f"field_candidates[{index}].field_path contains non-wire segment {segment!r}"
            )


def _field_path_segments(field_path: str) -> list[str]:
    return [segment for segment in re.split(r"[.]+", field_path.strip(".")) if segment]


def validate_manual_stage_response(stage: str, payload: dict[str, Any]) -> None:
    if not isinstance(payload, dict):
        raise AgentResponseValidationError(f"{stage} response must be a JSON object")
    if stage in {"meaning_resolution", "canonical_reconciliation"}:
        _validate_model(MeaningResolutionArtifactModel, payload, stage)
        _validate_meaning_resolution(payload, stage)
        return
    if stage in {"resolution_generation", "binding_generation"}:
        _validate_model(ResolutionGenerationArtifactModel, payload, stage)
        _validate_resolution_generation(payload, stage)
        return
    if stage in {"capability_generation", "capability_contracting"}:
        _validate_model(CapabilityGenerationArtifactModel, payload, stage)
        _validate_capability_generation(payload, stage)
        return
    raise AgentResponseValidationError(f"unknown agent response stage: {stage}")


def _validate_model(model: type[BaseModel], payload: dict[str, Any], stage: str) -> None:
    try:
        model(**payload)
    except Exception as exc:  # pragma: no cover - pydantic version dependent message
        raise AgentResponseValidationError(f"{stage} schema validation failed: {exc}") from exc


def _validate_meaning_resolution(payload: dict[str, Any], stage: str) -> None:
    concept_items = _items(payload, "concept_decisions")
    if not concept_items:
        concept_items = _items(payload, "meaning_decisions") or _items(payload, "decisions")
    concept_key_shapes: dict[str, dict[str, Any]] = {}
    for index, item in enumerate(concept_items):
        bad_keys = _schema_keys_in(item)
        concept = item.get("concept") if isinstance(item.get("concept"), dict) else {}
        bad_keys.update(_schema_keys_in(concept))
        if bad_keys:
            raise AgentResponseValidationError(
                f"{stage}.concept_decisions[{index}] contains representation schema keys "
                f"{sorted(bad_keys)}; put datatype/regex/enum/validation in representation_schema_decisions"
            )
        concept_key = str(item.get("concept_key") or concept.get("stable_key") or concept.get("concept_key") or "")
        kind = str(concept.get("kind") or item.get("kind") or "")
        if concept_key and kind and kind not in VALID_CONCEPT_KINDS:
            raise AgentResponseValidationError(
                f"{stage}.concept_decisions[{index}] concept {concept_key} has invalid kind {kind!r}; "
                f"must be one of {sorted(VALID_CONCEPT_KINDS)}"
            )
        if concept_key:
            existing = concept_key_shapes.get(concept_key)
            current = {"kind": kind, "label_ko": concept.get("label_ko"), "label_en": concept.get("label_en")}
            if existing and existing.get("kind") and kind and existing.get("kind") != kind:
                raise AgentResponseValidationError(
                    f"{stage} defines concept {concept_key} with conflicting kinds "
                    f"{existing.get('kind')!r} and {kind!r}; reuse one stable Concept definition"
                )
            concept_key_shapes.setdefault(concept_key, current)
    for index, item in enumerate(_items(payload, "representation_schema_decisions")):
        if str(item.get("decision") or "create") in {"skip", "conflict"}:
            continue
        schema = item.get("representation_schema") if isinstance(item.get("representation_schema"), dict) else {}
        has_schema_signal = bool(schema) or bool(_schema_keys_in(item)) or bool(item.get("representation_schema_key") or item.get("schema_key"))
        if not has_schema_signal:
            raise AgentResponseValidationError(
                f"{stage}.representation_schema_decisions[{index}] must include representation_schema or schema constraint keys"
            )


def _validate_resolution_generation(payload: dict[str, Any], stage: str) -> None:
    for key in ("field_bindings", "context_bindings", "parameter_bindings"):
        for index, item in enumerate(_items(payload, key)):
            decision = str(item.get("decision") or "bind")
            if decision in {"skip_binding", "conflict"}:
                continue
            if key == "field_bindings" and (item.get("context_key") or item.get("fills_context_key")):
                raise AgentResponseValidationError(
                    f"{stage}.{key}[{index}] has context_key; move context values to context_bindings"
                )
            if key == "context_bindings" and not (item.get("context_key") or item.get("fills_context_key")):
                raise AgentResponseValidationError(f"{stage}.{key}[{index}] must include context_key")
            if key == "parameter_bindings" and not (item.get("required_concept_key") or item.get("required_concept") or item.get("concept_key")):
                raise AgentResponseValidationError(f"{stage}.{key}[{index}] must include required_concept_key or concept_key")
            if key == "field_bindings" and not (item.get("representation_key") or item.get("representation_id") or item.get("canonical_class_slot_id")):
                raise AgentResponseValidationError(f"{stage}.{key}[{index}] must include representation_key or representation_id")
            if key == "context_bindings" and not (item.get("representation_key") or item.get("representation_id") or item.get("canonical_class_slot_id")):
                raise AgentResponseValidationError(f"{stage}.{key}[{index}] must include target representation_key or representation_id")


def _validate_capability_generation(payload: dict[str, Any], stage: str) -> None:
    suggestions = _items(payload, "suggestions")
    if not suggestions:
        suggestions = _items(payload, "capability_contracts") or _items(payload, "capabilities")
    for suggestion_index, item in enumerate(suggestions):
        if str(item.get("decision") or "propose_capability") != "propose_capability":
            continue
        capability = item.get("capability") if isinstance(item.get("capability"), dict) else {}
        if not capability.get("capability_key"):
            raise AgentResponseValidationError(f"{stage}.suggestions[{suggestion_index}] proposed capability must include capability.capability_key")
        outputs = item.get("outputs") if isinstance(item.get("outputs"), list) else []
        output_concepts = {_concept_key(output) for output in outputs if isinstance(output, dict)}
        output_concepts.discard("")
        declared_outputs = set(_concept_keys(capability.get("provides_concepts")))
        intent_spec = capability.get("intent_spec") if isinstance(capability.get("intent_spec"), dict) else {}
        declared_outputs.update(_concept_keys(intent_spec.get("canonical_outputs")))
        missing_outputs = sorted(concept for concept in declared_outputs if concept not in output_concepts)
        if missing_outputs:
            raise AgentResponseValidationError(
                f"{stage}.suggestions[{suggestion_index}] declares output concepts without capability outputs: "
                f"{missing_outputs}"
            )
        for output_index, output in enumerate(outputs):
            if not isinstance(output, dict):
                continue
            if not output.get("output_key"):
                raise AgentResponseValidationError(
                    f"{stage}.suggestions[{suggestion_index}].outputs[{output_index}] must include output_key"
                )
            if not (output.get("concept_key") or output.get("canonical_ref")):
                raise AgentResponseValidationError(
                    f"{stage}.suggestions[{suggestion_index}].outputs[{output_index}] must include concept_key or canonical_ref"
                )


def _validate_verification(payload: Any) -> None:
    if payload is None:
        return
    if not isinstance(payload, dict):
        raise AgentResponseValidationError("verification must be a JSON object")
    sample_parameters = payload.get("sample_parameters")
    if isinstance(sample_parameters, dict):
        _validate_sample_parameters(sample_parameters)
    secret_env = payload.get("secret_env")
    if secret_env is None:
        return
    if not isinstance(secret_env, dict):
        raise AgentResponseValidationError("verification.secret_env must be a JSON object")
    for key, value in secret_env.items():
        if not isinstance(value, str) or not value:
            raise AgentResponseValidationError(f"verification.secret_env.{key} must name an environment variable")
        if any(token in value.lower() for token in ("secret=", "apikey=", "servicekey=", "token=")):
            raise AgentResponseValidationError(f"verification.secret_env.{key} must not contain a secret value")


def _validate_sample_parameters(payload: dict[str, Any]) -> None:
    for scope, values in payload.items():
        if isinstance(values, dict):
            for name, sample in values.items():
                if SECRET_SAMPLE_PARAMETER_PATTERN.search(str(name)):
                    raise AgentResponseValidationError(
                        f"verification.sample_parameters.{scope}.{name} must not contain secret-like parameters; "
                        "use verification.secret_env instead"
                    )
                if sample is None or str(sample) == "":
                    raise AgentResponseValidationError(f"verification.sample_parameters.{scope}.{name} must be non-empty")
            continue
        if SECRET_SAMPLE_PARAMETER_PATTERN.search(str(scope)):
            raise AgentResponseValidationError(
                f"verification.sample_parameters.{scope} must not contain secret-like parameters; "
                "use verification.secret_env instead"
            )


def _items(payload: dict[str, Any], key: str) -> list[dict[str, Any]]:
    items = payload.get(key)
    if not isinstance(items, list):
        return []
    return [item for item in items if isinstance(item, dict)]


def _schema_keys_in(payload: dict[str, Any]) -> set[str]:
    return {key for key in payload.keys() if key in SCHEMA_CONSTRAINT_KEYS}


def _concept_key(item: dict[str, Any]) -> str:
    return str(item.get("concept_key") or item.get("required_concept_key") or "")


def _concept_keys(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    keys: list[str] = []
    for item in value:
        if isinstance(item, str) and item:
            keys.append(item)
        elif isinstance(item, dict):
            key = _concept_key(item)
            if key:
                keys.append(key)
    return keys
