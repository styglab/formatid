from __future__ import annotations

import json
import re
from decimal import Decimal, InvalidOperation
from typing import Any


QUALITY_APPROVAL_READY = "approval_ready"
QUALITY_REVIEW_REQUIRED = "review_required"
QUALITY_BLOCKED = "blocked"

_QUALITY_RANK = {
    QUALITY_APPROVAL_READY: 0,
    QUALITY_REVIEW_REQUIRED: 1,
    QUALITY_BLOCKED: 2,
}

_TRANSIENT_ERROR_CATEGORIES = {
    "transient_timeout",
    "rate_limited",
    "upstream_5xx",
    "network_error",
}


def evaluate_ingestion_quality(
    *,
    operations: list[dict[str, Any]],
    document_fields: list[dict[str, Any]],
    canonical_reconciliation: dict[str, Any],
    binding_generation: dict[str, Any],
    capability_generation: dict[str, Any],
    verification_result: dict[str, Any],
) -> dict[str, Any]:
    executable = bool(operations)
    semantic_index = _semantic_index(canonical_reconciliation)
    observed_evidence = _observed_evidence_summary(verification_result)
    normalization_previews = _normalization_previews(capability_generation, verification_result)
    gates = [
        _source_shape_gate(operations, document_fields),
        _evidence_gate(executable, verification_result, observed_evidence),
        _meaning_gate(canonical_reconciliation, semantic_index),
        _binding_gate(binding_generation, semantic_index),
        _preview_gate(executable, capability_generation, normalization_previews, verification_result),
        _capability_gate(executable, capability_generation, semantic_index),
        _verification_gate(executable, verification_result),
    ]
    status = _worst_status(gate["status"] for gate in gates)
    return {
        "type": "ingestion_quality_gate",
        "quality_status": status,
        "publishable": status == QUALITY_APPROVAL_READY,
        "executable_source": executable,
        "observed_evidence": observed_evidence,
        "normalization_previews": normalization_previews,
        "gates": gates,
        "gate_counts": _gate_counts(gates),
        "principles": {
            "registry_publish_policy": "Only approval_ready proposals should be published to the registry.",
            "proposal_policy": "All ingestion outcomes, including blocked and review_required results, remain reviewable proposals.",
            "capability_policy": "Capability gates are required only for executable sources.",
            "preview_policy": "Bindings that cannot produce a normalization preview must not be approved without review.",
        },
    }


def _source_shape_gate(operations: list[dict[str, Any]], document_fields: list[dict[str, Any]]) -> dict[str, Any]:
    if not operations and not document_fields:
        return _gate(
            "source_shape",
            QUALITY_BLOCKED,
            "No operation or field shape was extracted.",
            [{"code": "missing_source_shape"}],
        )
    invalid_operations = [
        item.get("id") or item.get("path") or item.get("name")
        for item in operations
        if not item.get("method") or not item.get("path")
    ]
    if invalid_operations:
        return _gate(
            "source_shape",
            QUALITY_BLOCKED,
            "One or more source operations are missing method/path.",
            [{"code": "invalid_operation_shape", "operations": invalid_operations[:10]}],
        )
    return _gate(
        "source_shape",
        QUALITY_APPROVAL_READY,
        "Source shape is present.",
        [],
        {"operation_count": len(operations), "document_field_count": len(document_fields)},
    )


def _evidence_gate(
    executable: bool,
    verification_result: dict[str, Any],
    observed_evidence: dict[str, Any],
) -> dict[str, Any]:
    if not executable:
        return _gate(
            "evidence",
            QUALITY_REVIEW_REQUIRED,
            "Non-executable source has document evidence but no runtime/sample execution evidence.",
            [{"code": "non_executable_source"}],
            observed_evidence,
        )
    summary = verification_result.get("summary") if isinstance(verification_result.get("summary"), dict) else {}
    if int(summary.get("verified") or 0) > 0:
        status = QUALITY_APPROVAL_READY if int(summary.get("failed") or 0) == 0 else QUALITY_REVIEW_REQUIRED
        return _gate(status=status, key="evidence", message="Observed evidence was collected.", issues=[], details=observed_evidence)
    if _only_transient_verification_failures(verification_result):
        return _gate(
            "evidence",
            QUALITY_REVIEW_REQUIRED,
            "Observed evidence collection failed with transient verification errors.",
            [{"code": "observed_evidence_transient_failure"}],
            observed_evidence,
        )
    return _gate(
        "evidence",
        QUALITY_BLOCKED,
        "No executable operation produced observed evidence.",
        [{"code": "missing_observed_evidence"}],
        observed_evidence,
    )


def _meaning_gate(canonical_reconciliation: dict[str, Any], semantic_index: dict[str, Any]) -> dict[str, Any]:
    blocking_issues: list[dict[str, Any]] = []
    review_issues: list[dict[str, Any]] = []
    for item in canonical_reconciliation.get("decisions") or []:
        if not isinstance(item, dict):
            continue
        decision = str(item.get("decision") or "")
        if decision == "skip":
            continue
        if decision == "conflict":
            blocking_issues.append({"code": "meaning_conflict", "field_path": _decision_field_path(item)})
            continue
        for key in ("concept_key", "representation_key", "representation_schema_key"):
            if not item.get(key):
                blocking_issues.append({"code": f"missing_{key}", "field_path": _decision_field_path(item)})
        scope_issue = _concept_scope_issue(item)
        if scope_issue:
            review_issues.append({**scope_issue, "field_path": _decision_field_path(item)})
        code_issue = _code_schema_value_domain_issue(item, semantic_index)
        if code_issue:
            review_issues.append({**code_issue, "field_path": _decision_field_path(item)})
    if blocking_issues:
        return _gate("meaning", QUALITY_BLOCKED, "Meaning decisions have missing references.", blocking_issues + review_issues)
    if review_issues:
        return _gate("meaning", QUALITY_REVIEW_REQUIRED, "Meaning decisions need model consistency review.", review_issues)
    return _gate("meaning", QUALITY_APPROVAL_READY, "Meaning decisions reference concept, representation, and schema.", [])


def _binding_gate(binding_generation: dict[str, Any], semantic_index: dict[str, Any]) -> dict[str, Any]:
    blocking_issues: list[dict[str, Any]] = []
    review_issues: list[dict[str, Any]] = []
    for item in binding_generation.get("suggestions") or []:
        if not isinstance(item, dict):
            continue
        decision = str(item.get("decision") or "")
        if decision == "skip_binding":
            if item.get("concept_key") or item.get("representation_key") or item.get("canonical_ref", {}).get("slot_name"):
                blocking_issues.append({"code": "skip_binding_kept_target", "field_path": item.get("field_path")})
            continue
        if decision != "bind":
            continue
        binding_kind = str(item.get("binding_kind") or "")
        if binding_kind not in {"field", "context", "parameter"}:
            blocking_issues.append({"code": "invalid_binding_kind", "field_path": item.get("field_path"), "binding_kind": binding_kind})
        if binding_kind == "field" and item.get("context_key"):
            blocking_issues.append({"code": "field_binding_has_context_key", "field_path": item.get("field_path")})
        if binding_kind == "context" and not item.get("context_key"):
            blocking_issues.append({"code": "context_binding_missing_context_key", "field_path": item.get("field_path")})
        if binding_kind == "parameter" and not (item.get("required_concept_key") or item.get("concept_key")):
            blocking_issues.append({"code": "parameter_binding_missing_required_concept", "field_path": item.get("field_path")})
        for key in ("concept_key", "representation_key", "representation_schema_key"):
            if not item.get(key):
                blocking_issues.append({"code": f"binding_missing_{key}", "field_path": item.get("field_path")})
        code_issue = _code_schema_value_domain_issue(item, semantic_index)
        if code_issue:
            review_issues.append({**code_issue, "field_path": item.get("field_path")})
    if blocking_issues:
        return _gate("binding", QUALITY_BLOCKED, "Binding invariants failed.", blocking_issues + review_issues)
    if review_issues:
        return _gate("binding", QUALITY_REVIEW_REQUIRED, "Bindings need model consistency review.", review_issues)
    return _gate("binding", QUALITY_APPROVAL_READY, "Bindings satisfy value/context/parameter invariants.", [])


def _preview_gate(
    executable: bool,
    capability_generation: dict[str, Any],
    previews: list[dict[str, Any]],
    verification_result: dict[str, Any],
) -> dict[str, Any]:
    if not executable:
        return _gate(
            "normalization_preview",
            QUALITY_REVIEW_REQUIRED,
            "No runtime preview is expected for a non-executable source.",
            [{"code": "non_executable_source"}],
            {"preview_count": len(previews)},
        )
    proposed = [
        item
        for item in capability_generation.get("suggestions") or []
        if isinstance(item, dict) and item.get("decision") == "propose_capability"
    ]
    if not proposed:
        return _gate(
            "normalization_preview",
            QUALITY_REVIEW_REQUIRED,
            "No capability was proposed, so no capability-level preview was generated.",
            [{"code": "missing_capability"}],
        )
    failed = [item for item in previews if item.get("preview_status") in {"failed", "unavailable"}]
    partial = [item for item in previews if item.get("preview_status") == "partial"]
    if not failed and not partial and len(previews) >= len(proposed):
        return _gate(
            "normalization_preview",
            QUALITY_APPROVAL_READY,
            "Normalization previews were generated for proposed capabilities.",
            [],
            {"preview_count": len(previews)},
        )
    if _only_transient_verification_failures(verification_result):
        return _gate(
            "normalization_preview",
            QUALITY_REVIEW_REQUIRED,
            "Some previews are unavailable because endpoint verification had transient failures.",
            [{"code": "preview_unavailable_transient"}],
            {"preview_count": len(previews), "failed_preview_count": len(failed), "partial_preview_count": len(partial)},
        )
    return _gate(
        "normalization_preview",
        QUALITY_BLOCKED,
        "Normalization preview failed or is incomplete.",
        [{"code": "preview_failed", "failed_preview_count": len(failed), "partial_preview_count": len(partial)}],
        {"preview_count": len(previews)},
    )


def _capability_gate(executable: bool, capability_generation: dict[str, Any], semantic_index: dict[str, Any]) -> dict[str, Any]:
    if not executable:
        return _gate(
            "capability",
            QUALITY_REVIEW_REQUIRED,
            "Capability is optional for non-executable sources.",
            [{"code": "capability_not_required"}],
        )
    blocking_issues: list[dict[str, Any]] = []
    review_issues: list[dict[str, Any]] = []
    for item in capability_generation.get("suggestions") or []:
        if not isinstance(item, dict) or item.get("decision") != "propose_capability":
            continue
        capability_key = str((item.get("capability") or {}).get("capability_key") or item.get("source_operation_id") or "")
        link = item.get("operation_link") if isinstance(item.get("operation_link"), dict) else {}
        if not link.get("source_operation_id"):
            blocking_issues.append({"code": "capability_missing_source_operation", "capability_key": capability_key})
        if not link.get("binding_spec"):
            blocking_issues.append({"code": "capability_missing_binding_spec", "capability_key": capability_key})
        if not item.get("inputs"):
            blocking_issues.append({"code": "capability_missing_inputs", "capability_key": capability_key})
        if not item.get("outputs"):
            blocking_issues.append({"code": "capability_missing_outputs", "capability_key": capability_key})
        for role in ("inputs", "outputs"):
            for io in item.get(role) or []:
                if not isinstance(io, dict):
                    continue
                for key in ("concept_key", "representation_key", "representation_schema_key"):
                    if not io.get(key):
                        blocking_issues.append({"code": f"capability_{role[:-1]}_missing_{key}", "capability_key": capability_key, "io_key": io.get("input_key") or io.get("output_key")})
                if _canonical_ref_empty(io.get("canonical_ref")):
                    review_issues.append({"code": f"capability_{role[:-1]}_missing_canonical_ref", "capability_key": capability_key, "io_key": io.get("input_key") or io.get("output_key")})
                code_issue = _code_schema_value_domain_issue(io, semantic_index)
                if code_issue:
                    review_issues.append({**code_issue, "capability_key": capability_key, "io_key": io.get("input_key") or io.get("output_key")})
        subject_issue = _repeated_output_subject_context_issue(item)
        if subject_issue:
            review_issues.append({**subject_issue, "capability_key": capability_key})
        declared = set((item.get("capability") or {}).get("provides_concepts") or [])
        intent_outputs = ((item.get("capability") or {}).get("intent_spec") or {}).get("canonical_outputs") or []
        for output in intent_outputs:
            if isinstance(output, str):
                declared.add(output)
            elif isinstance(output, dict) and output.get("concept_key"):
                declared.add(output["concept_key"])
        output_concepts = {output.get("concept_key") for output in item.get("outputs") or [] if isinstance(output, dict)}
        missing_outputs = sorted(concept for concept in declared if concept not in output_concepts)
        if missing_outputs:
            blocking_issues.append({"code": "capability_declared_outputs_missing", "capability_key": capability_key, "concepts": missing_outputs})
    if not blocking_issues and not review_issues:
        return _gate("capability", QUALITY_APPROVAL_READY, "Capability contracts are executable and complete.", [])
    if blocking_issues:
        return _gate("capability", QUALITY_BLOCKED, "Capability contracts are incomplete.", blocking_issues + review_issues)
    return _gate("capability", QUALITY_REVIEW_REQUIRED, "Capability contracts need review.", review_issues)


def _verification_gate(executable: bool, verification_result: dict[str, Any]) -> dict[str, Any]:
    if not executable:
        return _gate("verification", QUALITY_REVIEW_REQUIRED, "No endpoint verification is required for this non-executable source.", [{"code": "not_applicable"}])
    summary = verification_result.get("summary") if isinstance(verification_result.get("summary"), dict) else {}
    failed = int(summary.get("failed") or 0)
    needs_input = int(summary.get("needs_input") or 0)
    verified = int(summary.get("verified") or 0)
    if failed == 0 and needs_input == 0 and verified > 0:
        return _gate("verification", QUALITY_APPROVAL_READY, "Endpoint and capability checks passed.", [], summary)
    categories = _verification_error_categories(verification_result)
    if failed and categories.issubset(_TRANSIENT_ERROR_CATEGORIES):
        return _gate("verification", QUALITY_REVIEW_REQUIRED, "Endpoint verification failed only with transient errors.", [{"code": "transient_verification_failure", "categories": sorted(categories)}], summary)
    if needs_input:
        return _gate("verification", QUALITY_REVIEW_REQUIRED, "Endpoint verification needs sample input.", [{"code": "verification_needs_input"}], summary)
    return _gate("verification", QUALITY_BLOCKED, "Endpoint verification failed.", [{"code": "verification_failed", "categories": sorted(categories)}], summary)


def _semantic_index(canonical_reconciliation: dict[str, Any]) -> dict[str, Any]:
    concepts_with_value_domain: set[str] = set()
    value_domain_by_concept: dict[str, str] = {}
    for item in canonical_reconciliation.get("value_domain_decisions") or []:
        if not isinstance(item, dict) or item.get("decision") == "skip":
            continue
        concept_key = str(item.get("concept_key") or "")
        value_domain_key = str(item.get("value_domain_key") or "")
        if concept_key and value_domain_key:
            concepts_with_value_domain.add(concept_key)
            value_domain_by_concept[concept_key] = value_domain_key
    return {
        "concepts_with_value_domain": concepts_with_value_domain,
        "value_domain_by_concept": value_domain_by_concept,
    }


def _concept_scope_issue(item: dict[str, Any]) -> dict[str, Any] | None:
    concept = item.get("concept") if isinstance(item.get("concept"), dict) else {}
    concept_key = str(item.get("concept_key") or concept.get("stable_key") or "")
    meaning_scope = str(item.get("meaning_scope") or concept.get("meaning_scope") or "")
    namespace = _concept_namespace(concept_key)
    if not namespace or not meaning_scope:
        return None
    if namespace == meaning_scope:
        return None
    if namespace in {"tax", "finance", "identifier", "time", "currency", "company", "person", "organization"}:
        return {
            "code": "concept_scope_mismatch",
            "concept_key": concept_key,
            "expected_scope": namespace,
            "actual_scope": meaning_scope,
        }
    return None


def _concept_namespace(concept_key: str) -> str:
    parts = str(concept_key or "").split(".")
    if len(parts) >= 3 and parts[0] == "concept":
        return parts[1]
    return ""


def _code_schema_value_domain_issue(item: dict[str, Any], semantic_index: dict[str, Any]) -> dict[str, Any] | None:
    schema_key = str(item.get("representation_schema_key") or "")
    if not _is_code_value_schema(schema_key):
        return None
    concept_key = str(item.get("concept_key") or item.get("required_concept_key") or "")
    if not concept_key:
        return None
    if item.get("value_domain_key") or item.get("value_domain_id"):
        return None
    enum_mapping = item.get("enum_mapping") if isinstance(item.get("enum_mapping"), dict) else {}
    if enum_mapping:
        return None
    if concept_key in semantic_index.get("concepts_with_value_domain", set()):
        return None
    return {
        "code": "code_schema_missing_value_domain",
        "concept_key": concept_key,
        "representation_schema_key": schema_key,
    }


def _is_code_value_schema(schema_key: str) -> bool:
    value = str(schema_key or "").lower()
    return value.endswith(".code") or value.endswith("_code")


def _repeated_output_subject_context_issue(capability: dict[str, Any]) -> dict[str, Any] | None:
    link = capability.get("operation_link") if isinstance(capability.get("operation_link"), dict) else {}
    spec = link.get("binding_spec") if isinstance(link.get("binding_spec"), dict) else {}
    outputs = [item for item in spec.get("outputs") or [] if isinstance(item, dict)]
    if not any(_is_repeated_field_path(str(item.get("field_path") or "")) for item in outputs):
        return None
    input_concepts = {
        str(item.get("concept_key") or item.get("required_concept_key") or "")
        for item in spec.get("inputs") or []
        if isinstance(item, dict)
    }
    has_identifier_input = any(concept.startswith("concept.identifier.") for concept in input_concepts)
    if not has_identifier_input:
        return None
    contexts = [item for item in spec.get("contexts") or [] if isinstance(item, dict)]
    for context in contexts:
        context_key = str(context.get("context_key") or "")
        concept_key = str(context.get("concept_key") or context.get("required_concept_key") or "")
        if context_key in {"subject", "subject_identifier", "entity_identifier", "record_identifier"}:
            return None
        if concept_key.startswith("concept.identifier."):
            return None
    return {
        "code": "repeated_output_missing_subject_context",
        "input_identifier_concepts": sorted(input_concepts),
    }


def _is_repeated_field_path(field_path: str) -> bool:
    return "[]" in field_path or ".item." in field_path or ".items." in field_path


def _canonical_ref_empty(value: Any) -> bool:
    if not isinstance(value, dict):
        return True
    return not str(value.get("class_name") or value.get("entity_name") or "").strip() and not str(value.get("slot_name") or "").strip()


def _observed_evidence_summary(verification_result: dict[str, Any]) -> dict[str, Any]:
    checks = _all_checks(verification_result)
    return {
        "type": "observed_evidence_collection",
        "check_count": len(checks),
        "verified_check_count": len([item for item in checks if item.get("status") == "verified"]),
        "failed_check_count": len([item for item in checks if item.get("status") == "failed"]),
        "sample_count": len([item for item in checks if _body_preview(item)]),
        "error_categories": sorted(_verification_error_categories(verification_result)),
        "samples": [
            {
                "source_operation_id": item.get("source_operation_id"),
                "capability_key": item.get("capability_key") or "",
                "check_type": item.get("check_type"),
                "status": item.get("status"),
                "http_status": item.get("http_status"),
                "content_type": (item.get("response_sample_ref") or {}).get("content_type") if isinstance(item.get("response_sample_ref"), dict) else "",
                "body_truncated": bool((item.get("response_sample_ref") or {}).get("body_truncated")) if isinstance(item.get("response_sample_ref"), dict) else False,
                "field_coverage": item.get("field_coverage") if isinstance(item.get("field_coverage"), dict) else {},
            }
            for item in checks[:20]
        ],
    }


def _normalization_previews(capability_generation: dict[str, Any], verification_result: dict[str, Any]) -> list[dict[str, Any]]:
    operation_checks = {
        str(item.get("source_operation_id") or ""): item
        for item in verification_result.get("operation_checks") or []
        if isinstance(item, dict)
    }
    previews: list[dict[str, Any]] = []
    for suggestion in capability_generation.get("suggestions") or []:
        if not isinstance(suggestion, dict) or suggestion.get("decision") != "propose_capability":
            continue
        source_operation_id = str(suggestion.get("source_operation_id") or "")
        operation_check = operation_checks.get(source_operation_id) or {}
        sample = _sample_json(operation_check)
        spec = (suggestion.get("operation_link") or {}).get("binding_spec") if isinstance(suggestion.get("operation_link"), dict) else {}
        if not isinstance(spec, dict) or sample is None or operation_check.get("status") != "verified":
            previews.append(
                {
                    "capability_key": (suggestion.get("capability") or {}).get("capability_key") or "",
                    "source_operation_id": source_operation_id,
                    "preview_status": "unavailable",
                    "reason": "verified_sample_not_available",
                }
            )
            continue
        output_values = [_preview_item(item, sample, role="output") for item in spec.get("outputs") or [] if isinstance(item, dict)]
        context_values = [_preview_item(item, sample, role="context") for item in spec.get("contexts") or [] if isinstance(item, dict)]
        missing = [item for item in output_values if not item.get("present")]
        errors = [item for item in output_values + context_values if item.get("error")]
        status = "generated"
        if errors:
            status = "failed"
        elif missing:
            status = "partial"
        previews.append(
            {
                "capability_key": (suggestion.get("capability") or {}).get("capability_key") or "",
                "source_operation_id": source_operation_id,
                "preview_status": status,
                "outputs": output_values,
                "contexts": context_values,
                "canonical_object_preview": _canonical_object_preview(output_values, context_values),
            }
        )
    return previews


def _preview_item(binding: dict[str, Any], sample: Any, *, role: str) -> dict[str, Any]:
    field_path = str(binding.get("field_path") or "")
    value, present = _extract_path(sample, field_path)
    normalized_value: Any = None
    error = ""
    if present:
        try:
            normalized_value = _normalize_value(value, binding)
        except (InvalidOperation, ValueError, TypeError) as exc:
            error = str(exc)
    return {
        "role": role,
        "field_path": field_path,
        "source_field_id": binding.get("source_field_id"),
        "concept_key": binding.get("concept_key"),
        "representation_key": binding.get("representation_key"),
        "representation_schema_key": binding.get("representation_schema_key"),
        "context_key": binding.get("context_key"),
        "present": present,
        "sample_value": value if present else None,
        "normalized_value": normalized_value,
        "error": error,
    }


def _canonical_object_preview(outputs: list[dict[str, Any]], contexts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    context_by_key = {str(item.get("context_key") or item.get("concept_key") or ""): item.get("normalized_value") for item in contexts if item.get("present")}
    return [
        {
            "object_type": "object.observation",
            "concept": item.get("concept_key"),
            "representation": item.get("representation_key"),
            "schema": item.get("representation_schema_key"),
            "value": item.get("normalized_value"),
            "context": context_by_key,
        }
        for item in outputs
        if item.get("present")
    ][:20]


def _normalize_value(value: Any, binding: dict[str, Any]) -> Any:
    schema_key = str(binding.get("representation_schema_key") or "").lower()
    rule = binding.get("normalization_rule") if isinstance(binding.get("normalization_rule"), dict) else {}
    transform = binding.get("transform_spec") if isinstance(binding.get("transform_spec"), dict) else {}
    rule_id = str(rule.get("rule_id") or transform.get("rule_id") or "").lower()
    target_type = str(transform.get("target_type") or "").lower()
    if "decimal" in schema_key or "amount" in schema_key or rule_id in {"parse_decimal", "decimal_string_to_money_amount"} or target_type == "decimal":
        return str(Decimal(str(value).replace(",", "").strip()))
    if "integer" in schema_key or target_type in {"integer", "int"}:
        return int(str(value).replace(",", "").strip())
    if "year" in schema_key or rule_id in {"parse_year", "year_to_time_interval"}:
        year = str(value).strip()
        if not re.fullmatch(r"\d{4}", year):
            raise ValueError(f"expected four digit year, got {value!r}")
        return {"start_date": f"{year}-01-01", "end_date": f"{year}-12-31", "temporal_precision": "year"}
    if "yyyymmdd" in schema_key or rule_id == "parse_yyyymmdd_date":
        date_value = str(value).strip()
        if len(date_value) == 8 and date_value.isdigit():
            return f"{date_value[:4]}-{date_value[4:6]}-{date_value[6:8]}"
    return value


def _sample_json(check: dict[str, Any]) -> Any | None:
    body = _body_preview(check)
    if not body:
        return None
    try:
        return json.loads(body)
    except json.JSONDecodeError:
        return None


def _body_preview(check: dict[str, Any]) -> str:
    response_sample_ref = check.get("response_sample_ref") if isinstance(check.get("response_sample_ref"), dict) else {}
    return str(response_sample_ref.get("body_preview") or "")


def _extract_path(sample: Any, field_path: str) -> tuple[Any, bool]:
    paths = _candidate_field_paths(field_path, sample)
    current = sample
    for path in paths:
        value, present = _extract_normalized_path(current, path)
        if present:
            return value, True
    return None, False


def _extract_normalized_path(sample: Any, path: str) -> tuple[Any, bool]:
    current = sample
    for part in path.split("."):
        if part in {"item", "[]"}:
            if isinstance(current, list) and current:
                current = current[0]
                continue
        if isinstance(current, list):
            if not current:
                return None, False
            current = current[0]
        if isinstance(current, dict) and part in current:
            current = current[part]
            continue
        return None, False
    return current, True


def _candidate_field_paths(field_path: str, sample: Any) -> list[str]:
    value = str(field_path or "")
    value = value.replace("[0]", ".item")
    value = value.replace("[]", ".item")
    value = re.sub(r"\.+", ".", value).strip(".")
    candidates = [value]
    if value.startswith("response.body."):
        candidates.append(value.removeprefix("response.body."))
    if value.startswith("response."):
        candidates.append(value.removeprefix("response."))
    if isinstance(sample, dict) and "response" in sample and not value.startswith("response."):
        candidates.insert(0, f"response.{value}")
    return [item for item in dict.fromkeys(candidates) if item]


def _all_checks(verification_result: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        item
        for key in ("operation_checks", "capability_checks")
        for item in (verification_result.get(key) or [])
        if isinstance(item, dict)
    ]


def _verification_error_categories(verification_result: dict[str, Any]) -> set[str]:
    categories: set[str] = set()
    for item in _all_checks(verification_result):
        binding_validation = item.get("binding_validation") if isinstance(item.get("binding_validation"), dict) else {}
        category = str(binding_validation.get("error_category") or "")
        if category:
            categories.add(category)
    return categories


def _only_transient_verification_failures(verification_result: dict[str, Any]) -> bool:
    categories = _verification_error_categories(verification_result)
    if not categories:
        return False
    return categories.issubset(_TRANSIENT_ERROR_CATEGORIES)


def _decision_field_path(item: dict[str, Any]) -> str:
    source_term = item.get("source_term") if isinstance(item.get("source_term"), dict) else {}
    return str(item.get("field_path") or source_term.get("field_path") or item.get("raw_name") or "")


def _gate(key: str, status: str, message: str, issues: list[dict[str, Any]], details: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "key": key,
        "status": status,
        "message": message,
        "issues": issues,
        "details": details or {},
    }


def _worst_status(statuses: Any) -> str:
    worst = QUALITY_APPROVAL_READY
    for status in statuses:
        if _QUALITY_RANK.get(str(status), 0) > _QUALITY_RANK[worst]:
            worst = str(status)
    return worst


def _gate_counts(gates: list[dict[str, Any]]) -> dict[str, int]:
    counts = {QUALITY_APPROVAL_READY: 0, QUALITY_REVIEW_REQUIRED: 0, QUALITY_BLOCKED: 0}
    for gate in gates:
        status = str(gate.get("status") or QUALITY_REVIEW_REQUIRED)
        counts[status] = counts.get(status, 0) + 1
    return counts
