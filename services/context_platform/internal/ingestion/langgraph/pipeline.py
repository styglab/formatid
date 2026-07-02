from __future__ import annotations

from dataclasses import dataclass
from typing import Any, TypedDict

from langgraph.graph import END, StateGraph

from services.context_platform.internal.ingestion.llm.agent_response import AgentResponseValidationError
from services.context_platform.internal.ingestion.llm.agent_response import validate_agent_response_artifact
from services.context_platform.internal.ingestion.llm.agent_response import validate_manual_stage_response
from services.context_platform.internal.ingestion.operation_verification import verify_ingestion_contracts
from services.context_platform.internal.ingestion.source_contract import validate_source_contract
from services.context_platform.internal.storage import ContextPlatformRepository


class IngestionPipelineState(TypedDict, total=False):
    repo: ContextPlatformRepository
    run_id: str
    run: dict[str, Any]
    source: dict[str, Any]
    document: dict[str, Any]
    run_metadata: dict[str, Any]
    cleanup_counts: dict[str, int]
    parsed: dict[str, Any]
    operations: list[dict[str, Any]]
    document_fields: list[dict[str, Any]]
    meaning_resolution: dict[str, Any]
    resolution_generation: dict[str, Any]
    capability_generation: dict[str, Any]
    verification_result: dict[str, Any]
    proposals: list[dict[str, Any]]
    evidence_snapshot: dict[str, Any]
    proposal_bundle: dict[str, Any]
    status: str
    stage: str
    result: dict[str, Any]
    error: str


@dataclass
class IngestionPipelineResult:
    status: str
    stage: str
    result: dict[str, Any]


def run_ingestion_pipeline_graph(
    run_id: str,
    *,
    repository: ContextPlatformRepository | None = None,
) -> IngestionPipelineResult:
    state: IngestionPipelineState = {
        "repo": repository or ContextPlatformRepository(),
        "run_id": run_id,
    }
    result = _build_graph().invoke(state)
    return IngestionPipelineResult(
        status=str(result.get("status") or "failed"),
        stage=str(result.get("stage") or ""),
        result=result.get("result") if isinstance(result.get("result"), dict) else {"run_id": run_id, "status": "failed"},
    )


def _build_graph():
    graph = StateGraph(IngestionPipelineState)
    graph.add_node("load_run", _load_run)
    graph.add_node("prepare_run", _prepare_run)
    graph.add_node("parse_document", _parse_document)
    graph.add_node("persist_source_graph", _persist_source_graph)
    graph.add_node("run_meaning_resolution", _meaning_resolution)
    graph.add_node("run_resolution_generation", _resolution_generation)
    graph.add_node("run_capability_generation", _capability_generation)
    graph.add_node("operation_verification", _operation_verification)
    graph.add_node("create_proposal_bundle", _create_proposal_bundle)

    graph.set_entry_point("load_run")
    graph.add_conditional_edges("load_run", _continue_or_end, {"continue": "prepare_run", "end": END})
    graph.add_edge("prepare_run", "parse_document")
    graph.add_conditional_edges("parse_document", _continue_or_end, {"continue": "persist_source_graph", "end": END})
    graph.add_edge("persist_source_graph", "run_meaning_resolution")
    graph.add_conditional_edges("run_meaning_resolution", _continue_or_end, {"continue": "run_resolution_generation", "end": END})
    graph.add_conditional_edges("run_resolution_generation", _continue_or_end, {"continue": "run_capability_generation", "end": END})
    graph.add_conditional_edges("run_capability_generation", _continue_or_end, {"continue": "operation_verification", "end": END})
    graph.add_edge("operation_verification", "create_proposal_bundle")
    graph.add_edge("create_proposal_bundle", END)
    return graph.compile()


def _continue_or_end(state: IngestionPipelineState) -> str:
    return "continue" if str(state.get("status") or "running") == "running" else "end"


def _load_run(state: IngestionPipelineState) -> IngestionPipelineState:
    repo = state["repo"]
    run_id = state["run_id"]
    run = repo.get_onboarding_run(run_id)
    if run is None:
        return _finish(state, {"run_id": run_id, "status": "not_found"}, status="not_found", stage="run_missing")

    document_id = str(run.get("source_document_id") or "")
    document = repo.get_source_document(document_id)
    if document is None:
        repo.update_onboarding_run(run_id, status="failed", stage="document_missing")
        return _finish(state, {"run_id": run_id, "status": "failed", "reason": "document_missing"}, status="failed", stage="document_missing")

    source = repo.get_source(str(run["source_id"]))
    if source is None:
        repo.update_onboarding_run(run_id, status="failed", stage="source_missing")
        return _finish(state, {"run_id": run_id, "status": "failed", "reason": "source_missing"}, status="failed", stage="source_missing")

    helpers = _api_helpers()
    run_metadata = run.get("metadata") if isinstance(run.get("metadata"), dict) else {}
    validation_error = _agent_response_validation_error(run_metadata)
    if validation_error:
        repo.update_onboarding_run(
            run_id,
            status="failed_needs_review",
            stage="agent_response_validation",
            metadata={**run_metadata, "agent_response_validation_error": validation_error},
        )
        return _finish(
            state,
            {"run_id": run_id, "status": "failed_needs_review", "stage": "agent_response_validation", "reason": validation_error},
            status="failed_needs_review",
            stage="agent_response_validation",
        )
    state["run"] = run
    state["document"] = document
    state["source"] = helpers["_source_with_run_verification_config"](source, run_metadata)
    state["run_metadata"] = run_metadata
    state["status"] = "running"
    state["stage"] = "loaded"
    return state


def _prepare_run(state: IngestionPipelineState) -> IngestionPipelineState:
    repo = state["repo"]
    run_id = state["run_id"]
    document_id = str(state["document"].get("id") or "")
    state["cleanup_counts"] = repo.cleanup_draft_ingestion_outputs(run_id=run_id, source_document_id=document_id)
    repo.update_onboarding_run(run_id, status="running", stage="document_parsing")
    state["stage"] = "document_parsing"
    return state


def _parse_document(state: IngestionPipelineState) -> IngestionPipelineState:
    helpers = _api_helpers()
    parsed_result = helpers["parse_uploaded_source_document"](state["repo"], state["run"], state["source"], state["document"])
    if parsed_result.get("status") == "waiting_manual_llm":
        run_metadata = state.get("run_metadata") or {}
        state["repo"].update_onboarding_run(
            state["run_id"],
            status="waiting_manual_llm",
            stage=helpers["STAGE_STRUCTURE_REVIEW"],
            metadata={
                **run_metadata,
                "manual_llm_request": parsed_result.get("manual_llm_request") or {},
                "chunk_count": parsed_result.get("chunk_count") or 0,
                "parser": parsed_result.get("parser"),
                "cleanup_counts": state.get("cleanup_counts") or {},
            },
        )
        return _finish(
            state,
            {
                "run_id": state["run_id"],
                "status": "waiting_manual_llm",
                "manual_llm_request": parsed_result.get("manual_llm_request") or {},
            },
            status="waiting_manual_llm",
            stage=helpers["STAGE_STRUCTURE_REVIEW"],
        )
    parsed = parsed_result["parsed"]
    contract_errors = validate_source_contract(parsed)
    if contract_errors:
        reason = "source contract validation failed: " + "; ".join(contract_errors[:5])
        run_metadata = state.get("run_metadata") or {}
        state["repo"].update_onboarding_run(
            state["run_id"],
            status="failed_needs_review",
            stage="source_contract_validation",
            metadata={
                **run_metadata,
                "source_contract_errors": contract_errors,
                "parser": parsed_result.get("parser"),
                "chunk_count": parsed_result.get("chunk_count") or 0,
            },
        )
        return _finish(
            state,
            {
                "run_id": state["run_id"],
                "status": "failed_needs_review",
                "stage": "source_contract_validation",
                "reason": reason,
                "errors": contract_errors,
            },
            status="failed_needs_review",
            stage="source_contract_validation",
        )
    state["parsed"] = parsed
    state["status"] = "running"
    return state


def _persist_source_graph(state: IngestionPipelineState) -> IngestionPipelineState:
    helpers = _api_helpers()
    repo = state["repo"]
    repo.update_onboarding_run(state["run_id"], status="running", stage=helpers["STAGE_SOURCE_GRAPH"])
    state["operations"] = helpers["persist_discovered_operations"](repo, state["source"], state["document"], state["parsed"])
    state["document_fields"] = helpers["persist_document_fields"](repo, state["source"], state["document"], state["parsed"])
    state["stage"] = helpers["STAGE_SOURCE_GRAPH"]
    return state


def _meaning_resolution(state: IngestionPipelineState) -> IngestionPipelineState:
    helpers = _api_helpers()
    run_metadata = state.get("run_metadata") or {}
    manual_response = helpers["_manual_stage_response"](
        run_metadata,
        "manual_meaning_resolution_response",
        "meaning_resolution",
        legacy_direct_keys=("manual_canonical_reconciliation_response",),
        legacy_bundle_keys=("canonical_reconciliation",),
    )
    validation_error = _stage_response_validation_error("meaning_resolution", manual_response)
    if validation_error:
        return _fail_stage_validation(state, helpers["STAGE_MEANING_RESOLUTION"], run_metadata, validation_error)
    result = helpers["build_canonical_reconciliation_for_run"](
        state["repo"],
        run_id=state["run_id"],
        source=state["source"],
        document=state["document"],
        operations=state.get("operations") or [],
        document_fields=state.get("document_fields") or [],
        llm_mode=helpers["_metadata_agent_mode"](run_metadata),
        manual_llm_response=manual_response,
    )
    stage = helpers["STAGE_MEANING_RESOLUTION"]
    if result["status"] == "waiting_manual_llm":
        state["repo"].update_onboarding_run(
            state["run_id"],
            status="waiting_manual_llm",
            stage=stage,
            metadata={
                **run_metadata,
                "manual_meaning_resolution_request": result.get("manual_llm_request") or {},
                "manual_canonical_reconciliation_request": result.get("manual_llm_request") or {},
                **_operation_field_counts(state),
                "meaning_resolution_engine": result.get("engine"),
                "canonical_reconciliation_engine": result.get("engine"),
            },
        )
        return _finish(state, {"run_id": state["run_id"], "status": "waiting_manual_llm", "stage": stage, "manual_llm_request": result.get("manual_llm_request") or {}}, status="waiting_manual_llm", stage=stage)
    if result["status"] == "failed_needs_review":
        state["repo"].update_onboarding_run(
            state["run_id"],
            status="failed_needs_review",
            stage=stage,
            metadata={
                **run_metadata,
                "manual_meaning_resolution_request": result.get("manual_llm_request") or {},
                "manual_canonical_reconciliation_request": result.get("manual_llm_request") or {},
                "meaning_resolution_error": result.get("error"),
                "canonical_reconciliation_error": result.get("error"),
                "meaning_resolution_engine": result.get("engine"),
                "canonical_reconciliation_engine": result.get("engine"),
            },
        )
        return _finish(state, {"run_id": state["run_id"], "status": "failed_needs_review", "stage": stage, "reason": result.get("error") or "meaning_resolution_failed"}, status="failed_needs_review", stage=stage)

    payload = result["payload"]
    coverage_error = helpers["_manual_coverage_error"](payload, stage=stage)
    if helpers["_is_manual_agent_mode"](helpers["_metadata_agent_mode"](run_metadata)) and coverage_error:
        state["repo"].update_onboarding_run(
            state["run_id"],
            status="failed_needs_review",
            stage=stage,
            metadata={
                **run_metadata,
                "meaning_resolution_error": coverage_error,
                "canonical_reconciliation_error": coverage_error,
                "meaning_resolution_engine": result.get("engine"),
                "canonical_reconciliation_engine": result.get("engine"),
                "meaning_decision_counts": payload.get("decision_counts") or {},
                "canonical_decision_counts": payload.get("decision_counts") or {},
            },
        )
        return _finish(state, {"run_id": state["run_id"], "status": "failed_needs_review", "stage": stage, "reason": coverage_error}, status="failed_needs_review", stage=stage)

    state["meaning_resolution"] = payload
    state["status"] = "running"
    state["stage"] = stage
    return state


def _resolution_generation(state: IngestionPipelineState) -> IngestionPipelineState:
    helpers = _api_helpers()
    run_metadata = state.get("run_metadata") or {}
    manual_response = helpers["_manual_stage_response"](
        run_metadata,
        "manual_resolution_generation_response",
        "resolution_generation",
        legacy_direct_keys=("manual_binding_generation_response",),
        legacy_bundle_keys=("binding_generation",),
    )
    validation_error = _stage_response_validation_error("resolution_generation", manual_response)
    if validation_error:
        return _fail_stage_validation(state, helpers["STAGE_RESOLUTION_GENERATION"], run_metadata, validation_error)
    result = helpers["build_binding_generation_for_run"](
        run_id=state["run_id"],
        source=state["source"],
        document=state["document"],
        operations=state.get("operations") or [],
        document_fields=state.get("document_fields") or [],
        canonical_reconciliation=state["meaning_resolution"],
        llm_mode=helpers["_metadata_agent_mode"](run_metadata),
        manual_llm_response=manual_response,
    )
    stage = helpers["STAGE_RESOLUTION_GENERATION"]
    if result["status"] == "waiting_manual_llm":
        state["repo"].update_onboarding_run(
            state["run_id"],
            status="waiting_manual_llm",
            stage=stage,
            metadata={
                **run_metadata,
                "manual_resolution_generation_request": result.get("manual_llm_request") or {},
                "manual_binding_generation_request": result.get("manual_llm_request") or {},
                **_operation_field_counts(state),
                "meaning_decision_counts": state["meaning_resolution"].get("decision_counts") or {},
                "canonical_decision_counts": state["meaning_resolution"].get("decision_counts") or {},
                "resolution_generation_engine": result.get("engine"),
                "binding_generation_engine": result.get("engine"),
            },
        )
        return _finish(state, {"run_id": state["run_id"], "status": "waiting_manual_llm", "stage": stage, "manual_llm_request": result.get("manual_llm_request") or {}}, status="waiting_manual_llm", stage=stage)
    if result["status"] == "failed_needs_review":
        state["repo"].update_onboarding_run(
            state["run_id"],
            status="failed_needs_review",
            stage=stage,
            metadata={
                **run_metadata,
                "manual_resolution_generation_request": result.get("manual_llm_request") or {},
                "manual_binding_generation_request": result.get("manual_llm_request") or {},
                "resolution_generation_error": result.get("error"),
                "binding_generation_error": result.get("error"),
                "resolution_generation_engine": result.get("engine"),
                "binding_generation_engine": result.get("engine"),
            },
        )
        return _finish(state, {"run_id": state["run_id"], "status": "failed_needs_review", "stage": stage, "reason": result.get("error") or "resolution_generation_failed"}, status="failed_needs_review", stage=stage)

    payload = result["payload"]
    coverage_error = helpers["_manual_coverage_error"](payload, stage=stage)
    if helpers["_is_manual_agent_mode"](helpers["_metadata_agent_mode"](run_metadata)) and coverage_error:
        state["repo"].update_onboarding_run(
            state["run_id"],
            status="failed_needs_review",
            stage=stage,
            metadata={
                **run_metadata,
                "resolution_generation_error": coverage_error,
                "binding_generation_error": coverage_error,
                "resolution_generation_engine": result.get("engine"),
                "binding_generation_engine": result.get("engine"),
                "meaning_decision_counts": state["meaning_resolution"].get("decision_counts") or {},
                "canonical_decision_counts": state["meaning_resolution"].get("decision_counts") or {},
                "resolution_decision_counts": payload.get("decision_counts") or {},
                "binding_decision_counts": payload.get("decision_counts") or {},
            },
        )
        return _finish(state, {"run_id": state["run_id"], "status": "failed_needs_review", "stage": stage, "reason": coverage_error}, status="failed_needs_review", stage=stage)

    state["resolution_generation"] = payload
    state["status"] = "running"
    state["stage"] = stage
    return state


def _capability_generation(state: IngestionPipelineState) -> IngestionPipelineState:
    helpers = _api_helpers()
    run_metadata = state.get("run_metadata") or {}
    manual_response = helpers["_manual_stage_response"](
        run_metadata,
        "manual_capability_generation_response",
        "capability_generation",
        legacy_direct_keys=("manual_capability_contracting_response",),
        legacy_bundle_keys=("capability_contracting",),
    )
    validation_error = _stage_response_validation_error("capability_generation", manual_response)
    if validation_error:
        return _fail_stage_validation(state, helpers["STAGE_CAPABILITY_GENERATION"], run_metadata, validation_error)
    result = helpers["build_capability_generation_for_run"](
        run_id=state["run_id"],
        source=state["source"],
        document=state["document"],
        operations=state.get("operations") or [],
        canonical_reconciliation=state["meaning_resolution"],
        binding_generation=state["resolution_generation"],
        llm_mode=helpers["_metadata_agent_mode"](run_metadata),
        manual_llm_response=manual_response,
    )
    stage = helpers["STAGE_CAPABILITY_CONTRACTING"]
    if result["status"] == "waiting_manual_llm":
        state["repo"].update_onboarding_run(
            state["run_id"],
            status="waiting_manual_llm",
            stage=stage,
            metadata={
                **run_metadata,
                "manual_capability_contracting_request": result.get("manual_llm_request") or {},
                "manual_capability_generation_request": result.get("manual_llm_request") or {},
                **_operation_field_counts(state),
                "meaning_decision_counts": state["meaning_resolution"].get("decision_counts") or {},
                "canonical_decision_counts": state["meaning_resolution"].get("decision_counts") or {},
                "resolution_decision_counts": state["resolution_generation"].get("decision_counts") or {},
                "binding_decision_counts": state["resolution_generation"].get("decision_counts") or {},
                "capability_contracting_engine": result.get("engine"),
                "capability_generation_engine": result.get("engine"),
            },
        )
        return _finish(state, {"run_id": state["run_id"], "status": "waiting_manual_llm", "stage": stage, "manual_llm_request": result.get("manual_llm_request") or {}}, status="waiting_manual_llm", stage=stage)
    if result["status"] == "failed_needs_review":
        state["repo"].update_onboarding_run(
            state["run_id"],
            status="failed_needs_review",
            stage=stage,
            metadata={
                **run_metadata,
                "manual_capability_contracting_request": result.get("manual_llm_request") or {},
                "manual_capability_generation_request": result.get("manual_llm_request") or {},
                "capability_contracting_error": result.get("error"),
                "capability_generation_error": result.get("error"),
                "capability_contracting_engine": result.get("engine"),
                "capability_generation_engine": result.get("engine"),
            },
        )
        return _finish(state, {"run_id": state["run_id"], "status": "failed_needs_review", "stage": stage, "reason": result.get("error") or "capability_generation_failed"}, status="failed_needs_review", stage=stage)

    state["capability_generation"] = result["payload"]
    state["status"] = "running"
    state["stage"] = stage
    return state


def _operation_verification(state: IngestionPipelineState) -> IngestionPipelineState:
    helpers = _api_helpers()
    state["repo"].update_onboarding_run(state["run_id"], status="running", stage=helpers["STAGE_OPERATION_VERIFICATION"])
    state["verification_result"] = verify_ingestion_contracts(
        state["repo"],
        run_id=state["run_id"],
        source=state["source"],
        document=state["document"],
        operations=state.get("operations") or [],
        binding_generation=state["resolution_generation"],
        capability_generation=state["capability_generation"],
    )
    state["stage"] = helpers["STAGE_OPERATION_VERIFICATION"]
    return state


def _create_proposal_bundle(state: IngestionPipelineState) -> IngestionPipelineState:
    helpers = _api_helpers()
    repo = state["repo"]
    proposals = helpers["create_ingestion_proposals"](
        repo,
        state["source"],
        state["document"],
        state.get("operations") or [],
        state.get("document_fields") or [],
        state["meaning_resolution"],
        state["resolution_generation"],
        state["capability_generation"],
        state["verification_result"],
    )
    evidence_snapshot = helpers["create_evidence_snapshot"](
        repo,
        state["run"],
        state["source"],
        state["document"],
        state["parsed"],
        state.get("operations") or [],
        state.get("document_fields") or [],
        state["meaning_resolution"],
        state["resolution_generation"],
        state["capability_generation"],
        state["verification_result"],
    )
    bundle = helpers["create_final_proposal_bundle"](
        repo,
        state["run"],
        state["source"],
        state["document"],
        proposals,
        evidence_snapshot,
        state.get("operations") or [],
        state.get("document_fields") or [],
        state["meaning_resolution"],
        state["resolution_generation"],
        state["capability_generation"],
        state["verification_result"],
    )
    metadata = {
        **(state["run"].get("metadata") if isinstance(state["run"].get("metadata"), dict) else {}),
        "cleanup_counts": state.get("cleanup_counts") or {},
        **_operation_field_counts(state),
        "proposal_count": len(proposals),
        "proposal_bundle_id": bundle["id"],
        "meaning_decision_counts": state["meaning_resolution"].get("decision_counts") or {},
        "canonical_decision_counts": state["meaning_resolution"].get("decision_counts") or {},
        "relation_decision_counts": state["meaning_resolution"].get("relation_decision_counts") or {},
        "resolution_decision_counts": state["resolution_generation"].get("decision_counts") or {},
        "binding_decision_counts": state["resolution_generation"].get("decision_counts") or {},
        "capability_contracting_decision_counts": state["capability_generation"].get("decision_counts") or {},
        "capability_decision_counts": state["capability_generation"].get("decision_counts") or {},
        "verification_summary": state["verification_result"].get("summary") or {},
    }
    for key in (
        "manual_meaning_resolution_request",
        "manual_canonical_reconciliation_request",
        "manual_resolution_generation_request",
        "manual_binding_generation_request",
        "manual_capability_contracting_request",
        "manual_capability_generation_request",
        "meaning_resolution_error",
        "canonical_reconciliation_error",
        "resolution_generation_error",
        "binding_generation_error",
        "capability_contracting_error",
        "capability_generation_error",
    ):
        metadata.pop(key, None)
    repo.update_onboarding_run(
        state["run_id"],
        status="completed",
        stage=helpers["STAGE_PROPOSAL_BUNDLE"],
        metadata=metadata,
    )
    return _finish(
        state,
        {
            "run_id": state["run_id"],
            "status": "completed",
            "source_operation_count": len(state.get("operations") or []),
            "source_field_count": _operation_field_counts(state)["field_count"],
            "proposal_count": len(proposals),
            "proposal_bundle_id": bundle["id"],
            "verification_summary": state["verification_result"].get("summary") or {},
        },
        status="completed",
        stage=helpers["STAGE_PROPOSAL_BUNDLE"],
    )


def _operation_field_counts(state: IngestionPipelineState) -> dict[str, int]:
    operations = state.get("operations") or []
    document_fields = state.get("document_fields") or []
    return {
        "operation_count": len(operations),
        "field_count": len(document_fields) + sum(len(operation.get("fields", [])) for operation in operations),
    }


def _finish(state: IngestionPipelineState, result: dict[str, Any], *, status: str, stage: str) -> IngestionPipelineState:
    state["status"] = status
    state["stage"] = stage
    state["result"] = result
    return state


def _agent_response_validation_error(run_metadata: dict[str, Any]) -> str:
    payload = run_metadata.get("agent_response")
    if not isinstance(payload, dict):
        payload = run_metadata.get("manual_llm_response")
    if not isinstance(payload, dict):
        return ""
    try:
        validate_agent_response_artifact(payload)
    except AgentResponseValidationError as exc:
        return str(exc)
    return ""


def _stage_response_validation_error(stage: str, payload: dict[str, Any] | None) -> str:
    if payload is None:
        return ""
    try:
        validate_manual_stage_response(stage, payload)
    except AgentResponseValidationError as exc:
        return str(exc)
    return ""


def _fail_stage_validation(
    state: IngestionPipelineState,
    stage: str,
    run_metadata: dict[str, Any],
    validation_error: str,
) -> IngestionPipelineState:
    state["repo"].update_onboarding_run(
        state["run_id"],
        status="failed_needs_review",
        stage=stage,
        metadata={**run_metadata, f"{stage}_validation_error": validation_error},
    )
    return _finish(
        state,
        {"run_id": state["run_id"], "status": "failed_needs_review", "stage": stage, "reason": validation_error},
        status="failed_needs_review",
        stage=stage,
    )


def _api_helpers() -> dict[str, Any]:
    from services.context_platform.internal.ingestion import api_documents

    return {
        "STAGE_STRUCTURE_REVIEW": api_documents.STAGE_STRUCTURE_REVIEW,
        "STAGE_SOURCE_GRAPH": api_documents.STAGE_SOURCE_GRAPH,
        "STAGE_MEANING_RESOLUTION": api_documents.STAGE_MEANING_RESOLUTION,
        "STAGE_RESOLUTION_GENERATION": api_documents.STAGE_RESOLUTION_GENERATION,
        "STAGE_CAPABILITY_GENERATION": api_documents.STAGE_CAPABILITY_GENERATION,
        "STAGE_CAPABILITY_CONTRACTING": api_documents.STAGE_CAPABILITY_CONTRACTING,
        "STAGE_OPERATION_VERIFICATION": api_documents.STAGE_OPERATION_VERIFICATION,
        "STAGE_PROPOSAL_BUNDLE": api_documents.STAGE_PROPOSAL_BUNDLE,
        "_is_manual_agent_mode": api_documents._is_manual_agent_mode,
        "_manual_coverage_error": api_documents._manual_coverage_error,
        "_manual_stage_response": api_documents._manual_stage_response,
        "_metadata_agent_mode": api_documents._metadata_agent_mode,
        "_source_with_run_verification_config": api_documents._source_with_run_verification_config,
        "build_binding_generation_for_run": api_documents.build_binding_generation_for_run,
        "build_capability_generation_for_run": api_documents.build_capability_generation_for_run,
        "build_canonical_reconciliation_for_run": api_documents.build_canonical_reconciliation_for_run,
        "create_evidence_snapshot": api_documents.create_evidence_snapshot,
        "create_final_proposal_bundle": api_documents.create_final_proposal_bundle,
        "create_ingestion_proposals": api_documents.create_ingestion_proposals,
        "parse_uploaded_source_document": api_documents.parse_uploaded_source_document,
        "persist_discovered_operations": api_documents.persist_discovered_operations,
        "persist_document_fields": api_documents.persist_document_fields,
    }
