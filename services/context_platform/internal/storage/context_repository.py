from __future__ import annotations

import json
import os
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from core.runtime.runtime_db.url import get_database_url


SCHEMA_SQL_PATH = Path(__file__).resolve().parents[3] / "postgres" / "init" / "002_create_context_platform.sql"


class ContextPlatformRepository:
    def __init__(self, database_url: str | None = None) -> None:
        self.database_url = database_url or os.getenv("CONTEXT_PLATFORM_DATABASE_URL") or get_database_url(
            "POSTGRES_DATABASE_URL",
            host_default="postgres",
        )

    def ensure_schema(self) -> dict[str, Any]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(SCHEMA_SQL_PATH.read_text(encoding="utf-8"))
            conn.commit()
        return {"status": "ready", "schema": "context_platform"}

    def reset_context(self) -> dict[str, Any]:
        tables = [
            "policy_tags",
            "quality_checks",
            "lineage_edges",
            "metadata_aspects",
            "review_events",
            "execution_traces",
            "execution_results",
            "execution_step_runs",
            "execution_plan_steps",
            "proposal_bundle_items",
            "proposal_bundles",
            "evidence_snapshots",
            "onboarding_runs",
            "review_decisions",
            "proposals",
            "execution_logs",
            "endpoint_checks",
            "executions",
            "plan_steps",
            "plans",
            "capability_constraints",
            "capability_steps",
            "capability_operations",
            "capability_outputs",
            "capability_inputs",
            "capabilities",
            "resolution_rules",
            "parameter_bindings",
            "context_bindings",
            "field_bindings",
            "transform_rules",
            "binding_evidence",
            "bindings",
            "external_projections",
            "representation_schemas",
            "canonical_representations",
            "link_types",
            "property_types",
            "object_types",
            "value_domain_values",
            "value_domains",
            "concept_relations",
            "concepts",
            "concept_schemes",
            "meaning_scopes",
            "evidence_items",
            "canonical_relations",
            "canonical_class_slots",
            "canonical_classes",
            "canonical_enum_values",
            "canonical_enums",
            "canonical_slots",
            "canonical_types",
            "source_fields",
            "source_parameters",
            "source_operations",
            "source_documents",
            "sources",
        ]
        self.ensure_schema()
        with self._connect() as conn:
            with conn.cursor() as cur:
                for table in tables:
                    cur.execute("select to_regclass(%s)", [f"context_platform.{table}"])
                    if cur.fetchone()[0]:
                        cur.execute(f"delete from context_platform.{table}")
            conn.commit()
        self.ensure_schema()
        return {"status": "reset", "schema": "context_platform", "tables": tables}

    def cleanup_draft_ingestion_outputs(self, *, run_id: str, source_document_id: str) -> dict[str, int]:
        counts: dict[str, int] = {}
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute(
                    """
                    delete from context_platform.review_decisions rd
                    using context_platform.proposal_bundle_items pbi
                    join context_platform.proposal_bundles pb on pb.id = pbi.bundle_id
                    where rd.proposal_id = pbi.proposal_id
                      and pb.run_id = %s
                    """,
                    [run_id],
                )
                counts["review_decisions"] = cur.rowcount
                cur.execute(
                    """
                    delete from context_platform.proposal_bundle_items pbi
                    using context_platform.proposal_bundles pb
                    where pbi.bundle_id = pb.id
                      and pb.run_id = %s
                    """,
                    [run_id],
                )
                counts["proposal_bundle_items"] = cur.rowcount
                cur.execute("delete from context_platform.proposal_bundles where run_id = %s", [run_id])
                counts["proposal_bundles"] = cur.rowcount
                cur.execute(
                    """
                    delete from context_platform.proposals
                    where payload->>'source_document_id' = %s
                       or entity_id = %s
                    """,
                    [source_document_id, source_document_id],
                )
                counts["proposals"] = cur.rowcount
                cur.execute("delete from context_platform.evidence_snapshots where run_id = %s", [run_id])
                counts["evidence_snapshots"] = cur.rowcount
                cur.execute("delete from context_platform.endpoint_checks where run_id = %s", [run_id])
                counts["endpoint_checks"] = cur.rowcount
                cur.execute(
                    """
                    delete from context_platform.source_fields
                    where source_document_id = %s
                      and source_operation_id is null
                      and status not in ('approved', 'published')
                    """,
                    [source_document_id],
                )
                counts["document_fields"] = cur.rowcount
                cur.execute(
                    """
                    delete from context_platform.source_operations
                    where source_document_id = %s
                      and status not in ('approved', 'published')
                    """,
                    [source_document_id],
                )
                counts["source_operations"] = cur.rowcount
            conn.commit()
        return counts

    def list_sources(self, query: str = "", status: str = "") -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if query:
            clauses.append("(name ilike %s or provider ilike %s or description ilike %s)")
            like = f"%{query}%"
            params.extend([like, like, like])
        if status:
            clauses.append("status = %s")
            params.append(status)
        return self._select_many("sources", clauses, params, "name")

    def get_source(self, source_id: str) -> dict[str, Any] | None:
        return self._select_one("sources", "id = %s", [source_id])

    def create_source(self, payload: dict[str, Any]) -> dict[str, Any]:
        record = {
            "id": str(payload.get("id") or f"src_{uuid4().hex}"),
            "namespace": str(payload.get("namespace") or "public"),
            "name": _required(payload, "name"),
            "provider": str(payload.get("provider") or ""),
            "source_type": str(payload.get("source_type") or "api"),
            "description": str(payload.get("description") or ""),
            "version": str(payload.get("version") or "1.0.0"),
            "lifecycle": str(payload.get("lifecycle") or "draft"),
            "status": str(payload.get("status") or "draft"),
            "config": _object(payload.get("config")),
            "created_by": str(payload.get("created_by") or "system"),
            "reviewed_by": payload.get("reviewed_by"),
            "approved_at": payload.get("approved_at"),
            "evidence": _array(payload.get("evidence")),
            "confidence": payload.get("confidence"),
        }
        return self._insert(
            "sources",
            record,
            json_columns={"config", "evidence"},
            returning="*",
        )

    def overview(self) -> dict[str, Any]:
        tables = [
            "sources",
            "source_documents",
            "source_operations",
            "source_parameters",
            "source_fields",
            "meaning_scopes",
            "concept_schemes",
            "concepts",
            "concept_relations",
            "value_domains",
            "value_domain_values",
            "object_types",
            "property_types",
            "link_types",
            "canonical_representations",
            "representation_schemas",
            "external_projections",
            "field_bindings",
            "context_bindings",
            "parameter_bindings",
            "transform_rules",
            "resolution_rules",
            "canonical_types",
            "canonical_enums",
            "canonical_enum_values",
            "canonical_slots",
            "canonical_class_slots",
            "canonical_classes",
            "canonical_relations",
            "bindings",
            "capabilities",
            "capability_inputs",
            "capability_outputs",
            "capability_steps",
            "capability_constraints",
            "capability_operations",
            "plans",
            "execution_plan_steps",
            "executions",
            "execution_step_runs",
            "execution_results",
            "execution_traces",
            "endpoint_checks",
            "evidence_items",
            "review_events",
            "metadata_aspects",
            "proposals",
            "proposal_bundles",
        ]
        counts: dict[str, int] = {}
        recent_proposals: list[dict[str, Any]] = []
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                for table in tables:
                    cur.execute("select to_regclass(%s) as table_name", [f"context_platform.{table}"])
                    if not (cur.fetchone() or {}).get("table_name"):
                        counts[table] = 0
                        continue
                    cur.execute(f"select count(*) as count from context_platform.{table}")
                    counts[table] = int((cur.fetchone() or {}).get("count") or 0)
                cur.execute(
                    """
                    select *
                    from context_platform.proposals
                    order by created_at desc
                    limit 8
                    """
                )
                recent_proposals = [_row(row) for row in cur.fetchall()]
                cur.execute(
                    """
                    select count(*) as count
                    from context_platform.proposals
                    where status in ('proposed', 'reviewed')
                    """
                )
                counts["pending_proposals"] = int((cur.fetchone() or {}).get("count") or 0)
                cur.execute(
                    """
                    select count(*) as count
                    from context_platform.proposal_bundles
                    where status in ('draft', 'proposed', 'reviewed')
                    """
                )
                counts["pending_bundles"] = int((cur.fetchone() or {}).get("count") or 0)

        return {"counts": counts, "recent_proposals": recent_proposals}

    def workbench_workflow(self, source_document_id: str = "", run_id: str = "") -> dict[str, Any]:
        documents = self.list_source_documents()
        runs = self.list_onboarding_runs()
        bundles = self.list_proposal_bundles()
        operations = self.list_source_operations()
        parameters = self.list_source_parameters()
        fields = self.list_source_fields()
        types = self.list_canonical_types()
        enums = self.list_canonical_enums()
        slots = self.list_canonical_slots()
        classes = self.list_canonical_classes()
        class_slots = self.list_canonical_class_slots()
        bindings = self.list_bindings()
        capabilities = self.list_capabilities()
        capability_operations = self.list_capability_operations()

        active_document = _find_by_id(documents, source_document_id) if source_document_id else (documents[0] if documents else None)
        active_run = _find_by_id(runs, run_id) if run_id else None
        if not active_run and active_document:
            active_run = next((run for run in runs if run.get("source_document_id") == active_document.get("id")), None)
        if not active_run:
            active_run = runs[0] if runs else None

        active_bundle = None
        if active_run:
            active_bundle = next((bundle for bundle in bundles if bundle.get("run_id") == active_run.get("id")), None)
        if not active_bundle and active_document:
            active_bundle = next((bundle for bundle in bundles if bundle.get("source_id") == active_document.get("source_id")), None)
        if not active_bundle:
            active_bundle = bundles[0] if bundles else None
        endpoint_checks = self.list_endpoint_checks(run_id=str(active_run.get("id") or "")) if active_run else []

        source_id = str(active_document.get("source_id") or "") if active_document else ""
        document_id = str(active_document.get("id") or "") if active_document else ""
        scoped_operations = (
            [
                item
                for item in operations
                if item.get("source_document_id") == document_id
                or (not item.get("source_document_id") and item.get("source_id") == source_id)
            ]
            if active_document
            else []
        )
        operation_ids = {item["id"] for item in scoped_operations}
        scoped_parameters = [item for item in parameters if item.get("source_operation_id") in operation_ids]
        scoped_fields = (
            [
                item
                for item in fields
                if item.get("source_document_id") == document_id
                or item.get("source_operation_id") in operation_ids
                or (not item.get("source_document_id") and item.get("source_id") == source_id)
            ]
            if active_document
            else []
        )
        scoped_bindings = (
            [
                item
                for item in bindings
                if item.get("source_document_id") == document_id
                or item.get("source_operation_id") in operation_ids
                or item.get("source_id") == source_id
            ]
            if active_document
            else []
        )
        scoped_capability_operations = [item for item in capability_operations if item.get("source_operation_id") in operation_ids]
        linked_capability_ids = {item["capability_id"] for item in scoped_capability_operations}
        scoped_capabilities = [item for item in capabilities if item.get("id") in linked_capability_ids]

        has_document = active_document is not None
        run_status = str((active_run or {}).get("status") or "")
        run_running = active_run is not None and run_status in {"submitted", "started", "running", "agent_processing"}
        run_queued_for_agent = active_run is not None and run_status in {"queued_for_agent", "needs_agent", "agent_manual"}
        has_extracted_assets = bool(scoped_operations or scoped_fields or scoped_parameters)
        has_canonical_model = bool(types or enums or slots or classes or class_slots)
        has_bindings = bool(scoped_bindings)
        has_capabilities = bool(scoped_capabilities)
        has_bundle = active_bundle is not None
        has_executable_operations = bool(scoped_operations)
        has_executable_capability = bool(scoped_capability_operations and scoped_operations)
        verified_check_count = len([item for item in endpoint_checks if item.get("status") == "verified"])
        failed_check_count = len([item for item in endpoint_checks if item.get("status") == "failed"])
        mode = "executable" if has_executable_operations else "knowledge_only"

        steps = [
            _workflow_step(
                "upload_source",
                1,
                "Source Intake",
                "complete" if has_document else "ready",
                active_document.get("name")
                if active_document
                else "Upload a source document to create an agent ingestion request.",
            ),
            _workflow_step(
                "agent_ingestion",
                2,
                "Agent Ingestion",
                "running"
                if run_running
                else "complete"
                if has_extracted_assets
                else "ready"
                if run_queued_for_agent or has_document
                else "blocked",
                f"{len(scoped_operations)} operations, {len(scoped_parameters)} parameters, {len(scoped_fields)} fields extracted"
                if has_extracted_assets
                else "Upload a document first."
                if not has_document
                else "Queued for operator-agent ingestion; dashboard does not generate semantic drafts.",
                ["upload_source"],
            ),
            _workflow_step(
                "review_bundle",
                3,
                "Review Proposal Bundle",
                "ready" if has_bundle else "blocked",
                f"{len(classes)} object types, {len(slots)} property types, {len(scoped_bindings)} bindings, {len(scoped_capabilities)} capabilities"
                if has_bundle
                else "Agent ingestion must create a reviewable proposal bundle first.",
                ["agent_ingestion"],
            ),
            _workflow_step(
                "validate_bundle",
                4,
                "Validate Review Bundle",
                "ready" if has_bundle else "blocked",
                f"{verified_check_count} verified, {failed_check_count} failed endpoint checks."
                if endpoint_checks
                else "Validate source, meaning, representation, resolution, and capabilities together.",
                ["review_bundle"],
            ),
            _workflow_step(
                "submit_review",
                5,
                "Approve Or Publish",
                "ready" if has_bundle else "blocked",
                "Only the final bundle enters review, approval, and publication.",
                ["validate_bundle"],
            ),
        ]

        return {
            "mode": mode,
            "requires_operation": mode == "executable",
            "execution_ready": has_executable_capability,
            "active_document": active_document,
            "active_run": active_run,
            "active_bundle": active_bundle,
            "counts": {
                "source_documents": len(documents),
                "source_operations": len(scoped_operations),
                "source_parameters": len(scoped_parameters),
                "source_fields": len(scoped_fields),
                "canonical_types": len(types),
                "canonical_enums": len(enums),
                "canonical_slots": len(slots),
                "canonical_classes": len(classes),
                "canonical_class_slots": len(class_slots),
                "bindings": len(scoped_bindings),
                "capabilities": len(scoped_capabilities),
                "capability_operations": len(scoped_capability_operations),
                "endpoint_checks": len(endpoint_checks),
                "endpoint_checks_verified": verified_check_count,
                "endpoint_checks_failed": failed_check_count,
                "endpoint_checks_skipped": len([item for item in endpoint_checks if item.get("status") == "skipped"]),
                "endpoint_checks_needs_input": len([item for item in endpoint_checks if item.get("status") == "needs_input"]),
                "proposal_bundles": len(bundles),
            },
            "steps": steps,
        }

    def list_source_documents(self, source_id: str = "") -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if source_id:
            clauses.append("source_id = %s")
            params.append(source_id)
        return self._select_many("source_documents", clauses, params, "created_at desc")

    def get_source_document(self, source_document_id: str) -> dict[str, Any] | None:
        return self._select_one("source_documents", "id = %s", [source_document_id])

    def create_source_document(self, payload: dict[str, Any]) -> dict[str, Any]:
        record = {
            "id": str(payload.get("id") or f"doc_{uuid4().hex}"),
            "source_id": _required(payload, "source_id"),
            "document_type": str(payload.get("document_type") or "api_document"),
            "name": _required(payload, "name"),
            "uri": str(payload.get("uri") or ""),
            "content_hash": str(payload.get("content_hash") or ""),
            "content_type": str(payload.get("content_type") or ""),
            "status": str(payload.get("status") or "draft"),
            "metadata": _object(payload.get("metadata")),
            "evidence": _array(payload.get("evidence")),
            "created_by": str(payload.get("created_by") or "system"),
        }
        return self._insert("source_documents", record, json_columns={"metadata", "evidence"})

    def list_source_operations(self, source_id: str = "", status: str = "") -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if source_id:
            clauses.append("source_id = %s")
            params.append(source_id)
        if status:
            clauses.append("status = any(%s)")
            params.append(_status_values(status))
        return self._select_many("source_operations", clauses, params, "source_id, method, path")

    def get_source_operation_by_key(self, operation_key: str) -> dict[str, Any] | None:
        return self._select_one("source_operations", "operation_key = %s", [operation_key])

    def create_source_operation(self, payload: dict[str, Any]) -> dict[str, Any]:
        method = str(payload.get("method") or "GET").upper()
        path = _required(payload, "path")
        record = {
            "id": str(payload.get("id") or f"op_{uuid4().hex}"),
            "source_id": _required(payload, "source_id"),
            "source_document_id": payload.get("source_document_id"),
            "operation_key": str(payload.get("operation_key") or f"{method} {path}"),
            "method": method,
            "path": path,
            "name": str(payload.get("name") or f"{method} {path}"),
            "description": str(payload.get("description") or ""),
            "auth_spec": _object(payload.get("auth_spec")),
            "request_spec": _object(payload.get("request_spec")),
            "response_spec": _object(payload.get("response_spec")),
            "endpoint_metadata": _object(payload.get("endpoint_metadata")),
            "version": str(payload.get("version") or "1.0.0"),
            "lifecycle": str(payload.get("lifecycle") or "draft"),
            "status": str(payload.get("status") or "draft"),
            "created_by": str(payload.get("created_by") or "system"),
            "reviewed_by": payload.get("reviewed_by"),
            "approved_at": payload.get("approved_at"),
            "evidence": _array(payload.get("evidence")),
            "confidence": payload.get("confidence"),
        }
        return self._insert(
            "source_operations",
            record,
            json_columns={"auth_spec", "request_spec", "response_spec", "endpoint_metadata", "evidence"},
        )

    def list_source_parameters(self, source_operation_id: str = "") -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if source_operation_id:
            clauses.append("source_operation_id = %s")
            params.append(source_operation_id)
        return self._select_many("source_parameters", clauses, params, "source_operation_id, location, name")

    def create_source_parameter(self, payload: dict[str, Any]) -> dict[str, Any]:
        record = {
            "id": str(payload.get("id") or f"param_{uuid4().hex}"),
            "source_operation_id": _required(payload, "source_operation_id"),
            "name": _required(payload, "name"),
            "raw_name": str(payload.get("raw_name") or payload.get("name") or ""),
            "location": str(payload.get("location") or "query"),
            "parameter_path": str(payload.get("parameter_path") or ""),
            "data_type": str(payload.get("data_type") or "string"),
            "is_required": bool(payload.get("is_required", False)),
            "default_value": payload.get("default_value"),
            "description": str(payload.get("description") or ""),
            "enum_values": _array(payload.get("enum_values")),
            "metadata": _object(payload.get("metadata")),
            "status": str(payload.get("status") or "draft"),
            "evidence": _array(payload.get("evidence")),
            "confidence": payload.get("confidence"),
        }
        return self._insert("source_parameters", record, json_columns={"enum_values", "metadata", "evidence"})

    def list_source_fields(self, source_operation_id: str = "", source_document_id: str = "", direction: str = "") -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if source_operation_id:
            clauses.append("source_operation_id = %s")
            params.append(source_operation_id)
        if source_document_id:
            clauses.append("source_document_id = %s")
            params.append(source_document_id)
        if direction:
            clauses.append("direction = %s")
            params.append(direction)
        return self._select_many("source_fields", clauses, params, "source_document_id, source_operation_id, direction, field_path")

    def create_source_field(self, payload: dict[str, Any]) -> dict[str, Any]:
        record = {
            "id": str(payload.get("id") or f"field_{uuid4().hex}"),
            "source_id": payload.get("source_id"),
            "source_document_id": payload.get("source_document_id"),
            "source_operation_id": payload.get("source_operation_id"),
            "direction": _required(payload, "direction"),
            "field_path": _required(payload, "field_path"),
            "raw_name": str(payload.get("raw_name") or ""),
            "display_name": str(payload.get("display_name") or payload.get("raw_name") or ""),
            "data_type": str(payload.get("data_type") or "string"),
            "is_required": bool(payload.get("is_required", False)),
            "description": str(payload.get("description") or ""),
            "metadata": _object(payload.get("metadata")),
            "status": str(payload.get("status") or "draft"),
            "evidence": _array(payload.get("evidence")),
            "confidence": payload.get("confidence"),
        }
        if not record["source_operation_id"] and not record["source_document_id"]:
            raise ValueError("source_operation_id or source_document_id is required")
        return self._insert("source_fields", record, json_columns={"metadata", "evidence"})

    def list_endpoint_checks(
        self,
        *,
        run_id: str = "",
        source_operation_id: str = "",
        capability_key: str = "",
        status: str = "",
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if run_id:
            clauses.append("run_id = %s")
            params.append(run_id)
        if source_operation_id:
            clauses.append("source_operation_id = %s")
            params.append(source_operation_id)
        if capability_key:
            clauses.append("capability_key = %s")
            params.append(capability_key)
        if status:
            clauses.append("status = %s")
            params.append(status)
        return self._select_many("endpoint_checks", clauses, params, "checked_at desc, id")

    def create_endpoint_check(self, payload: dict[str, Any]) -> dict[str, Any]:
        record = {
            "id": str(payload.get("id") or f"check_{uuid4().hex}"),
            "run_id": payload.get("run_id"),
            "source_id": _required(payload, "source_id"),
            "source_document_id": payload.get("source_document_id"),
            "source_operation_id": payload.get("source_operation_id"),
            "capability_key": str(payload.get("capability_key") or ""),
            "check_type": _required(payload, "check_type"),
            "status": _required(payload, "status"),
            "http_status": payload.get("http_status"),
            "request_sample_redacted": _object(payload.get("request_sample_redacted")),
            "response_sample_ref": _object(payload.get("response_sample_ref")),
            "field_coverage": _object(payload.get("field_coverage")),
            "binding_validation": _object(payload.get("binding_validation")),
            "error_message": str(payload.get("error_message") or ""),
            "checked_by": str(payload.get("checked_by") or "context_platform_worker"),
            "checked_at": payload.get("checked_at") or _now(),
        }
        if not record["source_operation_id"] and not record["capability_key"]:
            raise ValueError("source_operation_id or capability_key is required")
        return self._insert(
            "endpoint_checks",
            record,
            json_columns={"request_sample_redacted", "response_sample_ref", "field_coverage", "binding_validation"},
        )

    def list_meaning_scopes(self, status: str = "") -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if status:
            clauses.append("status = any(%s)")
            params.append(_status_values(status))
        return self._select_many("meaning_scopes", clauses, params, "stable_key")

    def list_concept_schemes(self, status: str = "") -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if status:
            clauses.append("status = any(%s)")
            params.append(_status_values(status))
        return self._select_many("concept_schemes", clauses, params, "stable_key")

    def list_concepts(self, query: str = "", kind: str = "", status: str = "") -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if query:
            clauses.append("(stable_key ilike %s or label_ko ilike %s or label_en ilike %s or definition ilike %s)")
            like = f"%{query}%"
            params.extend([like, like, like, like])
        if kind:
            clauses.append("kind = %s")
            params.append(kind)
        if status:
            clauses.append("status = any(%s)")
            params.append(_status_values(status))
        return self._select_many("concepts", clauses, params, "meaning_scope_id, stable_key")

    def list_value_domains(self, status: str = "") -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if status:
            clauses.append("status = any(%s)")
            params.append(_status_values(status))
        return self._select_many("value_domains", clauses, params, "stable_key")

    def list_value_domain_values(self, value_domain_id: str = "", status: str = "") -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if value_domain_id:
            clauses.append("value_domain_id = %s")
            params.append(value_domain_id)
        if status:
            clauses.append("status = any(%s)")
            params.append(_status_values(status))
        return self._select_many("value_domain_values", clauses, params, "value_domain_id, code")

    def list_object_types(self, query: str = "", status: str = "") -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if query:
            clauses.append("(stable_key ilike %s or name ilike %s or description ilike %s)")
            like = f"%{query}%"
            params.extend([like, like, like])
        if status:
            clauses.append("status = any(%s)")
            params.append(_status_values(status))
        return self._select_many("object_types", clauses, params, "stable_key")

    def list_property_types(self, query: str = "", status: str = "") -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if query:
            clauses.append("(stable_key ilike %s or name ilike %s or description ilike %s)")
            like = f"%{query}%"
            params.extend([like, like, like])
        if status:
            clauses.append("status = any(%s)")
            params.append(_status_values(status))
        return self._select_many("property_types", clauses, params, "stable_key")

    def list_link_types(self, status: str = "") -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if status:
            clauses.append("status = any(%s)")
            params.append(_status_values(status))
        return self._select_many("link_types", clauses, params, "stable_key")

    def list_canonical_representations(self, concept_id: str = "", status: str = "") -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if concept_id:
            clauses.append("concept_id = %s")
            params.append(concept_id)
        if status:
            clauses.append("status = any(%s)")
            params.append(_status_values(status))
        return self._select_many("canonical_representations", clauses, params, "priority, stable_key")

    def list_representation_schemas(self, representation_id: str = "", status: str = "") -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if representation_id:
            clauses.append("representation_id = %s")
            params.append(representation_id)
        if status:
            clauses.append("status = any(%s)")
            params.append(_status_values(status))
        return self._select_many("representation_schemas", clauses, params, "representation_id, stable_key")

    def list_field_bindings(self, source_operation_id: str = "", status: str = "") -> list[dict[str, Any]]:
        return [item for item in self.list_bindings(source_operation_id=source_operation_id, status=status) if item.get("binding_type") == "field"]

    def list_context_bindings(self, source_operation_id: str = "", status: str = "") -> list[dict[str, Any]]:
        return [item for item in self.list_bindings(source_operation_id=source_operation_id, status=status) if item.get("binding_type") == "context"]

    def list_parameter_bindings(self, source_operation_id: str = "", status: str = "") -> list[dict[str, Any]]:
        return [item for item in self.list_bindings(source_operation_id=source_operation_id, status=status) if item.get("binding_type") == "parameter"]

    def list_canonical_types(self, query: str = "", status: str = "") -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if query:
            clauses.append("(stable_key ilike %s or datatype ilike %s)")
            like = f"%{query}%"
            params.extend([like, like])
        if status:
            clauses.append("status = any(%s)")
            params.append(_status_values(status))
        records = self._select_many("representation_schemas", clauses, params, "stable_key")
        return [
            {
                "id": item["id"],
                "namespace": "repr",
                "name": item["stable_key"],
                "description": "Compatibility alias for RepresentationSchema.",
                "base_type": item.get("datatype") or "string",
                "pattern": item.get("pattern") or "",
                "minimum": (item.get("validation_json") or {}).get("minimum") if isinstance(item.get("validation_json"), dict) else None,
                "maximum": (item.get("validation_json") or {}).get("maximum") if isinstance(item.get("validation_json"), dict) else None,
                "status": item.get("status"),
                "metadata": {"compatibility_source": "representation_schemas"},
            }
            for item in records
        ]

    def create_canonical_type(self, payload: dict[str, Any]) -> dict[str, Any]:
        raise ValueError("canonical_types were retired; create a representation_schema instead")

    def list_canonical_enums(self, query: str = "", status: str = "") -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if query:
            clauses.append("(name ilike %s or description ilike %s)")
            like = f"%{query}%"
            params.extend([like, like])
        if status:
            clauses.append("status = any(%s)")
            params.append(_status_values(status))
        records = self._select_many("value_domains", clauses, params, "stable_key")
        return [
            {
                "id": item["id"],
                "namespace": "meaning",
                "name": item["stable_key"],
                "description": item.get("description") or "",
                "permissible_values": {},
                "status": item.get("status"),
                "metadata": {"compatibility_source": "value_domains"},
            }
            for item in records
        ]

    def create_canonical_enum(self, payload: dict[str, Any]) -> dict[str, Any]:
        raise ValueError("canonical_enums were retired; create a value_domain instead")

    def list_canonical_enum_values(self, enum_id: str = "") -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if enum_id:
            clauses.append("enum_id = %s")
            params.append(enum_id)
        if enum_id:
            clauses = ["value_domain_id = %s"]
            params = [enum_id]
        records = self._select_many("value_domain_values", clauses, params, "value_domain_id, code")
        return [
            {
                "id": item["id"],
                "enum_id": item.get("value_domain_id"),
                "code": item.get("code"),
                "meaning": item.get("concept_id") or "",
                "description": item.get("description") or "",
                "aliases": [],
                "annotations": {},
                "metadata": item.get("metadata") or {},
                "status": item.get("status"),
            }
            for item in records
        ]

    def create_canonical_enum_value(self, payload: dict[str, Any]) -> dict[str, Any]:
        raise ValueError("canonical_enum_values were retired; create a value_domain_value instead")

    def list_canonical_slots(self, query: str = "", status: str = "") -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if query:
            clauses.append("(name ilike %s or description ilike %s)")
            like = f"%{query}%"
            params.extend([like, like])
        if status:
            clauses.append("status = any(%s)")
            params.append(_status_values(status))
        records = self._select_many("property_types", clauses, params, "stable_key")
        return [
            {
                "id": item["id"],
                "namespace": "repr",
                "name": item["stable_key"],
                "description": item.get("description") or "",
                "range_kind": "type",
                "range_ref": item.get("broad_datatype") or "string",
                "datatype": item.get("broad_datatype") or "string",
                "aliases": [],
                "examples": [],
                "mappings": [],
                "annotations": {},
                "constraints": {},
                "identity_role": "",
                "status": item.get("status"),
                "metadata": item.get("metadata") or {},
            }
            for item in records
        ]

    def create_canonical_slot(self, payload: dict[str, Any]) -> dict[str, Any]:
        raise ValueError("canonical_slots were retired; create a property_type or representation_schema instead")

    def list_canonical_classes(self, query: str = "", status: str = "") -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if query:
            clauses.append("(name ilike %s or description ilike %s)")
            like = f"%{query}%"
            params.extend([like, like])
        if status:
            clauses.append("status = any(%s)")
            params.append(_status_values(status))
        records = self._select_many("object_types", clauses, params, "stable_key")
        return [
            {
                "id": item["id"],
                "namespace": "repr",
                "name": item["stable_key"],
                "description": item.get("description") or "",
                "status": item.get("status"),
                "metadata": item.get("metadata") or {},
            }
            for item in records
        ]

    def create_canonical_class(self, payload: dict[str, Any]) -> dict[str, Any]:
        raise ValueError("canonical_classes were retired; create an object_type instead")

    def list_canonical_class_slot_usages(self, class_id: str = "", status: str = "") -> list[dict[str, Any]]:
        class_slots = self.list_canonical_class_slots(class_id=class_id, status=status)
        classes = {item["id"]: item for item in self.list_canonical_classes(status=status)}
        slots = {item["id"]: item for item in self.list_canonical_slots(status=status)}
        records: list[dict[str, Any]] = []
        for usage in class_slots:
            class_item = classes.get(str(usage.get("class_id") or "")) or {}
            slot_item = slots.get(str(usage.get("slot_id") or "")) or {}
            usage_name = str(usage.get("usage_name") or slot_item.get("name") or "")
            records.append(
                {
                    **usage,
                    "id": usage.get("id"),
                    "class_slot_id": usage.get("id"),
                    "canonical_class_slot_id": usage.get("id"),
                    "class_id": usage.get("class_id"),
                    "slot_id": usage.get("slot_id"),
                    "canonical_slot_id": usage.get("slot_id"),
                    "class_name": class_item.get("name") or "",
                    "slot_name": slot_item.get("name") or "",
                    "name": usage_name,
                    "description": slot_item.get("description") or "",
                    "datatype": usage.get("range_override") or slot_item.get("datatype") or slot_item.get("range_ref") or "string",
                    "identity_role": slot_item.get("identity_role") or "",
                    "constraints": usage.get("constraints") or slot_item.get("constraints") or {},
                    "metadata": slot_item.get("metadata") or {},
                    "namespace": class_item.get("namespace") or slot_item.get("namespace") or "public",
                    "lifecycle": class_item.get("lifecycle") or slot_item.get("lifecycle") or "draft",
                }
            )
        return records

    def create_canonical_class_slot_usage(self, payload: dict[str, Any]) -> dict[str, Any]:
        raise ValueError("canonical class-slot usages were retired; create a canonical_representation instead")

    def list_canonical_class_slots(self, class_id: str = "", slot_id: str = "", status: str = "") -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if class_id:
            clauses.append("carrier_object_type_id = %s")
            params.append(class_id)
        if slot_id:
            clauses.append("value_property_type_id = %s")
            params.append(slot_id)
        if status:
            clauses.append("status = any(%s)")
            params.append(_status_values(status))
        records = self._select_many("canonical_representations", clauses, params, "carrier_object_type_id, priority, value_property_type_id")
        return [
            {
                "id": item["id"],
                "class_id": item.get("carrier_object_type_id"),
                "slot_id": item.get("value_property_type_id"),
                "usage_name": item.get("stable_key"),
                "required": False,
                "multivalued": False,
                "slot_order": item.get("priority") or 100,
                "range_override": "",
                "constraints": {
                    "fixed_context": item.get("fixed_context_json") or {},
                    "required_context": item.get("required_context_json") or [],
                },
                "annotations": {},
                "status": item.get("status"),
                "metadata": {"concept_id": item.get("concept_id"), "compatibility_source": "canonical_representations"},
            }
            for item in records
        ]

    def create_canonical_class_slot(self, payload: dict[str, Any]) -> dict[str, Any]:
        raise ValueError("canonical_class_slots were retired; create a canonical_representation instead")

    def list_canonical_relations(self, status: str = "") -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if status:
            clauses.append("status = any(%s)")
            params.append(_status_values(status))
        records = self._select_many("link_types", clauses, params, "stable_key")
        return [
            {
                **item,
                "source_class_id": item.get("source_object_type_id"),
                "target_class_id": item.get("target_object_type_id"),
                "relation_type": item.get("name") or item.get("stable_key"),
                "forward_label": (item.get("metadata") or {}).get("forward_label") if isinstance(item.get("metadata"), dict) else "",
                "reverse_label": (item.get("metadata") or {}).get("reverse_label") if isinstance(item.get("metadata"), dict) else "",
            }
            for item in records
        ]

    def create_canonical_relation(self, payload: dict[str, Any]) -> dict[str, Any]:
        raise ValueError("canonical_relations were retired; create a link_type instead")

    def export_linkml_schema(self, *, namespace: str = "public", status: str = "") -> dict[str, Any]:
        include_all = namespace in {"", "public"}
        types = [item for item in self.list_canonical_types(status=status) if include_all or item.get("namespace") == namespace]
        enums = [item for item in self.list_canonical_enums(status=status) if include_all or item.get("namespace") == namespace]
        slots = [item for item in self.list_canonical_slots(status=status) if include_all or item.get("namespace") == namespace]
        classes = [item for item in self.list_canonical_classes(status=status) if include_all or item.get("namespace") == namespace]
        class_slots = self.list_canonical_class_slots(status=status)
        relations = self.list_canonical_relations(status=status)
        enum_values = self.list_canonical_enum_values()
        slots_by_id = {item["id"]: item for item in slots}
        classes_by_id = {item["id"]: item for item in classes}
        enum_values_by_enum: dict[str, dict[str, Any]] = {}
        for value in enum_values:
            enum_values_by_enum.setdefault(value["enum_id"], {})[value["code"]] = {
                key: value[key]
                for key in ("meaning", "description")
                if value.get(key)
            }

        schema: dict[str, Any] = {
            "id": f"https://context-platform.local/linkml/{namespace}",
            "name": namespace,
            "description": "Context Platform canonical model export",
            "prefixes": {namespace: f"https://context-platform.local/{namespace}/"},
            "default_prefix": namespace,
            "default_range": "string",
            "types": {},
            "enums": {},
            "slots": {},
            "classes": {},
        }
        for item in types:
            type_payload = {"typeof": item.get("typeof") or item.get("base_type") or "string"}
            if item.get("description"):
                type_payload["description"] = item["description"]
            if item.get("uri"):
                type_payload["uri"] = item["uri"]
            if item.get("pattern"):
                type_payload["pattern"] = item["pattern"]
            schema["types"][item["name"]] = type_payload
        for item in enums:
            values = dict(item.get("permissible_values") or {})
            values.update(enum_values_by_enum.get(item["id"], {}))
            enum_payload: dict[str, Any] = {"permissible_values": values}
            if item.get("description"):
                enum_payload["description"] = item["description"]
            schema["enums"][item["name"]] = enum_payload
        for item in slots:
            slot_payload: dict[str, Any] = {"range": item.get("range_ref") or item.get("datatype") or "string"}
            for key in ("description", "aliases", "examples", "mappings", "annotations"):
                if item.get(key):
                    slot_payload[key] = item[key]
            constraints = item.get("constraints") or {}
            for key, value in constraints.items():
                slot_payload[key] = value
            schema["slots"][item["name"]] = slot_payload
        class_slots_by_class: dict[str, list[dict[str, Any]]] = {}
        for item in class_slots:
            class_slots_by_class.setdefault(item["class_id"], []).append(item)
        relations_by_class: dict[str, list[dict[str, Any]]] = {}
        for item in relations:
            relations_by_class.setdefault(item["source_class_id"], []).append(item)
        for item in classes:
            class_payload: dict[str, Any] = {"slots": []}
            if item.get("description"):
                class_payload["description"] = item["description"]
            slot_usages: dict[str, Any] = {}
            for usage in class_slots_by_class.get(item["id"], []):
                slot = slots_by_id.get(usage["slot_id"])
                if not slot:
                    continue
                slot_name = usage.get("usage_name") or slot["name"]
                class_payload["slots"].append(slot_name)
                usage_payload = {
                    key: usage[key]
                    for key in ("required", "multivalued")
                    if usage.get(key)
                }
                if usage.get("range_override"):
                    usage_payload["range"] = usage["range_override"]
                if usage.get("annotations"):
                    usage_payload["annotations"] = usage["annotations"]
                if usage_payload:
                    slot_usages[slot_name] = usage_payload
            for relation in relations_by_class.get(item["id"], []):
                target_class = classes_by_id.get(str(relation.get("target_class_id") or ""))
                if not target_class:
                    continue
                relation_slot_name = str(relation.get("relation_type") or "")
                if not relation_slot_name:
                    continue
                class_payload["slots"].append(relation_slot_name)
                relation_slot: dict[str, Any] = {
                    "range": target_class["name"],
                    "annotations": {
                        "context_platform": {
                            "object_type": "canonical_relation",
                            "relation_id": relation.get("id"),
                            "relation_type": relation.get("relation_type"),
                            "source_class_id": relation.get("source_class_id"),
                            "target_class_id": relation.get("target_class_id"),
                        }
                    },
                }
                metadata = relation.get("metadata") if isinstance(relation.get("metadata"), dict) else {}
                description = str(metadata.get("description") or "")
                if description:
                    relation_slot["description"] = description
                if relation.get("forward_label"):
                    relation_slot["title"] = relation["forward_label"]
                schema["slots"][relation_slot_name] = relation_slot
                relation_usage: dict[str, Any] = {}
                if metadata.get("required") is not None:
                    relation_usage["required"] = bool(metadata.get("required"))
                if metadata.get("cardinality") in {"one_to_many", "many_to_many"}:
                    relation_usage["multivalued"] = True
                if relation_usage:
                    slot_usages[relation_slot_name] = relation_usage
            if slot_usages:
                class_payload["slot_usage"] = slot_usages
            schema["classes"][item["name"]] = class_payload
        return schema

    def list_bindings(self, source_operation_id: str = "", status: str = "") -> list[dict[str, Any]]:
        status_values = _status_values(status)
        params: list[Any] = []
        operation_filter = ""
        if source_operation_id:
            operation_filter = " and src.source_operation_id = %s"
            params.append(source_operation_id)
        status_filter = ""
        if status_values:
            status_filter = " and (b.status = any(%s) or b.review_status = any(%s))"
            params.extend([status_values, status_values])
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute(
                    f"""
                    select
                      b.id,
                      coalesce(b.stable_key, b.id) as stable_key,
                      src.source_id,
                      src.source_document_id,
                      src.source_operation_id,
                      null::text as source_parameter_id,
                      b.source_field_id,
                      b.representation_id,
                      b.representation_schema_id,
                      b.fills_property_type_id,
                      repr.concept_id as required_concept_id,
                      null::text as context_key,
                      'output'::text as direction,
                      'field'::text as binding_type,
                      b.transform_rule_id,
                      b.confidence,
                      b.review_status,
                      b.status,
                      b.metadata,
                      b.created_at,
                      b.updated_at,
                      b.representation_id as canonical_class_slot_id,
                      b.metadata as transform_spec,
                      '{{}}'::jsonb as normalization_rule,
                      '{{}}'::jsonb as enum_mapping
                    from context_platform.field_bindings b
                    join context_platform.source_fields src on src.id = b.source_field_id
                    join context_platform.canonical_representations repr on repr.id = b.representation_id
                    where true {operation_filter} {status_filter}
                    union all
                    select
                      b.id,
                      coalesce(b.stable_key, b.id) as stable_key,
                      src.source_id,
                      src.source_document_id,
                      src.source_operation_id,
                      null::text as source_parameter_id,
                      b.source_field_id,
                      b.representation_id,
                      b.representation_schema_id,
                      null::text as fills_property_type_id,
                      coalesce(b.target_concept_id, repr.concept_id) as required_concept_id,
                      b.context_key,
                      'output_context'::text as direction,
                      'context'::text as binding_type,
                      b.transform_rule_id,
                      b.confidence,
                      b.review_status,
                      b.status,
                      b.metadata,
                      b.created_at,
                      b.updated_at,
                      b.representation_id as canonical_class_slot_id,
                      b.metadata as transform_spec,
                      '{{}}'::jsonb as normalization_rule,
                      '{{}}'::jsonb as enum_mapping
                    from context_platform.context_bindings b
                    join context_platform.source_fields src on src.id = b.source_field_id
                    join context_platform.canonical_representations repr on repr.id = b.representation_id
                    where true {operation_filter} {status_filter}
                    union all
                    select
                      b.id,
                      coalesce(b.stable_key, b.id) as stable_key,
                      op.source_id,
                      op.source_document_id,
                      src.source_operation_id,
                      b.source_parameter_id,
                      null::text as source_field_id,
                      b.representation_id,
                      b.representation_schema_id,
                      null::text as fills_property_type_id,
                      b.required_concept_id,
                      null::text as context_key,
                      'input'::text as direction,
                      'parameter'::text as binding_type,
                      b.transform_rule_id,
                      b.confidence,
                      b.review_status,
                      b.status,
                      b.metadata,
                      b.created_at,
                      b.updated_at,
                      b.representation_id as canonical_class_slot_id,
                      b.metadata as transform_spec,
                      '{{}}'::jsonb as normalization_rule,
                      '{{}}'::jsonb as enum_mapping
                    from context_platform.parameter_bindings b
                    join context_platform.source_parameters src on src.id = b.source_parameter_id
                    join context_platform.source_operations op on op.id = src.source_operation_id
                    where true {operation_filter} {status_filter}
                    order by source_operation_id, direction, id
                    """,
                    params * 3 if source_operation_id or status_values else [],
                )
                return [_row(row) for row in cur.fetchall()]

    def create_binding(self, payload: dict[str, Any]) -> dict[str, Any]:
        direction = str(payload.get("direction") or "output")
        representation_id = str(payload.get("representation_id") or payload.get("canonical_class_slot_id") or "").strip()
        representation_schema_id = str(payload.get("representation_schema_id") or "").strip() or None
        status = str(payload.get("status") or "draft")
        review_status = str(payload.get("review_status") or ("approved" if status in {"approved", "active", "published"} else "proposed"))
        metadata = {
            "transform_spec": _object(payload.get("transform_spec")),
            "normalization_rule": _object(payload.get("normalization_rule")),
            "enum_mapping": _object(payload.get("enum_mapping")),
            "evidence": _array(payload.get("evidence")),
        }
        if payload.get("source_parameter_id") or direction == "input":
            source_parameter_id = _required(payload, "source_parameter_id")
            required_concept_id = str(payload.get("required_concept_id") or payload.get("concept_id") or "").strip()
            if not required_concept_id and representation_id:
                required_concept_id = self._concept_id_for_representation(representation_id) or ""
            if not required_concept_id:
                raise ValueError("required_concept_id is required for parameter bindings")
            record = {
                "id": str(payload.get("id") or f"pbind_{uuid4().hex}"),
                "stable_key": str(payload.get("stable_key") or payload.get("id") or f"pbind.{uuid4().hex}"),
                "source_parameter_id": source_parameter_id,
                "required_concept_id": required_concept_id,
                "representation_id": representation_id or None,
                "representation_schema_id": representation_schema_id,
                "transform_rule_id": payload.get("transform_rule_id"),
                "confidence": payload.get("confidence"),
                "review_status": review_status,
                "status": status,
                "metadata": metadata,
            }
            return self._insert("parameter_bindings", record, json_columns={"metadata"})
        if not representation_id:
            raise ValueError("representation_id is required for source field bindings")
        source_field_id = _required(payload, "source_field_id")
        if payload.get("context_key") or str(payload.get("binding_type") or "") == "context":
            record = {
                "id": str(payload.get("id") or f"cbind_{uuid4().hex}"),
                "stable_key": str(payload.get("stable_key") or payload.get("id") or f"cbind.{uuid4().hex}"),
                "source_field_id": source_field_id,
                "representation_id": representation_id,
                "representation_schema_id": representation_schema_id,
                "context_key": _required(payload, "context_key"),
                "target_concept_id": payload.get("target_concept_id") or payload.get("required_concept_id"),
                "transform_rule_id": payload.get("transform_rule_id"),
                "confidence": payload.get("confidence"),
                "review_status": review_status,
                "status": status,
                "metadata": metadata,
            }
            return self._insert("context_bindings", record, json_columns={"metadata"})
        record = {
            "id": str(payload.get("id") or f"fbind_{uuid4().hex}"),
            "stable_key": str(payload.get("stable_key") or payload.get("id") or f"fbind.{uuid4().hex}"),
            "source_field_id": source_field_id,
            "representation_id": representation_id,
            "representation_schema_id": representation_schema_id,
            "fills_property_type_id": payload.get("fills_property_type_id"),
            "transform_rule_id": payload.get("transform_rule_id"),
            "confidence": payload.get("confidence"),
            "review_status": review_status,
            "status": status,
            "metadata": metadata,
        }
        return self._insert("field_bindings", record, json_columns={"metadata"})

    def create_evidence_snapshot(self, payload: dict[str, Any]) -> dict[str, Any]:
        record = {
            "id": str(payload.get("id") or f"evidence_{uuid4().hex}"),
            "run_id": _required(payload, "run_id"),
            "source_id": _required(payload, "source_id"),
            "source_document_id": payload.get("source_document_id"),
            "snapshot_type": str(payload.get("snapshot_type") or "source_ingestion"),
            "content_hash": str(payload.get("content_hash") or ""),
            "source_ref": _object(payload.get("source_ref")),
            "operation_evidence": _array(payload.get("operation_evidence")),
            "schema_evidence": _array(payload.get("schema_evidence")),
            "sample_values": _object(payload.get("sample_values")),
            "ai_context": _object(payload.get("ai_context")),
        }
        return self._insert(
            "evidence_snapshots",
            record,
            json_columns={"source_ref", "operation_evidence", "schema_evidence", "sample_values", "ai_context"},
        )

    def list_proposal_bundles(self, status: str = "", source_id: str = "") -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if status:
            clauses.append("pb.status = %s")
            params.append(status)
        if source_id:
            clauses.append("pb.source_id = %s")
            params.append(source_id)
        where_sql = f"where {' and '.join(clauses)}" if clauses else ""
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute(
                    f"""
                    select pb.*, count(pbi.proposal_id)::int as proposal_count
                    from context_platform.proposal_bundles pb
                    left join context_platform.proposal_bundle_items pbi on pbi.bundle_id = pb.id
                    {where_sql}
                    group by pb.id
                    order by pb.created_at desc
                    """,
                    params,
                )
                return [_row(row) for row in cur.fetchall()]

    def get_proposal_bundle(self, bundle_id: str) -> dict[str, Any] | None:
        bundles = [item for item in self.list_proposal_bundles() if item.get("id") == bundle_id]
        return bundles[0] if bundles else None

    def list_proposal_bundle_items(self, bundle_id: str) -> list[dict[str, Any]]:
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute(
                    """
                    select p.*, pbi.item_order
                    from context_platform.proposal_bundle_items pbi
                    join context_platform.proposals p on p.id = pbi.proposal_id
                    where pbi.bundle_id = %s
                    order by pbi.item_order, p.created_at
                    """,
                    [bundle_id],
                )
                return [_row(row) for row in cur.fetchall()]

    def create_proposal_bundle(self, payload: dict[str, Any], proposal_ids: list[str]) -> dict[str, Any]:
        record = {
            "id": str(payload.get("id") or f"bundle_{uuid4().hex}"),
            "run_id": _required(payload, "run_id"),
            "source_id": _required(payload, "source_id"),
            "evidence_snapshot_id": payload.get("evidence_snapshot_id"),
            "title": _required(payload, "title"),
            "status": str(payload.get("status") or "proposed"),
            "summary": _object(payload.get("summary")),
            "created_by": str(payload.get("created_by") or "system"),
            "reviewed_by": payload.get("reviewed_by"),
            "reviewed_at": payload.get("reviewed_at"),
        }
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                columns = list(record.keys())
                placeholders = [f"%({column})s::jsonb" if column == "summary" else f"%({column})s" for column in columns]
                params = {
                    key: json.dumps(value) if key == "summary" else value
                    for key, value in record.items()
                }
                cur.execute(
                    f"""
                    insert into context_platform.proposal_bundles ({', '.join(columns)})
                    values ({', '.join(placeholders)})
                    returning *
                    """,
                    params,
                )
                bundle = _row(cur.fetchone())
                for index, proposal_id in enumerate(proposal_ids):
                    cur.execute(
                        """
                        insert into context_platform.proposal_bundle_items (bundle_id, proposal_id, item_order)
                        values (%s, %s, %s)
                        on conflict (bundle_id, proposal_id) do nothing
                        """,
                        [bundle["id"], proposal_id, (index + 1) * 10],
                    )
            conn.commit()
        bundle["proposal_count"] = len(proposal_ids)
        return bundle

    def approve_proposal_bundle(self, bundle_id: str, *, reviewer: str = "system") -> dict[str, Any]:
        reviewer = reviewer or "system"
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute("select * from context_platform.proposal_bundles where id = %s", [bundle_id])
                bundle = _row(cur.fetchone())
                if not bundle:
                    raise ValueError("proposal bundle not found")
                cur.execute(
                    """
                    select p.*, pbi.item_order
                    from context_platform.proposal_bundle_items pbi
                    join context_platform.proposals p on p.id = pbi.proposal_id
                    where pbi.bundle_id = %s
                    order by pbi.item_order, p.created_at
                    """,
                    [bundle_id],
                )
                proposals = [_row(row) for row in cur.fetchall()]
                if not proposals:
                    raise ValueError("proposal bundle has no proposal items")
                approval_errors = self._proposal_bundle_approval_errors(proposals)
                if approval_errors:
                    raise ValueError("proposal bundle is not approvable: " + "; ".join(approval_errors[:5]))

                context: dict[str, Any] = {
                    "class_slots_by_ref": {},
                    "capabilities_by_key": {},
                    "applied": [],
                    "skipped": [],
                }
                for proposal in proposals:
                    result = self._apply_proposal(cur, proposal, context, reviewer=reviewer)
                    context["applied"].extend(result.get("applied", []))
                    context["skipped"].extend(result.get("skipped", []))
                    cur.execute(
                        """
                        update context_platform.proposals
                        set status = 'approved',
                            reviewed_by = %s,
                            reviewed_at = now(),
                            approved_at = now()
                        where id = %s
                        """,
                        [reviewer, proposal["id"]],
                    )
                    cur.execute(
                        """
                        insert into context_platform.review_decisions
                          (id, proposal_id, reviewer, decision, rationale, evidence)
                        values (%s, %s, %s, 'approved', %s, %s::jsonb)
                        """,
                        [
                            f"review_{uuid4().hex}",
                            proposal["id"],
                            reviewer,
                            "Approved through proposal bundle application.",
                            json.dumps(proposal.get("evidence") or []),
                        ],
                    )

                cur.execute(
                    """
                    update context_platform.proposal_bundles
                    set status = 'approved',
                        reviewed_by = %s,
                        reviewed_at = now(),
                        updated_at = now()
                    where id = %s
                    returning *
                    """,
                    [reviewer, bundle_id],
                )
                approved_bundle = _row(cur.fetchone())
            conn.commit()

        approved_bundle["proposal_count"] = len(proposals)
        return {
            "status": "approved",
            "proposal_bundle": approved_bundle,
            "applied_count": len(context["applied"]),
            "skipped_count": len(context["skipped"]),
            "applied": context["applied"],
            "skipped": context["skipped"],
        }

    def reject_proposal_bundle(
        self,
        bundle_id: str,
        *,
        reviewer: str = "system",
        rationale: str = "",
    ) -> dict[str, Any]:
        reviewer = reviewer or "system"
        rationale = rationale or "Rejected through proposal bundle review."
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute("select * from context_platform.proposal_bundles where id = %s", [bundle_id])
                bundle = _row(cur.fetchone())
                if not bundle:
                    raise ValueError("proposal bundle not found")
                if str(bundle.get("status") or "") in {"approved", "published"}:
                    raise ValueError("approved or published proposal bundle cannot be rejected")
                cur.execute(
                    """
                    select p.*, pbi.item_order
                    from context_platform.proposal_bundle_items pbi
                    join context_platform.proposals p on p.id = pbi.proposal_id
                    where pbi.bundle_id = %s
                    order by pbi.item_order, p.created_at
                    """,
                    [bundle_id],
                )
                proposals = [_row(row) for row in cur.fetchall()]
                if not proposals:
                    raise ValueError("proposal bundle has no proposal items")

                for proposal in proposals:
                    cur.execute(
                        """
                        update context_platform.proposals
                        set status = 'rejected',
                            reviewed_by = %s,
                            reviewed_at = now()
                        where id = %s
                        """,
                        [reviewer, proposal["id"]],
                    )
                    cur.execute(
                        """
                        insert into context_platform.review_decisions
                          (id, proposal_id, reviewer, decision, rationale, evidence)
                        values (%s, %s, %s, 'rejected', %s, %s::jsonb)
                        """,
                        [
                            f"review_{uuid4().hex}",
                            proposal["id"],
                            reviewer,
                            rationale,
                            json.dumps(proposal.get("evidence") or []),
                        ],
                    )

                cur.execute(
                    """
                    update context_platform.proposal_bundles
                    set status = 'rejected',
                        reviewed_by = %s,
                        reviewed_at = now(),
                        updated_at = now()
                    where id = %s
                    returning *
                    """,
                    [reviewer, bundle_id],
                )
                rejected_bundle = _row(cur.fetchone())
            conn.commit()

        rejected_bundle["proposal_count"] = len(proposals)
        return {
            "status": "rejected",
            "proposal_bundle": rejected_bundle,
            "rejected_count": len(proposals),
            "rationale": rationale,
        }

    def _proposal_bundle_approval_errors(self, proposals: list[dict[str, Any]]) -> list[str]:
        errors: list[str] = []
        has_executable_capability = False
        verification_summary: dict[str, Any] | None = None
        for proposal in proposals:
            entity_type = str(proposal.get("entity_type") or "")
            payload = proposal.get("payload") if isinstance(proposal.get("payload"), dict) else {}
            if entity_type in {"canonical_class_slot", "meaning_resolution_decision", "canonical_representation"} and str(payload.get("canonical_decision") or "") == "conflict":
                errors.append(f"unresolved canonical decision:{proposal.get('entity_id')}")
            if entity_type in {"binding", "resolution_binding"} and str(payload.get("binding_decision") or "") == "conflict":
                errors.append(f"unresolved binding:{proposal.get('entity_id')}")
            if entity_type == "capability":
                if payload.get("source_operation_id") or isinstance(payload.get("operation_link"), dict):
                    has_executable_capability = True
            if entity_type in {"capability_operation", "capability_step"}:
                has_executable_capability = True
            if entity_type == "endpoint_check_summary":
                summary = payload.get("verification_summary")
                if isinstance(summary, dict):
                    verification_summary = summary
        if has_executable_capability:
            if not verification_summary:
                errors.append("executable capabilities require endpoint verification evidence")
            else:
                total = int(verification_summary.get("total") or 0)
                verified = int(verification_summary.get("verified") or 0)
                failed = int(verification_summary.get("failed") or 0)
                needs_input = int(verification_summary.get("needs_input") or 0)
                skipped = int(verification_summary.get("skipped") or 0)
                if total <= 0:
                    errors.append("executable capabilities require at least one endpoint verification check")
                elif failed or needs_input or skipped or verified < total:
                    errors.append(
                        "endpoint verification is incomplete "
                        f"(verified={verified}, total={total}, failed={failed}, needs_input={needs_input}, skipped={skipped})"
                    )
        return errors

    def _apply_proposal(self, cur: Any, proposal: dict[str, Any], context: dict[str, Any], *, reviewer: str) -> dict[str, list[dict[str, Any]]]:
        entity_type = str(proposal.get("entity_type") or "")
        payload = proposal.get("payload") if isinstance(proposal.get("payload"), dict) else {}
        if entity_type == "source_operation":
            operation_id = str(payload.get("source_operation_id") or proposal.get("entity_id") or "")
            if not operation_id:
                return {"applied": [], "skipped": [{"proposal_id": proposal["id"], "reason": "missing_source_operation_id"}]}
            cur.execute(
                """
                update context_platform.source_operations
                set status = 'approved',
                    lifecycle = 'approved',
                    reviewed_by = %s,
                    approved_at = now(),
                    updated_at = now()
                where id = %s
                """,
                [reviewer, operation_id],
            )
            cur.execute("update context_platform.source_parameters set status = 'approved', updated_at = now() where source_operation_id = %s", [operation_id])
            cur.execute("update context_platform.source_fields set status = 'approved', updated_at = now() where source_operation_id = %s", [operation_id])
            return {"applied": [{"type": "source_operation", "id": operation_id}], "skipped": []}

        if entity_type in {"canonical_class_slot", "meaning_resolution_decision", "canonical_representation"}:
            representation_id = self._ensure_representation_from_legacy_proposal(cur, proposal, context, reviewer=reviewer)
            if not representation_id:
                return {"applied": [], "skipped": [{"proposal_id": proposal["id"], "reason": "missing_representation_target"}]}
            return {"applied": [{"type": "canonical_representation", "id": representation_id}], "skipped": []}

        if entity_type in {"canonical_relation", "link_type"}:
            relation_id = self._ensure_link_type_from_legacy_relation_proposal(cur, proposal, reviewer=reviewer)
            if not relation_id:
                return {"applied": [], "skipped": [{"proposal_id": proposal["id"], "reason": "missing_link_type"}]}
            return {"applied": [{"type": "link_type", "id": relation_id}], "skipped": []}

        if entity_type in {"binding", "resolution_binding"}:
            binding_id = self._ensure_binding_from_proposal(cur, proposal, context, reviewer=reviewer)
            if not binding_id:
                return {"applied": [], "skipped": [{"proposal_id": proposal["id"], "reason": "binding_skipped_or_unresolved"}]}
            return {"applied": [{"type": "binding", "id": binding_id}], "skipped": []}

        if entity_type == "capability":
            capability_id = self._ensure_capability_from_proposal(cur, proposal, context, reviewer=reviewer)
            if not capability_id:
                return {"applied": [], "skipped": [{"proposal_id": proposal["id"], "reason": "missing_capability"}]}
            return {"applied": [{"type": "capability", "id": capability_id}], "skipped": []}

        if entity_type in {"capability_operation", "capability_step"}:
            operation_link_id = self._ensure_capability_operation_from_proposal(cur, proposal, context, reviewer=reviewer)
            if not operation_link_id:
                return {"applied": [], "skipped": [{"proposal_id": proposal["id"], "reason": "missing_capability_operation"}]}
            return {"applied": [{"type": "capability_step", "id": operation_link_id}], "skipped": []}

        return {"applied": [], "skipped": [{"proposal_id": proposal["id"], "reason": f"no_apply_handler:{entity_type}"}]}

    def _ensure_representation_from_legacy_proposal(self, cur: Any, proposal: dict[str, Any], context: dict[str, Any], *, reviewer: str) -> str | None:
        payload = proposal.get("payload") if isinstance(proposal.get("payload"), dict) else {}
        reconciliation = payload.get("canonical_reconciliation") if isinstance(payload.get("canonical_reconciliation"), dict) else {}
        proposed = reconciliation.get("proposed_canonical") if isinstance(reconciliation.get("proposed_canonical"), dict) else {}
        matched = reconciliation.get("matched_canonical_object") if isinstance(reconciliation.get("matched_canonical_object"), dict) else {}
        representation_id = str(matched.get("id") or payload.get("representation_id") or payload.get("canonical_class_slot_id") or "") if matched.get("object_type") in {"canonical_class_slot", "canonical_representation"} else ""
        class_name = str(proposed.get("class_name") or matched.get("class_name") or "").strip()
        slot_name = str(proposed.get("slot_name") or proposed.get("attribute_name") or matched.get("name") or "").strip()
        if _is_non_business_canonical_class_name(class_name):
            return None
        if representation_id:
            self._remember_class_slot(context, class_name, slot_name, representation_id)
            return representation_id
        if not class_name or not slot_name:
            return None
        object_type_id = self._ensure_object_type(cur, class_name, description=str(proposed.get("class_description") or ""), evidence=proposal.get("evidence") or [])
        property_type_id = self._ensure_property_type(
            cur,
            slot_name,
            datatype=str(proposed.get("datatype") or "string"),
            description=str(proposed.get("description") or ""),
            evidence=proposal.get("evidence") or [],
        )
        concept_id = self._ensure_concept_for_legacy_slot(cur, slot_name, evidence=proposal.get("evidence") or [])
        representation_id = self._ensure_canonical_representation(cur, concept_id, object_type_id, property_type_id, slot_name, proposed, evidence=proposal.get("evidence") or [])
        self._remember_class_slot(context, class_name, slot_name, representation_id)
        return representation_id

    def _ensure_link_type_from_legacy_relation_proposal(self, cur: Any, proposal: dict[str, Any], *, reviewer: str) -> str | None:
        payload = proposal.get("payload") if isinstance(proposal.get("payload"), dict) else {}
        if str(payload.get("relation_decision") or "propose_relation") != "propose_relation":
            return None
        source_class_id = str(payload.get("source_class_id") or "").strip()
        target_class_id = str(payload.get("target_class_id") or "").strip()
        source_class_name = str(payload.get("source_class_name") or "").strip()
        target_class_name = str(payload.get("target_class_name") or "").strip()
        relation_type = str(payload.get("relation_type") or "").strip()
        if not relation_type or (not source_class_id and not source_class_name) or (not target_class_id and not target_class_name):
            return None
        if _is_non_business_canonical_class_name(source_class_name) or _is_non_business_canonical_class_name(target_class_name):
            return None
        if not source_class_id:
            source_class_id = self._ensure_object_type(cur, source_class_name, description="", evidence=proposal.get("evidence") or [])
        if not target_class_id:
            target_class_id = self._ensure_object_type(cur, target_class_name, description="", evidence=proposal.get("evidence") or [])
        cur.execute(
            """
            select id from context_platform.link_types
            where source_object_type_id = %s
              and target_object_type_id = %s
              and name = %s
            limit 1
            """,
            [source_class_id, target_class_id, relation_type],
        )
        row = cur.fetchone()
        metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
        metadata = {
            **metadata,
            "description": str(payload.get("description") or metadata.get("description") or ""),
            "cardinality": str(payload.get("cardinality") or metadata.get("cardinality") or ""),
            "required": bool(payload.get("required", metadata.get("required", False))),
        }
        if row:
            relation_id = str(row["id"])
            cur.execute(
                """
                update context_platform.link_types
                set status = 'approved',
                    metadata = %s::jsonb,
                    evidence = %s::jsonb,
                    updated_at = now()
                where id = %s
                """,
                [
                    json.dumps(metadata),
                    json.dumps(proposal.get("evidence") or []),
                    relation_id,
                ],
            )
            return relation_id
        relation_id = f"link.{_to_snake(relation_type)}.{uuid4().hex[:8]}"
        cur.execute(
            """
            insert into context_platform.link_types
              (id, stable_key, name, source_object_type_id, target_object_type_id, description,
               status, metadata, evidence)
            values (%s, %s, %s, %s, %s, %s, 'approved', %s::jsonb, %s::jsonb)
            """,
            [
                relation_id,
                relation_id,
                relation_type,
                source_class_id,
                target_class_id,
                str(payload.get("description") or ""),
                json.dumps(metadata),
                json.dumps(proposal.get("evidence") or []),
            ],
        )
        return relation_id

    def _ensure_binding_from_proposal(self, cur: Any, proposal: dict[str, Any], context: dict[str, Any], *, reviewer: str) -> str | None:
        payload = proposal.get("payload") if isinstance(proposal.get("payload"), dict) else {}
        if str(payload.get("binding_decision") or "bind") == "skip_binding":
            return None
        representation_id = str(payload.get("representation_id") or payload.get("canonical_class_slot_id") or "")
        canonical_ref = payload.get("canonical_ref") if isinstance(payload.get("canonical_ref"), dict) else {}
        if not representation_id:
            representation_id = self._resolve_class_slot_ref(cur, canonical_ref, context, proposal.get("evidence") or [], reviewer=reviewer) or ""
        if not representation_id:
            return None
        source_parameter_id = payload.get("source_parameter_id")
        source_field_id = payload.get("source_field_id")
        direction = str(payload.get("direction") or "output")
        status = "approved"
        cur.execute(
            """
            select id from context_platform.field_bindings
            where source_field_id = %s
              and representation_id = %s
            limit 1
            """,
            [source_field_id, representation_id],
        )
        if source_parameter_id or direction == "input":
            concept_id = self._concept_id_for_representation(representation_id) or str(payload.get("required_concept_id") or "")
            if not concept_id:
                return None
            binding_id = str(payload.get("id") or f"pbind_{uuid4().hex}")
            cur.execute(
                """
                insert into context_platform.parameter_bindings
                  (id, stable_key, source_parameter_id, required_concept_id, representation_id,
                   representation_schema_id, confidence, review_status, status, metadata)
                values (%s, %s, %s, %s, %s, %s, %s, 'approved', 'approved', %s::jsonb)
                on conflict (id) do update
                  set review_status = 'approved',
                      status = 'approved',
                      updated_at = now()
                """,
                [
                    binding_id,
                    str(payload.get("stable_key") or binding_id),
                    source_parameter_id,
                    concept_id,
                    representation_id,
                    payload.get("representation_schema_id"),
                    payload.get("confidence"),
                    json.dumps(
                        {
                            "transform_spec": payload.get("transform_spec") if isinstance(payload.get("transform_spec"), dict) else {"type": "none"},
                            "normalization_rule": payload.get("normalization_rule") if isinstance(payload.get("normalization_rule"), dict) else {},
                            "enum_mapping": payload.get("enum_mapping") if isinstance(payload.get("enum_mapping"), dict) else {},
                            "evidence": proposal.get("evidence") or [],
                        }
                    ),
                ],
            )
            return binding_id
        if source_field_id:
            binding_id = str(payload.get("id") or f"fbind_{uuid4().hex}")
            cur.execute(
                """
                insert into context_platform.field_bindings
                  (id, stable_key, source_field_id, representation_id, representation_schema_id,
                   confidence, review_status, status, metadata)
                values (%s, %s, %s, %s, %s, %s, 'approved', 'approved', %s::jsonb)
                on conflict (id) do update
                  set review_status = 'approved',
                      status = 'approved',
                      updated_at = now()
                """,
                [
                    binding_id,
                    str(payload.get("stable_key") or binding_id),
                    source_field_id,
                    representation_id,
                    payload.get("representation_schema_id"),
                    payload.get("confidence"),
                    json.dumps(
                        {
                            "transform_spec": payload.get("transform_spec") if isinstance(payload.get("transform_spec"), dict) else {"type": "none"},
                            "normalization_rule": payload.get("normalization_rule") if isinstance(payload.get("normalization_rule"), dict) else {},
                            "enum_mapping": payload.get("enum_mapping") if isinstance(payload.get("enum_mapping"), dict) else {},
                            "evidence": proposal.get("evidence") or [],
                        }
                    ),
                ],
            )
            return binding_id
        return None

    def _ensure_capability_from_proposal(self, cur: Any, proposal: dict[str, Any], context: dict[str, Any], *, reviewer: str) -> str | None:
        payload = proposal.get("payload") if isinstance(proposal.get("payload"), dict) else {}
        capability = payload.get("capability") if isinstance(payload.get("capability"), dict) else {}
        capability_key = str(capability.get("capability_key") or proposal.get("entity_id") or "").strip()
        if not capability_key:
            return None
        cur.execute("select id from context_platform.capabilities where capability_key = %s limit 1", [capability_key])
        row = cur.fetchone()
        if row:
            capability_id = str(row["id"])
            cur.execute(
                """
                update context_platform.capabilities
                set status = 'approved',
                    lifecycle = 'approved',
                    reviewed_by = %s,
                    approved_at = now(),
                    updated_at = now()
                where id = %s
                """,
                [reviewer, capability_id],
            )
        else:
            capability_id = f"cap_{uuid4().hex}"
            cur.execute(
                """
                insert into context_platform.capabilities
                  (id, capability_key, namespace, name, description, intent_spec, lifecycle, status,
                   metadata, created_by, reviewed_by, approved_at, evidence, confidence)
                values (%s, %s, %s, %s, %s, %s::jsonb, 'approved', 'approved',
                        %s::jsonb, 'proposal_bundle', %s, now(), %s::jsonb, %s)
                """,
                [
                    capability_id,
                    capability_key,
                    str(capability.get("namespace") or "public"),
                    str(capability.get("name") or capability_key),
                    str(capability.get("description") or ""),
                    json.dumps(capability.get("intent_spec") if isinstance(capability.get("intent_spec"), dict) else {}),
                    json.dumps(capability.get("metadata") if isinstance(capability.get("metadata"), dict) else {}),
                    reviewer,
                    json.dumps(proposal.get("evidence") or []),
                    payload.get("confidence"),
                ],
            )
        context["capabilities_by_key"][capability_key] = capability_id
        for index, item in enumerate(payload.get("inputs") if isinstance(payload.get("inputs"), list) else []):
            representation_id = self._resolve_capability_io_class_slot(cur, item, context, proposal.get("evidence") or [], reviewer=reviewer)
            if representation_id:
                self._ensure_capability_input(cur, capability_id, representation_id, item, index)
        for index, item in enumerate(payload.get("outputs") if isinstance(payload.get("outputs"), list) else []):
            representation_id = self._resolve_capability_io_class_slot(cur, item, context, proposal.get("evidence") or [], reviewer=reviewer)
            if representation_id:
                self._ensure_capability_output(cur, capability_id, representation_id, item, index)
        self._ensure_capability_operation(cur, capability_id, payload.get("operation_link") if isinstance(payload.get("operation_link"), dict) else {}, proposal.get("evidence") or [], reviewer=reviewer)
        return capability_id

    def _ensure_capability_operation_from_proposal(self, cur: Any, proposal: dict[str, Any], context: dict[str, Any], *, reviewer: str) -> str | None:
        payload = proposal.get("payload") if isinstance(proposal.get("payload"), dict) else {}
        capability_key = str(payload.get("capability_key") or "").strip()
        capability_id = context.get("capabilities_by_key", {}).get(capability_key)
        if not capability_id and capability_key:
            cur.execute("select id from context_platform.capabilities where capability_key = %s limit 1", [capability_key])
            row = cur.fetchone()
            capability_id = str(row["id"]) if row else ""
        if not capability_id:
            return None
        operation_link = payload.get("operation_link") if isinstance(payload.get("operation_link"), dict) else {}
        if not operation_link.get("source_operation_id") and payload.get("source_operation_id"):
            operation_link = {**operation_link, "source_operation_id": payload.get("source_operation_id")}
        return self._ensure_capability_operation(cur, capability_id, operation_link, proposal.get("evidence") or [], reviewer=reviewer)

    def _ensure_canonical_class(self, cur: Any, name: str, *, description: str, evidence: list[Any], reviewer: str) -> str:
        cur.execute("select id from context_platform.canonical_classes where namespace = 'public' and name = %s limit 1", [name])
        row = cur.fetchone()
        if row:
            class_id = str(row["id"])
            cur.execute("update context_platform.canonical_classes set status = 'approved', lifecycle = 'approved', reviewed_by = %s, approved_at = now(), updated_at = now() where id = %s", [reviewer, class_id])
            return class_id
        class_id = f"cclass_{uuid4().hex}"
        cur.execute(
            """
            insert into context_platform.canonical_classes
              (id, namespace, name, description, lifecycle, status, created_by, reviewed_by, approved_at, evidence)
            values (%s, 'public', %s, %s, 'approved', 'approved', 'proposal_bundle', %s, now(), %s::jsonb)
            """,
            [class_id, name, description, reviewer, json.dumps(evidence)],
        )
        return class_id

    def _ensure_canonical_slot(self, cur: Any, name: str, *, datatype: str, description: str, aliases: list[Any], identity_role: str, evidence: list[Any], reviewer: str) -> str:
        cur.execute("select id from context_platform.canonical_slots where namespace = 'public' and name = %s limit 1", [name])
        row = cur.fetchone()
        range_ref = _canonical_range(datatype)
        self._ensure_canonical_type(cur, range_ref, reviewer=reviewer)
        if row:
            slot_id = str(row["id"])
            cur.execute("update context_platform.canonical_slots set status = 'approved', lifecycle = 'approved', reviewed_by = %s, approved_at = now(), updated_at = now() where id = %s", [reviewer, slot_id])
            return slot_id
        slot_id = f"cslot_{uuid4().hex}"
        cur.execute(
            """
            insert into context_platform.canonical_slots
              (id, namespace, name, description, range_kind, range_ref, datatype, aliases,
               annotations, constraints, identity_role, lifecycle, status, created_by, reviewed_by, approved_at, evidence)
            values (%s, 'public', %s, %s, 'type', %s, %s, %s::jsonb,
                    '{}'::jsonb, '{}'::jsonb, %s, 'approved', 'approved', 'proposal_bundle', %s, now(), %s::jsonb)
            """,
            [slot_id, name, description, range_ref, range_ref, json.dumps(aliases), identity_role, reviewer, json.dumps(evidence)],
        )
        return slot_id

    def _ensure_canonical_type(self, cur: Any, name: str, *, reviewer: str) -> str:
        normalized = _canonical_range(name)
        cur.execute("select id from context_platform.canonical_types where namespace = 'public' and name = %s limit 1", [normalized])
        row = cur.fetchone()
        if row:
            type_id = str(row["id"])
            cur.execute("update context_platform.canonical_types set status = 'approved', lifecycle = 'approved', reviewed_by = coalesce(reviewed_by, %s), approved_at = coalesce(approved_at, now()), updated_at = now() where id = %s", [reviewer, type_id])
            return type_id
        type_id = f"ctype_{normalized}"
        cur.execute(
            """
            insert into context_platform.canonical_types
              (id, namespace, name, description, base_type, lifecycle, status, created_by, reviewed_by, approved_at)
            values (%s, 'public', %s, %s, %s, 'approved', 'approved', 'system', %s, now())
            on conflict (namespace, name) do update
              set status = 'approved',
                  lifecycle = 'approved',
                  updated_at = now()
            """,
            [type_id, normalized, f"Primitive {normalized} value.", normalized, reviewer],
        )
        return type_id

    def _ensure_canonical_class_slot(self, cur: Any, class_id: str, slot_id: str, *, usage_name: str, evidence: list[Any], reviewer: str) -> str:
        cur.execute("select id from context_platform.canonical_class_slots where class_id = %s and slot_id = %s limit 1", [class_id, slot_id])
        row = cur.fetchone()
        if row:
            class_slot_id = str(row["id"])
            cur.execute("update context_platform.canonical_class_slots set status = 'approved', reviewed_by = %s, approved_at = now(), updated_at = now() where id = %s", [reviewer, class_slot_id])
            return class_slot_id
        class_slot_id = f"cclassslot_{uuid4().hex}"
        cur.execute(
            """
            insert into context_platform.canonical_class_slots
              (id, class_id, slot_id, usage_name, status, created_by, reviewed_by, approved_at, evidence)
            values (%s, %s, %s, %s, 'approved', 'proposal_bundle', %s, now(), %s::jsonb)
            """,
            [class_slot_id, class_id, slot_id, usage_name, reviewer, json.dumps(evidence)],
        )
        return class_slot_id

    def _ensure_object_type(self, cur: Any, name: str, *, description: str, evidence: list[Any]) -> str:
        stable_key = name if str(name).startswith("object.") else f"object.{_to_snake(name)}"
        cur.execute("select id from context_platform.object_types where stable_key = %s limit 1", [stable_key])
        row = cur.fetchone()
        if row:
            object_type_id = str(row["id"])
            cur.execute("update context_platform.object_types set status = 'approved', updated_at = now() where id = %s", [object_type_id])
            return object_type_id
        object_type_id = stable_key
        cur.execute(
            """
            insert into context_platform.object_types
              (id, stable_key, name, description, status, evidence)
            values (%s, %s, %s, %s, 'approved', %s::jsonb)
            on conflict (id) do update
              set status = 'approved',
                  updated_at = now()
            """,
            [object_type_id, stable_key, name, description, json.dumps(evidence)],
        )
        return object_type_id

    def _ensure_property_type(self, cur: Any, name: str, *, datatype: str, description: str, evidence: list[Any]) -> str:
        stable_key = name if str(name).startswith("property.") else f"property.{_to_snake(name)}"
        broad_datatype = _canonical_range(datatype)
        cur.execute("select id from context_platform.property_types where stable_key = %s limit 1", [stable_key])
        row = cur.fetchone()
        if row:
            property_type_id = str(row["id"])
            cur.execute(
                """
                update context_platform.property_types
                set broad_datatype = %s,
                    status = 'approved',
                    updated_at = now()
                where id = %s
                """,
                [broad_datatype, property_type_id],
            )
            return property_type_id
        property_type_id = stable_key
        cur.execute(
            """
            insert into context_platform.property_types
              (id, stable_key, name, description, broad_datatype, status, evidence)
            values (%s, %s, %s, %s, %s, 'approved', %s::jsonb)
            on conflict (id) do update
              set status = 'approved',
                  updated_at = now()
            """,
            [property_type_id, stable_key, _to_snake(name), description, broad_datatype, json.dumps(evidence)],
        )
        return property_type_id

    def _ensure_concept_for_legacy_slot(self, cur: Any, slot_name: str, *, evidence: list[Any]) -> str:
        normalized = _to_snake(slot_name)
        stable_key = f"concept.global.{normalized}"
        cur.execute("select id from context_platform.concepts where stable_key = %s limit 1", [stable_key])
        row = cur.fetchone()
        if row:
            concept_id = str(row["id"])
            cur.execute("update context_platform.concepts set status = 'approved', updated_at = now() where id = %s", [concept_id])
            return concept_id
        cur.execute(
            """
            insert into context_platform.concepts
              (id, stable_key, meaning_scope_id, kind, label_en, definition, status)
            values (%s, %s, 'meaning_scope.global', 'metric_concept', %s, %s, 'approved')
            on conflict (id) do update
              set status = 'approved',
                  updated_at = now()
            """,
            [stable_key, stable_key, slot_name, f"Legacy canonical slot concept for {slot_name}."],
        )
        return stable_key

    def _ensure_canonical_representation(
        self,
        cur: Any,
        concept_id: str,
        object_type_id: str,
        property_type_id: str,
        slot_name: str,
        proposed: dict[str, Any],
        *,
        evidence: list[Any],
    ) -> str:
        representation_kind = str(proposed.get("representation_kind") or "observation_value")
        stable_key = str(proposed.get("representation_key") or f"repr.{concept_id.removeprefix('concept.')}.{_to_snake(property_type_id.removeprefix('property.'))}")
        cur.execute("select id from context_platform.canonical_representations where stable_key = %s limit 1", [stable_key])
        row = cur.fetchone()
        if row:
            representation_id = str(row["id"])
            cur.execute("update context_platform.canonical_representations set status = 'approved', updated_at = now() where id = %s", [representation_id])
        else:
            representation_id = stable_key
            cur.execute(
                """
                insert into context_platform.canonical_representations
                  (id, stable_key, concept_id, carrier_object_type_id, value_property_type_id,
                   fixed_context_json, required_context_json, representation_kind, status, evidence_id, metadata)
                values (%s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, 'approved', null, %s::jsonb)
                on conflict (id) do update
                  set status = 'approved',
                      updated_at = now()
                """,
                [
                    representation_id,
                    stable_key,
                    concept_id,
                    object_type_id,
                    property_type_id,
                    json.dumps({"concept": concept_id}),
                    json.dumps(proposed.get("required_context") if isinstance(proposed.get("required_context"), list) else []),
                    representation_kind,
                    json.dumps({"legacy_slot_name": slot_name, "evidence": evidence}),
                ],
            )
        datatype = _canonical_range(str(proposed.get("datatype") or "string"))
        schema_key = f"schema.{stable_key.removeprefix('repr.')}.{datatype}"
        cur.execute("select id from context_platform.representation_schemas where stable_key = %s limit 1", [schema_key])
        schema_row = cur.fetchone()
        if schema_row:
            cur.execute("update context_platform.representation_schemas set status = 'approved', updated_at = now() where id = %s", [schema_row["id"]])
        else:
            cur.execute(
                """
                insert into context_platform.representation_schemas
                  (id, stable_key, representation_id, datatype, pattern, cardinality, required,
                   examples_json, validation_json, status)
                values (%s, %s, %s, %s, %s, 'one', null, %s::jsonb, %s::jsonb, 'approved')
                on conflict (id) do nothing
                """,
                [
                    schema_key,
                    schema_key,
                    representation_id,
                    datatype,
                    proposed.get("pattern"),
                    json.dumps(proposed.get("examples") if isinstance(proposed.get("examples"), list) else []),
                    json.dumps(proposed.get("validation") if isinstance(proposed.get("validation"), dict) else {}),
                ],
            )
        return representation_id

    def _resolve_capability_io_class_slot(self, cur: Any, item: dict[str, Any], context: dict[str, Any], evidence: list[Any], *, reviewer: str) -> str | None:
        representation_id = str(item.get("representation_id") or item.get("canonical_class_slot_id") or "")
        if representation_id:
            return representation_id
        canonical_ref = item.get("canonical_ref") if isinstance(item.get("canonical_ref"), dict) else {}
        return self._resolve_class_slot_ref(cur, canonical_ref, context, evidence, reviewer=reviewer)

    def _resolve_class_slot_ref(self, cur: Any, canonical_ref: dict[str, Any], context: dict[str, Any], evidence: list[Any], *, reviewer: str) -> str | None:
        class_name = str(canonical_ref.get("class_name") or "").strip()
        slot_name = str(canonical_ref.get("slot_name") or "").strip()
        if not class_name or not slot_name:
            return None
        if _is_non_business_canonical_class_name(class_name):
            return None
        key = f"{class_name}.{slot_name}"
        if context.get("class_slots_by_ref", {}).get(key):
            return str(context["class_slots_by_ref"][key])
        object_type_id = self._ensure_object_type(cur, class_name, description="", evidence=evidence)
        property_type_id = self._ensure_property_type(cur, slot_name, datatype="string", description="", evidence=evidence)
        concept_id = self._ensure_concept_for_legacy_slot(cur, slot_name, evidence=evidence)
        representation_id = self._ensure_canonical_representation(cur, concept_id, object_type_id, property_type_id, slot_name, {}, evidence=evidence)
        self._remember_class_slot(context, class_name, slot_name, representation_id)
        return representation_id

    def _remember_class_slot(self, context: dict[str, Any], class_name: str, slot_name: str, class_slot_id: str) -> None:
        if class_name and slot_name:
            context.setdefault("class_slots_by_ref", {})[f"{class_name}.{slot_name}"] = class_slot_id

    def _ensure_capability_input(self, cur: Any, capability_id: str, representation_id: str, item: dict[str, Any], index: int) -> None:
        required = bool(item.get("required", True))
        input_order = int(item.get("input_order") or ((index + 1) * 10))
        concept_id = str(item.get("concept_id") or self._concept_id_for_representation(representation_id, cur=cur) or "")
        cur.execute("select id from context_platform.capability_inputs where capability_id = %s and representation_id = %s limit 1", [capability_id, representation_id])
        row = cur.fetchone()
        if row:
            cur.execute(
                """
                update context_platform.capability_inputs
                set concept_id = %s,
                    required = %s,
                    input_order = %s,
                    status = 'approved',
                    updated_at = now()
                where id = %s
                """,
                [concept_id or None, required, input_order, row["id"]],
            )
            return
        cur.execute(
            """
            insert into context_platform.capability_inputs
              (id, capability_id, concept_id, representation_id, representation_schema_id, required, input_order, status)
            values (%s, %s, %s, %s, %s, %s, %s, 'approved')
            """,
            [f"capin_{uuid4().hex}", capability_id, concept_id or None, representation_id, item.get("representation_schema_id"), required, input_order],
        )

    def _ensure_capability_output(self, cur: Any, capability_id: str, representation_id: str, item: dict[str, Any], index: int) -> None:
        output_order = int(item.get("output_order") or ((index + 1) * 10))
        concept_id = str(item.get("concept_id") or self._concept_id_for_representation(representation_id, cur=cur) or "")
        output_key = str(item.get("output_key") or item.get("name") or representation_id)
        cur.execute("select id from context_platform.capability_outputs where capability_id = %s and representation_id = %s limit 1", [capability_id, representation_id])
        row = cur.fetchone()
        if row:
            cur.execute(
                """
                update context_platform.capability_outputs
                set output_key = %s,
                    concept_id = %s,
                    output_order = %s,
                    status = 'approved',
                    updated_at = now()
                where id = %s
                """,
                [output_key, concept_id or None, output_order, row["id"]],
            )
            return
        cur.execute(
            """
            insert into context_platform.capability_outputs
              (id, capability_id, output_key, concept_id, representation_id, representation_schema_id,
               value_path, unit_path, period_path, is_primary, output_order, status)
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'approved')
            """,
            [
                f"capout_{uuid4().hex}",
                capability_id,
                output_key,
                concept_id or None,
                representation_id,
                item.get("representation_schema_id"),
                item.get("value_path"),
                item.get("unit_path"),
                item.get("period_path"),
                bool(item.get("is_primary", index == 0)),
                output_order,
            ],
        )

    def _ensure_capability_operation(self, cur: Any, capability_id: str, operation_link: dict[str, Any], evidence: list[Any], *, reviewer: str) -> str | None:
        source_operation_id = str(operation_link.get("source_operation_id") or "").strip()
        if not source_operation_id:
            return None
        cur.execute("select id from context_platform.capability_steps where capability_id = %s and source_operation_id = %s limit 1", [capability_id, source_operation_id])
        row = cur.fetchone()
        binding_spec = operation_link.get("binding_spec") if isinstance(operation_link.get("binding_spec"), dict) else {}
        if row:
            operation_link_id = str(row["id"])
            cur.execute(
                """
                update context_platform.capability_steps
                set status = 'approved',
                    binding_spec = %s::jsonb,
                    updated_at = now()
                where id = %s
                """,
                [json.dumps(binding_spec), operation_link_id],
            )
            cur.execute(
                "update context_platform.source_operations set status = 'approved', lifecycle = 'approved', reviewed_by = %s, approved_at = now(), updated_at = now() where id = %s",
                [reviewer, source_operation_id],
            )
            return operation_link_id
        operation_link_id = f"capstep_{uuid4().hex}"
        cur.execute(
            """
            insert into context_platform.capability_steps
              (id, capability_id, source_operation_id, step_order, step_kind, binding_spec, status, evidence)
            values (%s, %s, %s, %s, 'source_operation', %s::jsonb, 'approved', %s::jsonb)
            """,
            [operation_link_id, capability_id, source_operation_id, int(operation_link.get("priority") or operation_link.get("step_order") or 100), json.dumps(binding_spec), json.dumps(evidence)],
        )
        cur.execute(
            "update context_platform.source_operations set status = 'approved', lifecycle = 'approved', reviewed_by = %s, approved_at = now(), updated_at = now() where id = %s",
            [reviewer, source_operation_id],
        )
        return operation_link_id

    def list_capabilities(self, query: str = "", status: str = "") -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if query:
            clauses.append("(capability_key ilike %s or name ilike %s or description ilike %s)")
            like = f"%{query}%"
            params.extend([like, like, like])
        if status:
            clauses.append("status = any(%s)")
            params.append(_status_values(status))
        return self._select_many("capabilities", clauses, params, "namespace, capability_key")

    def get_capability(self, capability_id: str) -> dict[str, Any] | None:
        return self._select_one("capabilities", "id = %s", [capability_id])

    def create_capability(self, payload: dict[str, Any]) -> dict[str, Any]:
        record = {
            "id": str(payload.get("id") or f"cap_{uuid4().hex}"),
            "capability_key": _required(payload, "capability_key"),
            "namespace": str(payload.get("namespace") or "public"),
            "name": str(payload.get("name") or payload.get("capability_key") or ""),
            "description": str(payload.get("description") or ""),
            "intent_spec": _object(payload.get("intent_spec")),
            "version": str(payload.get("version") or "1.0.0"),
            "lifecycle": str(payload.get("lifecycle") or "draft"),
            "status": str(payload.get("status") or "draft"),
            "metadata": _object(payload.get("metadata")),
            "created_by": str(payload.get("created_by") or "system"),
            "reviewed_by": payload.get("reviewed_by"),
            "approved_at": payload.get("approved_at"),
            "evidence": _array(payload.get("evidence")),
            "confidence": payload.get("confidence"),
        }
        return self._insert("capabilities", record, json_columns={"intent_spec", "metadata", "evidence"})

    def list_capability_operations(self, capability_id: str = "") -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if capability_id:
            clauses.append("capability_id = %s")
            params.append(capability_id)
        records = self._select_many("capability_steps", clauses, params, "step_order, id")
        return [
            {
                **item,
                "priority": item.get("step_order"),
                "operation_link_kind": item.get("step_kind") or "source_operation",
            }
            for item in records
        ]

    def create_proposal(self, payload: dict[str, Any]) -> dict[str, Any]:
        record = {
            "id": str(payload.get("id") or f"proposal_{uuid4().hex}"),
            "source_type": str(payload.get("source_type") or "system"),
            "title": _required(payload, "title"),
            "entity_type": _required(payload, "entity_type"),
            "entity_id": _required(payload, "entity_id"),
            "change_type": str(payload.get("change_type") or "create"),
            "payload": _object(payload.get("payload")),
            "rationale": str(payload.get("rationale") or ""),
            "evidence": _array(payload.get("evidence")),
            "proposed_by": str(payload.get("proposed_by") or "system"),
            "reviewed_by": payload.get("reviewed_by"),
            "reviewed_at": payload.get("reviewed_at"),
            "approved_at": payload.get("approved_at"),
            "status": str(payload.get("status") or "proposed"),
        }
        return self._insert("proposals", record, json_columns={"payload", "evidence"})

    def list_proposals(self, status: str = "", entity_type: str = "", query: str = "") -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if status:
            normalized_status = "proposed" if status == "pending_review" else status
            clauses.append("status = %s")
            params.append(normalized_status)
        if entity_type:
            clauses.append("entity_type = %s")
            params.append(entity_type)
        if query:
            clauses.append("(title ilike %s or entity_type ilike %s or change_type ilike %s)")
            like = f"%{query}%"
            params.extend([like, like, like])
        return self._select_many("proposals", clauses, params, "created_at desc")

    def create_onboarding_run(self, payload: dict[str, Any]) -> dict[str, Any]:
        record = {
            "id": str(payload.get("id") or f"run_{uuid4().hex}"),
            "source_id": _required(payload, "source_id"),
            "source_document_id": payload.get("source_document_id"),
            "status": str(payload.get("status") or "started"),
            "stage": str(payload.get("stage") or "source_uploaded"),
            "created_by": str(payload.get("created_by") or "system"),
            "metadata": _object(payload.get("metadata")),
        }
        return self._insert("onboarding_runs", record, json_columns={"metadata"})

    def list_onboarding_runs(self, source_document_id: str = "", status: str = "") -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if source_document_id:
            clauses.append("source_document_id = %s")
            params.append(source_document_id)
        if status:
            clauses.append("status = %s")
            params.append(status)
        return self._select_many("onboarding_runs", clauses, params, "started_at desc")

    def get_onboarding_run(self, run_id: str) -> dict[str, Any] | None:
        return self._select_one("onboarding_runs", "id = %s", [run_id])

    def update_onboarding_run(
        self,
        run_id: str,
        *,
        status: str | None = None,
        stage: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        assignments = ["updated_at = now()"]
        params: dict[str, Any] = {"run_id": run_id}
        if status is not None:
            assignments.append("status = %(status)s")
            params["status"] = status
            if status in {"completed", "failed", "cancelled"}:
                assignments.append("completed_at = now()")
        if stage is not None:
            assignments.append("stage = %(stage)s")
            params["stage"] = stage
        if metadata is not None:
            assignments.append("metadata = %(metadata)s::jsonb")
            params["metadata"] = json.dumps(metadata)
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute(
                    f"""
                    update context_platform.onboarding_runs
                    set {', '.join(assignments)}
                    where id = %(run_id)s
                    returning *
                    """,
                    params,
                )
                row = cur.fetchone()
            conn.commit()
        return _row(row) if row else None

    def save_plan(self, payload: dict[str, Any]) -> dict[str, Any]:
        record = {
            "id": str(payload.get("plan_id") or payload.get("id") or f"plan_{uuid4().hex}"),
            "selected_capability_id": payload.get("selected_capability_id"),
            "selected_source_operation_id": payload.get("selected_source_operation_id"),
            "status": str(payload.get("status") or "draft"),
            "canonical_inputs": _object(payload.get("canonical_inputs")),
            "parameter_bindings": _array(payload.get("parameter_bindings")),
            "expected_outputs": _array(payload.get("expected_outputs")),
            "confidence": payload.get("confidence"),
            "requires_confirmation": bool(payload.get("requires_confirmation", False)),
            "validation_result": _object(payload.get("validation")),
            "request_payload": _object(payload.get("request_payload")),
            "plan_payload": _object(payload.get("plan_payload") or payload),
            "created_by": str(payload.get("created_by") or "planner"),
        }
        return self._insert(
            "plans",
            record,
            json_columns={"canonical_inputs", "parameter_bindings", "expected_outputs", "validation_result", "request_payload", "plan_payload"},
        )

    def get_plan(self, plan_id: str) -> dict[str, Any] | None:
        return self._select_one("plans", "id = %s", [plan_id])

    def list_plans(self, status: str = "") -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if status:
            clauses.append("status = %s")
            params.append(status)
        return self._select_many("plans", clauses, params, "created_at desc")

    def create_execution(self, plan_id: str, *, status: str, request_payload: dict[str, Any], result_payload: dict[str, Any]) -> dict[str, Any]:
        record = {
            "id": f"exec_{uuid4().hex}",
            "plan_id": plan_id,
            "status": status,
            "request_payload": request_payload,
            "result_payload": result_payload,
            "completed_at": _now() if status not in {"started", "running"} else None,
        }
        return self._insert("executions", record, json_columns={"request_payload", "result_payload"})

    def list_executions(self, status: str = "") -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if status:
            clauses.append("status = %s")
            params.append(status)
        return self._select_many("executions", clauses, params, "started_at desc")

    def _select_many(self, table: str, clauses: list[str], params: list[Any], order_by: str) -> list[dict[str, Any]]:
        where_sql = f"where {' and '.join(clauses)}" if clauses else ""
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute(
                    f"select * from context_platform.{table} {where_sql} order by {order_by}",
                    params,
                )
                return [_row(row) for row in cur.fetchall()]

    def _select_one(self, table: str, where_sql: str, params: list[Any]) -> dict[str, Any] | None:
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute(
                    f"select * from context_platform.{table} where {where_sql} limit 1",
                    params,
                )
                row = cur.fetchone()
                return _row(row) if row else None

    def _insert(self, table: str, record: dict[str, Any], *, json_columns: set[str], returning: str = "*") -> dict[str, Any]:
        columns = list(record.keys())
        placeholders = [f"%({column})s::jsonb" if column in json_columns else f"%({column})s" for column in columns]
        params = {
            key: json.dumps(value) if key in json_columns else value
            for key, value in record.items()
        }
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute(
                    f"""
                    insert into context_platform.{table} ({', '.join(columns)})
                    values ({', '.join(placeholders)})
                    returning {returning}
                    """,
                    params,
                )
                inserted = _row(cur.fetchone())
            conn.commit()
        return inserted

    def _concept_id_for_representation(self, representation_id: str, *, cur: Any | None = None) -> str | None:
        if not representation_id:
            return None
        if cur is not None:
            cur.execute("select concept_id from context_platform.canonical_representations where id = %s limit 1", [representation_id])
            row = cur.fetchone()
            return str(row["concept_id"]) if row and row.get("concept_id") else None
        with self._connect() as conn:
            with self._dict_cursor(conn) as local_cur:
                local_cur.execute("select concept_id from context_platform.canonical_representations where id = %s limit 1", [representation_id])
                row = local_cur.fetchone()
                return str(row["concept_id"]) if row and row.get("concept_id") else None

    def _connect(self) -> Any:
        from psycopg import connect

        return connect(self.database_url)

    def _dict_cursor(self, conn: Any) -> Any:
        from psycopg.rows import dict_row

        return conn.cursor(row_factory=dict_row)


def _required(payload: dict[str, Any], key: str) -> str:
    value = str(payload.get(key) or "").strip()
    if not value:
        raise ValueError(f"{key} is required")
    return value


def _object(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _array(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _status_values(status: str) -> list[str]:
    if not status:
        return []
    if status in {"approved", "active", "published"}:
        return ["approved", "active", "published"]
    if status == "reviewed":
        return ["reviewed", "approved", "active", "published"]
    return [status]


def _canonical_range(datatype: str) -> str:
    value = str(datatype or "string").strip().lower()
    if value in {"number", "decimal", "float", "double"}:
        return "decimal"
    if value in {"integer", "int"}:
        return "integer"
    if value in {"boolean", "bool"}:
        return "boolean"
    if value in {"date", "datetime", "time"}:
        return value
    return "string"


def _find_by_id(items: list[dict[str, Any]], item_id: str) -> dict[str, Any] | None:
    return next((item for item in items if item.get("id") == item_id), None)


def _is_non_business_canonical_class_name(value: str) -> bool:
    normalized = re.sub(r"[^a-z0-9_]+", "", _to_snake(value).lower())
    blocked = {
        "api_response",
        "apiresponse",
        "envelope",
        "parameter",
        "record",
        "request",
        "request_context",
        "requestcontext",
        "response",
        "response_envelope",
        "responseenvelope",
        "result",
    }
    return normalized in blocked or normalized.replace("_", "") in blocked


def _to_snake(value: str) -> str:
    value = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", str(value or ""))
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", value)
    return re.sub(r"[^a-zA-Z0-9]+", "_", value).strip("_").lower()


def _workflow_step(
    key: str,
    number: int,
    title: str,
    state: str,
    detail: str,
    depends_on: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "key": key,
        "number": number,
        "title": title,
        "state": state,
        "detail": detail,
        "depends_on": depends_on or [],
    }


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _row(row: Any) -> dict[str, Any]:
    result = dict(row or {})
    for key, value in list(result.items()):
        if isinstance(value, datetime):
            result[key] = value.astimezone(UTC).isoformat()
    return result
