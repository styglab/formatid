from __future__ import annotations

import json
from decimal import Decimal
import os
import re
import threading
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from core.runtime.runtime_db.url import get_database_url
from services.semantic_platform.internal.storage.execution_repository import (
    ExecutionRepositoryMixin,
)
from services.semantic_platform.internal.storage.governance_repository import (
    GovernanceRepositoryMixin,
)
from services.semantic_platform.internal.storage.onboarding_repository import (
    OnboardingRepositoryMixin,
)
from services.semantic_platform.internal.storage.registry_repository import (
    RegistryRepositoryMixin,
)
from services.semantic_platform.internal.onboarding.stages import (
    build_onboarding_stage_task_records,
)
from services.semantic_platform.internal.semantic import seed_core_semantic_types


_STORE_LOCK = threading.Lock()
_NAME_PATTERN = re.compile(r"^[A-Z][A-Za-z0-9]*$")
_SUPPORTED_DATATYPES = {"string", "number", "integer", "boolean", "date", "datetime", "object", "array"}
_SUPPORTED_ENTITY_KINDS = {"entity", "attribute"}
_SUPPORTED_SOURCE_TYPES = {"api", "table", "file", "stream", "queue", "other"}
_SUPPORTED_MAPPING_TYPES = {"exact", "transform", "composite", "enum", "reference"}
_SUPPORTED_CANONICAL_RELATION_TYPES = {"has_attribute", "belongs_to", "references", "contains", "issued_by", "awarded_to", "related_to"}
_CONTROL_PLANE_SCHEMA_SQL = """
create schema if not exists semantic_platform;

create table if not exists semantic_platform.onboarding_runs (
  id text primary key,
  source_id text not null references semantic_platform.execution_sources(id) on delete cascade,
  status text not null default 'started',
  stage text not null default 'source_uploaded',
  current_stage text not null default 'source_review',
  stage_status text not null default 'pending',
  run_mode text not null default 'ai_assisted',
  next_action text not null default 'Review source evidence and start onboarding tasks.',
  trigger_type text not null default 'source_upload',
  created_by text not null default 'system',
  metadata jsonb not null default '{}'::jsonb,
  started_at timestamptz not null default now(),
  completed_at timestamptz,
  updated_at timestamptz not null default now()
);

create table if not exists semantic_platform.evidence_snapshots (
  id text primary key,
  run_id text not null references semantic_platform.onboarding_runs(id) on delete cascade,
  source_id text not null references semantic_platform.execution_sources(id) on delete cascade,
  snapshot_type text not null default 'source_upload',
  content_hash text not null default '',
  source_ref jsonb not null default '{}'::jsonb,
  operation_evidence jsonb not null default '[]'::jsonb,
  schema_evidence jsonb not null default '[]'::jsonb,
  sample_values jsonb not null default '{}'::jsonb,
  ai_context jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create table if not exists semantic_platform.proposal_bundles (
  id text primary key,
  run_id text not null references semantic_platform.onboarding_runs(id) on delete cascade,
  source_id text not null references semantic_platform.execution_sources(id) on delete cascade,
  evidence_snapshot_id text references semantic_platform.evidence_snapshots(id) on delete set null,
  title text not null,
  status text not null default 'draft',
  summary jsonb not null default '{}'::jsonb,
  created_by text not null default 'system',
  reviewed_by text,
  reviewed_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists semantic_platform.proposal_bundle_items (
  bundle_id text not null references semantic_platform.proposal_bundles(id) on delete cascade,
  proposal_id text not null references semantic_platform.proposals(id) on delete cascade,
  item_order integer not null default 100,
  created_at timestamptz not null default now(),
  primary key (bundle_id, proposal_id)
);

create table if not exists semantic_platform.work_queue_tasks (
  id text primary key,
  run_id text not null references semantic_platform.onboarding_runs(id) on delete cascade,
  source_id text not null references semantic_platform.execution_sources(id) on delete cascade,
  evidence_snapshot_id text references semantic_platform.evidence_snapshots(id) on delete set null,
  operation_id text references semantic_platform.execution_operations(id) on delete cascade,
  field_id text references semantic_platform.operation_fields(id) on delete cascade,
  stage text not null default 'source_review',
  task_type text not null,
  status text not null default 'open',
  supports_ai_draft boolean not null default true,
  draft_status text not null default 'not_started',
  depends_on jsonb not null default '[]'::jsonb,
  recommended_action text not null default '',
  draft_payload jsonb not null default '{}'::jsonb,
  draft_rationale text not null default '',
  draft_confidence numeric(5,4),
  priority integer not null default 100,
  title text not null,
  payload jsonb not null default '{}'::jsonb,
  proposal_id text references semantic_platform.proposals(id) on delete set null,
  assigned_to text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists semantic_platform.capability_operation_bindings (
  id text primary key,
  capability_id text not null references semantic_platform.capabilities(id) on delete cascade,
  operation_id text not null references semantic_platform.execution_operations(id) on delete cascade,
  variant_id text references semantic_platform.operation_variants(id) on delete set null,
  run_id text references semantic_platform.onboarding_runs(id) on delete set null,
  evidence_snapshot_id text references semantic_platform.evidence_snapshots(id) on delete set null,
  binding_kind text not null default 'implementation',
  input_bindings jsonb not null default '[]'::jsonb,
  output_bindings jsonb not null default '[]'::jsonb,
  fixed_arguments jsonb not null default '{}'::jsonb,
  status text not null default 'draft',
  confidence numeric(5,4),
  created_by text not null default 'system',
  reviewed_by text,
  approved_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists idx_onboarding_runs_source_id on semantic_platform.onboarding_runs (source_id);
create index if not exists idx_evidence_snapshots_run_id on semantic_platform.evidence_snapshots (run_id);
create index if not exists idx_proposal_bundles_run_id on semantic_platform.proposal_bundles (run_id);
create index if not exists idx_proposal_bundle_items_proposal_id on semantic_platform.proposal_bundle_items (proposal_id);
create index if not exists idx_work_queue_tasks_run_status on semantic_platform.work_queue_tasks (run_id, status);
create index if not exists idx_work_queue_tasks_field_id on semantic_platform.work_queue_tasks (field_id);
create index if not exists idx_capability_operation_bindings_capability_id on semantic_platform.capability_operation_bindings (capability_id);
create index if not exists idx_capability_operation_bindings_operation_id on semantic_platform.capability_operation_bindings (operation_id);

create table if not exists semantic_platform.access_path_checks (
  id text primary key,
  access_path_id text not null,
  operation_id text not null,
  variant_id text,
  method text not null default '',
  locator text not null default '',
  status text not null default 'recorded',
  response_status integer,
  response_excerpt text not null default '',
  evidence jsonb not null default '{}'::jsonb,
  checked_at timestamptz not null default now(),
  created_at timestamptz not null default now()
);

alter table semantic_platform.access_path_checks add column if not exists operation_id text not null default '';
alter table semantic_platform.access_path_checks add column if not exists variant_id text;
alter table semantic_platform.access_path_checks add column if not exists method text not null default '';
alter table semantic_platform.access_path_checks add column if not exists locator text not null default '';
alter table semantic_platform.access_path_checks add column if not exists status text not null default 'recorded';
alter table semantic_platform.access_path_checks add column if not exists response_status integer;
alter table semantic_platform.access_path_checks add column if not exists response_excerpt text not null default '';
alter table semantic_platform.access_path_checks add column if not exists evidence jsonb not null default '{}'::jsonb;
alter table semantic_platform.access_path_checks add column if not exists checked_at timestamptz not null default now();

create index if not exists idx_access_path_checks_operation_id on semantic_platform.access_path_checks (operation_id);

alter table semantic_platform.onboarding_runs add column if not exists current_stage text not null default 'source_review';
alter table semantic_platform.onboarding_runs add column if not exists stage_status text not null default 'pending';
alter table semantic_platform.onboarding_runs add column if not exists run_mode text not null default 'ai_assisted';
alter table semantic_platform.onboarding_runs add column if not exists next_action text not null default 'Review source evidence and start onboarding tasks.';

alter table semantic_platform.work_queue_tasks add column if not exists stage text not null default 'source_review';
alter table semantic_platform.work_queue_tasks add column if not exists supports_ai_draft boolean not null default true;
alter table semantic_platform.work_queue_tasks add column if not exists draft_status text not null default 'not_started';
alter table semantic_platform.work_queue_tasks add column if not exists depends_on jsonb not null default '[]'::jsonb;
alter table semantic_platform.work_queue_tasks add column if not exists recommended_action text not null default '';
alter table semantic_platform.work_queue_tasks add column if not exists draft_payload jsonb not null default '{}'::jsonb;
alter table semantic_platform.work_queue_tasks add column if not exists draft_rationale text not null default '';
alter table semantic_platform.work_queue_tasks add column if not exists draft_confidence numeric(5,4);
"""


class SemanticLayerRepository(
    RegistryRepositoryMixin,
    ExecutionRepositoryMixin,
    OnboardingRepositoryMixin,
    GovernanceRepositoryMixin,
):
    """Semantic layer repository.

    Runtime uses Postgres by default. Tests and isolated local workflows may
    still pass an explicit ``store_path`` to use a small file-backed store.
    """

    def __init__(
        self,
        store_path: str | None = None,
        database_url: str | None = None,
    ) -> None:
        self.store_path = Path(store_path) if store_path else None
        self.database_url = database_url or os.getenv("SEMANTIC_PLATFORM_DATABASE_URL") or get_database_url(
            "POSTGRES_DATABASE_URL",
            host_default="postgres",
        )

    def reset_context(self) -> dict[str, Any]:
        if self.store_path is not None:
            with _STORE_LOCK:
                self._write_store(_empty_store())
            return {"status": "reset", "backend": "file"}
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("delete from semantic_platform.capability_operation_bindings")
                cur.execute("delete from semantic_platform.work_queue_tasks")
                cur.execute("delete from semantic_platform.proposal_bundle_items")
                cur.execute("delete from semantic_platform.proposal_bundles")
                cur.execute("delete from semantic_platform.evidence_snapshots")
                cur.execute("delete from semantic_platform.onboarding_runs")
                cur.execute("delete from semantic_platform.access_path_checks")
                cur.execute("delete from semantic_platform.field_mappings")
                cur.execute("delete from semantic_platform.capability_operations")
                cur.execute("delete from semantic_platform.operation_fields")
                cur.execute("delete from semantic_platform.operation_variants")
                cur.execute("delete from semantic_platform.capabilities")
                cur.execute("delete from semantic_platform.execution_operations")
                cur.execute("delete from semantic_platform.execution_access_paths")
                cur.execute("delete from semantic_platform.execution_assets")
                cur.execute("delete from semantic_platform.execution_sources")
                cur.execute("delete from semantic_platform.proposals")
                cur.execute("delete from semantic_platform.canonical_relations")
                cur.execute("delete from semantic_platform.canonical_attributes")
                cur.execute("delete from semantic_platform.canonical_entities")
                cur.execute("delete from semantic_platform.semantic_relationships")
                cur.execute("delete from semantic_platform.semantic_types")
            conn.commit()
        return {"status": "reset", "backend": "postgres"}

    def ensure_control_plane_schema(self) -> dict[str, Any]:
        if self.store_path is not None:
            return {"status": "skipped", "backend": "file"}
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(_CONTROL_PLANE_SCHEMA_SQL)
            conn.commit()
        return {"status": "ready", "backend": "postgres"}

    def seed_semantic_type_registry(self) -> dict[str, Any]:
        if self.store_path is not None:
            return self._seed_file_store()
        created = 0
        updated = 0
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                for item in seed_core_semantic_types():
                    parent_entity_id = ""
                    parent_entity_name = ""
                    if item.get("parent_name"):
                        cur.execute(
                            """
                            select id, name
                            from semantic_platform.semantic_types
                            where lower(name) = lower(%s)
                            """,
                            (str(item["parent_name"]),),
                        )
                        parent = cur.fetchone()
                        if parent is not None:
                            parent_entity_id = parent["id"]
                            parent_entity_name = parent["name"]
                    payload = {
                        "name": item["name"],
                        "description": item.get("description", ""),
                        "datatype": item.get("datatype", "string"),
                        "aliases": item.get("aliases", []),
                        "entity_kind": "attribute" if item.get("datatype") != "object" else "entity",
                        "parent_entity_id": parent_entity_id,
                        "parent_entity_name": parent_entity_name,
                        "semantic_role": "",
                        "status": "approved",
                        "owners": ["platform"],
                        "tags": ["core"],
                        "documentation": item.get("description", ""),
                    }
                    cur.execute(
                        """
                        select id
                        from semantic_platform.semantic_types
                        where lower(name) = lower(%s)
                        """,
                        (payload["name"],),
                    )
                    existing = cur.fetchone()
                    if existing is None:
                        record = _semantic_type_record(payload)
                        cur.execute(
                            """
                            insert into semantic_platform.semantic_types (
                              id, urn, name, description, datatype, entity_kind, semantic_role,
                              parent_entity_id, parent_entity_name,
                              aliases, owners, tags, documentation, status,
                              created_at, updated_at
                            ) values (
                              %(id)s, %(urn)s, %(name)s, %(description)s, %(datatype)s, %(entity_kind)s, %(semantic_role)s,
                              %(parent_entity_id)s, %(parent_entity_name)s,
                              %(aliases)s::jsonb, %(owners)s::jsonb, %(tags)s::jsonb, %(documentation)s, %(status)s,
                              %(created_at)s::timestamptz, %(updated_at)s::timestamptz
                            )
                            """,
                            _sql_semantic_type_params(record),
                        )
                        created += 1
                    else:
                        updates = _semantic_type_updates(payload)
                        updates["updated_at"] = _now()
                        cur.execute(
                            """
                            update semantic_platform.semantic_types
                            set description = %(description)s,
                                datatype = %(datatype)s,
                                entity_kind = %(entity_kind)s,
                                semantic_role = %(semantic_role)s,
                                parent_entity_id = %(parent_entity_id)s,
                                parent_entity_name = %(parent_entity_name)s,
                                aliases = %(aliases)s::jsonb,
                                owners = %(owners)s::jsonb,
                                tags = %(tags)s::jsonb,
                                documentation = %(documentation)s,
                                status = %(status)s,
                                updated_at = %(updated_at)s::timestamptz
                            where id = %(id)s
                            """,
                            {
                                "id": existing["id"],
                                "description": updates.get("description", ""),
                                "datatype": updates.get("datatype", "string"),
                                "entity_kind": updates.get("entity_kind", "attribute"),
                                "semantic_role": updates.get("semantic_role", ""),
                                "parent_entity_id": updates.get("parent_entity_id", ""),
                                "parent_entity_name": updates.get("parent_entity_name", ""),
                                "aliases": json.dumps(updates.get("aliases", [])),
                                "owners": json.dumps(updates.get("owners", [])),
                                "tags": json.dumps(updates.get("tags", [])),
                                "documentation": updates.get("documentation", ""),
                                "status": updates.get("status", "approved"),
                                "updated_at": updates["updated_at"],
                            },
                        )
                        updated += 1
            conn.commit()
        semantic_types = self.list_semantic_types()
        return {
            "status": "seeded",
            "created": created,
            "updated": updated,
            "semantic_type_count": len(semantic_types),
            "semantic_types": semantic_types,
        }

    def overview(self) -> dict[str, Any]:
        if self.store_path is not None:
            return self._overview_file_store()
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute(
                    """
                    select
                      (select count(*) from semantic_platform.semantic_types) as semantic_types,
                      (select count(*) from semantic_platform.semantic_types where status = 'approved') as approved_semantic_types,
                      (select count(*) from semantic_platform.semantic_types where status = 'draft') as draft_semantic_types,
                      (select count(*) from semantic_platform.canonical_entities) as canonical_entities,
                      (select count(*) from semantic_platform.canonical_attributes) as canonical_attributes,
                      (select count(*) from semantic_platform.canonical_relations) as canonical_relations,
                      (select count(*) from semantic_platform.execution_sources) as execution_sources,
                      (select count(*) from semantic_platform.proposals where status = 'pending_review') as pending_proposals,
                      (select count(*) from semantic_platform.semantic_relationships) as relationships
                    """
                )
                counts = dict(cur.fetchone() or {})
                cur.execute(
                    """
                    select *
                    from semantic_platform.proposals
                    order by created_at desc
                    limit 6
                    """
                )
                recent_proposals = [_proposal_from_row(row) for row in cur.fetchall()]
        return {"counts": counts, "recent_proposals": recent_proposals}

    def _connect(self) -> Any:
        from psycopg import connect

        return connect(self.database_url)

    def _dict_cursor(self, conn: Any) -> Any:
        from psycopg.rows import dict_row

        return conn.cursor(row_factory=dict_row)

    def _seed_file_store(self) -> dict[str, Any]:
        created = 0
        updated = 0
        with _STORE_LOCK:
            store = self._read_store()
            for item in seed_core_semantic_types():
                parent_entity_id = ""
                parent_entity_name = ""
                if item.get("parent_name"):
                    parent = _find_semantic_type_by_name(store, str(item["parent_name"]))
                    if parent is not None:
                        parent_entity_id = parent["id"]
                        parent_entity_name = parent["name"]
                existing = _find_semantic_type_by_name(store, str(item["name"]))
                payload = {
                    "name": item["name"],
                    "description": item.get("description", ""),
                    "datatype": item.get("datatype", "string"),
                    "aliases": item.get("aliases", []),
                    "entity_kind": "attribute" if item.get("datatype") != "object" else "entity",
                    "parent_entity_id": parent_entity_id,
                    "parent_entity_name": parent_entity_name,
                    "status": "approved",
                    "owners": ["platform"],
                    "tags": ["core"],
                    "documentation": item.get("description", ""),
                }
                if existing is None:
                    store["semantic_types"].append(_semantic_type_record(payload))
                    created += 1
                else:
                    existing.update({**payload, "updated_at": _now()})
                    updated += 1
            self._write_store(store)
        return {
            "status": "seeded",
            "created": created,
            "updated": updated,
            "semantic_type_count": len(store["semantic_types"]),
            "semantic_types": list(store["semantic_types"]),
        }

    def _overview_file_store(self) -> dict[str, Any]:
        store = self._read_store()
        semantic_types = store["semantic_types"]
        execution_sources = store["execution_sources"]
        proposals = store["proposals"]
        return {
            "counts": {
                "semantic_types": len(semantic_types),
                "approved_semantic_types": _count_status(semantic_types, "approved"),
                "draft_semantic_types": _count_status(semantic_types, "draft"),
                "execution_sources": len(execution_sources),
                "pending_proposals": _count_status(proposals, "pending_review"),
                "relationships": len(store["relationships"]),
            },
            "recent_proposals": sorted(proposals, key=lambda item: item["created_at"], reverse=True)[:6],
        }

    def _list_semantic_types_file_store(self, query: str = "", status: str = "") -> list[dict[str, Any]]:
        records = list(self._read_store()["semantic_types"])
        if query:
            lowered = query.lower()
            records = [
                item
                for item in records
                if lowered in item["name"].lower()
                or lowered in item.get("description", "").lower()
                or any(lowered in alias.lower() for alias in item.get("aliases", []))
            ]
        if status:
            records = [item for item in records if item.get("status") == status]
        return sorted(records, key=lambda item: item["name"])

    def _get_semantic_type_file_store(self, semantic_type_id: str) -> dict[str, Any] | None:
        store = self._read_store()
        record = _find_semantic_type(store, semantic_type_id)
        if record is None:
            return None
        relationships = [
            item
            for item in store["relationships"]
            if item["source_id"] == semantic_type_id or item["target_id"] == semantic_type_id
        ]
        return {**record, "relationships": relationships}

    def _create_semantic_type_file_store(self, payload: dict[str, Any]) -> dict[str, Any]:
        with _STORE_LOCK:
            store = self._read_store()
            payload = _normalize_semantic_type_payload_file_store(store, payload)
            if _find_semantic_type_by_name(store, str(payload["name"])) is not None:
                raise ValueError(f"semantic type already exists: {payload['name']}")
            record = _semantic_type_record(payload)
            store["semantic_types"].append(record)
            proposal = _proposal_record(
                source_type="manual_authoring",
                title=f"Create semantic type {record['name']}",
                entity_type="semantic_type",
                entity_id=record["id"],
                change_type="create",
                payload=record,
            )
            store["proposals"].append(proposal)
            self._write_store(store)
        return {"semantic_type": record, "proposal": proposal}

    def _list_execution_sources_file_store(self, query: str = "", status: str = "") -> list[dict[str, Any]]:
        records = list(self._read_store()["execution_sources"])
        if query:
            lowered = query.lower()
            records = [
                item
                for item in records
                if lowered in item["name"].lower()
                or lowered in item.get("provider", "").lower()
                or lowered in item.get("description", "").lower()
            ]
        if status:
            records = [item for item in records if item.get("status") == status]
        return sorted(records, key=lambda item: item["name"])

    def _get_execution_source_file_store(self, source_id: str) -> dict[str, Any] | None:
        store = self._read_store()
        return next((item for item in store["execution_sources"] if item["id"] == source_id), None)

    def _create_execution_source_file_store(self, payload: dict[str, Any]) -> dict[str, Any]:
        with _STORE_LOCK:
            store = self._read_store()
            if _find_execution_source_by_name(store, str(payload["name"])) is not None:
                raise ValueError(f"execution source already exists: {payload['name']}")
            record = _execution_source_record(payload)
            store["execution_sources"].append(record)
            proposal = _proposal_record(
                source_type="manual_authoring",
                title=f"Create execution source {record['name']}",
                entity_type="execution_source",
                entity_id=record["id"],
                change_type="create",
                payload=record,
            )
            store["proposals"].append(proposal)
            self._write_store(store)
        return {"execution_source": record, "proposal": proposal}

    def _list_execution_assets_file_store(
        self,
        query: str = "",
        status: str = "",
        source_id: str = "",
    ) -> list[dict[str, Any]]:
        records = list(self._read_store()["execution_assets"])
        if query:
            lowered = query.lower()
            records = [
                item
                for item in records
                if lowered in item["name"].lower()
                or lowered in item.get("locator", "").lower()
                or lowered in item.get("description", "").lower()
                or lowered in item.get("source_name", "").lower()
            ]
        if status:
            records = [item for item in records if item.get("status") == status]
        if source_id:
            records = [item for item in records if item.get("source_id") == source_id]
        return sorted(records, key=lambda item: (item.get("source_name", ""), item["name"]))

    def _get_execution_asset_file_store(self, asset_id: str) -> dict[str, Any] | None:
        store = self._read_store()
        return next((item for item in store["execution_assets"] if item["id"] == asset_id), None)

    def _list_execution_operations_file_store(
        self,
        query: str = "",
        status: str = "",
        source_id: str = "",
        asset_id: str = "",
    ) -> list[dict[str, Any]]:
        records = list(self._read_store()["execution_operations"])
        if query:
            lowered = query.lower()
            records = [
                item
                for item in records
                if lowered in item["name"].lower()
                or lowered in item.get("operation_key", "").lower()
                or lowered in item.get("description", "").lower()
                or lowered in item.get("asset_name", "").lower()
            ]
        if status:
            records = [item for item in records if item.get("status") == status]
        if source_id:
            records = [item for item in records if item.get("source_id") == source_id]
        if asset_id:
            records = [item for item in records if item.get("asset_id") == asset_id]
        return sorted(records, key=lambda item: (item.get("source_name", ""), item.get("asset_name", ""), item["name"]))

    def _get_execution_operation_file_store(self, operation_id: str) -> dict[str, Any] | None:
        store = self._read_store()
        return next((item for item in store["execution_operations"] if item["id"] == operation_id), None)

    def _list_operation_fields_file_store(self, operation_id: str = "", variant_id: str = "") -> list[dict[str, Any]]:
        records = list(self._read_store()["operation_fields"])
        if operation_id:
            records = [item for item in records if item.get("operation_id") == operation_id]
        if variant_id:
            records = [item for item in records if item.get("variant_id") == variant_id]
        return sorted(records, key=lambda item: (item.get("scope", ""), item.get("raw_name", "")))

    def _list_operation_variants_file_store(
        self,
        query: str = "",
        status: str = "",
        operation_id: str = "",
    ) -> list[dict[str, Any]]:
        records = list(self._read_store().get("operation_variants", []))
        if query:
            lowered = query.lower()
            records = [
                item
                for item in records
                if lowered in item.get("name", "").lower()
                or lowered in item.get("variant_key", "").lower()
                or lowered in item.get("description", "").lower()
            ]
        if status:
            records = [item for item in records if item.get("status") == status]
        if operation_id:
            records = [item for item in records if item.get("operation_id") == operation_id]
        return sorted(records, key=lambda item: (item.get("operation_id", ""), item.get("name", "")))

    def _list_capabilities_file_store(self, query: str = "", status: str = "") -> list[dict[str, Any]]:
        records = list(self._read_store()["capabilities"])
        if query:
            lowered = query.lower()
            records = [
                item
                for item in records
                if lowered in item["name"].lower()
                or lowered in item.get("description", "").lower()
                or lowered in item.get("capability_key", "").lower()
            ]
        if status:
            records = [item for item in records if item.get("status") == status]
        return sorted(records, key=lambda item: item["name"])

    def _get_capability_file_store(self, capability_id: str) -> dict[str, Any] | None:
        store = self._read_store()
        return next((item for item in store["capabilities"] if item["id"] == capability_id), None)

    def _create_capability_file_store(self, payload: dict[str, Any]) -> dict[str, Any]:
        with _STORE_LOCK:
            store = self._read_store()
            if any(item["capability_key"].lower() == str(payload["capability_key"]).lower() for item in store["capabilities"]):
                raise ValueError(f"capability already exists: {payload['capability_key']}")
            record = _capability_record(payload)
            store["capabilities"].append(record)
            proposal = _proposal_record(
                source_type="manual_authoring",
                title=f"Create capability {record['name']}",
                entity_type="capability",
                entity_id=record["id"],
                change_type="create",
                payload=record,
            )
            store["proposals"].append(proposal)
            self._write_store(store)
        return {"capability": record, "proposal": proposal}

    def _update_capability_file_store(self, capability_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        with _STORE_LOCK:
            store = self._read_store()
            record = self._get_capability_file_store(capability_id)
            if record is None:
                raise KeyError(capability_id)
            updates = _capability_updates(payload)
            draft_snapshot = {**record, **updates, "updated_at": _now()}
            proposal = _proposal_record(
                source_type="manual_authoring",
                title=f"Update capability {record['name']}",
                entity_type="capability",
                entity_id=record["id"],
                change_type="update",
                payload={
                    "approved_snapshot": record,
                    "draft_snapshot": draft_snapshot,
                    "fields_changed": sorted(updates.keys()),
                },
            )
            store["proposals"] = [
                item
                for item in store["proposals"]
                if not (
                    item.get("entity_type") == "capability"
                    and item.get("entity_id") == capability_id
                    and item.get("change_type") == "update"
                    and item.get("status") == "pending_review"
                )
            ]
            store["proposals"].append(proposal)
            self._write_store(store)
        return {"capability": draft_snapshot, "proposal": proposal}

    def _delete_capability_file_store(self, capability_id: str) -> dict[str, Any]:
        with _STORE_LOCK:
            store = self._read_store()
            record = self._get_capability_file_store(capability_id)
            if record is None:
                raise KeyError(capability_id)
            proposal = _proposal_record(
                source_type="manual_authoring",
                title=f"Delete capability {record['name']}",
                entity_type="capability",
                entity_id=record["id"],
                change_type="delete",
                payload=dict(record),
            )
            store["proposals"].append(proposal)
            self._write_store(store)
        return {"capability": record, "proposal": proposal}

    def _list_field_mappings_file_store(self, query: str = "", status: str = "", operation_id: str = "") -> list[dict[str, Any]]:
        records = list(self._read_store()["field_mappings"])
        if query:
            lowered = query.lower()
            records = [
                item
                for item in records
                if lowered in item.get("field_path", "").lower() or lowered in item.get("notes", "").lower()
            ]
        if status:
            records = [item for item in records if item.get("status") == status]
        if operation_id:
            records = [item for item in records if item.get("operation_id") == operation_id]
        return sorted(records, key=lambda item: (item.get("operation_id", ""), item.get("field_path", "")))

    def _get_field_mapping_file_store(self, mapping_id: str) -> dict[str, Any] | None:
        store = self._read_store()
        return next((item for item in store["field_mappings"] if item["id"] == mapping_id), None)

    def _create_field_mapping_file_store(self, payload: dict[str, Any]) -> dict[str, Any]:
        with _STORE_LOCK:
            store = self._read_store()
            record = _field_mapping_record(payload)
            if any(
                item.get("operation_id") == record["operation_id"]
                and item.get("variant_id") == record["variant_id"]
                and item.get("field_path") == record["field_path"]
                and item.get("semantic_type_id") == record["semantic_type_id"]
                and item.get("canonical_attribute_id") == record["canonical_attribute_id"]
                for item in store["field_mappings"]
            ):
                raise ValueError("mapping context already exists")
            store["field_mappings"].append(record)
            proposal = _proposal_record(
                source_type="manual_authoring",
                title=f"Create mapping {record['operation_id']} {record['field_path']}",
                entity_type="field_mapping",
                entity_id=record["id"],
                change_type="create",
                payload=_proposal_payload_with_context(record, payload.get("proposal_context")),
            )
            store["proposals"].append(proposal)
            self._write_store(store)
        return {"field_mapping": record, "proposal": proposal}

    def _update_field_mapping_file_store(self, mapping_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        with _STORE_LOCK:
            store = self._read_store()
            record = self._get_field_mapping_file_store(mapping_id)
            if record is None:
                raise KeyError(mapping_id)
            updates = _field_mapping_updates(payload)
            draft_snapshot = {**record, **updates, "updated_at": _now()}
            proposal = _proposal_record(
                source_type="manual_authoring",
                title=f"Update mapping {record['operation_id']} {record['field_path']}",
                entity_type="field_mapping",
                entity_id=record["id"],
                change_type="update",
                payload={
                    "approved_snapshot": record,
                    "draft_snapshot": draft_snapshot,
                    "fields_changed": sorted(updates.keys()),
                },
            )
            store["proposals"] = [
                item
                for item in store["proposals"]
                if not (
                    item.get("entity_type") == "field_mapping"
                    and item.get("entity_id") == mapping_id
                    and item.get("change_type") == "update"
                    and item.get("status") == "pending_review"
                )
            ]
            store["proposals"].append(proposal)
            self._write_store(store)
        return {"field_mapping": draft_snapshot, "proposal": proposal}

    def _delete_field_mapping_file_store(self, mapping_id: str) -> dict[str, Any]:
        with _STORE_LOCK:
            store = self._read_store()
            record = self._get_field_mapping_file_store(mapping_id)
            if record is None:
                raise KeyError(mapping_id)
            proposal = _proposal_record(
                source_type="manual_authoring",
                title=f"Delete mapping {record['operation_id']} {record['field_path']}",
                entity_type="field_mapping",
                entity_id=record["id"],
                change_type="delete",
                payload=dict(record),
            )
            store["proposals"].append(proposal)
            self._write_store(store)
        return {"field_mapping": record, "proposal": proposal}

    def _update_execution_source_file_store(self, source_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        with _STORE_LOCK:
            store = self._read_store()
            record = _find_execution_source(store, source_id)
            if record is None:
                raise KeyError(source_id)
            updates = _execution_source_updates(payload)
            if "name" in updates and updates["name"] != record["name"]:
                if _find_execution_source_by_name(store, str(updates["name"])) is not None:
                    raise ValueError(f"execution source already exists: {updates['name']}")
            draft_snapshot = {**record, **updates, "updated_at": _now()}
            proposal = _proposal_record(
                source_type="manual_authoring",
                title=f"Update execution source {record['name']}",
                entity_type="execution_source",
                entity_id=record["id"],
                change_type="update",
                payload={
                    "approved_snapshot": record,
                    "draft_snapshot": draft_snapshot,
                    "fields_changed": sorted(updates.keys()),
                },
            )
            store["proposals"] = [
                item
                for item in store["proposals"]
                if not (
                    item.get("entity_type") == "execution_source"
                    and item.get("entity_id") == source_id
                    and item.get("change_type") == "update"
                    and item.get("status") == "pending_review"
                )
            ]
            store["proposals"].append(proposal)
            self._write_store(store)
        return {"execution_source": draft_snapshot, "proposal": proposal}

    def _delete_execution_source_file_store(self, source_id: str) -> dict[str, Any]:
        with _STORE_LOCK:
            store = self._read_store()
            record = _find_execution_source(store, source_id)
            if record is None:
                raise KeyError(source_id)
            proposal = _proposal_record(
                source_type="manual_authoring",
                title=f"Delete execution source {record['name']}",
                entity_type="execution_source",
                entity_id=record["id"],
                change_type="delete",
                payload=dict(record),
            )
            store["proposals"].append(proposal)
            self._write_store(store)
        return {"execution_source": record, "proposal": proposal}

    def _create_operation_variant_file_store(self, payload: dict[str, Any]) -> dict[str, Any]:
        with _STORE_LOCK:
            store = self._read_store()
            record = _operation_variant_record(payload)
            proposal = _proposal_record(
                source_type="manual_authoring",
                title=f"Create operation variant {record['name']}",
                entity_type="operation_variant",
                entity_id=record["id"],
                change_type="create",
                payload=record,
            )
            store["operation_variants"].append(record)
            store["proposals"].append(proposal)
            self._write_store(store)
        return {"operation_variant": record, "proposal": proposal}

    def _update_operation_variant_file_store(self, variant_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        with _STORE_LOCK:
            store = self._read_store()
            record = next((item for item in store["operation_variants"] if item["id"] == variant_id), None)
            if record is None:
                raise KeyError(variant_id)
            updates = _operation_variant_updates(payload)
            draft_snapshot = {**record, **updates, "updated_at": _now()}
            proposal = _proposal_record(
                source_type="manual_authoring",
                title=f"Update operation variant {record['name']}",
                entity_type="operation_variant",
                entity_id=record["id"],
                change_type="update",
                payload={
                    "approved_snapshot": dict(record),
                    "draft_snapshot": draft_snapshot,
                    "fields_changed": sorted(updates.keys()),
                },
            )
            store["proposals"].append(proposal)
            self._write_store(store)
        return {"operation_variant": draft_snapshot, "proposal": proposal}

    def _delete_operation_variant_file_store(self, variant_id: str) -> dict[str, Any]:
        with _STORE_LOCK:
            store = self._read_store()
            record = next((item for item in store["operation_variants"] if item["id"] == variant_id), None)
            if record is None:
                raise KeyError(variant_id)
            proposal = _proposal_record(
                source_type="manual_authoring",
                title=f"Delete operation variant {record['name']}",
                entity_type="operation_variant",
                entity_id=record["id"],
                change_type="delete",
                payload=dict(record),
            )
            store["proposals"].append(proposal)
            self._write_store(store)
        return {"operation_variant": record, "proposal": proposal}

    def _update_semantic_type_file_store(self, semantic_type_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        with _STORE_LOCK:
            store = self._read_store()
            record = _find_semantic_type(store, semantic_type_id)
            if record is None:
                raise KeyError(semantic_type_id)
            payload = _normalize_semantic_type_payload_file_store(store, payload, current=record)
            updates = _semantic_type_updates(payload)
            if "name" in updates and updates["name"] != record["name"]:
                if _find_semantic_type_by_name(store, str(updates["name"])) is not None:
                    raise ValueError(f"semantic type already exists: {updates['name']}")
            record.update({**updates, "updated_at": _now()})
            record["urn"] = f"urn:semantic-platform:semantic-type:{record['name']}"
            proposal = _proposal_record(
                source_type="manual_authoring",
                title=f"Update semantic type {record['name']}",
                entity_type="semantic_type",
                entity_id=record["id"],
                change_type="update",
                payload=updates,
            )
            store["proposals"].append(proposal)
            self._write_store(store)
        return {"semantic_type": record, "proposal": proposal}

    def _delete_semantic_type_file_store(self, semantic_type_id: str) -> dict[str, Any]:
        with _STORE_LOCK:
            store = self._read_store()
            record = _find_semantic_type(store, semantic_type_id)
            if record is None:
                raise KeyError(semantic_type_id)
            related_relationships = [
                item
                for item in store["relationships"]
                if item["source_id"] == semantic_type_id or item["target_id"] == semantic_type_id
            ]
            proposal = _proposal_record(
                source_type="manual_authoring",
                title=f"Delete semantic type {record['name']}",
                entity_type="semantic_type",
                entity_id=record["id"],
                change_type="delete",
                payload={
                    "semantic_type": dict(record),
                    "related_relationship_ids": [item["id"] for item in related_relationships],
                },
            )
            store["proposals"].append(proposal)
            self._write_store(store)
        return {"semantic_type": record, "proposal": proposal}

    def _add_semantic_relationship_file_store(self, semantic_type_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        target_id = str(payload.get("target_id") or "").strip()
        relation_type = str(payload.get("relation_type") or "").strip()
        with _STORE_LOCK:
            store = self._read_store()
            source = _find_semantic_type(store, semantic_type_id)
            target = _find_semantic_type(store, target_id)
            if source is None:
                raise KeyError(semantic_type_id)
            if target is None:
                raise KeyError(target_id)
            relationship = _relationship_record(source=source, target=target, relation_type=relation_type)
            proposal = _proposal_record(
                source_type="manual_authoring",
                title=f"Relate {source['name']} to {target['name']}",
                entity_type="semantic_relationship",
                entity_id=relationship["id"],
                change_type="create",
                payload=relationship,
            )
            store["relationships"].append(relationship)
            store["proposals"].append(proposal)
            self._write_store(store)
        return {"relationship": relationship, "proposal": proposal}

    def _update_semantic_relationship_file_store(self, relationship_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        relation_type = str(payload.get("relation_type") or "").strip()
        source_id = str(payload.get("source_id") or "").strip()
        target_id = str(payload.get("target_id") or "").strip()
        with _STORE_LOCK:
            store = self._read_store()
            relationship = _find_relationship(store, relationship_id)
            if relationship is None:
                raise KeyError(relationship_id)
            source = _find_semantic_type(store, source_id)
            target = _find_semantic_type(store, target_id)
            if source is None:
                raise KeyError(source_id)
            if target is None:
                raise KeyError(target_id)
            updates = {
                "source_id": source["id"],
                "source_name": source["name"],
                "target_id": target["id"],
                "target_name": target["name"],
                "relation_type": relation_type,
                "updated_at": _now(),
            }
            relationship.update(updates)
            proposal = _proposal_record(
                source_type="manual_authoring",
                title=f"Update relationship {source['name']} to {target['name']}",
                entity_type="semantic_relationship",
                entity_id=relationship["id"],
                change_type="update",
                payload=updates,
            )
            store["proposals"].append(proposal)
            self._write_store(store)
        return {"relationship": relationship, "proposal": proposal}

    def _delete_semantic_relationship_file_store(self, relationship_id: str) -> dict[str, Any]:
        with _STORE_LOCK:
            store = self._read_store()
            relationship = _find_relationship(store, relationship_id)
            if relationship is None:
                raise KeyError(relationship_id)
            proposal = _proposal_record(
                source_type="manual_authoring",
                title=f"Delete relationship {relationship['source_name']} to {relationship['target_name']}",
                entity_type="semantic_relationship",
                entity_id=relationship["id"],
                change_type="delete",
                payload=dict(relationship),
            )
            store["proposals"].append(proposal)
            self._write_store(store)
        return {"relationship": relationship, "proposal": proposal}

    def _list_proposals_file_store(self, status: str = "") -> list[dict[str, Any]]:
        proposals = list(self._read_store()["proposals"])
        if status:
            proposals = [item for item in proposals if item.get("status") == status]
        return sorted(proposals, key=lambda item: item["created_at"], reverse=True)

    def _list_relationships_file_store(self, *, semantic_type_id: str = "", status: str = "") -> list[dict[str, Any]]:
        relationships = list(self._read_store()["relationships"])
        if semantic_type_id:
            relationships = [
                item
                for item in relationships
                if item.get("source_id") == semantic_type_id or item.get("target_id") == semantic_type_id
            ]
        if status:
            relationships = [item for item in relationships if item.get("status") == status]
        return sorted(relationships, key=lambda item: item["created_at"], reverse=True)

    def _semantic_catalog_file_store(self) -> dict[str, Any]:
        store = self._read_store()
        semantic_types = sorted(store["semantic_types"], key=lambda item: item["name"])
        relationships = sorted(store["relationships"], key=lambda item: item["created_at"], reverse=True)
        proposals = sorted(store["proposals"], key=lambda item: item["created_at"], reverse=True)
        return {
            "core": {
                "semantic_types": semantic_types,
                "relationships": relationships,
                "execution_sources": sorted(store["execution_sources"], key=lambda item: item["name"]),
            },
            "governance": {
                "pending_proposals": [item for item in proposals if item.get("status") == "pending_review"][:10],
            },
            "capabilities": {
                "items": sorted(store["capabilities"], key=lambda item: item["name"]),
            },
            "mappings": {
                "items": sorted(
                    store["field_mappings"],
                    key=lambda item: (item.get("operation_id", ""), item.get("field_path", "")),
                ),
            },
            "status": "available",
        }

    def _review_proposal_file_store(self, proposal_id: str, decision: str, reviewer: str = "admin") -> dict[str, Any]:
        with _STORE_LOCK:
            store = self._read_store()
            proposal = next((item for item in store["proposals"] if item["id"] == proposal_id), None)
            if proposal is None:
                raise KeyError(proposal_id)
            proposal["status"] = decision
            proposal["reviewed_by"] = reviewer
            proposal["reviewed_at"] = _now()
            if decision == "approved":
                _apply_approval_file_store(store, proposal)
            if decision == "rejected":
                _apply_rejection_file_store(store, proposal)
            self._write_store(store)
        return proposal

    def _create_onboarding_run_for_source_file_store(
        self,
        *,
        source: dict[str, Any],
        proposal: dict[str, Any] | None = None,
        upload_metadata: dict[str, Any] | None = None,
        trigger_type: str = "source_upload",
        created_by: str = "system",
    ) -> dict[str, Any]:
        now = _now()
        run = {
            "id": f"run_{uuid4().hex}",
            "source_id": source["id"],
            "source_name": source.get("name") or source["id"],
            "status": "started",
            "stage": "source_uploaded",
            "current_stage": "source_review",
            "stage_status": "pending",
            "run_mode": "ai_assisted",
            "next_action": "Review source evidence and generate onboarding drafts.",
            "trigger_type": trigger_type,
            "evidence_snapshot_id": "",
            "operation_count": 0,
            "field_count": 0,
            "mapping_count": 0,
            "proposal_count": 1 if proposal else 0,
            "pending_proposal_count": 1 if proposal and proposal.get("status") == "pending_review" else 0,
            "suggestion_status": "ready_for_field_extraction",
            "metadata": {"upload": upload_metadata or {}, "created_by": created_by},
            "created_at": now,
            "updated_at": now,
        }
        evidence = {
            "id": f"evidence_{uuid4().hex}",
            "run_id": run["id"],
            "source_id": source["id"],
            "snapshot_type": "source_upload",
            "content_hash": str((upload_metadata or {}).get("sha256") or ""),
            "source_ref": {"source_id": source["id"], "upload": upload_metadata or {}},
            "operation_evidence": [],
            "schema_evidence": [],
            "sample_values": {},
            "ai_context": {"suggestion_mode": "deterministic_assist", "status": "ready_for_field_extraction"},
            "created_at": now,
        }
        run["evidence_snapshot_id"] = evidence["id"]
        bundle = {
            "id": f"bundle_{uuid4().hex}",
            "run_id": run["id"],
            "source_id": source["id"],
            "source_name": source.get("name") or source["id"],
            "status": "pending_review" if proposal else "draft",
            "proposal_count": 1 if proposal else 0,
            "pending_count": 1 if proposal and proposal.get("status") == "pending_review" else 0,
            "approved_count": 0,
            "rejected_count": 0,
            "entity_counts": {"execution_source": 1} if proposal else {},
            "evidence_snapshot_id": evidence["id"],
            "proposal_ids": [proposal["id"]] if proposal else [],
            "updated_at": now,
        }
        tasks = build_onboarding_stage_task_records(
            run_id=run["id"],
            source_id=source["id"],
            source_name=source.get("name") or source["id"],
            evidence_snapshot_id=evidence["id"],
            proposal_id=proposal.get("id") if proposal else None,
            created_at=now,
        )
        with _STORE_LOCK:
            store = self._read_store()
            store["onboarding_runs"].append(run)
            store["evidence_snapshots"].append(evidence)
            store["proposal_bundles"].append(bundle)
            store["work_queue_tasks"].extend(tasks)
            self._write_store(store)
        return {"onboarding_run": run, "evidence_snapshot": evidence, "proposal_bundle": bundle, "work_queue_tasks": tasks}

    def _read_store(self) -> dict[str, Any]:
        if self.store_path is None or not self.store_path.exists():
            return _empty_store()
        payload = json.loads(self.store_path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            return _empty_store()
        return {
            "semantic_types": list(payload.get("semantic_types", [])),
            "canonical_entities": list(payload.get("canonical_entities", [])),
            "canonical_attributes": list(payload.get("canonical_attributes", [])),
            "canonical_relations": list(payload.get("canonical_relations", [])),
            "execution_sources": list(payload.get("execution_sources", [])),
            "execution_assets": list(payload.get("execution_assets", [])),
            "execution_operations": list(payload.get("execution_operations", [])),
            "operation_variants": list(payload.get("operation_variants", [])),
            "operation_fields": list(payload.get("operation_fields", [])),
            "capabilities": list(payload.get("capabilities", [])),
            "field_mappings": list(payload.get("field_mappings", [])),
            "relationships": list(payload.get("relationships", [])),
            "proposals": list(payload.get("proposals", [])),
            "onboarding_runs": list(payload.get("onboarding_runs", [])),
            "evidence_snapshots": list(payload.get("evidence_snapshots", [])),
            "proposal_bundles": list(payload.get("proposal_bundles", [])),
            "proposal_bundle_items": list(payload.get("proposal_bundle_items", [])),
            "work_queue_tasks": list(payload.get("work_queue_tasks", [])),
            "capability_operation_bindings": list(payload.get("capability_operation_bindings", [])),
            "access_path_checks": list(payload.get("access_path_checks", [])),
        }

    def _write_store(self, store: dict[str, Any]) -> None:
        if self.store_path is None:
            raise RuntimeError("store_path is not configured")
        self.store_path.parent.mkdir(parents=True, exist_ok=True)
        self.store_path.write_text(json.dumps(store, ensure_ascii=False, indent=2), encoding="utf-8")


def _empty_store() -> dict[str, Any]:
    return {
        "semantic_types": [],
        "canonical_entities": [],
        "canonical_attributes": [],
        "canonical_relations": [],
        "execution_sources": [],
        "execution_assets": [],
        "execution_operations": [],
        "operation_variants": [],
        "operation_fields": [],
        "capabilities": [],
        "field_mappings": [],
        "relationships": [],
        "proposals": [],
        "onboarding_runs": [],
        "evidence_snapshots": [],
        "proposal_bundles": [],
        "proposal_bundle_items": [],
        "work_queue_tasks": [],
        "capability_operation_bindings": [],
        "access_path_checks": [],
    }


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _semantic_type_record(payload: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    name = str(payload["name"]).strip()
    return {
        "id": f"st_{uuid4().hex}",
        "urn": f"urn:semantic-platform:semantic-type:{name}",
        "name": name,
        "description": str(payload.get("description") or "").strip(),
        "datatype": str(payload.get("datatype") or "string").strip(),
        "entity_kind": str(payload.get("entity_kind") or "attribute").strip(),
        "parent_entity_id": str(payload.get("parent_entity_id") or "").strip(),
        "parent_entity_name": str(payload.get("parent_entity_name") or "").strip(),
        "semantic_role": str(payload.get("semantic_role") or "").strip(),
        "aliases": _string_list(payload.get("aliases")),
        "owners": _string_list(payload.get("owners")),
        "tags": _string_list(payload.get("tags")),
        "documentation": str(payload.get("documentation") or "").strip(),
        "status": str(payload.get("status") or "draft").strip(),
        "created_at": now,
        "updated_at": now,
    }


def _relationship_record(*, source: dict[str, Any], target: dict[str, Any], relation_type: str) -> dict[str, Any]:
    now = _now()
    return {
        "id": f"rel_{uuid4().hex}",
        "source_id": source["id"],
        "source_name": source["name"],
        "target_id": target["id"],
        "target_name": target["name"],
        "relation_type": relation_type,
        "status": "draft",
        "created_at": now,
        "updated_at": now,
    }


def _proposal_record(
    *,
    source_type: str,
    title: str,
    entity_type: str,
    entity_id: str,
    change_type: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    return {
        "id": f"proposal_{uuid4().hex}",
        "source_type": source_type,
        "title": title,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "change_type": change_type,
        "payload": payload,
        "status": "pending_review",
        "created_at": _now(),
        "reviewed_by": None,
        "reviewed_at": None,
    }


def _proposal_payload_with_context(payload: dict[str, Any], proposal_context: dict[str, Any] | None = None) -> dict[str, Any]:
    if not isinstance(proposal_context, dict) or not proposal_context:
        return payload
    return {**payload, "proposal_context": proposal_context}


def _execution_source_record(payload: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    return {
        "id": f"src_{uuid4().hex}",
        "name": str(payload["name"]).strip(),
        "provider": str(payload.get("provider") or "").strip(),
        "source_type": str(payload.get("source_type") or "api").strip(),
        "description": str(payload.get("description") or "").strip(),
        "status": str(payload.get("status") or "draft").strip(),
        "config": dict(payload.get("config") or {}),
        "created_at": now,
        "updated_at": now,
    }


def _operation_variant_record(payload: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    return {
        "id": f"variant_{uuid4().hex}",
        "operation_id": str(payload["operation_id"]).strip(),
        "variant_key": str(payload["variant_key"]).strip(),
        "name": str(payload.get("name") or payload["variant_key"]).strip(),
        "description": str(payload.get("description") or "").strip(),
        "version": str(payload.get("version") or "1.0.0").strip(),
        "lifecycle": str(payload.get("lifecycle") or "draft").strip(),
        "status": str(payload.get("status") or "draft").strip(),
        "fixed_semantic_arguments": dict(payload.get("fixed_semantic_arguments") or {}),
        "fixed_raw_arguments": dict(payload.get("fixed_raw_arguments") or {}),
        "metadata": dict(payload.get("metadata") or {}),
        "created_at": now,
        "updated_at": now,
    }


def _capability_record(payload: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    return {
        "id": f"cap_{uuid4().hex}",
        "capability_key": str(payload["capability_key"]).strip(),
        "namespace": str(payload.get("namespace") or "public").strip(),
        "name": str(payload.get("name") or payload["capability_key"]).strip(),
        "description": str(payload.get("description") or "").strip(),
        "version": str(payload.get("version") or "1.0.0").strip(),
        "lifecycle": str(payload.get("lifecycle") or "draft").strip(),
        "status": str(payload.get("status") or "draft").strip(),
        "intent_spec": dict(payload.get("intent_spec") or {}),
        "input_semantic_types": _string_list(payload.get("input_semantic_types")),
        "output_semantic_types": _string_list(payload.get("output_semantic_types")),
        "metadata": dict(payload.get("metadata") or {}),
        "created_by": str(payload.get("created_by") or "system").strip(),
        "reviewed_by": payload.get("reviewed_by"),
        "approved_at": payload.get("approved_at"),
        "evidence": list(payload.get("evidence") or []),
        "confidence": payload.get("confidence"),
        "created_at": now,
        "updated_at": now,
    }


def _field_mapping_record(payload: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    return {
        "id": f"map_{uuid4().hex}",
        "field_id": str(payload.get("field_id") or "").strip() or None,
        "source_id": str(payload.get("source_id") or "").strip() or None,
        "operation_id": str(payload["operation_id"]).strip(),
        "variant_id": str(payload.get("variant_id") or "").strip() or None,
        "access_path_id": str(payload.get("access_path_id") or "").strip() or None,
        "field_path": str(payload.get("field_path") or "").strip(),
        "semantic_type_id": str(payload["semantic_type_id"]).strip(),
        "canonical_attribute_id": str(payload.get("canonical_attribute_id") or "").strip() or None,
        "mapping_kind": str(payload.get("mapping_kind") or "direct").strip(),
        "mapping_type": str(payload.get("mapping_type") or "exact").strip(),
        "version": str(payload.get("version") or "1.0.0").strip(),
        "lifecycle": str(payload.get("lifecycle") or "draft").strip(),
        "status": str(payload.get("status") or "draft").strip(),
        "namespace": str(payload.get("namespace") or "public").strip(),
        "transform_spec": dict(payload.get("transform_spec") or {}),
        "enum_mapping": dict(payload.get("enum_mapping") or {}),
        "notes": str(payload.get("notes") or "").strip(),
        "created_by": str(payload.get("created_by") or "system").strip(),
        "reviewed_by": payload.get("reviewed_by"),
        "approved_at": payload.get("approved_at"),
        "evidence": list(payload.get("evidence") or []),
        "confidence": payload.get("confidence"),
        "created_at": now,
        "updated_at": now,
    }


def _canonical_entity_record(payload: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    return {
        "id": str(payload.get("id") or f"ce_{uuid4().hex}"),
        "semantic_type_id": str(payload.get("semantic_type_id") or "").strip() or None,
        "name": str(payload["name"]).strip(),
        "namespace": str(payload.get("namespace") or "public").strip(),
        "description": str(payload.get("description") or "").strip(),
        "version": str(payload.get("version") or "1.0.0").strip(),
        "lifecycle": str(payload.get("lifecycle") or "draft").strip(),
        "status": str(payload.get("status") or "draft").strip(),
        "metadata": dict(payload.get("metadata") or {}),
        "created_by": str(payload.get("created_by") or "system").strip(),
        "reviewed_by": payload.get("reviewed_by"),
        "approved_at": payload.get("approved_at"),
        "evidence": list(payload.get("evidence") or []),
        "confidence": payload.get("confidence"),
        "created_at": now,
        "updated_at": now,
    }


def _canonical_attribute_record(payload: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    return {
        "id": str(payload.get("id") or f"ca_{uuid4().hex}"),
        "entity_id": str(payload["entity_id"]).strip(),
        "semantic_type_id": str(payload.get("semantic_type_id") or "").strip() or None,
        "name": str(payload["name"]).strip(),
        "namespace": str(payload.get("namespace") or "public").strip(),
        "description": str(payload.get("description") or "").strip(),
        "datatype": str(payload.get("datatype") or "string").strip(),
        "identity_role": str(payload.get("identity_role") or "").strip(),
        "version": str(payload.get("version") or "1.0.0").strip(),
        "lifecycle": str(payload.get("lifecycle") or "draft").strip(),
        "status": str(payload.get("status") or "draft").strip(),
        "metadata": dict(payload.get("metadata") or {}),
        "created_by": str(payload.get("created_by") or "system").strip(),
        "reviewed_by": payload.get("reviewed_by"),
        "approved_at": payload.get("approved_at"),
        "evidence": list(payload.get("evidence") or []),
        "confidence": payload.get("confidence"),
        "created_at": now,
        "updated_at": now,
    }


def _canonical_relation_record(payload: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    return {
        "id": str(payload.get("id") or f"cr_{uuid4().hex}"),
        "source_entity_id": str(payload["source_entity_id"]).strip(),
        "target_entity_id": str(payload["target_entity_id"]).strip(),
        "relation_type": str(payload["relation_type"]).strip(),
        "forward_label": str(payload.get("forward_label") or "").strip(),
        "reverse_label": str(payload.get("reverse_label") or "").strip(),
        "version": str(payload.get("version") or "1.0.0").strip(),
        "lifecycle": str(payload.get("lifecycle") or "draft").strip(),
        "status": str(payload.get("status") or "draft").strip(),
        "metadata": dict(payload.get("metadata") or {}),
        "created_by": str(payload.get("created_by") or "system").strip(),
        "reviewed_by": payload.get("reviewed_by"),
        "approved_at": payload.get("approved_at"),
        "evidence": list(payload.get("evidence") or []),
        "confidence": payload.get("confidence"),
        "created_at": now,
        "updated_at": now,
    }


def _validate_semantic_type_payload(payload: dict[str, Any], *, creating: bool = False) -> None:
    if creating and not payload.get("name"):
        raise ValueError("name is required")
    if payload.get("name"):
        _validate_name(str(payload["name"]))
    if payload.get("datatype") and str(payload["datatype"]) not in _SUPPORTED_DATATYPES:
        raise ValueError("datatype is not supported")
    if payload.get("entity_kind") and str(payload["entity_kind"]) not in _SUPPORTED_ENTITY_KINDS:
        raise ValueError("entity_kind must be entity or attribute")


def _validate_execution_source_payload(payload: dict[str, Any], *, creating: bool = False) -> None:
    if creating and not payload.get("name"):
        raise ValueError("name is required")
    if payload.get("source_type") and str(payload["source_type"]) not in _SUPPORTED_SOURCE_TYPES:
        raise ValueError("source_type is not supported")


def _validate_capability_payload(payload: dict[str, Any], *, creating: bool = False) -> None:
    if creating and not payload.get("capability_key"):
        raise ValueError("capability_key is required")
    if payload.get("capability_key"):
        if not re.fullmatch(r"^[a-z][a-z0-9_]*$", str(payload["capability_key"]).strip()):
            raise ValueError("capability_key must be snake_case alphanumeric")


def _validate_field_mapping_payload(payload: dict[str, Any], *, creating: bool = False) -> None:
    if creating and not payload.get("operation_id"):
        raise ValueError("operation_id is required")
    if creating and not payload.get("semantic_type_id"):
        raise ValueError("semantic_type_id is required")
    if creating and not payload.get("field_path"):
        raise ValueError("field_path is required")
    if payload.get("mapping_type") and str(payload["mapping_type"]) not in _SUPPORTED_MAPPING_TYPES:
        raise ValueError("mapping_type is not supported")


def _validate_canonical_entity_payload(payload: dict[str, Any], *, creating: bool = False) -> None:
    if creating and not payload.get("name"):
        raise ValueError("name is required")


def _validate_canonical_attribute_payload(payload: dict[str, Any], *, creating: bool = False) -> None:
    if creating and not payload.get("entity_id"):
        raise ValueError("entity_id is required")
    if creating and not payload.get("name"):
        raise ValueError("name is required")
    if payload.get("datatype") and str(payload["datatype"]) not in _SUPPORTED_DATATYPES:
        raise ValueError("datatype is not supported")


def _validate_canonical_relation_payload(payload: dict[str, Any], *, creating: bool = False) -> None:
    if creating and not payload.get("source_entity_id"):
        raise ValueError("source_entity_id is required")
    if creating and not payload.get("target_entity_id"):
        raise ValueError("target_entity_id is required")
    if creating and not payload.get("relation_type"):
        raise ValueError("relation_type is required")
    if payload.get("relation_type") and str(payload["relation_type"]) not in _SUPPORTED_CANONICAL_RELATION_TYPES:
        raise ValueError("relation_type is not supported")


def _validate_name(name: str) -> None:
    if not _NAME_PATTERN.fullmatch(name):
        raise ValueError("name must be PascalCase alphanumeric, for example BusinessRegistrationNumber")


def _semantic_type_updates(payload: dict[str, Any]) -> dict[str, Any]:
    allowed = {
        "name",
        "description",
        "datatype",
        "entity_kind",
        "parent_entity_id",
        "parent_entity_name",
        "semantic_role",
        "aliases",
        "owners",
        "tags",
        "documentation",
        "status",
    }
    updates = {key: value for key, value in payload.items() if key in allowed}
    _validate_semantic_type_payload(updates)
    for key in ("aliases", "owners", "tags"):
        if key in updates:
            updates[key] = _string_list(updates[key])
    return updates


def _execution_source_updates(payload: dict[str, Any]) -> dict[str, Any]:
    allowed = {"name", "provider", "source_type", "description", "status", "config"}
    updates = {key: value for key, value in payload.items() if key in allowed}
    _validate_execution_source_payload(updates)
    if "config" in updates and not isinstance(updates["config"], dict):
        raise ValueError("config must be an object")
    return updates


def _capability_updates(payload: dict[str, Any]) -> dict[str, Any]:
    allowed = {
        "capability_key",
        "namespace",
        "name",
        "description",
        "version",
        "lifecycle",
        "status",
        "intent_spec",
        "input_semantic_types",
        "output_semantic_types",
        "metadata",
        "created_by",
        "reviewed_by",
        "approved_at",
        "evidence",
        "confidence",
    }
    updates = {key: value for key, value in payload.items() if key in allowed}
    _validate_capability_payload(updates)
    for key in ("input_semantic_types", "output_semantic_types"):
        if key in updates:
            updates[key] = _string_list(updates[key])
    return updates


def _field_mapping_updates(payload: dict[str, Any]) -> dict[str, Any]:
    allowed = {
        "field_id",
        "source_id",
        "operation_id",
        "variant_id",
        "access_path_id",
        "field_path",
        "semantic_type_id",
        "canonical_attribute_id",
        "mapping_kind",
        "mapping_type",
        "version",
        "lifecycle",
        "status",
        "namespace",
        "transform_spec",
        "enum_mapping",
        "notes",
        "created_by",
        "reviewed_by",
        "approved_at",
        "evidence",
        "confidence",
    }
    updates = {key: value for key, value in payload.items() if key in allowed}
    _validate_field_mapping_payload(updates)
    return updates


def _canonical_entity_updates(payload: dict[str, Any]) -> dict[str, Any]:
    allowed = {
        "semantic_type_id",
        "name",
        "namespace",
        "description",
        "version",
        "lifecycle",
        "status",
        "metadata",
        "created_by",
        "reviewed_by",
        "approved_at",
        "evidence",
        "confidence",
    }
    updates = {key: value for key, value in payload.items() if key in allowed}
    _validate_canonical_entity_payload(updates)
    return updates


def _canonical_attribute_updates(payload: dict[str, Any]) -> dict[str, Any]:
    allowed = {
        "entity_id",
        "semantic_type_id",
        "name",
        "namespace",
        "description",
        "datatype",
        "identity_role",
        "version",
        "lifecycle",
        "status",
        "metadata",
        "created_by",
        "reviewed_by",
        "approved_at",
        "evidence",
        "confidence",
    }
    updates = {key: value for key, value in payload.items() if key in allowed}
    _validate_canonical_attribute_payload(updates)
    return updates


def _canonical_relation_updates(payload: dict[str, Any]) -> dict[str, Any]:
    allowed = {
        "source_entity_id",
        "target_entity_id",
        "relation_type",
        "forward_label",
        "reverse_label",
        "version",
        "lifecycle",
        "status",
        "metadata",
        "created_by",
        "reviewed_by",
        "approved_at",
        "evidence",
        "confidence",
    }
    updates = {key: value for key, value in payload.items() if key in allowed}
    _validate_canonical_relation_payload(updates)
    return updates


def _validate_operation_variant_payload(payload: dict[str, Any], *, creating: bool) -> None:
    if creating and not str(payload.get("operation_id") or "").strip():
        raise ValueError("operation_id is required")
    if creating and not str(payload.get("variant_key") or "").strip():
        raise ValueError("variant_key is required")
    if creating and not str(payload.get("name") or "").strip():
        raise ValueError("name is required")
    if "fixed_semantic_arguments" in payload and not isinstance(payload.get("fixed_semantic_arguments") or {}, dict):
        raise ValueError("fixed_semantic_arguments must be an object")
    if "fixed_raw_arguments" in payload and not isinstance(payload.get("fixed_raw_arguments") or {}, dict):
        raise ValueError("fixed_raw_arguments must be an object")
    if "metadata" in payload and not isinstance(payload.get("metadata") or {}, dict):
        raise ValueError("metadata must be an object")


def _operation_variant_updates(payload: dict[str, Any]) -> dict[str, Any]:
    updates: dict[str, Any] = {}
    for key in {"operation_id", "variant_key", "name", "description", "version", "lifecycle", "status"}:
        if key in payload:
            updates[key] = str(payload.get(key) or "").strip()
    for key in {"fixed_semantic_arguments", "fixed_raw_arguments", "metadata"}:
        if key in payload:
            updates[key] = dict(payload.get(key) or {})
    return updates


def _sql_operation_variant_params(record: dict[str, Any]) -> dict[str, Any]:
    return dict(record)


def _string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    return []


def _sql_semantic_type_params(record: dict[str, Any]) -> dict[str, Any]:
    return {
        **record,
        "aliases": json.dumps(record.get("aliases", [])),
        "owners": json.dumps(record.get("owners", [])),
        "tags": json.dumps(record.get("tags", [])),
    }


def _sql_execution_source_params(record: dict[str, Any]) -> dict[str, Any]:
    return {
        **record,
        "config": json.dumps(record.get("config", {})),
    }


def _sql_capability_params(record: dict[str, Any]) -> dict[str, Any]:
    return {
        **record,
        "intent_spec": json.dumps(record.get("intent_spec", {})),
        "input_semantic_types": json.dumps(record.get("input_semantic_types", [])),
        "output_semantic_types": json.dumps(record.get("output_semantic_types", [])),
        "metadata": json.dumps(record.get("metadata", {})),
        "evidence": json.dumps(record.get("evidence", [])),
    }


def _sql_field_mapping_params(record: dict[str, Any]) -> dict[str, Any]:
    return {
        **record,
        "transform_spec": json.dumps(record.get("transform_spec", {})),
        "enum_mapping": json.dumps(record.get("enum_mapping", {})),
        "evidence": json.dumps(record.get("evidence", [])),
    }


def _sql_evidence_snapshot_params(record: dict[str, Any]) -> dict[str, Any]:
    return {
        **record,
        "source_ref": json.dumps(record.get("source_ref", {})),
        "operation_evidence": json.dumps(record.get("operation_evidence", [])),
        "schema_evidence": json.dumps(record.get("schema_evidence", [])),
        "sample_values": json.dumps(record.get("sample_values", {})),
        "ai_context": json.dumps(record.get("ai_context", {})),
    }


def _sql_canonical_entity_params(record: dict[str, Any]) -> dict[str, Any]:
    return {**record, "metadata": json.dumps(record.get("metadata", {})), "evidence": json.dumps(record.get("evidence", []))}


def _sql_canonical_attribute_params(record: dict[str, Any]) -> dict[str, Any]:
    return {**record, "metadata": json.dumps(record.get("metadata", {})), "evidence": json.dumps(record.get("evidence", []))}


def _sql_canonical_relation_params(record: dict[str, Any]) -> dict[str, Any]:
    return {**record, "metadata": json.dumps(record.get("metadata", {})), "evidence": json.dumps(record.get("evidence", []))}


def _insert_proposal(cur: Any, proposal: dict[str, Any]) -> None:
    cur.execute(
        """
        insert into semantic_platform.proposals (
          id, source_type, title, entity_type, entity_id,
          change_type, payload, status, reviewed_by, reviewed_at, created_at
        ) values (
          %(id)s, %(source_type)s, %(title)s, %(entity_type)s, %(entity_id)s,
          %(change_type)s, %(payload)s::jsonb, %(status)s, %(reviewed_by)s,
          %(reviewed_at)s::timestamptz, %(created_at)s::timestamptz
        )
        """,
        {
            **proposal,
            "payload": json.dumps(_json_safe_value(proposal["payload"])),
        },
    )


def _json_safe_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, dict):
        return {key: _json_safe_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe_value(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe_value(item) for item in value]
    return value


def _load_semantic_type(cur: Any, semantic_type_id: str) -> dict[str, Any]:
    cur.execute("select * from semantic_platform.semantic_types where id = %s", (semantic_type_id,))
    row = cur.fetchone()
    if row is None:
        raise KeyError(semantic_type_id)
    return _semantic_type_from_row(row)


def _ensure_semantic_type_name_available(cur: Any, name: str, *, exclude_id: str | None = None) -> None:
    cur.execute("select id from semantic_platform.semantic_types where lower(name) = lower(%s)", (name,))
    row = cur.fetchone()
    if row is not None and row["id"] != exclude_id:
        raise ValueError(f"semantic type already exists: {name}")


def _ensure_execution_source_name_available(cur: Any, name: str, *, exclude_id: str | None = None) -> None:
    cur.execute("select id from semantic_platform.execution_sources where lower(name) = lower(%s)", (name,))
    row = cur.fetchone()
    if row is not None and row["id"] != exclude_id:
        raise ValueError(f"execution source already exists: {name}")


def _ensure_execution_operation_exists(cur: Any, operation_id: str) -> None:
    cur.execute("select id from semantic_platform.execution_operations where id = %s", (operation_id,))
    if cur.fetchone() is None:
        raise ValueError(f"execution operation not found: {operation_id}")


def _ensure_operation_variant_key_available(cur: Any, variant_key: str, *, exclude_id: str | None = None) -> None:
    cur.execute("select id from semantic_platform.operation_variants where lower(variant_key) = lower(%s)", (variant_key,))
    row = cur.fetchone()
    if row is None:
        return
    existing_id = row["id"] if isinstance(row, dict) else row[0]
    if exclude_id and existing_id == exclude_id:
        return
    raise ValueError(f"operation variant already exists: {variant_key}")


def _ensure_capability_key_available(cur: Any, capability_key: str, *, exclude_id: str | None = None) -> None:
    cur.execute("select id from semantic_platform.capabilities where lower(capability_key) = lower(%s)", (capability_key,))
    row = cur.fetchone()
    if row is not None and row["id"] != exclude_id:
        raise ValueError(f"capability already exists: {capability_key}")


def _ensure_mapping_context_available(cur: Any, record: dict[str, Any], *, exclude_id: str | None = None) -> None:
    cur.execute(
        """
        select id
        from semantic_platform.field_mappings
        where operation_id = %s
          and coalesce(variant_id, '') = coalesce(%s, '')
          and field_path = %s
        """,
        (
            record["operation_id"],
            record.get("variant_id"),
            record["field_path"],
        ),
    )
    row = cur.fetchone()
    if row is not None and row["id"] != exclude_id:
        raise ValueError("A mapping for the selected source field already exists.")


def _populate_mapping_field_id(cur: Any, record: dict[str, Any]) -> None:
    if record.get("field_id"):
        return
    cur.execute(
        """
        select id
        from semantic_platform.operation_fields
        where operation_id = %s
          and coalesce(variant_id, '') = coalesce(%s, '')
          and (field_path = %s or raw_name = %s)
        limit 1
        """,
        (
            record["operation_id"],
            record.get("variant_id"),
            record["field_path"],
            record["field_path"],
        ),
    )
    row = cur.fetchone()
    if row is None:
        raise ValueError("source field does not exist for the selected operation.")
    record["field_id"] = row["id"]


def _ensure_canonical_entity_exists(cur: Any, entity_id: str) -> None:
    cur.execute("select id from semantic_platform.canonical_entities where id = %s", (entity_id,))
    if cur.fetchone() is None:
        raise KeyError(entity_id)


def _ensure_canonical_entity_name_available(cur: Any, name: str, *, exclude_id: str | None = None) -> None:
    cur.execute("select id from semantic_platform.canonical_entities where lower(name) = lower(%s)", (name,))
    row = cur.fetchone()
    if row is not None and row["id"] != exclude_id:
        raise ValueError(f"canonical entity already exists: {name}")


def _ensure_canonical_attribute_name_available(
    cur: Any, entity_id: str, name: str, *, exclude_id: str | None = None
) -> None:
    cur.execute(
        "select id from semantic_platform.canonical_attributes where entity_id = %s and lower(name) = lower(%s)",
        (entity_id, name),
    )
    row = cur.fetchone()
    if row is not None and row["id"] != exclude_id:
        raise ValueError(f"canonical attribute already exists on entity: {name}")


def _semantic_type_from_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row["id"],
        "urn": row["urn"],
        "name": row["name"],
        "description": row.get("description") or "",
        "datatype": row.get("datatype") or "string",
        "entity_kind": row.get("entity_kind") or "attribute",
        "parent_entity_id": row.get("parent_entity_id") or "",
        "parent_entity_name": row.get("parent_entity_name") or "",
        "semantic_role": row.get("semantic_role") or "",
        "aliases": _json_list(row.get("aliases")),
        "owners": _json_list(row.get("owners")),
        "tags": _json_list(row.get("tags")),
        "documentation": row.get("documentation") or "",
        "status": row.get("status") or "draft",
        "created_at": _isoformat(row.get("created_at")),
        "updated_at": _isoformat(row.get("updated_at")),
    }


def _relationship_from_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row["id"],
        "source_id": row["source_id"],
        "source_name": row["source_name"],
        "target_id": row["target_id"],
        "target_name": row["target_name"],
        "relation_type": row["relation_type"],
        "status": row.get("status") or "draft",
        "created_at": _isoformat(row.get("created_at")),
        "updated_at": _isoformat(row.get("updated_at")),
    }


def _proposal_from_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row["id"],
        "source_type": row["source_type"],
        "title": row["title"],
        "entity_type": row["entity_type"],
        "entity_id": row["entity_id"],
        "change_type": row["change_type"],
        "payload": row.get("payload") or {},
        "status": row.get("status") or "pending_review",
        "reviewed_by": row.get("reviewed_by"),
        "reviewed_at": _isoformat(row.get("reviewed_at")),
        "created_at": _isoformat(row.get("created_at")),
    }


def _onboarding_run_from_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row["id"],
        "source_id": row["source_id"],
        "source_name": row.get("source_name") or row.get("source_id") or "",
        "status": row.get("status") or "started",
        "stage": row.get("stage") or "source_uploaded",
        "current_stage": row.get("current_stage") or "source_review",
        "stage_status": row.get("stage_status") or "pending",
        "run_mode": row.get("run_mode") or "ai_assisted",
        "next_action": row.get("next_action") or "Review source evidence and generate onboarding drafts.",
        "trigger_type": row.get("trigger_type") or "source_upload",
        "evidence_snapshot_id": row.get("evidence_snapshot_id") or "",
        "operation_count": int(row.get("operation_count") or 0),
        "field_count": int(row.get("field_count") or 0),
        "mapping_count": int(row.get("mapping_count") or 0),
        "proposal_count": int(row.get("proposal_count") or 0),
        "pending_proposal_count": int(row.get("pending_proposal_count") or 0),
        "suggestion_status": row.get("suggestion_status") or "ready_for_field_extraction",
        "created_at": _isoformat(row.get("started_at") or row.get("created_at")),
        "updated_at": _isoformat(row.get("updated_at")),
    }


def _proposal_bundle_from_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row["id"],
        "run_id": row["run_id"],
        "source_id": row["source_id"],
        "source_name": row.get("source_name") or row.get("source_id") or "",
        "status": row.get("status") or "draft",
        "proposal_count": int(row.get("proposal_count") or 0),
        "pending_count": int(row.get("pending_count") or 0),
        "approved_count": int(row.get("approved_count") or 0),
        "rejected_count": int(row.get("rejected_count") or 0),
        "entity_counts": row.get("entity_counts") or {},
        "evidence_snapshot_id": row.get("evidence_snapshot_id") or "",
        "proposal_ids": _json_list(row.get("proposal_ids")),
        "updated_at": _isoformat(row.get("updated_at")),
    }


def _evidence_snapshot_from_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row["id"],
        "run_id": row.get("run_id") or "",
        "source_id": row.get("source_id") or "",
        "snapshot_type": row.get("snapshot_type") or "source_upload",
        "content_hash": row.get("content_hash") or "",
        "source_ref": row.get("source_ref") or {},
        "operation_evidence": row.get("operation_evidence") or [],
        "schema_evidence": row.get("schema_evidence") or [],
        "sample_values": row.get("sample_values") or {},
        "ai_context": row.get("ai_context") or {},
        "created_at": _isoformat(row.get("created_at")),
    }


def _work_queue_task_from_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row["id"],
        "run_id": row.get("run_id") or "",
        "source_id": row.get("source_id") or "",
        "evidence_snapshot_id": row.get("evidence_snapshot_id") or "",
        "operation_id": row.get("operation_id"),
        "operation_name": row.get("operation_name") or "",
        "field_id": row.get("field_id"),
        "field_name": row.get("field_name") or "",
        "field_path": row.get("field_path") or "",
        "stage": row.get("stage") or "source_review",
        "task_type": row.get("task_type") or "",
        "status": row.get("status") or "open",
        "supports_ai_draft": bool(row.get("supports_ai_draft", True)),
        "draft_status": row.get("draft_status") or "not_started",
        "depends_on": row.get("depends_on") or [],
        "recommended_action": row.get("recommended_action") or "",
        "draft_payload": row.get("draft_payload") or {},
        "draft_rationale": row.get("draft_rationale") or "",
        "draft_confidence": float(row["draft_confidence"]) if row.get("draft_confidence") is not None else None,
        "priority": int(row.get("priority") or 100),
        "title": row.get("title") or "",
        "payload": row.get("payload") or {},
        "proposal_id": row.get("proposal_id"),
        "assigned_to": row.get("assigned_to"),
        "created_at": _isoformat(row.get("created_at")),
        "updated_at": _isoformat(row.get("updated_at")),
    }


def _execution_source_from_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row["id"],
        "name": row["name"],
        "provider": row.get("provider") or "",
        "source_type": row.get("source_type") or "api",
        "description": row.get("description") or "",
        "status": row.get("status") or "draft",
        "config": row.get("config") or {},
        "created_at": _isoformat(row.get("created_at")),
        "updated_at": _isoformat(row.get("updated_at")),
    }


def _execution_asset_from_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row["id"],
        "source_id": row["source_id"],
        "source_name": row.get("source_name") or "",
        "source_type": row.get("source_source_type") or "",
        "name": row["name"],
        "asset_type": row.get("asset_type") or "other",
        "locator": row.get("locator") or "",
        "description": row.get("description") or "",
        "version": row.get("version") or "1.0.0",
        "lifecycle": row.get("lifecycle") or "draft",
        "status": row.get("status") or "draft",
        "metadata": row.get("metadata") or {},
        "created_at": _isoformat(row.get("created_at")),
        "updated_at": _isoformat(row.get("updated_at")),
    }


def _execution_operation_from_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row["id"],
        "access_path_id": row["access_path_id"],
        "asset_id": row.get("asset_id"),
        "source_id": row.get("source_id"),
        "operation_key": row["operation_key"],
        "namespace": row.get("namespace") or "public",
        "name": row["name"],
        "description": row.get("description") or "",
        "version": row.get("version") or "1.0.0",
        "lifecycle": row.get("lifecycle") or "draft",
        "status": row.get("status") or "draft",
        "input_spec": row.get("input_spec") or [],
        "output_spec": row.get("output_spec") or [],
        "auth_spec": row.get("auth_spec") or {},
        "contract_spec": row.get("contract_spec") or {},
        "metadata": row.get("metadata") or {},
        "source_name": row.get("source_name") or "",
        "source_type": row.get("source_source_type") or "",
        "asset_name": row.get("asset_name") or "",
        "asset_type": row.get("asset_type") or "",
        "access_path_name": row.get("access_path_name") or "",
        "access_type": row.get("access_type") or "",
        "access_path_locator": row.get("access_path_locator") or "",
        "http_method": row.get("http_method") or "",
        "created_at": _isoformat(row.get("created_at")),
        "updated_at": _isoformat(row.get("updated_at")),
    }


def _operation_field_from_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row["id"],
        "operation_id": row["operation_id"],
        "variant_id": row.get("variant_id"),
        "scope": row.get("scope") or "input",
        "raw_name": row.get("raw_name") or "",
        "display_name": row.get("display_name") or "",
        "field_path": row.get("field_path") or "",
        "data_type": row.get("data_type") or "string",
        "is_required": bool(row.get("is_required")),
        "description": row.get("description") or "",
        "version": row.get("version") or "1.0.0",
        "lifecycle": row.get("lifecycle") or "draft",
        "metadata": row.get("metadata") or {},
        "created_at": _isoformat(row.get("created_at")),
        "updated_at": _isoformat(row.get("updated_at")),
    }


def _operation_variant_from_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row["id"],
        "operation_id": row["operation_id"],
        "variant_key": row.get("variant_key") or "",
        "name": row.get("name") or "",
        "description": row.get("description") or "",
        "version": row.get("version") or "1.0.0",
        "lifecycle": row.get("lifecycle") or "draft",
        "status": row.get("status") or "draft",
        "fixed_semantic_arguments": row.get("fixed_semantic_arguments") or {},
        "fixed_raw_arguments": row.get("fixed_raw_arguments") or {},
        "metadata": {
            **(row.get("metadata") or {}),
            "operation_name": row.get("operation_name") or "",
            "operation_key": row.get("operation_key") or "",
            "source_name": row.get("source_name") or "",
        },
        "created_at": _isoformat(row.get("created_at")),
        "updated_at": _isoformat(row.get("updated_at")),
    }


def _capability_from_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row["id"],
        "capability_key": row["capability_key"],
        "namespace": row.get("namespace") or "public",
        "name": row["name"],
        "description": row.get("description") or "",
        "version": row.get("version") or "1.0.0",
        "lifecycle": row.get("lifecycle") or "draft",
        "status": row.get("status") or "draft",
        "intent_spec": row.get("intent_spec") or {},
        "input_semantic_types": _json_list(row.get("input_semantic_types")),
        "output_semantic_types": _json_list(row.get("output_semantic_types")),
        "metadata": row.get("metadata") or {},
        "created_by": row.get("created_by") or "system",
        "reviewed_by": row.get("reviewed_by"),
        "approved_at": _isoformat(row.get("approved_at")),
        "evidence": row.get("evidence") or [],
        "confidence": row.get("confidence"),
        "created_at": _isoformat(row.get("created_at")),
        "updated_at": _isoformat(row.get("updated_at")),
    }


def _field_mapping_from_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row["id"],
        "field_id": row.get("field_id"),
        "source_id": row.get("source_id"),
        "operation_id": row["operation_id"],
        "variant_id": row.get("variant_id"),
        "access_path_id": row.get("access_path_id"),
        "field_path": row.get("field_path") or "",
        "semantic_type_id": row["semantic_type_id"],
        "canonical_attribute_id": row.get("canonical_attribute_id"),
        "mapping_kind": row.get("mapping_kind") or "direct",
        "mapping_type": row.get("mapping_type") or "exact",
        "version": row.get("version") or "1.0.0",
        "lifecycle": row.get("lifecycle") or "draft",
        "status": row.get("status") or "draft",
        "namespace": row.get("namespace") or "public",
        "transform_spec": row.get("transform_spec") or {},
        "enum_mapping": row.get("enum_mapping") or {},
        "notes": row.get("notes") or "",
        "created_by": row.get("created_by") or "system",
        "reviewed_by": row.get("reviewed_by"),
        "approved_at": _isoformat(row.get("approved_at")),
        "evidence": row.get("evidence") or [],
        "confidence": row.get("confidence"),
        "created_at": _isoformat(row.get("created_at")),
        "updated_at": _isoformat(row.get("updated_at")),
    }


def _canonical_entity_from_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row["id"],
        "semantic_type_id": row.get("semantic_type_id"),
        "name": row["name"],
        "namespace": row.get("namespace") or "public",
        "description": row.get("description") or "",
        "version": row.get("version") or "1.0.0",
        "lifecycle": row.get("lifecycle") or "draft",
        "status": row.get("status") or "draft",
        "metadata": row.get("metadata") or {},
        "created_by": row.get("created_by") or "system",
        "reviewed_by": row.get("reviewed_by"),
        "approved_at": _isoformat(row.get("approved_at")),
        "evidence": row.get("evidence") or [],
        "confidence": row.get("confidence"),
        "created_at": _isoformat(row.get("created_at")),
        "updated_at": _isoformat(row.get("updated_at")),
    }


def _canonical_attribute_from_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row["id"],
        "entity_id": row["entity_id"],
        "entity_name": row.get("entity_name") or "",
        "semantic_type_id": row.get("semantic_type_id"),
        "name": row["name"],
        "namespace": row.get("namespace") or "public",
        "description": row.get("description") or "",
        "datatype": row.get("datatype") or "string",
        "identity_role": row.get("identity_role") or "",
        "version": row.get("version") or "1.0.0",
        "lifecycle": row.get("lifecycle") or "draft",
        "status": row.get("status") or "draft",
        "metadata": row.get("metadata") or {},
        "created_by": row.get("created_by") or "system",
        "reviewed_by": row.get("reviewed_by"),
        "approved_at": _isoformat(row.get("approved_at")),
        "evidence": row.get("evidence") or [],
        "confidence": row.get("confidence"),
        "created_at": _isoformat(row.get("created_at")),
        "updated_at": _isoformat(row.get("updated_at")),
    }


def _canonical_relation_from_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row["id"],
        "source_entity_id": row["source_entity_id"],
        "target_entity_id": row["target_entity_id"],
        "source_entity_name": row.get("source_entity_name") or "",
        "target_entity_name": row.get("target_entity_name") or "",
        "relation_type": row["relation_type"],
        "forward_label": row.get("forward_label") or "",
        "reverse_label": row.get("reverse_label") or "",
        "version": row.get("version") or "1.0.0",
        "lifecycle": row.get("lifecycle") or "draft",
        "status": row.get("status") or "draft",
        "metadata": row.get("metadata") or {},
        "created_by": row.get("created_by") or "system",
        "reviewed_by": row.get("reviewed_by"),
        "approved_at": _isoformat(row.get("approved_at")),
        "evidence": row.get("evidence") or [],
        "confidence": row.get("confidence"),
        "created_at": _isoformat(row.get("created_at")),
        "updated_at": _isoformat(row.get("updated_at")),
    }


def _attach_execution_source_overlay(
    source: dict[str, Any],
    proposal: dict[str, Any] | None,
) -> dict[str, Any]:
    approved_snapshot = {
        key: value
        for key, value in source.items()
        if key not in {"draft_change_type", "draft_snapshot", "approved_snapshot", "pending_proposal_id"}
    }
    if proposal is None:
        source["draft_change_type"] = ""
        source["draft_snapshot"] = None
        source["approved_snapshot"] = approved_snapshot if source.get("status") == "approved" else None
        return source
    payload = proposal.get("payload") or {}
    source["draft_change_type"] = proposal.get("change_type") or ""
    source["approved_snapshot"] = payload.get("approved_snapshot") or approved_snapshot
    source["draft_snapshot"] = payload.get("draft_snapshot") or None
    source["pending_proposal_id"] = proposal.get("id")
    return source


def _attach_capability_overlay(capability: dict[str, Any], proposal: dict[str, Any] | None) -> dict[str, Any]:
    approved_snapshot = {
        key: value
        for key, value in capability.items()
        if key not in {"draft_change_type", "draft_snapshot", "approved_snapshot", "pending_proposal_id"}
    }
    if proposal is None:
        capability["draft_change_type"] = ""
        capability["draft_snapshot"] = None
        capability["approved_snapshot"] = approved_snapshot if capability.get("status") == "approved" else None
        return capability
    payload = proposal.get("payload") or {}
    capability["draft_change_type"] = proposal.get("change_type") or ""
    capability["approved_snapshot"] = payload.get("approved_snapshot") or approved_snapshot
    capability["draft_snapshot"] = None if proposal.get("change_type") == "delete" else payload.get("draft_snapshot") or None
    capability["pending_proposal_id"] = proposal.get("id")
    return capability


def _attach_mapping_overlay(mapping: dict[str, Any], proposal: dict[str, Any] | None) -> dict[str, Any]:
    approved_snapshot = {
        key: value
        for key, value in mapping.items()
        if key not in {"draft_change_type", "draft_snapshot", "approved_snapshot", "pending_proposal_id"}
    }
    if proposal is None:
        mapping["draft_change_type"] = ""
        mapping["draft_snapshot"] = None
        mapping["approved_snapshot"] = approved_snapshot if mapping.get("status") == "approved" else None
        return mapping
    payload = proposal.get("payload") or {}
    mapping["draft_change_type"] = proposal.get("change_type") or ""
    mapping["approved_snapshot"] = payload.get("approved_snapshot") or approved_snapshot
    mapping["draft_snapshot"] = None if proposal.get("change_type") == "delete" else payload.get("draft_snapshot") or None
    mapping["pending_proposal_id"] = proposal.get("id")
    return mapping


def _attach_operation_variant_overlay(variant: dict[str, Any], proposal: dict[str, Any] | None) -> dict[str, Any]:
    approved_snapshot = {
        key: value
        for key, value in variant.items()
        if key not in {"draft_change_type", "draft_snapshot", "approved_snapshot", "pending_proposal_id"}
    }
    if proposal is None:
        variant["draft_change_type"] = ""
        variant["draft_snapshot"] = None
        variant["approved_snapshot"] = approved_snapshot if variant.get("status") == "approved" else None
        return variant
    payload = proposal.get("payload") or {}
    variant["draft_change_type"] = proposal.get("change_type") or ""
    variant["approved_snapshot"] = payload.get("approved_snapshot") or approved_snapshot
    variant["draft_snapshot"] = None if proposal.get("change_type") == "delete" else payload.get("draft_snapshot") or None
    variant["pending_proposal_id"] = proposal.get("id")
    return variant


def _attach_canonical_entity_overlay(entity: dict[str, Any], proposal: dict[str, Any] | None) -> dict[str, Any]:
    approved_snapshot = {
        key: value
        for key, value in entity.items()
        if key not in {"draft_change_type", "draft_snapshot", "approved_snapshot", "pending_proposal_id"}
    }
    if proposal is None:
        entity["draft_change_type"] = ""
        entity["draft_snapshot"] = None
        entity["approved_snapshot"] = approved_snapshot if entity.get("status") == "approved" else None
        return entity
    payload = proposal.get("payload") or {}
    entity["draft_change_type"] = proposal.get("change_type") or ""
    entity["approved_snapshot"] = payload.get("approved_snapshot") or approved_snapshot
    entity["draft_snapshot"] = None if proposal.get("change_type") == "delete" else payload.get("draft_snapshot") or None
    entity["pending_proposal_id"] = proposal.get("id")
    return entity


def _attach_canonical_attribute_overlay(attribute: dict[str, Any], proposal: dict[str, Any] | None) -> dict[str, Any]:
    approved_snapshot = {
        key: value
        for key, value in attribute.items()
        if key not in {"draft_change_type", "draft_snapshot", "approved_snapshot", "pending_proposal_id"}
    }
    if proposal is None:
        attribute["draft_change_type"] = ""
        attribute["draft_snapshot"] = None
        attribute["approved_snapshot"] = approved_snapshot if attribute.get("status") == "approved" else None
        return attribute
    payload = proposal.get("payload") or {}
    attribute["draft_change_type"] = proposal.get("change_type") or ""
    attribute["approved_snapshot"] = payload.get("approved_snapshot") or approved_snapshot
    attribute["draft_snapshot"] = None if proposal.get("change_type") == "delete" else payload.get("draft_snapshot") or None
    attribute["pending_proposal_id"] = proposal.get("id")
    return attribute


def _attach_canonical_relation_overlay(relation: dict[str, Any], proposal: dict[str, Any] | None) -> dict[str, Any]:
    approved_snapshot = {
        key: value
        for key, value in relation.items()
        if key not in {"draft_change_type", "draft_snapshot", "approved_snapshot", "pending_proposal_id"}
    }
    if proposal is None:
        relation["draft_change_type"] = ""
        relation["draft_snapshot"] = None
        relation["approved_snapshot"] = approved_snapshot if relation.get("status") == "approved" else None
        return relation
    payload = proposal.get("payload") or {}
    relation["draft_change_type"] = proposal.get("change_type") or ""
    relation["approved_snapshot"] = payload.get("approved_snapshot") or approved_snapshot
    relation["draft_snapshot"] = None if proposal.get("change_type") == "delete" else payload.get("draft_snapshot") or None
    relation["pending_proposal_id"] = proposal.get("id")
    return relation


def _attach_relationship_overlay(
    relationship: dict[str, Any],
    proposal: dict[str, Any] | None,
) -> dict[str, Any]:
    approved_snapshot = {
        key: value
        for key, value in relationship.items()
        if key not in {"draft_change_type", "draft_snapshot", "approved_snapshot", "pending_proposal_id"}
    }
    if proposal is None:
        relationship["draft_change_type"] = ""
        relationship["draft_snapshot"] = None
        relationship["approved_snapshot"] = approved_snapshot if relationship.get("status") == "approved" else None
        return relationship
    payload = proposal.get("payload") or {}
    relationship["draft_change_type"] = proposal.get("change_type") or ""
    relationship["approved_snapshot"] = payload.get("approved_snapshot") or approved_snapshot
    relationship["draft_snapshot"] = (
        None if proposal.get("change_type") == "delete" else payload.get("draft_snapshot") or None
    )
    relationship["pending_proposal_id"] = proposal.get("id")
    return relationship


def _attach_semantic_type_overlay(
    semantic_type: dict[str, Any],
    proposal: dict[str, Any] | None,
) -> dict[str, Any]:
    approved_snapshot = {
        key: value
        for key, value in semantic_type.items()
        if key not in {"draft_change_type", "draft_snapshot", "approved_snapshot", "pending_proposal_id"}
    }
    if proposal is None:
        semantic_type["draft_change_type"] = ""
        semantic_type["draft_snapshot"] = None
        semantic_type["approved_snapshot"] = approved_snapshot if semantic_type.get("status") == "approved" else None
        return semantic_type
    payload = proposal.get("payload") or {}
    semantic_type["draft_change_type"] = proposal.get("change_type") or ""
    semantic_type["approved_snapshot"] = payload.get("approved_snapshot") or approved_snapshot
    semantic_type["draft_snapshot"] = payload.get("draft_snapshot") or None
    semantic_type["pending_proposal_id"] = proposal.get("id")
    return semantic_type


def _load_pending_semantic_type_update_proposals(cur: Any, semantic_type_ids: list[str]) -> dict[str, dict[str, Any]]:
    if not semantic_type_ids:
        return {}
    cur.execute(
        """
        select *
        from semantic_platform.proposals
        where entity_type = 'semantic_type'
          and status = 'pending_review'
          and change_type = 'update'
          and entity_id = any(%s)
        order by created_at desc
        """,
        (semantic_type_ids,),
    )
    items = [_proposal_from_row(row) for row in cur.fetchall()]
    result: dict[str, dict[str, Any]] = {}
    for item in items:
        entity_id = str(item.get("entity_id") or "")
        if entity_id and entity_id not in result:
            result[entity_id] = item
    return result


def _load_pending_semantic_type_update_proposal(cur: Any, semantic_type_id: str) -> dict[str, Any] | None:
    proposals = _load_pending_semantic_type_update_proposals(cur, [semantic_type_id])
    return proposals.get(semantic_type_id)


def _load_pending_relationship_proposals(cur: Any, relationship_ids: list[str]) -> dict[str, dict[str, Any]]:
    if not relationship_ids:
        return {}
    cur.execute(
        """
        select *
        from semantic_platform.proposals
        where entity_type = 'semantic_relationship'
          and status = 'pending_review'
          and entity_id = any(%s)
        order by created_at desc
        """,
        (relationship_ids,),
    )
    items = [_proposal_from_row(row) for row in cur.fetchall()]
    result: dict[str, dict[str, Any]] = {}
    for item in items:
        entity_id = str(item.get("entity_id") or "")
        if entity_id and entity_id not in result:
            result[entity_id] = item
    return result


def _load_pending_execution_source_update_proposals(cur: Any, source_ids: list[str]) -> dict[str, dict[str, Any]]:
    if not source_ids:
        return {}
    cur.execute(
        """
        select *
        from semantic_platform.proposals
        where entity_type = 'execution_source'
          and status = 'pending_review'
          and change_type = 'update'
          and entity_id = any(%s)
        order by created_at desc
        """,
        (source_ids,),
    )
    items = [_proposal_from_row(row) for row in cur.fetchall()]
    result: dict[str, dict[str, Any]] = {}
    for item in items:
        entity_id = str(item.get("entity_id") or "")
        if entity_id and entity_id not in result:
            result[entity_id] = item
    return result


def _load_pending_execution_source_update_proposal(cur: Any, source_id: str) -> dict[str, Any] | None:
    proposals = _load_pending_execution_source_update_proposals(cur, [source_id])
    return proposals.get(source_id)


def _load_pending_capability_update_proposals(cur: Any, capability_ids: list[str]) -> dict[str, dict[str, Any]]:
    if not capability_ids:
        return {}
    cur.execute(
        """
        select *
        from semantic_platform.proposals
        where entity_type = 'capability'
          and status = 'pending_review'
          and change_type = 'update'
          and entity_id = any(%s)
        order by created_at desc
        """,
        (capability_ids,),
    )
    items = [_proposal_from_row(row) for row in cur.fetchall()]
    result: dict[str, dict[str, Any]] = {}
    for item in items:
        entity_id = str(item.get("entity_id") or "")
        if entity_id and entity_id not in result:
            result[entity_id] = item
    return result


def _load_pending_capability_update_proposal(cur: Any, capability_id: str) -> dict[str, Any] | None:
    proposals = _load_pending_capability_update_proposals(cur, [capability_id])
    return proposals.get(capability_id)


def _load_pending_mapping_update_proposals(cur: Any, mapping_ids: list[str]) -> dict[str, dict[str, Any]]:
    if not mapping_ids:
        return {}
    cur.execute(
        """
        select *
        from semantic_platform.proposals
        where entity_type = 'field_mapping'
          and status = 'pending_review'
          and change_type = 'update'
          and entity_id = any(%s)
        order by created_at desc
        """,
        (mapping_ids,),
    )
    items = [_proposal_from_row(row) for row in cur.fetchall()]
    result: dict[str, dict[str, Any]] = {}
    for item in items:
        entity_id = str(item.get("entity_id") or "")
        if entity_id and entity_id not in result:
            result[entity_id] = item
    return result


def _load_pending_mapping_update_proposal(cur: Any, mapping_id: str) -> dict[str, Any] | None:
    proposals = _load_pending_mapping_update_proposals(cur, [mapping_id])
    return proposals.get(mapping_id)


def _load_pending_operation_variant_proposals(cur: Any, variant_ids: list[str]) -> dict[str, dict[str, Any]]:
    if not variant_ids:
        return {}
    cur.execute(
        """
        select *
        from semantic_platform.proposals
        where entity_type = 'operation_variant'
          and status = 'pending_review'
          and entity_id = any(%s)
        order by created_at desc
        """,
        (variant_ids,),
    )
    items = [_proposal_from_row(row) for row in cur.fetchall()]
    result: dict[str, dict[str, Any]] = {}
    for item in items:
        entity_id = str(item.get("entity_id") or "")
        if entity_id and entity_id not in result:
            result[entity_id] = item
    return result


def _load_pending_canonical_entity_update_proposals(cur: Any, entity_ids: list[str]) -> dict[str, dict[str, Any]]:
    if not entity_ids:
        return {}
    cur.execute(
        """
        select *
        from semantic_platform.proposals
        where entity_type = 'canonical_entity'
          and status = 'pending_review'
          and change_type = 'update'
          and entity_id = any(%s)
        order by created_at desc
        """,
        (entity_ids,),
    )
    items = [_proposal_from_row(row) for row in cur.fetchall()]
    result: dict[str, dict[str, Any]] = {}
    for item in items:
        entity_id = str(item.get("entity_id") or "")
        if entity_id and entity_id not in result:
            result[entity_id] = item
    return result


def _load_pending_canonical_attribute_update_proposals(cur: Any, attribute_ids: list[str]) -> dict[str, dict[str, Any]]:
    if not attribute_ids:
        return {}
    cur.execute(
        """
        select *
        from semantic_platform.proposals
        where entity_type = 'canonical_attribute'
          and status = 'pending_review'
          and change_type = 'update'
          and entity_id = any(%s)
        order by created_at desc
        """,
        (attribute_ids,),
    )
    items = [_proposal_from_row(row) for row in cur.fetchall()]
    result: dict[str, dict[str, Any]] = {}
    for item in items:
        entity_id = str(item.get("entity_id") or "")
        if entity_id and entity_id not in result:
            result[entity_id] = item
    return result


def _load_pending_canonical_relation_update_proposals(cur: Any, relation_ids: list[str]) -> dict[str, dict[str, Any]]:
    if not relation_ids:
        return {}
    cur.execute(
        """
        select *
        from semantic_platform.proposals
        where entity_type = 'canonical_relation'
          and status = 'pending_review'
          and change_type = 'update'
          and entity_id = any(%s)
        order by created_at desc
        """,
        (relation_ids,),
    )
    items = [_proposal_from_row(row) for row in cur.fetchall()]
    result: dict[str, dict[str, Any]] = {}
    for item in items:
        entity_id = str(item.get("entity_id") or "")
        if entity_id and entity_id not in result:
            result[entity_id] = item
    return result


def _json_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value]
    if isinstance(value, str):
        return _string_list(value)
    return []


def _isoformat(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    return str(value)


def _find_semantic_type(store: dict[str, Any], semantic_type_id: str) -> dict[str, Any] | None:
    return next((item for item in store["semantic_types"] if item["id"] == semantic_type_id), None)


def _find_semantic_type_by_name(store: dict[str, Any], name: str) -> dict[str, Any] | None:
    return next((item for item in store["semantic_types"] if item["name"].lower() == name.lower()), None)


def _find_relationship(store: dict[str, Any], relationship_id: str) -> dict[str, Any] | None:
    return next((item for item in store["relationships"] if item["id"] == relationship_id), None)


def _find_execution_source(store: dict[str, Any], source_id: str) -> dict[str, Any] | None:
    return next((item for item in store["execution_sources"] if item["id"] == source_id), None)


def _find_execution_source_by_name(store: dict[str, Any], name: str) -> dict[str, Any] | None:
    return next((item for item in store["execution_sources"] if item["name"].lower() == name.lower()), None)


def _normalize_semantic_type_payload_db(
    cur: Any,
    payload: dict[str, Any],
    *,
    current: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized = dict(payload)
    entity_kind = str(normalized.get("entity_kind") or (current or {}).get("entity_kind") or "attribute").strip()
    normalized["entity_kind"] = entity_kind
    if entity_kind == "attribute":
        parent_entity_id = str(normalized.get("parent_entity_id") or (current or {}).get("parent_entity_id") or "").strip()
        if not parent_entity_id:
            raise ValueError("parent_entity_id is required for attribute semantic types")
        parent = _load_semantic_type(cur, parent_entity_id)
        if parent.get("entity_kind") != "entity":
            raise ValueError("parent_entity_id must reference an entity semantic type")
        normalized["parent_entity_id"] = parent["id"]
        normalized["parent_entity_name"] = parent["name"]
    else:
        normalized["parent_entity_id"] = ""
        normalized["parent_entity_name"] = ""
    return normalized


def _normalize_semantic_type_payload_file_store(
    store: dict[str, Any],
    payload: dict[str, Any],
    *,
    current: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized = dict(payload)
    entity_kind = str(normalized.get("entity_kind") or (current or {}).get("entity_kind") or "attribute").strip()
    normalized["entity_kind"] = entity_kind
    if entity_kind == "attribute":
        parent_entity_id = str(normalized.get("parent_entity_id") or (current or {}).get("parent_entity_id") or "").strip()
        if not parent_entity_id:
            raise ValueError("parent_entity_id is required for attribute semantic types")
        parent = _find_semantic_type(store, parent_entity_id)
        if parent is None:
            raise KeyError(parent_entity_id)
        if parent.get("entity_kind") != "entity":
            raise ValueError("parent_entity_id must reference an entity semantic type")
        normalized["parent_entity_id"] = parent["id"]
        normalized["parent_entity_name"] = parent["name"]
    else:
        normalized["parent_entity_id"] = ""
        normalized["parent_entity_name"] = ""
    return normalized


def _count_status(items: list[dict[str, Any]], status: str) -> int:
    return sum(1 for item in items if item.get("status") == status)


def _apply_approval_file_store(store: dict[str, Any], proposal: dict[str, Any]) -> None:
    if proposal["entity_type"] == "semantic_type":
        if proposal.get("change_type") == "delete":
            semantic_type_id = proposal["entity_id"]
            store["semantic_types"] = [item for item in store["semantic_types"] if item["id"] != semantic_type_id]
            store["relationships"] = [
                item
                for item in store["relationships"]
                if item["source_id"] != semantic_type_id and item["target_id"] != semantic_type_id
            ]
        else:
            record = _find_semantic_type(store, proposal["entity_id"])
            if record is not None:
                record["status"] = "approved"
                record["updated_at"] = _now()
    if proposal["entity_type"] == "semantic_relationship":
        if proposal.get("change_type") == "delete":
            store["relationships"] = [item for item in store["relationships"] if item["id"] != proposal["entity_id"]]
        else:
            relationship = _find_relationship(store, proposal["entity_id"])
            if relationship is not None:
                relationship["status"] = "approved"
                relationship["updated_at"] = _now()
    if proposal["entity_type"] == "execution_source":
        if proposal.get("change_type") == "delete":
            store["execution_sources"] = [item for item in store["execution_sources"] if item["id"] != proposal["entity_id"]]
        elif proposal.get("change_type") == "update":
            source = _find_execution_source(store, proposal["entity_id"])
            payload = proposal.get("payload") or {}
            draft_snapshot = payload.get("draft_snapshot") or {}
            if source is not None and draft_snapshot:
                source.update({**draft_snapshot, "status": "approved", "updated_at": _now()})
        else:
            source = _find_execution_source(store, proposal["entity_id"])
            if source is not None:
                source["status"] = "approved"
                source["updated_at"] = _now()
    if proposal["entity_type"] == "operation_variant":
        if proposal.get("change_type") == "delete":
            store["operation_variants"] = [item for item in store["operation_variants"] if item["id"] != proposal["entity_id"]]
        elif proposal.get("change_type") == "update":
            variant = next((item for item in store["operation_variants"] if item["id"] == proposal["entity_id"]), None)
            payload = proposal.get("payload") or {}
            draft_snapshot = payload.get("draft_snapshot") or {}
            if variant is not None and draft_snapshot:
                variant.update({**draft_snapshot, "status": "approved", "updated_at": _now()})
        else:
            variant = next((item for item in store["operation_variants"] if item["id"] == proposal["entity_id"]), None)
            if variant is not None:
                variant["status"] = "approved"
                variant["updated_at"] = _now()
    if proposal["entity_type"] == "capability":
        if proposal.get("change_type") == "delete":
            store["capabilities"] = [item for item in store["capabilities"] if item["id"] != proposal["entity_id"]]
        elif proposal.get("change_type") == "update":
            capability = next((item for item in store["capabilities"] if item["id"] == proposal["entity_id"]), None)
            payload = proposal.get("payload") or {}
            draft_snapshot = payload.get("draft_snapshot") or {}
            if capability is not None and draft_snapshot:
                capability.update({**draft_snapshot, "status": "approved", "updated_at": _now()})
        else:
            capability = next((item for item in store["capabilities"] if item["id"] == proposal["entity_id"]), None)
            if capability is not None:
                capability["status"] = "approved"
                capability["updated_at"] = _now()
    if proposal["entity_type"] == "field_mapping":
        if proposal.get("change_type") == "delete":
            store["field_mappings"] = [item for item in store["field_mappings"] if item["id"] != proposal["entity_id"]]
        elif proposal.get("change_type") == "update":
            mapping = next((item for item in store["field_mappings"] if item["id"] == proposal["entity_id"]), None)
            payload = proposal.get("payload") or {}
            draft_snapshot = payload.get("draft_snapshot") or {}
            if mapping is not None and draft_snapshot:
                mapping.update({**draft_snapshot, "status": "approved", "updated_at": _now()})
        else:
            mapping = next((item for item in store["field_mappings"] if item["id"] == proposal["entity_id"]), None)
            if mapping is not None:
                mapping["status"] = "approved"
                mapping["updated_at"] = _now()
    if proposal["entity_type"] == "canonical_entity":
        if proposal.get("change_type") == "delete":
            entity_id = proposal["entity_id"]
            store["canonical_entities"] = [item for item in store["canonical_entities"] if item["id"] != entity_id]
            store["canonical_attributes"] = [item for item in store["canonical_attributes"] if item["entity_id"] != entity_id]
            store["canonical_relations"] = [
                item
                for item in store["canonical_relations"]
                if item["source_entity_id"] != entity_id and item["target_entity_id"] != entity_id
            ]
        elif proposal.get("change_type") == "update":
            entity = next((item for item in store["canonical_entities"] if item["id"] == proposal["entity_id"]), None)
            payload = proposal.get("payload") or {}
            draft_snapshot = payload.get("draft_snapshot") or {}
            if entity is not None and draft_snapshot:
                entity.update({**draft_snapshot, "status": "approved", "updated_at": _now()})
        else:
            entity = next((item for item in store["canonical_entities"] if item["id"] == proposal["entity_id"]), None)
            if entity is not None:
                entity["status"] = "approved"
                entity["updated_at"] = _now()
    if proposal["entity_type"] == "canonical_attribute":
        if proposal.get("change_type") == "delete":
            store["canonical_attributes"] = [item for item in store["canonical_attributes"] if item["id"] != proposal["entity_id"]]
        elif proposal.get("change_type") == "update":
            attribute = next((item for item in store["canonical_attributes"] if item["id"] == proposal["entity_id"]), None)
            payload = proposal.get("payload") or {}
            draft_snapshot = payload.get("draft_snapshot") or {}
            if attribute is not None and draft_snapshot:
                attribute.update({**draft_snapshot, "status": "approved", "updated_at": _now()})
        else:
            attribute = next((item for item in store["canonical_attributes"] if item["id"] == proposal["entity_id"]), None)
            if attribute is not None:
                attribute["status"] = "approved"
                attribute["updated_at"] = _now()
    if proposal["entity_type"] == "canonical_relation":
        if proposal.get("change_type") == "delete":
            store["canonical_relations"] = [item for item in store["canonical_relations"] if item["id"] != proposal["entity_id"]]
        elif proposal.get("change_type") == "update":
            relation = next((item for item in store["canonical_relations"] if item["id"] == proposal["entity_id"]), None)
            payload = proposal.get("payload") or {}
            draft_snapshot = payload.get("draft_snapshot") or {}
            if relation is not None and draft_snapshot:
                relation.update({**draft_snapshot, "status": "approved", "updated_at": _now()})
        else:
            relation = next((item for item in store["canonical_relations"] if item["id"] == proposal["entity_id"]), None)
            if relation is not None:
                relation["status"] = "approved"
                relation["updated_at"] = _now()


def _apply_rejection_file_store(store: dict[str, Any], proposal: dict[str, Any]) -> None:
    if proposal["change_type"] != "create":
        return
    if proposal["entity_type"] == "semantic_type":
        semantic_type_id = proposal["entity_id"]
        store["semantic_types"] = [item for item in store["semantic_types"] if item["id"] != semantic_type_id]
        store["relationships"] = [
            item
            for item in store["relationships"]
            if item["source_id"] != semantic_type_id and item["target_id"] != semantic_type_id
        ]
    if proposal["entity_type"] == "semantic_relationship":
        store["relationships"] = [item for item in store["relationships"] if item["id"] != proposal["entity_id"]]
    if proposal["entity_type"] == "execution_source":
        store["execution_sources"] = [item for item in store["execution_sources"] if item["id"] != proposal["entity_id"]]
    if proposal["entity_type"] == "operation_variant":
        store["operation_variants"] = [item for item in store["operation_variants"] if item["id"] != proposal["entity_id"]]
    if proposal["entity_type"] == "capability":
        store["capabilities"] = [item for item in store["capabilities"] if item["id"] != proposal["entity_id"]]
    if proposal["entity_type"] == "field_mapping":
        store["field_mappings"] = [item for item in store["field_mappings"] if item["id"] != proposal["entity_id"]]
    if proposal["entity_type"] == "canonical_entity":
        store["canonical_entities"] = [item for item in store["canonical_entities"] if item["id"] != proposal["entity_id"]]
    if proposal["entity_type"] == "canonical_attribute":
        store["canonical_attributes"] = [item for item in store["canonical_attributes"] if item["id"] != proposal["entity_id"]]
    if proposal["entity_type"] == "canonical_relation":
        store["canonical_relations"] = [item for item in store["canonical_relations"] if item["id"] != proposal["entity_id"]]


def _apply_approval_db(cur: Any, proposal: dict[str, Any]) -> None:
    if proposal["entity_type"] == "semantic_type":
        if proposal.get("change_type") == "delete":
            cur.execute("delete from semantic_platform.semantic_types where id = %s", (proposal["entity_id"],))
        elif proposal.get("change_type") == "update":
            payload = proposal.get("payload") or {}
            draft_snapshot = payload.get("draft_snapshot") or {}
            if draft_snapshot:
                cur.execute(
                    """
                    update semantic_platform.semantic_types
                    set urn = %(urn)s,
                        name = %(name)s,
                        description = %(description)s,
                        datatype = %(datatype)s,
                        entity_kind = %(entity_kind)s,
                        semantic_role = %(semantic_role)s,
                        parent_entity_id = %(parent_entity_id)s,
                        parent_entity_name = %(parent_entity_name)s,
                        aliases = %(aliases)s::jsonb,
                        owners = %(owners)s::jsonb,
                        tags = %(tags)s::jsonb,
                        documentation = %(documentation)s,
                        status = 'approved',
                        updated_at = now()
                    where id = %(id)s
                    """,
                    _sql_semantic_type_params(
                        {
                            **draft_snapshot,
                            "status": "approved",
                        }
                    ),
                )
        else:
            cur.execute(
                """
                update semantic_platform.semantic_types
                set status = 'approved',
                    updated_at = now()
                where id = %s
                """,
                (proposal["entity_id"],),
            )
    if proposal["entity_type"] == "semantic_relationship":
        if proposal.get("change_type") == "delete":
            cur.execute("delete from semantic_platform.semantic_relationships where id = %s", (proposal["entity_id"],))
        elif proposal.get("change_type") == "update":
            payload = proposal.get("payload") or {}
            draft_snapshot = payload.get("draft_snapshot") or {}
            if draft_snapshot:
                cur.execute(
                    """
                    update semantic_platform.semantic_relationships
                    set source_id = %(source_id)s,
                        source_name = %(source_name)s,
                        target_id = %(target_id)s,
                        target_name = %(target_name)s,
                        relation_type = %(relation_type)s,
                        status = 'approved',
                        updated_at = now()
                    where id = %(id)s
                    """,
                    {
                        **draft_snapshot,
                        "status": "approved",
                    },
                )
        else:
            cur.execute(
                """
                update semantic_platform.semantic_relationships
                set status = 'approved',
                    updated_at = now()
                where id = %s
                """,
                (proposal["entity_id"],),
            )
    if proposal["entity_type"] == "execution_source":
        if proposal.get("change_type") == "delete":
            cur.execute("delete from semantic_platform.execution_sources where id = %s", (proposal["entity_id"],))
        elif proposal.get("change_type") == "update":
            payload = proposal.get("payload") or {}
            draft_snapshot = payload.get("draft_snapshot") or {}
            if draft_snapshot:
                cur.execute(
                    """
                    update semantic_platform.execution_sources
                    set name = %(name)s,
                        provider = %(provider)s,
                        source_type = %(source_type)s,
                        description = %(description)s,
                        status = 'approved',
                        config = %(config)s::jsonb,
                        updated_at = now()
                    where id = %(id)s
                    """,
                    _sql_execution_source_params(
                        {
                            **draft_snapshot,
                            "status": "approved",
                        }
                    ),
                )
        else:
            cur.execute(
                """
                update semantic_platform.execution_sources
                set status = 'approved',
                    updated_at = now()
                where id = %s
                """,
                (proposal["entity_id"],),
            )
    if proposal["entity_type"] == "operation_variant":
        if proposal.get("change_type") == "delete":
            cur.execute("delete from semantic_platform.operation_variants where id = %s", (proposal["entity_id"],))
        elif proposal.get("change_type") == "update":
            payload = proposal.get("payload") or {}
            draft_snapshot = payload.get("draft_snapshot") or {}
            if draft_snapshot:
                cur.execute(
                    """
                    update semantic_platform.operation_variants
                    set operation_id = %(operation_id)s,
                        variant_key = %(variant_key)s,
                        name = %(name)s,
                        description = %(description)s,
                        version = %(version)s,
                        lifecycle = %(lifecycle)s,
                        status = 'approved',
                        fixed_semantic_arguments = %(fixed_semantic_arguments)s::jsonb,
                        fixed_raw_arguments = %(fixed_raw_arguments)s::jsonb,
                        metadata = %(metadata)s::jsonb,
                        updated_at = now()
                    where id = %(id)s
                    """,
                    _sql_operation_variant_params({**draft_snapshot, "status": "approved"}),
                )
        else:
            cur.execute(
                """
                update semantic_platform.operation_variants
                set status = 'approved',
                    updated_at = now()
                where id = %s
                """,
                (proposal["entity_id"],),
            )
    if proposal["entity_type"] == "capability":
        if proposal.get("change_type") == "delete":
            cur.execute("delete from semantic_platform.capabilities where id = %s", (proposal["entity_id"],))
        elif proposal.get("change_type") == "update":
            payload = proposal.get("payload") or {}
            draft_snapshot = payload.get("draft_snapshot") or {}
            if draft_snapshot:
                cur.execute(
                    """
                    update semantic_platform.capabilities
                    set capability_key = %(capability_key)s,
                        namespace = %(namespace)s,
                        name = %(name)s,
                        description = %(description)s,
                        version = %(version)s,
                        lifecycle = %(lifecycle)s,
                        status = 'approved',
                        intent_spec = %(intent_spec)s::jsonb,
                        input_semantic_types = %(input_semantic_types)s::jsonb,
                        output_semantic_types = %(output_semantic_types)s::jsonb,
                        metadata = %(metadata)s::jsonb,
                        created_by = %(created_by)s,
                        reviewed_by = %(reviewed_by)s,
                        approved_at = %(approved_at)s::timestamptz,
                        evidence = %(evidence)s::jsonb,
                        confidence = %(confidence)s,
                        updated_at = now()
                    where id = %(id)s
                    """,
                    _sql_capability_params({**draft_snapshot, "status": "approved"}),
                )
        else:
            cur.execute(
                """
                update semantic_platform.capabilities
                set status = 'approved',
                    updated_at = now()
                where id = %s
                """,
                (proposal["entity_id"],),
            )
    if proposal["entity_type"] == "field_mapping":
        if proposal.get("change_type") == "delete":
            cur.execute("delete from semantic_platform.field_mappings where id = %s", (proposal["entity_id"],))
        elif proposal.get("change_type") == "update":
            payload = proposal.get("payload") or {}
            draft_snapshot = payload.get("draft_snapshot") or {}
            if draft_snapshot:
                cur.execute(
                    """
                    update semantic_platform.field_mappings
                    set field_id = %(field_id)s,
                        source_id = %(source_id)s,
                        operation_id = %(operation_id)s,
                        variant_id = %(variant_id)s,
                        access_path_id = %(access_path_id)s,
                        field_path = %(field_path)s,
                        semantic_type_id = %(semantic_type_id)s,
                        canonical_attribute_id = %(canonical_attribute_id)s,
                        mapping_kind = %(mapping_kind)s,
                        mapping_type = %(mapping_type)s,
                        version = %(version)s,
                        lifecycle = %(lifecycle)s,
                        status = 'approved',
                        namespace = %(namespace)s,
                        transform_spec = %(transform_spec)s::jsonb,
                        enum_mapping = %(enum_mapping)s::jsonb,
                        notes = %(notes)s,
                        created_by = %(created_by)s,
                        reviewed_by = %(reviewed_by)s,
                        approved_at = %(approved_at)s::timestamptz,
                        evidence = %(evidence)s::jsonb,
                        confidence = %(confidence)s,
                        updated_at = now()
                    where id = %(id)s
                    """,
                    _sql_field_mapping_params({**draft_snapshot, "status": "approved"}),
                )
        else:
            cur.execute(
                """
                update semantic_platform.field_mappings
                set status = 'approved',
                    updated_at = now()
                where id = %s
                """,
                (proposal["entity_id"],),
            )
    if proposal["entity_type"] == "canonical_entity":
        if proposal.get("change_type") == "delete":
            cur.execute("delete from semantic_platform.canonical_entities where id = %s", (proposal["entity_id"],))
        elif proposal.get("change_type") == "update":
            payload = proposal.get("payload") or {}
            draft_snapshot = payload.get("draft_snapshot") or {}
            if draft_snapshot:
                cur.execute(
                    """
                    update semantic_platform.canonical_entities
                    set semantic_type_id = %(semantic_type_id)s,
                        name = %(name)s,
                        namespace = %(namespace)s,
                        description = %(description)s,
                        version = %(version)s,
                        lifecycle = %(lifecycle)s,
                        status = 'approved',
                        metadata = %(metadata)s::jsonb,
                        created_by = %(created_by)s,
                        reviewed_by = %(reviewed_by)s,
                        approved_at = %(approved_at)s::timestamptz,
                        evidence = %(evidence)s::jsonb,
                        confidence = %(confidence)s,
                        updated_at = now()
                    where id = %(id)s
                    """,
                    _sql_canonical_entity_params({**draft_snapshot, "status": "approved"}),
                )
        else:
            cur.execute("update semantic_platform.canonical_entities set status = 'approved', updated_at = now() where id = %s", (proposal["entity_id"],))
    if proposal["entity_type"] == "canonical_attribute":
        if proposal.get("change_type") == "delete":
            cur.execute("delete from semantic_platform.canonical_attributes where id = %s", (proposal["entity_id"],))
        elif proposal.get("change_type") == "update":
            payload = proposal.get("payload") or {}
            draft_snapshot = payload.get("draft_snapshot") or {}
            if draft_snapshot:
                cur.execute(
                    """
                    update semantic_platform.canonical_attributes
                    set entity_id = %(entity_id)s,
                        semantic_type_id = %(semantic_type_id)s,
                        name = %(name)s,
                        namespace = %(namespace)s,
                        description = %(description)s,
                        datatype = %(datatype)s,
                        identity_role = %(identity_role)s,
                        version = %(version)s,
                        lifecycle = %(lifecycle)s,
                        status = 'approved',
                        metadata = %(metadata)s::jsonb,
                        created_by = %(created_by)s,
                        reviewed_by = %(reviewed_by)s,
                        approved_at = %(approved_at)s::timestamptz,
                        evidence = %(evidence)s::jsonb,
                        confidence = %(confidence)s,
                        updated_at = now()
                    where id = %(id)s
                    """,
                    _sql_canonical_attribute_params({**draft_snapshot, "status": "approved"}),
                )
        else:
            cur.execute("update semantic_platform.canonical_attributes set status = 'approved', updated_at = now() where id = %s", (proposal["entity_id"],))
    if proposal["entity_type"] == "canonical_relation":
        if proposal.get("change_type") == "delete":
            cur.execute("delete from semantic_platform.canonical_relations where id = %s", (proposal["entity_id"],))
        elif proposal.get("change_type") == "update":
            payload = proposal.get("payload") or {}
            draft_snapshot = payload.get("draft_snapshot") or {}
            if draft_snapshot:
                cur.execute(
                    """
                    update semantic_platform.canonical_relations
                    set source_entity_id = %(source_entity_id)s,
                        target_entity_id = %(target_entity_id)s,
                        relation_type = %(relation_type)s,
                        forward_label = %(forward_label)s,
                        reverse_label = %(reverse_label)s,
                        version = %(version)s,
                        lifecycle = %(lifecycle)s,
                        status = 'approved',
                        metadata = %(metadata)s::jsonb,
                        created_by = %(created_by)s,
                        reviewed_by = %(reviewed_by)s,
                        approved_at = %(approved_at)s::timestamptz,
                        evidence = %(evidence)s::jsonb,
                        confidence = %(confidence)s,
                        updated_at = now()
                    where id = %(id)s
                    """,
                    _sql_canonical_relation_params({**draft_snapshot, "status": "approved"}),
                )
        else:
            cur.execute("update semantic_platform.canonical_relations set status = 'approved', updated_at = now() where id = %s", (proposal["entity_id"],))


def _apply_rejection_db(cur: Any, proposal: dict[str, Any]) -> None:
    if proposal["change_type"] != "create":
        return
    if proposal["entity_type"] == "semantic_type":
        cur.execute("delete from semantic_platform.semantic_types where id = %s", (proposal["entity_id"],))
    if proposal["entity_type"] == "semantic_relationship":
        cur.execute("delete from semantic_platform.semantic_relationships where id = %s", (proposal["entity_id"],))
    if proposal["entity_type"] == "execution_source":
        cur.execute("delete from semantic_platform.execution_sources where id = %s", (proposal["entity_id"],))
    if proposal["entity_type"] == "operation_variant":
        cur.execute("delete from semantic_platform.operation_variants where id = %s", (proposal["entity_id"],))
    if proposal["entity_type"] == "capability":
        cur.execute("delete from semantic_platform.capabilities where id = %s", (proposal["entity_id"],))
    if proposal["entity_type"] == "field_mapping":
        cur.execute("delete from semantic_platform.field_mappings where id = %s", (proposal["entity_id"],))
    if proposal["entity_type"] == "canonical_entity":
        cur.execute("delete from semantic_platform.canonical_entities where id = %s", (proposal["entity_id"],))
    if proposal["entity_type"] == "canonical_attribute":
        cur.execute("delete from semantic_platform.canonical_attributes where id = %s", (proposal["entity_id"],))
    if proposal["entity_type"] == "canonical_relation":
        cur.execute("delete from semantic_platform.canonical_relations where id = %s", (proposal["entity_id"],))


CapabilityContextRepository = SemanticLayerRepository
SemanticCatalogRepository = SemanticLayerRepository
