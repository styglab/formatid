from __future__ import annotations

import json
from typing import Any
from uuid import uuid4

from services.semantic_platform.internal.onboarding.stages import (
    build_onboarding_stage_task_records,
)


class OnboardingRepositoryMixin:
    def create_onboarding_run_for_source(
        self,
        *,
        source: dict[str, Any],
        proposal: dict[str, Any] | None = None,
        upload_metadata: dict[str, Any] | None = None,
        trigger_type: str = "source_upload",
        created_by: str = "system",
    ) -> dict[str, Any]:
        from services.semantic_platform.internal.storage import repository as repo_mod

        if self.store_path is not None:
            return self._create_onboarding_run_for_source_file_store(
                source=source,
                proposal=proposal,
                upload_metadata=upload_metadata,
                trigger_type=trigger_type,
                created_by=created_by,
            )
        self.ensure_control_plane_schema()
        now = repo_mod._now()
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
            "created_by": created_by,
            "metadata": {
                "source_name": source.get("name") or "",
                "source_type": source.get("source_type") or "",
                "provider": source.get("provider") or "",
                "upload": upload_metadata or {},
            },
            "created_at": now,
            "updated_at": now,
        }
        evidence = {
            "id": f"evidence_{uuid4().hex}",
            "run_id": run["id"],
            "source_id": source["id"],
            "snapshot_type": "source_upload",
            "content_hash": str((upload_metadata or {}).get("sha256") or ""),
            "source_ref": {
                "source_id": source["id"],
                "source_name": source.get("name") or "",
                "upload": upload_metadata or {},
            },
            "operation_evidence": [],
            "schema_evidence": [],
            "sample_values": {},
            "ai_context": {
                "suggestion_mode": "deterministic_assist",
                "status": "ready_for_field_extraction",
            },
            "created_at": now,
        }
        bundle = {
            "id": f"bundle_{uuid4().hex}",
            "run_id": run["id"],
            "source_id": source["id"],
            "source_name": source.get("name") or source["id"],
            "evidence_snapshot_id": evidence["id"],
            "title": f"Onboard {source.get('name') or source['id']}",
            "status": "pending_review" if proposal else "draft",
            "summary": {
                "source_id": source["id"],
                "proposal_count": 1 if proposal else 0,
                "entity_counts": {"execution_source": 1} if proposal else {},
            },
            "created_at": now,
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
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute(
                    """
                    insert into semantic_platform.onboarding_runs (
                      id, source_id, status, stage, current_stage, stage_status, run_mode, next_action, trigger_type, created_by,
                      metadata, started_at, updated_at
                    ) values (
                      %(id)s, %(source_id)s, %(status)s, %(stage)s, %(current_stage)s, %(stage_status)s, %(run_mode)s, %(next_action)s, %(trigger_type)s, %(created_by)s,
                      %(metadata)s::jsonb, %(created_at)s::timestamptz, %(updated_at)s::timestamptz
                    )
                    """,
                    {**run, "metadata": json.dumps(run["metadata"])},
                )
                cur.execute(
                    """
                    insert into semantic_platform.evidence_snapshots (
                      id, run_id, source_id, snapshot_type, content_hash, source_ref,
                      operation_evidence, schema_evidence, sample_values, ai_context, created_at
                    ) values (
                      %(id)s, %(run_id)s, %(source_id)s, %(snapshot_type)s, %(content_hash)s, %(source_ref)s::jsonb,
                      %(operation_evidence)s::jsonb, %(schema_evidence)s::jsonb, %(sample_values)s::jsonb, %(ai_context)s::jsonb,
                      %(created_at)s::timestamptz
                    )
                    """,
                    repo_mod._sql_evidence_snapshot_params(evidence),
                )
                cur.execute(
                    """
                    insert into semantic_platform.proposal_bundles (
                      id, run_id, source_id, evidence_snapshot_id, title, status,
                      summary, created_by, created_at, updated_at
                    ) values (
                      %(id)s, %(run_id)s, %(source_id)s, %(evidence_snapshot_id)s, %(title)s, %(status)s,
                      %(summary)s::jsonb, %(created_by)s, %(created_at)s::timestamptz, %(updated_at)s::timestamptz
                    )
                    """,
                    {**bundle, "summary": json.dumps(bundle["summary"]), "created_by": created_by},
                )
                if proposal:
                    cur.execute(
                        """
                        insert into semantic_platform.proposal_bundle_items (bundle_id, proposal_id, item_order)
                        values (%s, %s, 10)
                        on conflict (bundle_id, proposal_id) do nothing
                        """,
                        (bundle["id"], proposal["id"]),
                    )
                for task in tasks:
                    cur.execute(
                        """
                        insert into semantic_platform.work_queue_tasks (
                          id, run_id, source_id, evidence_snapshot_id, operation_id, field_id,
                          stage, task_type, status, supports_ai_draft, draft_status, depends_on,
                          recommended_action, draft_payload, draft_rationale, draft_confidence,
                          priority, title, payload, proposal_id, assigned_to, created_at, updated_at
                        ) values (
                          %(id)s, %(run_id)s, %(source_id)s, %(evidence_snapshot_id)s, %(operation_id)s, %(field_id)s,
                          %(stage)s, %(task_type)s, %(status)s, %(supports_ai_draft)s, %(draft_status)s, %(depends_on)s::jsonb,
                          %(recommended_action)s, %(draft_payload)s::jsonb, %(draft_rationale)s, %(draft_confidence)s,
                          %(priority)s, %(title)s, %(payload)s::jsonb, %(proposal_id)s, %(assigned_to)s, %(created_at)s::timestamptz, %(updated_at)s::timestamptz
                        )
                        """,
                        {
                            **task,
                            "payload": json.dumps(task["payload"]),
                            "depends_on": json.dumps(task["depends_on"]),
                            "draft_payload": json.dumps(task["draft_payload"]),
                        },
                    )
            conn.commit()
        return {"onboarding_run": run, "evidence_snapshot": evidence, "proposal_bundle": bundle, "work_queue_tasks": tasks}

    def list_onboarding_runs(self) -> list[dict[str, Any]]:
        from services.semantic_platform.internal.storage import repository as repo_mod

        if self.store_path is not None:
            return sorted(self._read_store().get("onboarding_runs", []), key=lambda item: item.get("updated_at", ""), reverse=True)
        self.ensure_control_plane_schema()
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute(
                    """
                    select
                      run.*,
                      src.name as source_name,
                      evidence.id as evidence_snapshot_id,
                      count(distinct op.id) as operation_count,
                      count(distinct field.id) as field_count,
                      count(distinct mapping.id) as mapping_count,
                      count(distinct item.proposal_id) as proposal_count,
                      count(distinct item.proposal_id) filter (where proposal.status = 'pending_review') as pending_proposal_count,
                      coalesce((run.metadata->'upload'->'suggestion_generation'->>'status'), 'ready_for_field_extraction') as suggestion_status
                    from semantic_platform.onboarding_runs run
                    join semantic_platform.execution_sources src on src.id = run.source_id
                    left join semantic_platform.evidence_snapshots evidence on evidence.run_id = run.id
                    left join semantic_platform.execution_assets asset on asset.source_id = run.source_id
                    left join semantic_platform.execution_access_paths access_path on access_path.asset_id = asset.id
                    left join semantic_platform.execution_operations op on op.access_path_id = access_path.id
                    left join semantic_platform.operation_fields field on field.operation_id = op.id
                    left join semantic_platform.field_mappings mapping on mapping.source_id = run.source_id
                    left join semantic_platform.proposal_bundles bundle on bundle.run_id = run.id
                    left join semantic_platform.proposal_bundle_items item on item.bundle_id = bundle.id
                    left join semantic_platform.proposals proposal on proposal.id = item.proposal_id
                    group by run.id, src.name, evidence.id
                    order by run.updated_at desc, run.started_at desc
                    """
                )
                return [repo_mod._onboarding_run_from_row(row) for row in cur.fetchall()]

    def list_proposal_bundles(self) -> list[dict[str, Any]]:
        from services.semantic_platform.internal.storage import repository as repo_mod

        if self.store_path is not None:
            return sorted(self._read_store().get("proposal_bundles", []), key=lambda item: item.get("updated_at", ""), reverse=True)
        self.ensure_control_plane_schema()
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute(
                    """
                    select
                      bundle.*,
                      src.name as source_name,
                      count(item.proposal_id) as proposal_count,
                      count(item.proposal_id) filter (where proposal.status = 'pending_review') as pending_count,
                      count(item.proposal_id) filter (where proposal.status = 'approved') as approved_count,
                      count(item.proposal_id) filter (where proposal.status = 'rejected') as rejected_count,
                      coalesce(
                        jsonb_object_agg(proposal.entity_type, entity_counts.count_value)
                          filter (where proposal.entity_type is not null),
                        '{}'::jsonb
                      ) as entity_counts,
                      coalesce(jsonb_agg(item.proposal_id order by item.item_order) filter (where item.proposal_id is not null), '[]'::jsonb) as proposal_ids
                    from semantic_platform.proposal_bundles bundle
                    join semantic_platform.execution_sources src on src.id = bundle.source_id
                    left join semantic_platform.proposal_bundle_items item on item.bundle_id = bundle.id
                    left join semantic_platform.proposals proposal on proposal.id = item.proposal_id
                    left join lateral (
                      select p.entity_type, count(*) as count_value
                      from semantic_platform.proposal_bundle_items bi
                      join semantic_platform.proposals p on p.id = bi.proposal_id
                      where bi.bundle_id = bundle.id
                      group by p.entity_type
                    ) entity_counts on entity_counts.entity_type = proposal.entity_type
                    group by bundle.id, src.name
                    order by bundle.updated_at desc, bundle.created_at desc
                    """
                )
                return [repo_mod._proposal_bundle_from_row(row) for row in cur.fetchall()]

    def add_proposal_to_bundle(self, bundle_id: str, proposal_id: str, item_order: int = 100) -> None:
        from services.semantic_platform.internal.storage import repository as repo_mod

        if self.store_path is not None:
            with repo_mod._STORE_LOCK:
                store = self._read_store()
                items = store.setdefault("proposal_bundle_items", [])
                exists = next(
                    (
                        item
                        for item in items
                        if item.get("bundle_id") == bundle_id and item.get("proposal_id") == proposal_id
                    ),
                    None,
                )
                if exists is None:
                    items.append(
                        {
                            "bundle_id": bundle_id,
                            "proposal_id": proposal_id,
                            "item_order": item_order,
                            "created_at": repo_mod._now(),
                        }
                    )
                self._write_store(store)
            return
        self.ensure_control_plane_schema()
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute(
                    """
                    insert into semantic_platform.proposal_bundle_items (bundle_id, proposal_id, item_order)
                    values (%s, %s, %s)
                    on conflict (bundle_id, proposal_id) do update set item_order = excluded.item_order
                    """,
                    (bundle_id, proposal_id, item_order),
                )
            conn.commit()

    def update_proposal_bundle_summary(self, bundle_id: str, summary_updates: dict[str, Any], *, status: str | None = None) -> dict[str, Any] | None:
        from services.semantic_platform.internal.storage import repository as repo_mod

        if self.store_path is not None:
            with repo_mod._STORE_LOCK:
                store = self._read_store()
                for item in store.get("proposal_bundles", []):
                    if item.get("id") != bundle_id:
                        continue
                    summary = item.get("summary") if isinstance(item.get("summary"), dict) else {}
                    item["summary"] = {**summary, **summary_updates}
                    if status is not None:
                        item["status"] = status
                    item["updated_at"] = repo_mod._now()
                    self._write_store(store)
                    return item
            return None
        self.ensure_control_plane_schema()
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute("select * from semantic_platform.proposal_bundles where id = %s", (bundle_id,))
                existing = cur.fetchone()
                if existing is None:
                    return None
                summary = existing.get("summary") if isinstance(existing.get("summary"), dict) else {}
                merged = {**summary, **summary_updates}
                cur.execute(
                    """
                    update semantic_platform.proposal_bundles
                    set summary = %s::jsonb,
                        status = %s,
                        updated_at = now()
                    where id = %s
                    """,
                    (
                        json.dumps(merged),
                        status or existing.get("status") or "draft",
                        bundle_id,
                    ),
                )
            conn.commit()
        bundle = next((item for item in self.list_proposal_bundles() if item.get("id") == bundle_id), None)
        return bundle

    def list_evidence_snapshots(self, run_id: str = "", source_id: str = "") -> list[dict[str, Any]]:
        from services.semantic_platform.internal.storage import repository as repo_mod

        if self.store_path is not None:
            records = list(self._read_store().get("evidence_snapshots", []))
            if run_id:
                records = [item for item in records if item.get("run_id") == run_id]
            if source_id:
                records = [item for item in records if item.get("source_id") == source_id]
            return sorted(records, key=lambda item: item.get("created_at", ""), reverse=True)
        self.ensure_control_plane_schema()
        clauses: list[str] = []
        params: list[Any] = []
        if run_id:
            clauses.append("run_id = %s")
            params.append(run_id)
        if source_id:
            clauses.append("source_id = %s")
            params.append(source_id)
        where_sql = f"where {' and '.join(clauses)}" if clauses else ""
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute(
                    f"""
                    select *
                    from semantic_platform.evidence_snapshots
                    {where_sql}
                    order by created_at desc
                    """,
                    params,
                )
                return [repo_mod._evidence_snapshot_from_row(row) for row in cur.fetchall()]

    def list_work_queue_tasks(self, run_id: str = "", source_id: str = "") -> list[dict[str, Any]]:
        from services.semantic_platform.internal.storage import repository as repo_mod

        if self.store_path is not None:
            records = list(self._read_store().get("work_queue_tasks", []))
            if run_id:
                records = [item for item in records if item.get("run_id") == run_id]
            if source_id:
                records = [item for item in records if item.get("source_id") == source_id]
            return sorted(records, key=lambda item: (item.get("priority", 100), item.get("created_at", "")))
        self.ensure_control_plane_schema()
        clauses: list[str] = []
        params: list[Any] = []
        if run_id:
            clauses.append("task.run_id = %s")
            params.append(run_id)
        if source_id:
            clauses.append("task.source_id = %s")
            params.append(source_id)
        where_sql = f"where {' and '.join(clauses)}" if clauses else ""
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute(
                    f"""
                    select task.*, op.name as operation_name, field.raw_name as field_name, field.field_path
                    from semantic_platform.work_queue_tasks task
                    left join semantic_platform.execution_operations op on op.id = task.operation_id
                    left join semantic_platform.operation_fields field on field.id = task.field_id
                    {where_sql}
                    order by task.priority asc, task.created_at desc
                    """,
                    params,
                )
                return [repo_mod._work_queue_task_from_row(row) for row in cur.fetchall()]

    def get_onboarding_run(self, run_id: str) -> dict[str, Any] | None:
        from services.semantic_platform.internal.storage import repository as repo_mod

        if self.store_path is not None:
            return next((item for item in self._read_store().get("onboarding_runs", []) if item.get("id") == run_id), None)
        self.ensure_control_plane_schema()
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute("select * from semantic_platform.onboarding_runs where id = %s", (run_id,))
                row = cur.fetchone()
                return repo_mod._onboarding_run_from_row(row) if row else None

    def update_onboarding_run_stage(
        self,
        run_id: str,
        *,
        current_stage: str | None = None,
        stage_status: str | None = None,
        next_action: str | None = None,
        status: str | None = None,
    ) -> dict[str, Any] | None:
        from services.semantic_platform.internal.storage import repository as repo_mod

        if self.store_path is not None:
            with repo_mod._STORE_LOCK:
                store = self._read_store()
                for item in store["onboarding_runs"]:
                    if item.get("id") != run_id:
                        continue
                    if current_stage is not None:
                        item["current_stage"] = current_stage
                    if stage_status is not None:
                        item["stage_status"] = stage_status
                    if next_action is not None:
                        item["next_action"] = next_action
                    if status is not None:
                        item["status"] = status
                    item["updated_at"] = repo_mod._now()
                    self._write_store(store)
                    return item
            return None
        self.ensure_control_plane_schema()
        existing = self.get_onboarding_run(run_id)
        if existing is None:
            return None
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute(
                    """
                    update semantic_platform.onboarding_runs
                    set current_stage = %s,
                        stage_status = %s,
                        next_action = %s,
                        status = %s,
                        updated_at = now()
                    where id = %s
                    """,
                    (
                        current_stage or existing.get("current_stage") or "source_review",
                        stage_status or existing.get("stage_status") or "pending",
                        next_action or existing.get("next_action") or "",
                        status or existing.get("status") or "started",
                        run_id,
                    ),
                )
            conn.commit()
        return self.get_onboarding_run(run_id)

    def update_onboarding_run_metadata(self, run_id: str, metadata_updates: dict[str, Any]) -> dict[str, Any] | None:
        from services.semantic_platform.internal.storage import repository as repo_mod

        if not metadata_updates:
            return self.get_onboarding_run(run_id)
        if self.store_path is not None:
            with repo_mod._STORE_LOCK:
                store = self._read_store()
                for item in store["onboarding_runs"]:
                    if item.get("id") != run_id:
                        continue
                    metadata = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
                    item["metadata"] = {**metadata, **metadata_updates}
                    item["updated_at"] = repo_mod._now()
                    self._write_store(store)
                    return item
            return None
        self.ensure_control_plane_schema()
        existing = self.get_onboarding_run(run_id)
        if existing is None:
            return None
        metadata = existing.get("metadata") if isinstance(existing.get("metadata"), dict) else {}
        merged = {**metadata, **metadata_updates}
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute(
                    """
                    update semantic_platform.onboarding_runs
                    set metadata = %s::jsonb,
                        updated_at = now()
                    where id = %s
                    """,
                    (json.dumps(merged), run_id),
                )
            conn.commit()
        return self.get_onboarding_run(run_id)

    def get_work_queue_task(self, task_id: str) -> dict[str, Any] | None:
        from services.semantic_platform.internal.storage import repository as repo_mod

        if self.store_path is not None:
            return next((item for item in self._read_store().get("work_queue_tasks", []) if item.get("id") == task_id), None)
        self.ensure_control_plane_schema()
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute(
                    """
                    select task.*, op.name as operation_name, field.raw_name as field_name, field.field_path
                    from semantic_platform.work_queue_tasks task
                    left join semantic_platform.execution_operations op on op.id = task.operation_id
                    left join semantic_platform.operation_fields field on field.id = task.field_id
                    where task.id = %s
                    """,
                    (task_id,),
                )
                row = cur.fetchone()
                return repo_mod._work_queue_task_from_row(row) if row else None

    def update_work_queue_task(
        self,
        task_id: str,
        *,
        status: str | None = None,
        draft_status: str | None = None,
        draft_payload: dict[str, Any] | None = None,
        draft_rationale: str | None = None,
        draft_confidence: float | None = None,
        recommended_action: str | None = None,
        assigned_to: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        from services.semantic_platform.internal.storage import repository as repo_mod

        if self.store_path is not None:
            with repo_mod._STORE_LOCK:
                store = self._read_store()
                for item in store["work_queue_tasks"]:
                    if item.get("id") != task_id:
                        continue
                    if status is not None:
                        item["status"] = status
                    if draft_status is not None:
                        item["draft_status"] = draft_status
                    if draft_payload is not None:
                        item["draft_payload"] = draft_payload
                    if draft_rationale is not None:
                        item["draft_rationale"] = draft_rationale
                    if draft_confidence is not None:
                        item["draft_confidence"] = draft_confidence
                    if recommended_action is not None:
                        item["recommended_action"] = recommended_action
                    if assigned_to is not None:
                        item["assigned_to"] = assigned_to
                    if payload is not None:
                        item["payload"] = payload
                    item["updated_at"] = repo_mod._now()
                    self._write_store(store)
                    return item
            return None
        existing = self.get_work_queue_task(task_id)
        if existing is None:
            return None
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute(
                    """
                    update semantic_platform.work_queue_tasks
                    set status = %s,
                        draft_status = %s,
                        draft_payload = %s::jsonb,
                        draft_rationale = %s,
                        draft_confidence = %s,
                        recommended_action = %s,
                        assigned_to = %s,
                        payload = %s::jsonb,
                        updated_at = now()
                    where id = %s
                    """,
                    (
                        status or existing.get("status") or "open",
                        draft_status or existing.get("draft_status") or "not_started",
                        json.dumps(draft_payload if draft_payload is not None else (existing.get("draft_payload") or {})),
                        draft_rationale if draft_rationale is not None else existing.get("draft_rationale") or "",
                        draft_confidence if draft_confidence is not None else existing.get("draft_confidence"),
                        recommended_action if recommended_action is not None else existing.get("recommended_action") or "",
                        assigned_to if assigned_to is not None else existing.get("assigned_to"),
                        json.dumps(payload if payload is not None else (existing.get("payload") or {})),
                        task_id,
                    ),
                )
            conn.commit()
        return self.get_work_queue_task(task_id)
