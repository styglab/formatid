from __future__ import annotations

from typing import Any


class GovernanceRepositoryMixin:
    def list_proposals(self, status: str = "") -> list[dict[str, Any]]:
        from services.semantic_platform.internal.storage import repository as repo_mod

        if self.store_path is not None:
            return self._list_proposals_file_store(status=status)
        clauses: list[str] = []
        params: list[Any] = []
        if status:
            clauses.append("status = %s")
            params.append(status)
        where_sql = f"where {' and '.join(clauses)}" if clauses else ""
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute(
                    f"""
                    select *
                    from semantic_platform.proposals
                    {where_sql}
                    order by created_at desc
                    """,
                    params,
                )
                return [repo_mod._proposal_from_row(row) for row in cur.fetchall()]

    def list_capabilities(self, query: str = "", status: str = "") -> list[dict[str, Any]]:
        from services.semantic_platform.internal.storage import repository as repo_mod

        if self.store_path is not None:
            return self._list_capabilities_file_store(query=query, status=status)
        clauses: list[str] = []
        params: list[Any] = []
        if query:
            clauses.append("(name ilike %s or description ilike %s or capability_key ilike %s)")
            like = f"%{query}%"
            params.extend([like, like, like])
        if status:
            clauses.append("status = %s")
            params.append(status)
        where_sql = f"where {' and '.join(clauses)}" if clauses else ""
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute(
                    f"""
                    select *
                    from semantic_platform.capabilities
                    {where_sql}
                    order by name
                    """,
                    params,
                )
                records = [repo_mod._capability_from_row(row) for row in cur.fetchall()]
                pending_updates = repo_mod._load_pending_capability_update_proposals(cur, [item["id"] for item in records])
                return [repo_mod._attach_capability_overlay(item, pending_updates.get(item["id"])) for item in records]

    def get_capability(self, capability_id: str) -> dict[str, Any] | None:
        from services.semantic_platform.internal.storage import repository as repo_mod

        if self.store_path is not None:
            return self._get_capability_file_store(capability_id)
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute("select * from semantic_platform.capabilities where id = %s", (capability_id,))
                row = cur.fetchone()
                if row is None:
                    return None
                pending_update = repo_mod._load_pending_capability_update_proposal(cur, capability_id)
        return repo_mod._attach_capability_overlay(repo_mod._capability_from_row(row), pending_update)

    def create_capability(self, payload: dict[str, Any]) -> dict[str, Any]:
        from services.semantic_platform.internal.storage import repository as repo_mod

        repo_mod._validate_capability_payload(payload, creating=True)
        if self.store_path is not None:
            return self._create_capability_file_store(payload)
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                repo_mod._ensure_capability_key_available(cur, str(payload["capability_key"]))
                record = repo_mod._capability_record(payload)
                cur.execute(
                    """
                    insert into semantic_platform.capabilities (
                      id, capability_key, namespace, name, description,
                      version, lifecycle, status, intent_spec, input_semantic_types,
                      output_semantic_types, metadata, created_by, reviewed_by,
                      approved_at, evidence, confidence, created_at, updated_at
                    ) values (
                      %(id)s, %(capability_key)s, %(namespace)s, %(name)s, %(description)s,
                      %(version)s, %(lifecycle)s, %(status)s, %(intent_spec)s::jsonb, %(input_semantic_types)s::jsonb,
                      %(output_semantic_types)s::jsonb, %(metadata)s::jsonb, %(created_by)s, %(reviewed_by)s,
                      %(approved_at)s::timestamptz, %(evidence)s::jsonb, %(confidence)s, %(created_at)s::timestamptz, %(updated_at)s::timestamptz
                    )
                    """,
                    repo_mod._sql_capability_params(record),
                )
                proposal = repo_mod._proposal_record(
                    source_type="manual_authoring",
                    title=f"Create capability {record['name']}",
                    entity_type="capability",
                    entity_id=record["id"],
                    change_type="create",
                    payload=record,
                )
                repo_mod._insert_proposal(cur, proposal)
            conn.commit()
        return {"capability": record, "proposal": proposal}

    def update_capability(self, capability_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        from services.semantic_platform.internal.storage import repository as repo_mod

        repo_mod._validate_capability_payload(payload)
        if self.store_path is not None:
            return self._update_capability_file_store(capability_id, payload)
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute("select * from semantic_platform.capabilities where id = %s", (capability_id,))
                row = cur.fetchone()
                if row is None:
                    raise KeyError(capability_id)
                current = repo_mod._capability_from_row(row)
                updates = repo_mod._capability_updates(payload)
                if "capability_key" in updates and updates["capability_key"] != current["capability_key"]:
                    repo_mod._ensure_capability_key_available(cur, updates["capability_key"], exclude_id=capability_id)
                draft_snapshot = {**current, **updates, "updated_at": repo_mod._now()}
                proposal = repo_mod._proposal_record(
                    source_type="manual_authoring",
                    title=f"Update capability {current['name']}",
                    entity_type="capability",
                    entity_id=current["id"],
                    change_type="update",
                    payload={
                        "approved_snapshot": current,
                        "draft_snapshot": draft_snapshot,
                        "fields_changed": sorted(updates.keys()),
                    },
                )
                cur.execute(
                    """
                    delete from semantic_platform.proposals
                    where entity_type = 'capability'
                      and entity_id = %s
                      and change_type = 'update'
                      and status = 'pending_review'
                    """,
                    (capability_id,),
                )
                repo_mod._insert_proposal(cur, proposal)
            conn.commit()
        return {"capability": draft_snapshot, "proposal": proposal}

    def delete_capability(self, capability_id: str) -> dict[str, Any]:
        from services.semantic_platform.internal.storage import repository as repo_mod

        if self.store_path is not None:
            return self._delete_capability_file_store(capability_id)
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute("select * from semantic_platform.capabilities where id = %s", (capability_id,))
                row = cur.fetchone()
                if row is None:
                    raise KeyError(capability_id)
                capability = repo_mod._capability_from_row(row)
                proposal = repo_mod._proposal_record(
                    source_type="manual_authoring",
                    title=f"Delete capability {capability['name']}",
                    entity_type="capability",
                    entity_id=capability["id"],
                    change_type="delete",
                    payload=dict(capability),
                )
                repo_mod._insert_proposal(cur, proposal)
            conn.commit()
        return {"capability": capability, "proposal": proposal}

    def list_field_mappings(self, query: str = "", status: str = "", operation_id: str = "") -> list[dict[str, Any]]:
        from services.semantic_platform.internal.storage import repository as repo_mod

        if self.store_path is not None:
            return self._list_field_mappings_file_store(query=query, status=status, operation_id=operation_id)
        clauses: list[str] = []
        params: list[Any] = []
        if query:
            clauses.append("(field_path ilike %s or notes ilike %s)")
            like = f"%{query}%"
            params.extend([like, like])
        if status:
            clauses.append("status = %s")
            params.append(status)
        if operation_id:
            clauses.append("operation_id = %s")
            params.append(operation_id)
        where_sql = f"where {' and '.join(clauses)}" if clauses else ""
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute(
                    f"""
                    select *
                    from semantic_platform.field_mappings
                    {where_sql}
                    order by operation_id, field_path, semantic_type_id
                    """,
                    params,
                )
                records = [repo_mod._field_mapping_from_row(row) for row in cur.fetchall()]
                pending_updates = repo_mod._load_pending_mapping_update_proposals(cur, [item["id"] for item in records])
                return [repo_mod._attach_mapping_overlay(item, pending_updates.get(item["id"])) for item in records]

    def field_mapping_exists(
        self,
        *,
        operation_id: str,
        field_path: str,
        exclude_mapping_id: str = "",
    ) -> dict[str, Any]:
        normalized_operation_id = operation_id.strip()
        normalized_field_path = field_path.strip()
        if not normalized_operation_id or not normalized_field_path:
            return {"exists": False, "mapping_id": None}
        if self.store_path is not None:
            records = self._list_field_mappings_file_store(operation_id=normalized_operation_id)
            for item in records:
                if exclude_mapping_id and item.get("id") == exclude_mapping_id:
                    continue
                if item.get("field_path", "").strip() == normalized_field_path:
                    return {"exists": True, "mapping_id": item.get("id")}
            return {"exists": False, "mapping_id": None}
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                clauses = ["operation_id = %s", "field_path = %s"]
                params: list[Any] = [normalized_operation_id, normalized_field_path]
                if exclude_mapping_id.strip():
                    clauses.append("id <> %s")
                    params.append(exclude_mapping_id.strip())
                cur.execute(
                    f"""
                    select id
                    from semantic_platform.field_mappings
                    where {' and '.join(clauses)}
                    limit 1
                    """,
                    params,
                )
                row = cur.fetchone()
                return {"exists": row is not None, "mapping_id": row["id"] if row else None}

    def get_field_mapping(self, mapping_id: str) -> dict[str, Any] | None:
        from services.semantic_platform.internal.storage import repository as repo_mod

        if self.store_path is not None:
            return self._get_field_mapping_file_store(mapping_id)
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute("select * from semantic_platform.field_mappings where id = %s", (mapping_id,))
                row = cur.fetchone()
                if row is None:
                    return None
                pending_update = repo_mod._load_pending_mapping_update_proposal(cur, mapping_id)
        return repo_mod._attach_mapping_overlay(repo_mod._field_mapping_from_row(row), pending_update)

    def create_field_mapping(self, payload: dict[str, Any]) -> dict[str, Any]:
        from services.semantic_platform.internal.storage import repository as repo_mod

        repo_mod._validate_field_mapping_payload(payload, creating=True)
        if self.store_path is not None:
            return self._create_field_mapping_file_store(payload)
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                record = repo_mod._field_mapping_record(payload)
                repo_mod._populate_mapping_field_id(cur, record)
                repo_mod._ensure_mapping_context_available(cur, record)
                cur.execute(
                    """
                    insert into semantic_platform.field_mappings (
                      id, field_id, source_id, operation_id, variant_id, access_path_id,
                      field_path, semantic_type_id, canonical_attribute_id, mapping_kind,
                      mapping_type, version, lifecycle, status, namespace,
                      transform_spec, enum_mapping, notes, created_by, reviewed_by,
                      approved_at, evidence, confidence, created_at, updated_at
                    ) values (
                      %(id)s, %(field_id)s, %(source_id)s, %(operation_id)s, %(variant_id)s, %(access_path_id)s,
                      %(field_path)s, %(semantic_type_id)s, %(canonical_attribute_id)s, %(mapping_kind)s,
                      %(mapping_type)s, %(version)s, %(lifecycle)s, %(status)s, %(namespace)s,
                      %(transform_spec)s::jsonb, %(enum_mapping)s::jsonb, %(notes)s, %(created_by)s, %(reviewed_by)s,
                      %(approved_at)s::timestamptz, %(evidence)s::jsonb, %(confidence)s, %(created_at)s::timestamptz, %(updated_at)s::timestamptz
                    )
                    """,
                    repo_mod._sql_field_mapping_params(record),
                )
                proposal = repo_mod._proposal_record(
                    source_type="manual_authoring",
                    title=f"Create mapping {record['operation_id']} {record['field_path']}",
                    entity_type="field_mapping",
                    entity_id=record["id"],
                    change_type="create",
                    payload=repo_mod._proposal_payload_with_context(record, payload.get("proposal_context")),
                )
                repo_mod._insert_proposal(cur, proposal)
            conn.commit()
        return {"field_mapping": record, "proposal": proposal}

    def update_field_mapping(self, mapping_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        from services.semantic_platform.internal.storage import repository as repo_mod

        repo_mod._validate_field_mapping_payload(payload)
        if self.store_path is not None:
            return self._update_field_mapping_file_store(mapping_id, payload)
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute("select * from semantic_platform.field_mappings where id = %s", (mapping_id,))
                row = cur.fetchone()
                if row is None:
                    raise KeyError(mapping_id)
                current = repo_mod._field_mapping_from_row(row)
                updates = repo_mod._field_mapping_updates(payload)
                draft_snapshot = {**current, **updates, "updated_at": repo_mod._now()}
                repo_mod._ensure_mapping_context_available(cur, draft_snapshot, exclude_id=mapping_id)
                proposal = repo_mod._proposal_record(
                    source_type="manual_authoring",
                    title=f"Update mapping {current['operation_id']} {current['field_path']}",
                    entity_type="field_mapping",
                    entity_id=current["id"],
                    change_type="update",
                    payload={
                        "approved_snapshot": current,
                        "draft_snapshot": draft_snapshot,
                        "fields_changed": sorted(updates.keys()),
                    },
                )
                cur.execute(
                    """
                    delete from semantic_platform.proposals
                    where entity_type = 'field_mapping'
                      and entity_id = %s
                      and change_type = 'update'
                      and status = 'pending_review'
                    """,
                    (mapping_id,),
                )
                repo_mod._insert_proposal(cur, proposal)
            conn.commit()
        return {"field_mapping": draft_snapshot, "proposal": proposal}

    def delete_field_mapping(self, mapping_id: str) -> dict[str, Any]:
        from services.semantic_platform.internal.storage import repository as repo_mod

        if self.store_path is not None:
            return self._delete_field_mapping_file_store(mapping_id)
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute("select * from semantic_platform.field_mappings where id = %s", (mapping_id,))
                row = cur.fetchone()
                if row is None:
                    raise KeyError(mapping_id)
                mapping = repo_mod._field_mapping_from_row(row)
                proposal = repo_mod._proposal_record(
                    source_type="manual_authoring",
                    title=f"Delete mapping {mapping['operation_id']} {mapping['field_path']}",
                    entity_type="field_mapping",
                    entity_id=mapping["id"],
                    change_type="delete",
                    payload=dict(mapping),
                )
                repo_mod._insert_proposal(cur, proposal)
            conn.commit()
        return {"field_mapping": mapping, "proposal": proposal}

    def list_relationships(
        self,
        *,
        semantic_type_id: str = "",
        status: str = "",
    ) -> list[dict[str, Any]]:
        from services.semantic_platform.internal.storage import repository as repo_mod

        if self.store_path is not None:
            return self._list_relationships_file_store(semantic_type_id=semantic_type_id, status=status)
        clauses: list[str] = []
        params: list[Any] = []
        if semantic_type_id:
            clauses.append("(source_id = %s or target_id = %s)")
            params.extend([semantic_type_id, semantic_type_id])
        if status:
            clauses.append("status = %s")
            params.append(status)
        where_sql = f"where {' and '.join(clauses)}" if clauses else ""
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute(
                    f"""
                    select *
                    from semantic_platform.semantic_relationships
                    {where_sql}
                    order by created_at desc
                    """,
                    params,
                )
                records = [repo_mod._relationship_from_row(row) for row in cur.fetchall()]
                pending_updates = repo_mod._load_pending_relationship_proposals(cur, [item["id"] for item in records])
                return [repo_mod._attach_relationship_overlay(item, pending_updates.get(item["id"])) for item in records]

    def semantic_catalog(self) -> dict[str, Any]:
        if self.store_path is not None:
            return self._semantic_catalog_file_store()
        semantic_types = self.list_semantic_types(status="approved")
        relationships = self.list_relationships(status="approved")
        proposals = self.list_proposals()
        return {
            "core": {
                "semantic_types": semantic_types,
                "relationships": relationships,
            },
            "governance": {
                "pending_proposals": [item for item in proposals if item.get("status") == "pending_review"][:10],
            },
            "capabilities": {},
            "mappings": {},
            "status": "available",
        }

    def review_proposal(self, proposal_id: str, decision: str, reviewer: str = "admin") -> dict[str, Any]:
        from services.semantic_platform.internal.storage import repository as repo_mod

        if decision not in {"approved", "rejected"}:
            raise ValueError("decision must be approved or rejected")
        if self.store_path is not None:
            return self._review_proposal_file_store(proposal_id, decision, reviewer=reviewer)
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute("select * from semantic_platform.proposals where id = %s", (proposal_id,))
                row = cur.fetchone()
                if row is None:
                    raise KeyError(proposal_id)
                reviewed_at = repo_mod._now()
                cur.execute(
                    """
                    update semantic_platform.proposals
                    set status = %s,
                        reviewed_by = %s,
                        reviewed_at = %s::timestamptz
                    where id = %s
                    """,
                    (decision, reviewer, reviewed_at, proposal_id),
                )
                proposal = repo_mod._proposal_from_row(
                    {**dict(row), "status": decision, "reviewed_by": reviewer, "reviewed_at": reviewed_at}
                )
                if decision == "approved":
                    repo_mod._apply_approval_db(cur, proposal)
            conn.commit()
        return proposal
