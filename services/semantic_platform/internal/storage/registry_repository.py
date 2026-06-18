from __future__ import annotations

from typing import Any


class RegistryRepositoryMixin:
    def list_semantic_types(self, query: str = "", status: str = "") -> list[dict[str, Any]]:
        from services.semantic_platform.internal.storage import repository as repo_mod

        if self.store_path is not None:
            return self._list_semantic_types_file_store(query=query, status=status)
        clauses: list[str] = []
        params: list[Any] = []
        if query:
            clauses.append("(name ilike %s or description ilike %s or aliases::text ilike %s)")
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
                    from semantic_platform.semantic_types
                    {where_sql}
                    order by name
                    """,
                    params,
                )
                records = [repo_mod._semantic_type_from_row(row) for row in cur.fetchall()]
                pending_updates = repo_mod._load_pending_semantic_type_update_proposals(
                    cur,
                    [item["id"] for item in records],
                )
                return [repo_mod._attach_semantic_type_overlay(item, pending_updates.get(item["id"])) for item in records]

    def get_semantic_type(self, semantic_type_id: str) -> dict[str, Any] | None:
        from services.semantic_platform.internal.storage import repository as repo_mod

        if self.store_path is not None:
            return self._get_semantic_type_file_store(semantic_type_id)
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute(
                    """
                    select *
                    from semantic_platform.semantic_types
                    where id = %s
                    """,
                    (semantic_type_id,),
                )
                record = cur.fetchone()
                if record is None:
                    return None
                cur.execute(
                    """
                    select *
                    from semantic_platform.semantic_relationships
                    where source_id = %s or target_id = %s
                    order by created_at desc
                    """,
                    (semantic_type_id, semantic_type_id),
                )
                relationships = [repo_mod._relationship_from_row(row) for row in cur.fetchall()]
                pending_update = repo_mod._load_pending_semantic_type_update_proposal(cur, semantic_type_id)
        semantic_type = repo_mod._semantic_type_from_row(record)
        semantic_type["relationships"] = relationships
        return repo_mod._attach_semantic_type_overlay(semantic_type, pending_update)

    def create_semantic_type(self, payload: dict[str, Any]) -> dict[str, Any]:
        from services.semantic_platform.internal.storage import repository as repo_mod

        repo_mod._validate_semantic_type_payload(payload, creating=True)
        if self.store_path is not None:
            return self._create_semantic_type_file_store(payload)
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                payload = repo_mod._normalize_semantic_type_payload_db(cur, payload)
                repo_mod._ensure_semantic_type_name_available(cur, str(payload["name"]))
                record = repo_mod._semantic_type_record(payload)
                proposal = repo_mod._proposal_record(
                    source_type="manual_authoring",
                    title=f"Create semantic type {record['name']}",
                    entity_type="semantic_type",
                    entity_id=record["id"],
                    change_type="create",
                    payload=record,
                )
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
                    repo_mod._sql_semantic_type_params(record),
                )
                repo_mod._insert_proposal(cur, proposal)
            conn.commit()
        return {"semantic_type": record, "proposal": proposal}

    def update_semantic_type(self, semantic_type_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        from services.semantic_platform.internal.storage import repository as repo_mod

        if self.store_path is not None:
            return self._update_semantic_type_file_store(semantic_type_id, payload)
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute(
                    "select * from semantic_platform.semantic_types where id = %s",
                    (semantic_type_id,),
                )
                row = cur.fetchone()
                if row is None:
                    raise KeyError(semantic_type_id)
                record = repo_mod._semantic_type_from_row(row)
                payload = repo_mod._normalize_semantic_type_payload_db(cur, payload, current=record)
                updates = repo_mod._semantic_type_updates(payload)
                if "name" in updates and updates["name"] != record["name"]:
                    repo_mod._ensure_semantic_type_name_available(cur, str(updates["name"]), exclude_id=semantic_type_id)
                draft_snapshot = {**record, **updates, "updated_at": repo_mod._now()}
                draft_snapshot["urn"] = f"urn:semantic-platform:semantic-type:{draft_snapshot['name']}"
                cur.execute(
                    """
                    delete from semantic_platform.proposals
                    where entity_type = 'semantic_type'
                      and entity_id = %s
                      and change_type = 'update'
                      and status = 'pending_review'
                    """,
                    (semantic_type_id,),
                )
                proposal = repo_mod._proposal_record(
                    source_type="manual_authoring",
                    title=f"Update semantic type {record['name']}",
                    entity_type="semantic_type",
                    entity_id=record["id"],
                    change_type="update",
                    payload={
                        "approved_snapshot": record,
                        "draft_snapshot": draft_snapshot,
                        "fields_changed": sorted(updates.keys()),
                    },
                )
                repo_mod._insert_proposal(cur, proposal)
            conn.commit()
        return {"semantic_type": draft_snapshot, "proposal": proposal}

    def delete_semantic_type(self, semantic_type_id: str) -> dict[str, Any]:
        from services.semantic_platform.internal.storage import repository as repo_mod

        if self.store_path is not None:
            return self._delete_semantic_type_file_store(semantic_type_id)
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute("select * from semantic_platform.semantic_types where id = %s", (semantic_type_id,))
                row = cur.fetchone()
                if row is None:
                    raise KeyError(semantic_type_id)
                record = repo_mod._semantic_type_from_row(row)
                cur.execute(
                    """
                    select id
                    from semantic_platform.semantic_relationships
                    where source_id = %s or target_id = %s
                    """,
                    (semantic_type_id, semantic_type_id),
                )
                relationship_ids = [item["id"] for item in cur.fetchall()]
                proposal = repo_mod._proposal_record(
                    source_type="manual_authoring",
                    title=f"Delete semantic type {record['name']}",
                    entity_type="semantic_type",
                    entity_id=record["id"],
                    change_type="delete",
                    payload={
                        "semantic_type": record,
                        "related_relationship_ids": relationship_ids,
                    },
                )
                repo_mod._insert_proposal(cur, proposal)
            conn.commit()
        return {"semantic_type": record, "proposal": proposal}

    def add_semantic_relationship(self, semantic_type_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        from services.semantic_platform.internal.storage import repository as repo_mod

        target_id = str(payload.get("target_id") or "").strip()
        relation_type = str(payload.get("relation_type") or "").strip()
        if not target_id or not relation_type:
            raise ValueError("target_id and relation_type are required")
        if self.store_path is not None:
            return self._add_semantic_relationship_file_store(semantic_type_id, payload)
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                source = repo_mod._load_semantic_type(cur, semantic_type_id)
                target = repo_mod._load_semantic_type(cur, target_id)
                relationship = repo_mod._relationship_record(source=source, target=target, relation_type=relation_type)
                proposal = repo_mod._proposal_record(
                    source_type="manual_authoring",
                    title=f"Relate {source['name']} to {target['name']}",
                    entity_type="semantic_relationship",
                    entity_id=relationship["id"],
                    change_type="create",
                    payload=relationship,
                )
                cur.execute(
                    """
                    insert into semantic_platform.semantic_relationships (
                      id, source_id, source_name, target_id, target_name,
                      relation_type, status, created_at, updated_at
                    ) values (
                      %(id)s, %(source_id)s, %(source_name)s, %(target_id)s, %(target_name)s,
                      %(relation_type)s, %(status)s, %(created_at)s::timestamptz, %(updated_at)s::timestamptz
                    )
                    """,
                    relationship,
                )
                repo_mod._insert_proposal(cur, proposal)
            conn.commit()
        return {"relationship": relationship, "proposal": proposal}

    def update_semantic_relationship(self, relationship_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        from services.semantic_platform.internal.storage import repository as repo_mod

        relation_type = str(payload.get("relation_type") or "").strip()
        source_id = str(payload.get("source_id") or "").strip()
        target_id = str(payload.get("target_id") or "").strip()
        if not relation_type or not source_id or not target_id:
            raise ValueError("source_id, target_id, and relation_type are required")
        if self.store_path is not None:
            return self._update_semantic_relationship_file_store(relationship_id, payload)
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute("select * from semantic_platform.semantic_relationships where id = %s", (relationship_id,))
                current = cur.fetchone()
                if current is None:
                    raise KeyError(relationship_id)
                source = repo_mod._load_semantic_type(cur, source_id)
                target = repo_mod._load_semantic_type(cur, target_id)
                relationship = repo_mod._relationship_from_row(current)
                draft_snapshot = {
                    **relationship,
                    "source_id": source["id"],
                    "source_name": source["name"],
                    "target_id": target["id"],
                    "target_name": target["name"],
                    "relation_type": relation_type,
                    "updated_at": repo_mod._now(),
                }
                cur.execute(
                    """
                    delete from semantic_platform.proposals
                    where entity_type = 'semantic_relationship'
                      and entity_id = %s
                      and change_type = 'update'
                      and status = 'pending_review'
                    """,
                    (relationship_id,),
                )
                proposal = repo_mod._proposal_record(
                    source_type="manual_authoring",
                    title=f"Update relationship {source['name']} to {target['name']}",
                    entity_type="semantic_relationship",
                    entity_id=relationship["id"],
                    change_type="update",
                    payload={
                        "approved_snapshot": relationship,
                        "draft_snapshot": draft_snapshot,
                        "fields_changed": ["relation_type", "source_id", "target_id"],
                    },
                )
                repo_mod._insert_proposal(cur, proposal)
            conn.commit()
        return {"relationship": draft_snapshot, "proposal": proposal}

    def delete_semantic_relationship(self, relationship_id: str) -> dict[str, Any]:
        from services.semantic_platform.internal.storage import repository as repo_mod

        if self.store_path is not None:
            return self._delete_semantic_relationship_file_store(relationship_id)
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute("select * from semantic_platform.semantic_relationships where id = %s", (relationship_id,))
                row = cur.fetchone()
                if row is None:
                    raise KeyError(relationship_id)
                relationship = repo_mod._relationship_from_row(row)
                proposal = repo_mod._proposal_record(
                    source_type="manual_authoring",
                    title=f"Delete relationship {relationship['source_name']} to {relationship['target_name']}",
                    entity_type="semantic_relationship",
                    entity_id=relationship["id"],
                    change_type="delete",
                    payload=dict(relationship),
                )
                repo_mod._insert_proposal(cur, proposal)
            conn.commit()
        return {"relationship": relationship, "proposal": proposal}

    def list_canonical_entities(self, query: str = "", status: str = "") -> list[dict[str, Any]]:
        from services.semantic_platform.internal.storage import repository as repo_mod

        if self.store_path is not None:
            return self._list_canonical_entities_file_store(query=query, status=status)
        clauses: list[str] = []
        params: list[Any] = []
        if query:
            like = f"%{query}%"
            clauses.append("(name ilike %s or description ilike %s)")
            params.extend([like, like])
        if status:
            clauses.append("status = %s")
            params.append(status)
        where_sql = f"where {' and '.join(clauses)}" if clauses else ""
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute(
                    f"""
                    select *
                    from semantic_platform.canonical_entities
                    {where_sql}
                    order by name
                    """,
                    params,
                )
                records = [repo_mod._canonical_entity_from_row(row) for row in cur.fetchall()]
                pending_updates = repo_mod._load_pending_canonical_entity_update_proposals(cur, [item["id"] for item in records])
                return [repo_mod._attach_canonical_entity_overlay(item, pending_updates.get(item["id"])) for item in records]

    def list_canonical_attributes(self, entity_id: str = "", status: str = "") -> list[dict[str, Any]]:
        from services.semantic_platform.internal.storage import repository as repo_mod

        if self.store_path is not None:
            return self._list_canonical_attributes_file_store(entity_id=entity_id, status=status)
        clauses: list[str] = []
        params: list[Any] = []
        if entity_id:
            clauses.append("entity_id = %s")
            params.append(entity_id)
        if status:
            clauses.append("status = %s")
            params.append(status)
        where_sql = f"where {' and '.join(clauses)}" if clauses else ""
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute(
                    f"""
                    select attr.*, ent.name as entity_name
                    from semantic_platform.canonical_attributes attr
                    join semantic_platform.canonical_entities ent on ent.id = attr.entity_id
                    {where_sql}
                    order by ent.name, attr.name
                    """,
                    params,
                )
                records = [repo_mod._canonical_attribute_from_row(row) for row in cur.fetchall()]
                pending_updates = repo_mod._load_pending_canonical_attribute_update_proposals(cur, [item["id"] for item in records])
                return [repo_mod._attach_canonical_attribute_overlay(item, pending_updates.get(item["id"])) for item in records]

    def list_canonical_relations(self, entity_id: str = "", status: str = "") -> list[dict[str, Any]]:
        from services.semantic_platform.internal.storage import repository as repo_mod

        if self.store_path is not None:
            return self._list_canonical_relations_file_store(entity_id=entity_id, status=status)
        clauses: list[str] = []
        params: list[Any] = []
        if entity_id:
            clauses.append("(rel.source_entity_id = %s or rel.target_entity_id = %s)")
            params.extend([entity_id, entity_id])
        if status:
            clauses.append("rel.status = %s")
            params.append(status)
        where_sql = f"where {' and '.join(clauses)}" if clauses else ""
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute(
                    f"""
                    select
                      rel.*,
                      src.name as source_entity_name,
                      tgt.name as target_entity_name
                    from semantic_platform.canonical_relations rel
                    join semantic_platform.canonical_entities src on src.id = rel.source_entity_id
                    join semantic_platform.canonical_entities tgt on tgt.id = rel.target_entity_id
                    {where_sql}
                    order by src.name, rel.relation_type, tgt.name
                    """,
                    params,
                )
                records = [repo_mod._canonical_relation_from_row(row) for row in cur.fetchall()]
                pending_updates = repo_mod._load_pending_canonical_relation_update_proposals(cur, [item["id"] for item in records])
                return [repo_mod._attach_canonical_relation_overlay(item, pending_updates.get(item["id"])) for item in records]

    def create_canonical_entity(self, payload: dict[str, Any]) -> dict[str, Any]:
        from services.semantic_platform.internal.storage import repository as repo_mod

        repo_mod._validate_canonical_entity_payload(payload, creating=True)
        if self.store_path is not None:
            return self._create_canonical_entity_file_store(payload)
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                repo_mod._ensure_canonical_entity_name_available(cur, str(payload["name"]))
                record = repo_mod._canonical_entity_record(payload)
                cur.execute(
                    """
                    insert into semantic_platform.canonical_entities (
                      id, semantic_type_id, name, namespace, description, version, lifecycle, status,
                      metadata, created_by, reviewed_by, approved_at, evidence, confidence, created_at, updated_at
                    ) values (
                      %(id)s, %(semantic_type_id)s, %(name)s, %(namespace)s, %(description)s, %(version)s, %(lifecycle)s, %(status)s,
                      %(metadata)s::jsonb, %(created_by)s, %(reviewed_by)s, %(approved_at)s::timestamptz, %(evidence)s::jsonb, %(confidence)s, %(created_at)s::timestamptz, %(updated_at)s::timestamptz
                    )
                    """,
                    repo_mod._sql_canonical_entity_params(record),
                )
                proposal = repo_mod._proposal_record(
                    source_type="manual_authoring",
                    title=f"Create canonical entity {record['name']}",
                    entity_type="canonical_entity",
                    entity_id=record["id"],
                    change_type="create",
                    payload=record,
                )
                repo_mod._insert_proposal(cur, proposal)
            conn.commit()
        return {"canonical_entity": record, "proposal": proposal}

    def update_canonical_entity(self, entity_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        from services.semantic_platform.internal.storage import repository as repo_mod

        repo_mod._validate_canonical_entity_payload(payload)
        if self.store_path is not None:
            return self._update_canonical_entity_file_store(entity_id, payload)
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute("select * from semantic_platform.canonical_entities where id = %s", (entity_id,))
                row = cur.fetchone()
                if row is None:
                    raise KeyError(entity_id)
                current = repo_mod._canonical_entity_from_row(row)
                updates = repo_mod._canonical_entity_updates(payload)
                if "name" in updates and updates["name"] != current["name"]:
                    repo_mod._ensure_canonical_entity_name_available(cur, updates["name"], exclude_id=entity_id)
                draft_snapshot = {**current, **updates, "updated_at": repo_mod._now()}
                cur.execute(
                    """
                    delete from semantic_platform.proposals
                    where entity_type = 'canonical_entity'
                      and entity_id = %s
                      and change_type = 'update'
                      and status = 'pending_review'
                    """,
                    (entity_id,),
                )
                proposal = repo_mod._proposal_record(
                    source_type="manual_authoring",
                    title=f"Update canonical entity {current['name']}",
                    entity_type="canonical_entity",
                    entity_id=current["id"],
                    change_type="update",
                    payload={"approved_snapshot": current, "draft_snapshot": draft_snapshot, "fields_changed": sorted(updates.keys())},
                )
                repo_mod._insert_proposal(cur, proposal)
            conn.commit()
        return {"canonical_entity": draft_snapshot, "proposal": proposal}

    def delete_canonical_entity(self, entity_id: str) -> dict[str, Any]:
        from services.semantic_platform.internal.storage import repository as repo_mod

        if self.store_path is not None:
            return self._delete_canonical_entity_file_store(entity_id)
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute("select * from semantic_platform.canonical_entities where id = %s", (entity_id,))
                row = cur.fetchone()
                if row is None:
                    raise KeyError(entity_id)
                entity = repo_mod._canonical_entity_from_row(row)
                proposal = repo_mod._proposal_record(
                    source_type="manual_authoring",
                    title=f"Delete canonical entity {entity['name']}",
                    entity_type="canonical_entity",
                    entity_id=entity["id"],
                    change_type="delete",
                    payload=dict(entity),
                )
                repo_mod._insert_proposal(cur, proposal)
            conn.commit()
        return {"canonical_entity": entity, "proposal": proposal}

    def create_canonical_attribute(self, payload: dict[str, Any]) -> dict[str, Any]:
        from services.semantic_platform.internal.storage import repository as repo_mod

        repo_mod._validate_canonical_attribute_payload(payload, creating=True)
        if self.store_path is not None:
            return self._create_canonical_attribute_file_store(payload)
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                repo_mod._ensure_canonical_entity_exists(cur, str(payload["entity_id"]))
                record = repo_mod._canonical_attribute_record(payload)
                repo_mod._ensure_canonical_attribute_name_available(cur, record["entity_id"], record["name"])
                cur.execute(
                    """
                    insert into semantic_platform.canonical_attributes (
                      id, entity_id, semantic_type_id, name, namespace, description, datatype, identity_role,
                      version, lifecycle, status, metadata, created_by, reviewed_by, approved_at,
                      evidence, confidence, created_at, updated_at
                    ) values (
                      %(id)s, %(entity_id)s, %(semantic_type_id)s, %(name)s, %(namespace)s, %(description)s, %(datatype)s, %(identity_role)s,
                      %(version)s, %(lifecycle)s, %(status)s, %(metadata)s::jsonb, %(created_by)s, %(reviewed_by)s, %(approved_at)s::timestamptz,
                      %(evidence)s::jsonb, %(confidence)s, %(created_at)s::timestamptz, %(updated_at)s::timestamptz
                    )
                    """,
                    repo_mod._sql_canonical_attribute_params(record),
                )
                proposal = repo_mod._proposal_record(
                    source_type="manual_authoring",
                    title=f"Create canonical attribute {record['name']}",
                    entity_type="canonical_attribute",
                    entity_id=record["id"],
                    change_type="create",
                    payload=record,
                )
                repo_mod._insert_proposal(cur, proposal)
            conn.commit()
        return {"canonical_attribute": record, "proposal": proposal}

    def update_canonical_attribute(self, attribute_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        from services.semantic_platform.internal.storage import repository as repo_mod

        repo_mod._validate_canonical_attribute_payload(payload)
        if self.store_path is not None:
            return self._update_canonical_attribute_file_store(attribute_id, payload)
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute("select * from semantic_platform.canonical_attributes where id = %s", (attribute_id,))
                row = cur.fetchone()
                if row is None:
                    raise KeyError(attribute_id)
                current = repo_mod._canonical_attribute_from_row(row)
                updates = repo_mod._canonical_attribute_updates(payload)
                next_entity_id = str(updates.get("entity_id") or current["entity_id"])
                next_name = str(updates.get("name") or current["name"])
                repo_mod._ensure_canonical_entity_exists(cur, next_entity_id)
                repo_mod._ensure_canonical_attribute_name_available(cur, next_entity_id, next_name, exclude_id=attribute_id)
                draft_snapshot = {**current, **updates, "updated_at": repo_mod._now()}
                cur.execute(
                    """
                    delete from semantic_platform.proposals
                    where entity_type = 'canonical_attribute'
                      and entity_id = %s
                      and change_type = 'update'
                      and status = 'pending_review'
                    """,
                    (attribute_id,),
                )
                proposal = repo_mod._proposal_record(
                    source_type="manual_authoring",
                    title=f"Update canonical attribute {current['name']}",
                    entity_type="canonical_attribute",
                    entity_id=current["id"],
                    change_type="update",
                    payload={"approved_snapshot": current, "draft_snapshot": draft_snapshot, "fields_changed": sorted(updates.keys())},
                )
                repo_mod._insert_proposal(cur, proposal)
            conn.commit()
        return {"canonical_attribute": draft_snapshot, "proposal": proposal}

    def delete_canonical_attribute(self, attribute_id: str) -> dict[str, Any]:
        from services.semantic_platform.internal.storage import repository as repo_mod

        if self.store_path is not None:
            return self._delete_canonical_attribute_file_store(attribute_id)
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute("select * from semantic_platform.canonical_attributes where id = %s", (attribute_id,))
                row = cur.fetchone()
                if row is None:
                    raise KeyError(attribute_id)
                attribute = repo_mod._canonical_attribute_from_row(row)
                proposal = repo_mod._proposal_record(
                    source_type="manual_authoring",
                    title=f"Delete canonical attribute {attribute['name']}",
                    entity_type="canonical_attribute",
                    entity_id=attribute["id"],
                    change_type="delete",
                    payload=dict(attribute),
                )
                repo_mod._insert_proposal(cur, proposal)
            conn.commit()
        return {"canonical_attribute": attribute, "proposal": proposal}

    def create_canonical_relation(self, payload: dict[str, Any]) -> dict[str, Any]:
        from services.semantic_platform.internal.storage import repository as repo_mod

        repo_mod._validate_canonical_relation_payload(payload, creating=True)
        if self.store_path is not None:
            return self._create_canonical_relation_file_store(payload)
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                repo_mod._ensure_canonical_entity_exists(cur, str(payload["source_entity_id"]))
                repo_mod._ensure_canonical_entity_exists(cur, str(payload["target_entity_id"]))
                record = repo_mod._canonical_relation_record(payload)
                proposal = repo_mod._proposal_record(
                    source_type="manual_authoring",
                    title=f"Create canonical relation {record['relation_type']}",
                    entity_type="canonical_relation",
                    entity_id=record["id"],
                    change_type="create",
                    payload=record,
                )
                cur.execute(
                    """
                    insert into semantic_platform.canonical_relations (
                      id, source_entity_id, target_entity_id, relation_type, forward_label, reverse_label,
                      version, lifecycle, status, metadata, created_by, reviewed_by, approved_at,
                      evidence, confidence, created_at, updated_at
                    ) values (
                      %(id)s, %(source_entity_id)s, %(target_entity_id)s, %(relation_type)s, %(forward_label)s, %(reverse_label)s,
                      %(version)s, %(lifecycle)s, %(status)s, %(metadata)s::jsonb, %(created_by)s, %(reviewed_by)s, %(approved_at)s::timestamptz,
                      %(evidence)s::jsonb, %(confidence)s, %(created_at)s::timestamptz, %(updated_at)s::timestamptz
                    )
                    """,
                    repo_mod._sql_canonical_relation_params(record),
                )
                repo_mod._insert_proposal(cur, proposal)
            conn.commit()
        return {"canonical_relation": record, "proposal": proposal}

    def update_canonical_relation(self, relation_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        from services.semantic_platform.internal.storage import repository as repo_mod

        repo_mod._validate_canonical_relation_payload(payload)
        if self.store_path is not None:
            return self._update_canonical_relation_file_store(relation_id, payload)
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute("select * from semantic_platform.canonical_relations where id = %s", (relation_id,))
                row = cur.fetchone()
                if row is None:
                    raise KeyError(relation_id)
                current = repo_mod._canonical_relation_from_row(row)
                updates = repo_mod._canonical_relation_updates(payload)
                if "source_entity_id" in updates:
                    repo_mod._ensure_canonical_entity_exists(cur, str(updates["source_entity_id"]))
                if "target_entity_id" in updates:
                    repo_mod._ensure_canonical_entity_exists(cur, str(updates["target_entity_id"]))
                draft_snapshot = {**current, **updates, "updated_at": repo_mod._now()}
                cur.execute(
                    """
                    delete from semantic_platform.proposals
                    where entity_type = 'canonical_relation'
                      and entity_id = %s
                      and change_type = 'update'
                      and status = 'pending_review'
                    """,
                    (relation_id,),
                )
                proposal = repo_mod._proposal_record(
                    source_type="manual_authoring",
                    title=f"Update canonical relation {current['relation_type']}",
                    entity_type="canonical_relation",
                    entity_id=current["id"],
                    change_type="update",
                    payload={"approved_snapshot": current, "draft_snapshot": draft_snapshot, "fields_changed": sorted(updates.keys())},
                )
                repo_mod._insert_proposal(cur, proposal)
            conn.commit()
        return {"canonical_relation": draft_snapshot, "proposal": proposal}

    def delete_canonical_relation(self, relation_id: str) -> dict[str, Any]:
        from services.semantic_platform.internal.storage import repository as repo_mod

        if self.store_path is not None:
            return self._delete_canonical_relation_file_store(relation_id)
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute("select * from semantic_platform.canonical_relations where id = %s", (relation_id,))
                row = cur.fetchone()
                if row is None:
                    raise KeyError(relation_id)
                relation = repo_mod._canonical_relation_from_row(row)
                proposal = repo_mod._proposal_record(
                    source_type="manual_authoring",
                    title=f"Delete canonical relation {relation['relation_type']}",
                    entity_type="canonical_relation",
                    entity_id=relation["id"],
                    change_type="delete",
                    payload=dict(relation),
                )
                repo_mod._insert_proposal(cur, proposal)
            conn.commit()
        return {"canonical_relation": relation, "proposal": proposal}
