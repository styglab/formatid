from __future__ import annotations

import json
from typing import Any
from uuid import uuid4


class ExecutionRepositoryMixin:
    def list_execution_sources(self, query: str = "", status: str = "") -> list[dict[str, Any]]:
        from services.semantic_platform.internal.storage import repository as repo_mod

        if self.store_path is not None:
            return self._list_execution_sources_file_store(query=query, status=status)
        clauses: list[str] = []
        params: list[Any] = []
        if query:
            clauses.append("(name ilike %s or provider ilike %s or description ilike %s)")
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
                    from semantic_platform.execution_sources
                    {where_sql}
                    order by name
                    """,
                    params,
                )
                records = [repo_mod._execution_source_from_row(row) for row in cur.fetchall()]
                pending_updates = repo_mod._load_pending_execution_source_update_proposals(
                    cur,
                    [item["id"] for item in records],
                )
                return [repo_mod._attach_execution_source_overlay(item, pending_updates.get(item["id"])) for item in records]

    def get_execution_source(self, source_id: str) -> dict[str, Any] | None:
        from services.semantic_platform.internal.storage import repository as repo_mod

        if self.store_path is not None:
            return self._get_execution_source_file_store(source_id)
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute(
                    """
                    select *
                    from semantic_platform.execution_sources
                    where id = %s
                    """,
                    (source_id,),
                )
                row = cur.fetchone()
                if row is None:
                    return None
                pending_update = repo_mod._load_pending_execution_source_update_proposal(cur, source_id)
        return repo_mod._attach_execution_source_overlay(repo_mod._execution_source_from_row(row), pending_update)

    def list_execution_assets(
        self,
        query: str = "",
        status: str = "",
        source_id: str = "",
    ) -> list[dict[str, Any]]:
        from services.semantic_platform.internal.storage import repository as repo_mod

        if self.store_path is not None:
            return self._list_execution_assets_file_store(query=query, status=status, source_id=source_id)
        clauses: list[str] = []
        params: list[Any] = []
        if query:
            clauses.append(
                "(asset.name ilike %s or asset.locator ilike %s or asset.description ilike %s or src.name ilike %s)"
            )
            like = f"%{query}%"
            params.extend([like, like, like, like])
        if status:
            clauses.append("asset.status = %s")
            params.append(status)
        if source_id:
            clauses.append("asset.source_id = %s")
            params.append(source_id)
        where_sql = f"where {' and '.join(clauses)}" if clauses else ""
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute(
                    f"""
                    select
                      asset.*,
                      src.name as source_name,
                      src.source_type as source_source_type
                    from semantic_platform.execution_assets asset
                    join semantic_platform.execution_sources src on src.id = asset.source_id
                    {where_sql}
                    order by src.name, asset.name
                    """,
                    params,
                )
                return [repo_mod._execution_asset_from_row(row) for row in cur.fetchall()]

    def get_execution_asset(self, asset_id: str) -> dict[str, Any] | None:
        from services.semantic_platform.internal.storage import repository as repo_mod

        if self.store_path is not None:
            return self._get_execution_asset_file_store(asset_id)
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute(
                    """
                    select
                      asset.*,
                      src.name as source_name,
                      src.source_type as source_source_type
                    from semantic_platform.execution_assets asset
                    join semantic_platform.execution_sources src on src.id = asset.source_id
                    where asset.id = %s
                    """,
                    (asset_id,),
                )
                row = cur.fetchone()
                if row is None:
                    return None
        return repo_mod._execution_asset_from_row(row)

    def save_execution_asset(self, payload: dict[str, Any]) -> dict[str, Any]:
        from services.semantic_platform.internal.storage import repository as repo_mod

        source_id = str(payload.get("source_id") or "")
        name = str(payload.get("name") or "").strip()
        if not source_id or not name:
            raise ValueError("source_id and name are required")
        record = {
            "id": str(payload.get("id") or f"asset_{uuid4().hex}"),
            "source_id": source_id,
            "name": name,
            "asset_type": str(payload.get("asset_type") or "other"),
            "locator": str(payload.get("locator") or ""),
            "description": str(payload.get("description") or ""),
            "version": str(payload.get("version") or "1.0.0"),
            "lifecycle": str(payload.get("lifecycle") or "draft"),
            "status": str(payload.get("status") or "draft"),
            "metadata": payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {},
            "created_by": str(payload.get("created_by") or "semantic-platform-worker"),
            "reviewed_by": payload.get("reviewed_by"),
            "approved_at": payload.get("approved_at"),
            "evidence": payload.get("evidence") if isinstance(payload.get("evidence"), list) else [],
            "confidence": payload.get("confidence"),
            "created_at": repo_mod._now(),
            "updated_at": repo_mod._now(),
        }
        if self.store_path is not None:
            with repo_mod._STORE_LOCK:
                store = self._read_store()
                existing = next(
                    (item for item in store["execution_assets"] if item.get("source_id") == source_id and item.get("name") == name),
                    None,
                )
                if existing is not None:
                    existing.update({**record, "id": existing.get("id") or record["id"], "created_at": existing.get("created_at") or record["created_at"]})
                    self._write_store(store)
                    return existing
                store["execution_assets"].append(record)
                self._write_store(store)
                return record
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute(
                    "select * from semantic_platform.execution_assets where source_id = %s and name = %s",
                    (source_id, name),
                )
                existing = cur.fetchone()
                if existing is None:
                    cur.execute(
                        """
                        insert into semantic_platform.execution_assets (
                          id, source_id, name, asset_type, locator, description, version, lifecycle, status,
                          metadata, created_by, reviewed_by, approved_at, evidence, confidence, created_at, updated_at
                        ) values (
                          %(id)s, %(source_id)s, %(name)s, %(asset_type)s, %(locator)s, %(description)s, %(version)s, %(lifecycle)s, %(status)s,
                          %(metadata)s::jsonb, %(created_by)s, %(reviewed_by)s, %(approved_at)s::timestamptz, %(evidence)s::jsonb, %(confidence)s,
                          %(created_at)s::timestamptz, %(updated_at)s::timestamptz
                        )
                        """,
                        {**record, "metadata": json.dumps(record["metadata"]), "evidence": json.dumps(record["evidence"])},
                    )
                else:
                    record["id"] = existing["id"]
                    record["created_at"] = repo_mod._isoformat(existing.get("created_at"))
                    cur.execute(
                        """
                        update semantic_platform.execution_assets
                        set asset_type = %(asset_type)s,
                            locator = %(locator)s,
                            description = %(description)s,
                            version = %(version)s,
                            lifecycle = %(lifecycle)s,
                            status = %(status)s,
                            metadata = %(metadata)s::jsonb,
                            created_by = %(created_by)s,
                            reviewed_by = %(reviewed_by)s,
                            approved_at = %(approved_at)s::timestamptz,
                            evidence = %(evidence)s::jsonb,
                            confidence = %(confidence)s,
                            updated_at = now()
                        where id = %(id)s
                        """,
                        {**record, "metadata": json.dumps(record["metadata"]), "evidence": json.dumps(record["evidence"])},
                    )
                conn.commit()
        saved = self.list_execution_assets(source_id=source_id)
        return next(item for item in saved if item.get("name") == name)

    def save_execution_access_path(self, payload: dict[str, Any]) -> dict[str, Any]:
        from services.semantic_platform.internal.storage import repository as repo_mod

        asset_id = str(payload.get("asset_id") or "")
        name = str(payload.get("name") or "").strip()
        if not asset_id or not name:
            raise ValueError("asset_id and name are required")
        record = {
            "id": str(payload.get("id") or f"ap_{uuid4().hex}"),
            "asset_id": asset_id,
            "name": name,
            "access_type": str(payload.get("access_type") or "other"),
            "locator": str(payload.get("locator") or ""),
            "http_method": str(payload.get("http_method") or ""),
            "description": str(payload.get("description") or ""),
            "version": str(payload.get("version") or "1.0.0"),
            "lifecycle": str(payload.get("lifecycle") or "draft"),
            "status": str(payload.get("status") or "draft"),
            "request_shape": payload.get("request_shape") if isinstance(payload.get("request_shape"), dict) else {},
            "response_shape": payload.get("response_shape") if isinstance(payload.get("response_shape"), dict) else {},
            "execution_hints": payload.get("execution_hints") if isinstance(payload.get("execution_hints"), dict) else {},
            "created_by": str(payload.get("created_by") or "semantic-platform-worker"),
            "reviewed_by": payload.get("reviewed_by"),
            "approved_at": payload.get("approved_at"),
            "evidence": payload.get("evidence") if isinstance(payload.get("evidence"), list) else [],
            "confidence": payload.get("confidence"),
        }
        if self.store_path is not None:
            with repo_mod._STORE_LOCK:
                store = self._read_store()
                existing = next(
                    (item for item in store["execution_access_paths"] if item.get("asset_id") == asset_id and item.get("name") == name),
                    None,
                )
                if existing is not None:
                    existing.update({**record, "id": existing.get("id") or record["id"]})
                    self._write_store(store)
                    return existing
                store["execution_access_paths"].append(record)
                self._write_store(store)
                return record
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute(
                    "select * from semantic_platform.execution_access_paths where asset_id = %s and name = %s",
                    (asset_id, name),
                )
                existing = cur.fetchone()
                if existing is None:
                    cur.execute(
                        """
                        insert into semantic_platform.execution_access_paths (
                          id, asset_id, name, access_type, locator, http_method, description, version, lifecycle, status,
                          request_shape, response_shape, execution_hints, created_by, reviewed_by, approved_at, evidence, confidence, created_at, updated_at
                        ) values (
                          %(id)s, %(asset_id)s, %(name)s, %(access_type)s, %(locator)s, %(http_method)s, %(description)s, %(version)s, %(lifecycle)s, %(status)s,
                          %(request_shape)s::jsonb, %(response_shape)s::jsonb, %(execution_hints)s::jsonb, %(created_by)s, %(reviewed_by)s, %(approved_at)s::timestamptz,
                          %(evidence)s::jsonb, %(confidence)s, now(), now()
                        )
                        """,
                        {
                            **record,
                            "request_shape": json.dumps(record["request_shape"]),
                            "response_shape": json.dumps(record["response_shape"]),
                            "execution_hints": json.dumps(record["execution_hints"]),
                            "evidence": json.dumps(record["evidence"]),
                        },
                    )
                else:
                    record["id"] = existing["id"]
                    cur.execute(
                        """
                        update semantic_platform.execution_access_paths
                        set access_type = %(access_type)s,
                            locator = %(locator)s,
                            http_method = %(http_method)s,
                            description = %(description)s,
                            version = %(version)s,
                            lifecycle = %(lifecycle)s,
                            status = %(status)s,
                            request_shape = %(request_shape)s::jsonb,
                            response_shape = %(response_shape)s::jsonb,
                            execution_hints = %(execution_hints)s::jsonb,
                            created_by = %(created_by)s,
                            reviewed_by = %(reviewed_by)s,
                            approved_at = %(approved_at)s::timestamptz,
                            evidence = %(evidence)s::jsonb,
                            confidence = %(confidence)s,
                            updated_at = now()
                        where id = %(id)s
                        """,
                        {
                            **record,
                            "request_shape": json.dumps(record["request_shape"]),
                            "response_shape": json.dumps(record["response_shape"]),
                            "execution_hints": json.dumps(record["execution_hints"]),
                            "evidence": json.dumps(record["evidence"]),
                        },
                    )
                conn.commit()
        return {
            "id": record["id"],
            "asset_id": asset_id,
            "name": name,
            "access_type": record["access_type"],
            "locator": record["locator"],
            "http_method": record["http_method"],
            "description": record["description"],
            "status": record["status"],
            "lifecycle": record["lifecycle"],
            "request_shape": record["request_shape"],
            "response_shape": record["response_shape"],
            "execution_hints": record["execution_hints"],
            "evidence": record["evidence"],
            "confidence": record["confidence"],
        }

    def save_execution_operation(self, payload: dict[str, Any]) -> dict[str, Any]:
        from services.semantic_platform.internal.storage import repository as repo_mod

        access_path_id = str(payload.get("access_path_id") or "")
        operation_key = str(payload.get("operation_key") or "").strip()
        name = str(payload.get("name") or operation_key).strip()
        if not access_path_id or not operation_key:
            raise ValueError("access_path_id and operation_key are required")
        record = {
            "id": str(payload.get("id") or f"op_{uuid4().hex}"),
            "access_path_id": access_path_id,
            "operation_key": operation_key,
            "name": name,
            "description": str(payload.get("description") or ""),
            "namespace": str(payload.get("namespace") or "public"),
            "version": str(payload.get("version") or "1.0.0"),
            "lifecycle": str(payload.get("lifecycle") or "draft"),
            "status": str(payload.get("status") or "draft"),
            "input_spec": payload.get("input_spec") if isinstance(payload.get("input_spec"), list) else [],
            "output_spec": payload.get("output_spec") if isinstance(payload.get("output_spec"), list) else [],
            "auth_spec": payload.get("auth_spec") if isinstance(payload.get("auth_spec"), dict) else {},
            "contract_spec": payload.get("contract_spec") if isinstance(payload.get("contract_spec"), dict) else {},
            "metadata": payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {},
            "created_by": str(payload.get("created_by") or "semantic-platform-worker"),
            "reviewed_by": payload.get("reviewed_by"),
            "approved_at": payload.get("approved_at"),
            "evidence": payload.get("evidence") if isinstance(payload.get("evidence"), list) else [],
            "confidence": payload.get("confidence"),
        }
        if self.store_path is not None:
            with repo_mod._STORE_LOCK:
                store = self._read_store()
                existing = next(
                    (item for item in store["execution_operations"] if item.get("operation_key") == operation_key),
                    None,
                )
                if existing is not None:
                    existing.update({**record, "id": existing.get("id") or record["id"]})
                    self._write_store(store)
                    return existing
                store["execution_operations"].append(record)
                self._write_store(store)
                return record
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute(
                    "select * from semantic_platform.execution_operations where operation_key = %s",
                    (operation_key,),
                )
                existing = cur.fetchone()
                if existing is None:
                    cur.execute(
                        """
                        insert into semantic_platform.execution_operations (
                          id, access_path_id, operation_key, name, description, namespace, version, lifecycle, status,
                          input_spec, output_spec, auth_spec, contract_spec, metadata, created_by, reviewed_by, approved_at, evidence, confidence, created_at, updated_at
                        ) values (
                          %(id)s, %(access_path_id)s, %(operation_key)s, %(name)s, %(description)s, %(namespace)s, %(version)s, %(lifecycle)s, %(status)s,
                          %(input_spec)s::jsonb, %(output_spec)s::jsonb, %(auth_spec)s::jsonb, %(contract_spec)s::jsonb, %(metadata)s::jsonb,
                          %(created_by)s, %(reviewed_by)s, %(approved_at)s::timestamptz, %(evidence)s::jsonb, %(confidence)s, now(), now()
                        )
                        """,
                        {
                            **record,
                            "input_spec": json.dumps(record["input_spec"]),
                            "output_spec": json.dumps(record["output_spec"]),
                            "auth_spec": json.dumps(record["auth_spec"]),
                            "contract_spec": json.dumps(record["contract_spec"]),
                            "metadata": json.dumps(record["metadata"]),
                            "evidence": json.dumps(record["evidence"]),
                        },
                    )
                else:
                    record["id"] = existing["id"]
                    cur.execute(
                        """
                        update semantic_platform.execution_operations
                        set access_path_id = %(access_path_id)s,
                            name = %(name)s,
                            description = %(description)s,
                            namespace = %(namespace)s,
                            version = %(version)s,
                            lifecycle = %(lifecycle)s,
                            status = %(status)s,
                            input_spec = %(input_spec)s::jsonb,
                            output_spec = %(output_spec)s::jsonb,
                            auth_spec = %(auth_spec)s::jsonb,
                            contract_spec = %(contract_spec)s::jsonb,
                            metadata = %(metadata)s::jsonb,
                            created_by = %(created_by)s,
                            reviewed_by = %(reviewed_by)s,
                            approved_at = %(approved_at)s::timestamptz,
                            evidence = %(evidence)s::jsonb,
                            confidence = %(confidence)s,
                            updated_at = now()
                        where id = %(id)s
                        """,
                        {
                            **record,
                            "input_spec": json.dumps(record["input_spec"]),
                            "output_spec": json.dumps(record["output_spec"]),
                            "auth_spec": json.dumps(record["auth_spec"]),
                            "contract_spec": json.dumps(record["contract_spec"]),
                            "metadata": json.dumps(record["metadata"]),
                            "evidence": json.dumps(record["evidence"]),
                        },
                    )
                conn.commit()
        saved = self.get_execution_operation(record["id"])
        return saved or record

    def save_operation_field(self, payload: dict[str, Any]) -> dict[str, Any]:
        from services.semantic_platform.internal.storage import repository as repo_mod

        operation_id = str(payload.get("operation_id") or "")
        scope = str(payload.get("scope") or "input")
        raw_name = str(payload.get("raw_name") or "").strip()
        if not operation_id or not raw_name:
            raise ValueError("operation_id and raw_name are required")
        record = {
            "id": str(payload.get("id") or f"field_{uuid4().hex}"),
            "operation_id": operation_id,
            "variant_id": payload.get("variant_id"),
            "scope": scope,
            "raw_name": raw_name,
            "display_name": str(payload.get("display_name") or raw_name),
            "field_path": str(payload.get("field_path") or raw_name),
            "data_type": str(payload.get("data_type") or "string"),
            "is_required": bool(payload.get("is_required")),
            "description": str(payload.get("description") or ""),
            "version": str(payload.get("version") or "1.0.0"),
            "lifecycle": str(payload.get("lifecycle") or "draft"),
            "metadata": payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {},
            "created_by": str(payload.get("created_by") or "semantic-platform-worker"),
            "reviewed_by": payload.get("reviewed_by"),
            "approved_at": payload.get("approved_at"),
            "evidence": payload.get("evidence") if isinstance(payload.get("evidence"), list) else [],
            "confidence": payload.get("confidence"),
        }
        if self.store_path is not None:
            with repo_mod._STORE_LOCK:
                store = self._read_store()
                existing = next(
                    (
                        item
                        for item in store["operation_fields"]
                        if item.get("operation_id") == operation_id
                        and item.get("scope") == scope
                        and item.get("raw_name") == raw_name
                    ),
                    None,
                )
                if existing is not None:
                    existing.update({**record, "id": existing.get("id") or record["id"]})
                    self._write_store(store)
                    return existing
                store["operation_fields"].append(record)
                self._write_store(store)
                return record
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute(
                    """
                    select * from semantic_platform.operation_fields
                    where operation_id = %s and scope = %s and raw_name = %s
                    """,
                    (operation_id, scope, raw_name),
                )
                existing = cur.fetchone()
                if existing is None:
                    cur.execute(
                        """
                        insert into semantic_platform.operation_fields (
                          id, operation_id, variant_id, scope, raw_name, display_name, field_path, data_type, is_required,
                          description, version, lifecycle, metadata, created_by, reviewed_by, approved_at, evidence, confidence, created_at, updated_at
                        ) values (
                          %(id)s, %(operation_id)s, %(variant_id)s, %(scope)s, %(raw_name)s, %(display_name)s, %(field_path)s, %(data_type)s, %(is_required)s,
                          %(description)s, %(version)s, %(lifecycle)s, %(metadata)s::jsonb, %(created_by)s, %(reviewed_by)s, %(approved_at)s::timestamptz,
                          %(evidence)s::jsonb, %(confidence)s, now(), now()
                        )
                        """,
                        {**record, "metadata": json.dumps(record["metadata"]), "evidence": json.dumps(record["evidence"])},
                    )
                else:
                    record["id"] = existing["id"]
                    cur.execute(
                        """
                        update semantic_platform.operation_fields
                        set variant_id = %(variant_id)s,
                            display_name = %(display_name)s,
                            field_path = %(field_path)s,
                            data_type = %(data_type)s,
                            is_required = %(is_required)s,
                            description = %(description)s,
                            version = %(version)s,
                            lifecycle = %(lifecycle)s,
                            metadata = %(metadata)s::jsonb,
                            created_by = %(created_by)s,
                            reviewed_by = %(reviewed_by)s,
                            approved_at = %(approved_at)s::timestamptz,
                            evidence = %(evidence)s::jsonb,
                            confidence = %(confidence)s,
                            updated_at = now()
                        where id = %(id)s
                        """,
                        {**record, "metadata": json.dumps(record["metadata"]), "evidence": json.dumps(record["evidence"])},
                    )
                conn.commit()
        saved = self.list_operation_fields(operation_id=operation_id)
        return next(item for item in saved if item.get("scope") == scope and item.get("raw_name") == raw_name)

    def list_execution_operations(
        self,
        query: str = "",
        status: str = "",
        source_id: str = "",
        asset_id: str = "",
    ) -> list[dict[str, Any]]:
        from services.semantic_platform.internal.storage import repository as repo_mod

        if self.store_path is not None:
            return self._list_execution_operations_file_store(
                query=query,
                status=status,
                source_id=source_id,
                asset_id=asset_id,
            )
        clauses: list[str] = []
        params: list[Any] = []
        if query:
            clauses.append(
                "(op.name ilike %s or op.operation_key ilike %s or op.description ilike %s or ap.name ilike %s or asset.name ilike %s)"
            )
            like = f"%{query}%"
            params.extend([like, like, like, like, like])
        if status:
            clauses.append("op.status = %s")
            params.append(status)
        if source_id:
            clauses.append("asset.source_id = %s")
            params.append(source_id)
        if asset_id:
            clauses.append("ap.asset_id = %s")
            params.append(asset_id)
        where_sql = f"where {' and '.join(clauses)}" if clauses else ""
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute(
                    f"""
                    select
                      op.*,
                      ap.asset_id,
                      ap.name as access_path_name,
                      ap.access_type,
                      ap.locator as access_path_locator,
                      ap.http_method,
                      asset.source_id,
                      asset.name as asset_name,
                      asset.asset_type,
                      src.name as source_name,
                      src.source_type as source_source_type
                    from semantic_platform.execution_operations op
                    join semantic_platform.execution_access_paths ap on ap.id = op.access_path_id
                    join semantic_platform.execution_assets asset on asset.id = ap.asset_id
                    join semantic_platform.execution_sources src on src.id = asset.source_id
                    {where_sql}
                    order by src.name, asset.name, op.name
                    """,
                    params,
                )
                return [repo_mod._execution_operation_from_row(row) for row in cur.fetchall()]

    def get_execution_operation(self, operation_id: str) -> dict[str, Any] | None:
        from services.semantic_platform.internal.storage import repository as repo_mod

        if self.store_path is not None:
            return self._get_execution_operation_file_store(operation_id)
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute(
                    """
                    select
                      op.*,
                      ap.asset_id,
                      ap.name as access_path_name,
                      ap.access_type,
                      ap.locator as access_path_locator,
                      ap.http_method,
                      asset.source_id,
                      asset.name as asset_name,
                      asset.asset_type,
                      src.name as source_name,
                      src.source_type as source_source_type
                    from semantic_platform.execution_operations op
                    join semantic_platform.execution_access_paths ap on ap.id = op.access_path_id
                    join semantic_platform.execution_assets asset on asset.id = ap.asset_id
                    join semantic_platform.execution_sources src on src.id = asset.source_id
                    where op.id = %s
                    """,
                    (operation_id,),
                )
                row = cur.fetchone()
                if row is None:
                    return None
        return repo_mod._execution_operation_from_row(row)

    def list_operation_fields(self, operation_id: str = "", variant_id: str = "") -> list[dict[str, Any]]:
        from services.semantic_platform.internal.storage import repository as repo_mod

        if self.store_path is not None:
            return self._list_operation_fields_file_store(operation_id=operation_id, variant_id=variant_id)
        clauses: list[str] = []
        params: list[Any] = []
        if operation_id:
            clauses.append("operation_id = %s")
            params.append(operation_id)
        if variant_id:
            clauses.append("variant_id = %s")
            params.append(variant_id)
        where_sql = f"where {' and '.join(clauses)}" if clauses else ""
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute(
                    f"""
                    select *
                    from semantic_platform.operation_fields
                    {where_sql}
                    order by
                      case scope when 'input' then 0 when 'output' then 1 else 2 end,
                      raw_name
                    """,
                    params,
                )
                return [repo_mod._operation_field_from_row(row) for row in cur.fetchall()]

    def list_operation_variants(self, query: str = "", status: str = "", operation_id: str = "") -> list[dict[str, Any]]:
        from services.semantic_platform.internal.storage import repository as repo_mod

        if self.store_path is not None:
            return self._list_operation_variants_file_store(query=query, status=status, operation_id=operation_id)
        clauses: list[str] = []
        params: list[Any] = []
        if query:
            clauses.append("(variant.name ilike %s or variant.variant_key ilike %s or variant.description ilike %s)")
            like = f"%{query}%"
            params.extend([like, like, like])
        if status:
            clauses.append("variant.status = %s")
            params.append(status)
        if operation_id:
            clauses.append("variant.operation_id = %s")
            params.append(operation_id)
        where_sql = f"where {' and '.join(clauses)}" if clauses else ""
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute(
                    f"""
                    select
                      variant.*,
                      op.name as operation_name,
                      op.operation_key,
                      src.name as source_name
                    from semantic_platform.operation_variants variant
                    join semantic_platform.execution_operations op on op.id = variant.operation_id
                    join semantic_platform.execution_access_paths ap on ap.id = op.access_path_id
                    join semantic_platform.execution_assets asset on asset.id = ap.asset_id
                    join semantic_platform.execution_sources src on src.id = asset.source_id
                    {where_sql}
                    order by src.name, op.name, variant.name
                    """,
                    params,
                )
                records = [repo_mod._operation_variant_from_row(row) for row in cur.fetchall()]
                pending_updates = repo_mod._load_pending_operation_variant_proposals(cur, [item["id"] for item in records])
                return [repo_mod._attach_operation_variant_overlay(item, pending_updates.get(item["id"])) for item in records]

    def create_operation_variant(self, payload: dict[str, Any]) -> dict[str, Any]:
        from services.semantic_platform.internal.storage import repository as repo_mod

        repo_mod._validate_operation_variant_payload(payload, creating=True)
        if self.store_path is not None:
            return self._create_operation_variant_file_store(payload)
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                repo_mod._ensure_execution_operation_exists(cur, str(payload["operation_id"]))
                repo_mod._ensure_operation_variant_key_available(cur, str(payload["variant_key"]))
                record = repo_mod._operation_variant_record(payload)
                proposal = repo_mod._proposal_record(
                    source_type="manual_authoring",
                    title=f"Create operation variant {record['name']}",
                    entity_type="operation_variant",
                    entity_id=record["id"],
                    change_type="create",
                    payload=record,
                )
                cur.execute(
                    """
                    insert into semantic_platform.operation_variants (
                      id, operation_id, variant_key, name, description, version, lifecycle, status,
                      fixed_semantic_arguments, fixed_raw_arguments, metadata, created_at, updated_at
                    ) values (
                      %(id)s, %(operation_id)s, %(variant_key)s, %(name)s, %(description)s, %(version)s, %(lifecycle)s, %(status)s,
                      %(fixed_semantic_arguments)s::jsonb, %(fixed_raw_arguments)s::jsonb, %(metadata)s::jsonb, %(created_at)s::timestamptz, %(updated_at)s::timestamptz
                    )
                    """,
                    repo_mod._sql_operation_variant_params(record),
                )
                repo_mod._insert_proposal(cur, proposal)
            conn.commit()
        return {"operation_variant": record, "proposal": proposal}

    def update_operation_variant(self, variant_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        from services.semantic_platform.internal.storage import repository as repo_mod

        if self.store_path is not None:
            return self._update_operation_variant_file_store(variant_id, payload)
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute("select * from semantic_platform.operation_variants where id = %s", (variant_id,))
                row = cur.fetchone()
                if row is None:
                    raise KeyError(variant_id)
                record = repo_mod._operation_variant_from_row(row)
                updates = repo_mod._operation_variant_updates(payload)
                if "operation_id" in updates:
                    repo_mod._ensure_execution_operation_exists(cur, str(updates["operation_id"]))
                if "variant_key" in updates and updates["variant_key"] != record["variant_key"]:
                    repo_mod._ensure_operation_variant_key_available(cur, str(updates["variant_key"]), exclude_id=variant_id)
                draft_snapshot = {**record, **updates, "updated_at": repo_mod._now()}
                cur.execute(
                    """
                    delete from semantic_platform.proposals
                    where entity_type = 'operation_variant'
                      and entity_id = %s
                      and change_type = 'update'
                      and status = 'pending_review'
                    """,
                    (variant_id,),
                )
                proposal = repo_mod._proposal_record(
                    source_type="manual_authoring",
                    title=f"Update operation variant {record['name']}",
                    entity_type="operation_variant",
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
        return {"operation_variant": draft_snapshot, "proposal": proposal}

    def delete_operation_variant(self, variant_id: str) -> dict[str, Any]:
        from services.semantic_platform.internal.storage import repository as repo_mod

        if self.store_path is not None:
            return self._delete_operation_variant_file_store(variant_id)
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute("select * from semantic_platform.operation_variants where id = %s", (variant_id,))
                row = cur.fetchone()
                if row is None:
                    raise KeyError(variant_id)
                record = repo_mod._operation_variant_from_row(row)
                proposal = repo_mod._proposal_record(
                    source_type="manual_authoring",
                    title=f"Delete operation variant {record['name']}",
                    entity_type="operation_variant",
                    entity_id=record["id"],
                    change_type="delete",
                    payload=dict(record),
                )
                repo_mod._insert_proposal(cur, proposal)
            conn.commit()
        return {"operation_variant": record, "proposal": proposal}

    def create_execution_source(self, payload: dict[str, Any]) -> dict[str, Any]:
        from services.semantic_platform.internal.storage import repository as repo_mod

        repo_mod._validate_execution_source_payload(payload, creating=True)
        if self.store_path is not None:
            return self._create_execution_source_file_store(payload)
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                repo_mod._ensure_execution_source_name_available(cur, str(payload["name"]))
                record = repo_mod._execution_source_record(payload)
                proposal = repo_mod._proposal_record(
                    source_type="manual_authoring",
                    title=f"Create execution source {record['name']}",
                    entity_type="execution_source",
                    entity_id=record["id"],
                    change_type="create",
                    payload=record,
                )
                cur.execute(
                    """
                    insert into semantic_platform.execution_sources (
                      id, name, provider, source_type, description, status,
                      config, created_at, updated_at
                    ) values (
                      %(id)s, %(name)s, %(provider)s, %(source_type)s, %(description)s, %(status)s,
                      %(config)s::jsonb, %(created_at)s::timestamptz, %(updated_at)s::timestamptz
                    )
                    """,
                    repo_mod._sql_execution_source_params(record),
                )
                repo_mod._insert_proposal(cur, proposal)
            conn.commit()
        return {"execution_source": record, "proposal": proposal}

    def update_execution_source(self, source_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        from services.semantic_platform.internal.storage import repository as repo_mod

        if self.store_path is not None:
            return self._update_execution_source_file_store(source_id, payload)
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute("select * from semantic_platform.execution_sources where id = %s", (source_id,))
                row = cur.fetchone()
                if row is None:
                    raise KeyError(source_id)
                record = repo_mod._execution_source_from_row(row)
                updates = repo_mod._execution_source_updates(payload)
                if "name" in updates and updates["name"] != record["name"]:
                    repo_mod._ensure_execution_source_name_available(cur, str(updates["name"]), exclude_id=source_id)
                draft_snapshot = {**record, **updates, "updated_at": repo_mod._now()}
                cur.execute(
                    """
                    delete from semantic_platform.proposals
                    where entity_type = 'execution_source'
                      and entity_id = %s
                      and change_type = 'update'
                      and status = 'pending_review'
                    """,
                    (source_id,),
                )
                proposal = repo_mod._proposal_record(
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
                repo_mod._insert_proposal(cur, proposal)
            conn.commit()
        return {"execution_source": draft_snapshot, "proposal": proposal}

    def delete_execution_source(self, source_id: str) -> dict[str, Any]:
        from services.semantic_platform.internal.storage import repository as repo_mod

        if self.store_path is not None:
            return self._delete_execution_source_file_store(source_id)
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute("select * from semantic_platform.execution_sources where id = %s", (source_id,))
                row = cur.fetchone()
                if row is None:
                    raise KeyError(source_id)
                record = repo_mod._execution_source_from_row(row)
                proposal = repo_mod._proposal_record(
                    source_type="manual_authoring",
                    title=f"Delete execution source {record['name']}",
                    entity_type="execution_source",
                    entity_id=record["id"],
                    change_type="delete",
                    payload=dict(record),
                )
                repo_mod._insert_proposal(cur, proposal)
            conn.commit()
        return {"execution_source": record, "proposal": proposal}

    def record_access_path_check(self, payload: dict[str, Any]) -> dict[str, Any]:
        from services.semantic_platform.internal.storage import repository as repo_mod

        record = {
            "id": f"check_{repo_mod.uuid4().hex}",
            "access_path_id": str(payload.get("access_path_id") or ""),
            "operation_id": str(payload.get("operation_id") or ""),
            "variant_id": str(payload.get("variant_id") or "") or None,
            "method": str(payload.get("method") or ""),
            "locator": str(payload.get("locator") or ""),
            "status": str(payload.get("status") or "recorded"),
            "response_status": payload.get("response_status"),
            "response_excerpt": str(payload.get("response_excerpt") or ""),
            "evidence": payload.get("evidence") if isinstance(payload.get("evidence"), dict) else {},
            "checked_at": str(payload.get("checked_at") or repo_mod._now()),
            "created_at": repo_mod._now(),
        }
        if self.store_path is not None:
            with repo_mod._STORE_LOCK:
                store = self._read_store()
                store.setdefault("access_path_checks", []).append(record)
                self._write_store(store)
            return record
        self.ensure_control_plane_schema()
        with self._connect() as conn:
            with self._dict_cursor(conn) as cur:
                cur.execute(
                    """
                    insert into semantic_platform.access_path_checks (
                      id, access_path_id, operation_id, variant_id, method, locator,
                      status, response_status, response_excerpt, evidence, checked_at, created_at
                    ) values (
                      %(id)s, %(access_path_id)s, %(operation_id)s, %(variant_id)s, %(method)s, %(locator)s,
                      %(status)s, %(response_status)s, %(response_excerpt)s, %(evidence)s::jsonb,
                      %(checked_at)s::timestamptz, %(created_at)s::timestamptz
                    )
                    """,
                    {
                        **record,
                        "evidence": repo_mod.json.dumps(record["evidence"]),
                    },
                )
            conn.commit()
        return record
