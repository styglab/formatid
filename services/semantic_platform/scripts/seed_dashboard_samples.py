from __future__ import annotations

import json
from typing import Any

from services.semantic_platform.internal.storage import SemanticLayerRepository
from services.semantic_platform.internal.storage.repository import (
    _execution_source_record,
    _field_mapping_record,
    _insert_proposal,
    _proposal_record,
    _relationship_record,
    _semantic_type_record,
    _sql_execution_source_params,
    _sql_field_mapping_params,
    _sql_semantic_type_params,
)


def main() -> None:
    repo = SemanticLayerRepository()
    repo.seed_semantic_type_registry()

    with repo._connect() as conn:
        with repo._dict_cursor(conn) as cur:
            semantic_types = ensure_semantic_types(cur)
            ensure_relationship(cur, semantic_types["ProcurementNotice"], semantic_types["ProcurementNoticeTitle"], "has_attribute")
            ensure_relationship(cur, semantic_types["ProcurementNotice"], semantic_types["NoticePublishedAt"], "has_attribute")

            sources = ensure_sources(cur)
            assets = ensure_assets(cur, sources)
            access_paths = ensure_access_paths(cur, assets)
            operations = ensure_operations(cur, access_paths)
            variants = ensure_variants(cur, operations)
            fields = ensure_fields(cur, operations, variants)
            capabilities = ensure_capabilities(cur, semantic_types)
            mappings = ensure_mappings(cur, semantic_types, sources, operations, variants, fields)
            ensure_pending_proposals(cur, sources, capabilities, mappings)

        conn.commit()

    overview = repo.overview()
    print(json.dumps({"status": "ok", "overview": overview}, ensure_ascii=False, indent=2))


def ensure_semantic_types(cur: Any) -> dict[str, dict[str, Any]]:
    payloads = [
        {
            "name": "ProcurementNotice",
            "description": "Canonical procurement notice entity used in dashboard samples.",
            "datatype": "object",
            "entity_kind": "entity",
            "status": "approved",
            "lifecycle": "published",
            "owners": ["platform"],
            "tags": ["sample", "procurement"],
            "documentation": "Dashboard sample root entity.",
        },
        {
            "name": "ProcurementNoticeId",
            "description": "Stable procurement notice identifier.",
            "datatype": "string",
            "entity_kind": "attribute",
            "parent_name": "ProcurementNotice",
            "status": "approved",
            "lifecycle": "published",
            "owners": ["platform"],
            "tags": ["sample", "identifier"],
        },
        {
            "name": "ProcurementNoticeTitle",
            "description": "Human-readable procurement notice title.",
            "datatype": "string",
            "entity_kind": "attribute",
            "parent_name": "ProcurementNotice",
            "status": "approved",
            "lifecycle": "published",
            "owners": ["platform"],
            "tags": ["sample", "title"],
        },
        {
            "name": "NoticePublishedAt",
            "description": "Timestamp when a procurement notice became visible.",
            "datatype": "datetime",
            "entity_kind": "attribute",
            "parent_name": "ProcurementNotice",
            "status": "approved",
            "lifecycle": "published",
            "owners": ["platform"],
            "tags": ["sample", "date"],
        },
        {
            "name": "BidClosingAt",
            "description": "Timestamp when bidding closes.",
            "datatype": "datetime",
            "entity_kind": "attribute",
            "parent_name": "ProcurementNotice",
            "status": "approved",
            "lifecycle": "published",
            "owners": ["platform"],
            "tags": ["sample", "date"],
        },
        {
            "name": "InquiryBasis",
            "description": "Semantic control that selects operation-local inquiry mode.",
            "datatype": "string",
            "entity_kind": "attribute",
            "status": "approved",
            "lifecycle": "published",
            "owners": ["platform"],
            "tags": ["sample", "control"],
        },
        {
            "name": "DisclosureCompanyName",
            "description": "Company name returned from disclosure search APIs.",
            "datatype": "string",
            "entity_kind": "attribute",
            "status": "approved",
            "lifecycle": "published",
            "owners": ["platform"],
            "tags": ["sample", "finance"],
        },
        {
            "name": "ProcurementAgencyName",
            "description": "Name of the procurement agency that published the notice.",
            "datatype": "string",
            "entity_kind": "attribute",
            "parent_name": "ProcurementNotice",
            "status": "approved",
            "lifecycle": "published",
            "owners": ["platform"],
            "tags": ["sample", "agency"],
        },
        {
            "name": "DemandAgencyName",
            "description": "Demand-side agency referenced by the procurement notice.",
            "datatype": "string",
            "entity_kind": "attribute",
            "parent_name": "ProcurementNotice",
            "status": "approved",
            "lifecycle": "published",
            "owners": ["platform"],
            "tags": ["sample", "agency"],
        },
        {
            "name": "EstimatedAmount",
            "description": "Estimated procurement amount represented as a numeric value.",
            "datatype": "number",
            "entity_kind": "attribute",
            "parent_name": "ProcurementNotice",
            "status": "approved",
            "lifecycle": "published",
            "owners": ["platform"],
            "tags": ["sample", "amount"],
        },
        {
            "name": "NoticeSearchStartDate",
            "description": "Start date used when querying procurement notices.",
            "datatype": "date",
            "entity_kind": "attribute",
            "status": "approved",
            "lifecycle": "published",
            "owners": ["platform"],
            "tags": ["sample", "date"],
        },
        {
            "name": "NoticeSearchEndDate",
            "description": "End date used when querying procurement notices.",
            "datatype": "date",
            "entity_kind": "attribute",
            "status": "approved",
            "lifecycle": "published",
            "owners": ["platform"],
            "tags": ["sample", "date"],
        },
        {
            "name": "DisclosureCompanyCode",
            "description": "Stable company code returned from disclosure search APIs.",
            "datatype": "string",
            "entity_kind": "attribute",
            "status": "approved",
            "lifecycle": "published",
            "owners": ["platform"],
            "tags": ["sample", "finance", "identifier"],
        },
        {
            "name": "DisclosureReportName",
            "description": "Disclosure report title returned from disclosure search APIs.",
            "datatype": "string",
            "entity_kind": "attribute",
            "status": "approved",
            "lifecycle": "published",
            "owners": ["platform"],
            "tags": ["sample", "finance", "title"],
        },
    ]
    records: dict[str, dict[str, Any]] = {}
    for payload in payloads:
        cur.execute("select * from semantic_platform.semantic_types where lower(name) = lower(%s)", (payload["name"],))
        existing = cur.fetchone()
        if existing is not None:
          records[payload["name"]] = existing
          continue

        parent_entity_id = ""
        parent_entity_name = ""
        if payload.get("parent_name"):
            parent = records[str(payload["parent_name"])]
            parent_entity_id = parent["id"]
            parent_entity_name = parent["name"]
        record = _semantic_type_record(
            {
                **payload,
                "parent_entity_id": parent_entity_id,
                "parent_entity_name": parent_entity_name,
                "semantic_role": "",
                "namespace": "public",
                "version": "1.0.0",
                "constraint_spec": {},
                "identity_type": "",
                "created_by": "dashboard_seed",
                "approved_at": None,
                "evidence": [{"kind": "seed", "name": payload["name"]}],
                "confidence": 0.95,
            }
        )
        semantic_params = {
            **_sql_semantic_type_params(record),
            "namespace": "public",
            "version": "1.0.0",
            "lifecycle": payload["lifecycle"],
            "constraint_spec": json.dumps({}),
            "identity_type": "",
            "created_by": "dashboard_seed",
            "reviewed_by": "dashboard_seed",
            "approved_at": None,
            "evidence": json.dumps([{"kind": "seed", "name": payload["name"]}]),
            "confidence": 0.95,
            "semantic_role": "",
            "parent_entity_id": parent_entity_id,
            "parent_entity_name": parent_entity_name,
        }
        cur.execute(
            """
            insert into semantic_platform.semantic_types (
              id, urn, namespace, name, description, datatype, entity_kind, version, lifecycle,
              constraint_spec, identity_type, aliases, owners, tags, documentation, created_by,
              reviewed_by, approved_at, evidence, confidence, status, created_at, updated_at,
              semantic_role, parent_entity_id, parent_entity_name
            ) values (
              %(id)s, %(urn)s, %(namespace)s, %(name)s, %(description)s, %(datatype)s, %(entity_kind)s, %(version)s, %(lifecycle)s,
              %(constraint_spec)s::jsonb, %(identity_type)s, %(aliases)s::jsonb, %(owners)s::jsonb, %(tags)s::jsonb, %(documentation)s, %(created_by)s,
              %(reviewed_by)s, %(approved_at)s::timestamptz, %(evidence)s::jsonb, %(confidence)s, %(status)s, %(created_at)s::timestamptz, %(updated_at)s::timestamptz,
              %(semantic_role)s, %(parent_entity_id)s, %(parent_entity_name)s
            )
            """,
            semantic_params,
        )
        record.update(
            {
                "namespace": semantic_params["namespace"],
                "version": semantic_params["version"],
                "lifecycle": semantic_params["lifecycle"],
                "constraint_spec": {},
                "identity_type": "",
                "created_by": semantic_params["created_by"],
                "reviewed_by": semantic_params["reviewed_by"],
                "approved_at": semantic_params["approved_at"],
                "evidence": [{"kind": "seed", "name": payload["name"]}],
                "confidence": semantic_params["confidence"],
            }
        )
        records[payload["name"]] = record
    return records


def ensure_relationship(cur: Any, source: dict[str, Any], target: dict[str, Any], relation_type: str) -> None:
    cur.execute(
        """
        select id from semantic_platform.semantic_relationships
        where source_id = %s and target_id = %s and relation_type = %s
        """,
        (source["id"], target["id"], relation_type),
    )
    if cur.fetchone() is not None:
        return
    record = _relationship_record(source=source, target=target, relation_type=relation_type)
    record.update(
        {
            "version": "1.0.0",
            "lifecycle": "published",
            "created_by": "dashboard_seed",
            "reviewed_by": "dashboard_seed",
            "approved_at": None,
            "evidence": [{"kind": "seed", "relation_type": relation_type}],
            "confidence": 0.9,
            "status": "approved",
        }
    )
    cur.execute(
        """
        insert into semantic_platform.semantic_relationships (
          id, source_id, source_name, target_id, target_name, relation_type, version, lifecycle,
          created_by, reviewed_by, approved_at, evidence, confidence, status, created_at, updated_at
        ) values (
          %(id)s, %(source_id)s, %(source_name)s, %(target_id)s, %(target_name)s, %(relation_type)s, %(version)s, %(lifecycle)s,
          %(created_by)s, %(reviewed_by)s, %(approved_at)s::timestamptz, %(evidence)s::jsonb, %(confidence)s, %(status)s,
          %(created_at)s::timestamptz, %(updated_at)s::timestamptz
        )
        """,
        {**record, "evidence": json.dumps(record["evidence"])},
    )


def ensure_sources(cur: Any) -> dict[str, dict[str, Any]]:
    payloads = [
        {
            "name": "PPS OpenAPI",
            "provider": "PPS",
            "source_type": "api",
            "description": "Public procurement source with bid notice search and detail operations.",
            "status": "approved",
            "config": {
                "input_mode": "openapi",
                "reference_uri": "https://example.local/pps/openapi.yaml",
                "manual_notes": "inqryDiv acts as an operation-local control field and should be reviewed as variants."
            },
        },
        {
            "name": "DART OpenAPI",
            "provider": "DART",
            "source_type": "api",
            "description": "Disclosure search source kept in draft for review workflow contrast.",
            "status": "draft",
            "config": {
                "input_mode": "openapi",
                "reference_uri": "https://example.local/dart/openapi.yaml",
                "manual_notes": "Draft source intentionally incomplete."
            },
        },
        {
            "name": "Legacy Contract CSV",
            "provider": "Internal",
            "source_type": "file",
            "description": "Historical CSV snapshot retained to demonstrate non-API onboarding.",
            "status": "approved",
            "config": {
                "input_mode": "csv",
                "reference_uri": "s3://semantic-platform-samples/legacy-contracts.csv",
                "manual_notes": "Sample file-backed source."
            },
        },
    ]
    records: dict[str, dict[str, Any]] = {}
    for payload in payloads:
        cur.execute("select * from semantic_platform.execution_sources where lower(name) = lower(%s)", (payload["name"],))
        existing = cur.fetchone()
        if existing is not None:
            records[payload["name"]] = existing
            continue
        record = _execution_source_record(
            {
                **payload,
                "namespace": "public",
                "version": "1.0.0",
                "lifecycle": "published" if payload["status"] == "approved" else "review",
                "created_by": "dashboard_seed",
                "reviewed_by": "dashboard_seed",
                "approved_at": None,
                "evidence": [{"kind": "seed", "source": payload["name"]}],
                "confidence": 0.9,
            }
        )
        cur.execute(
            """
            insert into semantic_platform.execution_sources (
              id, namespace, name, provider, source_type, description, version, lifecycle, status,
              config, created_by, reviewed_by, approved_at, evidence, confidence, created_at, updated_at
            ) values (
              %(id)s, %(namespace)s, %(name)s, %(provider)s, %(source_type)s, %(description)s, %(version)s, %(lifecycle)s, %(status)s,
              %(config)s::jsonb, %(created_by)s, %(reviewed_by)s, %(approved_at)s::timestamptz, %(evidence)s::jsonb, %(confidence)s,
              %(created_at)s::timestamptz, %(updated_at)s::timestamptz
            )
            """,
            {
                **_sql_execution_source_params(record),
                "namespace": record.get("namespace", "public"),
                "version": record.get("version", "1.0.0"),
                "lifecycle": record.get("lifecycle", "draft"),
                "created_by": record.get("created_by", "system"),
                "reviewed_by": record.get("reviewed_by"),
                "approved_at": record.get("approved_at"),
                "evidence": json.dumps(record.get("evidence", [])),
                "confidence": record.get("confidence"),
            },
        )
        records[payload["name"]] = record
    return records


def ensure_assets(cur: Any, sources: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    payloads = [
        {
            "source_id": sources["PPS OpenAPI"]["id"],
            "name": "Bid Notice API",
            "asset_type": "endpoint",
            "locator": "/apis/bid-notice",
            "description": "PPS bid notice endpoints.",
            "status": "approved",
            "lifecycle": "published",
        },
        {
            "source_id": sources["DART OpenAPI"]["id"],
            "name": "Disclosure Search API",
            "asset_type": "endpoint",
            "locator": "/api/disclosures",
            "description": "DART disclosure search endpoints.",
            "status": "draft",
            "lifecycle": "review",
        },
    ]
    records: dict[str, dict[str, Any]] = {}
    for payload in payloads:
        cur.execute(
            "select * from semantic_platform.execution_assets where source_id = %s and name = %s",
            (payload["source_id"], payload["name"]),
        )
        existing = cur.fetchone()
        if existing is not None:
            records[payload["name"]] = existing
            continue
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
            {
                "id": f"asset_{payload['name'].lower().replace(' ', '_')}",
                **payload,
                "version": "1.0.0",
                "metadata": json.dumps({"seed": True}),
                "created_by": "dashboard_seed",
                "reviewed_by": "dashboard_seed",
                "approved_at": None,
                "evidence": json.dumps([{"kind": "seed_asset", "name": payload["name"]}]),
                "confidence": 0.9,
                "created_at": sources["PPS OpenAPI"].get("created_at") or None,
                "updated_at": sources["PPS OpenAPI"].get("updated_at") or None,
            },
        )
        cur.execute("select * from semantic_platform.execution_assets where source_id = %s and name = %s", (payload["source_id"], payload["name"]))
        records[payload["name"]] = cur.fetchone()
    return records


def ensure_access_paths(cur: Any, assets: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    payloads = [
        {
            "asset_id": assets["Bid Notice API"]["id"],
            "name": "searchBidNotices",
            "access_type": "http",
            "locator": "/api/v1/bid-notices/search",
            "http_method": "GET",
            "description": "Search bid notices with pagination and inquiry basis controls.",
            "status": "approved",
            "lifecycle": "published",
        },
        {
            "asset_id": assets["Bid Notice API"]["id"],
            "name": "getBidNoticeDetail",
            "access_type": "http",
            "locator": "/api/v1/bid-notices/detail",
            "http_method": "GET",
            "description": "Fetch detail for a specific bid notice.",
            "status": "approved",
            "lifecycle": "published",
        },
        {
            "asset_id": assets["Disclosure Search API"]["id"],
            "name": "searchDisclosures",
            "access_type": "http",
            "locator": "/api/v1/disclosures/search",
            "http_method": "GET",
            "description": "Search disclosure filings by company and date.",
            "status": "draft",
            "lifecycle": "review",
        },
    ]
    records: dict[str, dict[str, Any]] = {}
    for payload in payloads:
        cur.execute(
            "select * from semantic_platform.execution_access_paths where asset_id = %s and name = %s",
            (payload["asset_id"], payload["name"]),
        )
        existing = cur.fetchone()
        if existing is not None:
            records[payload["name"]] = existing
            continue
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
                "id": f"ap_{payload['name']}",
                **payload,
                "version": "1.0.0",
                "request_shape": json.dumps({}),
                "response_shape": json.dumps({}),
                "execution_hints": json.dumps({}),
                "created_by": "dashboard_seed",
                "reviewed_by": "dashboard_seed",
                "approved_at": None,
                "evidence": json.dumps([{"kind": "seed_access_path", "name": payload["name"]}]),
                "confidence": 0.9,
            },
        )
        cur.execute("select * from semantic_platform.execution_access_paths where asset_id = %s and name = %s", (payload["asset_id"], payload["name"]))
        records[payload["name"]] = cur.fetchone()
    return records


def ensure_operations(cur: Any, access_paths: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    payloads = [
        {
            "access_path_name": "searchBidNotices",
            "operation_key": "search_bid_notices",
            "name": "Search Bid Notices",
            "description": "Retrieve procurement notices using provider control fields and pagination.",
            "namespace": "public",
            "status": "approved",
            "lifecycle": "published",
        },
        {
            "access_path_name": "getBidNoticeDetail",
            "operation_key": "get_bid_notice_detail",
            "name": "Get Bid Notice Detail",
            "description": "Fetch one procurement notice detail record.",
            "namespace": "public",
            "status": "approved",
            "lifecycle": "published",
        },
        {
            "access_path_name": "searchDisclosures",
            "operation_key": "search_disclosures",
            "name": "Search Disclosures",
            "description": "Search filing disclosures by company name.",
            "namespace": "finance",
            "status": "draft",
            "lifecycle": "review",
        },
    ]
    records: dict[str, dict[str, Any]] = {}
    for payload in payloads:
        cur.execute("select * from semantic_platform.execution_operations where operation_key = %s", (payload["operation_key"],))
        existing = cur.fetchone()
        if existing is not None:
            records[payload["operation_key"]] = existing
            continue
        access_path = access_paths[payload["access_path_name"]]
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
                "id": f"op_{payload['operation_key']}",
                "access_path_id": access_path["id"],
                **payload,
                "version": "1.0.0",
                "input_spec": json.dumps([]),
                "output_spec": json.dumps([]),
                "auth_spec": json.dumps({"type": "query_key"}),
                "contract_spec": json.dumps({"response_root": "body"}),
                "metadata": json.dumps({"seed": True}),
                "created_by": "dashboard_seed",
                "reviewed_by": "dashboard_seed",
                "approved_at": None,
                "evidence": json.dumps([{"kind": "seed_operation", "operation_key": payload["operation_key"]}]),
                "confidence": 0.9,
            },
        )
        cur.execute("select * from semantic_platform.execution_operations where operation_key = %s", (payload["operation_key"],))
        row = cur.fetchone()
        row["access_path_id"] = access_path["id"]
        records[payload["operation_key"]] = row
    return records


def ensure_variants(cur: Any, operations: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    payloads = [
        {
            "operation_id": operations["search_bid_notices"]["id"],
            "variant_key": "search_bid_notices_by_notice_date",
            "name": "Search Bid Notices By Notice Date",
            "description": "Variant with inquiry basis fixed to notice date.",
            "status": "approved",
            "lifecycle": "published",
            "fixed_semantic_arguments": {"inquiry_basis": "notice_date"},
            "fixed_raw_arguments": {"inqryDiv": "1"},
        },
        {
            "operation_id": operations["search_bid_notices"]["id"],
            "variant_key": "search_bid_notices_by_contract_date",
            "name": "Search Bid Notices By Contract Date",
            "description": "Variant with inquiry basis fixed to contract date.",
            "status": "review",
            "lifecycle": "review",
            "fixed_semantic_arguments": {"inquiry_basis": "contract_date"},
            "fixed_raw_arguments": {"inqryDiv": "2"},
        },
    ]
    records: dict[str, dict[str, Any]] = {}
    for payload in payloads:
        cur.execute("select * from semantic_platform.operation_variants where variant_key = %s", (payload["variant_key"],))
        existing = cur.fetchone()
        if existing is not None:
            records[payload["variant_key"]] = existing
            continue
        cur.execute(
            """
            insert into semantic_platform.operation_variants (
              id, operation_id, variant_key, name, description, version, lifecycle, status,
              fixed_semantic_arguments, fixed_raw_arguments, metadata, created_by, reviewed_by, approved_at, evidence, confidence, created_at, updated_at
            ) values (
              %(id)s, %(operation_id)s, %(variant_key)s, %(name)s, %(description)s, %(version)s, %(lifecycle)s, %(status)s,
              %(fixed_semantic_arguments)s::jsonb, %(fixed_raw_arguments)s::jsonb, %(metadata)s::jsonb, %(created_by)s, %(reviewed_by)s, %(approved_at)s::timestamptz,
              %(evidence)s::jsonb, %(confidence)s, now(), now()
            )
            """,
            {
                "id": f"variant_{payload['variant_key']}",
                **payload,
                "version": "1.0.0",
                "fixed_semantic_arguments": json.dumps(payload["fixed_semantic_arguments"]),
                "fixed_raw_arguments": json.dumps(payload["fixed_raw_arguments"]),
                "metadata": json.dumps({"seed": True}),
                "created_by": "dashboard_seed",
                "reviewed_by": "dashboard_seed",
                "approved_at": None,
                "evidence": json.dumps([{"kind": "seed_variant", "variant_key": payload["variant_key"]}]),
                "confidence": 0.88,
            },
        )
        cur.execute("select * from semantic_platform.operation_variants where variant_key = %s", (payload["variant_key"],))
        records[payload["variant_key"]] = cur.fetchone()
    return records


def ensure_fields(cur: Any, operations: dict[str, dict[str, Any]], variants: dict[str, dict[str, Any]]) -> dict[tuple[str, str], dict[str, Any]]:
    payloads = [
        ("search_bid_notices", None, "input", "pageNo", "query.pageNo", "integer", False, "Pagination page number."),
        ("search_bid_notices", None, "input", "numOfRows", "query.numOfRows", "integer", False, "Pagination page size."),
        ("search_bid_notices", None, "input", "bidNtceBgnDt", "query.bidNtceBgnDt", "date", False, "Search start date."),
        ("search_bid_notices", None, "input", "bidNtceEndDt", "query.bidNtceEndDt", "date", False, "Search end date."),
        ("search_bid_notices", "search_bid_notices_by_notice_date", "control", "inqryDiv", "query.inqryDiv", "string", True, "Control field for notice-date variant."),
        ("search_bid_notices", "search_bid_notices_by_contract_date", "control", "inqryDivContract", "query.inqryDiv", "string", True, "Control field for contract-date variant."),
        ("search_bid_notices", None, "output", "bidNtceNo", "body.items.item[].bidNtceNo", "string", True, "Provider notice identifier."),
        ("search_bid_notices", None, "output", "bidNtceNm", "body.items.item[].bidNtceNm", "string", True, "Provider notice title."),
        ("search_bid_notices", None, "output", "bidNtceDt", "body.items.item[].bidNtceDt", "datetime", False, "Published timestamp."),
        ("search_bid_notices", None, "output", "ntceInsttNm", "body.items.item[].ntceInsttNm", "string", False, "Publishing procurement agency name."),
        ("search_bid_notices", None, "output", "dmndInsttNm", "body.items.item[].dmndInsttNm", "string", False, "Demand agency name."),
        ("search_bid_notices", None, "output", "presmptPrce", "body.items.item[].presmptPrce", "number", False, "Estimated procurement amount."),
        ("get_bid_notice_detail", None, "input", "bidNtceNo", "query.bidNtceNo", "string", True, "Notice identifier."),
        ("get_bid_notice_detail", None, "output", "bidNtceNm", "body.item.bidNtceNm", "string", False, "Detailed notice title."),
        ("get_bid_notice_detail", None, "output", "ntceInsttNm", "body.item.ntceInsttNm", "string", False, "Detailed procurement agency name."),
        ("get_bid_notice_detail", None, "output", "bidClseDt", "body.item.bidClseDt", "datetime", False, "Bid closing timestamp."),
        ("search_disclosures", None, "input", "corp_name_query", "query.corp_name", "string", False, "Company name query."),
        ("search_disclosures", None, "output", "corp_code", "list[].corp_code", "string", False, "Disclosure company code."),
        ("search_disclosures", None, "output", "corp_name", "list[].corp_name", "string", False, "Disclosure company name."),
        ("search_disclosures", None, "output", "report_nm", "list[].report_nm", "string", False, "Disclosure report title."),
    ]
    records: dict[tuple[str, str], dict[str, Any]] = {}
    for operation_key, variant_key, scope, raw_name, field_path, data_type, required, description in payloads:
        operation = operations[operation_key]
        cur.execute(
            "select * from semantic_platform.operation_fields where operation_id = %s and scope = %s and raw_name = %s",
            (operation["id"], scope, raw_name),
        )
        existing = cur.fetchone()
        if existing is not None:
            records[(operation["id"], raw_name)] = existing
            continue
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
            {
                "id": f"field_{operation_key}_{raw_name}",
                "operation_id": operation["id"],
                "variant_id": variants[variant_key]["id"] if variant_key else None,
                "scope": scope,
                "raw_name": raw_name,
                "display_name": raw_name,
                "field_path": field_path,
                "data_type": data_type,
                "is_required": required,
                "description": description,
                "version": "1.0.0",
                "lifecycle": operation.get("lifecycle", "draft"),
                "metadata": json.dumps({"seed": True}),
                "created_by": "dashboard_seed",
                "reviewed_by": "dashboard_seed",
                "approved_at": None,
                "evidence": json.dumps([{"kind": "seed_field", "field_path": field_path}]),
                "confidence": 0.9,
            },
        )
        cur.execute(
            "select * from semantic_platform.operation_fields where operation_id = %s and scope = %s and raw_name = %s",
            (operation["id"], scope, raw_name),
        )
        records[(operation["id"], raw_name)] = cur.fetchone()
    return records


def ensure_capabilities(cur: Any, semantic_types: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    payloads = [
        {
            "capability_key": "search_contract_notices",
            "name": "Search Contract Notices",
            "namespace": "public",
            "description": "Planner-facing capability for procurement notice lookup.",
            "status": "approved",
            "lifecycle": "published",
            "intent_spec": {"examples": ["최근 입찰공고 보여줘"]},
            "input_semantic_types": [semantic_types["InquiryBasis"]["id"]],
            "output_semantic_types": [
                semantic_types["ProcurementNoticeId"]["id"],
                semantic_types["ProcurementNoticeTitle"]["id"],
                semantic_types["NoticePublishedAt"]["id"],
            ],
            "metadata": {"owner": "semantic-platform"},
            "created_by": "dashboard_seed",
            "evidence": [{"kind": "seed_capability"}],
            "confidence": 0.94,
        },
        {
            "capability_key": "get_contract_notice_detail",
            "name": "Get Contract Notice Detail",
            "namespace": "public",
            "description": "Planner-facing capability for notice detail retrieval.",
            "status": "approved",
            "lifecycle": "published",
            "intent_spec": {"examples": ["이 공고 상세 보여줘"]},
            "input_semantic_types": [semantic_types["ProcurementNoticeId"]["id"]],
            "output_semantic_types": [
                semantic_types["ProcurementNoticeId"]["id"],
                semantic_types["ProcurementNoticeTitle"]["id"],
                semantic_types["BidClosingAt"]["id"],
            ],
            "metadata": {"owner": "semantic-platform"},
            "created_by": "dashboard_seed",
            "evidence": [{"kind": "seed_capability"}],
            "confidence": 0.91,
        },
        {
            "capability_key": "search_disclosures",
            "name": "Search Disclosures",
            "namespace": "finance",
            "description": "Planner-facing capability for disclosure lookups.",
            "status": "draft",
            "lifecycle": "review",
            "intent_spec": {"examples": ["공시 검색"]},
            "input_semantic_types": [],
            "output_semantic_types": [semantic_types["DisclosureCompanyName"]["id"]],
            "metadata": {"owner": "semantic-platform"},
            "created_by": "dashboard_seed",
            "evidence": [{"kind": "seed_capability"}],
            "confidence": 0.67,
        },
    ]
    records: dict[str, dict[str, Any]] = {}
    for payload in payloads:
        cur.execute("select * from semantic_platform.capabilities where capability_key = %s", (payload["capability_key"],))
        existing = cur.fetchone()
        if existing is not None:
            records[payload["capability_key"]] = existing
            continue
        record = _execution_capability_record(payload)
        cur.execute(
            """
            insert into semantic_platform.capabilities (
              id, capability_key, namespace, name, description, version, lifecycle, status,
              intent_spec, input_semantic_types, output_semantic_types, metadata, created_by,
              reviewed_by, approved_at, evidence, confidence, created_at, updated_at
            ) values (
              %(id)s, %(capability_key)s, %(namespace)s, %(name)s, %(description)s, %(version)s, %(lifecycle)s, %(status)s,
              %(intent_spec)s::jsonb, %(input_semantic_types)s::jsonb, %(output_semantic_types)s::jsonb, %(metadata)s::jsonb, %(created_by)s,
              %(reviewed_by)s, %(approved_at)s::timestamptz, %(evidence)s::jsonb, %(confidence)s, %(created_at)s::timestamptz, %(updated_at)s::timestamptz
            )
            """,
            record,
        )
        cur.execute("select * from semantic_platform.capabilities where capability_key = %s", (payload["capability_key"],))
        records[payload["capability_key"]] = cur.fetchone()
    return records


def ensure_mappings(
    cur: Any,
    semantic_types: dict[str, dict[str, Any]],
    sources: dict[str, dict[str, Any]],
    operations: dict[str, dict[str, Any]],
    variants: dict[str, dict[str, Any]],
    fields: dict[tuple[str, str], dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    search_op = operations["search_bid_notices"]
    detail_op = operations["get_bid_notice_detail"]
    payloads = [
        {
            "field_id": fields[(search_op["id"], "bidNtceNo")]["id"],
            "source_id": sources["PPS OpenAPI"]["id"],
            "operation_id": search_op["id"],
            "access_path_id": search_op["access_path_id"],
            "field_path": "body.items.item[].bidNtceNo",
            "semantic_type_id": semantic_types["ProcurementNoticeId"]["id"],
            "canonical_attribute_id": semantic_types["ProcurementNoticeId"]["id"],
            "mapping_kind": "direct",
            "mapping_type": "exact",
            "status": "approved",
            "lifecycle": "published",
            "notes": "Primary notice identifier mapping.",
            "evidence": [{"field": "bidNtceNo", "sample": "2024-001"}],
            "confidence": 0.99,
        },
        {
            "field_id": fields[(search_op["id"], "bidNtceNm")]["id"],
            "source_id": sources["PPS OpenAPI"]["id"],
            "operation_id": search_op["id"],
            "access_path_id": search_op["access_path_id"],
            "field_path": "body.items.item[].bidNtceNm",
            "semantic_type_id": semantic_types["ProcurementNoticeTitle"]["id"],
            "canonical_attribute_id": semantic_types["ProcurementNoticeTitle"]["id"],
            "mapping_kind": "direct",
            "mapping_type": "exact",
            "status": "approved",
            "lifecycle": "published",
            "notes": "Procurement notice title mapping.",
            "evidence": [{"field": "bidNtceNm", "sample": "Server upgrade bid"}],
            "confidence": 0.97,
        },
        {
            "field_id": fields[(search_op["id"], "bidNtceDt")]["id"],
            "source_id": sources["PPS OpenAPI"]["id"],
            "operation_id": search_op["id"],
            "access_path_id": search_op["access_path_id"],
            "field_path": "body.items.item[].bidNtceDt",
            "semantic_type_id": semantic_types["NoticePublishedAt"]["id"],
            "canonical_attribute_id": semantic_types["NoticePublishedAt"]["id"],
            "mapping_kind": "direct",
            "mapping_type": "transform",
            "status": "approved",
            "lifecycle": "published",
            "transform_spec": {"from_format": "yyyyMMddHHmmss", "to": "datetime"},
            "notes": "Published timestamp requires parsing.",
            "evidence": [{"field": "bidNtceDt", "sample": "20240611093000"}],
            "confidence": 0.92,
        },
        {
            "field_id": fields[(detail_op["id"], "bidClseDt")]["id"],
            "source_id": sources["PPS OpenAPI"]["id"],
            "operation_id": detail_op["id"],
            "access_path_id": detail_op["access_path_id"],
            "field_path": "body.item.bidClseDt",
            "semantic_type_id": semantic_types["BidClosingAt"]["id"],
            "canonical_attribute_id": semantic_types["BidClosingAt"]["id"],
            "mapping_kind": "direct",
            "mapping_type": "transform",
            "status": "approved",
            "lifecycle": "published",
            "transform_spec": {"from_format": "yyyyMMddHHmmss", "to": "datetime"},
            "notes": "Closing timestamp transform.",
            "evidence": [{"field": "bidClseDt", "sample": "20240620150000"}],
            "confidence": 0.93,
        },
        {
            "field_id": fields[(search_op["id"], "bidNtceBgnDt")]["id"],
            "source_id": sources["PPS OpenAPI"]["id"],
            "operation_id": search_op["id"],
            "access_path_id": search_op["access_path_id"],
            "field_path": "query.bidNtceBgnDt",
            "semantic_type_id": semantic_types["NoticeSearchStartDate"]["id"],
            "canonical_attribute_id": None,
            "mapping_kind": "direct",
            "mapping_type": "exact",
            "status": "approved",
            "lifecycle": "published",
            "notes": "Start date query parameter for notice search.",
            "evidence": [{"field": "bidNtceBgnDt", "sample": "20240601"}],
            "confidence": 0.95,
        },
        {
            "field_id": fields[(search_op["id"], "bidNtceEndDt")]["id"],
            "source_id": sources["PPS OpenAPI"]["id"],
            "operation_id": search_op["id"],
            "access_path_id": search_op["access_path_id"],
            "field_path": "query.bidNtceEndDt",
            "semantic_type_id": semantic_types["NoticeSearchEndDate"]["id"],
            "canonical_attribute_id": None,
            "mapping_kind": "direct",
            "mapping_type": "exact",
            "status": "approved",
            "lifecycle": "published",
            "notes": "End date query parameter for notice search.",
            "evidence": [{"field": "bidNtceEndDt", "sample": "20240630"}],
            "confidence": 0.95,
        },
        {
            "field_id": fields[(search_op["id"], "inqryDiv")]["id"],
            "source_id": sources["PPS OpenAPI"]["id"],
            "operation_id": search_op["id"],
            "variant_id": variants["search_bid_notices_by_notice_date"]["id"],
            "access_path_id": search_op["access_path_id"],
            "field_path": "query.inqryDiv",
            "semantic_type_id": semantic_types["InquiryBasis"]["id"],
            "canonical_attribute_id": None,
            "mapping_kind": "direct",
            "mapping_type": "exact",
            "status": "approved",
            "lifecycle": "published",
            "notes": "Notice-date inquiry basis control.",
            "evidence": [{"field": "inqryDiv", "sample": "1"}],
            "confidence": 0.9,
        },
        {
            "field_id": fields[(search_op["id"], "ntceInsttNm")]["id"],
            "source_id": sources["PPS OpenAPI"]["id"],
            "operation_id": search_op["id"],
            "access_path_id": search_op["access_path_id"],
            "field_path": "body.items.item[].ntceInsttNm",
            "semantic_type_id": semantic_types["ProcurementAgencyName"]["id"],
            "canonical_attribute_id": None,
            "mapping_kind": "direct",
            "mapping_type": "exact",
            "status": "approved",
            "lifecycle": "published",
            "notes": "Publishing procurement agency name.",
            "evidence": [{"field": "ntceInsttNm", "sample": "조달청"}],
            "confidence": 0.94,
        },
        {
            "field_id": fields[(search_op["id"], "dmndInsttNm")]["id"],
            "source_id": sources["PPS OpenAPI"]["id"],
            "operation_id": search_op["id"],
            "access_path_id": search_op["access_path_id"],
            "field_path": "body.items.item[].dmndInsttNm",
            "semantic_type_id": semantic_types["DemandAgencyName"]["id"],
            "canonical_attribute_id": None,
            "mapping_kind": "direct",
            "mapping_type": "exact",
            "status": "approved",
            "lifecycle": "published",
            "notes": "Demand agency name.",
            "evidence": [{"field": "dmndInsttNm", "sample": "과학기술정보통신부"}],
            "confidence": 0.93,
        },
        {
            "field_id": fields[(search_op["id"], "presmptPrce")]["id"],
            "source_id": sources["PPS OpenAPI"]["id"],
            "operation_id": search_op["id"],
            "access_path_id": search_op["access_path_id"],
            "field_path": "body.items.item[].presmptPrce",
            "semantic_type_id": semantic_types["EstimatedAmount"]["id"],
            "canonical_attribute_id": None,
            "mapping_kind": "direct",
            "mapping_type": "transform",
            "status": "approved",
            "lifecycle": "published",
            "transform_spec": {"to": "number", "remove_commas": True},
            "notes": "Estimated amount normalized to numeric value.",
            "evidence": [{"field": "presmptPrce", "sample": "120,000,000"}],
            "confidence": 0.91,
        },
        {
            "field_id": fields[(detail_op["id"], "bidNtceNm")]["id"],
            "source_id": sources["PPS OpenAPI"]["id"],
            "operation_id": detail_op["id"],
            "access_path_id": detail_op["access_path_id"],
            "field_path": "body.item.bidNtceNm",
            "semantic_type_id": semantic_types["ProcurementNoticeTitle"]["id"],
            "canonical_attribute_id": semantic_types["ProcurementNoticeTitle"]["id"],
            "mapping_kind": "direct",
            "mapping_type": "exact",
            "status": "approved",
            "lifecycle": "published",
            "notes": "Detailed title mirrors the search title mapping.",
            "evidence": [{"field": "bidNtceNm", "sample": "Server upgrade bid"}],
            "confidence": 0.95,
        },
        {
            "field_id": fields[(detail_op["id"], "ntceInsttNm")]["id"],
            "source_id": sources["PPS OpenAPI"]["id"],
            "operation_id": detail_op["id"],
            "access_path_id": detail_op["access_path_id"],
            "field_path": "body.item.ntceInsttNm",
            "semantic_type_id": semantic_types["ProcurementAgencyName"]["id"],
            "canonical_attribute_id": None,
            "mapping_kind": "direct",
            "mapping_type": "exact",
            "status": "approved",
            "lifecycle": "published",
            "notes": "Detailed procurement agency name.",
            "evidence": [{"field": "ntceInsttNm", "sample": "조달청"}],
            "confidence": 0.94,
        },
        {
            "field_id": fields[(operations["search_disclosures"]["id"], "corp_name_query")]["id"],
            "source_id": sources["DART OpenAPI"]["id"],
            "operation_id": operations["search_disclosures"]["id"],
            "access_path_id": operations["search_disclosures"]["access_path_id"],
            "field_path": "query.corp_name",
            "semantic_type_id": semantic_types["DisclosureCompanyName"]["id"],
            "canonical_attribute_id": None,
            "mapping_kind": "direct",
            "mapping_type": "exact",
            "status": "draft",
            "lifecycle": "review",
            "notes": "Disclosure company search term.",
            "evidence": [{"field": "corp_name_query", "sample": "삼성전자"}],
            "confidence": 0.8,
        },
        {
            "field_id": fields[(operations["search_disclosures"]["id"], "corp_code")]["id"],
            "source_id": sources["DART OpenAPI"]["id"],
            "operation_id": operations["search_disclosures"]["id"],
            "access_path_id": operations["search_disclosures"]["access_path_id"],
            "field_path": "list[].corp_code",
            "semantic_type_id": semantic_types["DisclosureCompanyCode"]["id"],
            "canonical_attribute_id": None,
            "mapping_kind": "direct",
            "mapping_type": "exact",
            "status": "draft",
            "lifecycle": "review",
            "notes": "Disclosure company code output.",
            "evidence": [{"field": "corp_code", "sample": "00126380"}],
            "confidence": 0.84,
        },
        {
            "field_id": fields[(operations["search_disclosures"]["id"], "corp_name")]["id"],
            "source_id": sources["DART OpenAPI"]["id"],
            "operation_id": operations["search_disclosures"]["id"],
            "access_path_id": operations["search_disclosures"]["access_path_id"],
            "field_path": "list[].corp_name",
            "semantic_type_id": semantic_types["DisclosureCompanyName"]["id"],
            "canonical_attribute_id": None,
            "mapping_kind": "direct",
            "mapping_type": "exact",
            "status": "draft",
            "lifecycle": "review",
            "notes": "Disclosure company name output.",
            "evidence": [{"field": "corp_name", "sample": "삼성전자"}],
            "confidence": 0.87,
        },
        {
            "field_id": fields[(operations["search_disclosures"]["id"], "report_nm")]["id"],
            "source_id": sources["DART OpenAPI"]["id"],
            "operation_id": operations["search_disclosures"]["id"],
            "access_path_id": operations["search_disclosures"]["access_path_id"],
            "field_path": "list[].report_nm",
            "semantic_type_id": semantic_types["DisclosureReportName"]["id"],
            "canonical_attribute_id": None,
            "mapping_kind": "direct",
            "mapping_type": "exact",
            "status": "draft",
            "lifecycle": "review",
            "notes": "Disclosure report title output.",
            "evidence": [{"field": "report_nm", "sample": "사업보고서"}],
            "confidence": 0.86,
        },
    ]
    records: dict[str, dict[str, Any]] = {}
    for payload in payloads:
        cur.execute(
            """
            select * from semantic_platform.field_mappings
            where operation_id = %s
              and coalesce(variant_id, '') = coalesce(%s, '')
              and field_path = %s
              and semantic_type_id = %s
              and coalesce(canonical_attribute_id, '') = coalesce(%s, '')
            """,
            (
                payload["operation_id"],
                payload.get("variant_id"),
                payload["field_path"],
                payload["semantic_type_id"],
                payload.get("canonical_attribute_id"),
            ),
        )
        existing = cur.fetchone()
        if existing is not None:
            records[payload["field_path"]] = existing
            continue
        record = _field_mapping_record({**payload, "namespace": "public", "version": "1.0.0", "created_by": "dashboard_seed"})
        cur.execute(
            """
            insert into semantic_platform.field_mappings (
              id, field_id, source_id, operation_id, variant_id, access_path_id, field_path,
              semantic_type_id, canonical_attribute_id, mapping_kind, mapping_type, version, lifecycle, status,
              namespace, transform_spec, enum_mapping, notes, created_by, reviewed_by, approved_at, evidence, confidence, created_at, updated_at
            ) values (
              %(id)s, %(field_id)s, %(source_id)s, %(operation_id)s, %(variant_id)s, %(access_path_id)s, %(field_path)s,
              %(semantic_type_id)s, %(canonical_attribute_id)s, %(mapping_kind)s, %(mapping_type)s, %(version)s, %(lifecycle)s, %(status)s,
              %(namespace)s, %(transform_spec)s::jsonb, %(enum_mapping)s::jsonb, %(notes)s, %(created_by)s, %(reviewed_by)s,
              %(approved_at)s::timestamptz, %(evidence)s::jsonb, %(confidence)s, %(created_at)s::timestamptz, %(updated_at)s::timestamptz
            )
            """,
            _sql_field_mapping_params(record),
        )
        cur.execute("select * from semantic_platform.field_mappings where id = %s", (record["id"],))
        records[payload["field_path"]] = cur.fetchone()
    return records


def ensure_pending_proposals(cur: Any, sources: dict[str, dict[str, Any]], capabilities: dict[str, dict[str, Any]], mappings: dict[str, dict[str, Any]]) -> None:
    proposals = [
        _proposal_record(
            source_type="manual_authoring",
            title="Refine PPS OpenAPI source onboarding notes",
            entity_type="execution_source",
            entity_id=sources["PPS OpenAPI"]["id"],
            change_type="update",
            payload={
                "approved_snapshot": normalize_row(sources["PPS OpenAPI"]),
                "draft_snapshot": {
                    **normalize_row(sources["PPS OpenAPI"]),
                    "description": "Public procurement source with bid notice search, detail operations, and richer onboarding notes.",
                    "config": {
                        **dict(sources["PPS OpenAPI"].get("config") or {}),
                        "manual_notes": "Expanded evidence notes for variant review and onboarding handoff.",
                    },
                },
            },
        ),
        _proposal_record(
            source_type="manual_authoring",
            title="Expand search_contract_notices capability output coverage",
            entity_type="capability",
            entity_id=capabilities["search_contract_notices"]["id"],
            change_type="update",
            payload={
                "approved_snapshot": normalize_row(capabilities["search_contract_notices"]),
                "draft_snapshot": {
                    **normalize_row(capabilities["search_contract_notices"]),
                    "description": "Planner-facing capability for procurement notice lookup with explicit change-history expectations.",
                    "output_semantic_types": [
                        *list(capabilities["search_contract_notices"].get("output_semantic_types") or []),
                        capabilities["get_contract_notice_detail"]["output_semantic_types"][-1],
                    ],
                    "evidence": [{"kind": "review", "note": "expanded output after operator feedback"}],
                },
            },
        ),
        _proposal_record(
            source_type="manual_authoring",
            title="Adjust bid notice publish date mapping normalization",
            entity_type="field_mapping",
            entity_id=mappings["body.items.item[].bidNtceDt"]["id"],
            change_type="update",
            payload={
                "approved_snapshot": normalize_row(mappings["body.items.item[].bidNtceDt"]),
                "draft_snapshot": {
                    **normalize_row(mappings["body.items.item[].bidNtceDt"]),
                    "notes": "Published timestamp transform plus UTC normalization.",
                    "transform_spec": {
                        **dict(mappings["body.items.item[].bidNtceDt"].get("transform_spec") or {}),
                        "timezone": "Asia/Seoul",
                        "normalize_to": "UTC",
                    },
                },
            },
        ),
    ]
    for proposal in proposals:
        cur.execute(
            """
            select id from semantic_platform.proposals
            where entity_type = %s and entity_id = %s and change_type = %s and status = 'pending_review'
            """,
            (proposal["entity_type"], proposal["entity_id"], proposal["change_type"]),
        )
        if cur.fetchone() is None:
            _insert_proposal(cur, proposal)


def _execution_capability_record(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": f"cap_{payload['capability_key']}",
        "capability_key": payload["capability_key"],
        "namespace": payload.get("namespace", "public"),
        "name": payload["name"],
        "description": payload.get("description", ""),
        "version": "1.0.0",
        "lifecycle": payload.get("lifecycle", "draft"),
        "status": payload.get("status", "draft"),
        "intent_spec": json.dumps(payload.get("intent_spec", {})),
        "input_semantic_types": json.dumps(payload.get("input_semantic_types", [])),
        "output_semantic_types": json.dumps(payload.get("output_semantic_types", [])),
        "metadata": json.dumps(payload.get("metadata", {})),
        "created_by": payload.get("created_by", "dashboard_seed"),
        "reviewed_by": "dashboard_seed",
        "approved_at": None,
        "evidence": json.dumps(payload.get("evidence", [])),
        "confidence": payload.get("confidence", 0.9),
        "created_at": sourcesafe_now(),
        "updated_at": sourcesafe_now(),
    }


def normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    return {key: normalize_value(value) for key, value in dict(row).items()}


def normalize_value(value: Any) -> Any:
    from datetime import date, datetime
    from decimal import Decimal

    if isinstance(value, dict):
        return {key: normalize_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [normalize_value(item) for item in value]
    if isinstance(value, tuple):
        return [normalize_value(item) for item in value]
    if isinstance(value, bytes):
        return value.decode()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def sourcesafe_now() -> str:
    from datetime import UTC, datetime

    return datetime.now(UTC).isoformat()


if __name__ == "__main__":
    main()
