from __future__ import annotations

import os
import json
import hashlib
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterator

LLM_MODES = {"disabled", "codex_manual", "openai"}
DEFAULT_EMBEDDING_MODEL = "text-embedding-3-small"
DEFAULT_EMBEDDING_DIMENSIONS = 1536
INGESTION_GRAPH_VERSION = "2026-05-27.llm-first-verification-v1"
INGESTION_PROMPT_VERSION = "2026-05-27.contract-interpreter-v4"
CATALOG_VERSION_SCOPE = "approved_declarative_catalog_v1"
CATALOG_VERSION_SECTIONS = (
    "semantic_types",
    "entities",
    "entity_identifiers",
    "semantic_join_rules",
    "capabilities",
    "capability_entity_links",
    "capability_dependencies",
    "planning_examples",
    "resources",
    "operations",
    "operation_fields",
    "operation_contracts",
    "operation_variants",
    "field_mappings",
    "capability_implementations",
)
CATALOG_VERSION_RESTORE_APPLY_ORDER = (
    "semantic_types",
    "entities",
    "capabilities",
    "resources",
    "operations",
    "entity_identifiers",
    "semantic_join_rules",
    "capability_entity_links",
    "capability_dependencies",
    "planning_examples",
    "operation_fields",
    "operation_contracts",
    "operation_variants",
    "field_mappings",
    "capability_implementations",
)
CATALOG_VERSION_RESTORE_DELETE_ORDER = tuple(reversed(CATALOG_VERSION_RESTORE_APPLY_ORDER))
_SCHEMA_READY = False


def database_url() -> str:
    if os.getenv("SEMANTIC_PLATFORM_DATABASE_URL"):
        return os.environ["SEMANTIC_PLATFORM_DATABASE_URL"]
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "postgres")
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT", "5432")
    database = os.getenv("POSTGRES_DB", "postgres")
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


def _secret_fingerprint(secret_value: str | None) -> dict[str, Any]:
    if not secret_value:
        return {}
    value = str(secret_value)
    if not value:
        return {}
    if len(value) <= 8:
        preview = f"{value[:2]}...{value[-2:]}"
    else:
        preview = f"{value[:4]}...{value[-4:]}"
    return {
        "preview": preview,
        "sha256": hashlib.sha256(value.encode("utf-8")).hexdigest()[:12],
        "length": len(value),
    }


class SemanticCatalogRepository:
    def __init__(self, url: str | None = None) -> None:
        self.database_url = url or database_url()

    @contextmanager
    def connect(self) -> Iterator[Any]:
        import psycopg
        from psycopg.rows import dict_row

        with psycopg.connect(self.database_url, row_factory=dict_row) as conn:
            yield conn

    def ensure_schema(self) -> None:
        global _SCHEMA_READY
        if _SCHEMA_READY:
            return
        with self.connect() as conn:
            conn.execute("select pg_advisory_xact_lock(hashtext('semantic_platform.ensure_schema'))")
            if _SCHEMA_READY:
                return
            vector_enabled = self._ensure_vector_extension(conn)
            conn.execute(
                """
                create table if not exists sp_source_documents (
                    id text primary key,
                    path text not null unique,
                    file_name text,
                    sha256 text not null,
                    mime_type text,
                    size_bytes bigint not null default 0,
                    status text not null default 'active',
                    metadata jsonb not null default '{}'::jsonb,
                    created_at timestamptz not null default now(),
                    updated_at timestamptz not null default now()
                )
                """
            )
            conn.execute("alter table sp_source_documents add column if not exists file_name text")
            conn.execute(
                """
                update sp_source_documents
                set file_name = coalesce(file_name, metadata->>'file_name')
                where file_name is null
                """
            )
            conn.execute(
                """
                create table if not exists sp_sources (
                    id text primary key,
                    provider text,
                    provider_name_ko text,
                    title text not null,
                    status text not null default 'active',
                    current_revision_id text,
                    auth_secret_refs text[] not null default '{}',
                    auth_parameter_names text[] not null default '{}',
                    metadata jsonb not null default '{}'::jsonb,
                    created_at timestamptz not null default now(),
                    updated_at timestamptz not null default now()
                )
                """
            )
            conn.execute(
                """
                create table if not exists sp_source_revisions (
                    id text primary key,
                    source_id text not null references sp_sources(id) on delete cascade,
                    revision_number integer not null,
                    file_name text not null,
                    content_type text,
                    size_bytes bigint not null default 0,
                    sha256 text not null,
                    object_uri text not null,
                    object_bucket text not null,
                    object_key text not null,
                    uploaded_by text,
                    metadata jsonb not null default '{}'::jsonb,
                    created_at timestamptz not null default now(),
                    unique(source_id, revision_number),
                    unique(source_id, sha256)
                )
                """
            )
            conn.execute(
                """
                create table if not exists sp_secrets (
                    id text primary key,
                    provider text,
                    name text not null,
                    description text,
                    secret_value text,
                    value_preview text,
                    value_sha256 text,
                    value_length integer,
                    status text not null default 'active',
                    metadata jsonb not null default '{}'::jsonb,
                    created_at timestamptz not null default now(),
                    updated_at timestamptz not null default now()
                )
                """
            )
            conn.execute("alter table sp_secrets add column if not exists value_preview text")
            conn.execute("alter table sp_secrets add column if not exists value_sha256 text")
            conn.execute("alter table sp_secrets add column if not exists value_length integer")
            conn.execute(
                """
                create table if not exists sp_source_chunks (
                    id text primary key,
                    source_document_id text not null references sp_source_documents(id) on delete cascade,
                    chunk_index integer not null,
                    title text,
                    text text not null,
                    evidence jsonb not null default '{}'::jsonb,
                    created_at timestamptz not null default now(),
                    unique(source_document_id, chunk_index)
                )
                """
            )
            conn.execute(
                """
                create table if not exists sp_source_evidence_snapshots (
                    id text primary key,
                    source_document_id text not null references sp_source_documents(id) on delete cascade,
                    snapshot_type text not null,
                    payload jsonb not null default '{}'::jsonb,
                    file_path text,
                    created_at timestamptz not null default now(),
                    updated_at timestamptz not null default now()
                )
                """
            )
            conn.execute(
                """
                create table if not exists sp_resources (
                    id text primary key,
                    provider text not null default 'unknown',
                    name_ko text,
                    base_url text,
                    source_document_id text references sp_source_documents(id) on delete set null,
                    evidence jsonb not null default '{}'::jsonb,
                    status text not null default 'pending_review',
                    created_at timestamptz not null default now(),
                    updated_at timestamptz not null default now()
                )
                """
            )
            conn.execute(
                """
                create table if not exists sp_operations (
                    operation_id text primary key,
                    resource_id text references sp_resources(id) on delete set null,
                    provider text not null default 'unknown',
                    method text,
                    path text,
                    title_ko text,
                    description_ko text,
                    source_document_id text references sp_source_documents(id) on delete set null,
                    source_chunk_id text references sp_source_chunks(id) on delete set null,
                    evidence jsonb not null default '{}'::jsonb,
                    status text not null default 'pending_review',
                    created_at timestamptz not null default now(),
                    updated_at timestamptz not null default now()
                )
                """
            )
            conn.execute(
                """
                create table if not exists sp_operation_fields (
                    id text primary key,
                    operation_id text not null references sp_operations(operation_id) on delete cascade,
                    direction text not null,
                    raw_name text not null,
                    location text,
                    path text,
                    label_ko text,
                    description_ko text,
                    example text,
                    type_hint text,
                    unit_hint text,
                    required boolean,
                    evidence jsonb not null default '{}'::jsonb,
                    status text not null default 'pending_review',
                    created_at timestamptz not null default now(),
                    updated_at timestamptz not null default now()
                )
                """
            )
            conn.execute(
                """
                create table if not exists sp_semantic_types (
                    id text primary key,
                    description_ko text,
                    entity text,
                    value_kind text,
                    unit text,
                    canonical_format text,
                    value_shape jsonb not null default '{}'::jsonb,
                    value_contract jsonb not null default '{}'::jsonb,
                    aliases text[] not null default '{}',
                    status text not null default 'active',
                    provenance jsonb not null default '{}'::jsonb,
                    created_at timestamptz not null default now(),
                    updated_at timestamptz not null default now()
                )
                """
            )
            conn.execute("alter table sp_semantic_types add column if not exists value_kind text")
            conn.execute("alter table sp_semantic_types add column if not exists unit text")
            conn.execute("alter table sp_semantic_types add column if not exists canonical_format text")
            conn.execute(
                """
                create table if not exists sp_entities (
                    id text primary key,
                    name_ko text,
                    description_ko text,
                    entity_type text not null default 'entity',
                    aliases text[] not null default '{}',
                    properties jsonb not null default '{}'::jsonb,
                    status text not null default 'active',
                    provenance jsonb not null default '{}'::jsonb,
                    created_at timestamptz not null default now(),
                    updated_at timestamptz not null default now()
                )
                """
            )
            conn.execute(
                """
                create table if not exists sp_entity_identifiers (
                    id text primary key,
                    entity_id text not null references sp_entities(id) on delete cascade,
                    semantic_type_id text not null references sp_semantic_types(id) on delete restrict,
                    identifier_role text not null default 'identifier',
                    validation jsonb not null default '{}'::jsonb,
                    aliases text[] not null default '{}',
                    status text not null default 'active',
                    provenance jsonb not null default '{}'::jsonb,
                    created_at timestamptz not null default now(),
                    updated_at timestamptz not null default now(),
                    unique(entity_id, semantic_type_id, identifier_role)
                )
                """
            )
            conn.execute(
                """
                create table if not exists sp_capabilities (
                    id text primary key,
                    description_ko text,
                    use_when jsonb not null default '[]'::jsonb,
                    inputs jsonb not null default '[]'::jsonb,
                    outputs jsonb not null default '[]'::jsonb,
                    examples jsonb not null default '[]'::jsonb,
                    status text not null default 'active',
                    provenance jsonb not null default '{}'::jsonb,
                    created_at timestamptz not null default now(),
                    updated_at timestamptz not null default now()
                )
                """
            )
            conn.execute(
                """
                create table if not exists sp_capability_entity_links (
                    id text primary key,
                    capability_id text not null references sp_capabilities(id) on delete cascade,
                    entity_id text not null references sp_entities(id) on delete cascade,
                    role text not null,
                    semantic_type_id text references sp_semantic_types(id) on delete set null,
                    required boolean,
                    status text not null default 'active',
                    evidence jsonb not null default '{}'::jsonb,
                    created_at timestamptz not null default now(),
                    updated_at timestamptz not null default now()
                )
                """
            )
            conn.execute(
                """
                create table if not exists sp_capability_dependencies (
                    id text primary key,
                    capability_id text not null references sp_capabilities(id) on delete cascade,
                    depends_on_capability_id text not null references sp_capabilities(id) on delete cascade,
                    dependency_type text not null default 'requires',
                    semantic_type_id text references sp_semantic_types(id) on delete set null,
                    status text not null default 'active',
                    evidence jsonb not null default '{}'::jsonb,
                    created_at timestamptz not null default now(),
                    updated_at timestamptz not null default now()
                )
                """
            )
            conn.execute("drop table if exists sp_semantic_dictionary_terms")
            conn.execute(
                """
                create table if not exists sp_capability_documents (
                    id text primary key,
                    capability_id text not null references sp_capabilities(id) on delete cascade,
                    document_text text not null,
                    aliases jsonb not null default '[]'::jsonb,
                    examples jsonb not null default '[]'::jsonb,
                    intent_patterns jsonb not null default '[]'::jsonb,
                    semantic_entities jsonb not null default '[]'::jsonb,
                    planning_hints jsonb not null default '{}'::jsonb,
                    inputs jsonb not null default '[]'::jsonb,
                    outputs jsonb not null default '[]'::jsonb,
                    tags jsonb not null default '[]'::jsonb,
                    embedding_model text,
                    embedding jsonb,
                    vector_status text not null default 'not_embedded',
                    status text not null default 'active',
                    provenance jsonb not null default '{}'::jsonb,
                    created_at timestamptz not null default now(),
                    updated_at timestamptz not null default now()
                )
                """
            )
            if vector_enabled:
                self._ensure_capability_vector_table(conn)
            conn.execute(
                """
                create table if not exists sp_operation_contracts (
                    operation_id text primary key references sp_operations(operation_id) on delete cascade,
                    capability_id text references sp_capabilities(id) on delete set null,
                    resource_id text references sp_resources(id) on delete set null,
                    provider text not null default 'unknown',
                    method text,
                    path text,
                    auth jsonb not null default '{}'::jsonb,
                    request jsonb not null default '{}'::jsonb,
                    response jsonb not null default '{}'::jsonb,
                    selectors jsonb not null default '{}'::jsonb,
                    status text not null default 'approved',
                    provenance jsonb not null default '{}'::jsonb,
                    created_at timestamptz not null default now(),
                    updated_at timestamptz not null default now()
                )
                """
            )
            conn.execute(
                """
                create table if not exists sp_operation_variants (
                    variant_id text primary key,
                    operation_id text not null references sp_operations(operation_id) on delete cascade,
                    capability_id text references sp_capabilities(id) on delete set null,
                    name text,
                    fixed_semantic_arguments jsonb not null default '{}'::jsonb,
                    fixed_raw_arguments jsonb not null default '{}'::jsonb,
                    verification jsonb not null default '{}'::jsonb,
                    status text not null default 'approved',
                    provenance jsonb not null default '{}'::jsonb,
                    created_at timestamptz not null default now(),
                    updated_at timestamptz not null default now()
                )
                """
            )
            conn.execute(
                """
                create table if not exists sp_field_mappings (
                    id text primary key,
                    operation_field_id text references sp_operation_fields(id) on delete cascade,
                    operation_id text not null references sp_operations(operation_id) on delete cascade,
                    direction text not null,
                    raw_name text not null,
                    semantic_type_id text not null references sp_semantic_types(id) on delete restrict,
                    confidence numeric,
                    status text not null default 'approved',
                    evidence jsonb not null default '{}'::jsonb,
                    created_at timestamptz not null default now(),
                    updated_at timestamptz not null default now(),
                    unique(operation_id, direction, raw_name, semantic_type_id)
                )
                """
            )
            conn.execute(
                """
                create table if not exists sp_capability_implementations (
                    id text primary key,
                    capability_id text not null references sp_capabilities(id) on delete cascade,
                    operation_id text not null references sp_operations(operation_id) on delete cascade,
                    variant_id text references sp_operation_variants(variant_id) on delete set null,
                    tool text,
                    status text not null default 'planned',
                    metadata jsonb not null default '{}'::jsonb,
                    created_at timestamptz not null default now(),
                    updated_at timestamptz not null default now(),
                    unique(capability_id, operation_id, variant_id)
                )
                """
            )
            conn.execute(
                """
                create table if not exists sp_proposals (
                    id text primary key,
                    source_document_id text references sp_source_documents(id) on delete set null,
                    kind text not null,
                    status text not null default 'pending_review',
                    payload jsonb not null default '{}'::jsonb,
                    created_by text not null default 'system',
                    created_at timestamptz not null default now(),
                    reviewed_at timestamptz
                )
                """
            )
            conn.execute(
                """
                create table if not exists sp_proposal_items (
                    id text primary key,
                    proposal_id text not null references sp_proposals(id) on delete cascade,
                    item_type text not null,
                    target_id text,
                    action text not null default 'upsert',
                    status text not null default 'pending_review',
                    payload jsonb not null default '{}'::jsonb,
                    evidence jsonb not null default '{}'::jsonb,
                    created_at timestamptz not null default now()
                )
                """
            )
            conn.execute(
                """
                create table if not exists sp_catalog_lineage (
                    id bigserial primary key,
                    catalog_object_type text not null,
                    catalog_object_id text not null,
                    source_document_id text references sp_source_documents(id) on delete set null,
                    source_chunk_id text references sp_source_chunks(id) on delete set null,
                    operation_id text references sp_operations(operation_id) on delete set null,
                    operation_field_id text references sp_operation_fields(id) on delete set null,
                    proposal_id text references sp_proposals(id) on delete set null,
                    evidence_type text,
                    evidence_text text,
                    confidence numeric,
                    created_at timestamptz not null default now()
                )
                """
            )
            conn.execute(
                """
                create table if not exists sp_endpoint_checks (
                    id text primary key,
                    operation_id text not null,
                    variant_id text,
                    capability_id text,
                    proposal_id text,
                    proposal_item_id text,
                    check_type text not null default 'smoke_test',
                    status text not null,
                    request_payload jsonb not null default '{}'::jsonb,
                    response_sample jsonb not null default '{}'::jsonb,
                    normalized_sample jsonb not null default '{}'::jsonb,
                    error_message text,
                    executor text not null default 'pubdata_mcp',
                    duration_ms integer,
                    checked_at timestamptz not null default now(),
                    created_at timestamptz not null default now()
                )
                """
            )
            conn.execute(
                """
                create table if not exists sp_semantic_join_rules (
                    id text primary key,
                    from_entity_id text references sp_entities(id) on delete cascade,
                    from_semantic_type_id text not null references sp_semantic_types(id) on delete restrict,
                    to_entity_id text references sp_entities(id) on delete cascade,
                    to_semantic_type_id text not null references sp_semantic_types(id) on delete restrict,
                    relation text not null default 'joinable_with',
                    transform jsonb not null default '{}'::jsonb,
                    confidence numeric,
                    status text not null default 'active',
                    evidence jsonb not null default '{}'::jsonb,
                    created_at timestamptz not null default now(),
                    updated_at timestamptz not null default now()
                )
                """
            )
            conn.execute(
                """
                create table if not exists sp_planning_examples (
                    id text primary key,
                    question text not null,
                    expected_capability_ids text[] not null default '{}',
                    expected_operation_ids text[] not null default '{}',
                    expected_variant_ids text[] not null default '{}',
                    expected_arguments jsonb not null default '{}'::jsonb,
                    expected_graph jsonb not null default '{}'::jsonb,
                    tags text[] not null default '{}',
                    source text,
                    status text not null default 'active',
                    provenance jsonb not null default '{}'::jsonb,
                    created_at timestamptz not null default now(),
                    updated_at timestamptz not null default now()
                )
                """
            )
            conn.execute(
                """
                create table if not exists sp_execution_graphs (
                    id text primary key,
                    query text not null,
                    graph jsonb not null default '{}'::jsonb,
                    planner jsonb not null default '{}'::jsonb,
                    retrieved_capabilities jsonb not null default '[]'::jsonb,
                    errors jsonb not null default '[]'::jsonb,
                    status text not null default 'planned',
                    created_at timestamptz not null default now(),
                    updated_at timestamptz not null default now()
                )
                """
            )
            conn.execute(
                """
                create table if not exists sp_ingestion_runs (
                    id text primary key,
                    source_id text not null references sp_sources(id) on delete cascade,
                    revision_id text references sp_source_revisions(id) on delete set null,
                    status text not null default 'queued',
                    commit_mode text not null default 'proposal',
                    current_step text,
                    requested_by text,
                    request jsonb not null default '{}'::jsonb,
                    result jsonb not null default '{}'::jsonb,
                    error_message text,
                    started_at timestamptz,
                    finished_at timestamptz,
                    created_at timestamptz not null default now(),
                    updated_at timestamptz not null default now()
                )
                """
            )
            conn.execute(
                """
                create table if not exists sp_planner_feedback (
                    id text primary key,
                    execution_graph_id text references sp_execution_graphs(id) on delete set null,
                    query text,
                    feedback_type text not null,
                    capability_id text references sp_capabilities(id) on delete set null,
                    variant_id text references sp_operation_variants(variant_id) on delete set null,
                    operation_id text references sp_operations(operation_id) on delete set null,
                    payload jsonb not null default '{}'::jsonb,
                    status text not null default 'open',
                    created_at timestamptz not null default now(),
                    updated_at timestamptz not null default now()
                )
                """
            )
            conn.execute(
                """
                create table if not exists sp_catalog_versions (
                    id text primary key,
                    version_number bigint not null unique,
                    status text not null default 'active',
                    reason text,
                    proposal_id text references sp_proposals(id) on delete set null,
                    snapshot jsonb not null default '{}'::jsonb,
                    snapshot_sha256 text not null,
                    counts jsonb not null default '{}'::jsonb,
                    metadata jsonb not null default '{}'::jsonb,
                    created_by text not null default 'system',
                    created_at timestamptz not null default now()
                )
                """
            )
            conn.execute("alter table sp_capability_implementations add column if not exists variant_id text")
            conn.execute(
                "alter table sp_capability_implementations "
                "drop constraint if exists sp_capability_implementations_capability_id_operation_id_key"
            )
            conn.execute("alter table sp_endpoint_checks add column if not exists variant_id text")
            conn.execute("alter table sp_endpoint_checks add column if not exists capability_id text")
            conn.execute("alter table sp_endpoint_checks add column if not exists proposal_id text")
            conn.execute("alter table sp_endpoint_checks add column if not exists proposal_item_id text")
            conn.execute("create index if not exists sp_operation_fields_operation_idx on sp_operation_fields(operation_id)")
            conn.execute("create index if not exists sp_entity_identifiers_entity_idx on sp_entity_identifiers(entity_id)")
            conn.execute("create index if not exists sp_entity_identifiers_semantic_type_idx on sp_entity_identifiers(semantic_type_id)")
            conn.execute("create index if not exists sp_capability_entity_links_capability_idx on sp_capability_entity_links(capability_id)")
            conn.execute("create index if not exists sp_capability_dependencies_capability_idx on sp_capability_dependencies(capability_id)")
            conn.execute(
                """
                create unique index if not exists sp_capability_entity_links_unique_idx
                on sp_capability_entity_links(capability_id, entity_id, role, coalesce(semantic_type_id, ''))
                """
            )
            conn.execute(
                """
                create unique index if not exists sp_capability_dependencies_unique_idx
                on sp_capability_dependencies(capability_id, depends_on_capability_id, dependency_type, coalesce(semantic_type_id, ''))
                """
            )
            conn.execute("create index if not exists sp_semantic_join_rules_from_idx on sp_semantic_join_rules(from_entity_id, from_semantic_type_id)")
            conn.execute("create index if not exists sp_semantic_join_rules_to_idx on sp_semantic_join_rules(to_entity_id, to_semantic_type_id)")
            conn.execute("create index if not exists sp_planning_examples_status_idx on sp_planning_examples(status)")
            conn.execute("create index if not exists sp_source_evidence_snapshots_source_idx on sp_source_evidence_snapshots(source_document_id, snapshot_type)")
            conn.execute("create index if not exists sp_sources_provider_idx on sp_sources(provider, status)")
            conn.execute("create index if not exists sp_source_revisions_source_idx on sp_source_revisions(source_id, revision_number desc)")
            conn.execute("create index if not exists sp_secrets_provider_idx on sp_secrets(provider, status)")
            conn.execute(
                """
                create unique index if not exists sp_operation_fields_unique_idx
                on sp_operation_fields(operation_id, direction, raw_name, coalesce(path, ''))
                """
            )
            conn.execute("create index if not exists sp_field_mappings_operation_idx on sp_field_mappings(operation_id)")
            conn.execute("create index if not exists sp_lineage_object_idx on sp_catalog_lineage(catalog_object_type, catalog_object_id)")
            conn.execute("create index if not exists sp_endpoint_checks_operation_idx on sp_endpoint_checks(operation_id, checked_at desc)")
            conn.execute("create index if not exists sp_operation_variants_operation_idx on sp_operation_variants(operation_id)")
            conn.execute("create index if not exists sp_endpoint_checks_variant_idx on sp_endpoint_checks(variant_id, checked_at desc)")
            conn.execute("create index if not exists sp_capability_documents_capability_idx on sp_capability_documents(capability_id)")
            if vector_enabled:
                conn.execute(
                    """
                    create index if not exists sp_capability_document_vectors_embedding_idx
                    on sp_capability_document_vectors
                    using ivfflat (embedding vector_cosine_ops)
                    with (lists = 100)
                    """
            )
            conn.execute("create index if not exists sp_execution_graphs_created_idx on sp_execution_graphs(created_at desc)")
            conn.execute("create index if not exists sp_ingestion_runs_created_idx on sp_ingestion_runs(created_at desc)")
            conn.execute("create index if not exists sp_ingestion_runs_source_idx on sp_ingestion_runs(source_id, created_at desc)")
            conn.execute("create index if not exists sp_planner_feedback_created_idx on sp_planner_feedback(created_at desc)")
            conn.execute("create index if not exists sp_catalog_versions_created_idx on sp_catalog_versions(created_at desc)")
            conn.execute("create index if not exists sp_catalog_versions_status_idx on sp_catalog_versions(status, version_number desc)")
            conn.execute(
                """
                create unique index if not exists sp_capability_implementations_variant_unique_idx
                on sp_capability_implementations(capability_id, operation_id, coalesce(variant_id, ''))
                """
            )
            conn.commit()
            _SCHEMA_READY = True

    def secrets(self) -> dict[str, Any]:
        self.ensure_schema()
        with self.connect() as conn:
            rows = conn.execute(
                """
                select id, provider, name, description, status, metadata,
                       secret_value is not null and secret_value <> '' as has_value,
                       value_preview,
                       value_sha256,
                       value_length,
                       created_at, updated_at
                from sp_secrets
                order by provider nulls last, name, id
                """
            ).fetchall()
            return {"secrets": [dict(row) for row in rows]}

    def secret_value(self, secret_id: str) -> str | None:
        self.ensure_schema()
        normalized = str(secret_id or "").strip()
        if normalized and not normalized.startswith("secret."):
            normalized = f"secret.{normalized}"
        with self.connect() as conn:
            row = conn.execute(
                """
                select secret_value
                from sp_secrets
                where id = %s
                  and status in ('active', 'approved')
                """,
                (normalized,),
            ).fetchone()
            if not row:
                return None
            value = row.get("secret_value")
            return str(value) if value else None

    def upsert_secret(
        self,
        *,
        secret_id: str,
        name: str,
        provider: str | None = None,
        description: str | None = None,
        secret_value: str | None = None,
        metadata: dict[str, Any] | None = None,
        allow_update: bool = True,
    ) -> dict[str, Any]:
        self.ensure_schema()
        fingerprint = _secret_fingerprint(secret_value)
        with self.connect() as conn:
            if not allow_update:
                row = conn.execute(
                    """
                    insert into sp_secrets(id, provider, name, description, secret_value,
                                           value_preview, value_sha256, value_length, metadata, updated_at)
                    values (%s, %s, %s, %s, nullif(%s, ''), %s, %s, %s, %s, now())
                    on conflict (id) do nothing
                    returning id, provider, name, description, status, metadata,
                              secret_value is not null and secret_value <> '' as has_value,
                              value_preview, value_sha256, value_length,
                              created_at, updated_at
                    """,
                    (
                        secret_id,
                        provider,
                        name,
                        description,
                        secret_value,
                        fingerprint.get("preview"),
                        fingerprint.get("sha256"),
                        fingerprint.get("length"),
                        _jsonb(metadata or {}),
                    ),
                ).fetchone()
                if not row:
                    conn.rollback()
                    raise FileExistsError(secret_id)
                conn.commit()
                return dict(row)

            row = conn.execute(
                """
                insert into sp_secrets(id, provider, name, description, secret_value,
                                       value_preview, value_sha256, value_length, metadata, updated_at)
                values (%s, %s, %s, %s, nullif(%s, ''), %s, %s, %s, %s, now())
                on conflict (id) do update set
                    provider = excluded.provider,
                    name = excluded.name,
                    description = excluded.description,
                    secret_value = coalesce(excluded.secret_value, sp_secrets.secret_value),
                    value_preview = coalesce(excluded.value_preview, sp_secrets.value_preview),
                    value_sha256 = coalesce(excluded.value_sha256, sp_secrets.value_sha256),
                    value_length = coalesce(excluded.value_length, sp_secrets.value_length),
                    metadata = sp_secrets.metadata || excluded.metadata,
                    updated_at = now()
                returning id, provider, name, description, status, metadata,
                          secret_value is not null and secret_value <> '' as has_value,
                          value_preview, value_sha256, value_length,
                          created_at, updated_at
                """,
                (
                    secret_id,
                    provider,
                    name,
                    description,
                    secret_value,
                    fingerprint.get("preview"),
                    fingerprint.get("sha256"),
                    fingerprint.get("length"),
                    _jsonb(metadata or {}),
                ),
            ).fetchone()
            conn.commit()
            return dict(row)

    def delete_secret(self, secret_id: str) -> dict[str, Any]:
        self.ensure_schema()
        with self.connect() as conn:
            row = conn.execute(
                """
                delete from sp_secrets
                where id = %s
                returning id, provider, name, status
                """,
                (secret_id,),
            ).fetchone()
            conn.commit()
            if not row:
                raise FileNotFoundError(secret_id)
            return {"deleted": dict(row)}

    def source_exists(self, source_id: str) -> bool:
        self.ensure_schema()
        with self.connect() as conn:
            row = conn.execute("select 1 from sp_sources where id = %s", (source_id,)).fetchone()
            return bool(row)

    def source_revision_by_sha(self, source_id: str, sha256: str) -> dict[str, Any] | None:
        self.ensure_schema()
        with self.connect() as conn:
            revision = conn.execute(
                "select * from sp_source_revisions where source_id = %s and sha256 = %s",
                (source_id, sha256),
            ).fetchone()
            if not revision:
                return None
            source = conn.execute("select * from sp_sources where id = %s", (source_id,)).fetchone()
            return {
                "source": dict(source) if source else None,
                "revision": dict(revision),
                "created": False,
                "reason": "revision_already_exists",
            }

    def source_revision(self, source_id: str, revision_id: str | None = None) -> dict[str, Any]:
        self.ensure_schema()
        with self.connect() as conn:
            source = conn.execute("select * from sp_sources where id = %s", (source_id,)).fetchone()
            if not source:
                raise FileNotFoundError(source_id)
            selected_revision_id = revision_id or source.get("current_revision_id")
            if not selected_revision_id:
                raise FileNotFoundError(f"{source_id}: current revision not found")
            revision = conn.execute(
                "select * from sp_source_revisions where source_id = %s and id = %s",
                (source_id, selected_revision_id),
            ).fetchone()
            if not revision:
                raise FileNotFoundError(selected_revision_id)
            return {"source": dict(source), "revision": dict(revision)}

    def create_ingestion_run(
        self,
        *,
        run_id: str,
        source_id: str,
        revision_id: str | None = None,
        commit_mode: str = "proposal",
        requested_by: str | None = None,
        request: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self.ensure_schema()
        with self.connect() as conn:
            row = conn.execute(
                """
                insert into sp_ingestion_runs(id, source_id, revision_id, status, commit_mode,
                                              requested_by, request, current_step, updated_at)
                values (%s, %s, %s, 'queued', %s, %s, %s, 'queued', now())
                returning *
                """,
                (run_id, source_id, revision_id, commit_mode, requested_by, _jsonb(request or {})),
            ).fetchone()
            conn.commit()
            return dict(row)

    def update_ingestion_run(
        self,
        run_id: str,
        *,
        status: str | None = None,
        current_step: str | None = None,
        result: dict[str, Any] | None = None,
        error_message: str | None = None,
        mark_started: bool = False,
        mark_finished: bool = False,
    ) -> dict[str, Any]:
        self.ensure_schema()
        with self.connect() as conn:
            row = conn.execute(
                """
                update sp_ingestion_runs
                set status = coalesce(%s, status),
                    current_step = coalesce(%s, current_step),
                    result = case when %s::jsonb is null then result else %s::jsonb end,
                    error_message = %s,
                    started_at = case when %s then coalesce(started_at, now()) else started_at end,
                    finished_at = case when %s then now() else finished_at end,
                    updated_at = now()
                where id = %s
                returning *
                """,
                (
                    status,
                    current_step,
                    _jsonb(result) if result is not None else None,
                    _jsonb(result) if result is not None else None,
                    error_message,
                    mark_started,
                    mark_finished,
                    run_id,
                ),
            ).fetchone()
            conn.commit()
            if not row:
                raise FileNotFoundError(run_id)
            return dict(row)

    def ingestion_runs(self, limit: int = 100, source_id: str | None = None) -> dict[str, Any]:
        self.ensure_schema()
        limit = max(1, min(int(limit), 500))
        with self.connect() as conn:
            if source_id:
                rows = conn.execute(
                    """
                    select run.*, source.title, revision.file_name, revision.revision_number
                    from sp_ingestion_runs run
                    left join sp_sources source on source.id = run.source_id
                    left join sp_source_revisions revision on revision.id = run.revision_id
                    where run.source_id = %s
                    order by run.created_at desc
                    limit %s
                    """,
                    (source_id, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    select run.*, source.title, revision.file_name, revision.revision_number
                    from sp_ingestion_runs run
                    left join sp_sources source on source.id = run.source_id
                    left join sp_source_revisions revision on revision.id = run.revision_id
                    order by run.created_at desc
                    limit %s
                    """,
                    (limit,),
                ).fetchall()
            return {"ingestion_runs": [dict(row) for row in rows]}

    def ingestion_run(self, run_id: str) -> dict[str, Any]:
        self.ensure_schema()
        with self.connect() as conn:
            row = conn.execute(
                """
                select run.*, source.title, revision.file_name, revision.revision_number
                from sp_ingestion_runs run
                left join sp_sources source on source.id = run.source_id
                left join sp_source_revisions revision on revision.id = run.revision_id
                where run.id = %s
                """,
                (run_id,),
            ).fetchone()
            if not row:
                raise FileNotFoundError(run_id)
            return {"ingestion_run": dict(row)}

    def update_source(
        self,
        source_id: str,
        *,
        provider: str | None = None,
        provider_name_ko: str | None = None,
        title: str | None = None,
        auth_secret_refs: list[str] | None = None,
        auth_parameter_names: list[str] | None = None,
        status: str | None = None,
    ) -> dict[str, Any]:
        self.ensure_schema()
        with self.connect() as conn:
            row = conn.execute(
                """
                update sp_sources
                set provider = %s,
                    provider_name_ko = %s,
                    title = %s,
                    auth_secret_refs = %s,
                    auth_parameter_names = %s,
                    status = coalesce(%s, status),
                    updated_at = now()
                where id = %s
                returning *
                """,
                (
                    provider,
                    provider_name_ko,
                    title,
                    auth_secret_refs or [],
                    auth_parameter_names or [],
                    status,
                    source_id,
                ),
            ).fetchone()
            conn.commit()
            if not row:
                raise FileNotFoundError(source_id)
            return {"source": dict(row)}

    def source_delete_plan(self, source_id: str) -> dict[str, Any]:
        self.ensure_schema()
        with self.connect() as conn:
            source = conn.execute("select * from sp_sources where id = %s", (source_id,)).fetchone()
            if not source:
                raise FileNotFoundError(source_id)
            counts = self._source_related_counts(conn, source_id)
            return {
                "source": {key: source[key] for key in ("id", "provider", "title", "status") if key in source},
                "counts": counts,
                "modes": [
                    {"mode": "archive", "label": "Archive source only", "destructive": False},
                    {"mode": "registry_only", "label": "Delete source registry only", "destructive": True},
                    {"mode": "with_ingestion_artifacts", "label": "Delete source + proposals/evidence", "destructive": True},
                    {"mode": "with_catalog", "label": "Delete source + related catalog", "destructive": True},
                ],
            }

    def delete_source(self, source_id: str, *, mode: str = "archive") -> dict[str, Any]:
        self.ensure_schema()
        allowed_modes = {"archive", "registry_only", "with_ingestion_artifacts", "with_catalog"}
        if mode not in allowed_modes:
            raise ValueError(f"unsupported delete mode: {mode}")
        with self.connect() as conn:
            source = conn.execute("select * from sp_sources where id = %s", (source_id,)).fetchone()
            if not source:
                raise FileNotFoundError(source_id)
            before = self._source_related_counts(conn, source_id)
            if mode == "archive":
                row = conn.execute(
                    """
                    update sp_sources
                    set status = 'archived', updated_at = now()
                    where id = %s
                    returning id, provider, title, status
                    """,
                    (source_id,),
                ).fetchone()
                conn.commit()
                return {"mode": mode, "source": dict(row), "before": before}

            if mode == "with_catalog":
                self._delete_source_catalog(conn, source_id)
            if mode in {"with_ingestion_artifacts", "with_catalog"}:
                conn.execute("delete from sp_proposals where source_document_id = %s", (source_id,))
                conn.execute("delete from sp_source_evidence_snapshots where source_document_id = %s", (source_id,))
                conn.execute("delete from sp_source_chunks where source_document_id = %s", (source_id,))
                conn.execute("delete from sp_source_documents where id = %s", (source_id,))
            row = conn.execute(
                """
                delete from sp_sources
                where id = %s
                returning id, provider, title, status
                """,
                (source_id,),
            ).fetchone()
            conn.commit()
            after = self._source_related_counts(conn, source_id)
            return {"mode": mode, "deleted": dict(row), "before": before, "after": after}

    def reset_catalog(self) -> dict[str, Any]:
        """Clear semantic platform catalog data while keeping schema and extensions."""
        self.ensure_schema()
        tables = _semantic_catalog_tables()
        before = self._table_counts(tables)
        with self.connect() as conn:
            existing_tables = [
                table
                for table in tables
                if conn.execute("select to_regclass(%s) as table_name", (f"public.{table}",)).fetchone()["table_name"]
            ]
            if existing_tables:
                quoted_tables = ", ".join(existing_tables)
                conn.execute(f"truncate table {quoted_tables} restart identity cascade")
            conn.commit()
        after = self._table_counts(tables)
        return {
            "status": "reset",
            "tables": tables,
            "before": before,
            "after": after,
        }

    def _table_counts(self, tables: list[str]) -> dict[str, int]:
        counts: dict[str, int] = {}
        with self.connect() as conn:
            for table in tables:
                exists = conn.execute("select to_regclass(%s) as table_name", (f"public.{table}",)).fetchone()[
                    "table_name"
                ]
                if not exists:
                    counts[table] = 0
                    continue
                row = conn.execute(f"select count(*) as count from {table}").fetchone()
                counts[table] = int(row["count"] or 0)
        return counts

    def _source_related_counts(self, conn: Any, source_id: str) -> dict[str, int]:
        counts: dict[str, int] = {}
        queries = {
            "source_revisions": "select count(*)::int as count from sp_source_revisions where source_id = %s",
            "source_documents": "select count(*)::int as count from sp_source_documents where id = %s",
            "source_chunks": "select count(*)::int as count from sp_source_chunks where source_document_id = %s",
            "source_evidence_snapshots": "select count(*)::int as count from sp_source_evidence_snapshots where source_document_id = %s",
            "proposals": "select count(*)::int as count from sp_proposals where source_document_id = %s",
            "proposal_items": """
                select count(*)::int as count
                from sp_proposal_items item
                join sp_proposals proposal on proposal.id = item.proposal_id
                where proposal.source_document_id = %s
            """,
            "resources": "select count(*)::int as count from sp_resources where source_document_id = %s",
            "operations": "select count(*)::int as count from sp_operations where source_document_id = %s",
            "operation_fields": """
                select count(*)::int as count
                from sp_operation_fields field
                join sp_operations operation on operation.operation_id = field.operation_id
                where operation.source_document_id = %s
            """,
            "capabilities": "select count(*)::int as count from sp_capabilities where provenance->>'source_document_id' = %s",
            "capability_documents": """
                select count(*)::int as count
                from sp_capability_documents document
                join sp_capabilities capability on capability.id = document.capability_id
                where capability.provenance->>'source_document_id' = %s
            """,
            "operation_contracts": """
                select count(*)::int as count
                from sp_operation_contracts contract
                join sp_operations operation on operation.operation_id = contract.operation_id
                where operation.source_document_id = %s
            """,
            "operation_variants": """
                select count(*)::int as count
                from sp_operation_variants variant
                join sp_operations operation on operation.operation_id = variant.operation_id
                where operation.source_document_id = %s
            """,
            "field_mappings": """
                select count(*)::int as count
                from sp_field_mappings mapping
                join sp_operations operation on operation.operation_id = mapping.operation_id
                where operation.source_document_id = %s
            """,
            "capability_implementations": """
                select count(*)::int as count
                from sp_capability_implementations implementation
                join sp_operations operation on operation.operation_id = implementation.operation_id
                where operation.source_document_id = %s
            """,
            "lineage": "select count(*)::int as count from sp_catalog_lineage where source_document_id = %s",
        }
        for key, query in queries.items():
            try:
                counts[key] = int(conn.execute(query, (source_id,)).fetchone()["count"])
            except Exception:
                conn.rollback()
                counts[key] = 0
        return counts

    def _delete_source_catalog(self, conn: Any, source_id: str) -> None:
        operation_rows = conn.execute(
            "select operation_id from sp_operations where source_document_id = %s",
            (source_id,),
        ).fetchall()
        operation_ids = [row["operation_id"] for row in operation_rows]
        capability_rows = conn.execute(
            "select id from sp_capabilities where provenance->>'source_document_id' = %s",
            (source_id,),
        ).fetchall()
        capability_ids = [row["id"] for row in capability_rows]
        if operation_ids:
            conn.execute("delete from sp_operation_contracts where operation_id = any(%s)", (operation_ids,))
            conn.execute("delete from sp_operation_variants where operation_id = any(%s)", (operation_ids,))
            conn.execute("delete from sp_field_mappings where operation_id = any(%s)", (operation_ids,))
            conn.execute("delete from sp_capability_implementations where operation_id = any(%s)", (operation_ids,))
            conn.execute("delete from sp_operation_fields where operation_id = any(%s)", (operation_ids,))
            conn.execute("delete from sp_endpoint_checks where operation_id = any(%s)", (operation_ids,))
            conn.execute("delete from sp_operations where operation_id = any(%s)", (operation_ids,))
        if capability_ids:
            conn.execute("delete from sp_capability_documents where capability_id = any(%s)", (capability_ids,))
            conn.execute("delete from sp_capability_entity_links where capability_id = any(%s)", (capability_ids,))
            conn.execute("delete from sp_capability_dependencies where capability_id = any(%s)", (capability_ids,))
            conn.execute("delete from sp_capability_dependencies where depends_on_capability_id = any(%s)", (capability_ids,))
            conn.execute("delete from sp_capability_implementations where capability_id = any(%s)", (capability_ids,))
            conn.execute("delete from sp_planning_examples where expected_capability_ids && %s", (capability_ids,))
            conn.execute("delete from sp_capabilities where id = any(%s)", (capability_ids,))
        conn.execute("delete from sp_resources where source_document_id = %s", (source_id,))
        conn.execute("delete from sp_catalog_lineage where source_document_id = %s", (source_id,))

    def _ensure_vector_extension(self, conn: Any) -> bool:
        try:
            conn.execute("create extension if not exists vector")
            return True
        except Exception:
            conn.rollback()
            return False

    def _ensure_capability_vector_table(self, conn: Any) -> None:
        dimensions = embedding_dimensions()
        existing = conn.execute(
            "select to_regclass('public.sp_capability_document_vectors') as table_name"
        ).fetchone()
        if existing and existing["table_name"]:
            count = conn.execute("select count(*) as count from sp_capability_document_vectors").fetchone()["count"]
            current_dimensions = conn.execute(
                """
                select atttypmod - 4 as dimensions
                from pg_attribute
                where attrelid = 'public.sp_capability_document_vectors'::regclass
                  and attname = 'embedding'
                """
            ).fetchone()
            if (
                current_dimensions
                and current_dimensions["dimensions"] > 0
                and int(current_dimensions["dimensions"]) != dimensions
                and int(count) == 0
            ):
                conn.execute("drop table sp_capability_document_vectors")
        conn.execute(
            f"""
            create table if not exists sp_capability_document_vectors (
                document_id text primary key references sp_capability_documents(id) on delete cascade,
                capability_id text not null references sp_capabilities(id) on delete cascade,
                embedding_model text not null,
                embedding vector({dimensions}) not null,
                vector_status text not null default 'embedded',
                embedded_at timestamptz not null default now(),
                updated_at timestamptz not null default now()
            )
            """
        )

    def upsert_evidence_snapshot(self, snapshot: dict[str, Any]) -> dict[str, Any]:
        self.ensure_schema()
        with self.connect() as conn:
            row = conn.execute(
                """
                insert into sp_source_evidence_snapshots(id, source_document_id, snapshot_type, payload, file_path, updated_at)
                values (%s, %s, %s, %s, %s, now())
                on conflict (id) do update set
                    payload = excluded.payload,
                    file_path = excluded.file_path,
                    updated_at = now()
                returning *
                """,
                (
                    snapshot["id"],
                    snapshot["source_document_id"],
                    snapshot["snapshot_type"],
                    _jsonb(snapshot.get("payload", {})),
                    snapshot.get("file_path"),
                ),
            ).fetchone()
            conn.commit()
            return dict(row)

    def evidence_snapshots(self, source_document_id: str | None = None, limit: int = 100) -> dict[str, Any]:
        self.ensure_schema()
        with self.connect() as conn:
            if source_document_id:
                rows = conn.execute(
                    """
                    select * from sp_source_evidence_snapshots
                    where source_document_id = %s
                    order by updated_at desc
                    limit %s
                    """,
                    (source_document_id, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    select * from sp_source_evidence_snapshots
                    order by updated_at desc
                    limit %s
                    """,
                    (limit,),
                ).fetchall()
            return {"evidence_snapshots": [dict(row) for row in rows]}

    def upsert_source_document(self, document: dict[str, Any]) -> dict[str, Any]:
        self.ensure_schema()
        with self.connect() as conn:
            row = conn.execute(
                """
                insert into sp_source_documents(id, path, file_name, sha256, mime_type, size_bytes, status, metadata, updated_at)
                values (%s, %s, %s, %s, %s, %s, %s, %s, now())
                on conflict (id) do update set
                    path = excluded.path,
                    file_name = excluded.file_name,
                    sha256 = excluded.sha256,
                    mime_type = excluded.mime_type,
                    size_bytes = excluded.size_bytes,
                    status = excluded.status,
                    metadata = excluded.metadata,
                    updated_at = now()
                returning *
                """,
                (
                    document["id"],
                    document["path"],
                    document.get("file_name"),
                    document["sha256"],
                    document.get("mime_type"),
                    int(document.get("size_bytes") or 0),
                    document.get("status", "active"),
                    _jsonb(document.get("metadata", {})),
                ),
            ).fetchone()
            conn.commit()
            return dict(row)

    def create_source_revision(
        self,
        *,
        source_id: str,
        provider: str | None,
        provider_name_ko: str | None,
        title: str,
        file_name: str,
        content_type: str | None,
        size_bytes: int,
        sha256: str,
        object_uri: str,
        object_bucket: str,
        object_key: str,
        status: str = "active",
        auth_secret_refs: list[str] | None = None,
        auth_parameter_names: list[str] | None = None,
        uploaded_by: str | None = None,
        metadata: dict[str, Any] | None = None,
        allow_update: bool = True,
    ) -> dict[str, Any]:
        self.ensure_schema()
        with self.connect() as conn:
            if not allow_update:
                source = conn.execute("select * from sp_sources where id = %s", (source_id,)).fetchone()
                if source:
                    raise FileExistsError(source_id)
            existing = conn.execute(
                "select * from sp_source_revisions where source_id = %s and sha256 = %s",
                (source_id, sha256),
            ).fetchone()
            if existing:
                source = conn.execute("select * from sp_sources where id = %s", (source_id,)).fetchone()
                return {
                    "source": dict(source) if source else None,
                    "revision": dict(existing),
                    "created": False,
                    "reason": "revision_already_exists",
                }
            current = conn.execute(
                "select coalesce(max(revision_number), 0) + 1 as next_revision from sp_source_revisions where source_id = %s",
                (source_id,),
            ).fetchone()
            revision_number = int(current["next_revision"])
            revision_id = f"source_revision.{source_id.removeprefix('source.')}.{revision_number:03d}"
            source = conn.execute(
                """
                insert into sp_sources(id, provider, provider_name_ko, title, status,
                                       auth_secret_refs, auth_parameter_names, metadata, updated_at)
                values (%s, %s, %s, %s, %s, %s, %s, %s, now())
                on conflict (id) do update set
                    provider = coalesce(excluded.provider, sp_sources.provider),
                    provider_name_ko = coalesce(excluded.provider_name_ko, sp_sources.provider_name_ko),
                    title = excluded.title,
                    status = excluded.status,
                    auth_secret_refs = excluded.auth_secret_refs,
                    auth_parameter_names = excluded.auth_parameter_names,
                    metadata = sp_sources.metadata || excluded.metadata,
                    updated_at = now()
                returning *
                """,
                (
                    source_id,
                    provider,
                    provider_name_ko,
                    title,
                    status,
                    auth_secret_refs or [],
                    auth_parameter_names or [],
                    _jsonb(metadata or {}),
                ),
            ).fetchone()
            revision = conn.execute(
                """
                insert into sp_source_revisions(id, source_id, revision_number, file_name, content_type,
                                                size_bytes, sha256, object_uri, object_bucket, object_key,
                                                uploaded_by, metadata)
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                returning *
                """,
                (
                    revision_id,
                    source_id,
                    revision_number,
                    file_name,
                    content_type,
                    int(size_bytes),
                    sha256,
                    object_uri,
                    object_bucket,
                    object_key,
                    uploaded_by,
                    _jsonb(metadata or {}),
                ),
            ).fetchone()
            conn.execute(
                "update sp_sources set current_revision_id = %s, updated_at = now() where id = %s",
                (revision_id, source_id),
            )
            source = conn.execute("select * from sp_sources where id = %s", (source_id,)).fetchone()
            conn.commit()
            return {"source": dict(source), "revision": dict(revision), "created": True}

    def record_endpoint_check(self, check: dict[str, Any]) -> dict[str, Any]:
        self.ensure_schema()
        with self.connect() as conn:
            row = conn.execute(
                """
                insert into sp_endpoint_checks(id, operation_id, variant_id, capability_id, proposal_id, proposal_item_id,
                                               check_type, status, request_payload,
                                               response_sample, normalized_sample, error_message, executor, duration_ms)
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                on conflict (id) do update set
                    variant_id = excluded.variant_id,
                    capability_id = excluded.capability_id,
                    proposal_id = excluded.proposal_id,
                    proposal_item_id = excluded.proposal_item_id,
                    status = excluded.status,
                    request_payload = excluded.request_payload,
                    response_sample = excluded.response_sample,
                    normalized_sample = excluded.normalized_sample,
                    error_message = excluded.error_message,
                    executor = excluded.executor,
                    duration_ms = excluded.duration_ms,
                    checked_at = now()
                returning *
                """,
                (
                    check["id"],
                    check["operation_id"],
                    check.get("variant_id"),
                    check.get("capability_id"),
                    check.get("proposal_id"),
                    check.get("proposal_item_id"),
                    check.get("check_type", "smoke_test"),
                    check["status"],
                    _jsonb(check.get("request_payload", {})),
                    _jsonb(check.get("response_sample", {})),
                    _jsonb(check.get("normalized_sample", {})),
                    check.get("error_message"),
                    check.get("executor", "pubdata_mcp"),
                    check.get("duration_ms"),
                ),
            ).fetchone()
            conn.commit()
            return dict(row)

    def endpoint_checks(
        self,
        operation_id: str | None = None,
        variant_id: str | None = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        self.ensure_schema()
        with self.connect() as conn:
            if variant_id:
                rows = conn.execute(
                    """
                    select * from sp_endpoint_checks
                    where variant_id = %s
                    order by checked_at desc
                    limit %s
                    """,
                    (variant_id, limit),
                ).fetchall()
            elif operation_id:
                rows = conn.execute(
                    """
                    select * from sp_endpoint_checks
                    where operation_id = %s
                    order by checked_at desc
                    limit %s
                    """,
                    (operation_id, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    select * from sp_endpoint_checks
                    order by checked_at desc
                    limit %s
                    """,
                    (limit,),
                ).fetchall()
            return {"endpoint_checks": [dict(row) for row in rows]}

    def capability_documents(self, limit: int = 100) -> dict[str, Any]:
        self.ensure_schema()
        with self.connect() as conn:
            rows = conn.execute(
                """
                select * from sp_capability_documents
                where status in ('active', 'approved')
                order by updated_at desc
                limit %s
                """,
                (limit,),
            ).fetchall()
            return {"capability_documents": [dict(row) for row in rows]}

    def rebuild_capability_documents(self, capability_ids: list[str] | None = None) -> dict[str, Any]:
        self.ensure_schema()
        selected_capability_ids = {str(value) for value in (capability_ids or []) if str(value or "")}
        with self.connect() as conn:
            if capability_ids is not None and not selected_capability_ids:
                total_count = conn.execute(
                    "select count(*) from sp_capability_documents where status in ('active', 'approved')"
                ).fetchone()["count"]
                return {"capability_documents": [], "count": 0, "total_count": total_count, "capability_ids": []}
            documents = self._rebuild_capability_documents_in_conn(
                conn,
                capability_ids=sorted(selected_capability_ids) if selected_capability_ids else None,
            )
            conn.commit()
            total_count = conn.execute(
                "select count(*) from sp_capability_documents where status in ('active', 'approved')"
            ).fetchone()["count"]
            return {
                "capability_documents": documents,
                "count": len(documents),
                "total_count": total_count,
                "capability_ids": [document["capability_id"] for document in documents],
            }

    def _rebuild_capability_documents_in_conn(
        self,
        conn: Any,
        capability_ids: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        catalog = self._catalog_from_conn(conn)
        selected_capability_ids = {str(value) for value in (capability_ids or []) if str(value or "")}
        documents = []
        for capability_id, capability in catalog.get("capabilities", {}).items():
            if selected_capability_ids and str(capability_id) not in selected_capability_ids:
                continue
            document = _capability_document_from_capability(capability_id, capability, catalog)
            row = conn.execute(
                """
                insert into sp_capability_documents(id, capability_id, document_text, aliases, examples,
                                                    intent_patterns, semantic_entities, planning_hints,
                                                    inputs, outputs, tags, embedding_model, embedding,
                                                    vector_status, status, provenance, updated_at)
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                on conflict (id) do update set
                    document_text = excluded.document_text,
                    aliases = excluded.aliases,
                    examples = excluded.examples,
                    intent_patterns = excluded.intent_patterns,
                    semantic_entities = excluded.semantic_entities,
                    planning_hints = excluded.planning_hints,
                    inputs = excluded.inputs,
                    outputs = excluded.outputs,
                    tags = excluded.tags,
                    embedding_model = excluded.embedding_model,
                    embedding = excluded.embedding,
                    vector_status = excluded.vector_status,
                    status = excluded.status,
                    provenance = excluded.provenance,
                    updated_at = now()
                returning *
                """,
                (
                    document["id"],
                    document["capability_id"],
                    document["document_text"],
                    _jsonb(document.get("aliases", [])),
                    _jsonb(document.get("examples", [])),
                    _jsonb(document.get("intent_patterns", [])),
                    _jsonb(document.get("semantic_entities", [])),
                    _jsonb(document.get("planning_hints", {})),
                    _jsonb(document.get("inputs", [])),
                    _jsonb(document.get("outputs", [])),
                    _jsonb(document.get("tags", [])),
                    document.get("embedding_model"),
                    _jsonb(document.get("embedding")) if document.get("embedding") is not None else None,
                    document.get("vector_status", "not_embedded"),
                    document.get("status", "active"),
                    _jsonb(document.get("provenance", {})),
                ),
            ).fetchone()
            documents.append(dict(row))
        return documents

    def embed_capability_documents(
        self,
        limit: int = 100,
        force: bool = False,
        capability_ids: list[str] | None = None,
        document_ids: list[str] | None = None,
    ) -> dict[str, Any]:
        self.ensure_schema()
        provider = embedding_provider()
        if provider == "openai" and llm_mode() != "openai":
            return {
                "status": "skipped",
                "reason": f"llm_mode_{llm_mode()}",
                "embedded_count": 0,
                "vector_status": "not_generated",
            }
        api_key = os.getenv("OPENAI_API_KEY")
        if provider == "openai" and not api_key:
            return {
                "status": "skipped",
                "reason": "openai_api_key_missing",
                "embedded_count": 0,
                "vector_status": "not_generated",
            }
        with self.connect() as conn:
            if not self._vector_extension_available(conn):
                return {
                    "status": "skipped",
                    "reason": "pgvector_extension_unavailable",
                    "embedded_count": 0,
                    "vector_status": "not_generated",
                }
            documents = self._documents_for_embedding(
                conn,
                limit=limit,
                force=force,
                capability_ids=capability_ids,
                document_ids=document_ids,
            )
            if not documents:
                total_count = conn.execute(
                    "select count(*) from sp_capability_documents where status in ('active', 'approved')"
                ).fetchone()["count"]
                return {
                    "status": "complete",
                    "embedded_count": 0,
                    "total_count": total_count,
                    "vector_status": "unchanged",
                }
            model = embedding_model()
            vectors = _call_embedding_api(
                [str(document.get("document_text") or "") for document in documents],
                model=model,
            )
            if len(vectors) != len(documents):
                return {
                    "status": "failed",
                    "reason": "embedding_count_mismatch",
                    "embedded_count": 0,
                    "vector_status": "failed",
                }
            embedded = []
            for document, vector in zip(documents, vectors):
                if len(vector) != embedding_dimensions():
                    return {
                        "status": "failed",
                        "reason": "embedding_dimension_mismatch",
                        "expected_dimensions": embedding_dimensions(),
                        "actual_dimensions": len(vector),
                        "embedded_count": len(embedded),
                        "vector_status": "failed",
                    }
                conn.execute(
                    """
                    insert into sp_capability_document_vectors(document_id, capability_id, embedding_model, embedding,
                                                               vector_status, embedded_at, updated_at)
                    values (%s, %s, %s, %s::vector, 'embedded', now(), now())
                    on conflict (document_id) do update set
                        capability_id = excluded.capability_id,
                        embedding_model = excluded.embedding_model,
                        embedding = excluded.embedding,
                        vector_status = 'embedded',
                        embedded_at = now(),
                        updated_at = now()
                    """,
                    (
                        document["id"],
                        document["capability_id"],
                        model,
                        _vector_literal(vector),
                    ),
                )
                conn.execute(
                    """
                    update sp_capability_documents
                    set embedding_model = %s,
                        embedding = %s,
                        vector_status = 'embedded',
                        updated_at = now()
                    where id = %s
                    """,
                    (model, _jsonb(vector), document["id"]),
                )
                embedded.append(document["id"])
            conn.commit()
            total_count = conn.execute(
                "select count(*) from sp_capability_documents where status in ('active', 'approved')"
            ).fetchone()["count"]
            return {
                "status": "complete",
                "embedded_count": len(embedded),
                "total_count": total_count,
                "document_ids": embedded,
                "embedding_model": model,
                "embedding_provider": provider,
                "vector_status": "embedded",
            }

    def retrieve_capabilities(self, query: str, limit: int = 10) -> dict[str, Any]:
        self.ensure_schema()
        documents = self.capability_documents(limit=10000)["capability_documents"]
        if not documents and self.catalog().get("capabilities"):
            documents = self.rebuild_capability_documents()["capability_documents"]
        vector_matches = self._retrieve_capabilities_by_vector(query, limit=limit)
        if vector_matches is not None:
            lexical_by_id = {
                str(match["document"].get("id")): float(match["score"])
                for match in _lexical_matches(query, documents, limit=10000)
            }
            for match in vector_matches:
                document_id = str(match["document"].get("id"))
                match["lexical_score"] = lexical_by_id.get(document_id, 0.0)
                match["score"] = float(match["score"]) + min(match["lexical_score"], 3.0) * 0.05
            vector_matches.sort(key=lambda item: item["score"], reverse=True)
            return {
                "query": query,
                "retriever": {
                    "type": "pgvector_hybrid",
                    "vector_status": "embedded",
                    "embedding_model": embedding_model(),
                    "embedding_provider": embedding_provider(),
                },
                "matches": vector_matches[:limit],
            }
        scored = []
        for document in documents:
            score = _lexical_score(query, document)
            if score > 0:
                scored.append({"score": score, "document": document})
        scored.sort(key=lambda item: item["score"], reverse=True)
        return {
            "query": query,
            "retriever": {"type": "lexical_fallback", "vector_status": "not_implemented"},
            "matches": scored[:limit],
        }

    def _documents_for_embedding(
        self,
        conn: Any,
        limit: int,
        force: bool,
        capability_ids: list[str] | None = None,
        document_ids: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        selected_capability_ids = [str(value) for value in (capability_ids or []) if str(value or "")]
        selected_document_ids = [str(value) for value in (document_ids or []) if str(value or "")]
        if capability_ids is not None and not selected_capability_ids:
            return []
        if document_ids is not None and not selected_document_ids:
            return []
        filters = ["status in ('active', 'approved')"]
        joined_filters = ["d.status in ('active', 'approved')"]
        params: list[Any] = []
        if selected_capability_ids:
            filters.append("capability_id = any(%s)")
            joined_filters.append("d.capability_id = any(%s)")
            params.append(selected_capability_ids)
        if selected_document_ids:
            filters.append("id = any(%s)")
            joined_filters.append("d.id = any(%s)")
            params.append(selected_document_ids)
        where_sql = " and ".join(filters)
        if force:
            rows = conn.execute(
                f"""
                select * from sp_capability_documents
                where {where_sql}
                order by updated_at desc
                limit %s
                """,
                (*params, limit),
            ).fetchall()
        else:
            where_sql = " and ".join(joined_filters)
            rows = conn.execute(
                f"""
                select d.*
                from sp_capability_documents d
                left join sp_capability_document_vectors v on v.document_id = d.id
                where {where_sql}
                  and (d.vector_status <> 'embedded' or v.document_id is null)
                order by d.updated_at desc
                limit %s
                """,
                (*params, limit),
            ).fetchall()
        return [dict(row) for row in rows]

    def _vector_extension_available(self, conn: Any) -> bool:
        row = conn.execute("select exists(select 1 from pg_extension where extname = 'vector') as ok").fetchone()
        return bool(row and row["ok"])

    def _retrieve_capabilities_by_vector(self, query: str, limit: int) -> list[dict[str, Any]] | None:
        if embedding_provider() == "openai" and (llm_mode() != "openai" or not os.getenv("OPENAI_API_KEY")):
            return None
        with self.connect() as conn:
            if not self._vector_extension_available(conn):
                return None
            count = conn.execute("select count(*) as count from sp_capability_document_vectors").fetchone()["count"]
            if not count:
                return None
            vectors = _call_embedding_api([query], model=embedding_model())
            if not vectors:
                return None
            rows = conn.execute(
                """
                select d.*,
                       1 - (v.embedding <=> %s::vector) as vector_score
                from sp_capability_document_vectors v
                join sp_capability_documents d on d.id = v.document_id
                where d.status in ('active', 'approved')
                order by v.embedding <=> %s::vector
                limit %s
                """,
                (_vector_literal(vectors[0]), _vector_literal(vectors[0]), limit * 4),
            ).fetchall()
        matches = []
        for row in rows:
            document = dict(row)
            vector_score = float(document.pop("vector_score") or 0.0)
            matches.append({"score": vector_score, "vector_score": vector_score, "document": document})
        return matches

    def upsert_execution_graph(self, graph: dict[str, Any]) -> dict[str, Any]:
        self.ensure_schema()
        with self.connect() as conn:
            row = conn.execute(
                """
                insert into sp_execution_graphs(id, query, graph, planner, retrieved_capabilities, errors, status, updated_at)
                values (%s, %s, %s, %s, %s, %s, %s, now())
                on conflict (id) do update set
                    query = excluded.query,
                    graph = excluded.graph,
                    planner = excluded.planner,
                    retrieved_capabilities = excluded.retrieved_capabilities,
                    errors = excluded.errors,
                    status = excluded.status,
                    updated_at = now()
                returning *
                """,
                (
                    graph["id"],
                    graph.get("query", ""),
                    _jsonb(graph.get("graph", {})),
                    _jsonb(graph.get("planner", {})),
                    _jsonb(graph.get("retrieved_capabilities", [])),
                    _jsonb(graph.get("errors", [])),
                    graph.get("status", "planned"),
                ),
            ).fetchone()
            conn.commit()
            return dict(row)

    def execution_graphs(self, limit: int = 100) -> dict[str, Any]:
        self.ensure_schema()
        with self.connect() as conn:
            rows = conn.execute(
                "select * from sp_execution_graphs order by created_at desc limit %s",
                (limit,),
            ).fetchall()
            return {"execution_graphs": [dict(row) for row in rows]}

    def catalog_versions(self, limit: int = 100, offset: int = 0) -> dict[str, Any]:
        self.ensure_schema()
        limit = max(1, min(int(limit), 500))
        offset = max(0, int(offset))
        with self.connect() as conn:
            total = conn.execute("select count(*)::int as count from sp_catalog_versions").fetchone()["count"]
            rows = conn.execute(
                """
                select id, version_number, status, reason, proposal_id, snapshot_sha256,
                       counts, metadata, created_by, created_at
                from sp_catalog_versions
                order by version_number desc
                limit %s offset %s
                """,
                (limit, offset),
            ).fetchall()
        return {
            "catalog_versions": [dict(row) for row in rows],
            "total": int(total),
            "limit": limit,
            "offset": offset,
            "next_offset": offset + limit if offset + limit < int(total) else None,
        }

    def catalog_version(self, version_id: str) -> dict[str, Any]:
        self.ensure_schema()
        with self.connect() as conn:
            row = conn.execute(
                "select * from sp_catalog_versions where id = %s",
                (version_id,),
            ).fetchone()
            if not row:
                raise FileNotFoundError(version_id)
            return {"catalog_version": dict(row)}

    def catalog_version_diff(self, version_id: str, base_version_id: str | None = None) -> dict[str, Any]:
        self.ensure_schema()
        with self.connect() as conn:
            target = conn.execute("select * from sp_catalog_versions where id = %s", (version_id,)).fetchone()
            if not target:
                raise FileNotFoundError(version_id)
            if base_version_id:
                base = conn.execute("select * from sp_catalog_versions where id = %s", (base_version_id,)).fetchone()
            else:
                base = conn.execute(
                    """
                    select *
                    from sp_catalog_versions
                    where version_number < %s
                    order by version_number desc
                    limit 1
                    """,
                    (target["version_number"],),
                ).fetchone()
            if not base:
                return {
                    "base_version": None,
                    "target_version": _catalog_version_summary(dict(target)),
                    "diff": _catalog_snapshot_diff({}, target.get("snapshot") or {}),
                }
            return {
                "base_version": _catalog_version_summary(dict(base)),
                "target_version": _catalog_version_summary(dict(target)),
                "diff": _catalog_snapshot_diff(base.get("snapshot") or {}, target.get("snapshot") or {}),
            }

    def restore_catalog_version(self, version_id: str, *, restored_by: str = "system") -> dict[str, Any]:
        self.ensure_schema()
        with self.connect() as conn:
            target = conn.execute(
                """
                select id, version_number, status, reason, proposal_id, snapshot_sha256,
                       snapshot, counts, metadata, created_by, created_at
                from sp_catalog_versions
                where id = %s
                """,
                (version_id,),
            ).fetchone()
            if not target:
                raise FileNotFoundError(version_id)
            target_version = dict(target)
            target_snapshot = _catalog_version_snapshot(target_version.get("snapshot") or {})
            current_snapshot = _catalog_version_snapshot(self._catalog_from_conn(conn))
            diff = _catalog_snapshot_diff(current_snapshot, target_snapshot)
            self._restore_catalog_snapshot(conn, target_snapshot)
            capability_ids = sorted(_snapshot_section_items(target_snapshot.get("capabilities")).keys())
            conn.execute("delete from sp_capability_documents")
            if capability_ids:
                self._rebuild_capability_documents_in_conn(conn, capability_ids=capability_ids)
            restored_version = self._create_catalog_version(
                conn,
                reason="version_restore",
                created_by=restored_by,
                metadata={
                    "restored_from_version_id": target_version.get("id"),
                    "restored_from_version_number": target_version.get("version_number"),
                    "restore_diff": diff.get("counts") or {},
                },
                allow_duplicate_snapshot=True,
            )
            conn.commit()
            return {
                "restored_from": _catalog_version_summary(target_version),
                "catalog_version": restored_version.get("catalog_version"),
                "created": restored_version.get("created"),
                "diff": diff,
            }

    def create_catalog_version(
        self,
        *,
        reason: str,
        proposal_id: str | None = None,
        created_by: str = "system",
        metadata: dict[str, Any] | None = None,
        allow_duplicate_snapshot: bool = False,
    ) -> dict[str, Any]:
        self.ensure_schema()
        with self.connect() as conn:
            result = self._create_catalog_version(
                conn,
                reason=reason,
                proposal_id=proposal_id,
                created_by=created_by,
                metadata=metadata,
                allow_duplicate_snapshot=allow_duplicate_snapshot,
            )
            conn.commit()
            return result

    def _restore_catalog_snapshot(self, conn: Any, snapshot: dict[str, Any]) -> None:
        specs = _catalog_section_specs()
        for section in CATALOG_VERSION_RESTORE_DELETE_ORDER:
            spec = specs[section]
            table = spec["table"]
            key = spec["key"]
            item_ids = sorted(_snapshot_section_items(snapshot.get(section)).keys())
            conn.execute(f"delete from {table} where {key} <> all(%s)", (item_ids,))
        apply_methods = {
            "semantic_types": self._apply_semantic_type,
            "entities": self._apply_entity,
            "entity_identifiers": self._apply_entity_identifier,
            "semantic_join_rules": self._apply_semantic_join_rule,
            "capabilities": self._apply_capability,
            "capability_entity_links": self._apply_capability_entity_link,
            "capability_dependencies": self._apply_capability_dependency,
            "planning_examples": self._apply_planning_example,
            "resources": self._apply_resource,
            "operations": self._apply_operation,
            "operation_fields": self._apply_operation_field,
            "operation_contracts": self._apply_operation_contract,
            "operation_variants": self._apply_operation_variant,
            "field_mappings": self._apply_field_mapping,
            "capability_implementations": self._apply_capability_implementation,
        }
        for section in CATALOG_VERSION_RESTORE_APPLY_ORDER:
            for item in _snapshot_section_items(snapshot.get(section)).values():
                if isinstance(item, dict):
                    apply_methods[section](conn, item)

    def _create_catalog_version(
        self,
        conn: Any,
        *,
        reason: str,
        proposal_id: str | None = None,
        created_by: str = "system",
        metadata: dict[str, Any] | None = None,
        allow_duplicate_snapshot: bool = False,
    ) -> dict[str, Any]:
        full_catalog = self._catalog_from_conn(conn)
        snapshot = _catalog_version_snapshot(full_catalog)
        counts = _catalog_snapshot_counts(snapshot)
        snapshot_payload = _stable_json(snapshot)
        snapshot_sha256 = hashlib.sha256(snapshot_payload.encode("utf-8")).hexdigest()
        version_metadata = {
            **(metadata or {}),
            "snapshot_scope": CATALOG_VERSION_SCOPE,
            "snapshot_sections": list(CATALOG_VERSION_SECTIONS),
            "excluded_sections": sorted(set(full_catalog) - set(CATALOG_VERSION_SECTIONS)),
        }
        if not allow_duplicate_snapshot:
            existing = conn.execute(
                """
                select id, version_number, status, reason, proposal_id, snapshot_sha256,
                       counts, metadata, created_by, created_at
                from sp_catalog_versions
                where snapshot_sha256 = %s
                order by version_number desc
                limit 1
                """,
                (snapshot_sha256,),
            ).fetchone()
            if existing:
                return {"catalog_version": dict(existing), "created": False, "reason": "snapshot_unchanged"}
        next_number = conn.execute(
            "select coalesce(max(version_number), 0) + 1 as version_number from sp_catalog_versions"
        ).fetchone()["version_number"]
        version_number = int(next_number)
        version_id = f"catalog_version.{version_number:06d}"
        conn.execute("update sp_catalog_versions set status = 'archived' where status = 'active'")
        row = conn.execute(
            """
            insert into sp_catalog_versions(id, version_number, status, reason, proposal_id,
                                            snapshot, snapshot_sha256, counts, metadata, created_by)
            values (%s, %s, 'active', %s, %s, %s, %s, %s, %s, %s)
            returning id, version_number, status, reason, proposal_id, snapshot_sha256,
                      counts, metadata, created_by, created_at
            """,
            (
                version_id,
                version_number,
                reason,
                proposal_id,
                _jsonb(snapshot),
                snapshot_sha256,
                _jsonb(counts),
                _jsonb(version_metadata),
                created_by,
            ),
        ).fetchone()
        return {"catalog_version": dict(row), "created": True}

    def record_planner_feedback(self, feedback: dict[str, Any]) -> dict[str, Any]:
        self.ensure_schema()
        with self.connect() as conn:
            row = conn.execute(
                """
                insert into sp_planner_feedback(id, execution_graph_id, query, feedback_type,
                                                capability_id, variant_id, operation_id, payload, status, updated_at)
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                on conflict (id) do update set
                    execution_graph_id = excluded.execution_graph_id,
                    query = excluded.query,
                    feedback_type = excluded.feedback_type,
                    capability_id = excluded.capability_id,
                    variant_id = excluded.variant_id,
                    operation_id = excluded.operation_id,
                    payload = excluded.payload,
                    status = excluded.status,
                    updated_at = now()
                returning *
                """,
                (
                    feedback["id"],
                    feedback.get("execution_graph_id"),
                    feedback.get("query"),
                    feedback["feedback_type"],
                    feedback.get("capability_id"),
                    feedback.get("variant_id"),
                    feedback.get("operation_id"),
                    _jsonb(feedback.get("payload", {})),
                    feedback.get("status", "open"),
                ),
            ).fetchone()
            conn.commit()
            return dict(row)

    def planner_feedback(self, limit: int = 100) -> dict[str, Any]:
        self.ensure_schema()
        with self.connect() as conn:
            rows = conn.execute(
                "select * from sp_planner_feedback order by created_at desc limit %s",
                (limit,),
            ).fetchall()
            return {"planner_feedback": [dict(row) for row in rows]}

    def replace_chunks(self, source_document_id: str, chunks: list[dict[str, Any]]) -> None:
        self.ensure_schema()
        with self.connect() as conn:
            conn.execute("delete from sp_source_chunks where source_document_id = %s", (source_document_id,))
            for chunk in chunks:
                conn.execute(
                    """
                    insert into sp_source_chunks(id, source_document_id, chunk_index, title, text, evidence)
                    values (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        chunk["id"],
                        source_document_id,
                        int(chunk["chunk_index"]),
                        chunk.get("title"),
                        chunk.get("text", ""),
                        _jsonb(chunk.get("evidence", {})),
                    ),
                )
            conn.commit()

    def create_proposal(self, proposal: dict[str, Any], items: list[dict[str, Any]]) -> dict[str, Any]:
        self.ensure_schema()
        with self.connect() as conn:
            row = conn.execute(
                """
                insert into sp_proposals(id, source_document_id, kind, status, payload, created_by)
                values (%s, %s, %s, %s, %s, %s)
                on conflict (id) do update set
                    source_document_id = excluded.source_document_id,
                    kind = excluded.kind,
                    status = excluded.status,
                    payload = excluded.payload
                returning *
                """,
                (
                    proposal["id"],
                    proposal.get("source_document_id"),
                    proposal.get("kind", "source_ingestion"),
                    proposal.get("status", "pending_review"),
                    _jsonb(proposal.get("payload", {})),
                    proposal.get("created_by", "system"),
                ),
            ).fetchone()
            conn.execute("delete from sp_proposal_items where proposal_id = %s", (proposal["id"],))
            for item in items:
                conn.execute(
                    """
                    insert into sp_proposal_items(id, proposal_id, item_type, target_id, action, status, payload, evidence)
                    values (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        item["id"],
                        proposal["id"],
                        item["item_type"],
                        item.get("target_id"),
                        item.get("action", "upsert"),
                        item.get("status", "pending_review"),
                        _jsonb(item.get("payload", {})),
                        _jsonb(item.get("evidence", {})),
                    ),
                )
            conn.commit()
            return dict(row)

    def apply_proposal(self, proposal_id: str, reviewer: str = "system") -> dict[str, Any]:
        self.ensure_schema()
        with self.connect() as conn:
            proposal = conn.execute("select * from sp_proposals where id = %s", (proposal_id,)).fetchone()
            if not proposal:
                raise FileNotFoundError(proposal_id)
            items = conn.execute(
                """
                select * from sp_proposal_items
                where proposal_id = %s
                order by
                  case item_type
                    when 'resource' then 10
                    when 'semantic_type' then 20
                    when 'entity' then 25
                    when 'entity_identifier' then 26
                    when 'capability' then 30
                    when 'capability_entity_link' then 32
                    when 'capability_dependency' then 33
                    when 'capability_document' then 35
                    when 'operation' then 40
                    when 'operation_field' then 50
                    when 'operation_contract' then 60
                    when 'operation_variant' then 70
                    when 'field_mapping' then 80
                    when 'capability_implementation' then 90
                    when 'semantic_join_rule' then 95
                    when 'planning_example' then 96
                    else 100
                  end,
                  id
                """,
                (proposal_id,),
            ).fetchall()
            self._validate_capability_proposal_items(items)
            applied: list[dict[str, Any]] = []
            for item in items:
                payload = dict(item["payload"] or {})
                item_type = item["item_type"]
                if item_type == "resource":
                    self._apply_resource(conn, payload)
                elif item_type == "operation":
                    self._apply_operation(conn, payload)
                elif item_type == "operation_field":
                    self._apply_operation_field(conn, payload)
                elif item_type == "semantic_type":
                    self._apply_semantic_type(conn, payload)
                elif item_type == "entity":
                    self._apply_entity(conn, payload)
                elif item_type == "entity_identifier":
                    self._apply_entity_identifier(conn, payload)
                elif item_type == "capability":
                    self._apply_capability(conn, payload)
                elif item_type == "capability_entity_link":
                    self._apply_capability_entity_link(conn, payload)
                elif item_type == "capability_dependency":
                    self._apply_capability_dependency(conn, payload)
                elif item_type == "capability_document":
                    self._apply_capability_document(conn, payload)
                elif item_type == "operation_contract":
                    self._apply_operation_contract(conn, payload)
                elif item_type == "operation_variant":
                    self._apply_operation_variant(conn, payload)
                elif item_type == "field_mapping":
                    self._apply_field_mapping(conn, payload)
                elif item_type == "capability_implementation":
                    self._apply_capability_implementation(conn, payload)
                elif item_type == "semantic_join_rule":
                    self._apply_semantic_join_rule(conn, payload)
                elif item_type == "planning_example":
                    self._apply_planning_example(conn, payload)
                else:
                    continue
                conn.execute(
                    """
                    update sp_proposal_items
                    set status = 'approved'
                    where id = %s
                    """,
                    (item["id"],),
                )
                self._insert_lineage(conn, item_type, payload, proposal_id, item)
                applied.append({"item_type": item_type, "target_id": item["target_id"]})
            capability_ids = _proposal_capability_ids(items, applied)
            rebuilt_documents = (
                self._rebuild_capability_documents_in_conn(conn, capability_ids=capability_ids)
                if capability_ids
                else []
            )
            conn.execute(
                """
                update sp_proposals
                set status = 'approved', reviewed_at = now()
                where id = %s
                """,
                (proposal_id,),
            )
            conn.commit()
            return {
                "proposal_id": proposal_id,
                "reviewer": reviewer,
                "applied": applied,
                "capability_documents": {
                    "capability_documents": rebuilt_documents,
                    "count": len(rebuilt_documents),
                    "capability_ids": [document["capability_id"] for document in rebuilt_documents],
                },
                "catalog_version": None,
            }

    def _validate_capability_proposal_items(self, items: list[dict[str, Any]]) -> None:
        capability_ids = {
            str((item.get("payload") or {}).get("id") or "")
            for item in items
            if item.get("item_type") == "capability" and (item.get("payload") or {}).get("id")
        }
        capability_ids.update(
            str((item.get("evidence") or {}).get("proposal_capability_id") or "")
            for item in items
            if (item.get("evidence") or {}).get("proposal_capability_id")
        )
        capability_ids.discard("")
        if not capability_ids:
            return
        if len(capability_ids) != 1:
            raise ValueError(f"proposal must contain exactly one capability closure, got {sorted(capability_ids)}")
        capability_id = next(iter(capability_ids))
        payloads = [dict(item["payload"] or {}) for item in items]
        allowed_operation_ids = {
            str(payload.get("operation_id") or "")
            for item, payload in zip(items, payloads)
            if item.get("item_type") in {"operation_contract", "operation_variant", "capability_implementation"}
            and payload.get("operation_id")
            and str(payload.get("capability_id") or payload.get("capability") or capability_id) == capability_id
        }
        allowed_operation_ids.discard("")
        allowed_semantic_type_ids = {
            str(value)
            for item, payload in zip(items, payloads)
            if item.get("item_type") == "capability"
            for value in [*_json_list(payload.get("inputs")), *_json_list(payload.get("outputs"))]
        }
        for item, payload in zip(items, payloads):
            item_type = item.get("item_type")
            item_capability = str(payload.get("capability_id") or payload.get("capability") or "")
            if item_type == "capability" and str(payload.get("id") or "") != capability_id:
                raise ValueError(f"proposal item outside capability closure: {item.get('id')}")
            if item_type in {"operation_variant", "capability_implementation"} and item_capability != capability_id:
                raise ValueError(f"proposal item outside capability closure: {item.get('id')}")
            if item_type == "operation_contract" and item_capability and item_capability != capability_id:
                raise ValueError(f"proposal item outside capability closure: {item.get('id')}")
            if item_type in {"operation", "operation_field", "operation_contract", "operation_variant", "field_mapping", "capability_implementation"}:
                operation_id = str(payload.get("operation_id") or "")
                if operation_id and operation_id not in allowed_operation_ids:
                    raise ValueError(f"proposal item outside operation closure: {item.get('id')}")
            if item_type == "operation_contract":
                allowed_semantic_type_ids.update(_semantic_types_from_payload_contract(payload.get("request")))
                allowed_semantic_type_ids.update(_semantic_types_from_payload_contract(payload.get("response")))
        allowed_semantic_type_ids.update(
            str(payload.get("semantic_type_id") or "")
            for item, payload in zip(items, payloads)
            if item.get("item_type") == "field_mapping" and payload.get("semantic_type_id")
        )
        for item, payload in zip(items, payloads):
            item_type = item.get("item_type")
            if item_type == "semantic_type" and str(payload.get("id") or "") not in allowed_semantic_type_ids:
                raise ValueError(f"proposal semantic type outside capability closure: {item.get('id')}")
            if item_type == "field_mapping" and str(payload.get("semantic_type_id") or "") not in allowed_semantic_type_ids:
                raise ValueError(f"proposal mapping outside semantic closure: {item.get('id')}")
        self._validate_capability_graph_integrity(items, payloads, capability_id, allowed_operation_ids, allowed_semantic_type_ids)

    def _validate_capability_graph_integrity(
        self,
        items: list[dict[str, Any]],
        payloads: list[dict[str, Any]],
        capability_id: str,
        operation_ids: set[str],
        semantic_type_ids: set[str],
    ) -> None:
        resources = {
            str(payload.get("id") or "")
            for item, payload in zip(items, payloads)
            if item.get("item_type") == "resource"
        }
        operations = {
            str(payload.get("operation_id") or "")
            for item, payload in zip(items, payloads)
            if item.get("item_type") == "operation"
        }
        operation_fields = {
            str(payload.get("id") or "")
            for item, payload in zip(items, payloads)
            if item.get("item_type") == "operation_field"
        }
        variants = {
            str(payload.get("variant_id") or "")
            for item, payload in zip(items, payloads)
            if item.get("item_type") == "operation_variant"
        }
        resources.discard("")
        operations.discard("")
        operation_fields.discard("")
        variants.discard("")
        if not operation_ids:
            raise ValueError(f"capability proposal has no executable operation: {capability_id}")
        for item, payload in zip(items, payloads):
            item_type = item.get("item_type")
            item_id = item.get("id")
            if item_type == "operation" and str(payload.get("operation_id") or "") not in operation_ids:
                raise ValueError(f"operation is not used by capability: {item_id}")
            if item_type == "operation" and payload.get("resource_id") and str(payload.get("resource_id")) not in resources:
                raise ValueError(f"operation references missing resource: {item_id}")
            if item_type == "operation_field" and str(payload.get("operation_id") or "") not in operations:
                raise ValueError(f"operation field references missing operation: {item_id}")
            if item_type == "operation_contract":
                if str(payload.get("operation_id") or "") not in operations:
                    raise ValueError(f"contract references missing operation: {item_id}")
                if payload.get("resource_id") and str(payload.get("resource_id")) not in resources:
                    raise ValueError(f"contract references missing resource: {item_id}")
            if item_type == "operation_variant":
                evidence = item.get("evidence") if isinstance(item.get("evidence"), dict) else {}
                verification = evidence.get("verification") if isinstance(evidence.get("verification"), dict) else {}
                if str(payload.get("operation_id") or "") not in operations:
                    raise ValueError(f"variant references missing operation: {item_id}")
                if str(payload.get("capability_id") or payload.get("capability") or "") != capability_id:
                    raise ValueError(f"variant references wrong capability: {item_id}")
                if verification.get("status") != "passed":
                    raise ValueError(f"variant verification is not passed: {item_id}")
            if item_type == "capability_implementation":
                if str(payload.get("operation_id") or "") not in operations:
                    raise ValueError(f"implementation references missing operation: {item_id}")
                if payload.get("variant_id") and str(payload.get("variant_id")) not in variants:
                    raise ValueError(f"implementation references missing variant: {item_id}")
            if item_type == "field_mapping":
                if str(payload.get("operation_id") or "") not in operations:
                    raise ValueError(f"mapping references missing operation: {item_id}")
                if payload.get("operation_field_id") and str(payload.get("operation_field_id")) not in operation_fields:
                    raise ValueError(f"mapping references missing operation field: {item_id}")
                if str(payload.get("semantic_type_id") or "") not in semantic_type_ids:
                    raise ValueError(f"mapping references missing semantic type: {item_id}")

    def reject_proposal(self, proposal_id: str) -> dict[str, Any]:
        self.ensure_schema()
        with self.connect() as conn:
            result = conn.execute(
                """
                update sp_proposals set status = 'rejected', reviewed_at = now()
                where id = %s
                returning id, status
                """,
                (proposal_id,),
            ).fetchone()
            if not result:
                raise FileNotFoundError(proposal_id)
            conn.execute("update sp_proposal_items set status = 'rejected' where proposal_id = %s", (proposal_id,))
            conn.commit()
            return dict(result)

    def catalog(self) -> dict[str, Any]:
        self.ensure_schema()
        with self.connect() as conn:
            return self._catalog_from_conn(conn)

    def _catalog_from_conn(self, conn: Any) -> dict[str, Any]:
        capabilities = _rows_by_id(
            conn.execute("select * from sp_capabilities where status in ('active', 'approved') order by id").fetchall()
        )
        capability_ids = set(capabilities)
        capability_documents = _rows_by_id(
            conn.execute(
                """
                select * from sp_capability_documents
                where status in ('active', 'approved') and capability_id = any(%s)
                order by id
                """,
                (list(capability_ids),),
            ).fetchall()
        )
        operation_variants = _rows_by_id(
            conn.execute(
                """
                select * from sp_operation_variants
                where status = 'approved' and capability_id = any(%s)
                order by variant_id
                """,
                (list(capability_ids),),
            ).fetchall(),
            key="variant_id",
        )
        capability_implementations = [
            dict(row)
            for row in conn.execute(
                """
                select * from sp_capability_implementations
                where capability_id = any(%s) and status in ('active', 'approved', 'planned')
                order by capability_id, operation_id
                """,
                (list(capability_ids),),
            ).fetchall()
        ]
        operation_ids = {
            str(item.get("operation_id") or "")
            for item in [*operation_variants.values(), *capability_implementations]
            if item.get("operation_id")
        }
        operation_contracts = _rows_by_id(
            conn.execute(
                """
                select * from sp_operation_contracts
                where status = 'approved'
                  and (capability_id = any(%s) or operation_id = any(%s))
                order by operation_id
                """,
                (list(capability_ids), list(operation_ids)),
            ).fetchall(),
            key="operation_id",
        )
        operation_ids.update(str(item.get("operation_id") or "") for item in operation_contracts.values())
        operations = _rows_by_id(
            conn.execute(
                """
                select * from sp_operations
                where status in ('active', 'approved') and operation_id = any(%s)
                order by operation_id
                """,
                (list(operation_ids),),
            ).fetchall(),
            key="operation_id",
        )
        resource_ids = {
            str(item.get("resource_id") or "")
            for item in [*operations.values(), *operation_contracts.values()]
            if item.get("resource_id")
        }
        semantic_type_ids = {
            str(value)
            for capability in capabilities.values()
            for value in [*_json_list(capability.get("inputs")), *_json_list(capability.get("outputs"))]
        }
        for contract in operation_contracts.values():
            semantic_type_ids.update(_semantic_types_from_payload_contract(contract.get("request")))
            semantic_type_ids.update(_semantic_types_from_payload_contract(contract.get("response")))
        field_mappings = _rows_by_id(
            conn.execute(
                """
                select * from sp_field_mappings
                where status = 'approved'
                  and operation_id = any(%s)
                  and semantic_type_id = any(%s)
                order by id
                """,
                (list(operation_ids), list(semantic_type_ids)),
            ).fetchall()
        )
        semantic_type_ids.update(str(row.get("semantic_type_id") or "") for row in field_mappings.values())
        semantic_type_ids.discard("")
        capability_entity_links = _rows_by_id(
            conn.execute(
                """
                select * from sp_capability_entity_links
                where status in ('active', 'approved') and capability_id = any(%s)
                order by capability_id, role, entity_id
                """,
                (list(capability_ids),),
            ).fetchall()
        )
        capability_dependencies = _rows_by_id(
            conn.execute(
                """
                select * from sp_capability_dependencies
                where status in ('active', 'approved') and capability_id = any(%s)
                order by capability_id, dependency_type, depends_on_capability_id
                """,
                (list(capability_ids),),
            ).fetchall()
        )
        entity_ids = {
            str(row.get("entity_id") or "")
            for row in capability_entity_links.values()
            if row.get("entity_id")
        }
        semantic_type_ids.update(
            str(row.get("semantic_type_id") or "")
            for row in capability_entity_links.values()
            if row.get("semantic_type_id")
        )
        semantic_type_ids.update(
            str(row.get("semantic_type_id") or "")
            for row in capability_dependencies.values()
            if row.get("semantic_type_id")
        )
        operation_field_ids = {
            str(row.get("operation_field_id") or "")
            for row in field_mappings.values()
            if row.get("operation_field_id")
        }
        resources = _rows_by_id(
            conn.execute(
                """
                select * from sp_resources
                where status in ('active', 'approved') and id = any(%s)
                order by id
                """,
                (list(resource_ids),),
            ).fetchall()
        )
        semantic_types = _rows_by_id(
            conn.execute(
                """
                select * from sp_semantic_types
                where status in ('active', 'approved') and id = any(%s)
                order by id
                """,
                (list(semantic_type_ids),),
            ).fetchall()
        )
        entity_identifiers = _rows_by_id(
            conn.execute(
                """
                select * from sp_entity_identifiers
                where status in ('active', 'approved')
                  and (
                    (%s::text[] <> '{}'::text[] and entity_id = any(%s))
                    or (%s::text[] <> '{}'::text[] and semantic_type_id = any(%s))
                  )
                order by entity_id, semantic_type_id
                """,
                (list(entity_ids), list(entity_ids), list(semantic_type_ids), list(semantic_type_ids)),
            ).fetchall()
        )
        entity_ids.update(
            str(row.get("entity_id") or "")
            for row in entity_identifiers.values()
            if row.get("entity_id")
        )
        entities = _rows_by_id(
            conn.execute(
                """
                select * from sp_entities
                where status in ('active', 'approved') and id = any(%s)
                order by id
                """,
                (list(entity_ids),),
            ).fetchall()
        )
        semantic_join_rules = _rows_by_id(
            conn.execute(
                """
                select * from sp_semantic_join_rules
                where status in ('active', 'approved')
                  and (
                    from_semantic_type_id = any(%s)
                    or to_semantic_type_id = any(%s)
                    or from_entity_id = any(%s)
                    or to_entity_id = any(%s)
                  )
                order by id
                """,
                (list(semantic_type_ids), list(semantic_type_ids), list(entity_ids), list(entity_ids)),
            ).fetchall()
        )
        planning_examples = _rows_by_id(
            conn.execute(
                """
                select * from sp_planning_examples
                where status in ('active', 'approved')
                  and (%s::text[] = '{}'::text[] or expected_capability_ids && %s::text[])
                order by updated_at desc, id
                limit 200
                """,
                (list(capability_ids), list(capability_ids)),
            ).fetchall()
        )
        operation_fields = [
            dict(row)
            for row in conn.execute(
                """
                select * from sp_operation_fields
                where operation_id = any(%s)
                  and (%s::text[] = '{}'::text[] or id = any(%s))
                order by operation_id, direction, raw_name
                """,
                (list(operation_ids), list(operation_field_ids), list(operation_field_ids)),
            ).fetchall()
        ]
        return {
            "semantic_types": semantic_types,
            "entities": entities,
            "entity_identifiers": entity_identifiers,
            "semantic_join_rules": semantic_join_rules,
            "capabilities": capabilities,
            "capability_documents": capability_documents,
            "capability_entity_links": capability_entity_links,
            "capability_dependencies": capability_dependencies,
            "planning_examples": planning_examples,
            "resources": resources,
            "operations": operations,
            "operation_fields": operation_fields,
            "operation_variants": operation_variants,
            "field_mappings": field_mappings,
            "operation_contracts": operation_contracts,
            "capability_implementations": capability_implementations,
        }

    def catalog_section(self, section: str, limit: int = 100, offset: int = 0, q: str | None = None) -> dict[str, Any]:
        self.ensure_schema()
        specs = _catalog_section_specs()
        if section not in specs:
            raise KeyError(section)
        spec = specs[section]
        limit = max(1, min(int(limit), 500))
        offset = max(0, int(offset))
        table = spec["table"]
        alias = spec["alias"]
        key = spec["key"]
        order_by = spec["order_by"]
        search = str(q or "").strip()
        where = list(spec["where"])
        params: list[Any] = []
        if search:
            where.append(f"({alias}.{key} ilike %s or to_jsonb({alias})::text ilike %s)")
            like = f"%{search}%"
            params.extend([like, like])
        where_sql = " and ".join(where) if where else "true"
        with self.connect() as conn:
            total = conn.execute(f"select count(*) as count from {table} {alias} where {where_sql}", params).fetchone()
            rows = conn.execute(
                f"""
                select {alias}.*
                from {table} {alias}
                where {where_sql}
                order by {order_by}
                limit %s offset %s
                """,
                [*params, limit, offset],
            ).fetchall()
        total_count = int(total["count"] if total else 0)
        items = [{"id": str(row[key]), "value": dict(row)} for row in rows]
        next_offset = offset + limit if offset + limit < total_count else None
        return {
            "section": section,
            "limit": limit,
            "offset": offset,
            "next_offset": next_offset,
            "total": total_count,
            "items": items,
        }

    def update_catalog_item(self, section: str, item_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        self.ensure_schema()
        allowed = _governance_catalog_specs()
        if section not in allowed:
            raise KeyError(section)
        spec = allowed[section]
        key = spec["key"]
        document = dict(payload)
        document[key] = item_id
        with self.connect() as conn:
            existing = _catalog_item_row(conn, spec, item_id)
            if not existing:
                raise FileNotFoundError(item_id)
            spec["apply"](self, conn, document)
            updated = _catalog_item_row(conn, spec, item_id)
            conn.commit()
        return {"section": section, "id": item_id, "item": updated, "catalog_version": None}

    def catalog_item_delete_plan(self, section: str, item_id: str) -> dict[str, Any]:
        self.ensure_schema()
        allowed = _governance_catalog_specs()
        if section not in allowed:
            raise KeyError(section)
        spec = allowed[section]
        with self.connect() as conn:
            existing = _catalog_item_row(conn, spec, item_id)
            if not existing:
                raise FileNotFoundError(item_id)
            blockers = _catalog_delete_blockers(conn, section, item_id)
        blocker_count = sum(int(item.get("count") or 0) for item in blockers)
        modes = []
        if spec.get("deprecate"):
            modes.append({"mode": "deprecate", "label": "Deprecate", "destructive": False})
        if blocker_count == 0:
            modes.append({"mode": "delete", "label": "Delete", "destructive": True})
        return {
            "section": section,
            "id": item_id,
            "item": existing,
            "blockers": blockers,
            "blocker_count": blocker_count,
            "modes": modes,
            "default_mode": "deprecate" if spec.get("deprecate") else "delete",
        }

    def delete_catalog_item(self, section: str, item_id: str, *, mode: str = "deprecate") -> dict[str, Any]:
        self.ensure_schema()
        allowed = _governance_catalog_specs()
        if section not in allowed:
            raise KeyError(section)
        spec = allowed[section]
        mode = str(mode or ("deprecate" if spec.get("deprecate") else "delete"))
        plan = self.catalog_item_delete_plan(section, item_id)
        blocker_count = int(plan.get("blocker_count") or 0)
        if mode == "deprecate":
            if not spec.get("deprecate"):
                raise ValueError(f"{section} does not support deprecate")
            with self.connect() as conn:
                conn.execute(
                    f"update {spec['table']} set status = 'deprecated', updated_at = now() where {spec['key']} = %s",
                    (item_id,),
                )
                conn.commit()
            return {
                "section": section,
                "id": item_id,
                "mode": "deprecate",
                "status": "deprecated",
                "plan": plan,
                "catalog_version": None,
            }
        if mode == "delete":
            if blocker_count:
                raise ValueError("catalog item has dependent objects; deprecate it or remove dependencies first")
            with self.connect() as conn:
                conn.execute(f"delete from {spec['table']} where {spec['key']} = %s", (item_id,))
                conn.commit()
            return {
                "section": section,
                "id": item_id,
                "mode": "delete",
                "status": "deleted",
                "plan": plan,
                "catalog_version": None,
            }
        raise ValueError("mode must be deprecate or delete")

    def execution_contracts(self) -> dict[str, Any]:
        catalog = self.catalog()
        implementations: dict[str, list[dict[str, Any]]] = {}
        for item in catalog["capability_implementations"]:
            implementations.setdefault(item["capability_id"], []).append(
                {
                    "operation_id": item["operation_id"],
                    "variant_id": item.get("variant_id"),
                    "resource_id": catalog["operation_contracts"].get(item["operation_id"], {}).get("resource_id"),
                    "provider": catalog["operation_contracts"].get(item["operation_id"], {}).get("provider"),
                    "tool": item.get("tool"),
                    "status": item.get("status"),
                    **(item.get("metadata") or {}),
                }
            )
        field_mappings = {
            row["id"]: {
                "operation_id": row["operation_id"],
                "field_name": row["raw_name"],
                "direction": row["direction"],
                "semantic_type": row["semantic_type_id"],
                "confidence": float(row["confidence"]) if row.get("confidence") is not None else None,
            }
            for row in catalog["field_mappings"].values()
        }
        operation_contracts = {}
        for operation_id, contract in catalog["operation_contracts"].items():
            operation_contracts[operation_id] = {
                "capability": contract.get("capability_id"),
                "resource_id": contract.get("resource_id"),
                "provider": contract.get("provider"),
                "method": contract.get("method"),
                "path": contract.get("path"),
                "auth": contract.get("auth") or {},
                "request": contract.get("request") or {},
                "response": contract.get("response") or {},
                "selectors": contract.get("selectors") or {},
            }
        return {
            "capability_implementations": implementations,
            "operation_field_mappings": field_mappings,
            "operation_contracts": operation_contracts,
            "operation_variants": {
                variant_id: {
                    "variant_id": variant_id,
                    "operation_id": variant.get("operation_id"),
                    "capability": variant.get("capability_id"),
                    "name": variant.get("name"),
                    "fixed_semantic_arguments": variant.get("fixed_semantic_arguments") or {},
                    "fixed_raw_arguments": variant.get("fixed_raw_arguments") or {},
                    "verification": variant.get("verification") or {},
                }
                for variant_id, variant in catalog["operation_variants"].items()
            },
            "resources": {
                resource_id: {
                    "id": resource_id,
                    "provider": resource.get("provider"),
                    "name_ko": resource.get("name_ko"),
                    "base_url": resource.get("base_url"),
                }
                for resource_id, resource in catalog["resources"].items()
            },
        }

    def proposals(
        self,
        *,
        status: str | None = None,
        limit: int = 100,
        offset: int = 0,
        include_payload: bool = False,
    ) -> dict[str, Any]:
        self.ensure_schema()
        limit = max(1, min(int(limit), 500))
        offset = max(0, int(offset))
        where = ["true"]
        params: list[Any] = []
        if status:
            where.append("proposal.status = %s")
            params.append(status)
        where_sql = " and ".join(where)
        payload_sql = ", proposal.payload" if include_payload else ""
        with self.connect() as conn:
            total = conn.execute(
                f"select count(*)::int as count from sp_proposals proposal where {where_sql}",
                params,
            ).fetchone()["count"]
            proposals = conn.execute(
                f"""
                select proposal.id, proposal.source_document_id, proposal.kind, proposal.status,
                       proposal.created_by, proposal.created_at, proposal.reviewed_at
                       {payload_sql},
                       count(item.id)::int as item_count
                from sp_proposals proposal
                left join sp_proposal_items item on item.proposal_id = proposal.id
                where {where_sql}
                group by proposal.id
                order by proposal.created_at desc
                limit %s offset %s
                """
                ,
                [*params, limit, offset],
            ).fetchall()
            return {
                "proposals": [dict(row) for row in proposals],
                "total": int(total),
                "limit": limit,
                "offset": offset,
                "next_offset": offset + limit if offset + limit < int(total) else None,
            }

    def proposal(self, proposal_id: str) -> dict[str, Any]:
        self.ensure_schema()
        with self.connect() as conn:
            proposal = conn.execute("select * from sp_proposals where id = %s", (proposal_id,)).fetchone()
            if not proposal:
                raise FileNotFoundError(proposal_id)
            items = conn.execute(
                "select * from sp_proposal_items where proposal_id = %s order by id",
                (proposal_id,),
            ).fetchall()
            return {"proposal": dict(proposal), "items": [dict(row) for row in items]}

    def update_proposal_item(
        self,
        proposal_id: str,
        item_id: str,
        *,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self.ensure_schema()
        with self.connect() as conn:
            proposal = conn.execute("select * from sp_proposals where id = %s", (proposal_id,)).fetchone()
            if not proposal:
                raise FileNotFoundError(proposal_id)
            if proposal["status"] != "pending_review":
                raise ValueError("only pending_review proposals can be edited")
            item = conn.execute(
                "select * from sp_proposal_items where proposal_id = %s and id = %s",
                (proposal_id, item_id),
            ).fetchone()
            if not item:
                raise FileNotFoundError(item_id)
            current = dict(item)
            if current.get("status") != "pending_review":
                raise ValueError("only pending_review proposal items can be edited")
            next_payload = payload if payload is not None else current.get("payload", {})
            self._validate_proposal_item_payload_edit(current, next_payload)
            updated = conn.execute(
                """
                update sp_proposal_items
                set payload = %s
                where proposal_id = %s and id = %s
                returning *
                """,
                (_jsonb(next_payload), proposal_id, item_id),
            ).fetchone()
            conn.commit()
            return {"proposal": dict(proposal), "item": dict(updated)}

    def _validate_proposal_item_payload_edit(self, item: dict[str, Any], payload: dict[str, Any]) -> None:
        if not payload or not isinstance(payload, dict):
            raise ValueError("payload must be a JSON object")
        item_type = str(item.get("item_type") or "")
        current_payload = item.get("payload") or {}
        editable_paths = _proposal_editable_payload_paths(item_type)
        if not editable_paths:
            if not _json_equal(current_payload, payload):
                raise ValueError(f"proposal item type is read-only: {item_type}")
            return
        identity_key = _proposal_payload_identity_key(item_type)
        if identity_key:
            current_identity = str(current_payload.get(identity_key) or "")
            next_identity = str(payload.get(identity_key) or "")
            if current_identity and next_identity != current_identity:
                raise ValueError(f"payload identity field is read-only: {identity_key}")
        target_id = str(item.get("target_id") or "")
        next_identity = str(payload.get(identity_key) or "") if identity_key else ""
        if target_id and next_identity and next_identity != target_id:
            raise ValueError("payload identity must match proposal item target_id")
        changed_paths = _json_changed_paths(current_payload, payload)
        blocked_paths = [
            ".".join(path)
            for path in changed_paths
            if not _path_allowed(path, editable_paths)
        ]
        if blocked_paths:
            raise ValueError(f"proposal payload fields are read-only: {', '.join(blocked_paths)}")

    def sources(self) -> dict[str, Any]:
        self.ensure_schema()
        with self.connect() as conn:
            registry_rows = conn.execute(
                """
                select source.*,
                       revision.id as revision_id,
                       revision.revision_number,
                       revision.file_name,
                       revision.content_type,
                       revision.size_bytes,
                       revision.sha256,
                       revision.object_uri,
                       revision.created_at as revision_created_at
                from sp_sources source
                left join sp_source_revisions revision on revision.id = source.current_revision_id
                order by source.updated_at desc
                """
            ).fetchall()
            legacy_rows = conn.execute(
                """
                select document.*
                from sp_source_documents document
                where not exists (
                    select 1
                    from sp_sources source
                    where source.id = document.id
                )
                order by document.updated_at desc
                """
            ).fetchall()
            sources = [
                {
                    **dict(row),
                    "source_kind": "registry",
                    "catalog_status": self._catalog_status_for_source(conn, row["id"], row.get("sha256")),
                }
                for row in registry_rows
            ]
            sources.extend(
                {
                    **dict(row),
                    "title": (row.get("metadata") or {}).get("title") or row.get("file_name") or row.get("path"),
                    "source_kind": "legacy_document",
                    "catalog_status": self._catalog_status_for_source(conn, row["id"], row.get("sha256")),
                }
                for row in legacy_rows
            )
            return {"sources": sources}

    def _catalog_status_for_source(self, conn: Any, source_id: str, sha256: str | None = None) -> str:
        document = conn.execute("select sha256 from sp_source_documents where id = %s", (source_id,)).fetchone()
        if document and sha256 and document.get("sha256") != sha256:
            return "stale"
        proposal_statuses = conn.execute(
            """
            select proposal.status, count(proposal.id)::int as count
            from sp_proposals proposal
            where proposal.source_document_id = %s
              and exists (
                select 1
                from sp_proposal_items item
                where item.proposal_id = proposal.id
              )
            group by proposal.status
            """,
            (source_id,),
        ).fetchall()
        if not proposal_statuses:
            empty_proposal_count = conn.execute(
                """
                select count(*)::int as count
                from sp_proposals proposal
                where proposal.source_document_id = %s
                  and not exists (
                    select 1
                    from sp_proposal_items item
                    where item.proposal_id = proposal.id
                  )
                """,
                (source_id,),
            ).fetchone()["count"]
            if int(empty_proposal_count or 0) > 0:
                return "ingestion_empty"
            return "not_ingested"
        counts = {str(row["status"]): int(row["count"]) for row in proposal_statuses}
        if counts.get("pending_review") or counts.get("planned"):
            return "proposal_pending"
        active_capabilities = conn.execute(
            """
            select count(*)::int as count
            from sp_capabilities
            where provenance->>'source_document_id' = %s
              and status in ('active', 'approved')
            """,
            (source_id,),
        ).fetchone()["count"]
        if int(active_capabilities) == 0:
            return "proposal_rejected"
        try:
            embedded = conn.execute(
                """
                select count(*)::int as count
                from sp_capability_document_vectors vector
                join sp_capability_documents document on document.id = vector.document_id
                join sp_capabilities capability on capability.id = document.capability_id
                where capability.provenance->>'source_document_id' = %s
                  and capability.status in ('active', 'approved')
                """,
                (source_id,),
            ).fetchone()["count"]
        except Exception:
            conn.rollback()
            embedded = 0
        return "ready" if int(embedded) > 0 else "cataloged"

    def source_ingestion_status(self, source_document_id: str, sha256: str | None = None) -> dict[str, Any]:
        self.ensure_schema()
        with self.connect() as conn:
            document = conn.execute(
                "select * from sp_source_documents where id = %s",
                (source_document_id,),
            ).fetchone()
            if not document:
                return {
                    "source_document_id": source_document_id,
                    "exists": False,
                    "same_sha256": False,
                    "processed": False,
                    "proposal_count": 0,
                    "evidence_snapshot_count": 0,
                    "chunk_count": 0,
                }
            proposal_count = conn.execute(
                "select count(*) as count from sp_proposals where source_document_id = %s",
                (source_document_id,),
            ).fetchone()["count"]
            proposal_item_count = conn.execute(
                """
                select count(*) as count
                from sp_proposal_items item
                join sp_proposals proposal on proposal.id = item.proposal_id
                where proposal.source_document_id = %s
                """,
                (source_document_id,),
            ).fetchone()["count"]
            evidence_snapshot_count = conn.execute(
                "select count(*) as count from sp_source_evidence_snapshots where source_document_id = %s",
                (source_document_id,),
            ).fetchone()["count"]
            chunk_count = conn.execute(
                "select count(*) as count from sp_source_chunks where source_document_id = %s",
                (source_document_id,),
            ).fetchone()["count"]
            same_sha256 = sha256 is None or document.get("sha256") == sha256
            metadata = document.get("metadata") if isinstance(document.get("metadata"), dict) else {}
            same_graph_version = metadata.get("ingestion_graph_version") == INGESTION_GRAPH_VERSION
            same_prompt_version = metadata.get("ingestion_prompt_version") == INGESTION_PROMPT_VERSION
            same_embedding_model = metadata.get("embedding_model") == embedding_model()
            return {
                "source_document_id": source_document_id,
                "exists": True,
                "same_sha256": same_sha256,
                "same_graph_version": same_graph_version,
                "same_prompt_version": same_prompt_version,
                "same_embedding_model": same_embedding_model,
                "processed": bool(
                    same_sha256
                    and same_graph_version
                    and same_prompt_version
                    and same_embedding_model
                    and proposal_item_count
                ),
                "proposal_count": proposal_count,
                "proposal_item_count": proposal_item_count,
                "evidence_snapshot_count": evidence_snapshot_count,
                "chunk_count": chunk_count,
                "document": dict(document),
            }

    def meta(self) -> dict[str, Any]:
        self.ensure_schema()
        tables = [
            "sp_sources",
            "sp_source_revisions",
            "sp_secrets",
            "sp_source_documents",
            "sp_source_chunks",
            "sp_source_evidence_snapshots",
            "sp_resources",
            "sp_operations",
            "sp_operation_fields",
            "sp_semantic_types",
            "sp_entities",
            "sp_entity_identifiers",
            "sp_capabilities",
            "sp_capability_entity_links",
            "sp_capability_dependencies",
            "sp_capability_documents",
            "sp_capability_document_vectors",
            "sp_operation_contracts",
            "sp_operation_variants",
            "sp_field_mappings",
            "sp_semantic_join_rules",
            "sp_planning_examples",
            "sp_endpoint_checks",
            "sp_execution_graphs",
            "sp_ingestion_runs",
            "sp_planner_feedback",
            "sp_catalog_versions",
            "sp_proposals",
        ]
        with self.connect() as conn:
            counts = {}
            for table in tables:
                try:
                    counts[table.removeprefix("sp_")] = conn.execute(f"select count(*) as count from {table}").fetchone()["count"]
                except Exception:
                    conn.rollback()
                    counts[table.removeprefix("sp_")] = 0
            return {
                "storage": "postgres",
                "counts": counts,
                "generated_at": _now_iso(),
                "llm": {
                    "mode": llm_mode(),
                    "openai_api_key_configured": bool(os.getenv("OPENAI_API_KEY")),
                },
            }

    def _apply_resource(self, conn: Any, payload: dict[str, Any]) -> None:
        conn.execute(
            """
            insert into sp_resources(id, provider, name_ko, base_url, source_document_id, evidence, status, updated_at)
            values (%s, %s, %s, %s, %s, %s, %s, now())
            on conflict (id) do update set
                provider = excluded.provider,
                name_ko = excluded.name_ko,
                base_url = excluded.base_url,
                source_document_id = excluded.source_document_id,
                evidence = excluded.evidence,
                status = excluded.status,
                updated_at = now()
            """,
            (
                payload["id"],
                payload.get("provider", "unknown"),
                payload.get("name_ko"),
                payload.get("base_url"),
                payload.get("source_document_id"),
                _jsonb(payload.get("evidence", {})),
                _applied_status(payload.get("status")),
            ),
        )

    def _apply_operation(self, conn: Any, payload: dict[str, Any]) -> None:
        conn.execute(
            """
            insert into sp_operations(operation_id, resource_id, provider, method, path, title_ko, description_ko,
                                      source_document_id, source_chunk_id, evidence, status, updated_at)
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
            on conflict (operation_id) do update set
                resource_id = excluded.resource_id,
                provider = excluded.provider,
                method = excluded.method,
                path = excluded.path,
                title_ko = excluded.title_ko,
                description_ko = excluded.description_ko,
                source_document_id = excluded.source_document_id,
                source_chunk_id = excluded.source_chunk_id,
                evidence = excluded.evidence,
                status = excluded.status,
                updated_at = now()
            """,
            (
                payload["operation_id"],
                payload.get("resource_id"),
                payload.get("provider", "unknown"),
                payload.get("method"),
                payload.get("path"),
                payload.get("title_ko"),
                payload.get("description_ko"),
                payload.get("source_document_id"),
                self._existing_source_chunk_id(conn, payload.get("source_chunk_id")),
                _jsonb(payload.get("evidence", {})),
                _applied_status(payload.get("status")),
            ),
        )

    def _apply_operation_field(self, conn: Any, payload: dict[str, Any]) -> None:
        conn.execute(
            """
            insert into sp_operation_fields(id, operation_id, direction, raw_name, location, path, label_ko,
                                            description_ko, example, type_hint, unit_hint, required, evidence, status, updated_at)
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
            on conflict (id) do update set
                location = excluded.location,
                path = excluded.path,
                label_ko = excluded.label_ko,
                description_ko = excluded.description_ko,
                example = excluded.example,
                type_hint = excluded.type_hint,
                unit_hint = excluded.unit_hint,
                required = excluded.required,
                evidence = excluded.evidence,
                status = excluded.status,
                updated_at = now()
            """,
            (
                payload["id"],
                payload["operation_id"],
                payload["direction"],
                payload["raw_name"],
                payload.get("location"),
                payload.get("path"),
                payload.get("label_ko"),
                payload.get("description_ko"),
                payload.get("example"),
                payload.get("type_hint"),
                payload.get("unit_hint"),
                payload.get("required"),
                _jsonb(payload.get("evidence", {})),
                _applied_status(payload.get("status")),
            ),
        )

    def _apply_semantic_type(self, conn: Any, payload: dict[str, Any]) -> None:
        conn.execute(
            """
            insert into sp_semantic_types(id, description_ko, entity, value_kind, unit, canonical_format,
                                          value_shape, value_contract, aliases, status, provenance, updated_at)
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
            on conflict (id) do update set
                description_ko = excluded.description_ko,
                entity = excluded.entity,
                value_kind = excluded.value_kind,
                unit = excluded.unit,
                canonical_format = excluded.canonical_format,
                value_shape = excluded.value_shape,
                value_contract = excluded.value_contract,
                aliases = excluded.aliases,
                status = excluded.status,
                provenance = excluded.provenance,
                updated_at = now()
            """,
            (
                payload["id"],
                payload.get("description_ko"),
                payload.get("entity"),
                payload.get("value_kind"),
                payload.get("unit"),
                payload.get("canonical_format"),
                _jsonb(payload.get("value_shape", {})),
                _jsonb(payload.get("value_contract", {})),
                payload.get("aliases", []),
                _applied_status(payload.get("status") or "active"),
                _jsonb(payload.get("provenance", {})),
            ),
        )

    def _apply_entity(self, conn: Any, payload: dict[str, Any]) -> None:
        conn.execute(
            """
            insert into sp_entities(id, name_ko, description_ko, entity_type, aliases, properties, status, provenance, updated_at)
            values (%s, %s, %s, %s, %s, %s, %s, %s, now())
            on conflict (id) do update set
                name_ko = excluded.name_ko,
                description_ko = excluded.description_ko,
                entity_type = excluded.entity_type,
                aliases = excluded.aliases,
                properties = excluded.properties,
                status = excluded.status,
                provenance = excluded.provenance,
                updated_at = now()
            """,
            (
                payload["id"],
                payload.get("name_ko"),
                payload.get("description_ko"),
                payload.get("entity_type", "entity"),
                payload.get("aliases", []),
                _jsonb(payload.get("properties", {})),
                _applied_status(payload.get("status") or "active"),
                _jsonb(payload.get("provenance", {})),
            ),
        )

    def _apply_entity_identifier(self, conn: Any, payload: dict[str, Any]) -> None:
        conn.execute(
            """
            insert into sp_entity_identifiers(id, entity_id, semantic_type_id, identifier_role,
                                              validation, aliases, status, provenance, updated_at)
            values (%s, %s, %s, %s, %s, %s, %s, %s, now())
            on conflict (id) do update set
                entity_id = excluded.entity_id,
                semantic_type_id = excluded.semantic_type_id,
                identifier_role = excluded.identifier_role,
                validation = excluded.validation,
                aliases = excluded.aliases,
                status = excluded.status,
                provenance = excluded.provenance,
                updated_at = now()
            """,
            (
                payload["id"],
                payload["entity_id"],
                payload["semantic_type_id"],
                payload.get("identifier_role", "identifier"),
                _jsonb(payload.get("validation", {})),
                payload.get("aliases", []),
                _applied_status(payload.get("status") or "active"),
                _jsonb(payload.get("provenance", {})),
            ),
        )

    def _apply_capability(self, conn: Any, payload: dict[str, Any]) -> None:
        conn.execute(
            """
            insert into sp_capabilities(id, description_ko, use_when, inputs, outputs, examples, status, provenance, updated_at)
            values (%s, %s, %s, %s, %s, %s, %s, %s, now())
            on conflict (id) do update set
                description_ko = excluded.description_ko,
                use_when = excluded.use_when,
                inputs = excluded.inputs,
                outputs = excluded.outputs,
                examples = excluded.examples,
                status = excluded.status,
                provenance = excluded.provenance,
                updated_at = now()
            """,
            (
                payload["id"],
                payload.get("description_ko"),
                _jsonb(payload.get("use_when", [])),
                _jsonb(payload.get("inputs", [])),
                _jsonb(payload.get("outputs", [])),
                _jsonb(payload.get("examples", [])),
                _applied_status(payload.get("status") or "active"),
                _jsonb(payload.get("provenance", {})),
            ),
        )

    def _apply_capability_document(self, conn: Any, payload: dict[str, Any]) -> None:
        conn.execute(
            """
            insert into sp_capability_documents(id, capability_id, document_text, aliases, examples,
                                                intent_patterns, semantic_entities, planning_hints,
                                                inputs, outputs, tags, embedding_model, embedding,
                                                vector_status, status, provenance, updated_at)
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
            on conflict (id) do update set
                document_text = excluded.document_text,
                aliases = excluded.aliases,
                examples = excluded.examples,
                intent_patterns = excluded.intent_patterns,
                semantic_entities = excluded.semantic_entities,
                planning_hints = excluded.planning_hints,
                inputs = excluded.inputs,
                outputs = excluded.outputs,
                tags = excluded.tags,
                embedding_model = excluded.embedding_model,
                embedding = excluded.embedding,
                vector_status = excluded.vector_status,
                status = excluded.status,
                provenance = excluded.provenance,
                updated_at = now()
            """,
            (
                payload["id"],
                payload["capability_id"],
                payload["document_text"],
                _jsonb(payload.get("aliases", [])),
                _jsonb(payload.get("examples", [])),
                _jsonb(payload.get("intent_patterns", [])),
                _jsonb(payload.get("semantic_entities", [])),
                _jsonb(payload.get("planning_hints", {})),
                _jsonb(payload.get("inputs", [])),
                _jsonb(payload.get("outputs", [])),
                _jsonb(payload.get("tags", [])),
                payload.get("embedding_model"),
                _jsonb(payload.get("embedding")) if payload.get("embedding") is not None else None,
                payload.get("vector_status", "not_embedded"),
                _applied_status(payload.get("status") or "active"),
                _jsonb(payload.get("provenance", {})),
            ),
        )

    def _apply_capability_entity_link(self, conn: Any, payload: dict[str, Any]) -> None:
        conn.execute(
            """
            insert into sp_capability_entity_links(id, capability_id, entity_id, role, semantic_type_id,
                                                   required, status, evidence, updated_at)
            values (%s, %s, %s, %s, %s, %s, %s, %s, now())
            on conflict (id) do update set
                capability_id = excluded.capability_id,
                entity_id = excluded.entity_id,
                role = excluded.role,
                semantic_type_id = excluded.semantic_type_id,
                required = excluded.required,
                status = excluded.status,
                evidence = excluded.evidence,
                updated_at = now()
            """,
            (
                payload["id"],
                payload["capability_id"],
                payload["entity_id"],
                payload["role"],
                payload.get("semantic_type_id"),
                payload.get("required"),
                _applied_status(payload.get("status") or "active"),
                _jsonb(payload.get("evidence", {})),
            ),
        )

    def _apply_capability_dependency(self, conn: Any, payload: dict[str, Any]) -> None:
        conn.execute(
            """
            insert into sp_capability_dependencies(id, capability_id, depends_on_capability_id, dependency_type,
                                                   semantic_type_id, status, evidence, updated_at)
            values (%s, %s, %s, %s, %s, %s, %s, now())
            on conflict (id) do update set
                capability_id = excluded.capability_id,
                depends_on_capability_id = excluded.depends_on_capability_id,
                dependency_type = excluded.dependency_type,
                semantic_type_id = excluded.semantic_type_id,
                status = excluded.status,
                evidence = excluded.evidence,
                updated_at = now()
            """,
            (
                payload["id"],
                payload["capability_id"],
                payload["depends_on_capability_id"],
                payload.get("dependency_type", "requires"),
                payload.get("semantic_type_id"),
                _applied_status(payload.get("status") or "active"),
                _jsonb(payload.get("evidence", {})),
            ),
        )

    def _apply_operation_contract(self, conn: Any, payload: dict[str, Any]) -> None:
        conn.execute(
            """
            insert into sp_operation_contracts(operation_id, capability_id, resource_id, provider, method, path,
                                               auth, request, response, selectors, status, provenance, updated_at)
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
            on conflict (operation_id) do update set
                capability_id = excluded.capability_id,
                resource_id = excluded.resource_id,
                provider = excluded.provider,
                method = excluded.method,
                path = excluded.path,
                auth = excluded.auth,
                request = excluded.request,
                response = excluded.response,
                selectors = excluded.selectors,
                status = excluded.status,
                provenance = excluded.provenance,
                updated_at = now()
            """,
            (
                payload["operation_id"],
                payload.get("capability_id") or payload.get("capability"),
                payload.get("resource_id"),
                payload.get("provider", "unknown"),
                payload.get("method"),
                payload.get("path"),
                _jsonb(payload.get("auth", {})),
                _jsonb(payload.get("request", {})),
                _jsonb(payload.get("response", {})),
                _jsonb(payload.get("selectors", {})),
                _applied_status(payload.get("status")),
                _jsonb(payload.get("provenance", {})),
            ),
        )

    def _apply_operation_variant(self, conn: Any, payload: dict[str, Any]) -> None:
        conn.execute(
            """
            insert into sp_operation_variants(variant_id, operation_id, capability_id, name,
                                              fixed_semantic_arguments, fixed_raw_arguments,
                                              verification, status, provenance, updated_at)
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, now())
            on conflict (variant_id) do update set
                operation_id = excluded.operation_id,
                capability_id = excluded.capability_id,
                name = excluded.name,
                fixed_semantic_arguments = excluded.fixed_semantic_arguments,
                fixed_raw_arguments = excluded.fixed_raw_arguments,
                verification = excluded.verification,
                status = excluded.status,
                provenance = excluded.provenance,
                updated_at = now()
            """,
            (
                payload["variant_id"],
                payload["operation_id"],
                payload.get("capability_id") or payload.get("capability"),
                payload.get("name"),
                _jsonb(payload.get("fixed_semantic_arguments", {})),
                _jsonb(payload.get("fixed_raw_arguments", {})),
                _jsonb(payload.get("verification", {})),
                _applied_status(payload.get("status")),
                _jsonb(payload.get("provenance", {})),
            ),
        )

    def _apply_field_mapping(self, conn: Any, payload: dict[str, Any]) -> None:
        conn.execute(
            """
            insert into sp_field_mappings(id, operation_field_id, operation_id, direction, raw_name, semantic_type_id,
                                          confidence, status, evidence, updated_at)
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, now())
            on conflict (id) do update set
                operation_field_id = excluded.operation_field_id,
                semantic_type_id = excluded.semantic_type_id,
                confidence = excluded.confidence,
                status = excluded.status,
                evidence = excluded.evidence,
                updated_at = now()
            """,
            (
                payload["id"],
                payload.get("operation_field_id"),
                payload["operation_id"],
                payload["direction"],
                payload["raw_name"],
                payload["semantic_type_id"],
                payload.get("confidence"),
                _applied_status(payload.get("status")),
                _jsonb(payload.get("evidence", {})),
            ),
        )

    def _apply_capability_implementation(self, conn: Any, payload: dict[str, Any]) -> None:
        conn.execute(
            """
            insert into sp_capability_implementations(id, capability_id, operation_id, variant_id, tool, status, metadata, updated_at)
            values (%s, %s, %s, %s, %s, %s, %s, now())
            on conflict (id) do update set
                variant_id = excluded.variant_id,
                tool = excluded.tool,
                status = excluded.status,
                metadata = excluded.metadata,
                updated_at = now()
            """,
            (
                payload["id"],
                payload["capability_id"],
                payload["operation_id"],
                payload.get("variant_id"),
                payload.get("tool"),
                _applied_status(payload.get("status")),
                _jsonb(payload.get("metadata", {})),
            ),
        )

    def _apply_semantic_join_rule(self, conn: Any, payload: dict[str, Any]) -> None:
        conn.execute(
            """
            insert into sp_semantic_join_rules(id, from_entity_id, from_semantic_type_id,
                                               to_entity_id, to_semantic_type_id, relation,
                                               transform, confidence, status, evidence, updated_at)
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
            on conflict (id) do update set
                from_entity_id = excluded.from_entity_id,
                from_semantic_type_id = excluded.from_semantic_type_id,
                to_entity_id = excluded.to_entity_id,
                to_semantic_type_id = excluded.to_semantic_type_id,
                relation = excluded.relation,
                transform = excluded.transform,
                confidence = excluded.confidence,
                status = excluded.status,
                evidence = excluded.evidence,
                updated_at = now()
            """,
            (
                payload["id"],
                payload.get("from_entity_id"),
                payload["from_semantic_type_id"],
                payload.get("to_entity_id"),
                payload["to_semantic_type_id"],
                payload.get("relation", "joinable_with"),
                _jsonb(payload.get("transform", {})),
                payload.get("confidence"),
                _applied_status(payload.get("status") or "active"),
                _jsonb(payload.get("evidence", {})),
            ),
        )

    def _apply_planning_example(self, conn: Any, payload: dict[str, Any]) -> None:
        conn.execute(
            """
            insert into sp_planning_examples(id, question, expected_capability_ids,
                                             expected_operation_ids, expected_variant_ids,
                                             expected_arguments, expected_graph, tags,
                                             source, status, provenance, updated_at)
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
            on conflict (id) do update set
                question = excluded.question,
                expected_capability_ids = excluded.expected_capability_ids,
                expected_operation_ids = excluded.expected_operation_ids,
                expected_variant_ids = excluded.expected_variant_ids,
                expected_arguments = excluded.expected_arguments,
                expected_graph = excluded.expected_graph,
                tags = excluded.tags,
                source = excluded.source,
                status = excluded.status,
                provenance = excluded.provenance,
                updated_at = now()
            """,
            (
                payload["id"],
                payload["question"],
                payload.get("expected_capability_ids", []),
                payload.get("expected_operation_ids", []),
                payload.get("expected_variant_ids", []),
                _jsonb(payload.get("expected_arguments", {})),
                _jsonb(payload.get("expected_graph", {})),
                payload.get("tags", []),
                payload.get("source"),
                _applied_status(payload.get("status") or "active"),
                _jsonb(payload.get("provenance", {})),
            ),
        )

    def _insert_lineage(
        self,
        conn: Any,
        item_type: str,
        payload: dict[str, Any],
        proposal_id: str,
        item: dict[str, Any],
    ) -> None:
        evidence = item.get("evidence") or {}
        source_document_id = payload.get("source_document_id") or evidence.get("source_document_id")
        source_chunk_id = self._existing_source_chunk_id(
            conn,
            payload.get("source_chunk_id") or evidence.get("source_chunk_id") or evidence.get("section_id"),
        )
        conn.execute(
            """
            insert into sp_catalog_lineage(catalog_object_type, catalog_object_id, source_document_id,
                                           source_chunk_id, operation_id, operation_field_id, proposal_id,
                                           evidence_type, evidence_text, confidence)
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                item_type,
                item.get("target_id") or payload.get("id") or payload.get("variant_id") or payload.get("operation_id"),
                source_document_id,
                source_chunk_id,
                payload.get("operation_id") or evidence.get("operation_id"),
                payload.get("operation_field_id") or evidence.get("operation_field_id"),
                proposal_id,
                evidence.get("type"),
                evidence.get("text"),
                payload.get("confidence"),
            ),
        )

    def _existing_source_chunk_id(self, conn: Any, source_chunk_id: str | None) -> str | None:
        if not source_chunk_id:
            return None
        candidate = str(source_chunk_id)
        if candidate.startswith("section."):
            candidate = "chunk." + candidate[len("section.") :]
        row = conn.execute("select id from sp_source_chunks where id = %s", (candidate,)).fetchone()
        return str(row["id"]) if row else None


def _rows_by_id(rows: list[dict[str, Any]], key: str = "id") -> dict[str, dict[str, Any]]:
    return {str(row[key]): dict(row) for row in rows}


def _proposal_payload_identity_key(item_type: str) -> str | None:
    return {
        "resource": "id",
        "semantic_type": "id",
        "entity": "id",
        "entity_identifier": "id",
        "capability": "id",
        "capability_entity_link": "id",
        "capability_dependency": "id",
        "capability_document": "id",
        "operation": "operation_id",
        "operation_field": "id",
        "operation_contract": "operation_id",
        "operation_variant": "variant_id",
        "field_mapping": "id",
        "capability_implementation": "id",
        "semantic_join_rule": "id",
        "planning_example": "id",
    }.get(item_type)


def _proposal_capability_ids(items: list[dict[str, Any]], applied: list[dict[str, Any]]) -> list[str]:
    capability_ids = {
        str(item.get("target_id") or "")
        for item in applied
        if item.get("item_type") == "capability" and item.get("target_id")
    }
    for item in items:
        payload = item.get("payload") if isinstance(item.get("payload"), dict) else {}
        evidence = item.get("evidence") if isinstance(item.get("evidence"), dict) else {}
        for value in (
            payload.get("capability_id"),
            payload.get("capability"),
            evidence.get("proposal_capability_id"),
        ):
            if value:
                capability_ids.add(str(value))
    capability_ids.discard("")
    return sorted(capability_ids)


def _proposal_editable_payload_paths(item_type: str) -> set[tuple[str, ...]]:
    return {
        "capability": {
            ("description_ko",),
            ("use_when",),
            ("examples",),
            ("provenance", "aliases"),
            ("provenance", "examples"),
            ("provenance", "intent_patterns"),
            ("provenance", "planning_hints"),
        },
        "operation": {
            ("name_ko",),
            ("summary",),
            ("description_ko",),
        },
        "operation_field": {
            ("label_ko",),
            ("description_ko",),
            ("example",),
            ("type_hint",),
        },
        "operation_contract": {
            ("summary",),
            ("description_ko",),
        },
        "operation_variant": {
            ("name_ko",),
            ("summary",),
            ("description_ko",),
        },
        "field_mapping": {
            ("semantic_type_id",),
            ("transform",),
            ("confidence",),
        },
        "planning_example": {
            ("question",),
            ("expected_arguments",),
            ("expected_graph",),
            ("tags",),
        },
    }.get(item_type, set())


def _json_equal(left: Any, right: Any) -> bool:
    return json.dumps(left, sort_keys=True, ensure_ascii=False, default=str) == json.dumps(
        right,
        sort_keys=True,
        ensure_ascii=False,
        default=str,
    )


def _json_changed_paths(left: Any, right: Any, prefix: tuple[str, ...] = ()) -> list[tuple[str, ...]]:
    if _json_equal(left, right):
        return []
    if isinstance(left, dict) and isinstance(right, dict):
        paths: list[tuple[str, ...]] = []
        for key in sorted(set(left) | set(right)):
            paths.extend(_json_changed_paths(left.get(key), right.get(key), (*prefix, str(key))))
        return paths
    return [prefix]


def _path_allowed(path: tuple[str, ...], allowed_paths: set[tuple[str, ...]]) -> bool:
    return any(path == allowed or path[: len(allowed)] == allowed for allowed in allowed_paths)


def _semantic_catalog_tables() -> list[str]:
    return [
        "sp_proposal_items",
        "sp_proposals",
        "sp_catalog_lineage",
        "sp_endpoint_checks",
        "sp_planner_feedback",
        "sp_execution_graphs",
        "sp_catalog_versions",
        "sp_capability_document_vectors",
        "sp_capability_documents",
        "sp_capability_implementations",
        "sp_field_mappings",
        "sp_operation_variants",
        "sp_operation_contracts",
        "sp_capability_dependencies",
        "sp_capability_entity_links",
        "sp_planning_examples",
        "sp_semantic_join_rules",
        "sp_entity_identifiers",
        "sp_entities",
        "sp_capabilities",
        "sp_semantic_types",
        "sp_operation_fields",
        "sp_operations",
        "sp_resources",
        "sp_source_evidence_snapshots",
        "sp_source_chunks",
        "sp_source_documents",
    ]


def _catalog_section_specs() -> dict[str, dict[str, Any]]:
    return {
        "semantic_types": {
            "table": "sp_semantic_types",
            "alias": "s",
            "key": "id",
            "where": ["s.status in ('active', 'approved')"],
            "order_by": "s.id",
        },
        "entities": {
            "table": "sp_entities",
            "alias": "e",
            "key": "id",
            "where": ["e.status in ('active', 'approved')"],
            "order_by": "e.id",
        },
        "entity_identifiers": {
            "table": "sp_entity_identifiers",
            "alias": "i",
            "key": "id",
            "where": ["i.status in ('active', 'approved')"],
            "order_by": "i.entity_id, i.semantic_type_id",
        },
        "capabilities": {
            "table": "sp_capabilities",
            "alias": "c",
            "key": "id",
            "where": ["c.status in ('active', 'approved')"],
            "order_by": "c.id",
        },
        "capability_entity_links": {
            "table": "sp_capability_entity_links",
            "alias": "l",
            "key": "id",
            "where": ["l.status in ('active', 'approved')"],
            "order_by": "l.capability_id, l.role, l.entity_id",
        },
        "capability_dependencies": {
            "table": "sp_capability_dependencies",
            "alias": "d",
            "key": "id",
            "where": ["d.status in ('active', 'approved')"],
            "order_by": "d.capability_id, d.dependency_type, d.depends_on_capability_id",
        },
        "capability_documents": {
            "table": "sp_capability_documents",
            "alias": "d",
            "key": "id",
            "where": ["d.status in ('active', 'approved')"],
            "order_by": "d.id",
        },
        "resources": {
            "table": "sp_resources",
            "alias": "r",
            "key": "id",
            "where": ["r.status in ('active', 'approved')"],
            "order_by": "r.id",
        },
        "operations": {
            "table": "sp_operations",
            "alias": "o",
            "key": "operation_id",
            "where": ["o.status in ('active', 'approved')"],
            "order_by": "o.operation_id",
        },
        "operation_fields": {
            "table": "sp_operation_fields",
            "alias": "f",
            "key": "id",
            "where": ["f.status in ('active', 'approved')"],
            "order_by": "f.operation_id, f.direction, f.raw_name",
        },
        "operation_contracts": {
            "table": "sp_operation_contracts",
            "alias": "k",
            "key": "operation_id",
            "where": ["k.status = 'approved'"],
            "order_by": "k.operation_id",
        },
        "operation_variants": {
            "table": "sp_operation_variants",
            "alias": "v",
            "key": "variant_id",
            "where": ["v.status = 'approved'"],
            "order_by": "v.variant_id",
        },
        "field_mappings": {
            "table": "sp_field_mappings",
            "alias": "m",
            "key": "id",
            "where": ["m.status = 'approved'"],
            "order_by": "m.id",
        },
        "capability_implementations": {
            "table": "sp_capability_implementations",
            "alias": "i",
            "key": "id",
            "where": ["i.status in ('active', 'approved')"],
            "order_by": "i.capability_id, i.operation_id, i.variant_id",
        },
        "semantic_join_rules": {
            "table": "sp_semantic_join_rules",
            "alias": "j",
            "key": "id",
            "where": ["j.status in ('active', 'approved')"],
            "order_by": "j.id",
        },
        "planning_examples": {
            "table": "sp_planning_examples",
            "alias": "p",
            "key": "id",
            "where": ["p.status in ('active', 'approved')"],
            "order_by": "p.updated_at desc, p.id",
        },
    }


def _governance_catalog_specs() -> dict[str, dict[str, Any]]:
    specs = _catalog_section_specs()
    return {
        "planning_examples": {
            **specs["planning_examples"],
            "deprecate": False,
            "apply": SemanticCatalogRepository._apply_planning_example,
        },
        "capabilities": {
            **specs["capabilities"],
            "deprecate": True,
            "apply": SemanticCatalogRepository._apply_capability,
        },
        "semantic_types": {
            **specs["semantic_types"],
            "deprecate": True,
            "apply": SemanticCatalogRepository._apply_semantic_type,
        },
        "entities": {
            **specs["entities"],
            "deprecate": True,
            "apply": SemanticCatalogRepository._apply_entity,
        },
        "semantic_join_rules": {
            **specs["semantic_join_rules"],
            "deprecate": False,
            "apply": SemanticCatalogRepository._apply_semantic_join_rule,
        },
        "capability_entity_links": {
            **specs["capability_entity_links"],
            "deprecate": False,
            "apply": SemanticCatalogRepository._apply_capability_entity_link,
        },
        "capability_dependencies": {
            **specs["capability_dependencies"],
            "deprecate": False,
            "apply": SemanticCatalogRepository._apply_capability_dependency,
        },
    }


def _catalog_item_row(conn: Any, spec: dict[str, Any], item_id: str) -> dict[str, Any] | None:
    row = conn.execute(
        f"select * from {spec['table']} where {spec['key']} = %s",
        (item_id,),
    ).fetchone()
    return dict(row) if row else None


def _catalog_delete_blockers(conn: Any, section: str, item_id: str) -> list[dict[str, Any]]:
    checks = {
        "capabilities": [
            ("capability_entity_links", "select count(*) as count from sp_capability_entity_links where capability_id = %s and status in ('active', 'approved')"),
            ("capability_dependencies", "select count(*) as count from sp_capability_dependencies where (capability_id = %s or depends_on_capability_id = %s) and status in ('active', 'approved')"),
            ("capability_documents", "select count(*) as count from sp_capability_documents where capability_id = %s and status in ('active', 'approved')"),
            ("operation_contracts", "select count(*) as count from sp_operation_contracts where capability_id = %s and status = 'approved'"),
            ("operation_variants", "select count(*) as count from sp_operation_variants where capability_id = %s and status = 'approved'"),
            ("capability_implementations", "select count(*) as count from sp_capability_implementations where capability_id = %s and status in ('active', 'approved', 'planned')"),
            ("planning_examples", "select count(*) as count from sp_planning_examples where expected_capability_ids && %s::text[] and status in ('active', 'approved')"),
        ],
        "semantic_types": [
            ("entity_identifiers", "select count(*) as count from sp_entity_identifiers where semantic_type_id = %s and status in ('active', 'approved')"),
            ("capability_entity_links", "select count(*) as count from sp_capability_entity_links where semantic_type_id = %s and status in ('active', 'approved')"),
            ("capability_dependencies", "select count(*) as count from sp_capability_dependencies where semantic_type_id = %s and status in ('active', 'approved')"),
            ("field_mappings", "select count(*) as count from sp_field_mappings where semantic_type_id = %s and status = 'approved'"),
            ("semantic_join_rules", "select count(*) as count from sp_semantic_join_rules where (from_semantic_type_id = %s or to_semantic_type_id = %s) and status in ('active', 'approved')"),
        ],
        "entities": [
            ("entity_identifiers", "select count(*) as count from sp_entity_identifiers where entity_id = %s and status in ('active', 'approved')"),
            ("capability_entity_links", "select count(*) as count from sp_capability_entity_links where entity_id = %s and status in ('active', 'approved')"),
            ("semantic_join_rules", "select count(*) as count from sp_semantic_join_rules where (from_entity_id = %s or to_entity_id = %s) and status in ('active', 'approved')"),
        ],
    }
    blockers = []
    for name, sql in checks.get(section, []):
        params: tuple[Any, ...]
        if section == "capabilities" and name == "planning_examples":
            params = ([item_id],)
        elif sql.count("%s") == 2:
            params = (item_id, item_id)
        else:
            params = (item_id,)
        row = conn.execute(sql, params).fetchone()
        count = int(row["count"] if row else 0)
        if count:
            blockers.append({"section": name, "count": count})
    return blockers


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _stable_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str, separators=(",", ":"))


def _catalog_version_snapshot(catalog: dict[str, Any]) -> dict[str, Any]:
    snapshot: dict[str, Any] = {}
    for section in CATALOG_VERSION_SECTIONS:
        value = catalog.get(section)
        if isinstance(value, list):
            snapshot[section] = list(value)
        elif isinstance(value, dict):
            snapshot[section] = dict(value)
        elif section in {"operation_fields", "capability_implementations"}:
            snapshot[section] = []
        else:
            snapshot[section] = {}
    return snapshot


def _catalog_version_summary(version: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": version.get("id"),
        "version_number": version.get("version_number"),
        "status": version.get("status"),
        "reason": version.get("reason"),
        "proposal_id": version.get("proposal_id"),
        "snapshot_sha256": version.get("snapshot_sha256"),
        "counts": version.get("counts") or {},
        "metadata": version.get("metadata") or {},
        "created_by": version.get("created_by"),
        "created_at": version.get("created_at"),
    }


def _catalog_snapshot_diff(base: dict[str, Any], target: dict[str, Any]) -> dict[str, Any]:
    sections: dict[str, dict[str, Any]] = {}
    for section in sorted(set(base) | set(target)):
        base_items = _snapshot_section_items(base.get(section))
        target_items = _snapshot_section_items(target.get(section))
        added = sorted(set(target_items) - set(base_items))
        removed = sorted(set(base_items) - set(target_items))
        changed = sorted(
            item_id
            for item_id in set(base_items) & set(target_items)
            if not _json_equal(base_items[item_id], target_items[item_id])
        )
        if not added and not removed and not changed:
            continue
        sections[section] = {
            "added": added,
            "removed": removed,
            "changed": changed,
            "counts": {
                "added": len(added),
                "removed": len(removed),
                "changed": len(changed),
            },
        }
    return {
        "sections": sections,
        "counts": {
            "sections_changed": len(sections),
            "added": sum(item["counts"]["added"] for item in sections.values()),
            "removed": sum(item["counts"]["removed"] for item in sections.values()),
            "changed": sum(item["counts"]["changed"] for item in sections.values()),
        },
    }


def _snapshot_section_items(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return {str(key): item for key, item in value.items()}
    if isinstance(value, list):
        items: dict[str, Any] = {}
        for index, item in enumerate(value):
            if isinstance(item, dict):
                item_id = (
                    item.get("id")
                    or item.get("operation_id")
                    or item.get("variant_id")
                    or item.get("document_id")
                    or item.get("key")
                    or index
                )
            else:
                item_id = index
            items[str(item_id)] = item
        return items
    return {}


def _catalog_snapshot_counts(snapshot: dict[str, Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for key, value in snapshot.items():
        if isinstance(value, dict):
            counts[key] = len(value)
        elif isinstance(value, list):
            counts[key] = len(value)
    return counts


def _capability_document_from_capability(
    capability_id: str,
    capability: dict[str, Any],
    catalog: dict[str, Any] | None = None,
) -> dict[str, Any]:
    provenance = capability.get("provenance") if isinstance(capability.get("provenance"), dict) else {}
    aliases = _json_list(provenance.get("aliases"))
    tags = _json_list(provenance.get("tags"))
    intent_patterns = _json_list(provenance.get("intent_patterns"))
    semantic_entities = _json_list(provenance.get("semantic_entities"))
    planning_hints = provenance.get("planning_hints") if isinstance(provenance.get("planning_hints"), dict) else {}
    examples = _json_list(capability.get("examples"))
    inputs = _json_list(capability.get("inputs"))
    outputs = _json_list(capability.get("outputs"))
    use_when = capability.get("use_when")
    use_when_text = " ".join(str(value) for value in use_when) if isinstance(use_when, list) else str(use_when or "")
    planning_hint_text = _planning_hint_text(planning_hints)
    variant_text = _capability_variant_text(capability_id, catalog or {})
    text_parts = [
        capability_id,
        str(capability.get("description_ko") or ""),
        use_when_text,
        " ".join(str(value) for value in aliases),
        " ".join(str(value) for value in examples),
        " ".join(str(value) for value in intent_patterns),
        " ".join(str(value) for value in semantic_entities),
        planning_hint_text,
        " ".join(str(value) for value in inputs),
        " ".join(str(value) for value in outputs),
        " ".join(str(value) for value in tags),
        variant_text,
    ]
    return {
        "id": f"capdoc.{capability_id}",
        "capability_id": capability_id,
        "document_text": "\n".join(part for part in text_parts if part.strip()),
        "aliases": aliases,
        "examples": examples,
        "intent_patterns": intent_patterns,
        "semantic_entities": semantic_entities,
        "planning_hints": planning_hints,
        "inputs": inputs,
        "outputs": outputs,
        "tags": tags,
        "vector_status": "not_embedded",
        "status": "active",
        "provenance": {"builder": "capability_document_from_capability"},
    }


def _planning_hint_text(planning_hints: dict[str, Any]) -> str:
    if not isinstance(planning_hints, dict):
        return ""
    parts: list[str] = []
    for key in ("returns", "requires", "default_variant_id"):
        value = planning_hints.get(key)
        if isinstance(value, list):
            parts.extend(str(item) for item in value)
        elif value:
            parts.append(str(value))
    return " ".join(parts)


def _capability_variant_text(capability_id: str, catalog: dict[str, Any]) -> str:
    variants = catalog.get("operation_variants", {}) if isinstance(catalog.get("operation_variants"), dict) else {}
    contracts = catalog.get("operation_contracts", {}) if isinstance(catalog.get("operation_contracts"), dict) else {}
    parts = []
    for variant_id, variant in variants.items():
        if not isinstance(variant, dict):
            continue
        if str(variant.get("capability_id") or variant.get("capability") or "") != capability_id:
            continue
        contract = contracts.get(str(variant.get("operation_id") or ""), {})
        parts.extend(
            [
                str(variant_id),
                str(variant.get("name") or ""),
                " ".join(f"{key}:{value}" for key, value in (variant.get("fixed_semantic_arguments") or {}).items()),
                " ".join(f"{key}:{value}" for key, value in (variant.get("fixed_raw_arguments") or {}).items()),
                str((contract or {}).get("path") or ""),
            ]
        )
    return "\n".join(part for part in parts if part.strip())


def _json_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _semantic_types_from_payload_contract(contract_part: Any) -> set[str]:
    found: set[str] = set()

    def walk(value: Any) -> None:
        if isinstance(value, dict):
            semantic_type = value.get("semantic_type")
            if semantic_type:
                found.add(str(semantic_type))
            for child in value.values():
                walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)

    walk(contract_part)
    return found


def _lexical_score(query: str, document: dict[str, Any]) -> float:
    query_terms = [term for term in query.lower().replace("_", " ").split() if term]
    if not query_terms:
        return 0
    haystack = " ".join(
        [
            str(document.get("id") or ""),
            str(document.get("capability_id") or ""),
            str(document.get("document_text") or ""),
            " ".join(str(value) for value in _json_list(document.get("aliases"))),
            " ".join(str(value) for value in _json_list(document.get("examples"))),
            " ".join(str(value) for value in _json_list(document.get("intent_patterns"))),
            " ".join(str(value) for value in _json_list(document.get("tags"))),
        ]
    ).lower()
    score = 0.0
    for term in query_terms:
        if term in haystack:
            score += 1.0
    if query.lower() in haystack:
        score += 3.0
    return score


def _applied_status(status: Any) -> str:
    return "approved" if status in (None, "", "pending_review", "planned") else str(status)


def _jsonb(value: Any) -> Any:
    from psycopg.types.json import Jsonb

    return Jsonb(value, dumps=lambda item: json.dumps(item, ensure_ascii=False, default=str))


def llm_mode() -> str:
    mode = os.getenv("SEMANTIC_PLATFORM_LLM_MODE") or os.getenv("LLM_MODE") or "disabled"
    normalized = mode.strip().lower()
    return normalized if normalized in LLM_MODES else "disabled"


def embedding_model() -> str:
    return os.getenv("SEMANTIC_PLATFORM_EMBEDDING_MODEL", DEFAULT_EMBEDDING_MODEL)


def embedding_provider() -> str:
    provider = os.getenv("SEMANTIC_PLATFORM_EMBEDDING_PROVIDER", "openai").strip().lower()
    return provider if provider in {"openai", "http"} else "openai"


def embedding_dimensions() -> int:
    raw = os.getenv("SEMANTIC_PLATFORM_EMBEDDING_DIMENSIONS", str(DEFAULT_EMBEDDING_DIMENSIONS))
    try:
        value = int(raw)
    except ValueError:
        value = DEFAULT_EMBEDDING_DIMENSIONS
    return value if 1 <= value <= 4096 else DEFAULT_EMBEDDING_DIMENSIONS


def _vector_literal(vector: list[float]) -> str:
    return "[" + ",".join(f"{float(value):.12g}" for value in vector) + "]"


def _call_embedding_api(texts: list[str], *, model: str) -> list[list[float]]:
    from urllib import request
    from urllib.error import HTTPError, URLError

    provider = embedding_provider()
    payload: dict[str, Any] = {"input": texts}
    headers = {"Content-Type": "application/json"}
    if provider == "openai":
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            return []
        payload["model"] = model
        dimensions = embedding_dimensions()
        if model.startswith("text-embedding-3"):
            payload["dimensions"] = dimensions
        headers["Authorization"] = f"Bearer {api_key}"
        url = os.getenv("SEMANTIC_PLATFORM_EMBEDDING_API_URL", "https://api.openai.com/v1/embeddings")
    else:
        payload["normalize"] = True
        url = os.getenv("SEMANTIC_PLATFORM_EMBEDDING_API_URL", "http://embedding-service:8000/embeddings")
    http_request = request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    try:
        with request.urlopen(http_request, timeout=float(os.getenv("SEMANTIC_PLATFORM_EMBEDDING_TIMEOUT_SECONDS", "60"))) as response:
            body = json.loads(response.read().decode("utf-8"))
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError, ValueError):
        return []
    data = body.get("data") if isinstance(body, dict) else None
    if not isinstance(data, list):
        return []
    vectors: list[list[float]] = []
    for item in sorted(data, key=lambda value: int(value.get("index", 0)) if isinstance(value, dict) else 0):
        embedding = item.get("embedding") if isinstance(item, dict) else None
        if not isinstance(embedding, list):
            return []
        vectors.append([float(value) for value in embedding])
    return vectors


def _lexical_matches(query: str, documents: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    scored = []
    for document in documents:
        score = _lexical_score(query, document)
        if score > 0:
            scored.append({"score": score, "document": document})
    scored.sort(key=lambda item: item["score"], reverse=True)
    return scored[:limit]
