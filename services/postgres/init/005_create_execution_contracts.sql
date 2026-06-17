create table if not exists semantic_platform.execution_sources (
  id text primary key,
  namespace text not null default 'public',
  name text not null unique,
  provider text not null default '',
  source_type text not null,
  description text not null default '',
  version text not null default '1.0.0',
  lifecycle text not null default 'draft',
  status text not null default 'draft',
  config jsonb not null default '{}'::jsonb,
  created_by text not null default 'system',
  reviewed_by text,
  approved_at timestamptz,
  evidence jsonb not null default '[]'::jsonb,
  confidence numeric(5,4),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint chk_execution_sources_source_type
    check (source_type in ('api', 'table', 'file', 'stream', 'queue', 'other')),
  constraint chk_execution_sources_lifecycle
    check (lifecycle in ('draft', 'review', 'approved', 'published', 'deprecated'))
);

create table if not exists semantic_platform.execution_assets (
  id text primary key,
  source_id text not null references semantic_platform.execution_sources(id) on delete cascade,
  name text not null,
  asset_type text not null,
  locator text not null,
  description text not null default '',
  version text not null default '1.0.0',
  lifecycle text not null default 'draft',
  status text not null default 'draft',
  metadata jsonb not null default '{}'::jsonb,
  created_by text not null default 'system',
  reviewed_by text,
  approved_at timestamptz,
  evidence jsonb not null default '[]'::jsonb,
  confidence numeric(5,4),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint chk_execution_assets_asset_type
    check (asset_type in ('endpoint', 'table', 'view', 'query', 'file', 'topic', 'other')),
  constraint chk_execution_assets_lifecycle
    check (lifecycle in ('draft', 'review', 'approved', 'published', 'deprecated')),
  constraint uq_execution_assets_source_name unique (source_id, name)
);

create table if not exists semantic_platform.execution_access_paths (
  id text primary key,
  asset_id text not null references semantic_platform.execution_assets(id) on delete cascade,
  name text not null,
  access_type text not null,
  locator text not null,
  http_method text not null default '',
  description text not null default '',
  version text not null default '1.0.0',
  lifecycle text not null default 'draft',
  status text not null default 'draft',
  request_shape jsonb not null default '{}'::jsonb,
  response_shape jsonb not null default '{}'::jsonb,
  execution_hints jsonb not null default '{}'::jsonb,
  created_by text not null default 'system',
  reviewed_by text,
  approved_at timestamptz,
  evidence jsonb not null default '[]'::jsonb,
  confidence numeric(5,4),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint chk_execution_access_paths_access_type
    check (access_type in ('http', 'sql', 'file_read', 'stream_read', 'rpc', 'other')),
  constraint chk_execution_access_paths_lifecycle
    check (lifecycle in ('draft', 'review', 'approved', 'published', 'deprecated')),
  constraint uq_execution_access_paths_asset_name unique (asset_id, name)
);

create table if not exists semantic_platform.execution_operations (
  id text primary key,
  access_path_id text not null references semantic_platform.execution_access_paths(id) on delete cascade,
  operation_key text not null unique,
  name text not null,
  description text not null default '',
  namespace text not null default 'public',
  version text not null default '1.0.0',
  lifecycle text not null default 'draft',
  status text not null default 'draft',
  input_spec jsonb not null default '[]'::jsonb,
  output_spec jsonb not null default '[]'::jsonb,
  auth_spec jsonb not null default '{}'::jsonb,
  contract_spec jsonb not null default '{}'::jsonb,
  metadata jsonb not null default '{}'::jsonb,
  created_by text not null default 'system',
  reviewed_by text,
  approved_at timestamptz,
  evidence jsonb not null default '[]'::jsonb,
  confidence numeric(5,4),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint chk_execution_operations_lifecycle
    check (lifecycle in ('draft', 'review', 'approved', 'published', 'deprecated'))
);

create table if not exists semantic_platform.capabilities (
  id text primary key,
  capability_key text not null unique,
  namespace text not null default 'public',
  name text not null,
  description text not null default '',
  version text not null default '1.0.0',
  lifecycle text not null default 'draft',
  status text not null default 'draft',
  intent_spec jsonb not null default '{}'::jsonb,
  input_semantic_types jsonb not null default '[]'::jsonb,
  output_semantic_types jsonb not null default '[]'::jsonb,
  metadata jsonb not null default '{}'::jsonb,
  created_by text not null default 'system',
  reviewed_by text,
  approved_at timestamptz,
  evidence jsonb not null default '[]'::jsonb,
  confidence numeric(5,4),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint chk_capabilities_lifecycle
    check (lifecycle in ('draft', 'review', 'approved', 'published', 'deprecated'))
);

create table if not exists semantic_platform.capability_operations (
  id text primary key,
  capability_id text not null references semantic_platform.capabilities(id) on delete cascade,
  operation_id text not null references semantic_platform.execution_operations(id) on delete cascade,
  variant_id text references semantic_platform.operation_variants(id) on delete set null,
  priority integer not null default 100,
  binding_spec jsonb not null default '{}'::jsonb,
  status text not null default 'draft',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint uq_capability_operations_binding unique (capability_id, operation_id, variant_id)
);

create table if not exists semantic_platform.operation_variants (
  id text primary key,
  operation_id text not null references semantic_platform.execution_operations(id) on delete cascade,
  variant_key text not null unique,
  name text not null,
  description text not null default '',
  version text not null default '1.0.0',
  lifecycle text not null default 'draft',
  status text not null default 'draft',
  fixed_semantic_arguments jsonb not null default '{}'::jsonb,
  fixed_raw_arguments jsonb not null default '{}'::jsonb,
  metadata jsonb not null default '{}'::jsonb,
  created_by text not null default 'system',
  reviewed_by text,
  approved_at timestamptz,
  evidence jsonb not null default '[]'::jsonb,
  confidence numeric(5,4),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint chk_operation_variants_lifecycle
    check (lifecycle in ('draft', 'review', 'approved', 'published', 'deprecated'))
);

create table if not exists semantic_platform.operation_fields (
  id text primary key,
  operation_id text not null references semantic_platform.execution_operations(id) on delete cascade,
  variant_id text references semantic_platform.operation_variants(id) on delete cascade,
  scope text not null,
  raw_name text not null,
  display_name text not null default '',
  field_path text not null default '',
  data_type text not null default 'string',
  is_required boolean not null default false,
  description text not null default '',
  version text not null default '1.0.0',
  lifecycle text not null default 'draft',
  metadata jsonb not null default '{}'::jsonb,
  created_by text not null default 'system',
  reviewed_by text,
  approved_at timestamptz,
  evidence jsonb not null default '[]'::jsonb,
  confidence numeric(5,4),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint chk_operation_fields_scope
    check (scope in ('input', 'output', 'control')),
  constraint chk_operation_fields_lifecycle
    check (lifecycle in ('draft', 'review', 'approved', 'published', 'deprecated')),
  constraint uq_operation_fields_operation_scope_raw_name unique (operation_id, scope, raw_name)
);

create table if not exists semantic_platform.field_mappings (
  id text primary key,
  field_id text references semantic_platform.operation_fields(id) on delete cascade,
  source_id text references semantic_platform.execution_sources(id) on delete cascade,
  operation_id text not null references semantic_platform.execution_operations(id) on delete cascade,
  variant_id text references semantic_platform.operation_variants(id) on delete set null,
  access_path_id text references semantic_platform.execution_access_paths(id) on delete cascade,
  field_path text not null default '',
  semantic_type_id text not null references semantic_platform.semantic_types(id) on delete cascade,
  canonical_attribute_id text references semantic_platform.semantic_types(id) on delete set null,
  mapping_type text not null default 'exact',
  version text not null default '1.0.0',
  lifecycle text not null default 'draft',
  status text not null default 'draft',
  namespace text not null default 'public',
  transform_spec jsonb not null default '{}'::jsonb,
  enum_mapping jsonb not null default '{}'::jsonb,
  notes text not null default '',
  created_by text not null default 'system',
  reviewed_by text,
  approved_at timestamptz,
  evidence jsonb not null default '[]'::jsonb,
  confidence numeric(5,4),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint chk_field_mappings_mapping_type
    check (mapping_type in ('exact', 'transform', 'composite', 'enum', 'reference')),
  constraint chk_field_mappings_lifecycle
    check (lifecycle in ('draft', 'review', 'approved', 'published', 'deprecated')),
  constraint uq_field_mappings_context unique (operation_id, variant_id, field_path, semantic_type_id, canonical_attribute_id)
);

create table if not exists semantic_platform.access_path_checks (
  id text primary key,
  access_path_id text not null references semantic_platform.execution_access_paths(id) on delete cascade,
  variant_id text references semantic_platform.operation_variants(id) on delete set null,
  check_type text not null default 'manual',
  status text not null default 'pending',
  request_snapshot jsonb not null default '{}'::jsonb,
  response_snapshot jsonb not null default '{}'::jsonb,
  notes text not null default '',
  checked_at timestamptz,
  created_at timestamptz not null default now(),
  constraint chk_access_path_checks_status
    check (status in ('pending', 'passed', 'failed', 'skipped'))
);

create index if not exists idx_execution_assets_source_id
  on semantic_platform.execution_assets (source_id);

create index if not exists idx_execution_access_paths_asset_id
  on semantic_platform.execution_access_paths (asset_id);

create index if not exists idx_execution_operations_access_path_id
  on semantic_platform.execution_operations (access_path_id);

create index if not exists idx_capabilities_capability_key
  on semantic_platform.capabilities (capability_key);

create index if not exists idx_capability_operations_capability_id
  on semantic_platform.capability_operations (capability_id);

create index if not exists idx_capability_operations_operation_id
  on semantic_platform.capability_operations (operation_id);

create index if not exists idx_operation_variants_operation_id
  on semantic_platform.operation_variants (operation_id);

create index if not exists idx_operation_fields_operation_id
  on semantic_platform.operation_fields (operation_id);

create index if not exists idx_operation_fields_variant_id
  on semantic_platform.operation_fields (variant_id);

create index if not exists idx_field_mappings_field_id
  on semantic_platform.field_mappings (field_id);

create index if not exists idx_field_mappings_source_id
  on semantic_platform.field_mappings (source_id);

create index if not exists idx_field_mappings_operation_id
  on semantic_platform.field_mappings (operation_id);

create index if not exists idx_field_mappings_semantic_type_id
  on semantic_platform.field_mappings (semantic_type_id);

create index if not exists idx_field_mappings_canonical_attribute_id
  on semantic_platform.field_mappings (canonical_attribute_id);

create index if not exists idx_access_path_checks_access_path_id
  on semantic_platform.access_path_checks (access_path_id);

create index if not exists idx_access_path_checks_variant_id
  on semantic_platform.access_path_checks (variant_id);
