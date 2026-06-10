create table if not exists semantic_layer.execution_sources (
  id text primary key,
  name text not null unique,
  provider text not null default '',
  source_type text not null,
  description text not null default '',
  status text not null default 'draft',
  config jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint chk_execution_sources_source_type
    check (source_type in ('api', 'table', 'file', 'stream', 'queue', 'other'))
);

create table if not exists semantic_layer.execution_assets (
  id text primary key,
  source_id text not null references semantic_layer.execution_sources(id) on delete cascade,
  name text not null,
  asset_type text not null,
  locator text not null,
  description text not null default '',
  status text not null default 'draft',
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint chk_execution_assets_asset_type
    check (asset_type in ('endpoint', 'table', 'view', 'query', 'file', 'topic', 'other')),
  constraint uq_execution_assets_source_name unique (source_id, name)
);

create table if not exists semantic_layer.execution_access_paths (
  id text primary key,
  asset_id text not null references semantic_layer.execution_assets(id) on delete cascade,
  name text not null,
  access_type text not null,
  locator text not null,
  http_method text not null default '',
  description text not null default '',
  status text not null default 'draft',
  request_shape jsonb not null default '{}'::jsonb,
  response_shape jsonb not null default '{}'::jsonb,
  execution_hints jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint chk_execution_access_paths_access_type
    check (access_type in ('http', 'sql', 'file_read', 'stream_read', 'rpc', 'other')),
  constraint uq_execution_access_paths_asset_name unique (asset_id, name)
);

create table if not exists semantic_layer.execution_operations (
  id text primary key,
  access_path_id text not null references semantic_layer.execution_access_paths(id) on delete cascade,
  operation_key text not null unique,
  name text not null,
  description text not null default '',
  output_semantic_type_id text references semantic_layer.semantic_types(id) on delete set null,
  status text not null default 'draft',
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists semantic_layer.operation_variants (
  id text primary key,
  operation_id text not null references semantic_layer.execution_operations(id) on delete cascade,
  variant_key text not null unique,
  name text not null,
  description text not null default '',
  status text not null default 'draft',
  fixed_semantic_arguments jsonb not null default '{}'::jsonb,
  fixed_raw_arguments jsonb not null default '{}'::jsonb,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists semantic_layer.operation_fields (
  id text primary key,
  operation_id text not null references semantic_layer.execution_operations(id) on delete cascade,
  variant_id text references semantic_layer.operation_variants(id) on delete cascade,
  scope text not null,
  raw_name text not null,
  display_name text not null default '',
  field_path text not null default '',
  data_type text not null default 'string',
  is_required boolean not null default false,
  description text not null default '',
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint chk_operation_fields_scope
    check (scope in ('input', 'output', 'control')),
  constraint uq_operation_fields_operation_scope_raw_name unique (operation_id, scope, raw_name)
);

create table if not exists semantic_layer.field_mappings (
  id text primary key,
  field_id text not null references semantic_layer.operation_fields(id) on delete cascade,
  semantic_type_id text not null references semantic_layer.semantic_types(id) on delete cascade,
  mapping_kind text not null default 'direct',
  status text not null default 'draft',
  transform_spec jsonb not null default '{}'::jsonb,
  enum_mapping jsonb not null default '{}'::jsonb,
  notes text not null default '',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint chk_field_mappings_mapping_kind
    check (mapping_kind in ('direct', 'enum', 'transform', 'reference')),
  constraint uq_field_mappings_field_semantic unique (field_id, semantic_type_id)
);

create table if not exists semantic_layer.access_path_checks (
  id text primary key,
  access_path_id text not null references semantic_layer.execution_access_paths(id) on delete cascade,
  variant_id text references semantic_layer.operation_variants(id) on delete set null,
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
  on semantic_layer.execution_assets (source_id);

create index if not exists idx_execution_access_paths_asset_id
  on semantic_layer.execution_access_paths (asset_id);

create index if not exists idx_execution_operations_access_path_id
  on semantic_layer.execution_operations (access_path_id);

create index if not exists idx_operation_variants_operation_id
  on semantic_layer.operation_variants (operation_id);

create index if not exists idx_operation_fields_operation_id
  on semantic_layer.operation_fields (operation_id);

create index if not exists idx_operation_fields_variant_id
  on semantic_layer.operation_fields (variant_id);

create index if not exists idx_field_mappings_field_id
  on semantic_layer.field_mappings (field_id);

create index if not exists idx_field_mappings_semantic_type_id
  on semantic_layer.field_mappings (semantic_type_id);

create index if not exists idx_access_path_checks_access_path_id
  on semantic_layer.access_path_checks (access_path_id);

create index if not exists idx_access_path_checks_variant_id
  on semantic_layer.access_path_checks (variant_id);
