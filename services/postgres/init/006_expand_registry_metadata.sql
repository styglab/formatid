alter table semantic_platform.semantic_types
  add column if not exists namespace text not null default 'public',
  add column if not exists version text not null default '1.0.0',
  add column if not exists lifecycle text not null default 'draft',
  add column if not exists constraint_spec jsonb not null default '{}'::jsonb,
  add column if not exists identity_type text not null default '',
  add column if not exists created_by text not null default 'system',
  add column if not exists reviewed_by text,
  add column if not exists approved_at timestamptz,
  add column if not exists evidence jsonb not null default '[]'::jsonb,
  add column if not exists confidence numeric(5,4);

alter table semantic_platform.semantic_relationships
  add column if not exists version text not null default '1.0.0',
  add column if not exists lifecycle text not null default 'draft',
  add column if not exists created_by text not null default 'system',
  add column if not exists reviewed_by text,
  add column if not exists approved_at timestamptz,
  add column if not exists evidence jsonb not null default '[]'::jsonb,
  add column if not exists confidence numeric(5,4);

alter table semantic_platform.proposals
  add column if not exists rationale text not null default '',
  add column if not exists evidence jsonb not null default '[]'::jsonb,
  add column if not exists proposed_by text not null default 'system',
  add column if not exists approved_at timestamptz;

alter table semantic_platform.execution_sources
  add column if not exists namespace text not null default 'public',
  add column if not exists version text not null default '1.0.0',
  add column if not exists lifecycle text not null default 'draft',
  add column if not exists created_by text not null default 'system',
  add column if not exists reviewed_by text,
  add column if not exists approved_at timestamptz,
  add column if not exists evidence jsonb not null default '[]'::jsonb,
  add column if not exists confidence numeric(5,4);

alter table semantic_platform.execution_assets
  add column if not exists version text not null default '1.0.0',
  add column if not exists lifecycle text not null default 'draft',
  add column if not exists created_by text not null default 'system',
  add column if not exists reviewed_by text,
  add column if not exists approved_at timestamptz,
  add column if not exists evidence jsonb not null default '[]'::jsonb,
  add column if not exists confidence numeric(5,4);

alter table semantic_platform.execution_access_paths
  add column if not exists version text not null default '1.0.0',
  add column if not exists lifecycle text not null default 'draft',
  add column if not exists created_by text not null default 'system',
  add column if not exists reviewed_by text,
  add column if not exists approved_at timestamptz,
  add column if not exists evidence jsonb not null default '[]'::jsonb,
  add column if not exists confidence numeric(5,4);

alter table semantic_platform.execution_operations
  add column if not exists namespace text not null default 'public',
  add column if not exists version text not null default '1.0.0',
  add column if not exists lifecycle text not null default 'draft',
  add column if not exists input_spec jsonb not null default '[]'::jsonb,
  add column if not exists output_spec jsonb not null default '[]'::jsonb,
  add column if not exists auth_spec jsonb not null default '{}'::jsonb,
  add column if not exists contract_spec jsonb not null default '{}'::jsonb,
  add column if not exists created_by text not null default 'system',
  add column if not exists reviewed_by text,
  add column if not exists approved_at timestamptz,
  add column if not exists evidence jsonb not null default '[]'::jsonb,
  add column if not exists confidence numeric(5,4);

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
  updated_at timestamptz not null default now()
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

alter table semantic_platform.operation_variants
  add column if not exists version text not null default '1.0.0',
  add column if not exists lifecycle text not null default 'draft',
  add column if not exists created_by text not null default 'system',
  add column if not exists reviewed_by text,
  add column if not exists approved_at timestamptz,
  add column if not exists evidence jsonb not null default '[]'::jsonb,
  add column if not exists confidence numeric(5,4);

alter table semantic_platform.operation_fields
  add column if not exists version text not null default '1.0.0',
  add column if not exists lifecycle text not null default 'draft',
  add column if not exists created_by text not null default 'system',
  add column if not exists reviewed_by text,
  add column if not exists approved_at timestamptz,
  add column if not exists evidence jsonb not null default '[]'::jsonb,
  add column if not exists confidence numeric(5,4);

alter table semantic_platform.field_mappings
  add column if not exists source_id text references semantic_platform.execution_sources(id) on delete cascade,
  add column if not exists operation_id text references semantic_platform.execution_operations(id) on delete cascade,
  add column if not exists variant_id text references semantic_platform.operation_variants(id) on delete set null,
  add column if not exists access_path_id text references semantic_platform.execution_access_paths(id) on delete cascade,
  add column if not exists field_path text not null default '',
  add column if not exists canonical_attribute_id text references semantic_platform.semantic_types(id) on delete set null,
  add column if not exists mapping_type text not null default 'exact',
  add column if not exists version text not null default '1.0.0',
  add column if not exists lifecycle text not null default 'draft',
  add column if not exists namespace text not null default 'public',
  add column if not exists created_by text not null default 'system',
  add column if not exists reviewed_by text,
  add column if not exists approved_at timestamptz,
  add column if not exists evidence jsonb not null default '[]'::jsonb,
  add column if not exists confidence numeric(5,4);

create index if not exists idx_capabilities_capability_key
  on semantic_platform.capabilities (capability_key);

create index if not exists idx_capability_operations_capability_id
  on semantic_platform.capability_operations (capability_id);

create index if not exists idx_capability_operations_operation_id
  on semantic_platform.capability_operations (operation_id);

create index if not exists idx_field_mappings_source_id
  on semantic_platform.field_mappings (source_id);

create index if not exists idx_field_mappings_operation_id
  on semantic_platform.field_mappings (operation_id);

create index if not exists idx_field_mappings_canonical_attribute_id
  on semantic_platform.field_mappings (canonical_attribute_id);
