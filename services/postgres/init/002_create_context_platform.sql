create schema if not exists context_platform;

alter table if exists context_platform.canonical_attributes
  drop column if exists semantic_concept_id;

drop table if exists context_platform.semantic_concepts cascade;

alter table if exists context_platform.bindings
  drop column if exists canonical_attribute_id cascade;

alter table if exists context_platform.capability_inputs
  drop column if exists canonical_attribute_id cascade;

alter table if exists context_platform.capability_outputs
  drop column if exists canonical_attribute_id cascade;

drop table if exists context_platform.canonical_attributes cascade;

do $$
begin
  if to_regclass('context_platform.canonical_classes') is null
     and to_regclass('context_platform.canonical_entities') is not null then
    alter table context_platform.canonical_entities rename to canonical_classes;
  end if;

  if to_regclass('context_platform.canonical_relations') is not null then
    if exists (
      select 1
      from information_schema.columns
      where table_schema = 'context_platform'
        and table_name = 'canonical_relations'
        and column_name = 'source_entity_id'
    ) then
      alter table context_platform.canonical_relations rename column source_entity_id to source_class_id;
    end if;

    if exists (
      select 1
      from information_schema.columns
      where table_schema = 'context_platform'
        and table_name = 'canonical_relations'
        and column_name = 'target_entity_id'
    ) then
      alter table context_platform.canonical_relations rename column target_entity_id to target_class_id;
    end if;
  end if;
end $$;

create table if not exists context_platform.sources (
  id text primary key,
  namespace text not null default 'public',
  name text not null unique,
  provider text not null default '',
  source_type text not null default 'api',
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
  constraint chk_sources_source_type
    check (source_type in ('api', 'table', 'file', 'stream', 'queue', 'other')),
  constraint chk_sources_lifecycle
    check (lifecycle in ('draft', 'review', 'approved', 'published', 'deprecated'))
);

create table if not exists context_platform.source_documents (
  id text primary key,
  source_id text not null references context_platform.sources(id) on delete cascade,
  document_type text not null default 'api_document',
  name text not null,
  uri text not null default '',
  content_hash text not null default '',
  content_type text not null default '',
  status text not null default 'draft',
  metadata jsonb not null default '{}'::jsonb,
  evidence jsonb not null default '[]'::jsonb,
  created_by text not null default 'system',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists context_platform.source_operations (
  id text primary key,
  source_id text not null references context_platform.sources(id) on delete cascade,
  source_document_id text references context_platform.source_documents(id) on delete set null,
  operation_key text not null unique,
  method text not null default 'GET',
  path text not null,
  name text not null,
  description text not null default '',
  auth_spec jsonb not null default '{}'::jsonb,
  request_spec jsonb not null default '{}'::jsonb,
  response_spec jsonb not null default '{}'::jsonb,
  endpoint_metadata jsonb not null default '{}'::jsonb,
  version text not null default '1.0.0',
  lifecycle text not null default 'draft',
  status text not null default 'draft',
  created_by text not null default 'system',
  reviewed_by text,
  approved_at timestamptz,
  evidence jsonb not null default '[]'::jsonb,
  confidence numeric(5,4),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint uq_source_operations_source_method_path unique (source_id, method, path),
  constraint chk_source_operations_lifecycle
    check (lifecycle in ('draft', 'review', 'approved', 'published', 'deprecated'))
);

create table if not exists context_platform.source_parameters (
  id text primary key,
  source_operation_id text not null references context_platform.source_operations(id) on delete cascade,
  name text not null,
  raw_name text not null default '',
  location text not null default 'query',
  parameter_path text not null default '',
  data_type text not null default 'string',
  is_required boolean not null default false,
  default_value text,
  description text not null default '',
  enum_values jsonb not null default '[]'::jsonb,
  metadata jsonb not null default '{}'::jsonb,
  status text not null default 'draft',
  evidence jsonb not null default '[]'::jsonb,
  confidence numeric(5,4),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint chk_source_parameters_location
    check (location in ('path', 'query', 'header', 'cookie', 'body', 'control')),
  constraint uq_source_parameters_operation_location_name unique (source_operation_id, location, name)
);

create table if not exists context_platform.source_fields (
  id text primary key,
  source_id text references context_platform.sources(id) on delete cascade,
  source_document_id text references context_platform.source_documents(id) on delete cascade,
  source_operation_id text references context_platform.source_operations(id) on delete cascade,
  direction text not null,
  field_path text not null,
  raw_name text not null default '',
  display_name text not null default '',
  data_type text not null default 'string',
  is_required boolean not null default false,
  description text not null default '',
  metadata jsonb not null default '{}'::jsonb,
  status text not null default 'draft',
  evidence jsonb not null default '[]'::jsonb,
  confidence numeric(5,4),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint chk_source_fields_direction
    check (direction in ('input', 'output')),
  constraint chk_source_fields_scope
    check (source_operation_id is not null or source_document_id is not null),
  constraint uq_source_fields_operation_direction_path unique (source_operation_id, direction, field_path)
);

create table if not exists context_platform.canonical_types (
  id text primary key,
  namespace text not null default 'public',
  name text not null,
  description text not null default '',
  base_type text not null default 'string',
  uri text not null default '',
  typeof text not null default '',
  pattern text not null default '',
  minimum numeric,
  maximum numeric,
  version text not null default '1.0.0',
  lifecycle text not null default 'draft',
  status text not null default 'draft',
  annotations jsonb not null default '{}'::jsonb,
  metadata jsonb not null default '{}'::jsonb,
  created_by text not null default 'system',
  reviewed_by text,
  approved_at timestamptz,
  evidence jsonb not null default '[]'::jsonb,
  confidence numeric(5,4),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint uq_canonical_types_namespace_name unique (namespace, name),
  constraint chk_canonical_types_lifecycle
    check (lifecycle in ('draft', 'review', 'approved', 'published', 'deprecated'))
);

create table if not exists context_platform.canonical_enums (
  id text primary key,
  namespace text not null default 'public',
  name text not null,
  description text not null default '',
  permissible_values jsonb not null default '{}'::jsonb,
  version text not null default '1.0.0',
  lifecycle text not null default 'draft',
  status text not null default 'draft',
  annotations jsonb not null default '{}'::jsonb,
  metadata jsonb not null default '{}'::jsonb,
  created_by text not null default 'system',
  reviewed_by text,
  approved_at timestamptz,
  evidence jsonb not null default '[]'::jsonb,
  confidence numeric(5,4),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint uq_canonical_enums_namespace_name unique (namespace, name),
  constraint chk_canonical_enums_lifecycle
    check (lifecycle in ('draft', 'review', 'approved', 'published', 'deprecated'))
);

create table if not exists context_platform.canonical_enum_values (
  id text primary key,
  enum_id text not null references context_platform.canonical_enums(id) on delete cascade,
  code text not null,
  meaning text not null default '',
  description text not null default '',
  aliases jsonb not null default '[]'::jsonb,
  annotations jsonb not null default '{}'::jsonb,
  metadata jsonb not null default '{}'::jsonb,
  status text not null default 'draft',
  evidence jsonb not null default '[]'::jsonb,
  confidence numeric(5,4),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint uq_canonical_enum_values_enum_code unique (enum_id, code)
);

create table if not exists context_platform.canonical_slots (
  id text primary key,
  namespace text not null default 'public',
  name text not null,
  description text not null default '',
  range_kind text not null default 'type',
  range_ref text not null default 'string',
  datatype text not null default 'string',
  aliases jsonb not null default '[]'::jsonb,
  examples jsonb not null default '[]'::jsonb,
  mappings jsonb not null default '[]'::jsonb,
  annotations jsonb not null default '{}'::jsonb,
  constraints jsonb not null default '{}'::jsonb,
  identity_role text not null default '',
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
  constraint uq_canonical_slots_namespace_name unique (namespace, name),
  constraint chk_canonical_slots_range_kind
    check (range_kind in ('type', 'enum', 'class')),
  constraint chk_canonical_slots_lifecycle
    check (lifecycle in ('draft', 'review', 'approved', 'published', 'deprecated'))
);

create table if not exists context_platform.canonical_classes (
  id text primary key,
  namespace text not null default 'public',
  name text not null,
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
  constraint uq_canonical_classes_namespace_name unique (namespace, name)
);

create table if not exists context_platform.canonical_class_slots (
  id text primary key,
  class_id text not null references context_platform.canonical_classes(id) on delete cascade,
  slot_id text not null references context_platform.canonical_slots(id) on delete cascade,
  usage_name text not null default '',
  required boolean not null default false,
  multivalued boolean not null default false,
  slot_order integer not null default 100,
  range_override text not null default '',
  constraints jsonb not null default '{}'::jsonb,
  annotations jsonb not null default '{}'::jsonb,
  status text not null default 'draft',
  created_by text not null default 'system',
  reviewed_by text,
  approved_at timestamptz,
  evidence jsonb not null default '[]'::jsonb,
  confidence numeric(5,4),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint uq_canonical_class_slots_class_slot unique (class_id, slot_id)
);

create table if not exists context_platform.canonical_relations (
  id text primary key,
  source_class_id text not null references context_platform.canonical_classes(id) on delete cascade,
  target_class_id text not null references context_platform.canonical_classes(id) on delete cascade,
  relation_type text not null,
  forward_label text not null default '',
  reverse_label text not null default '',
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
  updated_at timestamptz not null default now()
);

create unique index if not exists uq_canonical_relations_source_target_type
  on context_platform.canonical_relations (source_class_id, target_class_id, relation_type);

create table if not exists context_platform.bindings (
  id text primary key,
  source_id text not null references context_platform.sources(id) on delete cascade,
  source_document_id text references context_platform.source_documents(id) on delete set null,
  source_operation_id text references context_platform.source_operations(id) on delete cascade,
  source_parameter_id text references context_platform.source_parameters(id) on delete cascade,
  source_field_id text references context_platform.source_fields(id) on delete cascade,
  canonical_class_slot_id text not null references context_platform.canonical_class_slots(id) on delete cascade,
  direction text not null,
  binding_type text not null default 'exact',
  transform_spec jsonb not null default '{}'::jsonb,
  normalization_rule jsonb not null default '{}'::jsonb,
  enum_mapping jsonb not null default '{}'::jsonb,
  status text not null default 'draft',
  confidence numeric(5,4),
  evidence jsonb not null default '[]'::jsonb,
  created_by text not null default 'system',
  reviewed_by text,
  approved_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint chk_bindings_direction check (direction in ('input', 'output')),
  constraint chk_bindings_endpoint check (
    (direction = 'input' and source_parameter_id is not null and source_field_id is null)
    or (direction = 'output' and source_field_id is not null and source_parameter_id is null)
  ),
  constraint chk_bindings_type check (binding_type in ('exact', 'transform', 'composite', 'enum', 'reference'))
);

create table if not exists context_platform.binding_evidence (
  id text primary key,
  binding_id text not null references context_platform.bindings(id) on delete cascade,
  evidence_type text not null default 'source_document',
  source_document_id text references context_platform.source_documents(id) on delete set null,
  source_section text not null default '',
  excerpt text not null default '',
  payload jsonb not null default '{}'::jsonb,
  created_by text not null default 'system',
  created_at timestamptz not null default now()
);

create table if not exists context_platform.capabilities (
  id text primary key,
  capability_key text not null unique,
  namespace text not null default 'public',
  name text not null,
  description text not null default '',
  intent_spec jsonb not null default '{}'::jsonb,
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
  updated_at timestamptz not null default now()
);

create table if not exists context_platform.capability_inputs (
  id text primary key,
  capability_id text not null references context_platform.capabilities(id) on delete cascade,
  canonical_class_slot_id text not null references context_platform.canonical_class_slots(id) on delete cascade,
  required boolean not null default true,
  input_order integer not null default 100,
  status text not null default 'draft',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint uq_capability_inputs unique (capability_id, canonical_class_slot_id)
);

create table if not exists context_platform.capability_outputs (
  id text primary key,
  capability_id text not null references context_platform.capabilities(id) on delete cascade,
  canonical_class_slot_id text not null references context_platform.canonical_class_slots(id) on delete cascade,
  output_order integer not null default 100,
  status text not null default 'draft',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint uq_capability_outputs unique (capability_id, canonical_class_slot_id)
);

create table if not exists context_platform.capability_operations (
  id text primary key,
  capability_id text not null references context_platform.capabilities(id) on delete cascade,
  source_operation_id text not null references context_platform.source_operations(id) on delete cascade,
  priority integer not null default 100,
  binding_spec jsonb not null default '{}'::jsonb,
  status text not null default 'draft',
  created_by text not null default 'system',
  reviewed_by text,
  approved_at timestamptz,
  evidence jsonb not null default '[]'::jsonb,
  confidence numeric(5,4),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint uq_capability_operations unique (capability_id, source_operation_id)
);

create table if not exists context_platform.plans (
  id text primary key,
  selected_capability_id text references context_platform.capabilities(id) on delete set null,
  selected_source_operation_id text references context_platform.source_operations(id) on delete set null,
  status text not null default 'draft',
  canonical_inputs jsonb not null default '{}'::jsonb,
  parameter_bindings jsonb not null default '[]'::jsonb,
  expected_outputs jsonb not null default '[]'::jsonb,
  confidence numeric(5,4),
  requires_confirmation boolean not null default false,
  validation_result jsonb not null default '{}'::jsonb,
  request_payload jsonb not null default '{}'::jsonb,
  plan_payload jsonb not null default '{}'::jsonb,
  created_by text not null default 'planner',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists context_platform.plan_steps (
  id text primary key,
  plan_id text not null references context_platform.plans(id) on delete cascade,
  step_order integer not null default 100,
  capability_id text references context_platform.capabilities(id) on delete set null,
  source_operation_id text references context_platform.source_operations(id) on delete set null,
  canonical_inputs jsonb not null default '{}'::jsonb,
  parameter_bindings jsonb not null default '[]'::jsonb,
  expected_outputs jsonb not null default '[]'::jsonb,
  status text not null default 'draft',
  created_at timestamptz not null default now()
);

create table if not exists context_platform.executions (
  id text primary key,
  plan_id text not null references context_platform.plans(id) on delete cascade,
  status text not null default 'started',
  request_payload jsonb not null default '{}'::jsonb,
  result_payload jsonb not null default '{}'::jsonb,
  started_at timestamptz not null default now(),
  completed_at timestamptz
);

create table if not exists context_platform.execution_logs (
  id text primary key,
  execution_id text not null references context_platform.executions(id) on delete cascade,
  level text not null default 'info',
  message text not null,
  payload jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create table if not exists context_platform.endpoint_checks (
  id text primary key,
  run_id text references context_platform.onboarding_runs(id) on delete cascade,
  source_id text not null references context_platform.sources(id) on delete cascade,
  source_document_id text references context_platform.source_documents(id) on delete set null,
  source_operation_id text references context_platform.source_operations(id) on delete cascade,
  capability_key text not null default '',
  check_type text not null,
  status text not null,
  http_status integer,
  request_sample_redacted jsonb not null default '{}'::jsonb,
  response_sample_ref jsonb not null default '{}'::jsonb,
  field_coverage jsonb not null default '{}'::jsonb,
  binding_validation jsonb not null default '{}'::jsonb,
  error_message text not null default '',
  checked_by text not null default 'context_platform_worker',
  checked_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  constraint chk_endpoint_checks_type
    check (check_type in ('operation', 'capability')),
  constraint chk_endpoint_checks_status
    check (status in ('verified', 'failed', 'skipped', 'needs_input'))
);

create table if not exists context_platform.proposals (
  id text primary key,
  source_type text not null,
  title text not null,
  entity_type text not null,
  entity_id text not null,
  change_type text not null,
  payload jsonb not null,
  rationale text not null default '',
  evidence jsonb not null default '[]'::jsonb,
  proposed_by text not null default 'system',
  reviewed_by text,
  reviewed_at timestamptz,
  approved_at timestamptz,
  status text not null default 'proposed',
  created_at timestamptz not null default now(),
  constraint chk_proposals_status
    check (status in ('proposed', 'reviewed', 'approved', 'published', 'rejected', 'pending_review'))
);

create table if not exists context_platform.review_decisions (
  id text primary key,
  proposal_id text not null references context_platform.proposals(id) on delete cascade,
  reviewer text not null default 'system',
  decision text not null,
  rationale text not null default '',
  evidence jsonb not null default '[]'::jsonb,
  created_at timestamptz not null default now(),
  constraint chk_review_decisions_decision
    check (decision in ('approved', 'rejected', 'needs_changes', 'commented'))
);

create table if not exists context_platform.onboarding_runs (
  id text primary key,
  source_id text not null references context_platform.sources(id) on delete cascade,
  source_document_id text references context_platform.source_documents(id) on delete set null,
  status text not null default 'started',
  stage text not null default 'source_uploaded',
  created_by text not null default 'system',
  metadata jsonb not null default '{}'::jsonb,
  started_at timestamptz not null default now(),
  completed_at timestamptz,
  updated_at timestamptz not null default now()
);

create table if not exists context_platform.evidence_snapshots (
  id text primary key,
  run_id text not null references context_platform.onboarding_runs(id) on delete cascade,
  source_id text not null references context_platform.sources(id) on delete cascade,
  source_document_id text references context_platform.source_documents(id) on delete set null,
  snapshot_type text not null default 'source_upload',
  content_hash text not null default '',
  source_ref jsonb not null default '{}'::jsonb,
  operation_evidence jsonb not null default '[]'::jsonb,
  schema_evidence jsonb not null default '[]'::jsonb,
  sample_values jsonb not null default '{}'::jsonb,
  ai_context jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create table if not exists context_platform.proposal_bundles (
  id text primary key,
  run_id text not null references context_platform.onboarding_runs(id) on delete cascade,
  source_id text not null references context_platform.sources(id) on delete cascade,
  evidence_snapshot_id text references context_platform.evidence_snapshots(id) on delete set null,
  title text not null,
  status text not null default 'draft',
  summary jsonb not null default '{}'::jsonb,
  created_by text not null default 'system',
  reviewed_by text,
  reviewed_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists context_platform.proposal_bundle_items (
  bundle_id text not null references context_platform.proposal_bundles(id) on delete cascade,
  proposal_id text not null references context_platform.proposals(id) on delete cascade,
  item_order integer not null default 100,
  created_at timestamptz not null default now(),
  primary key (bundle_id, proposal_id)
);

create index if not exists idx_source_documents_source_id
  on context_platform.source_documents (source_id);
create index if not exists idx_source_operations_source_id
  on context_platform.source_operations (source_id);
create index if not exists idx_source_parameters_operation_id
  on context_platform.source_parameters (source_operation_id);
create index if not exists idx_source_fields_operation_id
  on context_platform.source_fields (source_operation_id);
create index if not exists idx_canonical_types_namespace_name
  on context_platform.canonical_types (namespace, name);
create index if not exists idx_canonical_enums_namespace_name
  on context_platform.canonical_enums (namespace, name);
create index if not exists idx_canonical_enum_values_enum_id
  on context_platform.canonical_enum_values (enum_id);
create index if not exists idx_canonical_slots_namespace_name
  on context_platform.canonical_slots (namespace, name);
create index if not exists idx_canonical_class_slots_class_id
  on context_platform.canonical_class_slots (class_id);
create index if not exists idx_canonical_class_slots_slot_id
  on context_platform.canonical_class_slots (slot_id);

alter table context_platform.bindings
  add column if not exists canonical_class_slot_id text references context_platform.canonical_class_slots(id) on delete cascade;

alter table context_platform.capability_inputs
  add column if not exists canonical_class_slot_id text references context_platform.canonical_class_slots(id) on delete cascade;

alter table context_platform.capability_outputs
  add column if not exists canonical_class_slot_id text references context_platform.canonical_class_slots(id) on delete cascade;

create index if not exists idx_bindings_source_operation_id
  on context_platform.bindings (source_operation_id);
create index if not exists idx_bindings_canonical_class_slot_id
  on context_platform.bindings (canonical_class_slot_id);
create index if not exists idx_binding_evidence_binding_id
  on context_platform.binding_evidence (binding_id);
create index if not exists idx_capability_inputs_capability_id
  on context_platform.capability_inputs (capability_id);
create index if not exists idx_capability_outputs_capability_id
  on context_platform.capability_outputs (capability_id);
create index if not exists idx_capability_operations_capability_id
  on context_platform.capability_operations (capability_id);
create index if not exists idx_capability_operations_source_operation_id
  on context_platform.capability_operations (source_operation_id);
create index if not exists idx_plans_status
  on context_platform.plans (status);
create index if not exists idx_executions_plan_id
  on context_platform.executions (plan_id);
create index if not exists idx_execution_logs_execution_id
  on context_platform.execution_logs (execution_id);
create index if not exists idx_endpoint_checks_run_id
  on context_platform.endpoint_checks (run_id);
create index if not exists idx_endpoint_checks_source_operation_id
  on context_platform.endpoint_checks (source_operation_id);
create index if not exists idx_endpoint_checks_capability_key
  on context_platform.endpoint_checks (capability_key);
create index if not exists idx_proposals_status
  on context_platform.proposals (status);

alter table context_platform.source_fields
  add column if not exists source_id text references context_platform.sources(id) on delete cascade;

alter table context_platform.source_fields
  add column if not exists source_document_id text references context_platform.source_documents(id) on delete cascade;

alter table context_platform.source_fields
  alter column source_operation_id drop not null;

update context_platform.source_fields sf
set source_id = so.source_id,
    source_document_id = coalesce(sf.source_document_id, so.source_document_id)
from context_platform.source_operations so
where sf.source_operation_id = so.id
  and (sf.source_id is null or sf.source_document_id is null);

alter table context_platform.bindings
  add column if not exists source_document_id text references context_platform.source_documents(id) on delete set null;

alter table context_platform.bindings
  alter column source_operation_id drop not null;

update context_platform.bindings b
set source_document_id = so.source_document_id
from context_platform.source_operations so
where b.source_operation_id = so.id
  and b.source_document_id is null;

create index if not exists idx_source_fields_document_id
  on context_platform.source_fields (source_document_id);

create index if not exists idx_bindings_source_document_id
  on context_platform.bindings (source_document_id);

-- Meaning Resolution Platform v2 registry.
-- Existing source/proposal tables remain compatibility storage. The old
-- canonical-slot-first catalog is superseded by Meaning, Representation,
-- Resolution, Capability, Execution, and Governance graph tables below.

create table if not exists context_platform.meaning_scopes (
  id text primary key,
  stable_key text not null unique,
  name text not null,
  description text not null default '',
  status text not null default 'active',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists context_platform.concept_schemes (
  id text primary key,
  stable_key text not null unique,
  meaning_scope_id text references context_platform.meaning_scopes(id) on delete set null,
  name text not null,
  description text not null default '',
  external_uri text not null default '',
  status text not null default 'draft',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists context_platform.concepts (
  id text primary key,
  stable_key text not null unique,
  meaning_scope_id text not null references context_platform.meaning_scopes(id) on delete restrict,
  domain_id text,
  scheme_id text references context_platform.concept_schemes(id) on delete set null,
  kind text not null,
  code text,
  label_ko text,
  label_en text,
  definition text,
  external_uri text not null default '',
  status text not null default 'draft',
  metadata jsonb not null default '{}'::jsonb,
  evidence jsonb not null default '[]'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint chk_concepts_kind check (
    kind in (
      'object_concept',
      'metric_concept',
      'identifier_concept',
      'status_concept',
      'value_concept',
      'unit_concept',
      'time_concept',
      'account_concept',
      'document_concept',
      'operation_concept'
    )
  )
);

create table if not exists context_platform.concept_relations (
  id text primary key,
  source_concept_id text not null references context_platform.concepts(id) on delete cascade,
  target_concept_id text not null references context_platform.concepts(id) on delete cascade,
  relation_type text not null,
  confidence numeric(5,4),
  status text not null default 'draft',
  evidence jsonb not null default '[]'::jsonb,
  created_at timestamptz not null default now(),
  constraint uq_concept_relations unique (source_concept_id, target_concept_id, relation_type),
  constraint chk_concept_relations_type check (
    relation_type in (
      'broader',
      'narrower',
      'exact_match',
      'close_match',
      'related',
      'has_unit',
      'has_value_domain',
      'has_value',
      'applicable_to_object',
      'represents_identifier_type',
      'derived_from',
      'requires_context'
    )
  )
);

create table if not exists context_platform.value_domains (
  id text primary key,
  stable_key text not null unique,
  meaning_scope_id text references context_platform.meaning_scopes(id) on delete set null,
  name text not null,
  description text not null default '',
  status text not null default 'draft',
  metadata jsonb not null default '{}'::jsonb,
  evidence jsonb not null default '[]'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists context_platform.value_domain_values (
  id text primary key,
  value_domain_id text not null references context_platform.value_domains(id) on delete cascade,
  code text not null,
  concept_id text references context_platform.concepts(id) on delete set null,
  label_ko text,
  label_en text,
  description text not null default '',
  status text not null default 'draft',
  metadata jsonb not null default '{}'::jsonb,
  evidence jsonb not null default '[]'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint uq_value_domain_values_code unique (value_domain_id, code)
);

create table if not exists context_platform.object_types (
  id text primary key,
  stable_key text not null unique,
  name text not null,
  description text not null default '',
  concept_id text references context_platform.concepts(id) on delete set null,
  linkml_class text not null default '',
  external_uri text not null default '',
  status text not null default 'draft',
  metadata jsonb not null default '{}'::jsonb,
  evidence jsonb not null default '[]'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists context_platform.property_types (
  id text primary key,
  stable_key text not null unique,
  name text not null,
  description text not null default '',
  broad_datatype text not null default 'string',
  linkml_slot text not null default '',
  external_uri text not null default '',
  status text not null default 'draft',
  metadata jsonb not null default '{}'::jsonb,
  evidence jsonb not null default '[]'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists context_platform.link_types (
  id text primary key,
  stable_key text not null unique,
  name text not null,
  source_object_type_id text references context_platform.object_types(id) on delete set null,
  target_object_type_id text references context_platform.object_types(id) on delete set null,
  description text not null default '',
  status text not null default 'draft',
  metadata jsonb not null default '{}'::jsonb,
  evidence jsonb not null default '[]'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists context_platform.canonical_representations (
  id text primary key,
  stable_key text not null unique,
  concept_id text not null references context_platform.concepts(id) on delete restrict,
  carrier_object_type_id text not null references context_platform.object_types(id) on delete restrict,
  value_property_type_id text references context_platform.property_types(id) on delete set null,
  fixed_context_json jsonb not null default '{}'::jsonb,
  required_context_json jsonb not null default '[]'::jsonb,
  representation_kind text not null,
  priority integer not null default 100,
  is_preferred boolean not null default false,
  domain_id text,
  status text not null default 'draft',
  evidence_id text,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists context_platform.representation_schemas (
  id text primary key,
  stable_key text not null unique,
  representation_id text not null references context_platform.canonical_representations(id) on delete cascade,
  datatype text not null,
  value_domain_id text references context_platform.value_domains(id) on delete set null,
  pattern text,
  structured_pattern_json jsonb not null default '{}'::jsonb,
  cardinality text,
  required boolean,
  default_json jsonb,
  examples_json jsonb not null default '[]'::jsonb,
  validation_json jsonb not null default '{}'::jsonb,
  status text not null default 'draft',
  evidence_id text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists context_platform.external_projections (
  id text primary key,
  stable_key text not null unique,
  representation_id text not null references context_platform.canonical_representations(id) on delete cascade,
  representation_schema_id text references context_platform.representation_schemas(id) on delete set null,
  target_surface text not null,
  output_key text not null,
  shape_json jsonb not null default '{}'::jsonb,
  status text not null default 'draft',
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists context_platform.transform_rules (
  id text primary key,
  stable_key text not null unique,
  rule_type text not null,
  description text not null default '',
  rule_json jsonb not null default '{}'::jsonb,
  status text not null default 'draft',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists context_platform.field_bindings (
  id text primary key,
  stable_key text not null unique,
  source_field_id text not null references context_platform.source_fields(id) on delete cascade,
  representation_id text not null references context_platform.canonical_representations(id) on delete cascade,
  representation_schema_id text references context_platform.representation_schemas(id) on delete set null,
  fills_property_type_id text references context_platform.property_types(id) on delete set null,
  transform_rule_id text references context_platform.transform_rules(id) on delete set null,
  confidence numeric(5,4),
  review_status text not null default 'proposed',
  status text not null default 'draft',
  evidence_id text,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists context_platform.context_bindings (
  id text primary key,
  stable_key text not null unique,
  source_field_id text not null references context_platform.source_fields(id) on delete cascade,
  representation_id text not null references context_platform.canonical_representations(id) on delete cascade,
  representation_schema_id text references context_platform.representation_schemas(id) on delete set null,
  context_key text not null,
  target_concept_id text references context_platform.concepts(id) on delete set null,
  transform_rule_id text references context_platform.transform_rules(id) on delete set null,
  confidence numeric(5,4),
  review_status text not null default 'proposed',
  status text not null default 'draft',
  evidence_id text,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists context_platform.parameter_bindings (
  id text primary key,
  stable_key text not null unique,
  source_parameter_id text not null references context_platform.source_parameters(id) on delete cascade,
  required_concept_id text not null references context_platform.concepts(id) on delete restrict,
  representation_id text references context_platform.canonical_representations(id) on delete set null,
  representation_schema_id text references context_platform.representation_schemas(id) on delete set null,
  transform_rule_id text references context_platform.transform_rules(id) on delete set null,
  confidence numeric(5,4),
  review_status text not null default 'proposed',
  status text not null default 'draft',
  evidence_id text,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists context_platform.resolution_rules (
  id text primary key,
  stable_key text not null unique,
  rule_type text not null,
  source_scope_json jsonb not null default '{}'::jsonb,
  target_scope_json jsonb not null default '{}'::jsonb,
  rule_json jsonb not null default '{}'::jsonb,
  status text not null default 'draft',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

alter table context_platform.bindings
  alter column canonical_class_slot_id drop not null;

alter table context_platform.bindings
  add column if not exists representation_id text references context_platform.canonical_representations(id) on delete set null;

alter table context_platform.bindings
  add column if not exists representation_schema_id text references context_platform.representation_schemas(id) on delete set null;

alter table context_platform.bindings
  add column if not exists fills_property_type_id text references context_platform.property_types(id) on delete set null;

alter table context_platform.bindings
  add column if not exists context_key text not null default '';

alter table context_platform.bindings
  add column if not exists required_concept_id text references context_platform.concepts(id) on delete set null;

alter table context_platform.capabilities
  add column if not exists kind text not null default 'lookup';

alter table context_platform.capabilities
  add column if not exists subject_concept_id text references context_platform.concepts(id) on delete set null;

alter table context_platform.capabilities
  add column if not exists domain_id text;

alter table context_platform.capabilities
  add column if not exists freshness_sla text;

alter table context_platform.capabilities
  add column if not exists determinism text;

alter table context_platform.capabilities
  add column if not exists cost_class text;

alter table context_platform.capability_inputs
  alter column canonical_class_slot_id drop not null;

alter table context_platform.capability_inputs
  add column if not exists concept_id text references context_platform.concepts(id) on delete set null;

alter table context_platform.capability_inputs
  add column if not exists representation_id text references context_platform.canonical_representations(id) on delete set null;

alter table context_platform.capability_inputs
  add column if not exists representation_schema_id text references context_platform.representation_schemas(id) on delete set null;

alter table context_platform.capability_outputs
  alter column canonical_class_slot_id drop not null;

alter table context_platform.capability_outputs
  add column if not exists output_key text not null default '';

alter table context_platform.capability_outputs
  add column if not exists concept_id text references context_platform.concepts(id) on delete set null;

alter table context_platform.capability_outputs
  add column if not exists representation_id text references context_platform.canonical_representations(id) on delete set null;

alter table context_platform.capability_outputs
  add column if not exists representation_schema_id text references context_platform.representation_schemas(id) on delete set null;

alter table context_platform.capability_outputs
  add column if not exists value_path text;

alter table context_platform.capability_outputs
  add column if not exists unit_path text;

alter table context_platform.capability_outputs
  add column if not exists period_path text;

alter table context_platform.capability_outputs
  add column if not exists is_primary boolean not null default false;

create table if not exists context_platform.capability_steps (
  id text primary key,
  capability_id text not null references context_platform.capabilities(id) on delete cascade,
  step_order integer not null default 100,
  source_operation_id text references context_platform.source_operations(id) on delete set null,
  step_kind text not null default 'source_operation',
  binding_spec jsonb not null default '{}'::jsonb,
  status text not null default 'draft',
  evidence jsonb not null default '[]'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists context_platform.capability_constraints (
  id text primary key,
  capability_id text not null references context_platform.capabilities(id) on delete cascade,
  constraint_key text not null,
  constraint_json jsonb not null default '{}'::jsonb,
  status text not null default 'draft',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint uq_capability_constraints_key unique (capability_id, constraint_key)
);

alter table context_platform.plans
  add column if not exists requested_concept_id text references context_platform.concepts(id) on delete set null;

alter table context_platform.plans
  add column if not exists selected_representation_id text references context_platform.canonical_representations(id) on delete set null;

alter table context_platform.plans
  add column if not exists selected_representation_schema_id text references context_platform.representation_schemas(id) on delete set null;

create table if not exists context_platform.execution_plan_steps (
  id text primary key,
  plan_id text not null references context_platform.plans(id) on delete cascade,
  step_order integer not null default 100,
  capability_step_id text references context_platform.capability_steps(id) on delete set null,
  source_operation_id text references context_platform.source_operations(id) on delete set null,
  input_json jsonb not null default '{}'::jsonb,
  output_json jsonb not null default '{}'::jsonb,
  status text not null default 'draft',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists context_platform.execution_step_runs (
  id text primary key,
  execution_id text not null references context_platform.executions(id) on delete cascade,
  plan_step_id text references context_platform.execution_plan_steps(id) on delete set null,
  status text not null default 'running',
  request_json jsonb not null default '{}'::jsonb,
  response_json jsonb not null default '{}'::jsonb,
  started_at timestamptz not null default now(),
  finished_at timestamptz,
  error_json jsonb
);

create table if not exists context_platform.execution_results (
  id text primary key,
  execution_id text not null references context_platform.executions(id) on delete cascade,
  concept_id text references context_platform.concepts(id) on delete set null,
  representation_id text references context_platform.canonical_representations(id) on delete set null,
  representation_schema_id text references context_platform.representation_schemas(id) on delete set null,
  output_key text not null default '',
  value_json jsonb not null default '{}'::jsonb,
  projection_json jsonb not null default '{}'::jsonb,
  status text not null default 'created',
  created_at timestamptz not null default now()
);

create table if not exists context_platform.execution_traces (
  id text primary key,
  execution_id text references context_platform.executions(id) on delete cascade,
  plan_id text references context_platform.plans(id) on delete cascade,
  trace_type text not null,
  payload_json jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create table if not exists context_platform.evidence_items (
  id text primary key,
  stable_key text unique,
  evidence_type text not null,
  source_ref_json jsonb not null default '{}'::jsonb,
  quote text not null default '',
  payload_json jsonb not null default '{}'::jsonb,
  created_by text not null default 'system',
  created_at timestamptz not null default now()
);

create table if not exists context_platform.review_events (
  id text primary key,
  target_kind text not null,
  target_id text not null,
  reviewer text not null default 'system',
  decision text not null,
  rationale text not null default '',
  evidence_id text references context_platform.evidence_items(id) on delete set null,
  payload_json jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create table if not exists context_platform.metadata_aspects (
  id text primary key,
  target_kind text not null,
  target_id text not null,
  aspect_type text not null,
  payload_json jsonb not null,
  evidence_id text references context_platform.evidence_items(id) on delete set null,
  status text not null default 'active',
  version integer not null default 1,
  created_at timestamptz not null default now(),
  constraint uq_metadata_aspects_version unique (target_kind, target_id, aspect_type, version)
);

create table if not exists context_platform.lineage_edges (
  id text primary key,
  source_kind text not null,
  source_id text not null,
  target_kind text not null,
  target_id text not null,
  relation_type text not null default 'derived_from',
  evidence_id text references context_platform.evidence_items(id) on delete set null,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create table if not exists context_platform.quality_checks (
  id text primary key,
  target_kind text not null,
  target_id text not null,
  check_key text not null,
  status text not null,
  payload_json jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create table if not exists context_platform.policy_tags (
  id text primary key,
  target_kind text not null,
  target_id text not null,
  tag_key text not null,
  tag_value text not null default '',
  status text not null default 'active',
  created_at timestamptz not null default now()
);

create index if not exists idx_concepts_scope
  on context_platform.concepts (meaning_scope_id);
create index if not exists idx_concepts_kind
  on context_platform.concepts (kind);
create index if not exists idx_representations_concept
  on context_platform.canonical_representations (concept_id);
create index if not exists idx_representation_schemas_representation
  on context_platform.representation_schemas (representation_id);
create index if not exists idx_field_bindings_source_field
  on context_platform.field_bindings (source_field_id);
create index if not exists idx_context_bindings_source_field
  on context_platform.context_bindings (source_field_id);
create index if not exists idx_parameter_bindings_source_parameter
  on context_platform.parameter_bindings (source_parameter_id);
create index if not exists idx_capability_outputs_concept
  on context_platform.capability_outputs (concept_id);
create index if not exists idx_metadata_aspects_target
  on context_platform.metadata_aspects (target_kind, target_id);

-- Model seed generated from tmp/context_platform/seed/*.linkml.yaml.
-- Keep this seed limited to model definitions: no sources, bindings, capabilities, executions, or proposals.

insert into context_platform.value_domains
  (id, stable_key, meaning_scope_id, name, description, status, metadata)
values
  ('value_domain.foundation.temporal_precision_enum', 'value_domain.foundation.temporal_precision_enum', null, 'TemporalPrecisionEnum', 'Precision of a temporal value when a source does not provide a full date/time.', 'active', '{"linkml_enum": "TemporalPrecisionEnum", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb)
on conflict (id) do update set stable_key = excluded.stable_key, name = excluded.name, description = excluded.description, status = excluded.status, metadata = excluded.metadata, updated_at = now();

insert into context_platform.value_domain_values
  (id, value_domain_id, code, concept_id, label_ko, label_en, description, status, metadata)
values
  ('value_domain_value.foundation.temporal_precision_enum.date', 'value_domain.foundation.temporal_precision_enum', 'date', null, null, 'date', 'Date precision.', 'active', '{"linkml_enum": "TemporalPrecisionEnum", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb),
  ('value_domain_value.foundation.temporal_precision_enum.month', 'value_domain.foundation.temporal_precision_enum', 'month', null, null, 'month', 'Month precision.', 'active', '{"linkml_enum": "TemporalPrecisionEnum", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb),
  ('value_domain_value.foundation.temporal_precision_enum.year', 'value_domain.foundation.temporal_precision_enum', 'year', null, null, 'year', 'Year precision.', 'active', '{"linkml_enum": "TemporalPrecisionEnum", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb)
on conflict (id) do update set value_domain_id = excluded.value_domain_id, code = excluded.code, label_en = excluded.label_en, description = excluded.description, status = excluded.status, metadata = excluded.metadata, updated_at = now();

insert into context_platform.object_types
  (id, stable_key, name, description, concept_id, linkml_class, external_uri, status, metadata, evidence)
values
  ('object.common_business.api', 'object.common_business.api', 'API', 'API, endpoint group, or machine-readable service surface.', null, 'API', 'hydra:ApiDocumentation', 'active', '{"in_subset": ["common_business_core"], "is_a": "Entity", "mixins": ["HasIdentifiers"], "slots": ["api_type", "provider", "base_url", "documentation_url", "auth_type", "version_label"], "source_schema": "common_business_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('object.foundation.address', 'object.foundation.address', 'Address', 'Postal, administrative, legal, operational, or source-provided address.', null, 'Address', 'schema:PostalAddress', 'active', '{"in_subset": ["value_object"], "slots": ["full_address", "street_address", "address_locality", "address_region", "postal_code", "address_country", "country_code", "geo_location", "description"], "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('object.foundation.classification', 'object.foundation.classification', 'Classification', 'Code or named concept from a controlled vocabulary, taxonomy, or source scheme.', null, 'Classification', 'skos:Concept', 'active', '{"in_subset": ["reference_value"], "is_a": "Entity", "slot_usage": {"classification_name": {"required": true}, "classification_scheme": {"required": true}}, "slots": ["classification_code", "classification_name", "classification_scheme", "broader", "narrower", "exact_match", "close_match"], "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('object.foundation.classification_scheme', 'object.foundation.classification_scheme', 'ClassificationScheme', 'Controlled vocabulary, taxonomy, code list, or classification system.', null, 'ClassificationScheme', 'skos:ConceptScheme', 'active', '{"in_subset": ["reference_scheme"], "is_a": "Entity", "slot_usage": {"name": {"required": true}}, "slots": ["scheme_code", "scheme_name", "scheme_uri", "issuing_authority"], "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('object.common_business.company', 'object.common_business.company', 'Company', 'Business organization or legal entity.', null, 'Company', 'schema:Organization', 'active', '{"in_subset": ["common_business_core"], "is_a": "Organization", "slot_usage": {"organization_type": {"recommended": true}}, "source_schema": "common_business_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('object.foundation.contact_method', 'object.foundation.contact_method', 'ContactMethod', 'Contact route such as email, phone, fax, URL, handle, or other contact value.', null, 'ContactMethod', 'schema:ContactPoint', 'active', '{"in_subset": ["value_object"], "slot_usage": {"contact_value": {"required": true}}, "slots": ["contact_method_type", "contact_value", "verified", "preferred", "valid_during", "description"], "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('object.common_business.dataset', 'object.common_business.dataset', 'Dataset', 'Dataset, table, file collection, feed, or other data product.', null, 'Dataset', 'dcat:Dataset', 'active', '{"in_subset": ["common_business_core"], "is_a": "Entity", "mixins": ["HasIdentifiers"], "slots": ["dataset_type", "publisher", "subject", "issued_date", "modified_date", "license_uri", "access_url", "download_url"], "source_schema": "common_business_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('object.foundation.document', 'object.foundation.document', 'Document', 'Business, legal, public, administrative, or source document.', null, 'Document', 'schema:CreativeWork', 'active', '{"in_subset": ["foundation_core"], "is_a": "Entity", "mixins": ["HasEvidence"], "slots": ["document_number", "document_type", "document_status", "issuer", "subject", "issued_date", "effective_date", "expiration_date", "covers_period"], "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('object.foundation.entity', 'object.foundation.entity', 'Entity', 'Minimal abstract anchor for objects that can be identified, related, documented, or observed. Concrete business objects belong in domain schemas.', null, 'Entity', 'prov:Entity', 'active', '{"abstract": true, "in_subset": ["foundation_core"], "mixins": ["HasAliases", "HasExternalReferences"], "slots": ["canonical_id", "name", "display_name", "description"], "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('object.foundation.geo_location', 'object.foundation.geo_location', 'GeoLocation', 'Geographic coordinate or spatial reference for an address, place, asset, or observation.', null, 'GeoLocation', 'schema:GeoCoordinates', 'active', '{"in_subset": ["value_object"], "slots": ["latitude", "longitude", "coordinate_system", "geohash", "description"], "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('object.foundation.has_addresses', 'object.foundation.has_addresses', 'HasAddresses', 'Mixin for domain classes that can have postal, legal, or operational addresses.', null, 'HasAddresses', '', 'active', '{"mixin": true, "slots": ["addresses"], "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('object.foundation.has_aliases', 'object.foundation.has_aliases', 'HasAliases', 'Mixin for classes that can carry alternate labels.', null, 'HasAliases', '', 'active', '{"mixin": true, "slots": ["aliases"], "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('object.foundation.has_contact_methods', 'object.foundation.has_contact_methods', 'HasContactMethods', 'Mixin for domain classes that can have contact methods.', null, 'HasContactMethods', '', 'active', '{"mixin": true, "slots": ["contact_methods"], "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('object.foundation.has_evidence', 'object.foundation.has_evidence', 'HasEvidence', 'Mixin for statements that can retain source evidence.', null, 'HasEvidence', '', 'active', '{"mixin": true, "slots": ["evidence"], "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('object.foundation.has_external_references', 'object.foundation.has_external_references', 'HasExternalReferences', 'Mixin for classes that can be linked to external equivalent resources.', null, 'HasExternalReferences', '', 'active', '{"mixin": true, "slots": ["same_as"], "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('object.foundation.has_identifiers', 'object.foundation.has_identifiers', 'HasIdentifiers', 'Mixin for domain classes that can be identified by governed external identifiers.', null, 'HasIdentifiers', '', 'active', '{"mixin": true, "slots": ["identifiers"], "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('object.foundation.identifier', 'object.foundation.identifier', 'Identifier', 'Identifier value assigned by a scheme or authority to an entity.', null, 'Identifier', 'schema:PropertyValue', 'active', '{"in_subset": ["foundation_core"], "is_a": "Entity", "slot_usage": {"identifier_type": {"required": true}, "identifier_value": {"required": true}, "identifies": {"required": true}}, "slots": ["identifier_value", "identifier_type", "identifier_scheme", "issuing_authority", "identifies", "issued_date", "valid_during", "expiration_date"], "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('object.foundation.money', 'object.foundation.money', 'Money', 'Monetary value object with amount and currency.', null, 'Money', 'schema:MonetaryAmount', 'active', '{"in_subset": ["value_object"], "slot_usage": {"amount": {"required": true}, "currency_code": {"required": true}}, "slots": ["amount", "currency_code"], "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('object.foundation.observation', 'object.foundation.observation', 'Observation', 'A reported or observed fact about a subject. Use this for metrics, measurements, statuses, and reported values when the value needs concept, time, source, or evidence context.', null, 'Observation', '', 'active', '{"in_subset": ["foundation_core"], "is_a": "Entity", "mixins": ["HasEvidence"], "slot_usage": {"concept": {"required": true}, "subject": {"required": true}}, "slots": ["subject", "concept", "observed_value", "observed_amount", "observed_quantity", "observed_date", "covers_period", "issuer"], "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('object.common_business.organization', 'object.common_business.organization', 'Organization', 'Legal, administrative, commercial, public, or informal organization.', null, 'Organization', 'schema:Organization', 'active', '{"in_subset": ["common_business_core"], "is_a": "Entity", "mixins": ["HasIdentifiers", "HasContactMethods", "HasAddresses"], "slots": ["legal_name", "organization_type", "parent_organization", "founding_date", "url"], "source_schema": "common_business_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('object.common_business.person', 'object.common_business.person', 'Person', 'Natural person. Sensitive identity fields belong in privacy/domain extensions.', null, 'Person', 'schema:Person', 'active', '{"in_subset": ["common_business_core"], "is_a": "Entity", "mixins": ["HasIdentifiers", "HasContactMethods", "HasAddresses"], "slots": ["full_name", "given_name", "family_name", "birth_date"], "source_schema": "common_business_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('object.foundation.quantity', 'object.foundation.quantity', 'Quantity', 'Numeric quantity with a unit of measure.', null, 'Quantity', '', 'active', '{"in_subset": ["value_object"], "slot_usage": {"quantity_value": {"required": true}}, "slots": ["quantity_value", "unit_code"], "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('object.foundation.relationship', 'object.foundation.relationship', 'Relationship', 'Reified relationship between two entities with type, role, validity, and evidence.', null, 'Relationship', '', 'active', '{"in_subset": ["foundation_core"], "is_a": "Entity", "mixins": ["HasEvidence"], "slot_usage": {"relationship_type": {"required": true}, "source_entity": {"required": true}, "target_entity": {"required": true}}, "slots": ["relationship_type", "source_entity", "target_entity", "role", "valid_during", "issued_date"], "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('object.foundation.time_interval', 'object.foundation.time_interval', 'TimeInterval', 'Temporal interval for validity, reporting, observation, or coverage context.', null, 'TimeInterval', 'TIME:TemporalEntity', 'active', '{"in_subset": ["context_object"], "slots": ["start_date", "end_date", "temporal_precision", "description"], "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb)
on conflict (id) do update set stable_key = excluded.stable_key, name = excluded.name, description = excluded.description, linkml_class = excluded.linkml_class, external_uri = excluded.external_uri, status = excluded.status, metadata = excluded.metadata, evidence = excluded.evidence, updated_at = now();

insert into context_platform.property_types
  (id, stable_key, name, description, broad_datatype, linkml_slot, external_uri, status, metadata, evidence)
values
  ('property.common_business.access_url', 'property.common_business.access_url', 'access_url', 'URL that provides access to a dataset or service.', 'uri', 'access_url', 'dcat:accessURL', 'active', '{"range": "uri", "source_schema": "common_business_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.address_country', 'property.foundation.address_country', 'address_country', 'Country name or country reference.', 'string', 'address_country', 'schema:addressCountry', 'active', '{"range": "string", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.address_locality', 'property.foundation.address_locality', 'address_locality', 'City, locality, municipality, or equivalent.', 'string', 'address_locality', 'schema:addressLocality', 'active', '{"range": "string", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.address_region', 'property.foundation.address_region', 'address_region', 'Region, province, state, or equivalent.', 'string', 'address_region', 'schema:addressRegion', 'active', '{"range": "string", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.addresses', 'property.foundation.addresses', 'addresses', 'Postal, legal, or operational addresses associated with a domain entity.', 'Address', 'addresses', 'schema:address', 'active', '{"inlined": false, "multivalued": true, "range": "Address", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.aliases', 'property.foundation.aliases', 'aliases', 'Alternative labels, field names, abbreviations, or search synonyms.', 'string', 'aliases', 'skos:altLabel', 'active', '{"multivalued": true, "range": "string", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.amount', 'property.foundation.amount', 'amount', 'Numeric monetary amount.', 'decimal', 'amount', '', 'active', '{"range": "decimal", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.common_business.api_type', 'property.common_business.api_type', 'api_type', 'Type or category of API.', 'Classification', 'api_type', '', 'active', '{"inlined": false, "range": "Classification", "source_schema": "common_business_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.common_business.auth_type', 'property.common_business.auth_type', 'auth_type', 'Authentication or authorization type used by an API.', 'Classification', 'auth_type', '', 'active', '{"inlined": false, "range": "Classification", "source_schema": "common_business_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.common_business.base_url', 'property.common_business.base_url', 'base_url', 'Base URL for an API or service.', 'uri', 'base_url', '', 'active', '{"range": "uri", "source_schema": "common_business_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.common_business.birth_date', 'property.common_business.birth_date', 'birth_date', 'Birth date of a person when appropriate and allowed by governance policy.', 'date', 'birth_date', 'schema:birthDate', 'active', '{"range": "date", "source_schema": "common_business_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.broader', 'property.foundation.broader', 'broader', 'Broader classification or concept.', 'Classification', 'broader', 'skos:broader', 'active', '{"inlined": false, "multivalued": true, "range": "Classification", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.canonical_id', 'property.foundation.canonical_id', 'canonical_id', 'Stable internal canonical identifier when one exists.', 'CanonicalId', 'canonical_id', 'schema:identifier', 'active', '{"identifier": true, "range": "CanonicalId", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.classification_code', 'property.foundation.classification_code', 'classification_code', 'Code value in a classification or controlled vocabulary.', 'NonEmptyString', 'classification_code', 'skos:notation', 'active', '{"range": "NonEmptyString", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.classification_name', 'property.foundation.classification_name', 'classification_name', 'Human-readable name of a classification value.', 'string', 'classification_name', 'skos:prefLabel', 'active', '{"range": "string", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.classification_scheme', 'property.foundation.classification_scheme', 'classification_scheme', 'Scheme, vocabulary, or authority that defines a classification.', 'ClassificationScheme', 'classification_scheme', 'skos:inScheme', 'active', '{"inlined": false, "range": "ClassificationScheme", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.close_match', 'property.foundation.close_match', 'close_match', 'Close but not strictly equivalent classification or concept in another scheme.', 'Classification', 'close_match', 'skos:closeMatch', 'active', '{"inlined": false, "multivalued": true, "range": "Classification", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.concept', 'property.foundation.concept', 'concept', 'Concept, metric, classification, or account represented by an observation.', 'Classification', 'concept', '', 'active', '{"inlined": false, "range": "Classification", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.contact_method_type', 'property.foundation.contact_method_type', 'contact_method_type', 'Purpose or type of contact method.', 'Classification', 'contact_method_type', 'schema:contactType', 'active', '{"inlined": false, "range": "Classification", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.contact_methods', 'property.foundation.contact_methods', 'contact_methods', 'Contact methods associated with a domain entity.', 'ContactMethod', 'contact_methods', 'schema:contactPoint', 'active', '{"inlined": false, "multivalued": true, "range": "ContactMethod", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.contact_value', 'property.foundation.contact_value', 'contact_value', 'Contact value such as email address, telephone number, fax number, URL, or handle.', 'NonEmptyString', 'contact_value', '', 'active', '{"range": "NonEmptyString", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.coordinate_system', 'property.foundation.coordinate_system', 'coordinate_system', 'Coordinate reference system or spatial reference identifier.', 'string', 'coordinate_system', '', 'active', '{"range": "string", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.country_code', 'property.foundation.country_code', 'country_code', 'Country code, preferably ISO 3166-1 alpha-2 when available.', 'CountryCode', 'country_code', '', 'active', '{"range": "CountryCode", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.covers_period', 'property.foundation.covers_period', 'covers_period', 'Reporting or coverage period for a document, observation, dataset, or source record.', 'TimeInterval', 'covers_period', '', 'active', '{"inlined": true, "range": "TimeInterval", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.currency_code', 'property.foundation.currency_code', 'currency_code', 'Currency code, preferably ISO 4217 when available.', 'CurrencyCode', 'currency_code', '', 'active', '{"examples": [{"value": "USD"}, {"value": "KRW"}], "range": "CurrencyCode", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.common_business.dataset_type', 'property.common_business.dataset_type', 'dataset_type', 'Type or category of dataset.', 'Classification', 'dataset_type', '', 'active', '{"inlined": false, "range": "Classification", "source_schema": "common_business_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.description', 'property.foundation.description', 'description', 'Human-readable description.', 'string', 'description', 'schema:description', 'active', '{"range": "string", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.display_name', 'property.foundation.display_name', 'display_name', 'Human-readable display label when it differs from the stable name.', 'string', 'display_name', 'skos:prefLabel', 'active', '{"range": "string", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.document_number', 'property.foundation.document_number', 'document_number', 'Human-facing document or record number.', 'string', 'document_number', '', 'active', '{"range": "string", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.document_status', 'property.foundation.document_status', 'document_status', 'Lifecycle status of a document, represented as governed classification rather than a fixed enum.', 'Classification', 'document_status', '', 'active', '{"inlined": false, "range": "Classification", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.document_type', 'property.foundation.document_type', 'document_type', 'Business, legal, administrative, or source type of document.', 'Classification', 'document_type', '', 'active', '{"inlined": false, "range": "Classification", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.common_business.documentation_url', 'property.common_business.documentation_url', 'documentation_url', 'URL for API documentation.', 'uri', 'documentation_url', 'schema:documentation', 'active', '{"range": "uri", "source_schema": "common_business_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.common_business.download_url', 'property.common_business.download_url', 'download_url', 'URL that downloads a dataset distribution or file.', 'uri', 'download_url', 'dcat:downloadURL', 'active', '{"range": "uri", "source_schema": "common_business_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.effective_date', 'property.foundation.effective_date', 'effective_date', 'Date on which a document, fact, status, or relationship becomes effective.', 'date', 'effective_date', '', 'active', '{"range": "date", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.end_date', 'property.foundation.end_date', 'end_date', 'End date of an interval.', 'date', 'end_date', 'TIME:hasEnd', 'active', '{"range": "date", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.evidence', 'property.foundation.evidence', 'evidence', 'Human-readable source evidence or reference text supporting a statement.', 'string', 'evidence', '', 'active', '{"multivalued": true, "range": "string", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.exact_match', 'property.foundation.exact_match', 'exact_match', 'Equivalent classification or concept in another scheme.', 'Classification', 'exact_match', 'skos:exactMatch', 'active', '{"inlined": false, "multivalued": true, "range": "Classification", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.expiration_date', 'property.foundation.expiration_date', 'expiration_date', 'Date on which a document, fact, status, identifier, or relationship expires.', 'date', 'expiration_date', 'schema:expires', 'active', '{"range": "date", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.common_business.family_name', 'property.common_business.family_name', 'family_name', 'Family name of a person.', 'string', 'family_name', 'schema:familyName', 'active', '{"range": "string", "source_schema": "common_business_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.common_business.founding_date', 'property.common_business.founding_date', 'founding_date', 'Date on which an organization was founded.', 'date', 'founding_date', 'schema:foundingDate', 'active', '{"range": "date", "source_schema": "common_business_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.full_address', 'property.foundation.full_address', 'full_address', 'Full address string as provided or normalized.', 'string', 'full_address', '', 'active', '{"range": "string", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.common_business.full_name', 'property.common_business.full_name', 'full_name', 'Full person name as represented by the source or canonical system.', 'string', 'full_name', 'schema:name', 'active', '{"range": "string", "source_schema": "common_business_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.geo_location', 'property.foundation.geo_location', 'geo_location', 'Geographic coordinate or spatial reference associated with an address or entity.', 'GeoLocation', 'geo_location', '', 'active', '{"inlined": true, "range": "GeoLocation", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.geohash', 'property.foundation.geohash', 'geohash', 'Geohash or equivalent compact spatial index.', 'string', 'geohash', '', 'active', '{"range": "string", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.common_business.given_name', 'property.common_business.given_name', 'given_name', 'Given name of a person.', 'string', 'given_name', 'schema:givenName', 'active', '{"range": "string", "source_schema": "common_business_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.identifier_scheme', 'property.foundation.identifier_scheme', 'identifier_scheme', 'Scheme, namespace, or authority under which an identifier value is meaningful.', 'ClassificationScheme', 'identifier_scheme', 'schema:propertyID', 'active', '{"inlined": false, "range": "ClassificationScheme", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.identifier_type', 'property.foundation.identifier_type', 'identifier_type', 'Governed type of identifier, such as registration number, tax identifier, LEI, DUNS, or source-local id.', 'Classification', 'identifier_type', '', 'active', '{"inlined": false, "range": "Classification", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.identifier_value', 'property.foundation.identifier_value', 'identifier_value', 'Identifier value exactly as represented after approved normalization.', 'NonEmptyString', 'identifier_value', 'schema:value', 'active', '{"range": "NonEmptyString", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.identifiers', 'property.foundation.identifiers', 'identifiers', 'Identifiers associated with a domain entity.', 'Identifier', 'identifiers', 'schema:identifier', 'active', '{"inlined": false, "multivalued": true, "range": "Identifier", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.identifies', 'property.foundation.identifies', 'identifies', 'Entity identified by this identifier.', 'Entity', 'identifies', '', 'active', '{"inlined": false, "range": "Entity", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.issued_date', 'property.foundation.issued_date', 'issued_date', 'Date an identifier, document, classification assignment, or statement was issued.', 'date', 'issued_date', 'schema:datePublished', 'active', '{"range": "date", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.issuer', 'property.foundation.issuer', 'issuer', 'Entity that issued a document, identifier, statement, or observation.', 'Entity', 'issuer', '', 'active', '{"inlined": false, "range": "Entity", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.issuing_authority', 'property.foundation.issuing_authority', 'issuing_authority', 'Entity that issued or maintains the identifier, document, classification, or statement.', 'Entity', 'issuing_authority', '', 'active', '{"inlined": false, "range": "Entity", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.latitude', 'property.foundation.latitude', 'latitude', 'Latitude coordinate in decimal degrees.', 'decimal', 'latitude', 'schema:latitude', 'active', '{"range": "decimal", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.common_business.legal_name', 'property.common_business.legal_name', 'legal_name', 'Official legal name of an organization.', 'string', 'legal_name', 'schema:legalName', 'active', '{"range": "string", "source_schema": "common_business_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.common_business.license_uri', 'property.common_business.license_uri', 'license_uri', 'URI for the license that governs a dataset, API, or document.', 'uri', 'license_uri', 'schema:license', 'active', '{"range": "uri", "source_schema": "common_business_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.longitude', 'property.foundation.longitude', 'longitude', 'Longitude coordinate in decimal degrees.', 'decimal', 'longitude', 'schema:longitude', 'active', '{"range": "decimal", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.common_business.modified_date', 'property.common_business.modified_date', 'modified_date', 'Date on which a dataset, API, document, or record was last modified.', 'date', 'modified_date', 'schema:dateModified', 'active', '{"range": "date", "source_schema": "common_business_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.name', 'property.foundation.name', 'name', 'Human-readable name or label.', 'NonEmptyString', 'name', 'schema:name', 'active', '{"range": "NonEmptyString", "required": true, "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.narrower', 'property.foundation.narrower', 'narrower', 'Narrower classification or concept.', 'Classification', 'narrower', 'skos:narrower', 'active', '{"inlined": false, "multivalued": true, "range": "Classification", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.observed_amount', 'property.foundation.observed_amount', 'observed_amount', 'Observed monetary amount.', 'Money', 'observed_amount', '', 'active', '{"inlined": false, "range": "Money", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.observed_date', 'property.foundation.observed_date', 'observed_date', 'Date on which an observation was made or reported.', 'date', 'observed_date', '', 'active', '{"range": "date", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.observed_quantity', 'property.foundation.observed_quantity', 'observed_quantity', 'Observed numeric quantity.', 'Quantity', 'observed_quantity', '', 'active', '{"inlined": false, "range": "Quantity", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.observed_value', 'property.foundation.observed_value', 'observed_value', 'Observed scalar value when the value is not modeled as money or quantity.', 'string', 'observed_value', '', 'active', '{"range": "string", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.common_business.organization_type', 'property.common_business.organization_type', 'organization_type', 'Type of organization, preferably from a governed classification scheme.', 'Classification', 'organization_type', '', 'active', '{"inlined": false, "range": "Classification", "source_schema": "common_business_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.common_business.parent_organization', 'property.common_business.parent_organization', 'parent_organization', 'Parent or controlling organization.', 'Organization', 'parent_organization', 'schema:parentOrganization', 'active', '{"inlined": false, "range": "Organization", "source_schema": "common_business_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.postal_code', 'property.foundation.postal_code', 'postal_code', 'Postal or ZIP code.', 'string', 'postal_code', 'schema:postalCode', 'active', '{"range": "string", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.preferred', 'property.foundation.preferred', 'preferred', 'Whether the contact method or value is preferred.', 'boolean', 'preferred', '', 'active', '{"range": "boolean", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.common_business.provider', 'property.common_business.provider', 'provider', 'Entity that provides or operates an API or service.', 'Entity', 'provider', 'schema:provider', 'active', '{"inlined": false, "range": "Entity", "source_schema": "common_business_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.common_business.publisher', 'property.common_business.publisher', 'publisher', 'Entity that publishes a dataset, document, or API.', 'Entity', 'publisher', 'dcat:publisher', 'active', '{"inlined": false, "range": "Entity", "source_schema": "common_business_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.quantity_value', 'property.foundation.quantity_value', 'quantity_value', 'Numeric quantity value.', 'decimal', 'quantity_value', '', 'active', '{"range": "decimal", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.relationship_type', 'property.foundation.relationship_type', 'relationship_type', 'Type of relationship between two entities.', 'Classification', 'relationship_type', '', 'active', '{"inlined": false, "range": "Classification", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.role', 'property.foundation.role', 'role', 'Role played by an entity within a relationship or observation context.', 'Classification', 'role', '', 'active', '{"inlined": false, "range": "Classification", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.same_as', 'property.foundation.same_as', 'same_as', 'External URI judged equivalent to this object.', 'uri', 'same_as', 'schema:sameAs', 'active', '{"multivalued": true, "range": "uri", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.scheme_code', 'property.foundation.scheme_code', 'scheme_code', 'Stable code for a scheme, namespace, or controlled vocabulary.', 'NonEmptyString', 'scheme_code', '', 'active', '{"range": "NonEmptyString", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.scheme_name', 'property.foundation.scheme_name', 'scheme_name', 'Human-readable name of a scheme, namespace, or controlled vocabulary.', 'string', 'scheme_name', '', 'active', '{"range": "string", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.scheme_uri', 'property.foundation.scheme_uri', 'scheme_uri', 'URI or canonical namespace for a scheme when available.', 'uri', 'scheme_uri', '', 'active', '{"range": "uri", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.source_entity', 'property.foundation.source_entity', 'source_entity', 'Source entity of a directed relationship.', 'Entity', 'source_entity', '', 'active', '{"inlined": false, "range": "Entity", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.start_date', 'property.foundation.start_date', 'start_date', 'Start date of an interval.', 'date', 'start_date', 'TIME:hasBeginning', 'active', '{"range": "date", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.street_address', 'property.foundation.street_address', 'street_address', 'Street address line.', 'string', 'street_address', 'schema:streetAddress', 'active', '{"range": "string", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.subject', 'property.foundation.subject', 'subject', 'Entity that a document, relationship, statement, or observation is about.', 'Entity', 'subject', 'schema:about', 'active', '{"inlined": false, "range": "Entity", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.target_entity', 'property.foundation.target_entity', 'target_entity', 'Target entity of a directed relationship.', 'Entity', 'target_entity', '', 'active', '{"inlined": false, "range": "Entity", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.temporal_precision', 'property.foundation.temporal_precision', 'temporal_precision', 'Precision of a temporal value.', 'TemporalPrecisionEnum', 'temporal_precision', '', 'active', '{"range": "TemporalPrecisionEnum", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.unit_code', 'property.foundation.unit_code', 'unit_code', 'Unit of measure code.', 'string', 'unit_code', '', 'active', '{"range": "string", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.common_business.url', 'property.common_business.url', 'url', 'URL associated with the object.', 'uri', 'url', 'schema:url', 'active', '{"range": "uri", "source_schema": "common_business_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.valid_during', 'property.foundation.valid_during', 'valid_during', 'Time interval during which an object, statement, identifier, or relationship is valid.', 'TimeInterval', 'valid_during', '', 'active', '{"inlined": true, "range": "TimeInterval", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.foundation.verified', 'property.foundation.verified', 'verified', 'Whether the contact method or value has been verified.', 'boolean', 'verified', '', 'active', '{"range": "boolean", "source_schema": "foundation_canonical_model.linkml.yaml"}'::jsonb, '[]'::jsonb),
  ('property.common_business.version_label', 'property.common_business.version_label', 'version_label', 'Human-readable version label.', 'string', 'version_label', 'schema:version', 'active', '{"range": "string", "source_schema": "common_business_model.linkml.yaml"}'::jsonb, '[]'::jsonb)
on conflict (id) do update set stable_key = excluded.stable_key, name = excluded.name, description = excluded.description, broad_datatype = excluded.broad_datatype, linkml_slot = excluded.linkml_slot, external_uri = excluded.external_uri, status = excluded.status, metadata = excluded.metadata, evidence = excluded.evidence, updated_at = now();

drop table if exists context_platform.binding_evidence cascade;
drop table if exists context_platform.bindings cascade;
drop table if exists context_platform.capability_operations cascade;
drop table if exists context_platform.plan_steps cascade;
drop table if exists context_platform.execution_logs cascade;
drop table if exists context_platform.canonical_relations cascade;
drop table if exists context_platform.canonical_class_slots cascade;
drop table if exists context_platform.canonical_classes cascade;
drop table if exists context_platform.canonical_enum_values cascade;
drop table if exists context_platform.canonical_enums cascade;
drop table if exists context_platform.canonical_slots cascade;
drop table if exists context_platform.canonical_types cascade;
