create schema if not exists semantic_platform;

create table if not exists semantic_platform.onboarding_runs (
  id text primary key,
  source_id text not null references semantic_platform.execution_sources(id) on delete cascade,
  status text not null default 'started',
  stage text not null default 'source_uploaded',
  trigger_type text not null default 'source_upload',
  created_by text not null default 'system',
  metadata jsonb not null default '{}'::jsonb,
  started_at timestamptz not null default now(),
  completed_at timestamptz,
  updated_at timestamptz not null default now(),
  constraint chk_onboarding_runs_status
    check (status in ('started', 'discovering', 'needs_mapping', 'needs_review', 'approved', 'published', 'failed', 'cancelled')),
  constraint chk_onboarding_runs_stage
    check (stage in ('source_uploaded', 'evidence_captured', 'operations_discovered', 'schemas_discovered', 'suggestions_generated', 'proposals_created', 'reviewed', 'published'))
);

create table if not exists semantic_platform.evidence_snapshots (
  id text primary key,
  run_id text not null references semantic_platform.onboarding_runs(id) on delete cascade,
  source_id text not null references semantic_platform.execution_sources(id) on delete cascade,
  snapshot_type text not null default 'source_upload',
  content_hash text not null default '',
  source_ref jsonb not null default '{}'::jsonb,
  operation_evidence jsonb not null default '[]'::jsonb,
  schema_evidence jsonb not null default '[]'::jsonb,
  sample_values jsonb not null default '{}'::jsonb,
  ai_context jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  constraint chk_evidence_snapshots_snapshot_type
    check (snapshot_type in ('source_upload', 'operation_discovery', 'schema_discovery', 'endpoint_check', 'manual_review'))
);

create table if not exists semantic_platform.proposal_bundles (
  id text primary key,
  run_id text not null references semantic_platform.onboarding_runs(id) on delete cascade,
  source_id text not null references semantic_platform.execution_sources(id) on delete cascade,
  evidence_snapshot_id text references semantic_platform.evidence_snapshots(id) on delete set null,
  title text not null,
  status text not null default 'draft',
  summary jsonb not null default '{}'::jsonb,
  created_by text not null default 'system',
  reviewed_by text,
  reviewed_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint chk_proposal_bundles_status
    check (status in ('draft', 'pending_review', 'partially_reviewed', 'approved', 'rejected', 'published'))
);

create table if not exists semantic_platform.proposal_bundle_items (
  bundle_id text not null references semantic_platform.proposal_bundles(id) on delete cascade,
  proposal_id text not null references semantic_platform.proposals(id) on delete cascade,
  item_order integer not null default 100,
  created_at timestamptz not null default now(),
  primary key (bundle_id, proposal_id)
);

create table if not exists semantic_platform.work_queue_tasks (
  id text primary key,
  run_id text not null references semantic_platform.onboarding_runs(id) on delete cascade,
  source_id text not null references semantic_platform.execution_sources(id) on delete cascade,
  evidence_snapshot_id text references semantic_platform.evidence_snapshots(id) on delete set null,
  operation_id text references semantic_platform.execution_operations(id) on delete cascade,
  field_id text references semantic_platform.operation_fields(id) on delete cascade,
  task_type text not null,
  status text not null default 'open',
  priority integer not null default 100,
  title text not null,
  payload jsonb not null default '{}'::jsonb,
  proposal_id text references semantic_platform.proposals(id) on delete set null,
  assigned_to text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint chk_work_queue_tasks_task_type
    check (task_type in ('discover_operations', 'review_schema', 'map_field', 'review_transform', 'create_variant', 'bind_capability', 'review_proposal')),
  constraint chk_work_queue_tasks_status
    check (status in ('open', 'in_progress', 'blocked', 'proposal_created', 'approved', 'rejected', 'closed'))
);

create table if not exists semantic_platform.capability_operation_bindings (
  id text primary key,
  capability_id text not null references semantic_platform.capabilities(id) on delete cascade,
  operation_id text not null references semantic_platform.execution_operations(id) on delete cascade,
  variant_id text references semantic_platform.operation_variants(id) on delete set null,
  run_id text references semantic_platform.onboarding_runs(id) on delete set null,
  evidence_snapshot_id text references semantic_platform.evidence_snapshots(id) on delete set null,
  binding_kind text not null default 'implementation',
  input_bindings jsonb not null default '[]'::jsonb,
  output_bindings jsonb not null default '[]'::jsonb,
  fixed_arguments jsonb not null default '{}'::jsonb,
  status text not null default 'draft',
  confidence numeric(5,4),
  created_by text not null default 'system',
  reviewed_by text,
  approved_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint chk_capability_operation_bindings_kind
    check (binding_kind in ('implementation', 'variant_implementation', 'candidate')),
  constraint chk_capability_operation_bindings_status
    check (status in ('draft', 'pending_review', 'approved', 'published', 'deprecated')),
  constraint uq_capability_operation_bindings unique (capability_id, operation_id, variant_id)
);

create index if not exists idx_onboarding_runs_source_id
  on semantic_platform.onboarding_runs (source_id);

create index if not exists idx_evidence_snapshots_run_id
  on semantic_platform.evidence_snapshots (run_id);

create index if not exists idx_proposal_bundles_run_id
  on semantic_platform.proposal_bundles (run_id);

create index if not exists idx_proposal_bundle_items_proposal_id
  on semantic_platform.proposal_bundle_items (proposal_id);

create index if not exists idx_work_queue_tasks_run_status
  on semantic_platform.work_queue_tasks (run_id, status);

create index if not exists idx_work_queue_tasks_field_id
  on semantic_platform.work_queue_tasks (field_id);

create index if not exists idx_capability_operation_bindings_capability_id
  on semantic_platform.capability_operation_bindings (capability_id);

create index if not exists idx_capability_operation_bindings_operation_id
  on semantic_platform.capability_operation_bindings (operation_id);
