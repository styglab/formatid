create schema if not exists semantic_platform;

create table if not exists semantic_platform.semantic_types (
  id text primary key,
  urn text not null unique,
  namespace text not null default 'public',
  name text not null unique,
  description text not null default '',
  datatype text not null default 'string',
  entity_kind text not null default 'scalar',
  version text not null default '1.0.0',
  lifecycle text not null default 'draft',
  constraint_spec jsonb not null default '{}'::jsonb,
  identity_type text not null default '',
  aliases jsonb not null default '[]'::jsonb,
  owners jsonb not null default '[]'::jsonb,
  tags jsonb not null default '[]'::jsonb,
  documentation text not null default '',
  created_by text not null default 'system',
  reviewed_by text,
  approved_at timestamptz,
  evidence jsonb not null default '[]'::jsonb,
  confidence numeric(5,4),
  status text not null default 'draft',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint chk_semantic_types_lifecycle
    check (lifecycle in ('draft', 'review', 'approved', 'published', 'deprecated'))
);

create table if not exists semantic_platform.semantic_relationships (
  id text primary key,
  source_id text not null references semantic_platform.semantic_types(id) on delete cascade,
  source_name text not null,
  target_id text not null references semantic_platform.semantic_types(id) on delete cascade,
  target_name text not null,
  relation_type text not null,
  version text not null default '1.0.0',
  lifecycle text not null default 'draft',
  created_by text not null default 'system',
  reviewed_by text,
  approved_at timestamptz,
  evidence jsonb not null default '[]'::jsonb,
  confidence numeric(5,4),
  status text not null default 'draft',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint chk_semantic_relationships_lifecycle
    check (lifecycle in ('draft', 'review', 'approved', 'published', 'deprecated'))
);

create table if not exists semantic_platform.proposals (
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
  approved_at timestamptz,
  status text not null default 'pending_review',
  reviewed_at timestamptz,
  created_at timestamptz not null default now()
);

create index if not exists idx_semantic_types_name
  on semantic_platform.semantic_types (name);

create index if not exists idx_semantic_types_status
  on semantic_platform.semantic_types (status);

create index if not exists idx_semantic_relationships_source_id
  on semantic_platform.semantic_relationships (source_id);

create index if not exists idx_semantic_relationships_target_id
  on semantic_platform.semantic_relationships (target_id);

create index if not exists idx_semantic_relationships_status
  on semantic_platform.semantic_relationships (status);

create index if not exists idx_semantic_proposals_status
  on semantic_platform.proposals (status);

create index if not exists idx_semantic_proposals_entity
  on semantic_platform.proposals (entity_type, entity_id);
