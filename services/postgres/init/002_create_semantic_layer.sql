create schema if not exists semantic_layer;

create table if not exists semantic_layer.semantic_types (
  id text primary key,
  urn text not null unique,
  name text not null unique,
  description text not null default '',
  datatype text not null default 'string',
  entity_kind text not null default 'scalar',
  aliases jsonb not null default '[]'::jsonb,
  owners jsonb not null default '[]'::jsonb,
  tags jsonb not null default '[]'::jsonb,
  documentation text not null default '',
  status text not null default 'draft',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists semantic_layer.semantic_relationships (
  id text primary key,
  source_id text not null references semantic_layer.semantic_types(id) on delete cascade,
  source_name text not null,
  target_id text not null references semantic_layer.semantic_types(id) on delete cascade,
  target_name text not null,
  relation_type text not null,
  status text not null default 'draft',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists semantic_layer.proposals (
  id text primary key,
  source_type text not null,
  title text not null,
  entity_type text not null,
  entity_id text not null,
  change_type text not null,
  payload jsonb not null,
  status text not null default 'pending_review',
  reviewed_by text,
  reviewed_at timestamptz,
  created_at timestamptz not null default now()
);

create index if not exists idx_semantic_types_name
  on semantic_layer.semantic_types (name);

create index if not exists idx_semantic_types_status
  on semantic_layer.semantic_types (status);

create index if not exists idx_semantic_relationships_source_id
  on semantic_layer.semantic_relationships (source_id);

create index if not exists idx_semantic_relationships_target_id
  on semantic_layer.semantic_relationships (target_id);

create index if not exists idx_semantic_relationships_status
  on semantic_layer.semantic_relationships (status);

create index if not exists idx_semantic_proposals_status
  on semantic_layer.proposals (status);

create index if not exists idx_semantic_proposals_entity
  on semantic_layer.proposals (entity_type, entity_id);
