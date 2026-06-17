alter table semantic_platform.semantic_types
  add column if not exists parent_entity_id text not null default '',
  add column if not exists parent_entity_name text not null default '';

create index if not exists idx_semantic_types_parent_entity_id
  on semantic_platform.semantic_types (parent_entity_id);
