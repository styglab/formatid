alter table semantic_platform.semantic_types
  add column if not exists semantic_role text not null default '';
