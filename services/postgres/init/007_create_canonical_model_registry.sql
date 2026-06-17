create table if not exists semantic_platform.canonical_entities (
  id text primary key,
  semantic_type_id text references semantic_platform.semantic_types(id) on delete set null,
  name text not null unique,
  namespace text not null default 'public',
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
  updated_at timestamptz not null default now()
);

create table if not exists semantic_platform.canonical_attributes (
  id text primary key,
  entity_id text not null references semantic_platform.canonical_entities(id) on delete cascade,
  semantic_type_id text references semantic_platform.semantic_types(id) on delete set null,
  name text not null,
  namespace text not null default 'public',
  description text not null default '',
  datatype text not null default 'string',
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
  constraint uq_canonical_attributes_name unique (entity_id, name)
);

create table if not exists semantic_platform.canonical_relations (
  id text primary key,
  source_entity_id text not null references semantic_platform.canonical_entities(id) on delete cascade,
  target_entity_id text not null references semantic_platform.canonical_entities(id) on delete cascade,
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

create index if not exists idx_canonical_entities_semantic_type_id
  on semantic_platform.canonical_entities (semantic_type_id);

create index if not exists idx_canonical_attributes_entity_id
  on semantic_platform.canonical_attributes (entity_id);

create index if not exists idx_canonical_attributes_semantic_type_id
  on semantic_platform.canonical_attributes (semantic_type_id);

create index if not exists idx_canonical_relations_source_entity_id
  on semantic_platform.canonical_relations (source_entity_id);

create index if not exists idx_canonical_relations_target_entity_id
  on semantic_platform.canonical_relations (target_entity_id);

insert into semantic_platform.canonical_entities (
  id, semantic_type_id, name, namespace, description, version, lifecycle, status,
  metadata, created_by, reviewed_by, approved_at, evidence, confidence, created_at, updated_at
)
select
  st.id,
  st.id,
  st.name,
  coalesce(st.namespace, 'public'),
  coalesce(st.description, ''),
  coalesce(st.version, '1.0.0'),
  coalesce(st.lifecycle, 'draft'),
  coalesce(st.status, 'draft'),
  '{}'::jsonb,
  coalesce(st.created_by, 'system'),
  st.reviewed_by,
  st.approved_at,
  coalesce(st.evidence, '[]'::jsonb),
  st.confidence,
  st.created_at,
  st.updated_at
from semantic_platform.semantic_types st
where st.entity_kind = 'entity'
  and not exists (
    select 1 from semantic_platform.canonical_entities ce where ce.id = st.id
  );

insert into semantic_platform.canonical_attributes (
  id, entity_id, semantic_type_id, name, namespace, description, datatype, identity_role,
  version, lifecycle, status, metadata, created_by, reviewed_by, approved_at, evidence,
  confidence, created_at, updated_at
)
select
  st.id,
  st.parent_entity_id,
  st.id,
  st.name,
  coalesce(st.namespace, 'public'),
  coalesce(st.description, ''),
  coalesce(st.datatype, 'string'),
  coalesce(st.identity_type, ''),
  coalesce(st.version, '1.0.0'),
  coalesce(st.lifecycle, 'draft'),
  coalesce(st.status, 'draft'),
  '{}'::jsonb,
  coalesce(st.created_by, 'system'),
  st.reviewed_by,
  st.approved_at,
  coalesce(st.evidence, '[]'::jsonb),
  st.confidence,
  st.created_at,
  st.updated_at
from semantic_platform.semantic_types st
where st.entity_kind = 'attribute'
  and st.parent_entity_id <> ''
  and exists (
    select 1 from semantic_platform.canonical_entities ce where ce.id = st.parent_entity_id
  )
  and not exists (
    select 1 from semantic_platform.canonical_attributes ca where ca.id = st.id
  );

insert into semantic_platform.canonical_relations (
  id, source_entity_id, target_entity_id, relation_type, forward_label, reverse_label,
  version, lifecycle, status, metadata, created_by, reviewed_by, approved_at,
  evidence, confidence, created_at, updated_at
)
select
  rel.id,
  rel.source_id,
  rel.target_id,
  rel.relation_type,
  rel.relation_type,
  '',
  coalesce(rel.version, '1.0.0'),
  coalesce(rel.lifecycle, 'draft'),
  coalesce(rel.status, 'draft'),
  '{}'::jsonb,
  coalesce(rel.created_by, 'system'),
  rel.reviewed_by,
  rel.approved_at,
  coalesce(rel.evidence, '[]'::jsonb),
  rel.confidence,
  rel.created_at,
  rel.updated_at
from semantic_platform.semantic_relationships rel
where exists (
  select 1 from semantic_platform.canonical_entities ce where ce.id = rel.source_id
)
and exists (
  select 1 from semantic_platform.canonical_entities ce where ce.id = rel.target_id
)
and not exists (
  select 1 from semantic_platform.canonical_relations cr where cr.id = rel.id
);

alter table semantic_platform.field_mappings
  drop constraint if exists field_mappings_canonical_attribute_id_fkey;

alter table semantic_platform.field_mappings
  add constraint field_mappings_canonical_attribute_id_fkey
  foreign key (canonical_attribute_id)
  references semantic_platform.canonical_attributes(id)
  on delete set null;
