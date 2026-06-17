do $$
begin
  if exists (
    select 1
    from information_schema.schemata
    where schema_name = 'semantic_layer'
  ) and not exists (
    select 1
    from information_schema.schemata
    where schema_name = 'semantic_platform'
  ) then
    execute 'alter schema semantic_layer rename to semantic_platform';
  end if;
end
$$;
