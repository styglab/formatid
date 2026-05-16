# execution contracts

Reviewed execution contracts live here because `semantic_platform` owns the
declarative semantic source of truth.

These files describe:

- semantic capability ids and their approved operation/tool implementations
- operation physical field mappings to canonical `SemanticType` ids

The canonical execution/mapping unit is the API operation, not the provider.
`provider` remains provenance and adapter metadata.

`pubdata_mcp` reads these contracts through the semantic platform API and uses
them as runtime input. It must not generate proposals or mutate these files.
