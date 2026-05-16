# semantic_mapper

Maps raw resource fields to canonical `SemanticType` values.

The mapper should be LLM-first with reviewed exact-match support:

- `rules/`: reviewed exact mappings such as 사업자번호 -> business_registration_number
- `llm/`: LLM-assisted interpretation with evidence
- `hybrid/`: orchestration that combines reviewed mappings, catalog lookup, and
  LLM proposals

Mapper output should be reviewable proposals before it changes canonical catalog
metadata.
