# storage

Storage abstraction for future persistence.

Initial recommendation:

- Postgres JSONB for catalog/resource/proposal metadata
- pgvector for semantic retrieval

Do not introduce a graph database as the source of truth until the semantic
model stabilizes.
