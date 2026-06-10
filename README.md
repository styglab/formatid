# formatid

AI-ready data platform for domain ingestion, semantic layer context, and AI access
apps. The current public-data direction is a **Semantic Layer Platform**:
public API documents and manual authoring both produce reviewed semantic,
capability, and execution-contract context for LLM planning.

The platform separates runtime execution, reusable service capabilities, and
app-owned domain logic. AI-facing apps such as MCP servers, RAG APIs, and domain
assistants consume reviewed semantic catalog data and execution contracts.

```text
Natural language
  -> semantic understanding
  -> capability resolution
  -> execution planning
  -> API orchestration
  -> semantic normalization / integration
  -> MCP / RAG / domain apps
```

## Architecture

- `core/*`: generic runtime, manifest catalog, observability, MCP tool loading,
  and cross-app contracts.
- `services/*`: platform services and backing capabilities such as Nginx,
  Postgres, Redis, platform API, platform dashboard, embedding service, and
  `services/semantic_layer`.
- `apps/*`: app orchestration, business rules, persistence, ontology, semantic transformers, and user-facing AI apps.

See [docs/folder_structure_ko.md](docs/folder_structure_ko.md) for the current
folder structure and ownership rules.

Layer rule:

- Generic contracts belong in `core/*`.
- Reusable IO or processing belongs in `services/*`.
- Domain meaning belongs in `apps/<app>/*`.

## Current Apps

- `apps/pubdata_mcp`: MCP runtime for approved semantic execution contracts
  produced by `services/semantic_layer`.

## Platform Services

The active platform service set is intentionally small:

- `nginx`: single local ingress
- `postgres`: runtime relational store
- `redis`: runtime queue/cache store
- `platform-api`: operational API
- `platform-dashboard`: operational UI

App-required services such as `prefect-*`, `minio`, and `qdrant` are enabled
only when an app or platform control plane declares them.

## Semantic Layer

For public API orchestration, `services/semantic_layer` is the declarative
semantic layer:

- Canonical Semantic Model: shared semantic types and relationships.
- Capability Catalog: provider-neutral capabilities optimized for retrieval and
  planner grounding.
- Execution Catalog: resources, operations, contracts, variants, mappings, and
  implementation metadata needed to call provider APIs.
- Governance Context: proposals, lineage, evidence, review decisions, and
  deprecation/merge decisions.
- Catalog Versions: approved declarative catalog snapshots for audit, export,
  dashboard read-only viewing, diff, and restore.

`apps/pubdata_mcp` is intentionally not the semantic layer. It is the MCP/tool
runtime and deterministic contract interpreter.

```text
Question
  -> pubdata_mcp semantic_query
  -> semantic_layer capability retrieval + planner
  -> semantic execution graph
  -> pubdata_mcp contract interpreter
  -> provider APIs
  -> structured semantic result
```

The executor must not contain provider/domain choices such as "if this Korean
word appears, choose this operation". Those choices belong in reviewed catalog
data as capabilities, operation variants, contracts, and field mappings.

Approved catalog mutations create catalog versions. A version snapshot contains
the declarative catalog only: semantic types, entities, capabilities, resources,
operations, contracts, variants, field mappings, implementations, join rules,
dependencies, and planning examples. Derived retrieval artifacts such as
capability documents/vectors, endpoint checks, proposals, and source evidence
remain outside the snapshot. Versions can be viewed read-only in the dashboard,
exported as JSON, compared, or restored; restore creates a new active version
instead of overwriting version history.

## Run

Run the generated compose stack:

```bash
docker compose --env-file deploy/compose/env/compose.env -f deploy/compose/docker-compose.yml up -d --build
```

Stop:

```bash
docker compose --env-file deploy/compose/env/compose.env -f deploy/compose/docker-compose.yml down
```

Logs:

```bash
docker compose --env-file deploy/compose/env/compose.env -f deploy/compose/docker-compose.yml logs -f platform-api
docker compose --env-file deploy/compose/env/compose.env -f deploy/compose/docker-compose.yml logs -f pubdata-mcp
```

## Validate

```bash
python3 scripts/ops.py validate-config
python3 scripts/ops.py lint-boundaries
python3 scripts/generate_compose.py --check
python3 -m unittest discover -s tests -t .
```

Run all checks:

```bash
python3 scripts/ops.py check-all
```

## Manifests

Compose is generated from manifests. After changing app/service manifests, run:

```bash
python3 scripts/generate_compose.py
python3 scripts/ops.py validate-config
```

Manifest sources:

- `services/*/manifests/*.json`
- `apps/**/manifests/app.json`
- `apps/**/manifests/services/*.json`

## Cleanup Notes

Current cleanup policy:

- Keep canonical source documents in Semantic Layer uploads/object storage, not in root `sources/`.
- Keep embedding models under `data/models/embeddings/`.
- Keep generated Python caches and temporary backup trees out of the repo.
- Retired app code is kept under `tmp/retired_apps/` for reference only.
- Do not revive old paths such as `apps/g2b_pipeline`, `apps/g2b_mcp`,
  `apps/g2b/*`, `apps/shared/*`, `apps/spec_rag/*`, or temporary proposal-agent backups.
- Keep MCP responses structured and semantic; natural-language synthesis is a
  later layer, not the execution contract.
