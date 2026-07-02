# formatid

AI-ready data platform for domain ingestion, canonical context, and AI access
apps. The current public-data direction is a **Context Platform**: API
documents and manual authoring produce reviewed Source Catalog, Canonical Model,
Binding Layer, and Capability Catalog data for server-side LLM planning.

The platform separates runtime execution, reusable service capabilities, and
app-owned domain logic. AI-facing apps such as MCP servers, RAG APIs, and domain
assistants consume reviewed canonical context and Planner Service results.

```text
Natural language
  -> canonical input/output understanding
  -> capability resolution
  -> validated plan
  -> source operation execution
  -> binding-based canonical normalization
  -> LLM MCP / RAG / domain apps
```

## Architecture

- `core/*`: generic runtime, manifest catalog, observability, MCP tool loading,
  and cross-app contracts.
- `services/*`: platform services and backing capabilities such as Nginx,
  Postgres, Redis, platform API, platform dashboard, embedding service, and
  `services/context_platform`.
- `apps/*`: app orchestration, business rules, persistence, ontology, semantic transformers, and user-facing AI apps.

See [docs/folder_structure_ko.md](docs/folder_structure_ko.md) for the current
folder structure and ownership rules.

Layer rule:

- Generic contracts belong in `core/*`.
- Reusable IO or processing belongs in `services/*`.
- Domain meaning belongs in `apps/<app>/*`.

## Current Apps

- `apps/pubdata_mcp`: optional LLM MCP adapter surface for planner-level tools
  backed by `services/context_platform`.

## Platform Services

The active platform service set is intentionally small:

- `nginx`: single local ingress
- `postgres`: runtime relational store
- `redis`: runtime queue/cache store
- `platform-api`: operational API
- `platform-dashboard`: operational UI

App-required services such as `prefect-*`, `minio`, and `qdrant` are enabled
only when an app or platform control plane declares them.

## Context Platform

For public API orchestration, `services/context_platform` is the current
implementation path for the Context Platform:

- Source Catalog: `sources`, `source_documents`, `source_operations`,
  `source_parameters`, and `source_fields`.
- Canonical Model: LinkML-compatible classes, reusable slots, types, enums,
  class-slot usages, and relations. PostgreSQL is the runtime registry; LinkML
  YAML/JSON is the import/export format, not the storage engine.
- Binding Layer: input/output bindings from source parameters and fields to
  canonical slots or class-slot usages, including evidence, confidence, status,
  and transforms.
- Capability Catalog: planner-facing business capabilities, inputs, outputs,
  and links to `source_operations`.
- Planner Service: server-side planning, validation, and validated execution.
- LLM MCP Adapter: optional high-level adapter exposing plan/execute/explain
  tools to an LLM client.

The core rule is that `source_operations` are the executable operations. Do not
add a separate Operation Registry. Do not add a standalone Semantic Registry:
slot descriptions, aliases, mappings, annotations, and validation constraints
belong on canonical slots in the LinkML-compatible Canonical Model.

`apps/pubdata_mcp` is intentionally not the Context Platform. It is an optional
LLM MCP adapter and must not execute raw source operations directly.

```text
Question
  -> LLM MCP Adapter plan_request
  -> Planner Service /planner/plan
  -> validated plan
  -> Planner Service /planner/execute
  -> source_operations-based provider call
  -> bindings-based canonical result
```

Planner execution must not contain provider/domain choices such as "if this
Korean word appears, choose this operation". Those choices belong in reviewed
Capability Catalog and Binding Layer data.

LLM-generated or automatically generated artifacts are never approved
automatically. Canonical classes, slots, types, enums, class-slot usages,
bindings, capabilities, and capability-operation links follow:

```text
proposed -> reviewed -> approved -> published
```

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
