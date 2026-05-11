# formatid

AI-ready data platform for domain ingestion, semantic enrichment, and AI access apps.

The platform separates runtime execution, reusable service capabilities, and app-owned domain logic. Domain apps build canonical data and semantic objects; AI-facing apps such as MCP servers, RAG APIs, and domain assistants consume those layers.

```text
Raw sources
  -> canonical normalized data
  -> app semantic layer
  -> MCP / RAG / domain apps
```

## Architecture

- `core/*`: execution runtime, catalog, observability, and generic semantic contracts.
- `services/*`: platform services and backing capabilities such as MinIO, Nginx, Prefect, Qdrant, platform API, and platform dashboard.
- `apps/*`: app orchestration, business rules, persistence, ontology, semantic transformers, and user-facing AI apps.

Layer rule:

- Generic contracts belong in `core/*`.
- Reusable IO or processing belongs in `services/*`.
- Domain meaning belongs in `apps/<app>/*`.

## Current Apps

- `apps/g2b/pipeline`: G2B ingestion pipeline. It collects raw API rows, normalizes bid notice/license/region data, and builds G2B semantic objects.
- `apps/g2b/mcp`: MCP access app over G2B data.

```text
apps/g2b/
  ontology/   domain entities, relationships, taxonomy, and inference helpers
  pipeline/   raw ingest -> canonical normalized tables -> semantic objects/documents
  mcp/        AI tool surface over canonical and semantic G2B data
  rag/        future semantic retrieval app
```

## Platform Services

The active platform service set is intentionally small:

- `nginx`: single local ingress
- `postgres`: runtime relational store
- `redis`: runtime queue/cache store
- `platform-api`: operational API
- `platform-dashboard`: operational UI

App-required services such as `prefect-*`, `minio`, and `qdrant` are enabled only when an app declares them.

`apps/g2b/pipeline/app` is organized as:

- `flows/`: Prefect flow wiring only.
- `tasks/`: Prefect task boundaries.
- `steps/`: pure transformation and parsing logic.
- `repositories/`: database access.
- `service/`: app runner helpers and locks.
- `semantic/`: G2B ontology, relationship names, tags, and semantic document builders.

## Semantic Layer

The semantic layer should not be a second database first. Start as a projection over canonical tables:

- entity types, such as `BidNotice`, `Agency`, `LicenseConstraint`, `ParticipationRegion`
- relationships, such as `issued_by`, `requires`, `restricted_to`, `categorized_as`
- semantic tags, such as `government_procurement`, `regulated_license`, `region_restricted`
- AI retrieval documents derived from semantic objects

`core/semantic` may contain only generic contracts. G2B ontology and meaning live under `apps/g2b/ontology`; pipeline-specific semantic builders live under `apps/g2b/pipeline/app/semantic`.

## Run

Core platform only:

```bash
docker compose --env-file deploy/compose/env/compose.env -f deploy/compose/docker-compose.yml up -d --build
```

G2B apps, including the Prefect control plane required by `apps/g2b/pipeline`:

```bash
docker compose --env-file deploy/compose/env/compose.env -f deploy/compose/docker-compose.yml --profile g2b up -d --build
```

Stop:

```bash
docker compose --env-file deploy/compose/env/compose.env -f deploy/compose/docker-compose.yml down
```

Logs:

```bash
docker compose --env-file deploy/compose/env/compose.env -f deploy/compose/docker-compose.yml logs -f g2b-pipeline-worker
docker compose --env-file deploy/compose/env/compose.env -f deploy/compose/docker-compose.yml logs -f g2b-mcp
docker compose --env-file deploy/compose/env/compose.env -f deploy/compose/docker-compose.yml logs -f platform-api
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

Keep future cleanup focused on these areas:

- Remove stale app references instead of reviving old paths such as `apps/g2b_pipeline`, `apps/g2b_mcp`, `apps/shared/*`, or `apps/spec_rag/*`.
- Simplify `apps/g2b/pipeline/app/service/ingest.py`; flow orchestration now lives in `flows/`, so sync runner helpers should be reduced or renamed.
- Move MCP responses toward semantic objects/documents instead of raw row-shaped responses.
- Add a dedicated semantic indexing app only after semantic documents stabilize.
