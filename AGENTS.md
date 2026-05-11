# AGENTS.md

## Purpose

This repository is an AI-ready data platform. Keep the structure simple and keep domain meaning in apps.

```text
Raw sources -> canonical data -> app semantic layer -> MCP / RAG / domain apps
```

## Layers

### core/

Generic platform code only:

- `core/catalog`: manifest discovery for platform and app services
- `core/runtime/app_service`: reusable app-service lifecycle, health, logging, request/event/run stores
- `core/runtime/runtime_db`: Postgres connection, checkpoint, and service observability schema helpers
- `core/observability`: shared log and retention helpers
- `core/contracts`: stable cross-app contracts such as execution identity
- `core/semantic`: generic semantic object/document contracts

Do not put app names, procurement fields, business rules, or app orchestration in `core/*`.

### services/

Runnable platform services and backing capabilities:

- `postgres`
- `redis`
- `nginx`
- `platform_api`
- `platform_dashboard`

Services expose generic platform capabilities. They must not contain app-specific business logic.
App-required services such as `prefect`, `minio`, and `qdrant` are enabled only when an app declares them.

### apps/

Apps own orchestration, business rules, persistence, ontology, semantic transformers, MCP/RAG/domain APIs, and app-specific pipeline workers.

Current apps:

- `apps/g2b/pipeline`
- `apps/g2b/mcp`

## Rules

- Domain logic belongs in `apps/*`.
- Generic service/runtime logic belongs in `services/*` or `core/*`.
- App ontology, relationship names, semantic tags, and semantic document builders belong in `apps/<app>/app/semantic`.
- `core/semantic` contains contracts only.
- Prefect control plane manifests live in `services/prefect`; app-specific Prefect workers live under `apps/<app>`.
- Compose is generated from manifests. Do not hand-edit `deploy/compose/docker-compose.yml` except to inspect generated output.
- Secret values must stay in env files, not manifests or payloads.

## Recommended App Structure

```text
apps/<app>/
  app/
    flows/          # orchestration wiring
    tasks/          # execution boundaries
    steps/          # pure transformation/domain logic
    repositories/   # app data access
    semantic/       # app ontology and semantic projection
    service/        # app runner helpers
  infra/
  manifests/
```

## Commands

```bash
python3 scripts/generate_compose.py
python3 scripts/ops.py validate-config
python3 scripts/ops.py lint-boundaries
python3 scripts/ops.py check-all
```

## Acceptance Checklist

- [ ] Correct layer placement
- [ ] No app logic in `core/*` or `services/*`
- [ ] App semantic meaning is under `apps/<app>/app/semantic`
- [ ] Manifest updated when services/apps change
- [ ] Generated compose is in sync
- [ ] `python3 scripts/ops.py validate-config` passes
- [ ] `python3 scripts/ops.py lint-boundaries` passes

END OF FILE
