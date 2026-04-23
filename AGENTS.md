# AGENTS.md

## 1. Purpose

This file defines strict rules and execution context for AI agents working in this repository.

Agents MUST follow this file to:

* understand architecture
* place code correctly
* avoid breaking layer boundaries
* produce consistent changes

---

## 2. Architecture

This project uses a 3-layer architecture with service subtypes.

### 1) Execution Layer

Location:

* `core/runtime/worker`
* `core/runtime/task_runtime`
* `core/runtime/runtime_db`
* `core/runtime/graph_runtime`
* `core/catalog`
* `core/observability`
* `core/runtime/app_service`
* `core/backing`

Responsibilities:

* queue consume
* retry / DLQ
* heartbeat
* execution state
* graph runtime primitives
* catalog loading
* observability
* reusable app-service runtime primitives

Do not modify behavior here unless explicitly asked.

---

### 2) Service Layer

Location:

* `services/ingest_api`
* `services/ingest_file`
* `services/extract`
* `services/llm`
* `services/runtime_api`
* `services/runtime_dashboard`
* `services/qdrant`

Responsibilities:

For worker services:

* external IO such as HTTP, S3, DB
* reusable data transformation
* stateless processing

For platform-facing services:

* runtime health, checkpoint, queue, and observability APIs
* runtime dashboard UI
* operational views over execution state

For optional dependency services:

* app-selected backing capabilities such as vector databases
* service manifests only, unless reusable task code is needed

Constraints:

* no app-specific logic
* no app orchestration
* no business decision
* no app status lifecycle management

Generic pipeline task implementations belong in `services/<capability>/app/tasks`.
Platform-facing services belong in `services/runtime_*`.

Naming policy:

* `services/<capability>_<kind>` contains generic worker service code, such as `ingest_api` or `ingest_file`.
* `services/runtime_<surface>` contains runtime-facing operational services, such as `runtime_api` or `runtime_dashboard`.
* Compose service names use kebab-case, such as `ingest-api-worker`, `runtime-api`, and `runtime-dashboard`.

Env-name config contract:

* `services/*` task payloads may reference environment variable names through `*_env` fields.
* These env names are declarative runtime config references, not secret values.
* `services/*` MUST NOT hardcode app-specific env names.
* `apps/*` may choose env names and inject them through app manifests and env files.
* Secret values MUST NEVER be placed directly in task payloads.
* App manifests that provide worker env files SHOULD declare `allowed_worker_env`.

---

### 3) Application Layer

Location:

* `apps/*`

Responsibilities:

* orchestration
* business logic
* task chaining
* job/status lifecycle
* app-specific rules, schemas, and persistence

---

## 3. Absolute Rules

### Rule 1 - No App Logic in services/*

Forbidden in `services/*` and `core/runtime/*`:

* app names such as `g2b_ingest` or `g2b_summary`
* business names such as `bid`, `notice`, or procurement-specific fields
* business-specific schemas
* app-specific branching

---

### Rule 2 - No App Orchestration in Service Workers

Forbidden in service worker implementations:

```python
enqueue_task(...)
_set_status(...)
if business_condition:
```

Workers return `TaskResult`. Applications decide what happens next.

Platform-facing services may read runtime state and expose operational APIs, but
they MUST NOT decide app task flow or update app-specific lifecycle tables.

---

### Rule 3 - apps/* Owns Flow

Only `apps/*` can:

* decide next task
* interpret task results
* manage job lifecycle
* update app-specific status tables

---

### Rule 4 - Payload is Declarative

Correct:

```json
{
  "source": {...},
  "target": {...}
}
```

Incorrect:

```json
{
  "process_and_store": true
}
```

### Rule 5 - Platform & Application Structure Policy

1. Platform Structure
formatid/

├── apps/                        # entrypoints
│   └── g2b/
│       ├── api/                 # runnable
│       │   ├── app/
│       │   ├── manifests/
│       │   └── infra/
│       │
│       ├── pipeline_scheduler/  # runnable
│       └── pipeline_worker/     # runnable
│           ├── app/
│           ├── manifests/
│           └── infra/

├── services/                   # execution units
│   ├── ingest_api/
│   │   ├── app/
│   │   │   └── tasks/
│   │   ├── manifests/
│   │   └── infra/
│   │
│   ├── ingest_file/
│   │   ├── app/
│   │   │   └── tasks/
│   │   ├── manifests/
│   │   └── infra/
│   │
│   ├── extract/
│   │   ├── app/
│   │   │   └── tasks/
│   │   ├── manifests/
│   │   └── infra/
│   │
│   └── llm/
│       ├── app/
│       │   └── tasks/
│       ├── manifests/
│       └── infra/

├── core/
│   ├── backing/               # postgres, redis
│   ├── runtime/               # task_runtime, worker core
│   │   └── graph_runtime/     # graph registry, runner, triggered queue
│   ├── observability/
│   └── catalog/

├── deploy/
└── scripts/


2. FastAPI Application Structure
```
example_api/
├── app/
│   ├── api/                     # HTTP layer
│   │   ├── v1/
│   │   │   └── routers/
│   │   │       └── g2b.py
│   │   └── deps.py              # dependency injection
│   │
│   ├── domain/                  # business logic layer
│   │   └── g2b/
│   │       ├── service.py       # orchestration / use-case entry
│   │       ├── schemas.py       # request/response models
│   │       ├── models.py        # DB models (optional)
│   │       └── repository.py    # data access layer
│   │
│   ├── core/                    # shared core utilities
│   │   ├── config.py
│   │   └── logging.py
│   │
│   └── main.py                  # application entrypoint
│
├── infra/
│   ├── env/                     # environment configs
│   └── images/                  # Docker-related assets
│
└── manifests/                   # deployment manifests (K8s, etc.)
```

3. Pipeline Application Structure
```
example_pipeline/
├── app/
│   ├── graph/
│   │   ├── registry.py          # graph registry
│   │   ├── ingest_graph.py      # graph definition
│   │   └── state.py             # shared state definition
│   │
│   ├── contracts/               # graph state and node/step IO contracts
│   │   └── ingest.py
│   │
│   ├── nodes/                   # node wrappers (execution boundary)
│   │   └── ingest_nodes.py
│   │
│   ├── steps/                   # pure logic layer
│   │   └── ingest_steps.py
│   │
│   ├── schemas/
│   │   └── types.py             # TypedDict / domain types
│   │
│   ├── service/
│   │   ├── run_scheduler.py     # scheduled graph runner entrypoint
│   │   └── run_worker.py        # triggered graph worker entrypoint
│   │
│   └── config.py
│
├── infra/
│   ├── env/
│   └── images/
│
└── manifests/
```

---

## 4. Worker Contract

Registered task handlers MUST accept `TaskMessage` and return `TaskResult`:

```python
async def some_handler(message: TaskMessage) -> TaskResult:
```

Rules:

* deterministic
* idempotent when possible
* no hidden orchestration side effects

---

## 5. Naming Conventions

### Task Name

Format:

```txt
<layer>.<capability>.<action>
```

Examples:

* `ingest.api.fetch`
* `ingest.file.download`
* `extract.text.run`
* `serve.llm.generate`

### Queue Name

Redis queue names use colon-separated capability/type:

* `ingest:api`
* `ingest:file`
* `extract:text`
* `serve:llm`

### Graph Names

Graph node names and graph state keys use `snake_case`.

### Graph Trigger Names

Graph triggers use these names:

* `scheduled`: schedule-based graph start
* `triggered`: queue-request-based graph start

Reusable graph runtime primitives belong in `core/runtime/graph_runtime`.
Graphs MUST NOT depend on trigger implementations. Trigger runners select a
registered graph and pass declarative params through `GraphRunContext`.
Use `create_graph_definition` for app graph registration so apps only provide
graph builders, optional step builders, and initial state.

### Payload / Output Contracts

Reusable service task payload/output contracts belong in `services/<service>/app/contracts`.
App graph state and app-specific node/step contracts belong in `apps/<app>/app/contracts`.
Runtime protocols such as `TaskMessage`, `TaskResult`, and `GraphRunContext` remain in `core/runtime/*`.

---

## 6. Where to Put Code

Before writing code, decide:

### Q1. Is this business logic?

Use `apps/<app>/tasks` or `apps/<app>/service`.

### Q2. Is this reusable IO / processing?

Use `services/ingest_api`, `services/ingest_file`, `services/extract`, or `services/llm`.

### Q3. Is this operational API or dashboard over runtime state?

Use `services/runtime_api` or `services/runtime_dashboard`.

### Q4. Is this runtime concern?

Use `core/runtime/task_runtime`, `core/runtime/worker`, `core/runtime/app_service`, `core/runtime/graph_runtime`, `core/runtime/runtime_db`, `core/catalog`, `core/observability`, or `core/backing`.

---

## 7. Manifest-Driven System

* Do not hardcode task routing.
* All tasks must be registered in manifest.
* Worker/task mapping must come from manifest.
* Platform-facing service definitions must come from manifest.
* App metadata lives in `apps/<app>/manifests/app.json`.

---

## 8. Development Workflow

### Run services

```bash
docker compose --env-file deploy/compose/env/compose.env -f deploy/compose/docker-compose.yml up -d --build
```

### Re-generate compose

```bash
python3 scripts/generate_compose.py
```

### Validate config

```bash
python3 scripts/ops.py validate-config
```

### Check all

```bash
python3 scripts/ops.py check-all
```

### Boundary lint

```bash
python3 scripts/ops.py lint-boundaries
```

### Enqueue task example

```bash
python3 scripts/ops.py enqueue ingest.api.fetch --payload '{...}'
```

---

## 9. Common Mistakes

### Wrong: worker doing business

```python
if bid_type == ...
```

### Wrong: worker chaining tasks

```python
enqueue_task(...)
```

### Wrong: app-specific task in services

```txt
services/extract/tasks/g2b_summary.py
```

### Correct: generic pipeline task in services

```txt
services/extract/tasks/text.py
```

### Correct: app-specific task in apps

```txt
apps/<app>/tasks/*
```

---

## 10. Acceptance Checklist

Before finishing any change:

* [ ] No app logic in `services/*` or `core/runtime/*`
* [ ] No app orchestration in service workers
* [ ] Platform-facing services only expose runtime operations
* [ ] Secret values are not placed in task payloads
* [ ] `services/*` does not hardcode app-specific env names
* [ ] Correct layer placement
* [ ] Manifest updated
* [ ] Naming convention followed

---

## 11. Design Identity

This project is:

```txt
Execution Core + Services + App Orchestrators
```

---

## 12. Agent Behavior Expectations

When modifying code:

* prefer minimal change
* follow existing patterns
* do not refactor unrelated code
* do not introduce new abstractions unless necessary
* ask for clarification if layer boundary is unclear

---

## 13. Final Principle

If you can explain why this logic exists, it belongs in `apps/*`.

If you can only explain what it does, it belongs in `services/*`.

---

END OF FILE
