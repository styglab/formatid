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

This project uses a 3-layer architecture.

### 1) Execution Layer

Location:

* `services/worker`
* `services/task_runtime`
* `services/runtime_db`
* `services/catalog`
* `services/observability`
* `services/app_service`
* `shared/*`

Responsibilities:

* queue consume
* retry / DLQ
* heartbeat
* execution state
* catalog loading
* observability
* reusable app-service runtime primitives

Do not modify behavior here unless explicitly asked.

---

### 2) Pipeline Layer

Location:

* `services/ingest`
* `services/extract`
* `services/llm`

Responsibilities:

* external IO such as HTTP, S3, DB
* reusable data transformation
* stateless processing

Constraints:

* no app-specific logic
* no orchestration
* no business decision
* no app status lifecycle management

Generic pipeline task implementations belong in `services/<capability>/tasks`.

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

Forbidden in `services/*` and `shared/*`:

* app names such as `g2b_ingest` or `g2b_summary`
* business names such as `bid`, `notice`, or procurement-specific fields
* business-specific schemas
* app-specific branching

---

### Rule 2 - No Orchestration in Workers

Forbidden in `services/*`:

```python
enqueue_task(...)
_set_status(...)
if business_condition:
```

Workers return `TaskResult`. Applications decide what happens next.

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

### Rule 5 - Application Structure Policy

1. FastAPI Application Structure
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

2. Pipeline Application Structure
```
example_pipeline/
├── app/
│   ├── graph/
│   │   ├── ingest_graph.py      # graph definition
│   │   └── state.py             # shared state definition
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
│   │   └── run_pipeline.py      # pipeline entrypoint
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

---

## 6. Where to Put Code

Before writing code, decide:

### Q1. Is this business logic?

Use `apps/<app>/tasks` or `apps/<app>/service`.

### Q2. Is this reusable IO / processing?

Use `services/ingest`, `services/extract`, or `services/llm`.

### Q3. Is this runtime concern?

Use `services/task_runtime`, `services/worker`, `services/app_service`, `services/runtime_db`, `services/catalog`, or `shared/*`.

---

## 7. Manifest-Driven System

* Do not hardcode task routing.
* All tasks must be registered in manifest.
* Worker/task mapping must come from manifest.
* App metadata lives in `apps/<app>/manifests/app.json`.

---

## 8. Development Workflow

### Run services

```bash
docker compose --env-file infra/env/compose.env -f infra/docker-compose.yml up -d --build
```

### Re-generate compose

```bash
python3 scripts/generate_compose.py
```

### Validate config

```bash
python3 scripts/ops.py validate-config
```

### Enqueue task example

```bash
python3 scripts/ops.py enqueue ingest:api ingest.api.fetch --payload '{...}'
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

* [ ] No app logic in `services/*` or `shared/*`
* [ ] No orchestration in pipeline workers
* [ ] Correct layer placement
* [ ] Manifest updated
* [ ] Naming convention followed

---

## 11. Design Identity

This project is:

```txt
Execution Engine + Pipeline Workers + App Orchestrators
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
