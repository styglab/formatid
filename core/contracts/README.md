# Core Contracts

`core/contracts` contains platform-wide shared contracts.

Rules:

- contracts define stable shapes, not business logic
- contracts must stay small and serialization-friendly
- large payloads must be referenced through artifact refs instead of embedded
- app-specific schemas stay under `apps/*`
- reusable service payload/output schemas stay under `services/*`

Current contract groups:

- `artifacts`: references to large payloads or stored outputs
- `execution`: shared execution identity fields such as `request_id`, `correlation_id`, `run_id`, and `task_id`
- `graph`: graph control and resume payloads
- `tool`: generic tool call and tool result contracts
- `retrieval`: retrieval query and retrieval result contracts

## Execution Identity Rules

The platform uses these shared identifiers:

- `request_id`: API request identity
- `correlation_id`: cross-surface trace identity
- `run_id`: graph run identity
- `thread_id`: graph thread identity, equal to `run_id`
- `task_id`: worker task identity
- `resource_key`: app/resource-level identity used to group work for the same target
- `artifact_id`: stored output identity
- `session_id`: long-lived conversation or agent session identity

Rules:

- ids are small control values and safe to place in graph state, task results, and logs
- ids should be propagated instead of embedding large payloads
- `run_id` and `thread_id` must match for LangGraph-backed graph execution
- API middleware records `request_id` and `correlation_id`; graph and task enqueue paths should preserve them
