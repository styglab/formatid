# Observability

`core/observability` contains platform-wide logging and correlation helpers.

## Execution Identity

The platform standardizes these identifiers:

- `request_id`: one inbound API request
- `correlation_id`: cross-surface trace id used across API, graph, and worker execution
- `run_id`: platform graph run id
- `thread_id`: LangGraph thread id, always the same value as `run_id`
- `task_id`: worker task execution id
- `artifact_id`: external stored output id
- `session_id`: long-lived agent or conversation session id

Rules:

- identifiers should stay stable as work crosses surfaces
- `correlation_id` should be copied from the originating request or run when fan-out occurs
- large payloads must not be used as observability joins; use ids and artifact refs instead
- logs and events should include either top-level id fields or `details.execution_identity`

## Correlation Helpers

Use `build_correlation_details()` when recording logs or events that need a
stable `execution_identity` block in `details`.
