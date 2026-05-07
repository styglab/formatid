# Graph Runtime

`core/runtime/graph_runtime` is the platform graph orchestration runtime.

It provides:

- scheduled graph execution
- triggered graph execution
- graph run tracking
- interrupt/suspend handling
- durable resume with LangGraph Postgres checkpointer

## Execution Conventions

These rules apply to every graph that uses this runtime.

### 1. Identity

- `run_id` is the platform graph run identifier
- `thread_id` must be the same value as `run_id`
- `task_id` identifies worker task execution
- `artifact_id` identifies stored large outputs referenced by graph state or task results

### 2. Small State Only

Graph state must contain:

- ids
- status
- summaries
- artifact refs
- small control payloads

Graph state must not contain:

- large row arrays
- full documents
- large model outputs
- large retrieval batches

Large payloads must be stored outside graph state and referenced with artifact refs.

### 3. Interrupt / Resume Model

Graphs that wait for external work must:

- enqueue external work in one node
- suspend in a separate node using interrupt
- resume from the same `run_id/thread_id`

The suspend node must not perform side effects before interrupt.

Recommended shape:

```txt
tool_dispatch -> tool_resume -> next_step
```

### 4. Triggered Graph Requests

Triggered graph requests use two modes:

- `request_kind = "start"` for a new run
- `request_kind = "resume"` for a suspended run

Resume requests must target the original `run_id`.

### 5. Resume Queue

Suspended graph runs must record a `resume_queue` in `params.__runtime.resume_queue`.

When a waited task reaches terminal status, the platform may enqueue a resume
request to that queue automatically.

### 6. Terminal Task Status

The runtime currently treats these task statuses as terminal for auto-resume:

- `succeeded`
- `failed`
- `interrupted`
- `dead_lettered`

Graphs must inspect the resume payload and decide whether to continue or fail.

### 7. Resume Payload Shape

Use small, serialization-friendly payloads.

Recommended payload shape:

```json
{
  "task_id": "task-123",
  "status": "succeeded"
}
```

If additional data is needed, store it in task result output or artifact storage
and pass only summaries or refs through the resume payload.

### 8. Observability

Every graph run should be traceable through:

- `run_id`
- `thread_id`
- `task_id`
- `correlation_id`
- `resource_key`
- optional `session_id` when the graph belongs to an agent or conversational run

Apps should reuse these identifiers consistently across API, graph, and worker layers.

`run_id` is the graph identity, while `correlation_id` is the cross-surface
trace identity that can connect the originating request, downstream tasks, and
service logs.
