from __future__ import annotations

from importlib import import_module
from typing import Any

_EXPORTS = {
    "AsyncGraphCheckpointer": "core.runtime.graph_runtime.checkpointer",
    "GraphDefinition": "core.runtime.graph_runtime.registry",
    "GraphRegistry": "core.runtime.graph_runtime.registry",
    "GraphRunContext": "core.runtime.graph_runtime.context",
    "GraphRunStore": "core.runtime.graph_runtime.state_store",
    "GraphTrigger": "core.runtime.graph_runtime.context",
    "TriggeredGraphQueue": "core.runtime.graph_runtime.queue",
    "TriggeredGraphRequest": "core.runtime.graph_runtime.queue",
    "create_graph_definition": "core.runtime.graph_runtime.factory",
    "enqueue_graph_resumes_for_task": "core.runtime.graph_runtime.resume",
    "run_registered_graph": "core.runtime.graph_runtime.runner",
    "run_tracked_node": "core.runtime.graph_runtime.progress",
}

__all__ = sorted(_EXPORTS)


def __getattr__(name: str) -> Any:
    try:
        module_name = _EXPORTS[name]
    except KeyError as exc:
        raise AttributeError(name) from exc
    value = getattr(import_module(module_name), name)
    globals()[name] = value
    return value
