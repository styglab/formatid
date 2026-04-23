from __future__ import annotations

from collections.abc import Callable, Mapping
from inspect import signature
from typing import Any, TypeVar

from core.runtime.graph_runtime.context import GraphRunContext
from core.runtime.graph_runtime.registry import GraphDefinition


StepsT = TypeVar("StepsT")
CompiledGraph = Any
BuildGraph = Callable[[], CompiledGraph] | Callable[[StepsT], CompiledGraph]
BuildSteps = Callable[[], StepsT]
InitialState = Mapping[str, Any] | Callable[[], Mapping[str, Any]] | Callable[[GraphRunContext], Mapping[str, Any]]


def create_graph_definition(
    *,
    name: str,
    build_graph: BuildGraph[StepsT],
    build_steps: BuildSteps[StepsT] | None = None,
    initial_state: InitialState | None = None,
    description: str = "",
) -> GraphDefinition:
    async def run(context: GraphRunContext) -> dict[str, Any]:
        graph = _build_graph(build_graph=build_graph, build_steps=build_steps)
        state = _build_initial_state(context=context, initial_state=initial_state)
        return await graph.ainvoke(state)

    return GraphDefinition(name=name, description=description, run=run)


def _build_graph(
    *,
    build_graph: BuildGraph[StepsT],
    build_steps: BuildSteps[StepsT] | None,
) -> CompiledGraph:
    if build_steps is None:
        return build_graph()
    return build_graph(build_steps())


def _build_initial_state(
    *,
    context: GraphRunContext,
    initial_state: InitialState | None,
) -> dict[str, Any]:
    state = {
        "checkpoint_store": context.checkpoint_store,
        "run_store": context.run_store,
        "graph_run_store": context.graph_run_store,
        "graph_run_id": context.run_id,
        "params": context.params,
    }
    if initial_state is None:
        return state
    extra_state = _call_initial_state(initial_state, context=context) if callable(initial_state) else initial_state
    return {**state, **dict(extra_state)}


def _call_initial_state(
    initial_state: Callable[[], Mapping[str, Any]] | Callable[[GraphRunContext], Mapping[str, Any]],
    *,
    context: GraphRunContext,
) -> Mapping[str, Any]:
    if len(signature(initial_state).parameters) == 0:
        return initial_state()
    return initial_state(context)
