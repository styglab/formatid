from __future__ import annotations

from core.runtime.graph_runtime import GraphDefinition, GraphRegistry, create_graph_definition

from apps.g2b.pipeline_scheduler.app.graph.ingest_graph import build_g2b_ingest_graph, build_g2b_ingest_initial_state
from apps.g2b.pipeline_scheduler.app.steps.ingest_steps import build_g2b_ingest_graph_steps


GRAPH_REGISTRY = GraphRegistry(
    [
        create_graph_definition(
            name="ingest_graph",
            description="Collect G2B notices and enqueue generic downstream pipeline tasks.",
            build_graph=build_g2b_ingest_graph,
            build_steps=build_g2b_ingest_graph_steps,
            initial_state=build_g2b_ingest_initial_state,
        ),
    ]
)


def get_graph_definition(name: str) -> GraphDefinition:
    return GRAPH_REGISTRY.get(name)


def list_graph_definitions() -> list[GraphDefinition]:
    return GRAPH_REGISTRY.list()
