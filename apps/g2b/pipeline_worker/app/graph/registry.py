from __future__ import annotations

from core.runtime.graph_runtime import GraphRegistry, create_graph_definition

from apps.g2b.pipeline_worker.app.graph.document_process_graph import (
    build_g2b_document_process_graph,
    build_g2b_document_process_initial_state,
)


GRAPH_REGISTRY = GraphRegistry(
    [
        create_graph_definition(
            name="document_process_graph",
            description="Process one requested document from a triggered graph run request.",
            build_graph=build_g2b_document_process_graph,
            initial_state=build_g2b_document_process_initial_state,
        ),
    ]
)
