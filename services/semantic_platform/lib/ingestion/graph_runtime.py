from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from langgraph.graph import END, START, StateGraph as LangGraphStateGraph

from services.semantic_platform.lib.ingestion.state import SourceGraphState

GraphNode = Callable[[SourceGraphState], SourceGraphState]


@dataclass(frozen=True)
class CompiledStateGraph:
    nodes: dict[str, GraphNode]
    edges: list[tuple[str, str]]
    compiled_graph: Any | None = None

    @property
    def node_names(self) -> list[str]:
        names: list[str] = []
        current = START
        visited = {START}
        while True:
            next_nodes = [target for source, target in self.edges if source == current]
            if not next_nodes:
                raise ValueError(f"graph has no outgoing edge from {current}")
            if len(next_nodes) > 1:
                raise ValueError(f"linear runner cannot execute branching edge from {current}")
            target = next_nodes[0]
            if target == END:
                return names
            if target in visited:
                raise ValueError(f"graph contains a cycle at {target}")
            if target not in self.nodes:
                raise ValueError(f"graph edge references unknown node {target}")
            names.append(target)
            visited.add(target)
            current = target

    @property
    def ordered_nodes(self) -> list[GraphNode]:
        return [self.nodes[name] for name in self.node_names]

    def invoke(self, state: SourceGraphState) -> SourceGraphState:
        return self.compiled_graph.invoke(state)


class StateGraph:
    def __init__(self) -> None:
        self._nodes: dict[str, GraphNode] = {}
        self._edges: list[tuple[str, str]] = []
        self._graph = LangGraphStateGraph(SourceGraphState)

    def add_node(self, name: str, node: GraphNode) -> None:
        if name in {START, END}:
            raise ValueError(f"{name} is a reserved graph node name")
        if name in self._nodes:
            raise ValueError(f"duplicate graph node {name}")
        self._nodes[name] = node
        self._graph.add_node(name, node)

    def add_edge(self, source: str, target: str) -> None:
        self._edges.append((source, target))
        self._graph.add_edge(source, target)

    def compile(self) -> CompiledStateGraph:
        compiled_graph = self._graph.compile()
        compiled = CompiledStateGraph(
            nodes=dict(self._nodes),
            edges=list(self._edges),
            compiled_graph=compiled_graph,
        )
        compiled.node_names
        return compiled
