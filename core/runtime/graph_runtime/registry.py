from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from typing import Any

from core.runtime.graph_runtime.context import GraphRunContext


GraphRun = Callable[[GraphRunContext], Awaitable[dict[str, Any]]]


@dataclass(frozen=True)
class GraphDefinition:
    name: str
    run: GraphRun
    description: str = ""


class GraphRegistry:
    def __init__(self, definitions: Iterable[GraphDefinition] = ()) -> None:
        self._definitions: dict[str, GraphDefinition] = {}
        for definition in definitions:
            self.register(definition)

    def register(self, definition: GraphDefinition) -> None:
        if definition.name in self._definitions:
            raise ValueError(f"Graph is already registered: {definition.name}")
        self._definitions[definition.name] = definition

    def get(self, name: str) -> GraphDefinition:
        try:
            return self._definitions[name]
        except KeyError as exc:
            raise ValueError(f"Unknown graph: {name}") from exc

    def list(self) -> list[GraphDefinition]:
        return list(self._definitions.values())
