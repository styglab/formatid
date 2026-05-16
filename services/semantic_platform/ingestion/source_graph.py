from __future__ import annotations

from services.semantic_platform.ingestion.graphs.source_ingestion.config import (
    DEFAULT_CHUNKS_OUTPUT_DIR,
    DEFAULT_CATALOG_DIR,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_SOURCE,
)
from services.semantic_platform.ingestion.graphs.source_ingestion.graph import main, run_source_graph
from services.semantic_platform.ingestion.graphs.source_ingestion.state import SourceGraphState

__all__ = [
    "DEFAULT_CHUNKS_OUTPUT_DIR",
    "DEFAULT_CATALOG_DIR",
    "DEFAULT_OUTPUT_DIR",
    "DEFAULT_SOURCE",
    "SourceGraphState",
    "main",
    "run_source_graph",
]


if __name__ == "__main__":
    main()
