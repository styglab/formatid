__all__ = ["run_source_ingestion"]


def __getattr__(name: str):
    if name == "run_source_ingestion":
        from services.semantic_platform.lib.ingestion.graph import run_source_ingestion

        return run_source_ingestion
    raise AttributeError(name)
