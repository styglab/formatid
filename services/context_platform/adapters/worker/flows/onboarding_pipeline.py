from __future__ import annotations

from prefect import flow, task

from services.context_platform.internal.ingestion.api_documents import ingest_source_document
from services.context_platform.internal.storage import ContextPlatformRepository


@task
def execute_document_ingestion(run_id: str) -> dict[str, object]:
    return ingest_source_document(run_id, repository=ContextPlatformRepository())


@flow(name="run-context-platform-ingestion")
def run_onboarding_pipeline(run_id: str) -> dict[str, object]:
    return execute_document_ingestion(run_id)
