from __future__ import annotations

from uuid import uuid4

from apps.spec_rag.api.app.domain.spec_rag.repository import SpecRagRepository
from apps.spec_rag.api.app.domain.spec_rag.schemas import (
    SpecRagRunCreateResponse,
    SpecRagRunResponse,
)


class SpecRagService:
    async def create_run(
        self,
        *,
        filename: str,
        content_type: str,
        content: bytes,
        request_id: str | None,
        correlation_id: str | None,
    ) -> SpecRagRunCreateResponse:
        repository = SpecRagRepository()
        run_id = uuid4().hex
        try:
            resource_key = await repository.create_document(
                filename=filename,
                content_type=content_type,
                content=content,
            )
            await repository.enqueue_workflow(
                run_id=run_id,
                resource_key=resource_key,
                request_id=request_id,
                correlation_id=correlation_id,
            )
            return SpecRagRunCreateResponse(
                run_id=run_id,
                graph_name="spec_indexing_graph",
                queue_name=repository.queue_name,
                status="queued",
            )
        finally:
            await repository.close()

    async def get_run(self, run_id: str) -> SpecRagRunResponse | None:
        repository = SpecRagRepository()
        try:
            row = await repository.get_run(run_id)
        finally:
            await repository.close()
        if row is None:
            return None
        return SpecRagRunResponse(
            run_id=str(row["run_id"]),
            status=str(row["status"]),
            current_node=row.get("current_node"),
            result=dict(row.get("result") or {}),
            error=None if row.get("error") is None else dict(row["error"]),
        )
