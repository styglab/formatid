from __future__ import annotations

import asyncio
import unittest
from typing import Any

from core.contracts.execution.identity import normalize_execution_identity
import core.runtime.graph_runtime.resume as graph_resume
from core.runtime.graph_runtime.queue import TriggeredGraphRequest
from scripts.generate_compose import check_compose
from scripts.ops.boundaries import lint_boundaries
from scripts.ops.validation import validate_config


class PlatformContractTests(unittest.TestCase):
    def test_validate_config_passes(self) -> None:
        result = validate_config()

        self.assertTrue(result["valid"], result)

    def test_boundary_lint_passes(self) -> None:
        result = lint_boundaries()

        self.assertTrue(result["valid"], result)

    def test_generated_compose_is_in_sync(self) -> None:
        self.assertTrue(check_compose())

    def test_execution_identity_normalizes_non_empty_fields(self) -> None:
        identity = normalize_execution_identity(
            {
                "request_id": "request-1",
                "correlation_id": "",
                "run_id": "run-1",
                "resource_key": "resource-1",
            },
            correlation_id="correlation-1",
            session_id=None,
        )

        self.assertEqual(
            identity,
            {
                "request_id": "request-1",
                "correlation_id": "correlation-1",
                "run_id": "run-1",
                "resource_key": "resource-1",
            },
        )

    def test_triggered_graph_request_preserves_identity_on_retry(self) -> None:
        request = TriggeredGraphRequest(
            graph_name="agent_service_graph",
            run_id="run-1",
            request_kind="resume",
            resume_value={"task_id": "task-1", "status": "succeeded"},
            requested_by="test",
            request_id="request-1",
            correlation_id="correlation-1",
            resource_key="resource-1",
            session_id="session-1",
        )

        decoded = TriggeredGraphRequest.from_json(request.to_json())
        retry = decoded.next_attempt()

        self.assertEqual(decoded.request_kind, "resume")
        self.assertEqual(decoded.resume_value, {"task_id": "task-1", "status": "succeeded"})
        self.assertEqual(retry.attempts, 1)
        self.assertEqual(retry.request_id, "request-1")
        self.assertEqual(retry.correlation_id, "correlation-1")
        self.assertEqual(retry.resource_key, "resource-1")
        self.assertEqual(retry.session_id, "session-1")

    def test_graph_resume_enqueue_preserves_original_identity(self) -> None:
        queued: dict[str, list[TriggeredGraphRequest]] = {}

        class FakeGraphRunStore:
            async def list_suspended_runs_for_task(self, *, task_id: str) -> list[dict[str, Any]]:
                self.task_id = task_id
                return [
                    {
                        "run_id": "run-1",
                        "graph_name": "agent_service_graph",
                        "params": {
                            "__runtime": {
                                "resume_queue": "demo:agent:runs",
                                "identity": {
                                    "request_id": "request-1",
                                    "correlation_id": "correlation-1",
                                    "run_id": "run-1",
                                    "thread_id": "run-1",
                                    "resource_key": "resource-1",
                                    "session_id": "session-1",
                                },
                            }
                        },
                    }
                ]

        class FakeTriggeredGraphQueue:
            def __init__(self, *, redis_url: str, queue_name: str) -> None:
                self.queue_name = queue_name
                queued.setdefault(queue_name, [])

            async def enqueue(self, request: TriggeredGraphRequest) -> None:
                queued[self.queue_name].append(request)

            async def close(self) -> None:
                return None

        original_queue = graph_resume.TriggeredGraphQueue
        graph_resume.TriggeredGraphQueue = FakeTriggeredGraphQueue
        try:
            total = asyncio.run(
                graph_resume.enqueue_graph_resumes_for_task(
                    redis_url="redis://localhost:6379/0",
                    graph_run_store=FakeGraphRunStore(),
                    task_id="task-1",
                    resume_value={"task_id": "task-1", "status": "succeeded"},
                    requested_by="unit-test",
                )
            )
        finally:
            graph_resume.TriggeredGraphQueue = original_queue

        self.assertEqual(total, 1)
        self.assertEqual(len(queued["demo:agent:runs"]), 1)
        request = queued["demo:agent:runs"][0]
        self.assertEqual(request.request_kind, "resume")
        self.assertEqual(request.run_id, "run-1")
        self.assertEqual(request.resume_value, {"task_id": "task-1", "status": "succeeded"})
        self.assertEqual(request.request_id, "request-1")
        self.assertEqual(request.correlation_id, "correlation-1")
        self.assertEqual(request.resource_key, "resource-1")
        self.assertEqual(request.session_id, "session-1")


if __name__ == "__main__":
    unittest.main()
