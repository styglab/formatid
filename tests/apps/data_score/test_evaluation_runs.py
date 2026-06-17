from __future__ import annotations

import unittest

from apps.data_score.domain.service.evaluation_runs import (
    execute_and_store_evaluation,
    process_next_pending_run,
)


CSV_TEXT = """company_name,description,category
Samsung Electronics,Global semiconductor and consumer electronics manufacturer,technology
LG Energy Solution,Battery manufacturer for electric vehicles,energy
"""


class InMemoryRunRepository:
    def __init__(self) -> None:
        self.runs: dict[str, dict] = {}
        self.counter = 0

    async def enqueue_run(self, *, dataset_name: str, llm_mode: str, business_context: str | None, request_payload: dict) -> dict:
        self.counter += 1
        run_id = f"dsrun_test_{self.counter}"
        run = {
            "run_id": run_id,
            "dataset_name": dataset_name,
            "llm_mode": llm_mode,
            "business_context": business_context,
            "status": "pending",
            "request_payload": request_payload,
            "summary": {},
            "report": None,
            "error": None,
            "created_at": "2026-01-01T00:00:00Z",
            "started_at": None,
            "updated_at": "2026-01-01T00:00:00Z",
            "finished_at": None,
            "duration_ms": None,
        }
        self.runs[run_id] = run
        return dict(run)

    async def claim_next_pending_run(self) -> dict | None:
        pending_ids = [run_id for run_id, run in self.runs.items() if run["status"] == "pending"]
        if not pending_ids:
            return None
        run = self.runs[pending_ids[0]]
        run["status"] = "running"
        run["started_at"] = "2026-01-01T00:00:00Z"
        return dict(run)

    async def complete_run(self, *, run_id: str, report: dict, duration_ms: float) -> None:
        self.runs[run_id]["status"] = "completed"
        self.runs[run_id]["report"] = report
        self.runs[run_id]["summary"] = report.get("summary", {})
        self.runs[run_id]["duration_ms"] = duration_ms
        self.runs[run_id]["finished_at"] = "2026-01-01T00:00:01Z"

    async def fail_run(self, *, run_id: str, error: dict, duration_ms: float) -> None:
        self.runs[run_id]["status"] = "failed"
        self.runs[run_id]["error"] = error
        self.runs[run_id]["duration_ms"] = duration_ms
        self.runs[run_id]["finished_at"] = "2026-01-01T00:00:01Z"

    async def get_run(self, run_id: str) -> dict | None:
        run = self.runs.get(run_id)
        return dict(run) if run is not None else None


class EvaluationRunServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_enqueue_and_process_evaluation_persists_completed_run(self) -> None:
        repository = InMemoryRunRepository()

        run = await execute_and_store_evaluation(
            repository=repository,
            dataset_name="dataset.company_profiles",
            csv_text=CSV_TEXT,
            business_context="vendor discovery",
            llm_mode="disabled",
            manual_judge_result=None,
        )

        self.assertEqual("pending", run["status"])
        completed = await process_next_pending_run(repository)
        assert completed is not None
        self.assertEqual("completed", completed["status"])
        self.assertEqual("dataset.company_profiles", completed["dataset_name"])
        self.assertIn("summary", completed["report"])
        self.assertIn("overall_score", completed["summary"])

    async def test_enqueue_and_process_evaluation_persists_failure(self) -> None:
        repository = InMemoryRunRepository()

        run = await execute_and_store_evaluation(
            repository=repository,
            dataset_name="dataset.empty",
            csv_text="",
            business_context=None,
            llm_mode="disabled",
            manual_judge_result=None,
        )

        self.assertEqual("pending", run["status"])
        failed = await process_next_pending_run(repository)
        assert failed is not None
        self.assertEqual("failed", failed["status"])
        self.assertEqual("ValueError", failed["error"]["type"])

    async def test_process_next_pending_run_returns_none_when_queue_is_empty(self) -> None:
        repository = InMemoryRunRepository()

        processed = await process_next_pending_run(repository)

        self.assertIsNone(processed)


if __name__ == "__main__":
    unittest.main()
