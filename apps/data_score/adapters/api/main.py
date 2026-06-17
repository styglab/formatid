from __future__ import annotations

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from apps.data_score.domain.flows.evaluation import run_evaluation
from apps.data_score.domain.repositories.evaluation_runs import DataScoreRunRepository
from apps.data_score.domain.service.evaluation_runs import execute_and_store_evaluation
from core.runtime.runtime_db.url import get_database_url


app = FastAPI(title="Data Score API")
_repository = DataScoreRunRepository(
    database_url=get_database_url("DATA_SCORE_DATABASE_URL", host_default="postgres")
)


class EvaluateRequest(BaseModel):
    dataset_name: str = Field(..., min_length=1)
    csv_text: str = Field(..., min_length=1)
    business_context: str | None = None
    llm_mode: str = "disabled"
    manual_judge_result: dict | None = None


@app.get("/health/ready")
def ready() -> dict[str, str]:
    return {"status": "ready"}


@app.get("/health/live")
def live() -> dict[str, str]:
    return {"status": "live"}


@app.get("/")
def root() -> dict[str, object]:
    return {
        "name": "data-score-api",
        "status": "ready",
        "features": [
            "csv_evaluation",
            "queued_evaluations",
            "traditional_dq",
            "semantic_quality",
            "langgraph_orchestration",
        ],
    }


@app.post("/evaluate")
def evaluate(payload: EvaluateRequest) -> dict:
    return run_evaluation(
        dataset_name=payload.dataset_name,
        csv_text=payload.csv_text,
        business_context=payload.business_context,
        llm_mode=payload.llm_mode,
        manual_judge_result=payload.manual_judge_result,
    )


@app.post("/evaluations")
async def create_evaluation(payload: EvaluateRequest) -> dict:
    run = await execute_and_store_evaluation(
        repository=_repository,
        dataset_name=payload.dataset_name,
        csv_text=payload.csv_text,
        business_context=payload.business_context,
        llm_mode=payload.llm_mode,
        manual_judge_result=payload.manual_judge_result,
    )
    return JSONResponse(
        status_code=202,
        content={
            "run_id": run["run_id"],
            "status": run["status"],
            "dataset_name": run["dataset_name"],
            "summary": run.get("summary", {}),
            "created_at": run["created_at"],
            "finished_at": run.get("finished_at"),
            "duration_ms": run.get("duration_ms"),
            "message": "evaluation has been accepted and queued for worker execution",
            "status_url": f"/evaluations/{run['run_id']}",
            "report_url": f"/evaluations/{run['run_id']}/report",
        },
    )


@app.get("/evaluations")
async def list_evaluations(limit: int = 20) -> dict[str, object]:
    runs = await _repository.list_runs(limit=limit)
    return {
        "runs": [
            {
                "run_id": run["run_id"],
                "dataset_name": run["dataset_name"],
                "status": run["status"],
                "llm_mode": run["llm_mode"],
                "created_at": run["created_at"],
                "finished_at": run.get("finished_at"),
                "duration_ms": run.get("duration_ms"),
                "summary": run.get("summary", {}),
            }
            for run in runs
        ]
    }


@app.get("/evaluations/{run_id}")
async def get_evaluation(run_id: str) -> dict:
    run = await _repository.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="evaluation run not found")
    return {
        "run_id": run["run_id"],
        "dataset_name": run["dataset_name"],
        "llm_mode": run["llm_mode"],
        "business_context": run.get("business_context"),
        "status": run["status"],
        "summary": run.get("summary", {}),
        "error": run.get("error"),
        "created_at": run["created_at"],
        "started_at": run.get("started_at"),
        "finished_at": run.get("finished_at"),
        "updated_at": run.get("updated_at"),
        "duration_ms": run.get("duration_ms"),
    }


@app.get("/evaluations/{run_id}/report")
async def get_evaluation_report(run_id: str) -> dict:
    run = await _repository.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="evaluation run not found")
    if not isinstance(run.get("report"), dict):
        raise HTTPException(status_code=409, detail="evaluation report is not available")
    return run["report"]


@app.get("/evaluations/{run_id}/summary")
async def get_evaluation_summary(run_id: str) -> dict:
    run = await _repository.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="evaluation run not found")
    report = run.get("report")
    if not isinstance(report, dict):
        return {
            "run_id": run["run_id"],
            "status": run["status"],
            "dataset_name": run["dataset_name"],
            "summary": run.get("summary", {}),
            "scores": {},
            "issues": [],
            "suggestions": [],
        }
    return {
        "run_id": run["run_id"],
        "status": run["status"],
        "dataset_name": run["dataset_name"],
        "summary": report.get("summary", {}),
        "scores": report.get("scores", {}),
        "issues": report.get("issues", []),
        "suggestions": report.get("suggestions", []),
    }
