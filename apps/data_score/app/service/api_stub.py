from __future__ import annotations

from fastapi import FastAPI
from pydantic import BaseModel, Field

from apps.data_score.app.flows.evaluation import run_evaluation


app = FastAPI(title="Data Score API")


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
