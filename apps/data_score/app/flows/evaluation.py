from __future__ import annotations

from typing import Any, TypedDict

from apps.data_score.app.steps.dataset import load_csv_dataset, sample_records
from apps.data_score.app.steps.judge import judge_semantic_quality
from apps.data_score.app.steps.profiler import build_profile
from apps.data_score.app.steps.reporting import build_quality_report
from apps.data_score.app.steps.rubric import generate_rubric
from apps.data_score.app.steps.scoring import calculate_scores
from apps.data_score.app.steps.traditional_dq import run_traditional_dq


class EvaluationState(TypedDict, total=False):
    dataset_name: str
    csv_text: str
    business_context: str | None
    llm_mode: str
    manual_judge_result: dict[str, Any] | None
    dataset: dict[str, Any]
    sample_records: list[dict[str, str]]
    profile: dict[str, Any]
    traditional_scores: dict[str, float]
    rubric: dict[str, Any]
    semantic_result: dict[str, Any]
    scores: dict[str, float | None]
    report: dict[str, Any]


def run_evaluation(
    *,
    dataset_name: str,
    csv_text: str,
    business_context: str | None = None,
    llm_mode: str = "disabled",
    manual_judge_result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    initial_state: EvaluationState = {
        "dataset_name": dataset_name,
        "csv_text": csv_text,
        "business_context": business_context,
        "llm_mode": llm_mode,
        "manual_judge_result": manual_judge_result,
    }

    try:
        graph = _build_langgraph()
        final_state = graph.invoke(initial_state)
    except ModuleNotFoundError as exc:
        if exc.name != "langgraph":
            raise
        final_state = _run_without_langgraph(initial_state)
    return final_state["report"]


def _build_langgraph():
    from langgraph.graph import END, START, StateGraph

    graph = StateGraph(EvaluationState)
    graph.add_node("load_dataset", _load_dataset_node)
    graph.add_node("profile_dataset", _profile_node)
    graph.add_node("run_traditional_dq", _traditional_dq_node)
    graph.add_node("generate_rubric", _rubric_node)
    graph.add_node("judge_semantic_quality", _judge_node)
    graph.add_node("calculate_scores", _score_node)
    graph.add_node("generate_report", _report_node)

    graph.add_edge(START, "load_dataset")
    graph.add_edge("load_dataset", "profile_dataset")
    graph.add_edge("profile_dataset", "run_traditional_dq")
    graph.add_edge("run_traditional_dq", "generate_rubric")
    graph.add_edge("generate_rubric", "judge_semantic_quality")
    graph.add_edge("judge_semantic_quality", "calculate_scores")
    graph.add_edge("calculate_scores", "generate_report")
    graph.add_edge("generate_report", END)
    return graph.compile()


def _run_without_langgraph(state: EvaluationState) -> EvaluationState:
    current = dict(state)
    for node in (
        _load_dataset_node,
        _profile_node,
        _traditional_dq_node,
        _rubric_node,
        _judge_node,
        _score_node,
        _report_node,
    ):
        current.update(node(current))
    return current


def _load_dataset_node(state: EvaluationState) -> EvaluationState:
    dataset = load_csv_dataset(state["csv_text"])
    return {
        "dataset": dataset,
        "sample_records": sample_records(dataset["rows"]),
    }


def _profile_node(state: EvaluationState) -> EvaluationState:
    return {"profile": build_profile(state["dataset"])}


def _traditional_dq_node(state: EvaluationState) -> EvaluationState:
    return {
        "traditional_scores": run_traditional_dq(state["profile"], state["dataset"])
    }


def _rubric_node(state: EvaluationState) -> EvaluationState:
    return {
        "rubric": generate_rubric(
            state["profile"],
            business_context=state.get("business_context"),
        )
    }


def _judge_node(state: EvaluationState) -> EvaluationState:
    return {
        "semantic_result": judge_semantic_quality(
            profile=state["profile"],
            rubric=state["rubric"],
            sample_records=state["sample_records"],
            business_context=state.get("business_context"),
            llm_mode=state.get("llm_mode", "disabled"),
            manual_judge_result=state.get("manual_judge_result"),
        )
    }


def _score_node(state: EvaluationState) -> EvaluationState:
    return {
        "scores": calculate_scores(
            state["traditional_scores"],
            state["semantic_result"],
            state["rubric"],
        )
    }


def _report_node(state: EvaluationState) -> EvaluationState:
    return {
        "report": build_quality_report(
            dataset_name=state["dataset_name"],
            business_context=state.get("business_context"),
            profile=state["profile"],
            traditional_scores=state["traditional_scores"],
            rubric=state["rubric"],
            semantic_result=state["semantic_result"],
            scores=state["scores"],
        )
    }
