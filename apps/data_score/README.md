
# Data Score

AI-Native Data Quality Evaluation

`apps/data_score`는 전통적인 Data Quality 검사와 LLM 기반 Semantic Quality Evaluation을 결합해 데이터 품질을 점수화하고, 문제 원인과 개선 방안을 제안하는 앱이다.

## Vision

목표는 데이터가 단순히 형식적으로 유효한지뿐 아니라 실제 업무 목적에 충분히 쓸 수 있는지를 평가하는 것이다.

- 데이터 품질을 점수화한다.
- 데이터 품질 문제를 설명한다.
- 데이터 품질 개선 방안을 제안한다.
- 데이터의 의미적 품질을 평가한다.

## Core Concept

### Traditional Data Quality

정형 품질 검사는 데이터의 기계적 신뢰도를 평가한다.

- Completeness
- Validity
- Consistency
- Uniqueness
- Timeliness

대표 지표:

- Null rate
- Duplicate rate
- Freshness
- Pattern validation
- Range validation
- Distinct count
- Length statistics

### Semantic Data Quality

Semantic 품질 평가는 LLM Judge가 데이터의 업무 적합성과 의미적 충분성을 평가한다.

예를 들어 기업 설명 데이터가 `"삼성전자는 반도체 회사이다."`라고 되어 있다면 traditional DQ 기준에서는 문제가 없을 수 있다. 하지만 실제 비즈니스 활용 관점에서는 정보가 부족할 수 있다.

Semantic Judge는 다음을 평가한다.

- Coverage
- Specificity
- Consistency
- Business fitness
- Documentation quality

## Architecture

```text
Dataset
  -> Profiler
  -> Traditional DQ Engine
  -> Rubric Generator
  -> LLM Judge
  -> Score Engine
  -> Quality Report
```

## MVP Scope

초기 MVP는 전체 플랫폼보다 `LangGraph` 기반 평가 파이프라인과 리포트 계약을 먼저 안정화한다.

1. CSV 또는 테이블 입력
2. `LangGraph` 실행 상태 생성
3. 데이터 프로파일 생성
4. Traditional DQ 수행
5. Semantic Rubric 생성
6. LLM Judge 또는 manual judge 수행
7. 품질 점수 계산
8. 결과 리포트와 대시보드 제공

MVP에서는 trend, advanced lineage, Langfuse integration, complex React Flow orchestration은 후순위로 둔다.

## Components

### 1. Profiler

데이터셋의 구조와 기초 통계를 생성한다.

수집 정보:

- Row count
- Column count
- Null rate
- Duplicate rate
- Distinct count
- Min/max
- Distribution
- Length statistics
- Freshness

권장 도구:

- `DuckDB`: CSV/Parquet/SQL profiling, local analytical query
- `Polars`: 빠른 dataframe 연산, column statistics

### 2. Traditional DQ Engine

정형 품질 규칙을 실행하고 dimension별 점수를 계산한다.

출력 예:

```json
{
  "completeness": 95,
  "validity": 98,
  "uniqueness": 99,
  "timeliness": 80
}
```

권장 도구:

- MVP: 자체 rule engine
- 이후: `Soda Core` 또는 `Great Expectations` 검토

MVP에서는 외부 DQ framework를 바로 도입하지 않는다. 초기에는 null, duplicate, pattern, range, freshness처럼 명확한 규칙을 앱 내부의 작은 rule engine으로 구현하는 편이 빠르고 제어하기 쉽다.

### 3. Rubric Generator

데이터셋에 적합한 Semantic Evaluation 기준을 만든다.

입력:

- Dataset metadata
- Column metadata
- Sample records
- Business context
- Profile result

출력 예:

```json
{
  "dimensions": [
    {"name": "coverage", "weight": 0.3},
    {"name": "specificity", "weight": 0.3},
    {"name": "consistency", "weight": 0.2},
    {"name": "business_fitness", "weight": 0.2}
  ]
}
```

Rubric은 Great Expectations rule이 아니다. Rubric은 LLM Judge가 의미적 품질을 평가할 때 사용하는 기준이다.

### 4. LLM Judge

Rubric 기반으로 record, column, dataset의 의미적 품질을 평가한다.

입력:

- Record or sample records
- Rubric
- Profile result
- Business context

출력 예:

```json
{
  "coverage": 80,
  "specificity": 60,
  "consistency": 90,
  "business_fitness": 75,
  "reason": "회사 설명이 사실과 충돌하지는 않지만 사업 부문, 제품, 시장 정보가 부족하다.",
  "suggestions": [
    "주요 사업 부문을 추가한다.",
    "핵심 제품과 시장 정보를 보강한다."
  ],
  "confidence": 0.82
}
```

지원 모드:

- `LLM_MODE=disabled`: 외부 LLM 호출 없이 semantic judge를 skipped 처리
- `LLM_MODE=codex_manual`: 개발 중 수동 judge payload를 입력
- `LLM_MODE=openai`: OpenAI API 사용

추후 provider abstraction을 통해 Claude, Gemini, open source LLM을 추가할 수 있다.

### 5. Score Engine

Traditional score와 Semantic score를 결합해 overall score를 계산한다.

초기 계산식:

```text
overall_score = traditional_score * 0.6 + semantic_score * 0.4
```

데이터셋 유형별 preset은 후속 단계에서 추가한다.

- 로그/이벤트 데이터: traditional 0.8, semantic 0.2
- 문서/설명형 데이터: traditional 0.4, semantic 0.6
- 마스터 데이터: traditional 0.6, semantic 0.4

### 6. Quality Report

평가 결과를 사람이 검토 가능한 형태로 제공한다.

- Quality score
- Dimension score
- Quality trend
- Root cause
- Improvement suggestions
- Judge reasoning
- Evidence samples

## Storage

초기 저장소는 PostgreSQL을 사용한다.

권장 테이블:

- `data_score_datasets`
- `data_score_dataset_profiles`
- `data_score_rubrics`
- `data_score_judge_results`
- `data_score_quality_scores`
- `data_score_quality_issues`

`overall_score`는 저장할 수 있지만, traditional score와 semantic score를 별도로 보존해야 한다. rubric version이 바뀌면 semantic score의 의미도 바뀌므로 rubric versioning은 필수다.

## Observability

초기에는 platform observability table과 structured logs를 사용한다.

Langfuse는 바로 필수로 넣지 않고, LLM Judge가 실제 운영 비용과 prompt versioning 요구를 만들기 시작하면 추가한다.

Langfuse 도입 목적:

- Prompt versioning
- Evaluation history
- Judge trace
- Cost tracking
- Score tracking

## Frontend

목표 stack:

- Next.js
- TypeScript
- Tailwind CSS
- shadcn/ui
- TanStack Table
- ECharts
- React Flow

MVP에서는 Next.js + TypeScript + Tailwind + TanStack Table 조합으로 시작한다. React Flow는 평가 파이프라인 DAG를 시각적으로 편집하거나 디버깅할 필요가 생긴 뒤 도입한다.

## Main Screens

### 1. Dashboard

- Overall score
- Dataset count
- Issue count
- Quality trend

### 2. Dataset Catalog

- Search
- Filter
- Score badge
- Last evaluated time

### 3. Dataset Detail

Tabs:

- Overview
- Profile
- Quality
- Semantic Evaluation
- Activity

### 4. Quality Center

- Traditional DQ
- Semantic DQ
- Trust score
- Critical issues

### 5. Judge Result

- Rubric
- Score breakdown
- Evidence
- Improvement suggestions
- Judge reasoning

## Project Structure

Repository layer rule에 맞춰 `apps/data_score` 아래에 앱 책임을 둔다.

```text
apps/data_score/
  app/
    flows/           # LangGraph orchestration: upload -> profile -> dq -> judge -> score -> report
    tasks/           # execution boundaries
    steps/           # pure profiling, dq, scoring, report logic
    repositories/    # persistence
    semantic/        # rubric, judge contracts, semantic quality models
    service/         # API runner helpers
  frontend/
  infra/
  manifests/
    app.json
    services/
  tests/
  README.md
```

## Tool Choices

### Recommended MVP Stack

- Workflow orchestration: `LangGraph` as the default pipeline runtime
- Data profiling: `DuckDB`, `Polars`
- API: `FastAPI`
- Storage: `PostgreSQL`
- Frontend: `Next.js`, `TypeScript`, `Tailwind CSS`, `TanStack Table`
- Charts: `ECharts`
- LLM provider: OpenAI first, provider abstraction later

### Why LangGraph

LangGraph is the default orchestration layer for this app. The evaluation flow is stateful, multi-step, and branches based on profile, DQ, and judge results, so the graph should own pipeline state transitions from the beginning.

LangGraph responsibilities in Data Score:

- Pipeline state management
- Conditional routing
- Human/manual judge payload support
- Retry or skip behavior per stage
- Persistable execution state
- Later expansion into review workflows

Recommended graph shape:

```text
load_dataset
  -> profile_dataset
  -> run_traditional_dq
  -> generate_rubric
  -> sample_records
  -> judge_semantic_quality
  -> calculate_scores
  -> generate_report
```

Conditional behavior:

```text
if LLM_MODE=disabled
  -> skip semantic judge
if semantic sample is empty
  -> semantic_score unavailable
if critical traditional issue exists
  -> still generate report with blocking issue
```

LangGraph should orchestrate the evaluation state from the first MVP. DuckDB, Polars, DQ rules, scoring, and repository writes should remain normal Python functions or services under `steps/` and `repositories/`.

### What Not To Put In LangGraph

Do not put all business logic directly inside graph nodes. Graph nodes should call small, testable functions.

Keep these outside the graph:

- Null/duplicate/statistical calculations
- SQL profiling queries
- Score formulas
- JSON schema validation
- Postgres repository implementation
- Frontend-specific formatting

## Initial Data Contract

Quality report output should be stable from the beginning.

```json
{
  "dataset_id": "dataset.customer_master",
  "profile": {
    "row_count": 10000,
    "column_count": 12
  },
  "traditional_scores": {
    "completeness": 95,
    "validity": 98,
    "consistency": 92,
    "uniqueness": 99,
    "timeliness": 80
  },
  "semantic_scores": {
    "coverage": 80,
    "specificity": 60,
    "consistency": 90,
    "business_fitness": 75
  },
  "scores": {
    "traditional_score": 92.8,
    "semantic_score": 76.5,
    "overall_score": 86.3
  },
  "issues": [
    {
      "severity": "medium",
      "dimension": "specificity",
      "message": "회사 설명 컬럼의 세부 정보가 부족하다."
    }
  ],
  "suggestions": [
    "설명 컬럼에 주요 제품, 시장, 사업 부문 정보를 포함한다."
  ]
}
```

## Implementation Order

1. Define report schema, graph state schema, and domain models.
2. Implement CSV/local table loader.
3. Implement DuckDB/Polars profiler.
4. Implement basic Traditional DQ rules.
5. Implement score engine.
6. Implement `LLM_MODE=disabled` and `codex_manual` semantic judge path.
7. Implement LangGraph orchestration as the default execution path.
8. Add FastAPI endpoints.
9. Add minimal dashboard.
10. Add `LLM_MODE=openai`.
11. Add trend, Langfuse, and advanced UI.

## MVP Acceptance Criteria

- A CSV can be evaluated end to end.
- Profile result is generated.
- Traditional DQ scores are generated.
- Semantic judge can be skipped or supplied manually.
- Overall score is calculated.
- Quality issues and suggestions are returned.
- Result is persisted or exportable as JSON.
- Dashboard can list evaluated datasets and open a report.
