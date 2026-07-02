# Context Platform Worker Pipeline

## 목적

이 문서는 `services/context_platform`의 source document ingestion worker pipeline을
정의한다.

목표는 source 문서를 Source Graph로 수집하고, Meaning Graph / Representation
Model / Resolution Graph / Capability Graph 제안을 만든 뒤, 사람이 review할
proposal bundle을 생성하는 것이다.

```text
upload source document
  -> IngestionPipelineGraph
     -> parse source operations
     -> extract parameters and response fields
     -> propose concepts
     -> propose canonical representations
     -> propose representation schemas
     -> propose field/context/parameter bindings
     -> propose capabilities
     -> verify operation/capability evidence
     -> create proposal bundle
  -> human review
  -> approve
  -> publish
```

## 아키텍처 기준

worker는 Executable Meaning Graph 경계를 따른다.

1. Source Graph
2. Meaning Graph
3. Representation Model
4. Resolution Graph
5. Capability Graph
6. Execution Graph
7. Evidence / Governance

worker가 만들 수 있는 것은 discovery facts와 proposal이다. LLM-generated 또는
automatically generated artifact는 자동 승인하지 않는다.

Source contract draft는 machine-readable spec을 먼저 사용한다. Swagger/OpenAPI
JSON/YAML이면 deterministic OpenAPI parser가 `paths` 아래 HTTP operation과
request/response schema를 읽어 `source_structure` agent artifact를 만든다.
`definitions`와 `components.schemas`는 schema flattening에만 사용하고 operation
후보로 만들지 않는다.

Swagger/OpenAPI가 아니거나 표/본문 중심 문서이면 LangExtract를 source contract
draft 생성 도구로 사용한다. Docling은 문서 layout, table, chunk를 만드는 기본
parser이고, LangExtract는 그 chunk text에서 `source_operation`,
`source_parameter`, `source_response_field`를 source-grounded extraction으로 뽑는
선택적 agent-side extractor다. OpenAPI parser와 LangExtract 결과 모두 catalog에
직접 쓰지 않고 `agent_response` artifact로 만든 뒤 source contract validation과
proposal review lifecycle을 통과시킨다.

Lifecycle:

```text
proposed -> reviewed -> approved -> published
```

적용 대상:

- concepts, concept schemes, concept relations, value domains
- object types, property types, canonical representations
- representation schemas
- source operations, parameters, fields
- field/context/parameter bindings
- capabilities, inputs, outputs, steps
- endpoint verification evidence

## Non-goals

worker에서 만들지 않는다.

- 별도 Operation Registry
- 독립 vocabulary product
- Mapping Registry라는 별도 제품 모듈
- multi-agent framework
- 별도 외부 workflow engine
- vector database
- knowledge graph database
- skills runtime
- raw operation execution path for LLM clients

`source_operations`가 executable operation의 단일 테이블이다.

## Active Runtime Structure

worker의 active runtime은 Prefect deployment가 단일 ingestion run을 시작하고,
내부에서는 하나의 LangGraph가 source document ingestion 전체를 orchestrate한다.

```text
Prefect run-context-platform-ingestion
  -> ingest_source_document(run_id)
  -> IngestionPipelineGraph
     -> load_run
     -> prepare_run
     -> parse_document
        -> OpenAPI/Swagger parser when machine-readable spec
        -> Docling/fallback chunks
        -> optional LangExtract source contract draft for text/table docs
     -> source_contract_validation
     -> persist_source_graph
     -> meaning_resolution
     -> resolution_generation
     -> capability_generation
     -> operation_verification
     -> create_proposal_bundle
```

`ingest_source_document()`는 API/worker 호환 wrapper이며, orchestration 본체는
`services/context_platform/internal/ingestion/langgraph/pipeline.py`에 둔다.

각 semantic 판단 단계는 여전히 agent/manual boundary를 가진다. `agent_manual`
모드에서 필요한 agent response가 없으면 graph는 해당 node에서
`waiting_manual_llm` 상태로 종료하고 `manual_llm_request`를 저장한다. Codex 또는
operator agent가 `--agent-response`를 통해 response artifact를 주입하면 같은
pipeline graph가 다시 실행되어 proposal bundle까지 진행한다.

단일 graph로 합치더라도 다음 경계는 유지한다.

- agent 판단은 runtime 코드에 hard-code하지 않는다.
- agent output은 `agent_response` / `manual_llm_response` boundary를 통과한다.
- catalog mutation은 직접 수행하지 않고 proposal bundle로 만든다.
- approval/publish는 review governance 단계에서만 수행한다.

agent response는 각 판단 graph에 전달되기 전에 schema와 핵심 불변식을 검증한다.
Concept는 meaning만 담아야 하므로 `datatype`, `regex`, `pattern`, `enum`,
`required`, `minimum`, `maximum`, `examples`, `validation` 같은 제약은
`representation_schema_decisions`에 둔다. Capability output은 `output_key`,
`concept_key` 또는 `canonical_ref`를 포함해야 하며, `output_key`는 canonical
property가 아니라 planner/API 소비자용 key다.

계약 회귀 fixture는
`tests/fixtures/context_platform/revenue_agent_response.json`이다. 이 fixture는
agent response artifact가 `validate_agent_response_artifact`, stage normalizer,
proposal 생성 shape까지 통과하는지 확인한다.

Source contract extraction은 semantic 판단 단계가 아니다. 이 단계의 출력은 실제
wire/API 계약만 담는다.

- `wire_name`: 실제 query/body/response key. 예: `crno`, `bizYear`,
  `enpSaleAmt`.
- `raw_name`: 별도 원천명이 없으면 `wire_name`과 동일하다.
- `label_ko`: 문서에 표시된 한국어 라벨. 예: `법인등록번호`, `사업연도`,
  `기업매출금액`.
- `field_path`: 실제 wire path. 예: `request.query.crno`,
  `response.body.items.item.enpSaleAmt`.

실행 가능한 API operation에서 `wire_name`, `raw_name`, `field_path`에 한국어
라벨이 들어오면 Source Graph에 저장하지 않고 `source_contract_validation`
단계에서 `failed_needs_review`로 중단한다.

Swagger/OpenAPI parser를 사용할 때는 다음 규칙을 적용한다.

- `paths` 아래 HTTP method만 SourceOperation 후보로 만든다.
- `definitions`와 `components.schemas`는 request/response schema flattening에만
  사용한다.
- OpenAPI security scheme의 apiKey query/header 값은 `scope=control` parameter로
  둔다.
- request body schema field는 `request.body.<wire_path>` parameter로 둔다.
- response body schema field는 `response.body.<wire_path>` response field로 둔다.

LangExtract를 사용할 때도 추출 class는 Source Graph 사실로 제한한다.

- `source_operation`: 실제 endpoint/function/job 후보
- `source_parameter`: request/query/body/header/control parameter 후보
- `source_response_field`: response payload field 후보

LangExtract의 character offset/source grounding은 evidence로 저장한다. Grounding이
없는 extraction은 review traceability가 없으므로 버린다. Concept,
CanonicalRepresentation, Binding, Capability 판단은 LangExtract source contract
draft가 아니라 후속 agent/manual stage에서 별도로 만든다.

명시 실행:

```bash
python3 scripts/ops.py context-platform draft-source-contract "<source-file>" \
  --output "tmp/context_platform/<문서명>.source-structure.agent-response.json"
```

이 명령은 proposal을 생성하지 않는다. 생성된 artifact를 사람이 확인하고 필요하면
보강한 뒤 다음 ingestion boundary로 넘긴다.

```bash
python3 scripts/ops.py context-platform ingest-source "<source-file>" \
  --agent-mode manual \
  --agent-response "tmp/context_platform/<문서명>.agent-response.json"
```

## Stage 설계

### 1. Source Upload

입력:

- uploaded source document
- source metadata
- optional reference URI

출력:

- source system/document metadata
- evidence snapshot
- onboarding run

### 2. Source Operation Parsing

API 문서에서 실행 가능한 operation을 추출한다.

출력:

- `source_operations`
- method/path
- operation summary/description
- auth/access metadata when documented
- source constraints

이 단계는 Operation Registry를 만들지 않는다. 추출 결과는 `source_operations`에
저장한다.

### 3. Parameter / Field Extraction

request parameter와 response field를 추출한다.

출력:

- `source_parameters`
- `source_fields`
- field path
- datatype / required / enum hints
- regex / pattern / validation hints
- examples
- source evidence reference

### 4. Meaning Suggestion

agent/manual response가 source term의 의미를 제안한다.

출력 proposal:

- MeaningScope
- ConceptScheme
- Concept
- ConceptRelation
- ValueDomain
- ValueDomainValue

주의:

- Concept는 meaning이다.
- `Concept.kind`와 `MeaningScope`를 반드시 둔다.
- source term이 business field인지, control인지, transport인지, response
  envelope인지 판단한 근거를 evidence로 남긴다.
- active response key는 `concept_decisions`, `representation_decisions`,
  `representation_schema_decisions`, `value_domain_decisions`,
  `relation_suggestions`다. `decisions`는 legacy alias다.
- datatype, regex, enum, required, cardinality, min/max, examples는 Concept가
  아니라 `representation_schema_decisions`에 둔다.

### 5. Representation Suggestion

Concept를 표준 구조로 표현하는 CanonicalRepresentation 후보를 만든다.

출력 proposal:

- ObjectType
- PropertyType
- LinkType
- CanonicalRepresentation
- RepresentationSchema
- ExternalProjection when needed

예:

```text
concept.finance.revenue
  -> repr.finance.revenue.observation_amount
  -> object.observation + property.observed_amount
```

`revenue_amount`는 canonical property가 아니라 capability output key 또는
projection key로 둔다.

`RepresentationSchema`는 datatype, regex/pattern, enum/value domain, cardinality,
required/default, examples, min/max 같은 제약을 담는다. Concept에는 이러한
검증 규칙을 넣지 않는다.

예:

```text
concept.identifier.kr_business_registration_number
  -> repr.identifier.kr_business_registration_number.identifier_value
  -> schema.identifier.kr_business_registration_number.plain_10_digit
     datatype string
     pattern ^\d{10}$
```

### 6. Resolution Suggestion

source parameter/source field를 CanonicalRepresentation에 연결하는 binding 후보를
생성한다.

출력 proposal:

- FieldBinding
- ContextBinding
- ParameterBinding
- TransformRule

binding 필수 정보:

- source id
- source operation id
- source parameter id 또는 source field id
- target canonical representation
- fills property 또는 context key
- required concept when parameter binding
- confidence
- status
- evidence
- transformation 또는 normalization rule
- active response key는 `field_bindings`, `context_bindings`,
  `parameter_bindings`, `transform_rules`다. `suggestions`는 legacy alias다.
- `field_bindings`는 value property를 채우고, `context_bindings`는 currency,
  fiscal_year, statement_type 같은 representation context를 채운다.
- `parameter_bindings`는 source parameter가 필요한 input concept를 채우는 계약이다.

예:

```text
enpSaleAmt -> repr.finance.revenue.observation_amount / observed_amount
curCd      -> repr.finance.revenue.observation_amount / context currency
bizYear    -> repr.finance.revenue.observation_amount / context fiscal_year
```

### 7. Capability Suggestion

Concept, Representation, SourceOperation, ResolutionBinding을 바탕으로
planner-facing capability 후보를 생성한다.

출력 proposal:

- Capability
- CapabilityInput
- CapabilityOutput
- CapabilityStep
- CapabilityConstraint

Capability는 WHAT을 설명한다. SourceOperation은 HOW를 설명한다.

Capability input/output은 가능한 경우 `concept_key`, `representation_key`,
`representation_schema_key`를 함께 참조한다. `output_key`는 planner/API 소비자용
이름이며 canonical property가 아니다.

### 8. Operation / Capability Verification

API 문서에서 실행 가능한 operation이 추출된 경우, proposal bundle 생성 전에 검증
evidence를 만든다.

검증 결과는 registry가 아니라 evidence다.

검증 범위:

- operation endpoint 호출 가능성
- required parameter sample 존재 여부
- response status
- response field coverage
- capability input/output binding readiness

기본 원칙:

- worker/admin plane만 검증 호출을 수행한다.
- LLM MCP Adapter는 raw operation을 실행하지 않는다.
- configured secret env만 사용한다.
- secret/auth 값은 request sample, proposal, tmp artifact, log에 저장하지 않는다.
- mutation endpoint는 실행하지 않고 evidence에 `skipped`로 남긴다.
- base URL이나 required sample input이 없으면 `skipped` 또는 `needs_input`으로
  남긴다.

### 9. Proposal Bundle

한 source document/run에서 나온 proposal을 review 가능한 bundle로 묶는다.

bundle은 UI 검토 컨테이너이고, 승인 단위는 proposal item이다. publish는 승인된
item만 반영한다.

## Agent Mode

Context Platform ingestion은 operator/agent assisted 흐름을 따른다. worker는 문서를
파싱하고 evidence/request를 만들며, business 의미 판단은 Codex 또는 별도 operator
agent가 만든 response artifact로 주입한다.

- `disabled`
  - agent response 없음
  - semantic 판단이 필요한 proposal은 skipped/not-generated로 남김
- `agent_manual` 또는 `manual`
  - worker가 `manual_llm_request` artifact 생성
  - Codex 또는 operator agent가 agent response JSON을 명시적으로 주입
  - response는 API/graph boundary를 통과해야 함
- `codex_manual`
  - `agent_manual`의 legacy alias

`openai`는 Context Platform ingestion mode가 아니다. `OPENAI_API_KEY` 값으로 mode를
판단하지 않는다.

agent-assisted ingestion에서 source term의 business 의미를 정하는 주체는 agent
response와 review proposal이다. runtime code는 특정 provider 필드명, 도메인 단어,
한글 키워드에 따라 concept, representation, binding, capability를 고르는 규칙을
추가하지 않는다.

`skip` 또는 `skip_binding`으로 판단된 term은 후속 proposal 생성 단계에서
fallback concept, representation, class, slot, binding으로 다시 채우지 않는다.

## CLI 실행 모드

```bash
python3 scripts/ops.py context-platform ingest-source \
  "tmp/sources/오픈API 활용자가이드_기업 재무정보.pdf"
```

`agent_manual`은 Codex나 operator agent가 만든 response JSON을 명시적으로 주입하는
모드다. 외부 LLM을 worker가 직접 호출하지 않는다.

## 관련 문서

- 개요: [context_platform_overview_ko.md](/workspace/docs/architecture/context_platform_overview_ko.md)
- 데이터 모델: [context_platform_registry_model_ko.md](/workspace/docs/architecture/context_platform_registry_model_ko.md)
- 설계 기준: [meaning-resolution-platform.md](/workspace/docs/architecture/meaning-resolution-platform.md)
