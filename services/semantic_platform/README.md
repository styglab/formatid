# semantic_platform

Postgres-backed semantic catalog and execution-planning platform for public API
specifications. This is the repository's **Semantic Agentic Data Platform**
control plane: it turns raw public API documents into reviewed semantic
capabilities, executable contracts, and planner-ready runtime context.

This service is the declarative semantic intelligence layer. It owns source
evidence, endpoint metadata, field evidence, proposals, approved semantic
catalog objects, capability retrieval metadata, and LLM execution planning.
Provider HTTP execution remains in `apps/pubdata_mcp`.

## Source Layout

`semantic_platform` is one control-plane boundary. Runnable adapters and
internal libraries are separated:

```text
services/semantic_platform/
  adapters/
    admin_api/          admin/control-plane HTTP API adapter
    planner_api/  runtime planner API adapter for MCP/executor clients
    dashboard/    browser UI adapter
    worker/       Prefect/manual background adapter
  lib/
    ingestion/  source document -> evidence/proposal/apply graph
    planner/    question -> semantic execution plan
    context/    planner/MCP runtime context packaging
    storage/    Postgres catalog repository
  manifests/    compose/catalog service declarations
```

`adapters/*` may expose processes and transports. Shared semantic intelligence,
catalog mutation, graph orchestration, planning, and repository code belong in
`lib/*`.

The API adapters are intentionally split:

- `adapters/admin_api`: admin/control plane for dashboard, source upload, ingestion,
  proposal review, catalog governance, and run tracking.
- `adapters/planner_api`: runtime plane for MCP/executor clients. It exposes
  approved catalog/contract reads, capability retrieval, endpoint check records,
  and `POST /semantic/planner/execution-plan`. It must not expose source upload,
  secret CRUD, ingestion, proposal review, or catalog mutation.

## Ingestion Execution Boundary

The semantic platform API is the canonical ingestion execution boundary.
Dashboard, CLI, Prefect/manual worker flows, and future automation must start
source ingestion through the API so every run is recorded in
`sp_ingestion_runs` and visible on `/ingestion-runs`.

```text
Source file
  -> POST /sources/upload
  -> POST /sources/{source_id}/ingest
  -> sp_ingestion_runs
  -> dashboard /ingestion-runs
```

The ingestion graph implementation remains in `lib/ingestion`, but direct graph
execution is an internal API-server implementation detail. CLI modules are thin
API clients; they must not bypass run tracking by calling the graph directly.

## Catalog Versions

Catalog changes that apply approved proposals or mutate governed catalog items
create catalog version rows in `sp_catalog_versions`. A version is an audit and
rollback snapshot of the approved declarative catalog, not a dump of every
runtime artifact.

Snapshot scope:

```text
approved_declarative_catalog_v1
```

Included sections:

```text
semantic_types
entities
entity_identifiers
semantic_join_rules
capabilities
capability_entity_links
capability_dependencies
planning_examples
resources
operations
operation_fields
operation_contracts
operation_variants
field_mappings
capability_implementations
```

Excluded sections include source documents/revisions, evidence snapshots,
proposals, endpoint checks, ingestion runs, planner feedback, execution graphs,
secrets, capability documents, and capability vectors.

Version operations:

```text
GET  /catalog/versions
GET  /catalog/versions/{version_id}
GET  /catalog/versions/{version_id}/diff
GET  /catalog/versions/{version_id}/export
POST /catalog/versions/{version_id}/restore
```

Dashboard users can view a version as a read-only catalog snapshot, download it
as JSON, compare it with the previous active version, or restore it. Restore
does not rewrite history: it applies the selected snapshot to current catalog
tables and creates a new active version with `reason=version_restore` and
`metadata.restored_from_version_id`.

## Local Development

Install semantic platform test/import dependencies before running local unit
tests. Use a virtual environment; system Python may reject direct `pip install`
in PEP 668 managed environments.

```bash
python3 -m venv .venv/semantic_platform
.venv/semantic_platform/bin/python -m pip install -r services/semantic_platform/requirements-dev.txt
.venv/semantic_platform/bin/python -m unittest discover -s tests/semantic_platform
```

The Docker API/worker images use their own runtime requirements under
`adapters/*/infra/image/requirements.txt`; keep shared package pins such as
`langgraph` aligned with `requirements-dev.txt`.

The product is not a public API wrapper. The target architecture is:

```text
Natural Language
  -> Semantic Understanding
  -> Capability Resolution
  -> Execution Planning
  -> API Orchestration
  -> Semantic Data Integration
  -> Structured Result / Answer Synthesis
```

## Current Requirement

The platform is now built around **retrieval-first tool routing**:

```text
User Question
  -> Capability Catalog
  -> Embedding / Vector Index
  -> Semantic Retrieval
  -> Rerank
  -> Planner
  -> Execution Graph
  -> pubdata_mcp execution
```

This avoids putting every tool/capability into the planner prompt. The planner
receives only the small set of retrieved capabilities and their executable
variants.

Capability coverage is decided by the LLM planner. The runtime must not add
deterministic keyword rules for domain or intent matching, such as interpreting
Korean words in code to decide that a capability exists or is missing. Retrieval
returns candidates; planner validation checks that selected ids exist in the
provided context; the LLM decides whether the candidates satisfy the question.
If none are sufficient, the planner should return a structured not-found plan:

```json
{
  "planner": {"status": "not_found", "reason": "capability_not_found"},
  "execution_graph": {"type": "dag", "status": "not_found", "nodes": []},
  "errors": [{"code": "capability_not_found"}]
}
```

The catalog is split into three concerns.

### 1. Capability Catalog

Retrieval-facing data. This is what gets embedded and searched.

It answers:

```text
"What can the platform do?"
```

Contains:

- `capabilities`
- capability names and descriptions
- aliases
- examples
- use cases / `use_when`
- inputs and outputs as semantic types
- tags/domains
- related semantic types
- capability naming policy
- eventually, capability graph dependencies such as required upstream
  resolution capabilities

Example:

```json
{
  "capability_id": "search_procurement_contracts",
  "description_ko": "공공 조달 계약을 검색한다.",
  "aliases": ["계약 검색", "수주 내역", "나라장터 계약"],
  "examples": ["300억 이상 수주 계약", "최근 공사 계약"],
  "inputs": ["contract_date_range", "business_registration_number"],
  "outputs": ["contract", "organization", "contract_amount"]
}
```

### 2. Execution Catalog

Execution-facing data. This is not the primary retrieval unit; it turns a
retrieved capability into API calls.

It answers:

```text
"How do we execute the selected capability?"
```

Contains:

- `resources`
- `operations`
- `operation_contracts`
- `operation_variants`
- `field_mappings`
- `capability_implementations`
- `endpoint_checks`

Execution contracts are declarative. Runtime provider choices, control
parameters, response paths, field mappings, request transforms, and validation
rules must come from approved catalog data.

Example:

```json
{
  "capability_id": "search_procurement_contracts",
  "variant_id": "procurement_contracts.goods.by_registration_datetime",
  "operation_id": "contract_info.get_goods_contracts",
  "method": "GET",
  "path": "/getCntrctInfoListThng",
  "fixed_raw_arguments": {"inqryDiv": "1"},
  "request_mapping": {
    "contract_date_range.from": "inqryBgnDt",
    "contract_date_range.to": "inqryEndDt"
  },
  "response_mapping": {
    "untyCntrctNo": "integrated_contract_number"
  }
}
```

### 3. Governance / Review Context

Quality-control data used during ingestion and review. This is not directly
used as a runtime tool.

Contains:

- naming decisions
- deprecated capability ids
- merge suggestions
- recent proposals
- conflicts
- lineage
- review status

### Catalog Context

`load_catalog_context` is an ingestion-time grounding step. It is not a fourth
catalog. It packages:

```text
Capability Catalog
+ Execution Catalog summary
+ Governance / Review Context
```

and gives that context to the LLM before it proposes changes. Its purpose is to
reuse existing canonical capability/semantic-type definitions and avoid creating
duplicate endpoint-shaped capabilities.

## Semantic Entity Registry

`SemanticType` alone is not enough for multi-step orchestration. The platform
must also maintain a lightweight semantic entity registry and join rules. The
near-term model should remain simple and Postgres-backed:

```text
Entity
  Business
  Contract
  Organization

Identifier / SemanticType
  business_registration_number
  corporate_registration_number
  integrated_contract_number

Join Rule
  Business.business_registration_number
    -> Contract.business_registration_number
```

Do not start RDF/OWL-first. Keep this as a lightweight knowledge graph over
entities, identifiers, capabilities, fields, restrictions, and joins. RDF export
can be added later if needed.

Desired ingestion shape:

```text
read_source                         # ingestion/source_loader.py
  -> extract_text
  -> extract_blocks
  -> detect_api_sections
  -> extract_structured_evidence
  -> load_catalog_context
  -> llm_propose_capability_catalog
  -> llm_propose_execution_catalog
  -> verify_endpoint_candidates      # diagnostic probe using generated resource/contract evidence
  -> verify_execution_variants       # ingestion/endpoint_probe.py
  -> build_review_proposal
  -> apply/reject
  -> build_capability_documents
  -> embed_capabilities
  -> upsert_vector_index
```

## Internal Layout

```text
services/semantic_platform/
  adapters/
    admin_api/    admin/control-plane HTTP API boundary
    planner_api/  runtime planner API boundary
    dashboard/    catalog/planner UI
    worker/       manual/background ingestion runner
  lib/
    ingestion/  source document -> evidence -> proposal -> apply
    planner/    question -> semantic execution plan
    context/    planner/MCP runtime context helpers
    storage/    Postgres schema and repository
  manifests/    compose/catalog service declarations
```

`ingestion/graph.py` should remain orchestration-focused. Keep large concerns
in smaller modules:

- `ingestion/state.py`: graph state and ingestion/prompt version constants
- `ingestion/graph.py`: LangGraph-style graph definition and CLI compatibility wrapper
- `ingestion/graph_runtime.py`: LangGraph-backed `StateGraph`/`add_node`/`add_edge`/`compile` wrapper
- `ingestion/runner.py`: graph execution, repository writes, apply, capability docs, embeddings
- `ingestion/evidence_snapshot.py`: evidence snapshot payload and file writing
- `ingestion/nodes/`: graph node entrypoints, each taking and returning `SourceGraphState`
- `ingestion/nodes/source.py`: source loading node
- `ingestion/nodes/evidence.py`: text/block/API-section/evidence nodes
- `ingestion/nodes/catalog_context.py`: ingestion-time catalog context packaging node
- `ingestion/nodes/endpoint.py`: endpoint/variant verification node exports
- `ingestion/nodes/llm_proposal.py`: LLM capability/execution proposal node entrypoints
- `ingestion/nodes/proposal.py`: proposal filtering/building node entrypoints
- `ingestion/llm/proposal.py`: LLM call, LLM context packaging, operation variant candidates
- `ingestion/llm/validation.py`: LLM response and execution-contract validation
- `ingestion/proposal/builder.py`: proposal envelope, capability closure, proposal item generation
- `ingestion/source_loader.py`: source bytes, sha256, source id, manifest and sidecar metadata
- `ingestion/endpoint_probe.py`: evidence-time endpoint probing and safe variant verification
- `ingestion/evidence.py`, `extraction.py`, `chunking.py`: document evidence extraction

Do not add provider execution runtime behavior to ingestion. Endpoint probing is
only evidence collection for proposal quality; durable provider execution,
pagination, retry, and response normalization belong in `apps/pubdata_mcp`.

Ingestion LLM output should propose both semantic meaning and executable
contracts. It should identify request field rules such as required fields,
defaults, enums, patterns, examples, and transforms. The runtime executor only
interprets approved rules; it must not infer them from provider names or Korean
keywords.

Request field rule shape:

```json
{
  "semantic_type": "phone_number",
  "required": true,
  "transform": {
    "name": "phone_format",
    "style": "kr_mobile_hyphen"
  },
  "pattern": "^01[016789]-[0-9]{3,4}-[0-9]{4}$",
  "examples": ["010-2222-3333"]
}
```

### Source File Metadata

Source file identity is managed by uploaded source metadata, not by a root
`sources/` directory. The content hash remains in `sha256` for change
detection, while files are stored as source revisions in object storage.

For CLI development only, a human-readable source id can be supplied with:

- sidecar files such as `example.docx.source.json` or `example.source.json`
- `SEMANTIC_PLATFORM_SOURCE_MANIFEST=/path/to/manifest.json`

Manifest shape:

```json
{
  "sources": [
    {
      "path": "국세청_사업자등록정보 진위확인 및 상태조회 서비스.md",
      "provider": "nts",
      "provider_name_ko": "국세청",
      "source_key": "business_registration_status",
      "title": "사업자등록정보 진위확인 및 상태조회 서비스",
      "version": "v1",
      "tags": ["business", "tax", "public_api"]
    }
  ]
}
```

When `provider` and `source_key` are present, ingestion uses a stable id like:

```text
source.nts.business_registration_status.v1
```

When metadata is absent, ingestion falls back to the legacy hash-prefixed id:

```text
source.<sha8>.<file_slug>
```

The current implementation includes the source/evidence/proposal/execution-contract
skeleton, retrieval-facing capability documents, a local HTTP embedding service,
and a Postgres/pgvector capability index.

Implemented foundation:

- Postgres tables for retrieval-facing capability documents
- Postgres/pgvector table for capability document vectors
- Postgres tables for stored execution graphs
- Postgres tables for planner/governance feedback
- API endpoints for dictionary, capability document rebuild/list, capability
  retrieval, execution graph storage/list, and feedback capture
- BGE-m3-ko embedding service via `services/embedding`
- Planner context starts from capability retrieval and validates the execution
  graph against approved execution contracts/variants
- Section-level catalog pagination for dashboard/API use
- Dashboard review screens for capability execution paths, operation contracts,
  variants, endpoint checks, and raw catalog rows

Still needs hardening:

- retrieval evaluation set
- examples dataset for question -> capability/variant selection
- semantic entity registry and join-rule authoring/review
- planner DAG schema validation and multi-step execution evaluation
- automatic feedback-driven reindexing
- LLM planner evaluation for ambiguous or missing capability cases

## Boundary

```text
services/semantic_platform = declarative semantic intelligence
apps/pubdata_mcp           = imperative provider execution runtime
```

`semantic_platform` decides what should be executed:

- source document ingestion
- operation and field evidence extraction
- semantic type and capability proposals
- capability catalog documents for retrieval
- capability embeddings and vector index updates
- approved operation contracts
- LLM-first execution plans

`pubdata_mcp` executes how it is called:

- auth
- HTTP transport
- retries/pagination
- raw response parsing
- normalization using approved contracts

## Internal Module Boundaries

Keep `services/semantic_platform` split by responsibility:

```text
adapters/admin_api        HTTP boundary only: catalog/proposal/planner endpoints
adapters/dashboard  browser UI only: consumes API data
adapters/worker     optional Prefect background/manual ingestion runner
lib/ingestion       source documents -> evidence -> proposals -> optional apply
lib/planner         user question + retrieved catalog context -> execution plan
lib/context         planner/MCP runtime context packaging helpers
lib/storage         Postgres schema, repository, catalog persistence
```

Rules:

- `api/` must not implement catalog mutation details directly; call domain or
  repository APIs.
- `dashboard/` must not encode provider/domain routing decisions.
- `ingestion/` may probe candidate endpoints for evidence, but must not become
  the provider execution runtime.
- `planner/` must not call provider APIs.
- `storage/` must not perform semantic/domain inference.
- `worker/` is optional orchestration around ingestion; it must not contain a
  separate ingestion implementation.
- Provider execution, auth, pagination, retry, and raw response quirks belong
  in `apps/pubdata_mcp`.

## Storage

Postgres is the source of truth. YAML files are not the catalog source of truth.

Required env:

```text
SEMANTIC_PLATFORM_DATABASE_URL=postgresql://postgres:postgres@postgres:5432/postgres
```

Core tables:

```text
sp_source_documents
sp_source_chunks
sp_resources
sp_operations
sp_operation_fields
sp_semantic_types
sp_entities
sp_entity_identifiers
sp_capabilities
sp_capability_entity_links
sp_capability_dependencies
sp_capability_documents
sp_operation_contracts
sp_operation_variants
sp_field_mappings
sp_capability_implementations
sp_semantic_join_rules
sp_planning_examples
sp_endpoint_checks
sp_execution_graphs
sp_planner_feedback
sp_proposals
sp_proposal_items
sp_catalog_lineage
sp_catalog_versions
```

Reset only semantic platform catalog data when starting a clean ingestion
verification loop:

```bash
python3 scripts/ops.py semantic-catalog reset
```

This keeps the database, schema, extensions, env files, source documents, and
model volumes intact. It clears `sp_*` semantic catalog rows, proposals,
source/evidence snapshots, planner feedback, execution graphs, and capability
vectors.

Lineage is first-class:

```text
source_document
  -> source_chunk
    -> operation
      -> operation_field
        -> proposal_item
          -> approved catalog object
```

## Ingestion

The current ingestion graph is an MVP and is intentionally small:

```text
read_source
  -> extract_text_node
  -> extract_blocks_node
  -> detect_api_sections_node
  -> extract_structured_evidence
  -> load_catalog_context
  -> llm_propose_capability_catalog
  -> llm_propose_execution_catalog
  -> verify_endpoint_candidates
  -> verify_capabilities
  -> build_review_proposal
  -> store source/chunks/proposals to Postgres
  -> optionally apply proposal
```

The graph stores review proposals per capability, not as one large source-level
proposal. Endpoint and variant verification results are attached as evidence;
they must not erase LLM-proposed semantic objects before review. A proposal id is shaped like
`proposal.<source_document_id>.<capability_id>.review`. Shared resources,
semantic types, operation contracts, variants, mappings, and implementations
are grouped into the capability proposal that needs them.

Each capability payload carries provenance for source and execution tracing:

```text
source_document_id
source_file_name
source_path
source_section_ids
operation_ids
variant_ids
endpoints
evidence_snapshot_id
```

When `apply=True`, the graph applies each capability proposal, rebuilds
capability documents, and writes embeddings/vector rows through the configured
embedding service and pgvector storage.

The extractor is generic and provider-neutral. It does not decide semantics; it
collects evidence:

```text
document_blocks
api_section_candidates
field_table_candidates
example_candidates
control_field_candidates
operation_variant_candidates
```

The LLM must receive existing catalog context before proposing changes:

```text
existing semantic_types
existing capabilities
existing variants/contracts summary
naming policy
recent proposals/conflicts
```

The LLM should reuse an existing capability when possible. Endpoint differences,
provider control values, goods/construction/service categories, and request
variants belong in `operation_variant` metadata unless they represent a truly
different user intent.
`verify_capabilities` attaches capability/variant scoped verification evidence
before review. During development, `codex_manual` may supply the LLM output and optional
`verification_results` explicitly; runtime code must not invent provider rules.

For review/debugging, each run writes the extracted evidence to both Postgres
and a JSON file:

```text
sp_source_evidence_snapshots
${SEMANTIC_PLATFORM_EVIDENCE_DIR:-/tmp/semantic_platform/evidence}/<source_document_id>.api_spec_evidence.json
```

The snapshot contains detected API sections, field table candidates, examples,
control-field candidates, capability summaries from the LLM/manual analysis,
and verification results.

Capability proposals must be based only on endpoint candidates whose probe
status is `passed`. `failed` and `inconclusive` endpoint candidates remain in
the DB/file evidence snapshot for review, but they are not eligible for
capability proposal or auto-apply.

Preferred production ingestion is dashboard/API upload. The API stores source
revisions and records the ingestion run in Postgres:

```bash
curl -F "file=@/path/to/api_spec.docx" \
  -F "provider=pps" \
  -F "title=나라장터 계약정보서비스" \
  http://localhost:18080/api/sources/upload
```

CLI ingestion is only a convenience wrapper around the same API boundary. It
uploads the local file first; it does not require or read the retired root
`sources/` directory:

```bash
python3 -m services.semantic_platform.lib.ingestion.graph \
  --source /path/to/api_spec.docx
```

Production dashboard ingestion expects the API service to run in OpenAI mode:

```env
LLM_MODE=openai
OPENAI_API_KEY=...
```

In that mode the dashboard sends no manual LLM payload. The API parses the
source, calls OpenAI from the semantic platform service, and records the run in
`sp_ingestion_runs`.

Apply directly only for controlled runs:

```bash
python3 -m services.semantic_platform.lib.ingestion.graph \
  --source /path/to/api_spec.docx \
  --llm-mode openai \
  --llm-secret-ref secret.openai_api_key \
  --apply
```

Worker/CLI development can still use `codex_manual` by passing the manual LLM
response explicitly through the API boundary. This does not require the API
service itself to switch out of OpenAI mode for dashboard users:

```bash
python3 -m services.semantic_platform.lib.ingestion.graph \
  --source /path/to/api_spec.docx \
  --manual-llm-response /tmp/source_llm_response.json
```

When `--manual-llm-response` is present, the request is recorded with
`llm_mode=codex_manual` and `manual_llm_response_provided=true`. Runtime code
must not auto-discover hidden fixture files or infer provider rules from Korean
terms; the manual payload is the only substituted LLM output.

For multi-document codex-manual work, keep the unit of execution source-based:
create one ingestion run per source/revision and one manual LLM payload per
source. The resulting review units remain capability-scoped proposals shaped as
`proposal.<source_document_id>.<capability_id>.review`. Do not merge several
source documents into one proposal. If multiple sources describe the same
meaning, produce merge/deprecate candidates in proposal metadata rather than
hard-coding a runtime rule.

Codex/manual development may create, update, validate, copy, and delete
non-secret temporary artifacts under `tmp/*` without confirmation. This includes
manual LLM payloads and one-off request/response JSON. Secret values must not be
written to `tmp/*`.

Candidate endpoint probes may need API keys before operation contracts exist.
Use source-specific env values first, with global fallback:

```env
SEMANTIC_PLATFORM_CANDIDATE_SERVICE_KEY_<SOURCE_SHA8>=...
SEMANTIC_PLATFORM_CANDIDATE_SERVICE_KEY_PARAMETER_<SOURCE_SHA8>=serviceKey

SEMANTIC_PLATFORM_CANDIDATE_SERVICE_KEY=...
SEMANTIC_PLATFORM_CANDIDATE_SERVICE_KEY_PARAMETER=ServiceKey
```

`<SOURCE_SHA8>` is the first eight hex characters in the source document id or
sha256, uppercased. For `source.c08195ad...`, use:

```env
SEMANTIC_PLATFORM_CANDIDATE_SERVICE_KEY_C08195AD=...
SEMANTIC_PLATFORM_CANDIDATE_SERVICE_KEY_PARAMETER_C08195AD=serviceKey
```

POST probes can also provide operation/path-specific JSON bodies:

```env
SEMANTIC_PLATFORM_CANDIDATE_PROBE_BODIES={"status":{"b_no":["0000000000"]}}
```

Secrets must remain in env. Evidence, proposals, and endpoint checks must store
only redacted request values.

## Proposal Shape

LLM/manual analysis should produce catalog objects that `build_review_proposal`
groups into capability-scoped `proposal_items` with these item types:

```text
resource
operation
operation_field
semantic_type
capability
operation_contract
operation_variant
field_mapping
capability_implementation
```

`operation_variant` is required when one physical endpoint can represent
multiple semantic capabilities or different provider control values. For
example, the same endpoint may use `inqryDiv=1` for contract-date search and
`inqryDiv=2` for another inquiry basis. Store those as separate variants:

```json
{
  "variant_id": "pps.contracts.by_contract_date",
  "operation_id": "pps.contracts.search",
  "capability_id": "search_procurement_contracts_by_contract_date",
  "fixed_semantic_arguments": {"inquiry_basis": "contract_date"},
  "fixed_raw_arguments": {"inqryDiv": "1"},
  "verification": {
    "safe_to_call": true,
    "sample_semantic_arguments": {
      "contract_date": {"from": "2025-01-01", "to": "2025-01-31"}
    }
  }
}
```

`operation_contract` is the physical operation contract. If the physical
operation has multiple semantic meanings, keep the variant-specific
`capability_id` on `operation_variant` and `capability_implementation`.

Validation and endpoint checks are capability/variant-scoped. A single
endpoint-level check is not enough when provider control parameters change the
meaning of the response.
Only variants whose verification status is `passed` are promoted into proposal
items. Failed variants remain in the evidence snapshot and proposal raw payload
for review, but they are not proposed as approved catalog objects.

Control fields are not global aliases. A name like `inqryDiv` belongs to the
current operation contract, and the same name may have different values or
meaning in another API. The ingestion graph therefore passes
`operation_variant_candidates` to the LLM and expects the LLM to decide variant
boundaries from the local request table, descriptions, examples, and endpoint
checks. Executors must only apply reviewed `fixed_raw_arguments`; they must not
guess control values from Korean/provider terms.

The system stores proposals first. Applying a proposal writes approved catalog
objects and lineage rows. Source evidence snapshots remain source-level, while
review/apply units are capability-level.

## Planner

The planner is capability-first and LLM-first.

Input:

```text
user question
+ approved capabilities
+ approved operation contracts
+ approved semantic types
```

Output:

```json
{
  "execution_graph": {
    "type": "dag",
    "nodes": [
      {
        "id": "contracts",
        "capability": "search_procurement_contracts",
        "variant_id": "pps.contracts.by_contract_date",
        "operation_id": "pps.contract_info.getCntrctInfoListCnstwk",
        "call": {
          "semantic_arguments": {
            "contract_date": {"from": "2025-01-01", "to": "2025-12-31"}
          }
        },
        "argument_bindings": {},
        "post_filters": []
      }
    ]
  }
}
```

Validator rule: a plan may use only approved `operation_id` values from
Postgres `sp_operation_contracts`. When approved `operation_variant` rows
exist, the planner should select `variant_id` first and inherit that variant's
fixed semantic/raw arguments.

## Execution Contracts

Operation contracts are declarative runtime instructions. Provider-specific
request defaults, auth parameters, success/error checks, item roots, and field
paths belong in approved catalog data, not in `pubdata_mcp` code.

Response contracts should declare extraction paths explicitly:

```json
{
  "response": {
    "items_path": ["response.body.items.item", "response.body.items"],
    "count_path": "response.body.totalCount",
    "success": {
      "path": "response.header.resultCode",
      "equals": "00",
      "message_path": "response.header.resultMsg"
    },
    "error": {
      "code_path": "response.header.resultCode",
      "not_equals": "00",
      "message_path": "response.header.resultMsg"
    },
    "fields": {
      "response.body.items.item[].untyCntrctNo": {
        "semantic_type": "integrated_contract_number"
      }
    }
  }
}
```

The executor may evaluate paths, apply declared transforms/defaults, inject the
declared auth env value, and normalize declared fields. It must not hard-code
provider meanings such as control parameter values, pagination fields,
success-code conventions, or response item locations.

## API

Important endpoints:

```text
GET  /semantic/catalog
GET  /semantic/capability-documents
POST /semantic/capability-documents/rebuild
POST /semantic/capabilities/retrieve
GET  /semantic/execution/contracts
GET  /semantic/execution/checks
POST /semantic/execution/checks
GET  /planner/execution-graphs
POST /planner/execution-graphs
GET  /semantic/governance/feedback
POST /semantic/governance/feedback
GET  /semantic/meta
GET  /sources
GET  /catalog/versions
GET  /catalog/versions/{version_id}
GET  /catalog/versions/{version_id}/diff
GET  /catalog/versions/{version_id}/export
POST /catalog/versions/{version_id}/restore
GET  /proposals
GET  /proposals/{proposal_id}
POST /proposals/{proposal_id}/apply
POST /proposals/{proposal_id}/reject
POST /planner/execution-plan
POST /runtime/context
```

## Next Work

1. Improve source chunking for tables and OpenAPI schemas.
2. Add LLM source proposal prompt and schema validation.
3. Add dashboard screens for source -> operation -> field -> proposal lineage.
4. Add planner eval cases before adding ontology/rule layers.
