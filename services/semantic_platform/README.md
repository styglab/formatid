# semantic_platform

Postgres-backed semantic catalog and execution-planning platform for public API
specifications.

This service is the declarative semantic intelligence layer. It owns source
evidence, endpoint metadata, field evidence, proposals, approved semantic
catalog objects, capability retrieval metadata, and LLM execution planning.
Provider HTTP execution remains in `apps/pubdata_mcp`.

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

Desired ingestion shape:

```text
read_source
  -> extract_text
  -> extract_blocks
  -> detect_api_sections
  -> extract_structured_evidence
  -> verify_endpoint_candidates
  -> load_catalog_context
  -> llm_propose_capability_catalog
  -> llm_propose_execution_catalog
  -> verify_execution_variants
  -> build_review_proposal
  -> apply/reject
  -> build_capability_documents
  -> embed_capabilities
  -> upsert_vector_index
```

### Source File Metadata

Source file identity should be managed by provider/source metadata, not only by
content hash. The content hash remains in `sha256` for change detection, but a
human-readable source id can be supplied with either:

- `sources/manifest.json`
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

Still needs hardening:

- retrieval evaluation set
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
sp_capabilities
sp_capability_documents
sp_operation_contracts
sp_operation_variants
sp_field_mappings
sp_capability_implementations
sp_endpoint_checks
sp_execution_graphs
sp_planner_feedback
sp_proposals
sp_proposal_items
sp_catalog_lineage
```

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
  -> verify_endpoint_candidates
  -> llm_propose_capability_catalog
  -> llm_propose_execution_catalog
  -> verify_capabilities
  -> keep_passed_verified_capabilities
  -> build_review_proposal
  -> store source/chunks/proposals to Postgres
  -> optionally apply proposal
```

The graph stores review proposals per capability, not as one large source-level
proposal. A proposal id is shaped like
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

Run one source:

```bash
python3 -m services.semantic_platform.ingestion.graph \
  --source sources/국세청_사업자등록정보\ 진위확인\ 및\ 상태조회\ 서비스.md
```

Apply directly only for controlled runs:

```bash
python3 -m services.semantic_platform.ingestion.graph \
  --source sources/some_api_spec.docx \
  --apply
```

`LLM_MODE=codex_manual` does not read hidden fixtures. If Codex substitutes the
LLM response, pass it explicitly:

```bash
LLM_MODE=codex_manual python3 -m services.semantic_platform.ingestion.graph \
  --source sources/some_api_spec.docx \
  --manual-llm-response /tmp/source_llm_response.json
```

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
