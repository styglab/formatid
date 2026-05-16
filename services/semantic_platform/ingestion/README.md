# ingestion

API specification ingestion lives here.

Responsibilities:

- crawl or load public API specifications
- parse OpenAPI, HTML, DOCX-extracted markdown, XML examples, and JSON examples
- extract resource candidates and raw request/response fields
- write metadata candidates for `semantic_mapper`

This layer must not decide canonical semantics directly.

## MVP Graph

`source_graph.py` is the first proposal-driven ingestion graph.

```text
read_source
  -> extract_text
  -> split_source_chunks
  -> write_source_chunks
  -> load_catalog_context
  -> analyze_source_with_llm
  -> write_proposals              # commit-mode: proposal
```

For trusted automation the same graph can swap only the final commit node:

```text
read_source
  -> extract_text
  -> split_source_chunks
  -> write_source_chunks
  -> load_catalog_context
  -> analyze_source_with_llm
  -> apply_catalog_changes         # commit-mode: direct_apply
```

`proposal` is the default and writes review artifacts for the dashboard.
`direct_apply` writes catalog YAML directly. The parsing, chunking, catalog
context loading, and LLM analysis nodes are identical; only the final commit
node changes.

The graph uses LangGraph when `langgraph` is installed. For local development it
falls back to the same sequential node order so the proposal flow can still be
tested.

The graph does not hard-code operation fields or provider choices. After text
extraction, the LLM receives:

- extracted source text
- API/operation candidate chunks extracted from the source
- current semantic catalog context
- required JSON output schema
- optional provider hint, only when supplied by the caller

`split_source_chunks` is intentionally structural, not semantic. It does not
decide canonical fields, providers, capabilities, or mappings. It only creates
reviewable API/operation candidate chunks using a hybrid strategy:

- explicit operation sections such as `오퍼레이션 명세`
- endpoint/path sections such as `GET /...`, `POST /...`, or `/status`
- operation-id windows such as `getContractInfo...`
- fallback windows around request/response/REST URI schema signals

Chunk output is JSONL so large raw documents can be reviewed, indexed, and
later stored in DB without changing the graph contract:

```text
sources/chunks/<document_id>.chunks.jsonl
```

`document_id` is source-based, not provider-based:

```text
source.<sha256_8>.<normalized_source_stem>
```

Provider names such as `nts`, `pps`, or `kma` must not be used as chunk or
proposal filename prefixes. Provider is operation metadata inferred or proposed
from content, while the source artifact identity belongs to the raw file.

Each chunk stores source offsets, text hash, chunk type, optional operation id,
service id candidate, and structural signals. LLM proposal generation should
use these chunks as focused evidence rather than reading an entire large source
document at once.

The LLM must return:

- `structured_spec`
- `semantic_platform_proposal`
- `execution_contract_proposal`

LLM mode is controlled separately from secrets:

```text
LLM_MODE=disabled      # writes explicit not_generated proposal
LLM_MODE=codex_manual  # requires an explicit manual LLM response payload
LLM_MODE=openai        # calls OpenAI; requires OPENAI_API_KEY
```

`OPENAI_API_KEY` is a secret only. Do not set it to sentinel values such as
`codex`.

In development, when `LLM_MODE=codex_manual`, Codex may manually act as the LLM.
The manual JSON is passed into the current graph run and must be labeled
`proposal_builder: codex_manual_llm` when it creates proposal content.

`codex_manual` still runs the graph. The only substituted part is the external
LLM API call inside `analyze_source_with_llm`: the node reads the explicit
`manual_llm_response` payload provided by the caller. The graph then continues
through `write_proposals`, so dashboard proposals are still graph outputs.

The graph must not auto-discover document-id fixtures such as
`sources/codex_manual_llm/<document_id>.json`; that pattern hides test data as
runtime behavior.

Provider identity must not be inferred from file names. The caller may pass a
provider hint:

```bash
LLM_MODE=codex_manual \
python3 -m services.semantic_platform.ingestion.source_graph \
  --source sources/some_api_guide.docx \
  --provider nts \
  --manual-llm-response /tmp/source_llm_response.json
```

Without `--provider`, the graph sends `provider_hint: null` and the LLM must
infer provider candidates from document content. If uncertain, proposals should
use `provider: unknown` and include `provider_candidates` with evidence.

Default source:

```text
sources/국세청_사업자등록정보 진위확인 및 상태조회 서비스.md
```

Run:

```bash
LLM_MODE=openai OPENAI_API_KEY=... \
python3 -m services.semantic_platform.ingestion.source_graph
```

Direct apply:

```bash
LLM_MODE=openai OPENAI_API_KEY=... \
python3 -m services.semantic_platform.ingestion.source_graph \
  --source sources/some_api_guide.docx \
  --commit-mode direct_apply
```

Output:

```text
sources/proposals/<document_id>.llm_graph_proposal.json
sources/chunks/<document_id>.chunks.jsonl
```

The proposal graph does not apply changes automatically. It writes two proposal
groups:

- `semantic_platform_proposal`: semantic types and capabilities for
  `services/semantic_platform/catalog/*`
- `execution_contract_proposal`: operation field mappings and capability
  implementation metadata for
  `services/semantic_platform/catalog/execution/*`. `pubdata_mcp` consumes these
  contracts after review; it does not own proposal files.

## Manual Worker

For development, run ingestion through the manual worker rather than adding a
schedule:

```bash
python3 -m services.semantic_platform.worker.flows.source_ingestion
```

Useful options:

```bash
python3 -m services.semantic_platform.worker.flows.source_ingestion --dry-run
python3 -m services.semantic_platform.worker.flows.source_ingestion --force
python3 -m services.semantic_platform.worker.flows.source_ingestion --source sources/example.docx
python3 -m services.semantic_platform.worker.flows.source_ingestion --chunks-output-dir sources/chunks
python3 -m services.semantic_platform.worker.flows.source_ingestion --commit-mode direct_apply
```

The worker scans source files, compares sha256 values with
`sources/.semantic_ingestion_registry.json`, and runs this graph only for
changed files unless `--force` is provided.

The same worker is registered as a Prefect manual deployment:

```text
semantic-platform-source-ingestion/manual
```

There is no periodic schedule. Prefect is used here for manual triggering, run
history, concurrency control, and future retries/observability.
