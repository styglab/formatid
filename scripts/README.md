# Scripts

Use only these public entrypoints:

- `python3 scripts/generate_compose.py`
- `python3 scripts/ops.py <command>`

`scripts/ops/*` contains implementation modules for `scripts/ops.py`.
Do not call those modules directly from shell scripts or CI.

Current commands:

- `validate-config`: validate manifests and generated compose
- `lint-boundaries`: check core/service boundary rules
- `check-all`: run local validation suite
- `catalog`: print platform/app service catalog
- `smoke`: run a generated compose smoke test for active services
- `context-platform reset`: clear Context Platform data and reapply the baseline seed graph
- `context-platform seed-registry`: apply the baseline Context Platform seed graph
- `context-platform ingest-source <source_path>`: upload a source document through
  the Context Platform worker and run ingestion to proposal
- `context-platform ingest-queued-source <run_id>`: run ingestion for a source
  document already queued by dashboard Source Intake
- `checkpoints`: inspect runtime checkpoints
- `prune-observability`: delete old observability rows

## Context Platform ingestion

Run from the host, not inside the worker container.

The baseline seed graph is model-only. It is generated from
`tmp/context_platform/seed/*.linkml.yaml` and seeds object types, property types,
and enum/value-domain definitions. It does not seed source systems, source
documents, source operations, source fields, bindings, concepts, canonical
representations, capabilities, executions, or proposals. Those records must come
from Source Intake and agent-assisted ingestion/review.

The shortest command uses the default environment-selected agent mode. If no
agent response artifact is supplied, ingestion may stop at the generated
request/evidence boundary instead of producing a final proposal bundle:

```bash
python3 scripts/ops.py context-platform ingest-source \
  "tmp/sources/오픈API 활용자가이드_기업 재무정보.pdf"
```

Use `agent_manual` when Codex or another operator agent supplies an explicit
response artifact:

```bash
python3 scripts/ops.py context-platform ingest-source \
  "tmp/sources/오픈API 활용자가이드_기업 재무정보.pdf" \
  --name "company-finance-api" \
  --provider "public-data" \
  --agent-mode manual \
  --agent-response "tmp/context-platform/company-finance.agent-response.json"
```

For a document uploaded through the dashboard, use the queued onboarding run id:

```bash
python3 scripts/ops.py context-platform ingest-queued-source "<run_id>" \
  --agent-mode manual \
  --agent-response "tmp/context-platform/company-finance.agent-response.json"
```

`--agent-response` must be a JSON file containing agent-supplied
`operation_candidates`, `field_candidates`, `meaning_resolution`,
`resolution_generation`, and `capability_generation` payloads. Legacy
`canonical_reconciliation`, `binding_generation`, and `capability_contracting`
keys are accepted only as aliases. Without that file,
`agent_manual` stops at `waiting_manual_llm` and returns the manual request
instead of producing the final proposal bundle.

The CLI does not auto-generate the agent response JSON by itself. Let Codex or
another operator agent create the JSON file explicitly and pass it through
`--agent-response`. `--llm-mode codex_manual` and `--manual-llm-response` remain
legacy aliases.

Use `--llm-mode disabled` only when you intentionally want deterministic
extraction without LLM judgment.
