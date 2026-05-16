# g2b/mcp

G2B procurement MCP.

The MCP app is intentionally thin. Domain queries live in
`apps/g2b/domain/procurement`, semantic metadata lives in `apps/g2b/semantic`,
and this app only loads MCP tool specs from YAML.

## Service

- compose service: `g2b-mcp`
- HTTP MCP endpoint through nginx: `http://localhost:8081/g2b-mcp/mcp`
- local debug endpoint: `http://127.0.0.1:8014/mcp`
- readiness through nginx: `http://localhost:8081/g2b-mcp/health/ready`

## Tools

Tool contracts are declared in:

- `apps/g2b/mcp/tools.yaml`

The YAML file defines each tool name, input fields, semantic hints, evidence
metadata, and the Python domain handler to call.

- `search_bid(...)`
- `search_success_bid(...)`
- `search_contract(...)`
- `get_bid_context(bid_notice_no, bid_notice_order=None, category=None)`
- `get_procurement_lifecycle(bid_notice_no, bid_notice_order=None, category=None)`
- `get_tool_capabilities()`

`category` accepts:

- `SERVICE`
- `GOODS`
- `CONSTRUCTION`
- `FOREIGN`

Search tools query normalized G2B tables and return an `evidence` block with
the tool name, source tables, filters, date basis, and sort.

`exclude_cancelled` defaults to `True`, so notices with `notice_kind`
containing `취소` are excluded unless explicitly requested.

Additional filters:

- `notice_kind`: exact match, such as `등록공고`, `변경공고`, `재공고`, `취소공고`
- `contract_method`: partial match, such as `수의계약`, `제한경쟁`, `일반경쟁`
- `bid_method`: partial match, such as `전자입찰`, `직찰`, `전자시담`
- `bid_notice_no`, `bid_notice_order`: exact notice identity filters
- `opening_from`, `opening_to`: opening date/time range
- `notice_agency_name`: 공고기관 partial match
- `demand_agency_name`: 수요기관 partial match
- `has_budget`: `true` for budget-only, `false` for no-budget-only

Date semantics:

- bid notice posting questions use `published_from` / `published_to`
- bid deadline questions use `deadline_from` / `deadline_to`
- award date questions use `final_success_from` / `final_success_to`
- contract date questions use `contract_date_from` / `contract_date_to`
- contract `registered_from` / `registered_to` means API record registration date, not actual contract date

Date filters accept values such as `20260531`, `202605311800`, `2026-05-31`,
or `2026-05-31T14:30:00`.

Required database env:

```text
G2B_MCP_DATABASE_URL=postgresql://...
```

## Run

```bash
docker compose --env-file deploy/compose/env/compose.env -f deploy/compose/docker-compose.yml up -d --build g2b-mcp
```
