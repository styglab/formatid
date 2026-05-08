# g2b_mcp

G2B bid search MCP prototype.

## Service

- compose service: `g2b-mcp`
- HTTP MCP endpoint through nginx: `http://localhost:8081/g2b-mcp/mcp`
- local debug endpoint: `http://127.0.0.1:8014/mcp`
- readiness through nginx: `http://localhost:8081/g2b-mcp/health/ready`

## Tools

- `search_bid(category=None, keyword=None, notice_kind=None, exclude_cancelled=True, contract_method=None, bid_method=None, bid_notice_no=None, bid_notice_order=None, published_from=None, published_to=None, deadline_from=None, deadline_to=None, opening_from=None, opening_to=None, organization_name=None, demand_org_name=None, has_budget=None, min_budget=None, max_budget=None, limit=10, offset=0, sort_by="published_at", sort_order="desc")`

`category` accepts:

- `SERVICE`
- `GOODS`
- `CONSTRUCTION`

`search_bid` queries the normalized `g2b.bid_public_notice` table.

`exclude_cancelled` defaults to `True`, so notices with `notice_kind`
containing `취소` are excluded unless explicitly requested.

Additional filters:

- `notice_kind`: exact match, such as `등록공고`, `변경공고`, `재공고`, `취소공고`
- `contract_method`: partial match, such as `수의계약`, `제한경쟁`, `일반경쟁`
- `bid_method`: partial match, such as `전자입찰`, `직찰`, `전자시담`
- `bid_notice_no`, `bid_notice_order`: exact notice identity filters
- `opening_from`, `opening_to`: opening date/time range
- `demand_org_name`: demand organization partial match
- `has_budget`: `true` for budget-only, `false` for no-budget-only

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
