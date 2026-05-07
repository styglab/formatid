# g2b_mcp

G2B bid search MCP prototype.

## Service

- compose service: `g2b-mcp`
- HTTP MCP endpoint: `http://localhost:8013/mcp`
- readiness: `http://localhost:8013/health/ready`

## Tools

- `search_bid(category=None, keyword=None, published_from=None, published_to=None, deadline_from=None, deadline_to=None, organization_name=None, min_budget=None, max_budget=None, limit=10, offset=0, sort_by="published_at", sort_order="desc")`

`category` accepts:

- `SERVICE`
- `GOODS`
- `CONSTRUCTION`

`search_bid` queries the normalized `g2b.bid_public_notice` table.
If the normalized table returns no rows, `search_bid` falls back to the live G2B
API and applies the same filters in memory.

Date filters accept values such as `20260531`, `202605311800`, `2026-05-31`,
or `2026-05-31T14:30:00`.

Required database env:

```text
G2B_MCP_DATABASE_URL=postgresql://...
```

`G2B_INGEST_DATABASE_URL` is also accepted as a fallback.

Required live API fallback env:

```text
G2B_API_KEY=...
```

`API_KEY` is also accepted as a fallback.

## Run

```bash
docker compose --env-file deploy/compose/env/compose.env -f deploy/compose/docker-compose.yml up -d --build g2b-mcp
```
