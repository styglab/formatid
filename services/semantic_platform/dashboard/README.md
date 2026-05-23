# semantic_platform dashboard

Dashboard for the semantic catalog, source-ingestion proposals, and reviewed
execution contracts.

It reads `semantic-platform-api` through its local `/api/` nginx proxy.

Main views:

- overview KPIs
- capabilities
- semantic graph sections: entities, identifiers, semantic types, join rules,
  capability/entity links, dependencies, and planning examples
- execution catalog sections: resources, operations, operation contracts,
  operation variants, mappings, and semantic types
- proposal review with apply/reject actions
- detail tabs for overview, execution review, evidence, and raw JSON
- endpoint check history tied to capabilities, contracts, and variants

Large catalog sections are loaded through paginated API calls:

```text
GET /catalog/sections/{section}?limit=25&offset=...
```

Proposal actions are explicit review gates:

- `Apply` writes approved catalog rows to semantic_platform Postgres tables
- `Reject` records review status in Postgres

The dashboard does not mutate `pubdata_mcp`; that app only consumes approved
semantic platform execution contracts.
