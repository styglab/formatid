# semantic_platform dashboard

Dashboard for the semantic catalog and source-ingestion proposals.

It reads `semantic-platform-api` through its local `/api/` nginx proxy.

Main views:

- overview KPIs
- source documents, operations, operation fields, semantic types, capabilities,
  and approved mappings
- proposal review with apply/reject actions

Proposal actions are explicit review gates:

- `Apply` writes approved catalog rows to semantic_platform Postgres tables
- `Reject` records review status in Postgres

The dashboard does not mutate `pubdata_mcp`; that app only consumes approved
semantic platform execution contracts.
