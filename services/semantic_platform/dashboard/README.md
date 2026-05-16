# semantic_platform dashboard

Dashboard for the semantic catalog and source-ingestion proposals.

It reads `semantic-platform-api` through its local `/api/` nginx proxy.

Main views:

- overview KPIs
- entities, fields, relationships, capabilities, vocabulary
- provider aliases and item details
- proposal review with apply/reject actions

Proposal actions are explicit review gates:

- `Apply` merges proposal changes into `services/semantic_platform/catalog/*`
  and `services/semantic_platform/catalog/execution/*`
- `Reject` records review status on the proposal artifact

The dashboard does not mutate `pubdata_mcp`; that app only consumes approved
semantic platform execution contracts.
