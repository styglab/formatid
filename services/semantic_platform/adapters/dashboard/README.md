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
- catalog versions with detail, diff summary, read-only snapshot view, JSON
  download, and restore
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

Catalog version actions are also explicit governance gates:

- `View Snapshot` switches the catalog views to the selected read-only snapshot.
- `Back to Current` returns to the active catalog.
- `Download JSON` exports the version snapshot envelope from the API.
- `Restore` calls the admin API restore endpoint and creates a new active
  catalog version instead of rewriting history.

Sources can be operated one at a time today. The intended batch codex-manual UX
is a multi-select prompt generator that includes selected source ids, latest
revision ids, secret refs, `commit_mode=proposal`, apply prohibition, and the
rule that Codex may use non-secret `tmp/*` artifacts without asking.
