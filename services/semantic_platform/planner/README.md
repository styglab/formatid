# planner

Planner components are LLM-first and produce semantic execution plans.

The canonical planner is:

```text
execution_planner/
  context.py      # retrieve catalog/capability/operation-contract context
  llm.py          # call the configured LLM provider
  validator.py    # validate LLM output against approved operation contracts
  service.py      # plan_execution(query, limit)
```

The old `query_planner` capability-ranking DAG has been removed. It was not a
true execution planner because it only wrapped candidate capabilities as graph
nodes.

The execution planner should answer:

- which approved `operation_id` values should be used
- what semantic arguments each operation needs
- which planner-selected control parameters are needed, such as `inquiry_basis`
- which filters are provider request arguments and which are post filters
- which previous node output becomes a later node input
- how results should be joined or integrated

The planner may include method/path metadata copied from
`catalog/execution/operation_contracts.yaml`, but it must not create raw URLs,
inject auth keys, implement pagination, or parse provider payloads.

`pubdata_mcp` executes the returned plan. It must not infer provider operation
choice from Korean/provider terms; those choices belong in the LLM execution
plan and are validated against operation contracts.

Provider control parameters belong in `operation_contracts.yaml`, not code. A
field such as `inqryDiv` should be modeled with `kind: control`,
`planner_selects: true`, and an `enum_mapping` from semantic values to raw API
values. The planner selects the semantic value; the executor applies the
mapping mechanically.
