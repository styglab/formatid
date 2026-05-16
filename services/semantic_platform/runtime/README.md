# runtime

Runtime retrieval and context packaging for LLM/MCP planners.

The runtime does not expose the whole catalog to the model. It builds compact
context packages containing:

- relevant semantic types
- small entity set
- candidate capabilities/resources
- join keys
- relations
- execution hints

The main API entrypoint is `POST /runtime/context`.
