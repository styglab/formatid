# Core Contracts

`core/contracts` contains small platform-wide contracts.

Current contract groups:

- `execution`: shared execution identity fields such as `request_id`, `correlation_id`, `run_id`, `resource_key`, and `session_id`

App-specific schemas stay under `apps/*`. Service payload schemas stay under `services/*` only when a generic platform service needs them.
