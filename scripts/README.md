# Scripts

Use only these public entrypoints:

- `python3 scripts/generate_compose.py`
- `python3 scripts/ops.py <command>`

`scripts/ops/*` contains implementation modules for `scripts/ops.py`.
Do not call those modules directly from shell scripts or CI.

Current commands:

- `validate-config`: validate manifests and generated compose
- `lint-boundaries`: check core/service boundary rules
- `check-all`: run local validation suite
- `catalog`: print platform/app service catalog
- `smoke`: run a generated compose smoke test for active services
- `semantic-platform reset`: clear semantic platform catalog data
- `semantic-platform seed-registry`: seed core semantic type registry
- `checkpoints`: inspect runtime checkpoints
- `prune-observability`: delete old observability rows
