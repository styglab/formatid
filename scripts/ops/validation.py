from __future__ import annotations

import ast
from pathlib import Path

from shared.platform_service_catalog import list_platform_service_definitions
from shared.schedule_catalog import list_schedule_definitions
from shared.service_catalog import list_service_definitions
from shared.tasking.catalog import list_task_definitions
from shared.domain_catalog import iter_domain_manifest_paths, load_json_file
from scripts.ops.common import COMPOSE_FILE, PROJECT_ROOT


def validate_config() -> dict:
    from scripts.generate_compose import render_compose

    errors: list[str] = []
    warnings: list[str] = []

    task_definitions = list_task_definitions()
    service_definitions = list_service_definitions()
    platform_service_definitions = list_platform_service_definitions()
    schedule_definitions = list_schedule_definitions()
    task_queue_names = {definition.queue_name for definition in task_definitions}
    service_queue_names = {definition.queue_name for definition in service_definitions}
    task_names = {definition.task_name for definition in task_definitions}
    task_by_name = {definition.task_name: definition for definition in task_definitions}

    for path in iter_domain_manifest_paths():
        payload = load_json_file(path)
        if not isinstance(payload, dict):
            errors.append(f"domain manifest must be an object: path={path}")
            continue
        domain_name = payload.get("domain")
        if not domain_name:
            errors.append(f"domain manifest must define domain: path={path}")
        elif domain_name != path.parent.parent.name:
            errors.append(
                f"domain manifest domain must match directory name: path={path} domain={domain_name} directory={path.parent.parent.name}"
            )
        data_store = payload.get("data_store")
        if isinstance(data_store, dict):
            sql_path = data_store.get("sql")
            if sql_path and not (PROJECT_ROOT / sql_path).exists():
                errors.append(f"domain data_store sql file does not exist: domain={domain_name} sql={sql_path}")
        for env_file in payload.get("scheduler_env_files", []):
            if not (PROJECT_ROOT / env_file).exists():
                errors.append(f"domain scheduler env file does not exist: domain={domain_name} env_file={env_file}")

    for definition in task_definitions:
        task_domain = _domain_from_task_module(definition.module_path)
        if task_domain is None:
            errors.append(
                f"task module_path must be inside domain tasks: task_name={definition.task_name} module_path={definition.module_path}"
            )
        elif not definition.task_name.startswith(f"{task_domain}."):
            errors.append(
                f"task_name must start with its domain prefix: task_name={definition.task_name} domain={task_domain}"
            )
        module_path = _module_file_path(definition.module_path)
        if not module_path.exists():
            errors.append(
                f"task module does not exist: task_name={definition.task_name} module_path={definition.module_path}"
            )
        if definition.max_retries is not None and definition.max_retries < 0:
            errors.append(f"task max_retries must be >= 0: task_name={definition.task_name}")
        if definition.timeout_seconds is not None and definition.timeout_seconds <= 0:
            errors.append(f"task timeout_seconds must be > 0: task_name={definition.task_name}")
        if definition.backoff_seconds is not None and definition.backoff_seconds < 0:
            errors.append(f"task backoff_seconds must be >= 0: task_name={definition.task_name}")
        if definition.payload_schema is not None:
            schema_module_path, _, schema_name = definition.payload_schema.rpartition(".")
            if task_domain is not None and not schema_module_path.startswith(f"domains.{task_domain}.tasks."):
                errors.append(
                    f"task payload_schema must be inside the same domain tasks package: task_name={definition.task_name} payload_schema={definition.payload_schema}"
                )
            schema_module_file = _module_file_path(schema_module_path)
            if not schema_module_file.exists():
                errors.append(
                    f"task payload schema module does not exist: task_name={definition.task_name} payload_schema={definition.payload_schema}"
                )
            elif schema_name and not _module_has_ast_name(
                schema_module_file,
                schema_name,
                allowed_node_types=(ast.ClassDef,),
            ):
                errors.append(
                    f"task payload schema class does not exist: task_name={definition.task_name} payload_schema={definition.payload_schema}"
                )
            elif not schema_name:
                errors.append(
                    f"task payload schema path is invalid: task_name={definition.task_name} payload_schema={definition.payload_schema}"
                )

    for definition in service_definitions:
        dockerfile_path = PROJECT_ROOT / definition.dockerfile
        if not dockerfile_path.exists():
            errors.append(
                f"worker service dockerfile does not exist: service_name={definition.service_name} dockerfile={definition.dockerfile}"
            )
        for env_file in definition.env_files:
            env_path = PROJECT_ROOT / env_file
            if not env_path.exists():
                errors.append(
                    f"worker service env file does not exist: service_name={definition.service_name} env_file={env_file}"
                )
        if definition.replicas < 1:
            errors.append(f"worker service replicas must be >= 1: service_name={definition.service_name}")
        env_values = _read_env_files(definition.env_files)
        worker_queue_name = env_values.get("WORKER_QUEUE_NAME")
        if worker_queue_name is None:
            errors.append(f"worker service env must define WORKER_QUEUE_NAME: service_name={definition.service_name}")
        elif worker_queue_name != definition.queue_name:
            errors.append(
                f"worker service WORKER_QUEUE_NAME must match manifest queue_name: service_name={definition.service_name} env_queue={worker_queue_name} manifest_queue={definition.queue_name}"
            )

    for definition in platform_service_definitions:
        if definition.dockerfile is not None:
            dockerfile_path = PROJECT_ROOT / definition.dockerfile
            if not dockerfile_path.exists():
                errors.append(
                    f"platform service dockerfile does not exist: service_name={definition.service_name} dockerfile={definition.dockerfile}"
                )
        for env_file in definition.env_files:
            env_path = PROJECT_ROOT / env_file
            if not env_path.exists():
                errors.append(
                    f"platform service env file does not exist: service_name={definition.service_name} env_file={env_file}"
                )

    schedule_names: set[str] = set()
    for definition in schedule_definitions:
        if definition.name in schedule_names:
            errors.append(f"duplicate schedule name: schedule_name={definition.name}")
        schedule_names.add(definition.name)
        if definition.interval_seconds <= 0:
            errors.append(f"schedule interval_seconds must be > 0: schedule_name={definition.name}")
        if definition.lock_ttl_seconds is not None and definition.lock_ttl_seconds <= 0:
            errors.append(f"schedule lock_ttl_seconds must be > 0: schedule_name={definition.name}")
        if definition.misfire_grace_seconds <= 0:
            errors.append(f"schedule misfire_grace_seconds must be > 0: schedule_name={definition.name}")
        if definition.max_instances <= 0:
            errors.append(f"schedule max_instances must be > 0: schedule_name={definition.name}")
        if definition.task_name not in task_names:
            errors.append(
                f"schedule task does not exist in task catalog: schedule_name={definition.name} task_name={definition.task_name}"
            )
        else:
            task_definition = task_by_name[definition.task_name]
            if definition.queue_name != task_definition.queue_name:
                errors.append(
                    f"schedule queue_name must match task queue_name: schedule_name={definition.name} schedule_queue={definition.queue_name} task_queue={task_definition.queue_name}"
                )
        if definition.queue_name not in service_queue_names:
            errors.append(
                f"schedule queue is not backed by any worker service: schedule_name={definition.name} queue_name={definition.queue_name}"
            )
        if definition.payload_factory is not None:
            factory_module_path, _, factory_name = definition.payload_factory.rpartition(".")
            factory_file = _module_file_path(factory_module_path)
            if not factory_module_path or not factory_name:
                errors.append(
                    f"schedule payload_factory path is invalid: schedule_name={definition.name} payload_factory={definition.payload_factory}"
                )
            elif not factory_file.exists():
                errors.append(
                    f"schedule payload_factory module does not exist: schedule_name={definition.name} payload_factory={definition.payload_factory}"
                )
            elif not _module_has_ast_name(
                factory_file,
                factory_name,
                allowed_node_types=(ast.FunctionDef, ast.AsyncFunctionDef),
            ):
                errors.append(
                    f"schedule payload_factory function does not exist: schedule_name={definition.name} payload_factory={definition.payload_factory}"
                )

    for queue_name in sorted(task_queue_names - service_queue_names):
        errors.append(f"task queue is not backed by any worker service: queue_name={queue_name}")

    for queue_name in sorted(service_queue_names - task_queue_names):
        warnings.append(f"worker service queue has no tasks in catalog: queue_name={queue_name}")

    current_compose = COMPOSE_FILE.read_text(encoding="utf-8") if COMPOSE_FILE.exists() else ""
    rendered_compose = render_compose()
    compose_in_sync = current_compose == rendered_compose
    if not compose_in_sync:
        errors.append("generated compose is out of sync: run `python3 scripts/generate_compose.py`")

    return {
        "valid": not errors,
        "errors": errors,
        "warnings": warnings,
        "summary": {
            "task_count": len(task_definitions),
            "worker_service_count": len(service_definitions),
            "platform_service_count": len(platform_service_definitions),
            "schedule_count": len(schedule_definitions),
            "task_queue_count": len(task_queue_names),
            "worker_service_queue_count": len(service_queue_names),
            "compose_in_sync": compose_in_sync,
        },
    }


def _module_file_path(module_path: str) -> Path:
    return PROJECT_ROOT / (module_path.replace(".", "/") + ".py")


def _domain_from_task_module(module_path: str) -> str | None:
    parts = module_path.split(".")
    if len(parts) < 4 or parts[0] != "domains" or parts[2] != "tasks":
        return None
    return parts[1]


def _module_has_ast_name(
    module_file: Path,
    name: str,
    *,
    allowed_node_types: tuple[type[ast.AST], ...],
) -> bool:
    tree = ast.parse(module_file.read_text(encoding="utf-8"), filename=str(module_file))
    return any(isinstance(node, allowed_node_types) and getattr(node, "name", None) == name for node in tree.body)


def _read_env_files(env_files: tuple[str, ...]) -> dict[str, str]:
    values: dict[str, str] = {}
    for env_file in env_files:
        env_path = PROJECT_ROOT / env_file
        if not env_path.exists():
            continue
        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip().strip("'\"")
    return values
