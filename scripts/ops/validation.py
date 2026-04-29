from __future__ import annotations

import ast
import re
from pathlib import Path

from core.catalog.registry import CatalogRegistry
from core.catalog.app_catalog import (
    iter_app_manifest_paths,
    list_required_platform_services,
    list_required_workers,
    load_json_file,
)
from scripts.ops.common import COMPOSE_FILE, PROJECT_ROOT


APP_SERVICE_TYPES = {"cron", "api", "consumer", "service"}
APPS_DIR = PROJECT_ROOT / "apps"
DOT_ID_PATTERN = re.compile(r"^[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)+$")
QUEUE_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_]*(?::[a-z][a-z0-9_]*)+$")
SERVICE_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9]*(?:-[a-z0-9]+)*$")
MODULE_PATH_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)+$")
SCHEMA_PATH_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)+$")


def validate_config() -> dict:
    from scripts.generate_compose import render_compose

    errors: list[str] = []
    warnings: list[str] = []

    try:
        registry = CatalogRegistry.load()
    except (KeyError, TypeError, ValueError, RuntimeError) as exc:
        return {
            "valid": False,
            "errors": [f"catalog load failed: {exc}"],
            "warnings": warnings,
            "summary": {},
        }
    task_definitions = registry.tasks
    service_definitions = registry.active_worker_services
    available_service_definitions = registry.available_worker_services
    platform_service_definitions = registry.active_platform_services
    available_platform_service_definitions = registry.available_platform_services
    app_service_definitions = registry.app_services
    queue_definitions = registry.queues
    queue_ids = {definition.queue for definition in queue_definitions}
    queue_names = {definition.queue_name for definition in queue_definitions}
    task_queue_names = {definition.queue_name for definition in task_definitions}
    service_queue_names = {definition.queue_name for definition in service_definitions}
    active_service_names = {definition.service_name for definition in service_definitions}
    active_platform_service_names = {definition.service_name for definition in platform_service_definitions}
    available_service_names = {definition.service_name for definition in available_service_definitions}
    available_platform_service_names = {
        definition.service_name for definition in available_platform_service_definitions
    }
    required_platform_service_names = set(registry.required_platform_services)
    required_worker_names = set(registry.required_workers)

    _validate_catalog_schema(
        errors=errors,
        warnings=warnings,
        task_definitions=task_definitions,
        queue_definitions=queue_definitions,
        available_service_definitions=available_service_definitions,
        available_platform_service_definitions=available_platform_service_definitions,
        app_service_definitions=app_service_definitions,
    )

    for definition in queue_definitions:
        if definition.worker_service not in available_service_names:
            errors.append(
                "queue manifest references unknown worker service: "
                f"queue={definition.queue} worker_service={definition.worker_service} available={sorted(available_service_names)}"
            )
        if not isinstance(definition.policies, dict):
            errors.append(f"queue policies must be an object: queue={definition.queue}")
            continue
        for key in sorted(set(definition.policies) - {"pause_supported", "dlq_enabled", "retry_policy", "dlq"}):
            warnings.append(f"queue policies has unsupported key: queue={definition.queue} key={key}")
        for key in ("pause_supported", "dlq_enabled"):
            value = definition.policies.get(key)
            if value is not None and not isinstance(value, bool):
                errors.append(f"queue policy must be boolean: queue={definition.queue} key={key}")
        retry_policy = definition.policies.get("retry_policy")
        if retry_policy is not None:
            if not isinstance(retry_policy, dict):
                errors.append(f"queue retry_policy must be an object: queue={definition.queue}")
            else:
                max_retries = retry_policy.get("max_retries")
                delay_seconds = retry_policy.get("delay_seconds")
                if max_retries is not None and (not isinstance(max_retries, int) or max_retries < 0):
                    errors.append(f"queue retry_policy.max_retries must be >= 0: queue={definition.queue}")
                if delay_seconds is not None and (not isinstance(delay_seconds, int) or delay_seconds < 0):
                    errors.append(f"queue retry_policy.delay_seconds must be >= 0: queue={definition.queue}")
        dlq = definition.policies.get("dlq")
        if dlq is not None:
            if not isinstance(dlq, dict):
                errors.append(f"queue dlq policy must be an object: queue={definition.queue}")
            else:
                enabled = dlq.get("enabled")
                suffix = dlq.get("suffix")
                if enabled is not None and not isinstance(enabled, bool):
                    errors.append(f"queue dlq.enabled must be boolean: queue={definition.queue}")
                if suffix is not None and (not isinstance(suffix, str) or not suffix):
                    errors.append(f"queue dlq.suffix must be a non-empty string: queue={definition.queue}")

    for service_name in sorted(required_platform_service_names - available_platform_service_names):
        errors.append(
            "required platform service is not defined: "
            f"service_name={service_name} available={sorted(available_platform_service_names)}"
        )
    for service_name in sorted(required_worker_names - available_service_names):
        errors.append(
            "required worker service is not defined: "
            f"service_name={service_name} available={sorted(available_service_names)}"
        )

    for path in iter_app_manifest_paths():
        payload = load_json_file(path)
        if not isinstance(payload, dict):
            errors.append(f"app manifest must be an object: path={path}")
            continue
        app_name = payload.get("app")
        if not app_name:
            errors.append(f"app manifest must define app: path={path}")
        elif app_name != _app_name_from_manifest_path(path):
            errors.append(
                f"app manifest app must match directory path: path={path} app={app_name} directory={_app_name_from_manifest_path(path)}"
            )
        requires = payload.get("requires", {})
        if requires is not None and not isinstance(requires, dict):
            errors.append(f"app manifest requires must be an object: app={app_name}")
        elif isinstance(requires, dict):
            allowed_keys = {"platform_services", "queues", "workers"}
            for key in sorted(set(requires) - allowed_keys):
                errors.append(f"app manifest requires has unsupported key: app={app_name} key={key}")
            for key in ("platform_services", "queues", "workers"):
                values = requires.get(key, [])
                if not isinstance(values, list) or not all(isinstance(value, str) for value in values):
                    errors.append(f"app manifest requires.{key} must be a list of strings: app={app_name}")
                    continue
                duplicated_values = _duplicated_strings(values)
                if duplicated_values:
                    errors.append(
                        f"app manifest requires.{key} contains duplicates: app={app_name} values={duplicated_values}"
                    )
            platform_services = requires.get("platform_services", [])
            if isinstance(platform_services, list):
                for service_name in sorted(set(platform_services) - available_platform_service_names):
                    errors.append(
                        "app manifest requires unknown platform service: "
                        f"app={app_name} service_name={service_name} available={sorted(available_platform_service_names)}"
                    )
            workers = requires.get("workers", [])
            if isinstance(workers, list):
                for service_name in sorted(set(workers) - available_service_names):
                    errors.append(
                        "app manifest requires unknown worker service: "
                        f"app={app_name} service_name={service_name} available={sorted(available_service_names)}"
                    )
            queues = requires.get("queues", [])
            if isinstance(queues, list):
                for queue in sorted(set(queues) - queue_ids):
                    errors.append(
                        "app manifest requires unknown queue: "
                        f"app={app_name} queue={queue} available={sorted(queue_ids)}"
                    )
        data_store = payload.get("data_store")
        if isinstance(data_store, dict):
            sql_path = data_store.get("sql")
            if sql_path and not (PROJECT_ROOT / sql_path).exists():
                errors.append(f"app data_store sql file does not exist: app={app_name} sql={sql_path}")
        dashboard = payload.get("dashboard")
        if isinstance(dashboard, dict):
            for env_file in dashboard.get("env_files", []):
                if not (PROJECT_ROOT / env_file).exists():
                    errors.append(f"app dashboard env file does not exist: app={app_name} env_file={env_file}")
        worker_env_files = payload.get("worker_env_files")
        if isinstance(worker_env_files, dict):
            worker_env_values: dict[str, str] = {}
            for service_name, env_files in worker_env_files.items():
                if service_name not in required_worker_names:
                    errors.append(
                        f"app worker env file targets an inactive worker; add it to requires.workers: app={app_name} service_name={service_name}"
                    )
                for env_file in env_files:
                    if not (PROJECT_ROOT / env_file).exists():
                        errors.append(
                            f"app worker env file does not exist: app={app_name} service_name={service_name} env_file={env_file}"
                        )
                    else:
                        worker_env_values.update(_read_env_files((env_file,)))
            allowed_worker_env = payload.get("allowed_worker_env")
            if allowed_worker_env is None and worker_env_values:
                warnings.append(
                    f"app worker env files are declared without allowed_worker_env allowlist: app={app_name}"
                )
            elif not isinstance(allowed_worker_env, list) or not all(
                isinstance(value, str) for value in allowed_worker_env
            ):
                errors.append(f"app allowed_worker_env must be a list of strings: app={app_name}")
            else:
                duplicated_values = _duplicated_strings(allowed_worker_env)
                if duplicated_values:
                    errors.append(
                        f"app allowed_worker_env contains duplicates: app={app_name} values={duplicated_values}"
                    )
                missing_env = sorted(set(allowed_worker_env) - set(worker_env_values))
                if missing_env:
                    errors.append(
                        f"app allowed_worker_env references env not present in worker env files: app={app_name} env={missing_env}"
                    )
    for definition in task_definitions:
        if definition.queue not in queue_ids:
            errors.append(f"task references unknown queue: task_name={definition.task_name} queue={definition.queue}")
        if definition.queue_name not in queue_names:
            errors.append(
                f"task resolved queue_name is not declared: task_name={definition.task_name} queue_name={definition.queue_name}"
            )
        task_app = _app_from_task_module(definition.module_path)
        runtime_namespace = _runtime_task_namespace(definition.module_path)
        runtime_task_package = _runtime_task_package(definition.module_path)
        if task_app is None and runtime_namespace is None:
            errors.append(
                f"task module_path must be inside app tasks or runtime task packages: task_name={definition.task_name} module_path={definition.module_path}"
            )
        elif not definition.task_name.startswith(f"{task_app}."):
            if task_app is not None:
                errors.append(
                    f"task_name must start with its app prefix: task_name={definition.task_name} app={task_app}"
                )
            elif runtime_namespace is not None and not definition.task_name.startswith(f"{runtime_namespace}."):
                errors.append(
                    f"runtime task_name must start with its namespace prefix: task_name={definition.task_name} namespace={runtime_namespace}"
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
        for schema_field in ("payload_schema", "output_schema"):
            schema_path = getattr(definition, schema_field, None)
            if schema_path is not None:
                _validate_task_schema_path(
                    errors=errors,
                    task_name=definition.task_name,
                    schema_field=schema_field,
                    schema_path=schema_path,
                    task_app=task_app,
                    runtime_task_package=runtime_task_package,
                )

    for definition in service_definitions:
        if definition.queue not in queue_ids:
            errors.append(f"worker service references unknown queue: service_name={definition.service_name} queue={definition.queue}")
        matching_queue = next((queue for queue in queue_definitions if queue.queue == definition.queue), None)
        if matching_queue is not None and matching_queue.worker_service != definition.service_name:
            errors.append(
                "worker service queue must reference a queue owned by that worker: "
                f"service_name={definition.service_name} queue={definition.queue} queue_worker_service={matching_queue.worker_service}"
            )
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

    for definition in available_platform_service_definitions:
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

    for definition in app_service_definitions:
        if definition.service_type not in APP_SERVICE_TYPES:
            errors.append(
                f"app service_type is not supported: service_name={definition.service_name} service_type={definition.service_type}"
            )
        if definition.dockerfile is not None:
            dockerfile_path = PROJECT_ROOT / definition.dockerfile
            if not dockerfile_path.exists():
                errors.append(
                    f"app service dockerfile does not exist: service_name={definition.service_name} dockerfile={definition.dockerfile}"
                )
        for env_file in definition.env_files:
            env_path = PROJECT_ROOT / env_file
            if not env_path.exists():
                errors.append(
                    f"app service env file does not exist: service_name={definition.service_name} env_file={env_file}"
                )
        for service_name in definition.depends_on_service_healthy:
            if service_name in active_platform_service_names or service_name in active_service_names:
                continue
            errors.append(
                f"app service dependency is not active; add it to app requires: service_name={definition.service_name} dependency={service_name}"
            )

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
            "available_platform_service_count": len(available_platform_service_definitions),
            "app_service_count": len(app_service_definitions),
            "queue_count": len(queue_definitions),
            "task_queue_count": len(task_queue_names),
            "worker_service_queue_count": len(service_queue_names),
            "compose_in_sync": compose_in_sync,
        },
    }


def _validate_catalog_schema(
    *,
    errors: list[str],
    warnings: list[str],
    task_definitions: tuple[object, ...],
    queue_definitions: tuple[object, ...],
    available_service_definitions: tuple[object, ...],
    available_platform_service_definitions: tuple[object, ...],
    app_service_definitions: tuple[object, ...],
) -> None:
    for definition in queue_definitions:
        queue = getattr(definition, "queue", "")
        queue_name = getattr(definition, "queue_name", "")
        capability = getattr(definition, "capability", "")
        kind = getattr(definition, "kind", "")
        worker_service = getattr(definition, "worker_service", "")
        if not _matches(DOT_ID_PATTERN, queue):
            errors.append(f"queue id must be dot-separated lowercase segments: queue={queue}")
        if not _matches(QUEUE_NAME_PATTERN, queue_name):
            errors.append(f"queue_name must be colon-separated lowercase segments: queue={queue} queue_name={queue_name}")
        if ":" in queue_name and queue_name.replace(":", ".") != queue:
            errors.append(
                f"queue id must match queue_name segments: queue={queue} queue_name={queue_name}"
            )
        if not _is_lower_identifier(capability):
            errors.append(f"queue capability must be a lowercase identifier: queue={queue} capability={capability}")
        if not _is_lower_identifier(kind):
            errors.append(f"queue kind must be a lowercase identifier: queue={queue} kind={kind}")
        if not _matches(SERVICE_NAME_PATTERN, worker_service):
            errors.append(f"queue worker_service must be kebab-case: queue={queue} worker_service={worker_service}")

    for definition in task_definitions:
        task_name = getattr(definition, "task_name", "")
        module_path = getattr(definition, "module_path", "")
        queue = getattr(definition, "queue", "")
        if not _matches(DOT_ID_PATTERN, task_name):
            errors.append(f"task_name must be dot-separated lowercase segments: task_name={task_name}")
        elif len(task_name.split(".")) < 3:
            errors.append(f"task_name must include at least layer.capability.action: task_name={task_name}")
        if not _matches(MODULE_PATH_PATTERN, module_path):
            errors.append(f"task module_path must be a valid Python module path: task_name={task_name} module_path={module_path}")
        if not _matches(DOT_ID_PATTERN, queue):
            errors.append(f"task queue must be a dot-separated queue id: task_name={task_name} queue={queue}")
        for schema_field in ("payload_schema", "output_schema"):
            schema_path = getattr(definition, schema_field, None)
            if schema_path is not None and not _matches(SCHEMA_PATH_PATTERN, schema_path):
                errors.append(
                    f"task {schema_field} must be a valid Python class path: task_name={task_name} {schema_field}={schema_path}"
                )
        for int_field in ("max_retries", "backoff_seconds", "timeout_seconds", "dlq_requeue_limit"):
            value = getattr(definition, int_field, None)
            if value is not None and not isinstance(value, int):
                errors.append(f"task {int_field} must be an integer: task_name={task_name}")
        for bool_field in ("retryable", "dlq_enabled", "dlq_requeue_keep_attempts"):
            value = getattr(definition, bool_field, None)
            if not isinstance(value, bool):
                errors.append(f"task {bool_field} must be boolean: task_name={task_name}")

    for definition in (*available_service_definitions, *available_platform_service_definitions, *app_service_definitions):
        service_name = getattr(definition, "service_name", "")
        service_type = getattr(definition, "service_type", None)
        if not _matches(SERVICE_NAME_PATTERN, service_name):
            errors.append(f"service_name must be kebab-case: service_name={service_name}")
        if service_type is not None and not _is_lower_identifier(service_type):
            errors.append(f"service_type must be a lowercase identifier: service_name={service_name} service_type={service_type}")
        _validate_string_tuple(errors, service_name, "command", getattr(definition, "command", ()))
        _validate_string_tuple(errors, service_name, "env_files", getattr(definition, "env_files", ()))
        _validate_string_tuple(errors, service_name, "ports", getattr(definition, "ports", ()))
        _validate_string_tuple(errors, service_name, "volumes", getattr(definition, "volumes", ()))
        _validate_string_tuple(
            errors,
            service_name,
            "depends_on_service_healthy",
            getattr(definition, "depends_on_service_healthy", ()),
        )
        healthcheck = getattr(definition, "healthcheck", None)
        if healthcheck is not None:
            if not getattr(healthcheck, "test", ()):
                errors.append(f"service healthcheck.test must be non-empty: service_name={service_name}")
            retries = getattr(healthcheck, "retries", None)
            if not isinstance(retries, int) or retries < 1:
                errors.append(f"service healthcheck.retries must be >= 1: service_name={service_name}")


def _validate_string_tuple(errors: list[str], service_name: str, field_name: str, values: object) -> None:
    if not isinstance(values, tuple) or not all(isinstance(value, str) for value in values):
        errors.append(f"service {field_name} must be a list of strings: service_name={service_name}")


def _matches(pattern: re.Pattern[str], value: object) -> bool:
    return isinstance(value, str) and bool(pattern.fullmatch(value))


def _is_lower_identifier(value: object) -> bool:
    return isinstance(value, str) and bool(re.fullmatch(r"[a-z][a-z0-9_]*", value))


def _module_file_path(module_path: str) -> Path:
    return PROJECT_ROOT / (module_path.replace(".", "/") + ".py")


def _app_name_from_manifest_path(path: Path) -> str:
    app_dir = path.parent.parent
    return ".".join(app_dir.relative_to(APPS_DIR).parts)


def _app_from_task_module(module_path: str) -> str | None:
    parts = module_path.split(".")
    if len(parts) < 4 or parts[0] != "apps" or "tasks" not in parts[1:]:
        return None
    tasks_index = parts.index("tasks", 1)
    if tasks_index == 1:
        return None
    app_parts = parts[1:tasks_index]
    if app_parts and app_parts[-1] == "app":
        app_parts = app_parts[:-1]
    return ".".join(app_parts)


def _runtime_task_namespace(module_path: str) -> str | None:
    parts = module_path.split(".")
    if len(parts) < 3:
        return None
    return {
        "chunk": "chunk",
        "ingest_api": "ingest",
        "ingest_file": "ingest",
        "extract": "extract",
        "llm": "llm",
        "parser": "parse",
        "index_dense": "index",
        "index_sparse": "index",
    }.get(parts[1])


def _runtime_task_package(module_path: str) -> str | None:
    parts = module_path.split(".")
    if len(parts) < 5 or parts[0] != "services" or parts[2] != "app" or parts[3] != "tasks":
        return None
    if parts[1] in {
        "chunk",
        "ingest_api",
        "ingest_file",
        "extract",
        "llm",
        "parser",
        "index_dense",
        "index_sparse",
    }:
        return ".".join(parts[:4])
    return None


def _validate_task_schema_path(
    *,
    errors: list[str],
    task_name: str,
    schema_field: str,
    schema_path: str,
    task_app: str | None,
    runtime_task_package: str | None,
) -> None:
    schema_module_path, _, schema_name = schema_path.rpartition(".")
    if task_app is not None and not (
        schema_module_path.startswith(f"apps.{task_app}.tasks.")
        or schema_module_path.startswith(f"apps.{task_app}.contracts.")
        or schema_module_path.startswith(f"apps.{task_app}.app.tasks.")
        or schema_module_path.startswith(f"apps.{task_app}.app.contracts.")
    ):
        errors.append(
            f"task {schema_field} must be inside the same app tasks/contracts package: "
            f"task_name={task_name} {schema_field}={schema_path}"
        )
    if runtime_task_package is not None and not _is_runtime_task_schema_module(
        schema_module_path=schema_module_path,
        runtime_task_package=runtime_task_package,
    ):
        errors.append(
            f"runtime task {schema_field} must be inside the same runtime task/contracts package: "
            f"task_name={task_name} {schema_field}={schema_path}"
        )
    schema_module_file = _module_file_path(schema_module_path)
    if not schema_module_file.exists():
        errors.append(
            f"task schema module does not exist: task_name={task_name} {schema_field}={schema_path}"
        )
    elif schema_name and not _module_has_ast_name(
        schema_module_file,
        schema_name,
        allowed_node_types=(ast.ClassDef,),
    ):
        errors.append(
            f"task schema class does not exist: task_name={task_name} {schema_field}={schema_path}"
        )
    elif not schema_name:
        errors.append(
            f"task schema path is invalid: task_name={task_name} {schema_field}={schema_path}"
        )


def _is_runtime_task_schema_module(*, schema_module_path: str, runtime_task_package: str) -> bool:
    if schema_module_path.startswith(f"{runtime_task_package}."):
        return True
    parts = runtime_task_package.split(".")
    if len(parts) < 4:
        return False
    contracts_package = ".".join([*parts[:3], "contracts"])
    return schema_module_path.startswith(f"{contracts_package}.")


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


def _duplicated_strings(values: list[str]) -> list[str]:
    seen: set[str] = set()
    duplicated: list[str] = []
    for value in values:
        if value in seen and value not in duplicated:
            duplicated.append(value)
        seen.add(value)
    return duplicated
