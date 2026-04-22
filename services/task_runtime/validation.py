from importlib import import_module
from typing import Any

from pydantic import BaseModel, ValidationError

from services.task_runtime.catalog import get_task_definition
from shared.tasking.errors import InvalidTaskPayloadError


def validate_task_payload(*, task_name: str, payload: dict[str, Any]) -> dict[str, Any]:
    definition = get_task_definition(task_name)
    if definition.payload_schema is None:
        return payload

    schema_type = _load_payload_schema(definition.payload_schema)
    try:
        validated = schema_type.model_validate(payload)
    except ValidationError as exc:
        raise InvalidTaskPayloadError(
            f"invalid payload for task_name={task_name}: {exc}"
        ) from exc
    return validated.model_dump(mode="python")


def _load_payload_schema(schema_path: str) -> type[BaseModel]:
    module_path, _, attr_name = schema_path.rpartition(".")
    if not module_path or not attr_name:
        raise RuntimeError(f"invalid payload_schema path: {schema_path}")

    module = import_module(module_path)
    schema_type = getattr(module, attr_name)
    if not isinstance(schema_type, type) or not issubclass(schema_type, BaseModel):
        raise RuntimeError(f"payload_schema is not a Pydantic model: {schema_path}")
    return schema_type
