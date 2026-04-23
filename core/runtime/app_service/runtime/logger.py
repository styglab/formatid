import json
import logging
import os
import sys
from datetime import datetime

from core.observability.log_store import record_service_log_best_effort
from core.runtime.time import now

_SERVICE_NAME = os.getenv("APP_NAME") or os.getenv("SERVICE_NAME") or "app-service"
_DATABASE_URL: str | None = None


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.fromtimestamp(record.created, tz=now().tzinfo).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        extra_fields = getattr(record, "extra_fields", None)
        if isinstance(extra_fields, dict):
            payload.update(extra_fields)
        return json.dumps(payload, ensure_ascii=True)


def configure_logging(*, level: str, service_name: str | None = None, database_url: str | None = None) -> None:
    global _DATABASE_URL, _SERVICE_NAME
    if service_name:
        _SERVICE_NAME = service_name
    _DATABASE_URL = database_url
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(level.upper())


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


def log_event(logger: logging.Logger, level: int, event: str, **fields: object) -> None:
    service_name = str(fields.get("service_name") or _SERVICE_NAME)
    fields["service_name"] = service_name
    message = str(fields.pop("message", event))
    record_service_log_best_effort(
        service_name=service_name,
        level=logging.getLevelName(level).lower(),
        event_name=event,
        message=message,
        database_url=_DATABASE_URL,
        logger_name=logger.name,
        request_id=_string_or_none(fields.get("request_id")),
        run_name=_string_or_none(fields.get("run_name")),
        task_id=_string_or_none(fields.get("task_id")),
        correlation_id=_string_or_none(fields.get("correlation_id")),
        resource_key=_string_or_none(fields.get("resource_key")),
        details=dict(fields),
    )
    logger.log(level, event, extra={"extra_fields": {"event": event, **fields}})


def _string_or_none(value: object) -> str | None:
    return None if value is None else str(value)
