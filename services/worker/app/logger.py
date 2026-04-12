import logging
import json
from datetime import datetime
from pathlib import Path
from typing import Any

from services.worker.app.config import get_settings
from shared.time import get_timezone, now


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=get_timezone()).isoformat(),
            "level": record.levelname.lower(),
            "logger": record.name,
            "message": record.getMessage(),
        }

        event = getattr(record, "event", None)
        if event is not None:
            payload["event"] = event

        for key, value in record.__dict__.items():
            if key.startswith("_") or key in _RESERVED_LOG_RECORD_FIELDS:
                continue
            payload[key] = value

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(payload, ensure_ascii=True, default=str)


class DatePartitionedFileHandler(logging.Handler):
    def __init__(self, *, base_dir: str, file_stem: str) -> None:
        super().__init__()
        self.base_dir = Path(base_dir)
        self.file_stem = file_stem
        self._current_path: Path | None = None
        self._stream = None

    def emit(self, record: logging.LogRecord) -> None:
        try:
            target_path = self._build_target_path()
            if target_path != self._current_path:
                self._reopen(target_path)
            if self._stream is None:
                return
            self._stream.write(self.format(record) + "\n")
            self._stream.flush()
        except Exception:
            self.handleError(record)

    def close(self) -> None:
        if self._stream is not None:
            self._stream.close()
            self._stream = None
        self._current_path = None
        super().close()

    def _build_target_path(self) -> Path:
        date_dir = now().strftime("%Y-%m-%d")
        return self.base_dir / date_dir / f"{self.file_stem}.log"

    def _reopen(self, target_path: Path) -> None:
        if self._stream is not None:
            self._stream.close()
        target_path.parent.mkdir(parents=True, exist_ok=True)
        self._stream = target_path.open("a", encoding="utf-8")
        self._current_path = target_path


def configure_logging() -> None:
    settings = get_settings()

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    formatter = JsonFormatter()

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    root_logger.addHandler(stream_handler)

    if settings.worker_log_to_file:
        file_handler = DatePartitionedFileHandler(
            base_dir=settings.worker_log_dir,
            file_stem=_build_log_file_stem(
                app_name=settings.app_name,
                queue_name=settings.worker_queue_name,
            ),
        )
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

    root_logger.setLevel(getattr(logging, settings.log_level.upper(), logging.INFO))
    root_logger.propagate = False


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


def log_event(logger: logging.Logger, level: int, event: str, **fields: Any) -> None:
    message = fields.pop("message", event)
    logger.log(level, message, extra={"event": event, **fields})


def _build_log_file_stem(*, app_name: str, queue_name: str) -> str:
    safe_queue_name = queue_name.replace(":", "-").replace("/", "-")
    return f"{app_name}.{safe_queue_name}"


_RESERVED_LOG_RECORD_FIELDS = {
    "name",
    "msg",
    "args",
    "levelname",
    "levelno",
    "pathname",
    "filename",
    "module",
    "exc_info",
    "exc_text",
    "stack_info",
    "lineno",
    "funcName",
    "created",
    "msecs",
    "relativeCreated",
    "thread",
    "threadName",
    "processName",
    "process",
    "message",
    "asctime",
}
