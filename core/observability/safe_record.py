from __future__ import annotations

import logging
from typing import Awaitable, Callable


async def safe_record(
    operation: Awaitable[None],
    *,
    logger,
    log_event: Callable,
    event: str,
    **fields,
) -> None:
    try:
        await operation
    except Exception as exc:
        log_event(
            logger,
            logging.WARNING,
            event,
            **fields,
            error_type=type(exc).__name__,
            error_message=str(exc),
        )
