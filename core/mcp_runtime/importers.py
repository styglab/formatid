from __future__ import annotations

import importlib
from typing import Any, Callable


def import_string(path: str) -> Callable[..., Any]:
    module_path, _, attr_name = path.rpartition(".")
    if not module_path or not attr_name:
        raise ValueError(f"Invalid import path: {path}")
    module = importlib.import_module(module_path)
    value = getattr(module, attr_name)
    if not callable(value):
        raise TypeError(f"Imported value is not callable: {path}")
    return value
