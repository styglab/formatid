from __future__ import annotations

import json

from scripts.ops.common import run_command
from services.semantic_platform.internal.storage.repository import SemanticLayerRepository


def reset_semantic_platform() -> dict:
    repo = SemanticLayerRepository()
    try:
        return repo.reset_context()
    except ModuleNotFoundError as exc:
        if exc.name != "psycopg":
            raise
        output = run_command(
            "docker",
            "exec",
            "infra-semantic-platform-api-1",
            "python",
            "-c",
            (
                "import json; "
                "from services.semantic_platform.internal.storage.repository import SemanticLayerRepository; "
                "print(json.dumps(SemanticLayerRepository().reset_context(), ensure_ascii=False))"
            ),
        )
        return json.loads(output)


def seed_semantic_registry() -> dict:
    repo = SemanticLayerRepository()
    try:
        return repo.seed_semantic_type_registry()
    except ModuleNotFoundError as exc:
        if exc.name != "psycopg":
            raise
        output = run_command(
            "docker",
            "exec",
            "infra-semantic-platform-api-1",
            "python",
            "-c",
            (
                "import json; "
                "from services.semantic_platform.internal.storage.repository import SemanticLayerRepository; "
                "print(json.dumps(SemanticLayerRepository().seed_semantic_type_registry(), ensure_ascii=False))"
            ),
        )
        return json.loads(output)
