from __future__ import annotations

import json

from services.semantic_platform.storage.repository import SemanticCatalogRepository
from scripts.ops.common import run_command


def reset_semantic_catalog() -> dict:
    repo = SemanticCatalogRepository()
    try:
        return repo.reset_catalog()
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
                "from services.semantic_platform.storage.repository import SemanticCatalogRepository; "
                "import json; "
                "print(json.dumps(SemanticCatalogRepository().reset_catalog(), ensure_ascii=False))"
            ),
        )
        return json.loads(output)
