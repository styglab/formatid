from fastapi import APIRouter

from services.semantic_platform.adapters.admin_api.app.catalog_governance import (
    router as governance_router,
)
from services.semantic_platform.adapters.admin_api.app.catalog_mappings import (
    router as mappings_router,
)
from services.semantic_platform.adapters.admin_api.app.catalog_semantic import (
    router as semantic_router,
)


router = APIRouter()
router.include_router(semantic_router)
router.include_router(mappings_router)
router.include_router(governance_router)
