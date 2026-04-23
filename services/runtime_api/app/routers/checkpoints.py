from fastapi import APIRouter, HTTPException

from services.runtime_api.app.schemas.checkpoints import CheckpointEntry, CheckpointListResponse
from services.runtime_api.app.services.checkpoint_service import get_checkpoint, list_checkpoints


router = APIRouter(prefix="/checkpoints", tags=["checkpoints"])


@router.get("", response_model=CheckpointListResponse)
async def get_checkpoints() -> CheckpointListResponse:
    checkpoints = await list_checkpoints()
    return CheckpointListResponse(checkpoints=[CheckpointEntry.model_validate(item) for item in checkpoints])


@router.get("/{name}", response_model=CheckpointEntry)
async def get_checkpoint_by_name(name: str) -> CheckpointEntry:
    checkpoint = await get_checkpoint(name)
    if checkpoint is None:
        raise HTTPException(status_code=404, detail={"message": f"checkpoint not found: {name}"})
    return CheckpointEntry.model_validate(checkpoint)
