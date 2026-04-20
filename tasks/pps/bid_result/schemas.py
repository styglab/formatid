from pydantic import BaseModel, ConfigDict, Field


class CollectBidResultPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    bidNtceNo: str = Field(min_length=1)
    bidNtceOrd: str = Field(min_length=1)
