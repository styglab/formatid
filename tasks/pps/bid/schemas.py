from pydantic import BaseModel, ConfigDict, Field


class CollectBidListPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    inqryBgnDt: str = Field(pattern=r"^\d{12}$")
    inqryEndDt: str = Field(pattern=r"^\d{12}$")
    pageNo: int = Field(default=1, ge=1)


class DownloadBidAttachmentPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    bidNtceNo: str = Field(min_length=1)
    bidNtceOrd: str = Field(min_length=1)


class EnqueueBidDownstreamPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    limit: int = Field(default=100, ge=1, le=1000)
    max_failed_retries: int = Field(default=3, ge=0, le=100)
    retry_failed_after_seconds: int = Field(default=86400, ge=0)
