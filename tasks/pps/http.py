from __future__ import annotations

from typing import Any

import httpx

from tasks.pps.config import get_settings


class PpsApiError(RuntimeError):
    pass


class PpsDailyQuotaExceededError(PpsApiError):
    def __init__(
        self,
        *,
        message: str = "daily PPS API quota exceeded",
        result_code: Any = None,
        result_msg: Any = None,
    ) -> None:
        super().__init__(message)
        self.result_code = result_code
        self.result_msg = result_msg

    def to_error_detail(self) -> dict[str, Any]:
        detail: dict[str, Any] = {
            "type": type(self).__name__,
            "message": str(self),
            "reason": "daily_quota_exceeded",
        }
        if self.result_code is not None:
            detail["result_code"] = self.result_code
        if self.result_msg is not None:
            detail["result_msg"] = self.result_msg
        return detail


class PpsApiClient:
    def __init__(self) -> None:
        settings = get_settings()
        if not settings.public_api_key:
            raise PpsApiError("PUBLIC_API_KEY is not configured")
        self._api_key = settings.public_api_key
        self._timeout = settings.api_timeout_seconds

    async def fetch_bid_list(
        self,
        *,
        inqry_bgn_dt: str,
        inqry_end_dt: str,
        page_no: int,
        num_of_rows: int,
    ) -> dict[str, Any]:
        return await self._request(
            url="https://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoThngPPSSrch",
            params={
                "serviceKey": self._api_key,
                "type": "json",
                "inqryDiv": 1,
                "inqryBgnDt": inqry_bgn_dt,
                "inqryEndDt": inqry_end_dt,
                "pageNo": page_no,
                "numOfRows": num_of_rows,
            },
        )

    async def fetch_bid_result_participants(
        self,
        *,
        bid_ntce_no: str,
        page_no: int,
        num_of_rows: int,
    ) -> dict[str, Any]:
        return await self._request(
            url="https://apis.data.go.kr/1230000/as/ScsbidInfoService/getOpengResultListInfoOpengCompt",
            params={
                "serviceKey": self._api_key,
                "type": "json",
                "inqryDiv": 4,
                "bidNtceNo": bid_ntce_no,
                "pageNo": page_no,
                "numOfRows": num_of_rows,
            },
        )

    async def fetch_bid_result_winners(
        self,
        *,
        bid_ntce_no: str,
        page_no: int,
        num_of_rows: int,
    ) -> dict[str, Any]:
        return await self._request(
            url="https://apis.data.go.kr/1230000/as/ScsbidInfoService/getScsbidListSttusThng",
            params={
                "serviceKey": self._api_key,
                "type": "json",
                "inqryDiv": 4,
                "bidNtceNo": bid_ntce_no,
                "pageNo": page_no,
                "numOfRows": num_of_rows,
            },
        )

    async def download_file(self, *, url: str) -> bytes:
        async with httpx.AsyncClient(timeout=self._timeout, follow_redirects=True) as client:
            response = await client.get(url)
            response.raise_for_status()
            return response.content

    async def _request(self, *, url: str, params: dict[str, Any]) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=self._timeout, follow_redirects=True) as client:
            response = await client.get(url, params=params)
            if response.status_code == 429:
                raise PpsDailyQuotaExceededError(
                    message="pps api request failed: HTTP 429 daily quota exceeded",
                    result_code=response.status_code,
                    result_msg=response.text[:500],
                )
            response.raise_for_status()
            payload = response.json()

        header = payload.get("response", {}).get("header", {})
        result_code = header.get("resultCode")
        result_msg = header.get("resultMsg")
        if _is_daily_quota_exceeded(result_code=result_code, result_msg=result_msg):
            raise PpsDailyQuotaExceededError(
                message=f"pps api daily quota exceeded: resultCode={result_code} resultMsg={result_msg}",
                result_code=result_code,
                result_msg=result_msg,
            )
        if result_code != "00":
            raise PpsApiError(
                f"pps api request failed: resultCode={result_code} resultMsg={result_msg}"
            )
        return payload


def _is_daily_quota_exceeded(*, result_code: Any, result_msg: Any) -> bool:
    code = str(result_code or "").strip().upper()
    if code in {"22", "LIMITED_NUMBER_OF_SERVICE_REQUESTS_EXCEEDS_ERROR"}:
        return True
    return False


def extract_items(payload: dict[str, Any]) -> list[dict[str, Any]]:
    body = payload.get("response", {}).get("body", {})
    items = body.get("items", [])
    if isinstance(items, dict):
        maybe_item = items.get("item")
        if isinstance(maybe_item, list):
            return [item for item in maybe_item if isinstance(item, dict)]
        if isinstance(maybe_item, dict):
            return [maybe_item]
    if isinstance(items, list):
        return [item for item in items if isinstance(item, dict)]
    if isinstance(items, dict):
        return [items]
    return []


def extract_total_count(payload: dict[str, Any]) -> int:
    body = payload.get("response", {}).get("body", {})
    total_count = body.get("totalCount", 0)
    try:
        return int(total_count)
    except (TypeError, ValueError):
        return 0
