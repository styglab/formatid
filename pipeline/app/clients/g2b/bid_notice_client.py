import aiohttp
import math
#
from app.core.external.g2b.endpoints import BidEndpoint, build_params


async def fetch_bid_page(
    *,
    session: aiohttp.ClientSession,
    endpoint: BidEndpoint,
    service_key: str,
    inqry_bgn_dt: str,
    inqry_end_dt: str,
    inqry_div: int,
    page_no: int,
    num_of_rows: int,
    bid_ntce_nm: str, # 옵션 파라미터인 경우, bid_ntce_nm: str | None = None
    **query_params,
) -> tuple[list[dict], dict]:
    
    url = endpoint.url()
    
    params = build_params(
        endpoint=endpoint,
        serviceKey=service_key,
        pageNo=page_no,
        numOfRows=num_of_rows,
        inqryDiv=inqry_div,
        inqryBgnDt=inqry_bgn_dt,
        inqryEndDt=inqry_end_dt,
        bidNtceNm=bid_ntce_nm,
        type="json",
        **query_params,
    )

    async with session.get(endpoint.url(), params=params, timeout=30) as resp:
        resp.raise_for_status()
        data = await resp.json()

    response = data.get("response", {})
    header = response.get("header", {})
    body = response.get("body", {})

    # 결과 코드 체크 (중요)
    if header.get("resultCode") != "00":
        raise RuntimeError(f"data.go.kr API error: {header}")

    items = body.get("items") or []

    # 혹시 단건 dict로 오는 경우 방어 (API 일관성 문제)
    if isinstance(items, dict):
        items = [items]

    # 정상 케이스: list
    return items, body

async def fetch_all_bid_pages(
    *,
    endpoint: BidEndpoint,
    service_key: str,
    inqry_bgn_dt: str,
    inqry_end_dt: str,
    inqry_div: int,
    num_of_rows: int,
    bid_ntce_nm: str, # 옵션 파라미터인 경우, bid_ntce_nm: str | None = None
    **query_params,
) -> list[dict]:

    async with aiohttp.ClientSession() as session:
        # 1) 첫 페이지
        first_items, body = await fetch_bid_page(
            session=session,
            endpoint=endpoint,
            service_key=service_key,
            inqry_bgn_dt=inqry_bgn_dt,
            inqry_end_dt=inqry_end_dt,
            inqry_div=inqry_div,
            page_no=1,
            num_of_rows=num_of_rows,
            bid_ntce_nm=bid_ntce_nm,
            **query_params,
        )

        total_count = body.get("totalCount", 0)
        if total_count == 0:
            return []

        total_pages = math.ceil(total_count / num_of_rows)
        all_items = list(first_items)

        # 2) 나머지 페이지
        for page_no in range(2, total_pages + 1):
            items, _ = await fetch_bid_page(
                session=session,
                endpoint=endpoint,
                service_key=service_key,
                inqry_bgn_dt=inqry_bgn_dt,
                inqry_end_dt=inqry_end_dt,
                inqry_div=inqry_div,
                page_no=page_no,
                num_of_rows=num_of_rows,
                bid_ntce_nm=bid_ntce_nm,
                **query_params,
            )
            all_items.extend(items)

        return all_items


