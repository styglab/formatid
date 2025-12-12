import aiohttp
#
#
BASE_URL = "https://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoThng"

async def fetch_bid_notices(
    *,
    service_key: str,
    inqry_bgn_dt: str,
    inqry_end_dt: str,
    inqry_div: int,
    page_no: int,
    num_of_rows: int,
) -> list[dict]:
    params = {
        "serviceKey": service_key,
        "inqryDiv": inqry_div,
        "inqryBgnDt": inqry_bgn_dt,
        "inqryEndDt": inqry_end_dt,
        "pageNo": page_no,
        "numOfRows": num_of_rows,
        "type": "json",
    }

    async with aiohttp.ClientSession() as session:
        async with session.get(BASE_URL, params=params, timeout=30) as resp:
            resp.raise_for_status()
            data = await resp.json()

    response = data.get("response", {})
    header = response.get("header", {})
    body = response.get("body", {})

    # 결과 코드 체크 (중요)
    if header.get("resultCode") != "00":
        raise RuntimeError(f"data.go.kr API error: {header}")

    items = body.get("items", [])

    # items가 None이거나 비어 있으면 빈 리스트
    if not items:
        return []

    # 혹시 단건 dict로 오는 경우 방어 (API 일관성 문제)
    if isinstance(items, dict):
        return [items]

    # 정상 케이스: list
    return items


