from typing import List, Dict
from datetime import datetime
from zoneinfo import ZoneInfo
#
#
KST = ZoneInfo("Asia/Seoul")

def parse_attachments(notice: Dict) -> List[Dict]:
    """
    공고 raw_json 에서 첨부파일 목록을 파싱한다.
    """
    attachments = []

    bid_ntce_no = notice["bidNtceNo"]
    bid_ntce_ord = notice["bidNtceOrd"]
    
    now = datetime.now(tz=KST)

    for i in range(1, 11):
        url_key = f"ntceSpecDocUrl{i}"
        name_key = f"ntceSpecFileNm{i}"

        download_url = notice.get(url_key)
        file_name = notice.get(name_key)

        # URL 없으면 스킵
        if not download_url:
            continue

        attachments.append({
            "bid_ntce_no": bid_ntce_no,
            "bid_ntce_ord": bid_ntce_ord,
            "file_seq": i,
            "file_name": file_name or None,
            "download_url": download_url,
            "status": "pending",
            "created_at": now,
            "updated_at": now,
        })

    return attachments
