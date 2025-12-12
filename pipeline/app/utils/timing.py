from datetime import datetime
from zoneinfo import ZoneInfo
#
#
KST = ZoneInfo("Asia/Seoul")

def parse_kst_datetime(value: str | None):
    """
    '2020-01-30 16:19:26' → KST aware datetime
    """
    if not value:
        return None

    dt = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    return dt.replace(tzinfo=KST)

