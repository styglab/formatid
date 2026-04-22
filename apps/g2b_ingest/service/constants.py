from __future__ import annotations

G2B_INGEST_BID_LIST_URL = (
    "https://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoThng" + "PP" + "SSrch"
)
G2B_INGEST_PARTICIPANTS_URL = "https://apis.data.go.kr/1230000/as/ScsbidInfoService/getOpengResultListInfoOpengCompt"
G2B_INGEST_WINNERS_URL = "https://apis.data.go.kr/1230000/as/ScsbidInfoService/getScsbidListSttusThng"

GENERIC_API_QUEUE = "ingest:api"
GENERIC_FILE_QUEUE = "ingest:file"
GENERIC_API_TABLE = "g2b_ingest_generic_api_ingest"
GENERIC_FILE_TABLE = "g2b_ingest_generic_file_ingest"
