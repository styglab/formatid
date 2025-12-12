from datetime import datetime
from zoneinfo import ZoneInfo
#
from app.utils.timing import parse_kst_datetime
#
#
KST = ZoneInfo("Asia/Seoul")

def to_bid_notice_row(notice: dict) -> dict:
    now = datetime.now(tz=KST)

    return {
        "bid_ntce_no": notice["bidNtceNo"],
        "bid_ntce_ord": notice["bidNtceOrd"],
        "bid_ntce_ord_num": int(notice["bidNtceOrd"]),
        "bid_ntce_nm": notice.get("bidNtceNm"),
        
        "ntce_kind_nm": notice.get("ntceKindNm"),
        "bid_ntce_dt": parse_kst_datetime(notice.get("chgDt")),
        
        "ntce_instt_cd": notice.get("ntceInsttCd"),
        "ntce_instt_nm": notice.get("ntceInsttNm"),
        "dminstt_cd": notice.get("dminsttCd"),
        "dminstt_nm": notice.get("dminsttNm"),
        
        "dtil_prdct_clsfc_no": notice.get("dtilPrdctClsfcNo"),
        "dtil_prdct_clsfc_no_nm": notice.get("dtilPrdctClsfcNoNm"),
        "prdct_spec_nm": notice.get("prdctSpecNm"),
        "prdct_qty": notice.get("prdctQty"),
        "prdct_unit": notice.get("prdctUnit"),
        "prdct_uprc": notice.get("prdctUprc"),
        
        "bid_begin_dt": parse_kst_datetime(notice.get("bidBeginDt")),
        "bid_clse_dt": parse_kst_datetime(notice.get("bidClseDt")),
        "openg_dt": parse_kst_datetime(notice.get("opengDt")),

        "asign_bdgt_amt": notice.get("asignBdgtAmt"),
        "presmpt_prce": notice.get("presmptPrce"),
        
        "raw_json": notice,
        "is_latest": False,
        "created_at": now,
        "updated_at": now,        
    }

