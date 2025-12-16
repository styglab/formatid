from datetime import datetime
from zoneinfo import ZoneInfo
#
from app.utils.timing import parse_kst_datetime
from app.utils.type_parser import safe_int, parse_amount
#
#
KST = ZoneInfo("Asia/Seoul")

def to_bid_notice_row(notice: dict) -> dict:
    bid_ntce_no = notice.get("bidNtceNo")
    if not bid_ntce_no:
        return None
    
    bdgt_amt = (
        notice.get("asignBdgtAmt")
        if notice.get("asignBdgtAmt") is not None
        else notice.get("BdgtAmt")
    )
    bdgt_amt = parse_amount(bdgt_amt)
    presmpt_prce = parse_amount(notice.get("presmpt_prce"))
    bid_ntce_ord_num = safe_int(notice.get("bidNtceOrd"))

    now = datetime.now(tz=KST)

    return {
        "bid_ntce_no": notice["bidNtceNo"],
        "bid_ntce_ord": notice["bidNtceOrd"],
        "bid_ntce_ord_num": bid_ntce_ord_num,
        
        "bid_type": notice.get("bid_type"),
        "bid_ntce_nm": notice.get("bidNtceNm"),
        "ntce_kind_nm": notice.get("ntceKindNm"),
        "bid_ntce_dt": parse_kst_datetime(notice.get("chgDt")),
        
        "ntce_instt_cd": notice.get("ntceInsttCd"),
        "ntce_instt_nm": notice.get("ntceInsttNm"),
        "dminstt_cd": notice.get("dminsttCd"),
        "dminstt_nm": notice.get("dminsttNm"),
        
        "dtil_prdct_clsfc_no": notice.get("dtilPrdctClsfcNo"),
        "dtil_prdct_clsfc_no_nm": notice.get("dtilPrdctClsfcNoNm"),
        
        "bid_begin_dt": parse_kst_datetime(notice.get("bidBeginDt")),
        "bid_clse_dt": parse_kst_datetime(notice.get("bidClseDt")),
        "openg_dt": parse_kst_datetime(notice.get("opengDt")),

        "bdgt_amt": bdgt_amt,
        "presmpt_prce": presmpt_prce,
        
        "raw_json": notice,
        "is_latest": False,
        "created_at": now,
        "updated_at": now,        
    }

