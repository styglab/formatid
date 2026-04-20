def build_bid_list_window_job_key(*, inqry_bgn_dt: str, inqry_end_dt: str) -> str:
    return f"{inqry_bgn_dt}:{inqry_end_dt}"


def build_bid_list_page_job_key(*, inqry_bgn_dt: str, inqry_end_dt: str, page_no: int) -> str:
    return f"{inqry_bgn_dt}:{inqry_end_dt}:{page_no}"


def build_bid_notice_job_key(*, bid_ntce_no: str, bid_ntce_ord: str) -> str:
    return f"{bid_ntce_no}:{bid_ntce_ord}"


def build_bid_number_job_key(*, bid_ntce_no: str) -> str:
    return bid_ntce_no
