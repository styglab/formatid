
SPEC_URL_KEYS = [f"ntceSpecDocUrl{i}" for i in range(1, 11)]

def has_spec_doc_url(item: dict) -> bool:
    for i in range(1, 11):
        url = item.get(f"ntceSpecDocUrl{i}", "")
        if url:  # '' 이 아니면 True
            return True
    return False

def is_valid_notice(item: dict) -> bool:
    """
    수집 대상 공고 필터링
    """
    # 1. 취소공고 제외
    if item.get("ntceKindNm") == "취소공고":
        return False

    # 2. 규격서 URL 필수
    if not has_spec_doc_url(item):
        return False

    return True

