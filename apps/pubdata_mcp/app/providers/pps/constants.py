from __future__ import annotations


BID_ENDPOINTS = {
    "GOODS": "http://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoThngPPSSrch",
    "SERVICE": "http://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoServcPPSSrch",
    "CONSTRUCTION": "http://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoCnstwkPPSSrch",
    "FOREIGN": "http://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoFrgcptPPSSrch",
}

CONTRACT_ENDPOINTS = {
    "GOODS": "http://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListThngPPSSrch",
    "SERVICE": "http://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListServcPPSSrch",
    "CONSTRUCTION": "http://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListCnstwkPPSSrch",
    "FOREIGN": "http://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListFrgcptPPSSrch",
}

CATEGORY_ALIASES = {
    "물품": "GOODS",
    "goods": "GOODS",
    "GOODS": "GOODS",
    "용역": "SERVICE",
    "service": "SERVICE",
    "SERVICE": "SERVICE",
    "공사": "CONSTRUCTION",
    "construction": "CONSTRUCTION",
    "CONSTRUCTION": "CONSTRUCTION",
    "외자": "FOREIGN",
    "foreign": "FOREIGN",
    "FOREIGN": "FOREIGN",
}
