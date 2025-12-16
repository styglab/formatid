from dataclasses import dataclass
#
#
BASE_URL = "https://apis.data.go.kr/1230000/ad/BidPublicInfoService"


@dataclass(frozen=True)
class BidEndpoint:
    name: str
    path: str
    allowed_params: set[str]
    required_params: set[str] = frozenset()

    def url(self) -> str:
        return f"https://apis.data.go.kr/1230000/ad/BidPublicInfoService/{self.path}"

BID_THNG_PPS = BidEndpoint(
    name="thng",
    path="getBidPblancListInfoThngPPSSrch",
    allowed_params={
        "serviceKey",
        "pageNo",
        "numOfRows",
        "inqryDiv",
        "inqryBgnDt",
        "inqryEndDt",
        "bidNtceNm",
        "type",
    },
)

BID_CNST_PPS = BidEndpoint(
    name="cnst_pps",
    path="getBidPblancListInfoCnstwkPPSSrch",
    allowed_params={
        "serviceKey",
        "pageNo",
        "numOfRows",
        "inqryDiv",
        "inqryBgnDt",
        "inqryEndDt",
        "bidNtceNm",
        "type",
    },
)

def build_params(endpoint: BidEndpoint, **kwargs) -> dict:
    params = {
        k: v
        for k, v in kwargs.items()
        if k in endpoint.allowed_params and v is not None
    }

    missing = endpoint.required_params - params.keys()
    if missing:
        raise ValueError(f"{endpoint.name} missing params: {missing}")

    return params


