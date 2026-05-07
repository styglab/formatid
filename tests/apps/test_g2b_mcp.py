from __future__ import annotations

import unittest
from datetime import datetime
from importlib.util import find_spec
from unittest.mock import patch

from apps.g2b_mcp.app.adapters import g2b
from apps.g2b_mcp.app.schemas.normalize import normalize_bid


class FixedDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        return cls(2026, 5, 5, 14, 30, tzinfo=tz)


class FakeResponse:
    def __init__(self, payload: dict) -> None:
        self.payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self.payload


class G2BMCPTests(unittest.TestCase):
    def test_fetch_bids_uses_published_from_and_walks_all_pages(self) -> None:
        calls: list[dict] = []
        payloads = [
            {
                "response": {
                    "body": {
                        "totalCount": "3",
                        "items": [{"bidNtceNo": "1"}, {"bidNtceNo": "2"}],
                    }
                }
            },
            {
                "response": {
                    "body": {
                        "totalCount": "3",
                        "items": [{"bidNtceNo": "3"}],
                    }
                }
            },
        ]

        def fake_get(url: str, *, params: dict, timeout: int) -> FakeResponse:
            calls.append(params)
            return FakeResponse(payloads[len(calls) - 1])

        with (
            patch.object(g2b, "API_KEY", "test-key"),
            patch.object(g2b, "datetime", FixedDateTime),
            patch.dict("os.environ", {"G2B_NUM_OF_ROWS": "2"}),
            patch.object(g2b.requests, "get", side_effect=fake_get),
        ):
            items = g2b.fetch_bids_by_category("SERVICE", "2026-05-01")

        self.assertEqual(items, [{"bidNtceNo": "1"}, {"bidNtceNo": "2"}, {"bidNtceNo": "3"}])
        self.assertEqual([call["pageNo"] for call in calls], [1, 2])
        self.assertEqual(calls[0]["numOfRows"], 2)
        self.assertEqual(calls[0]["inqryBgnDt"], "202605010000")
        self.assertEqual(calls[0]["inqryEndDt"], "202605051430")

    def test_normalize_bid_accepts_g2b_hyphenated_datetime(self) -> None:
        with patch("apps.g2b_mcp.app.schemas.normalize.datetime", FixedDateTime):
            bid = normalize_bid(
                {
                    "bidNtceNo": "1",
                    "bidNtceNm": "First bid",
                    "bidNtceDt": "2026-05-01 07:08:09",
                    "bidClseDt": "2026-05-06 14:00:00",
                }
            )

        self.assertEqual(bid["published_at"], "2026-05-01T07:08:09")
        self.assertEqual(bid["deadline"], "2026-05-06T14:00:00")
        self.assertEqual(bid["deadline_days"], 0)

    @unittest.skipUnless(find_spec("fastmcp"), "fastmcp is not installed")
    def test_search_bid_returns_first_10_normalized_bids(self) -> None:
        from apps.g2b_mcp.app.main import search_bid

        raw_items = [
            {
                "bidNtceNo": str(index),
                "bidNtceNm": f"Bid {index}",
                "bidNtceDt": "202605011200",
                "bidClseDt": "202605101200",
            }
            for index in range(12)
        ]

        with (
            patch("apps.g2b_mcp.app.main.fetch_bids_by_category", return_value=raw_items) as fetch,
            patch("apps.g2b_mcp.app.schemas.normalize.datetime", FixedDateTime),
        ):
            result = search_bid.fn("SERVICE", "20260501")

        fetch.assert_called_once_with("SERVICE", "20260501")
        self.assertEqual(result["count"], 12)
        self.assertEqual(len(result["bids"]), 10)
        self.assertEqual([bid["bid_id"] for bid in result["bids"]], [str(index) for index in range(10)])


if __name__ == "__main__":
    unittest.main()
