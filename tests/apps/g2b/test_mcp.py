from __future__ import annotations

import unittest
from datetime import datetime
from decimal import Decimal
from importlib.util import find_spec
from unittest.mock import patch

if find_spec("psycopg"):
    from apps.g2b.mcp.app.adapters import db
else:
    db = None


class FakeCursor:
    def __init__(self) -> None:
        self.calls: list[tuple[object, tuple[object, ...]]] = []

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def execute(self, query: object, params: tuple[object, ...]) -> None:
        self.calls.append((query, params))

    def fetchone(self) -> dict[str, int]:
        return {"count": 3}

    def fetchall(self) -> list[dict[str, object]]:
        return [
            {
                "resource_key": "SERVICE:1:000",
                "category": "SERVICE",
                "category_label": "용역",
                "bid_notice_no": "1",
                "bid_notice_order": "000",
                "title": "First bid",
                "organization_name": "Org",
                "demand_org_name": "Demand",
                "budget": Decimal("123000"),
                "published_at": datetime(2026, 5, 1, 12, 0, tzinfo=db.G2B_TIMEZONE),
                "deadline_at": datetime(2026, 5, 10, 18, 0, tzinfo=db.G2B_TIMEZONE),
                "opening_at": None,
                "contract_method": "일반경쟁",
                "bid_method": "전자입찰",
                "notice_kind": "일반공고",
                "detail_url": "https://example.test/bid/1",
            }
        ]


class FakeConnection:
    def __init__(self, cursor: FakeCursor) -> None:
        self._cursor = cursor

    def __enter__(self) -> FakeConnection:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def cursor(self) -> FakeCursor:
        return self._cursor


@unittest.skipUnless(find_spec("psycopg"), "psycopg is not installed")
class G2BMCPTests(unittest.TestCase):
    def test_search_bids_applies_filters_pagination_and_sort_metadata(self) -> None:
        cursor = FakeCursor()

        with (
            patch.dict("os.environ", {"G2B_MCP_DATABASE_URL": "postgresql://test", "G2B_MCP_MAX_LIMIT": "20"}),
            patch.object(db.psycopg, "connect", return_value=FakeConnection(cursor)) as connect,
        ):
            result = db.search_bids(
                category="service",
                keyword="cloud",
                notice_kind="등록공고",
                contract_method="일반경쟁",
                bid_method="전자입찰",
                bid_notice_no="1",
                bid_notice_order="000",
                deadline_to="2026-05-31",
                opening_from="2026-05-02",
                demand_org_name="Demand",
                has_budget=True,
                min_budget="100,000",
                limit=5,
                offset=2,
                sort_by="deadline_at",
                sort_order="asc",
            )

        connect.assert_called_once()
        self.assertEqual(result["count"], 3)
        self.assertEqual(result["returned"], 1)
        self.assertEqual(result["limit"], 5)
        self.assertEqual(result["offset"], 2)
        self.assertFalse(result["has_more"])
        self.assertEqual(result["sort"], {"by": "deadline_at", "order": "asc"})
        self.assertEqual(result["bids"][0]["id"], "SERVICE:1:000")
        self.assertEqual(result["bids"][0]["budget"], 123000)

        self.assertEqual(len(cursor.calls), 2)
        rows_params = cursor.calls[1][1]
        self.assertEqual(rows_params[0], "SERVICE")
        self.assertEqual(rows_params[1:4], ("%cloud%", "%cloud%", "%cloud%"))
        self.assertIn("등록공고", rows_params)
        self.assertIn("%일반경쟁%", rows_params)
        self.assertIn("%전자입찰%", rows_params)
        self.assertIn("1", rows_params)
        self.assertIn("000", rows_params)
        self.assertIn("%Demand%", rows_params)
        self.assertEqual(rows_params[-2:], (5, 2))

    def test_search_bids_excludes_cancelled_by_default_and_can_include_them(self) -> None:
        cursor = FakeCursor()

        with (
            patch.dict("os.environ", {"G2B_MCP_DATABASE_URL": "postgresql://test"}),
            patch.object(db.psycopg, "connect", return_value=FakeConnection(cursor)),
        ):
            db.search_bids(limit=5)

        self.assertIn("%취소%", cursor.calls[1][1])

        cursor = FakeCursor()
        with (
            patch.dict("os.environ", {"G2B_MCP_DATABASE_URL": "postgresql://test"}),
            patch.object(db.psycopg, "connect", return_value=FakeConnection(cursor)),
        ):
            db.search_bids(limit=5, exclude_cancelled=False)

        self.assertNotIn("%취소%", cursor.calls[1][1])

    def test_search_bids_rejects_invalid_input_before_connecting(self) -> None:
        with patch.object(db.psycopg, "connect") as connect:
            with self.assertRaisesRegex(ValueError, "Invalid category"):
                db.search_bids(category="bad")
        connect.assert_not_called()

        with patch.object(db.psycopg, "connect") as connect:
            with self.assertRaisesRegex(ValueError, "offset"):
                db.search_bids(offset=-1)
        connect.assert_not_called()

        with patch.object(db.psycopg, "connect") as connect:
            with self.assertRaisesRegex(ValueError, "Invalid sort_by"):
                db.search_bids(sort_by="title")
        connect.assert_not_called()

    def test_foreign_category_is_valid(self) -> None:
        cursor = FakeCursor()

        with (
            patch.dict("os.environ", {"G2B_MCP_DATABASE_URL": "postgresql://test"}),
            patch.object(db.psycopg, "connect", return_value=FakeConnection(cursor)),
        ):
            db.search_bids(category="foreign", limit=5)

        self.assertEqual(cursor.calls[1][1][0], "FOREIGN")

    def test_date_only_upper_bound_includes_entire_day(self) -> None:
        parsed = db._parse_datetime("2026-05-31", end_of_day=True)

        self.assertEqual(parsed.isoformat(), "2026-05-31T23:59:59+09:00")

    @unittest.skipUnless(find_spec("fastmcp"), "fastmcp is not installed")
    def test_search_bid_returns_structured_validation_error(self) -> None:
        from apps.g2b.mcp.app.main import search_bid

        with patch("apps.g2b.mcp.app.main.search_bids", side_effect=ValueError("Invalid category: BAD")):
            result = search_bid.fn(category="BAD")

        self.assertEqual(
            result,
            {
                "error": {
                    "type": "invalid_request",
                    "message": "Invalid category: BAD",
                }
            },
        )


if __name__ == "__main__":
    unittest.main()
