from __future__ import annotations

import unittest
from contextlib import contextmanager
from datetime import datetime
from importlib.util import find_spec
from unittest.mock import patch
from zoneinfo import ZoneInfo

from apps.g2b_pipeline.app.steps.bid_notices import (
    BASE_URLS,
    CATEGORY_LABELS,
    compute_realtime_window_value,
)
from apps.g2b_pipeline.app.steps.license_limits import (
    normalize_license_limit_raw_row,
    parse_main_field_groups,
    parse_name_code_list,
)
from apps.g2b_pipeline.app.steps.participation_regions import normalize_participation_region_raw_row

if find_spec("prefect"):
    from apps.g2b_pipeline.app.service import ingest
else:
    ingest = None


KST = ZoneInfo("Asia/Seoul")


class G2BBidPipelineTests(unittest.TestCase):
    def test_realtime_window_uses_current_time_and_lookback_minutes(self) -> None:
        window = compute_realtime_window_value(
            datetime(2026, 5, 7, 19, 33, 27, tzinfo=KST),
            lookback_minutes=90,
        )

        self.assertEqual(window, {"begin": "202605071803", "end": "202605071933"})

    def test_realtime_window_rejects_invalid_lookback(self) -> None:
        with self.assertRaisesRegex(ValueError, "lookback_minutes"):
            compute_realtime_window_value(lookback_minutes=0)

    def test_foreign_category_is_configured(self) -> None:
        self.assertEqual(CATEGORY_LABELS["FOREIGN"], "외자")
        self.assertIn("getBidPblancListInfoFrgcptPPSSrch", BASE_URLS["FOREIGN"])

    def test_license_limit_lists_are_parsed(self) -> None:
        self.assertEqual(
            parse_name_code_list("[폐기물종합처분업/1143],[폐기물중간처분업(의료폐기물)/1255]"),
            [
                {"name": "폐기물종합처분업", "code": "1143"},
                {"name": "폐기물중간처분업(의료폐기물)", "code": "1255"},
            ],
        )
        self.assertEqual(
            parse_main_field_groups("[1^포장공사^보링.그라우팅.파일공사],[2^토공사]"),
            [
                {"group_seq": "1", "all_of": ["포장공사", "보링.그라우팅.파일공사"]},
                {"group_seq": "2", "all_of": ["토공사"]},
            ],
        )

    def test_license_limit_raw_row_is_normalized(self) -> None:
        row = {
            "id": 7,
            "resource_key": "LICENSE:1:000:1:1:폐기물수집.운반업(의료폐기물)/6727",
            "raw_payload": {
                "bidNtceNo": "1",
                "bidNtceOrd": "000",
                "bsnsDivNm": "공사",
                "rgstDt": "2026-05-01 09:00:00",
                "lmtGrpNo": "1",
                "lmtSno": "1",
                "lcnsLmtNm": "폐기물수집.운반업(의료폐기물)/6727",
                "permsnIndstrytyList": "[폐기물종합처분업/1143]",
                "indstrytyMfrcFldList": "[1^포장공사^보링.그라우팅.파일공사]",
            },
        }

        normalized = normalize_license_limit_raw_row(row)

        self.assertEqual(normalized["category"], "CONSTRUCTION")
        self.assertEqual(normalized["license_limit_name"], "폐기물수집.운반업(의료폐기물)")
        self.assertEqual(normalized["license_limit_code"], "6727")
        self.assertEqual(normalized["allowed_industries"], [{"name": "폐기물종합처분업", "code": "1143"}])

    def test_participation_region_raw_row_is_normalized(self) -> None:
        row = {
            "id": 8,
            "resource_key": "REGION:1:000:1:1:서울특별시/11",
            "raw_payload": {
                "bidNtceNo": "1",
                "bidNtceOrd": "000",
                "bsnsDivNm": "용역",
                "rgstDt": "2026-05-01 09:00:00",
                "lmtGrpNo": "1",
                "lmtSno": "1",
                "prtcptPsblRgnNm": "서울특별시/11",
            },
        }

        normalized = normalize_participation_region_raw_row(row)

        self.assertEqual(normalized["category"], "SERVICE")
        self.assertEqual(normalized["region_name"], "서울특별시")
        self.assertEqual(normalized["region_code"], "11")

    @unittest.skipUnless(find_spec("prefect"), "prefect is not installed")
    def test_g2b_response_error_payload_raises(self) -> None:
        from apps.g2b_pipeline.app.tasks.api import _raise_for_g2b_error

        payload = {
            "nkoneps.com.response.ResponseError": {
                "header": {
                    "resultCode": "08",
                    "resultMsg": "필수값 입력 에러",
                }
            }
        }

        with self.assertRaisesRegex(RuntimeError, "resultCode=08"):
            _raise_for_g2b_error(payload)

    @unittest.skipUnless(find_spec("prefect"), "prefect is not installed")
    def test_g2b_response_header_error_raises(self) -> None:
        from apps.g2b_pipeline.app.tasks.api import _raise_for_g2b_error

        payload = {
            "response": {
                "header": {
                    "resultCode": "22",
                    "resultMsg": "LIMITED NUMBER OF SERVICE REQUESTS EXCEEDS ERROR.",
                },
                "body": {},
            }
        }

        with self.assertRaisesRegex(RuntimeError, "resultCode=22"):
            _raise_for_g2b_error(payload)

    @unittest.skipUnless(find_spec("prefect"), "prefect is not installed")
    def test_initial_ingest_normalizes_requested_window(self) -> None:
        with (
            patch.object(ingest.fetch_category, "fn", return_value=[{"bidNtceNo": "1"}]) as fetch,
            patch.object(ingest.write_records, "fn", return_value=1) as write,
            patch.object(
                ingest,
                "fetch_license_limits",
            ) as fetch_license_task,
            patch.object(
                ingest,
                "write_license_limits",
            ) as write_license_task,
            patch.object(
                ingest,
                "fetch_participation_regions",
            ) as fetch_region_task,
            patch.object(
                ingest,
                "write_participation_regions",
            ) as write_region_task,
            patch.object(
                ingest,
                "normalize_raw_notices_once",
                return_value={"target": {"count": 3}},
            ) as normalize,
            patch.object(
                ingest,
                "normalize_license_limits_once",
                return_value={"target": {"count": 1}},
            ) as normalize_license,
            patch.object(
                ingest,
                "normalize_participation_regions_once",
                return_value={"target": {"count": 2}},
            ) as normalize_region,
        ):
            fetch_license_task.fn.return_value = [{"bidNtceNo": "1"}]
            write_license_task.fn.return_value = 1
            fetch_region_task.fn.return_value = [{"bidNtceNo": "1"}]
            write_region_task.fn.return_value = 2
            result = ingest.run_g2b_bid_initial_ingest(
                begin="202605010000",
                end="202605012359",
            )

        self.assertEqual(fetch.call_count, 3)
        self.assertEqual(write.call_count, 3)
        fetch_license_task.fn.assert_called_once()
        write_license_task.fn.assert_called_once()
        fetch_region_task.fn.assert_called_once()
        write_region_task.fn.assert_called_once()
        normalize.assert_called_once_with(window_begin="202605010000", window_end="202605012359")
        normalize_license.assert_called_once_with(window_begin="202605010000", window_end="202605012359")
        normalize_region.assert_called_once_with(window_begin="202605010000", window_end="202605012359")
        self.assertEqual(result["raw"]["total"], 3)
        self.assertEqual(result["normalized"], {"target": {"count": 3}})
        self.assertEqual(result["license_limits"]["raw"]["count"], 1)
        self.assertEqual(result["participation_regions"]["raw"]["count"], 2)

    @unittest.skipUnless(find_spec("prefect"), "prefect is not installed")
    def test_initial_ingest_skips_when_run_lock_is_held(self) -> None:
        @contextmanager
        def locked():
            yield False

        with (
            patch.object(ingest, "g2b_bid_ingest_run_lock", locked),
            patch.object(ingest.fetch_category, "fn") as fetch,
        ):
            result = ingest.run_g2b_bid_initial_ingest(
                begin="202605010000",
                end="202605012359",
            )

        fetch.assert_not_called()
        self.assertEqual(result["skipped"], True)
        self.assertEqual(result["flow"], "g2b-bid-initial-ingest")

    @unittest.skipUnless(find_spec("prefect"), "prefect is not installed")
    def test_realtime_ingest_skips_when_run_lock_is_held(self) -> None:
        @contextmanager
        def locked():
            yield False

        with (
            patch.object(ingest, "g2b_bid_ingest_run_lock", locked),
            patch.object(ingest, "_ingest_notice_raw") as ingest_notice_raw,
        ):
            result = ingest.process_realtime_window(
                {"begin": "202605010000", "end": "202605010005"},
                use_prefect_tasks=False,
            )

        ingest_notice_raw.assert_not_called()
        self.assertEqual(result["skipped"], True)
        self.assertEqual(result["flow"], "g2b-bid-realtime-ingest")


if __name__ == "__main__":
    unittest.main()
