from __future__ import annotations

import unittest
import sys
import types


if "requests" not in sys.modules:
    requests_stub = types.ModuleType("requests")
    requests_stub.RequestException = Exception
    requests_stub.Timeout = TimeoutError
    sys.modules["requests"] = requests_stub

from apps.pubdata_mcp.app.common import catalog
from apps.pubdata_mcp.app.common.execution import (
    _contract_items,
    _contract_payload_error,
    _generic_items,
    _normalize_response,
    _redact_secrets,
    _result_status,
    _smoke_result,
    _semantic_to_raw_arguments,
    _split_request_arguments,
    _validate_raw_arguments,
    execute_semantic_plan,
)


class PubdataMcpExecutionTests(unittest.TestCase):
    def test_response_mapping_resolves_array_paths_against_item_rows(self) -> None:
        normalized = _normalize_response(
            {
                "items": [
                    {"b_no": "1234567890", "tax_type": "국세청에 등록되지 않은 사업자등록번호입니다."}
                ]
            },
            [{"field_name": "data[].tax_type", "semantic_type": "business_tax_status"}],
        )

        self.assertEqual(1, len(normalized["items"]))
        self.assertEqual(
            "국세청에 등록되지 않은 사업자등록번호입니다.",
            normalized["items"][0]["semantic"]["business_tax_status"],
        )

    def test_response_mapping_resolves_nested_paths(self) -> None:
        normalized = _normalize_response(
            {"items": [{"company": {"summary": {"revenue": "1000"}}}]},
            [{"field_name": "company.summary.revenue", "semantic_type": "corporate_revenue_amount"}],
        )

        self.assertEqual("1000", normalized["items"][0]["semantic"]["corporate_revenue_amount"])

    def test_generic_items_extracts_common_public_api_shapes(self) -> None:
        payload = {"response": {"body": {"items": {"item": [{"name": "one"}, {"name": "two"}]}}}}

        self.assertEqual([{"name": "one"}, {"name": "two"}], _generic_items(payload))

    def test_contract_items_path_controls_item_extraction(self) -> None:
        payload = {"response": {"body": {"items": {"item": [{"name": "one"}]}, "totalCount": 1}}}
        operation_contract = {"response": {"items_path": "response.body.items.item"}}

        self.assertEqual([{"name": "one"}], _contract_items(payload, operation_contract))

    def test_contract_items_path_empty_when_declared_path_is_absent(self) -> None:
        payload = {"response": {"body": {"totalCount": 0}}}
        operation_contract = {"response": {"items_path": "response.body.items.item"}}

        self.assertEqual([], _contract_items(payload, operation_contract))

    def test_contract_success_condition_controls_provider_errors(self) -> None:
        operation_contract = {
            "response": {
                "success": {
                    "path": "response.header.resultCode",
                    "equals": "00",
                    "message_path": "response.header.resultMsg",
                }
            }
        }

        self.assertIsNone(
            _contract_payload_error(
                {"response": {"header": {"resultCode": "00", "resultMsg": "정상"}}},
                operation_contract,
            )
        )
        error = _contract_payload_error(
            {"response": {"header": {"resultCode": "99", "resultMsg": "오류"}}},
            operation_contract,
        )
        self.assertEqual("provider_error", error["type"])
        self.assertEqual("99", error["provider_status"])

    def test_redacts_auth_keys_recursively(self) -> None:
        redacted = _redact_secrets(
            {
                "authkey": "secret-a",
                "ServiceKey": "secret-b",
                "nested": {"apiKey": "secret-c", "value": "ok"},
            }
        )

        self.assertEqual("***REDACTED***", redacted["authkey"])
        self.assertEqual("***REDACTED***", redacted["ServiceKey"])
        self.assertEqual("***REDACTED***", redacted["nested"]["apiKey"])
        self.assertEqual("ok", redacted["nested"]["value"])

    def test_required_semantic_arguments_are_validated_before_execution(self) -> None:
        execution = execute_semantic_plan(
            {
                "query": "환율 조회해줘",
                "execution_graph": {
                    "nodes": [
                        {
                            "id": "fx",
                            "capability": "get_exchange_rates",
                            "operation_id": "koreaexim.exchange_rate.search",
                            "call": {"semantic_arguments": {}},
                        }
                    ]
                },
            },
            {
                "operation_contracts": {
                    "koreaexim.exchange_rate.search": {
                        "provider": "koreaexim",
                        "resource_id": "koreaexim.exchange_rate",
                        "method": "GET",
                        "path": "/exchangeJSON",
                        "request": {
                            "query": {
                                "searchdate": {
                                    "required": True,
                                    "semantic_type": "exchange_rate_date",
                                }
                            }
                        },
                    }
                },
                "capability_implementations": {},
                "operation_field_mappings": {},
                "operation_variants": {},
                "resources": {},
            },
        )

        self.assertEqual("not_executed", execution["status"])
        self.assertEqual("skipped", execution["results"][0]["status"])
        self.assertEqual("validation_error", execution["results"][0]["result_status"])
        self.assertEqual("missing_required_semantic_arguments", execution["results"][0]["reason"])
        self.assertEqual(["exchange_rate_date"], execution["results"][0]["missing"])

    def test_contract_transform_formats_phone_before_raw_request(self) -> None:
        raw_arguments = _semantic_to_raw_arguments(
            {"phone_number": "01022223333"},
            [],
            {
                "request": {
                    "query": {
                        "phone": {
                            "semantic_type": "phone_number",
                            "transform": {"name": "phone_format", "style": "kr_mobile_hyphen"},
                            "pattern": r"^01[016789]-[0-9]{3,4}-[0-9]{4}$",
                        }
                    }
                }
            },
        )

        self.assertEqual({"phone": "010-2222-3333"}, raw_arguments)

    def test_contract_flat_fields_map_to_raw_arguments(self) -> None:
        raw_arguments = _semantic_to_raw_arguments(
            {
                "registration_datetime_range": {"from": "2025-01-01 00:00", "to": "2025-01-01 23:59"},
                "response_format": "json",
            },
            [],
            {
                "request": {
                    "fields": {
                        "inqryBgnDate": {
                            "location": "query",
                            "semantic_type": "registration_datetime_range",
                            "transform": "date_start",
                            "format": "yyyyMMdd",
                        },
                        "inqryEndDate": {
                            "location": "query",
                            "semantic_type": "registration_datetime_range",
                            "transform": "date_end",
                            "format": "yyyyMMdd",
                        },
                        "type": {"location": "query", "semantic_type": "response_format"},
                    },
                    "defaults": {"pageNo": 1, "numOfRows": 10},
                }
            },
        )

        self.assertEqual("20250101", raw_arguments["inqryBgnDate"])
        self.assertEqual("20250101", raw_arguments["inqryEndDate"])
        self.assertEqual("json", raw_arguments["type"])
        self.assertEqual(1, raw_arguments["pageNo"])

    def test_split_request_arguments_supports_flat_field_locations(self) -> None:
        query, body = _split_request_arguments(
            {"q": "one", "payload": {"value": 1}, "extra": "fallback"},
            {
                "request": {
                    "fields": {
                        "q": {"location": "query"},
                        "payload": {"location": "body"},
                    }
                }
            },
        )

        self.assertEqual({"q": "one"}, query)
        self.assertEqual({"payload": {"value": 1}, "extra": "fallback"}, body)

    def test_contract_validation_rejects_pattern_mismatch_before_execution(self) -> None:
        errors = _validate_raw_arguments(
            {"phone": "01022223333"},
            {
                "request": {
                    "query": {
                        "phone": {
                            "semantic_type": "phone_number",
                            "pattern": r"^01[016789]-[0-9]{3,4}-[0-9]{4}$",
                        }
                    }
                }
            },
        )

        self.assertEqual("pattern", errors[0]["rule"])
        self.assertEqual("phone_number", errors[0]["semantic_type"])

    def test_execution_returns_validation_error_for_invalid_contract_argument(self) -> None:
        execution = execute_semantic_plan(
            {
                "query": "전화번호 조회",
                "execution_graph": {
                    "nodes": [
                        {
                            "id": "phone",
                            "capability": "lookup_phone",
                            "operation_id": "provider.lookup_phone",
                            "call": {"semantic_arguments": {"phone_number": "01022223333"}},
                        }
                    ]
                },
            },
            {
                "operation_contracts": {
                    "provider.lookup_phone": {
                        "provider": "provider",
                        "resource_id": "provider.resource",
                        "method": "GET",
                        "path": "/lookup",
                        "request": {
                            "query": {
                                "phone": {
                                    "semantic_type": "phone_number",
                                    "pattern": r"^01[016789]-[0-9]{3,4}-[0-9]{4}$",
                                }
                            }
                        },
                    }
                },
                "capability_implementations": {},
                "operation_field_mappings": {},
                "operation_variants": {},
                "resources": {"provider.resource": {"base_url": "https://example.invalid"}},
            },
        )

        self.assertEqual("not_executed", execution["status"])
        self.assertEqual("skipped", execution["results"][0]["status"])
        self.assertEqual("validation_error", execution["results"][0]["result_status"])
        self.assertEqual("argument_validation_failed", execution["results"][0]["reason"])

    def test_result_status_distinguishes_empty_success_and_provider_error(self) -> None:
        self.assertEqual(
            "executed_empty",
            _result_status("executed", semantic_result={"items": []}),
        )
        self.assertEqual(
            "executed_with_items",
            _result_status("executed", semantic_result={"items": [{"semantic": {"x": 1}}]}),
        )
        self.assertEqual(
            "provider_error",
            _result_status("error", raw_result={"error": {"type": "provider_error"}}),
        )

    def test_smoke_result_exposes_result_status(self) -> None:
        result = _smoke_result(
            operation_id="op.test",
            status="executed",
            result_status="executed_with_items",
            semantic_arguments={},
        )

        self.assertEqual("executed_with_items", result["result_status"])

    def test_semantic_query_exposes_stable_top_level_schema(self) -> None:
        original_plan = catalog.semantic_plan_query
        original_contracts = catalog.load_execution_contracts
        try:
            catalog.semantic_plan_query = lambda query, limit, manual_plan=None: {
                "query": query,
                "planner": {"status": "valid"},
                "execution_graph": {
                    "type": "dag",
                    "status": "planned",
                    "nodes": [
                        {
                            "id": "node_1",
                            "capability": "get_exchange_rates",
                            "operation_id": "koreaexim.exchange_rate.search",
                            "variant_id": "koreaexim.exchange_rate.search.ap01",
                        }
                    ],
                },
                "errors": [],
            }
            catalog.load_execution_contracts = lambda: {
                "capability_implementations": {},
                "operation_field_mappings": {},
                "operation_contracts": {
                    "koreaexim.exchange_rate.search": {
                        "provider": "koreaexim",
                        "resource_id": "koreaexim.exchange_rate",
                        "method": "GET",
                        "path": "/exchangeJSON",
                    }
                },
                "operation_variants": {},
                "resources": {
                    "koreaexim.exchange_rate": {
                        "base_url": "https://example.invalid/exchange",
                    }
                },
            }

            response = catalog.semantic_query("환율 조회", execute=False)
        finally:
            catalog.semantic_plan_query = original_plan
            catalog.load_execution_contracts = original_contracts

        self.assertEqual("planned", response["status"])
        self.assertEqual("plan_only", response["result_status"])
        for key in ("selected_capabilities", "execution_graph", "results", "errors", "evidence"):
            self.assertIn(key, response)
        self.assertEqual("get_exchange_rates", response["selected_capabilities"][0]["capability"])


if __name__ == "__main__":
    unittest.main()
