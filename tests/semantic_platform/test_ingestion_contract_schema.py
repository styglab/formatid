from __future__ import annotations

import json
import os
import unittest

from services.semantic_platform.lib.ingestion.endpoint_probe import (
    _provider_status,
    _raw_arguments_by_contract_section,
    _secret_id_candidates,
    _with_auth,
)
from services.semantic_platform.lib.ingestion.llm.validation import validate_llm_analysis as _validate_llm_analysis
from services.semantic_platform.lib.ingestion.llm.proposal import (
    _contract_fields,
    _filter_executable_capabilities,
    _normalize_llm_analysis,
)
from services.semantic_platform.lib.ingestion.llm.runtime import llm_secret_context, openai_api_key


class IngestionContractSchemaTests(unittest.TestCase):
    def test_llm_secret_context_overrides_openai_env_without_persisting(self) -> None:
        original = os.environ.get("OPENAI_API_KEY")
        os.environ["OPENAI_API_KEY"] = "env-key"
        try:
            with llm_secret_context("secret-key"):
                self.assertEqual("secret-key", openai_api_key())
            self.assertEqual("env-key", openai_api_key())
        finally:
            if original is None:
                os.environ.pop("OPENAI_API_KEY", None)
            else:
                os.environ["OPENAI_API_KEY"] = original

    def test_operation_contract_requires_response_items_path(self) -> None:
        analysis = {
            "resources": [{"id": "resource.fx"}],
            "operations": [{"operation_id": "op.fx", "resource_id": "resource.fx"}],
            "semantic_types": [{"id": "exchange_rate_date"}, {"id": "currency_code"}],
            "capabilities": [{"id": "get_exchange_rates", "inputs": ["exchange_rate_date"], "outputs": ["currency_code"]}],
            "operation_contracts": [
                {
                    "operation_id": "op.fx",
                    "capability_id": "get_exchange_rates",
                    "resource_id": "resource.fx",
                    "response": {"fields": {"[].cur_unit": {"semantic_type": "currency_code"}}},
                }
            ],
            "operation_variants": [
                {"variant_id": "variant.fx", "operation_id": "op.fx", "capability_id": "get_exchange_rates"}
            ],
            "field_mappings": [
                {
                    "id": "fm.fx.cur_unit",
                    "operation_id": "op.fx",
                    "direction": "response",
                    "raw_name": "[].cur_unit",
                    "semantic_type_id": "currency_code",
                }
            ],
            "capability_implementations": [
                {"id": "impl.fx", "operation_id": "op.fx", "capability_id": "get_exchange_rates"}
            ],
        }

        with self.assertRaisesRegex(ValueError, "response.items_path required"):
            _validate_llm_analysis(analysis)

    def test_operation_contract_runtime_schema_accepts_declared_paths(self) -> None:
        analysis = {
            "resources": [{"id": "resource.fx"}],
            "operations": [{"operation_id": "op.fx", "resource_id": "resource.fx"}],
            "semantic_types": [{"id": "exchange_rate_date"}, {"id": "currency_code"}],
            "capabilities": [{"id": "get_exchange_rates", "inputs": ["exchange_rate_date"], "outputs": ["currency_code"]}],
            "operation_contracts": [
                {
                    "operation_id": "op.fx",
                    "capability_id": "get_exchange_rates",
                    "resource_id": "resource.fx",
                    "auth": {"parameter": "authkey", "env_names": ["FX_API_KEY"]},
                    "request": {"query": {"searchdate": {"semantic_type": "exchange_rate_date", "required": True}}},
                    "response": {
                        "items_path": "[]",
                        "fields": {"[].cur_unit": {"semantic_type": "currency_code"}},
                    },
                }
            ],
            "operation_variants": [
                {"variant_id": "variant.fx", "operation_id": "op.fx", "capability_id": "get_exchange_rates"}
            ],
            "field_mappings": [
                {
                    "id": "fm.fx.cur_unit",
                    "operation_id": "op.fx",
                    "direction": "response",
                    "raw_name": "[].cur_unit",
                    "semantic_type_id": "currency_code",
                }
            ],
            "capability_implementations": [
                {"id": "impl.fx", "operation_id": "op.fx", "capability_id": "get_exchange_rates"}
            ],
        }

        _validate_llm_analysis(analysis)

    def test_capability_implementation_variant_reference_must_exist(self) -> None:
        analysis = {
            "resources": [{"id": "resource.fx"}],
            "operations": [{"operation_id": "op.fx", "resource_id": "resource.fx"}],
            "semantic_types": [{"id": "exchange_rate_date"}, {"id": "currency_code"}],
            "entities": [],
            "entity_identifiers": [],
            "capabilities": [{"id": "get_exchange_rates", "inputs": ["exchange_rate_date"], "outputs": ["currency_code"]}],
            "capability_entity_links": [],
            "capability_dependencies": [],
            "semantic_join_rules": [],
            "planning_examples": [],
            "operation_contracts": [
                {
                    "operation_id": "op.fx",
                    "capability_id": "get_exchange_rates",
                    "resource_id": "resource.fx",
                    "response": {"items_path": "[]", "fields": {"[].cur_unit": {"semantic_type": "currency_code"}}},
                }
            ],
            "operation_variants": [
                {"variant_id": "variant.fx", "operation_id": "op.fx", "capability_id": "get_exchange_rates"}
            ],
            "field_mappings": [
                {
                    "id": "fm.fx.cur_unit",
                    "operation_id": "op.fx",
                    "direction": "response",
                    "raw_name": "[].cur_unit",
                    "semantic_type_id": "currency_code",
                }
            ],
            "capability_implementations": [
                {
                    "id": "impl.fx",
                    "operation_id": "op.fx",
                    "variant_id": "variant.missing",
                    "capability_id": "get_exchange_rates",
                }
            ],
        }

        with self.assertRaisesRegex(ValueError, "unknown variant"):
            _validate_llm_analysis(analysis)

    def test_semantic_graph_layers_accept_declared_references(self) -> None:
        analysis = {
            "resources": [{"id": "resource.phone"}],
            "operations": [{"operation_id": "op.phone", "resource_id": "resource.phone"}],
            "semantic_types": [{"id": "phone_number"}, {"id": "customer_id"}],
            "entities": [{"id": "Customer"}],
            "entity_identifiers": [
                {"id": "identifier.customer.phone", "entity_id": "Customer", "semantic_type_id": "phone_number"}
            ],
            "capabilities": [{"id": "lookup_customer", "inputs": ["phone_number"], "outputs": ["customer_id"]}],
            "capability_entity_links": [
                {
                    "id": "link.lookup_customer.customer.input",
                    "capability_id": "lookup_customer",
                    "entity_id": "Customer",
                    "role": "input",
                    "semantic_type_id": "phone_number",
                }
            ],
            "capability_dependencies": [],
            "operation_contracts": [
                {
                    "operation_id": "op.phone",
                    "capability_id": "lookup_customer",
                    "resource_id": "resource.phone",
                    "request": {"query": {"phone": {"semantic_type": "phone_number"}}},
                    "response": {"items_path": "items", "fields": {"items[].id": {"semantic_type": "customer_id"}}},
                }
            ],
            "operation_variants": [
                {"variant_id": "variant.phone", "operation_id": "op.phone", "capability_id": "lookup_customer"}
            ],
            "field_mappings": [
                {
                    "id": "fm.phone.id",
                    "operation_id": "op.phone",
                    "direction": "response",
                    "raw_name": "items[].id",
                    "semantic_type_id": "customer_id",
                }
            ],
            "semantic_join_rules": [
                {
                    "id": "join.phone.customer",
                    "from_entity_id": "Customer",
                    "from_semantic_type_id": "phone_number",
                    "to_entity_id": "Customer",
                    "to_semantic_type_id": "phone_number",
                }
            ],
            "planning_examples": [
                {"id": "example.lookup_customer", "question": "전화번호로 고객 찾아줘", "expected_capability_ids": ["lookup_customer"]}
            ],
            "capability_implementations": [
                {"id": "impl.phone", "operation_id": "op.phone", "capability_id": "lookup_customer"}
            ],
        }

        _validate_llm_analysis(analysis)

    def test_llm_analysis_normalizes_semantic_type_aliases(self) -> None:
        analysis = {
            "resources": [{"id": "resource.phone"}],
            "operations": [{"operation_id": "op.phone", "resource_id": "resource.phone"}],
            "semantic_types": [{"id": "phone_number"}, {"id": "customer_id"}],
            "entities": [{"id": "Customer"}],
            "entity_identifiers": [
                {"id": "identifier.customer.phone", "entity_id": "Customer", "semantic_type": "phone_number"}
            ],
            "capabilities": [{"id": "lookup_customer", "inputs": ["phone_number"], "outputs": ["customer_id"]}],
            "capability_entity_links": [
                {
                    "id": "link.lookup_customer.customer.input",
                    "capability_id": "lookup_customer",
                    "entity_id": "Customer",
                    "role": "input",
                    "semantic_type": "phone_number",
                }
            ],
            "capability_dependencies": [],
            "operation_contracts": [
                {
                    "operation_id": "op.phone",
                    "capability_id": "lookup_customer",
                    "resource_id": "resource.phone",
                    "request": {"query": {"phone": {"semantic_type": "phone_number"}}},
                    "response": {"items_path": "items", "fields": {"items[].id": {"semantic_type": "customer_id"}}},
                }
            ],
            "operation_variants": [
                {"variant_id": "variant.phone", "operation_id": "op.phone", "capability_id": "lookup_customer"}
            ],
            "field_mappings": [
                {
                    "id": "fm.phone.id",
                    "operation_id": "op.phone",
                    "direction": "response",
                    "raw_name": "items[].id",
                    "semantic_type": "customer_id",
                }
            ],
            "semantic_join_rules": [
                {
                    "id": "join.phone.customer",
                    "from_entity_id": "Customer",
                    "from_semantic_type": "phone_number",
                    "to_entity_id": "Customer",
                    "to_semantic_type": "phone_number",
                }
            ],
            "planning_examples": [
                {"id": "example.lookup_customer", "question": "전화번호로 고객 찾아줘", "expected_capability_ids": ["lookup_customer"]}
            ],
            "capability_implementations": [
                {"id": "impl.phone", "operation_id": "op.phone", "capability_id": "lookup_customer"}
            ],
        }

        _validate_llm_analysis(_normalize_llm_analysis(analysis))

    def test_llm_analysis_drops_incomplete_join_rules(self) -> None:
        analysis = {
            "semantic_join_rules": [
                {"id": "join.incomplete", "from_entity_id": "Customer", "to_entity_id": "Customer"}
            ]
        }

        self.assertEqual([], _normalize_llm_analysis(analysis)["semantic_join_rules"])

    def test_llm_analysis_drops_incomplete_optional_items(self) -> None:
        analysis = {
            "entity_identifiers": [{"id": "identifier.incomplete", "entity_id": "Customer"}],
            "capability_entity_links": [{"id": "link.incomplete", "capability_id": "lookup_customer"}],
            "capability_dependencies": [{"id": "dependency.incomplete", "capability_id": "lookup_customer"}],
            "field_mappings": [{"id": "fm.incomplete", "operation_id": "op.phone"}],
            "planning_examples": [{"id": "example.incomplete"}],
            "capability_implementations": [{"id": "impl.incomplete", "operation_id": "op.phone"}],
        }

        normalized = _normalize_llm_analysis(analysis)
        self.assertEqual([], normalized["entity_identifiers"])
        self.assertEqual([], normalized["capability_entity_links"])
        self.assertEqual([], normalized["capability_dependencies"])
        self.assertEqual([], normalized["field_mappings"])
        self.assertEqual([], normalized["planning_examples"])
        self.assertEqual([], normalized["capability_implementations"])

    def test_llm_analysis_drops_unknown_semantic_graph_refs(self) -> None:
        analysis = {
            "semantic_types": [{"id": "business_registration_number"}],
            "entities": [{"id": "Business"}],
            "entity_identifiers": [
                {
                    "id": "identifier.unknown",
                    "entity_id": "UnknownEntity",
                    "semantic_type_id": "business_registration_number",
                },
                {
                    "id": "identifier.unknown_type",
                    "entity_id": "Business",
                    "semantic_type_id": "string",
                },
            ],
            "capability_entity_links": [
                {"id": "link.unknown", "capability_id": "capability.status", "entity_id": "UnknownEntity"}
            ],
            "field_mappings": [
                {
                    "id": "fm.unknown_type",
                    "operation_id": "operation.status",
                    "direction": "response",
                    "raw_name": "data[].b_no",
                    "semantic_type_id": "string",
                }
            ],
            "semantic_join_rules": [
                {
                    "id": "join.unknown_entity",
                    "from_entity_id": "UnknownEntity",
                    "from_semantic_type_id": "business_registration_number",
                    "to_entity_id": "Business",
                    "to_semantic_type_id": "business_registration_number",
                }
            ],
        }

        normalized = _normalize_llm_analysis(analysis)
        self.assertEqual([], normalized["entity_identifiers"])
        self.assertEqual([], normalized["capability_entity_links"])
        self.assertEqual([], normalized["field_mappings"])
        self.assertEqual([], normalized["semantic_join_rules"])

    def test_llm_analysis_normalizes_operation_id_aliases(self) -> None:
        analysis = {
            "operations": [{"id": "op.status"}],
            "operation_contracts": [{"id": "op.status"}],
            "operation_variants": [{"id": "op.status", "variant_id": "variant.status"}],
        }

        normalized = _normalize_llm_analysis(analysis)
        self.assertEqual("op.status", normalized["operations"][0]["operation_id"])
        self.assertEqual("op.status", normalized["operation_contracts"][0]["operation_id"])
        self.assertEqual("op.status", normalized["operation_variants"][0]["operation_id"])

    def test_llm_analysis_normalizes_contract_field_semantic_aliases(self) -> None:
        analysis = {
            "operation_contracts": [
                {
                    "operation_id": "op.status",
                    "request": {"body": {"b_no": {"semantic_type_id": "business_registration_number"}}},
                    "response": {
                        "items_path": "data",
                        "fields": {
                            "data[].b_no": {"semantic_type_id": "business_registration_number"},
                            "data[].ignored": {},
                        },
                    },
                }
            ]
        }

        contract = _normalize_llm_analysis(analysis)["operation_contracts"][0]
        self.assertEqual("business_registration_number", contract["request"]["body"]["b_no"]["semantic_type"])
        self.assertEqual("business_registration_number", contract["response"]["fields"]["data[].b_no"]["semantic_type"])
        self.assertNotIn("data[].ignored", contract["response"]["fields"])

    def test_executable_filter_drops_undeclared_variant_control_capabilities(self) -> None:
        analysis = {
            "resources": [{"id": "resource.nts"}],
            "operations": [
                {"operation_id": "operation.status", "resource_id": "resource.nts"},
                {"operation_id": "operation.validate", "resource_id": "resource.nts"},
            ],
            "operation_fields": [],
            "semantic_types": [{"id": "business_registration_number"}, {"id": "business_status"}],
            "entities": [{"id": "Business"}],
            "entity_identifiers": [
                {"id": "identifier.business.registration", "entity_id": "Business", "semantic_type_id": "business_registration_number"}
            ],
            "capabilities": [
                {"id": "check_business_registration_status", "inputs": ["business_registration_number"], "outputs": ["business_status"]},
                {"id": "get_business_status_by_tax_type_01", "inputs": ["business_registration_number"], "outputs": ["business_status"]},
            ],
            "capability_entity_links": [
                {
                    "id": "link.status.business",
                    "capability_id": "check_business_registration_status",
                    "entity_id": "Business",
                    "role": "input",
                    "semantic_type_id": "business_registration_number",
                },
                {
                    "id": "link.tax.business",
                    "capability_id": "get_business_status_by_tax_type_01",
                    "entity_id": "Business",
                    "role": "input",
                    "semantic_type_id": "business_registration_number",
                },
            ],
            "capability_dependencies": [],
            "operation_contracts": [
                {
                    "operation_id": "operation.status",
                    "capability_id": "check_business_registration_status",
                    "resource_id": "resource.nts",
                    "request": {"body": {"b_no": {"semantic_type": "business_registration_number"}}},
                    "response": {"items_path": "data", "fields": {"data[].b_stt": {"semantic_type": "business_status"}}},
                }
            ],
            "operation_variants": [
                {
                    "variant_id": "variant.status.tax_type.01",
                    "operation_id": "operation.status",
                    "capability_id": "get_business_status_by_tax_type_01",
                    "fixed_raw_arguments": {"tax_type": "01"},
                }
            ],
            "field_mappings": [
                {
                    "id": "fm.status",
                    "operation_id": "operation.status",
                    "direction": "response",
                    "raw_name": "data[].b_stt",
                    "semantic_type_id": "business_status",
                }
            ],
            "semantic_join_rules": [],
            "planning_examples": [],
            "capability_implementations": [],
        }

        filtered = _filter_executable_capabilities(analysis)
        self.assertEqual(["check_business_registration_status"], [item["id"] for item in filtered["capabilities"]])
        self.assertEqual([], filtered["operation_variants"])

    def test_contract_fields_collects_all_request_sections(self) -> None:
        fields = _contract_fields(
            {
                "query": {"returnType": {"semantic_type": "response_format"}},
                "body": {"b_no": {"semantic_type": "business_registration_number"}},
                "header": {"x-trace": {"semantic_type": "request_number"}},
            }
        )

        self.assertEqual({"returnType", "b_no", "x-trace"}, set(fields))

    def test_with_auth_uses_declared_auth_only(self) -> None:
        os.environ["DECLARED_API_KEY"] = "secret"
        try:
            arguments = _with_auth(
                {},
                {"auth": {"parameter": "authkey", "env_names": ["DECLARED_API_KEY"]}},
            )
        finally:
            os.environ.pop("DECLARED_API_KEY", None)

        self.assertEqual({"authkey": "secret"}, arguments)
        self.assertNotIn("type", arguments)

    def test_secret_id_candidates_include_dashboard_secret_id_form(self) -> None:
        self.assertEqual(
            ["DATA_GO_KR_API_KEY", "secret.data_go_kr_api_key"],
            _secret_id_candidates("DATA_GO_KR_API_KEY"),
        )

    def test_raw_arguments_support_flat_request_fields_and_defaults(self) -> None:
        contract = {
            "request": {
                "fields": {
                    "inqryBgnDt": {
                        "location": "query",
                        "semantic_type": "registration_datetime_range",
                        "transform": "date_start",
                        "format": "yyyyMMddHHmm",
                    },
                    "inqryEndDt": {
                        "location": "query",
                        "semantic_type": "registration_datetime_range",
                        "transform": "date_end",
                        "format": "yyyyMMddHHmm",
                    },
                    "x-trace": {"location": "header", "semantic_type": "trace_id"},
                },
                "defaults": {"pageNo": 1, "numOfRows": 10},
            }
        }

        arguments = _raw_arguments_by_contract_section(
            {
                "registration_datetime_range": {"from": "2025-01-01 00:00", "to": "2025-01-01 23:59"},
                "trace_id": "abc",
            },
            contract,
        )

        self.assertEqual("202501010000", arguments["query"]["inqryBgnDt"])
        self.assertEqual("202501012359", arguments["query"]["inqryEndDt"])
        self.assertEqual(1, arguments["query"]["pageNo"])
        self.assertEqual("abc", arguments["header"]["x-trace"])

    def test_provider_status_uses_declared_success_condition(self) -> None:
        contract = {
            "response": {
                "success": {
                    "path": "response.header.resultCode",
                    "equals": "00",
                    "message_path": "response.header.resultMsg",
                }
            }
        }
        body = json.dumps({"response": {"header": {"resultCode": "00", "resultMsg": "정상"}}}).encode("utf-8")

        status, message, result, sample = _provider_status(body, "application/json", contract)

        self.assertEqual("00", status)
        self.assertEqual("정상", message)
        self.assertEqual("passed", result)
        self.assertEqual("00", sample["response"]["header"]["resultCode"])

    def test_provider_status_uses_declared_success_condition_for_xml(self) -> None:
        contract = {
            "response": {
                "success": {
                    "path": "response.header.code",
                    "equals": "OK",
                    "message_path": "response.header.message",
                }
            }
        }
        body = b"<response><header><code>OK</code><message>done</message></header></response>"

        status, message, result, sample = _provider_status(body, "application/xml", contract)

        self.assertEqual("OK", status)
        self.assertEqual("done", message)
        self.assertEqual("passed", result)
        self.assertEqual("OK", sample["response"]["header"]["code"])

    def test_provider_status_fails_when_declared_success_path_is_absent(self) -> None:
        contract = {"response": {"success": {"path": "response.header.code", "equals": "OK"}}}
        body = b"<response><header><message>done</message></header></response>"

        status, message, result, _sample = _provider_status(body, "application/xml", contract)

        self.assertEqual("unknown", status)
        self.assertEqual("declared success path not found", message)
        self.assertEqual("failed", result)


if __name__ == "__main__":
    unittest.main()
