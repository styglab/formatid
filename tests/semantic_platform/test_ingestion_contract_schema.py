from __future__ import annotations

import json
import os
import unittest

from services.semantic_platform.lib.ingestion.endpoint_probe import _provider_status, _with_auth
from services.semantic_platform.lib.ingestion.graph import _validate_llm_analysis
from services.semantic_platform.lib.ingestion.llm.proposal import _contract_fields


class IngestionContractSchemaTests(unittest.TestCase):
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

        status, message, result = _provider_status(body, "application/json", contract)

        self.assertEqual("00", status)
        self.assertEqual("정상", message)
        self.assertEqual("passed", result)


if __name__ == "__main__":
    unittest.main()
