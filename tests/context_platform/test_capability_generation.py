import unittest

from services.context_platform.internal.ingestion.capability_generation import build_manual_capability_generation_request
from services.context_platform.internal.ingestion.capability_generation import capability_suggestions_from_manual_response
from services.context_platform.internal.ingestion.capability_generation import collect_operation_contexts
from services.context_platform.internal.ingestion.capability_generation import suggest_capability_for_operation
from services.context_platform.internal.ingestion.llm.capability_generation import normalize_manual_capability_generation_response


class CapabilityGenerationTest(unittest.TestCase):
    def test_collect_operation_contexts_groups_bindings(self) -> None:
        contexts = collect_operation_contexts(
            source={"id": "src_1"},
            document={"id": "doc_1"},
            operations=[
                {
                    "id": "op_1",
                    "operation_key": "getFinancialStatement",
                    "name": "getFinancialStatement",
                    "method": "GET",
                    "path": "/getFinancialStatement",
                }
            ],
            binding_generation={
                "suggestions": [
                    {
                        "decision": "bind",
                        "source_operation_id": "op_1",
                        "source_parameter_id": "param_crno",
                        "field_path": "request.crno",
                        "direction": "input",
                        "canonical_ref": {
                            "class_name": "company",
                            "slot_name": "corporate_registration_number",
                        },
                    },
                    {
                        "decision": "bind",
                        "source_operation_id": "op_1",
                        "source_field_id": "field_sales",
                        "field_path": "response.enpSaleAmt",
                        "direction": "output",
                        "canonical_ref": {
                            "class_name": "financial_fact",
                            "slot_name": "amount",
                        },
                    },
                ]
            },
        )

        self.assertEqual(len(contexts), 1)
        self.assertEqual(len(contexts[0]["input_bindings"]), 1)
        self.assertEqual(len(contexts[0]["output_bindings"]), 1)

    def test_suggest_capability_uses_canonical_input_output_key(self) -> None:
        context = {
            "source_document_id": "doc_1",
            "operation": {
                "source_operation_id": "op_1",
                "operation_key": "getFinancialStatement",
                "name": "getFinancialStatement",
                "method": "GET",
                "path": "/getFinancialStatement",
            },
            "input_bindings": [
                {
                    "source_parameter_id": "param_crno",
                    "field_path": "request.crno",
                    "direction": "input",
                    "canonical_ref": {
                        "class_name": "company",
                        "slot_name": "corporate_registration_number",
                    },
                    "transform_spec": {"type": "normalization_rule", "rule_id": "normalize_identifier_digits"},
                    "normalization_rule": {"rule_id": "normalize_identifier_digits"},
                }
            ],
            "output_bindings": [
                {
                    "source_field_id": "field_sales",
                    "field_path": "response.enpSaleAmt",
                    "direction": "output",
                    "canonical_ref": {
                        "class_name": "financial_fact",
                        "slot_name": "amount",
                    },
                    "transform_spec": {"type": "cast", "target_type": "decimal"},
                    "normalization_rule": {"rule_id": "parse_decimal"},
                }
            ],
            "skipped_bindings": [],
        }

        suggestion = suggest_capability_for_operation(context)

        self.assertEqual(suggestion["decision"], "propose_capability")
        self.assertEqual(suggestion["capability"]["capability_key"], "company.financial_fact.lookup")
        self.assertEqual(suggestion["operation_link"]["source_operation_id"], "op_1")
        self.assertEqual(suggestion["inputs"][0]["source_parameter_id"], "param_crno")
        self.assertEqual(suggestion["outputs"][0]["source_field_id"], "field_sales")
        self.assertEqual(suggestion["operation_link"]["binding_spec"]["outputs"][0]["normalization_rule"]["rule_id"], "parse_decimal")

    def test_operation_without_outputs_is_skipped(self) -> None:
        suggestion = suggest_capability_for_operation(
            {
                "operation": {"source_operation_id": "op_1", "name": "noop"},
                "input_bindings": [],
                "output_bindings": [],
                "skipped_bindings": [],
            }
        )

        self.assertEqual(suggestion["decision"], "skip_capability")

    def test_manual_response_overrides_capability(self) -> None:
        contexts = [
            {
                "operation": {"source_operation_id": "op_1", "name": "getFinancialStatement"},
                "input_bindings": [],
                "output_bindings": [
                    {
                        "source_field_id": "field_sales",
                        "canonical_ref": {"class_name": "financial_fact", "slot_name": "amount"},
                        "direction": "output",
                    }
                ],
                "skipped_bindings": [],
            }
        ]
        payload = {
            "suggestions": [
                {
                    "decision": "propose_capability",
                    "source_operation_id": "op_1",
                    "capability": {
                        "capability_key": "financial_statement.lookup",
                        "namespace": "public",
                        "name": "Financial Statement Lookup",
                        "description": "Lookup financial statement data.",
                        "intent_spec": {"canonical_outputs": []},
                    },
                    "inputs": [],
                    "outputs": [],
                    "operation_link": {"source_operation_id": "op_1", "priority": 100, "binding_spec": {}},
                    "confidence": 0.94,
                    "rationale": "Manual LLM decision.",
                }
            ]
        }

        suggestions = capability_suggestions_from_manual_response(contexts, payload)

        self.assertEqual(suggestions[0]["capability"]["capability_key"], "financial_statement.lookup")
        self.assertEqual(suggestions[0]["confidence"], 0.94)
        self.assertTrue(suggestions[0]["llm_decision"])

    def test_normalize_active_capability_contract_preserves_concept_and_representation_keys(self) -> None:
        payload = {
            "suggestions": [
                {
                    "decision": "propose_capability",
                    "source_operation_id": "op_1",
                    "capability": {
                        "capability_key": "company.finance.get_revenue",
                        "namespace": "company.finance",
                        "name": "Get Revenue",
                        "description": "Lookup company revenue.",
                        "intent_spec": {"canonical_outputs": ["concept.finance.revenue"]},
                    },
                    "inputs": [
                        {
                            "concept_key": "concept.identifier.kr_corporate_registration_number",
                            "representation_key": "repr.identifier.kr_corporate_registration_number.identifier_value",
                            "representation_schema_key": "schema.identifier.kr_corporate_registration_number.plain_13_digit",
                            "canonical_ref": {"class_name": "Identifier", "slot_name": "identifier_value"},
                            "source_parameter_id": "param_crno",
                            "required": True,
                        }
                    ],
                    "outputs": [
                        {
                            "output_key": "revenue_amount",
                            "concept_key": "concept.finance.revenue",
                            "representation_key": "repr.finance.revenue.observation_amount",
                            "representation_schema_key": "schema.finance.revenue.money_amount",
                            "canonical_ref": {"class_name": "Observation", "slot_name": "observed_amount"},
                            "source_field_id": "field_sales",
                        }
                    ],
                    "operation_link": {"source_operation_id": "op_1", "priority": 100, "binding_spec": {}},
                    "confidence": 0.95,
                    "rationale": "Revenue lookup.",
                }
            ]
        }

        normalized = normalize_manual_capability_generation_response(payload)

        self.assertEqual(normalized["suggestions"][0]["inputs"][0]["concept_key"], "concept.identifier.kr_corporate_registration_number")
        self.assertEqual(normalized["suggestions"][0]["inputs"][0]["representation_key"], "repr.identifier.kr_corporate_registration_number.identifier_value")
        self.assertEqual(normalized["suggestions"][0]["outputs"][0]["output_key"], "revenue_amount")
        self.assertEqual(normalized["suggestions"][0]["outputs"][0]["concept_key"], "concept.finance.revenue")
        self.assertEqual(normalized["suggestions"][0]["outputs"][0]["representation_schema_key"], "schema.finance.revenue.money_amount")

    def test_strict_llm_missing_capability_does_not_propose_fallback(self) -> None:
        contexts = [
            {
                "operation": {"source_operation_id": "op_1", "name": "getFinancialStatement"},
                "input_bindings": [],
                "output_bindings": [
                    {
                        "source_field_id": "field_sales",
                        "canonical_ref": {"class_name": "financial_fact", "slot_name": "amount"},
                        "direction": "output",
                    }
                ],
                "skipped_bindings": [],
            }
        ]

        suggestions = capability_suggestions_from_manual_response(
            contexts,
            {"suggestions": []},
            allow_heuristic_propose=False,
        )

        self.assertEqual(suggestions[0]["decision"], "skip_capability")
        self.assertEqual(suggestions[0]["capability"], {})

    def test_transport_output_class_does_not_create_record_lookup(self) -> None:
        suggestion = suggest_capability_for_operation(
            {
                "operation": {"source_operation_id": "op_1", "name": "getEnvelope"},
                "input_bindings": [],
                "output_bindings": [
                    {
                        "source_field_id": "field_msg",
                        "canonical_ref": {"class_name": "record", "slot_name": "result_msg"},
                        "direction": "output",
                    }
                ],
                "skipped_bindings": [],
            }
        )

        self.assertEqual(suggestion["decision"], "skip_capability")

    def test_manual_request_contains_linkml_context(self) -> None:
        request = build_manual_capability_generation_request(
            run_id="run_1",
            source={"id": "src_1"},
            document={"id": "doc_1"},
            operations=[],
            canonical_reconciliation={"linkml_fragment": {"classes": {}, "slots": {}}},
            binding_generation={"suggestions": []},
        )

        self.assertEqual(request["type"], "capability_generation")
        self.assertEqual(request["legacy_type"], "capability_contracting")
        self.assertIn("canonical_model_linkml_fragment", request)


if __name__ == "__main__":
    unittest.main()
