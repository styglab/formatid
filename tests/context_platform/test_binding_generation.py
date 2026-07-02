import unittest

from services.context_platform.internal.ingestion.binding_generation import build_manual_binding_generation_request
from services.context_platform.internal.ingestion.binding_generation import binding_suggestions_from_manual_response
from services.context_platform.internal.ingestion.binding_generation import collect_binding_terms
from services.context_platform.internal.ingestion.binding_generation import infer_transformation_spec
from services.context_platform.internal.ingestion.binding_generation import suggest_binding_for_term
from services.context_platform.internal.ingestion.llm.binding_generation import normalize_manual_binding_generation_response


class BindingGenerationTest(unittest.TestCase):
    def test_collect_terms_uses_canonical_reconciliation_target(self) -> None:
        source = {"id": "src_1"}
        document = {"id": "doc_1"}
        operations = [
            {
                "id": "op_1",
                "operation_key": "getFinancialStatement",
                "name": "getFinancialStatement",
                "method": "GET",
                "path": "/getFinancialStatement",
                "parameters": [],
                "fields": [
                    {
                        "id": "field_sales",
                        "field_path": "response.enpSaleAmt",
                        "raw_name": "enpSaleAmt",
                        "data_type": "number",
                        "description": "기업매출금액",
                    }
                ],
            }
        ]
        canonical = {
            "decisions": [
                {
                    "source_term": {
                        "source_kind": "field",
                        "source_field_id": "field_sales",
                        "field_path": "response.enpSaleAmt",
                        "direction": "output",
                    },
                    "matched_canonical_object": None,
                    "proposed_canonical": {
                        "class_name": "financial_fact",
                        "slot_name": "amount",
                        "datatype": "number",
                    },
                }
            ]
        }

        terms = collect_binding_terms(
            source=source,
            document=document,
            operations=operations,
            document_fields=[],
            canonical_reconciliation=canonical,
        )

        self.assertEqual(terms[0]["canonical"]["class_name"], "financial_fact")
        self.assertEqual(terms[0]["canonical"]["slot_name"], "amount")
        self.assertTrue(terms[0]["depends_on_canonical_decision"])

    def test_suggest_binding_includes_declarative_decimal_transform(self) -> None:
        term = {
            "source_kind": "field",
            "raw_name": "enpSaleAmt",
            "field_path": "response.enpSaleAmt",
            "description": "기업매출금액",
            "direction": "output",
            "data_type": "number",
            "canonical": {
                "class_name": "financial_fact",
                "slot_name": "amount",
                "datatype": "number",
            },
            "depends_on_canonical_decision": True,
            "evidence_refs": [],
        }

        suggestion = suggest_binding_for_term(term)

        self.assertEqual(suggestion["decision"], "bind")
        self.assertEqual(suggestion["binding_type"], "transform")
        self.assertEqual(suggestion["normalization_rule"]["rule_id"], "parse_decimal")
        self.assertEqual(suggestion["transform_spec"]["type"], "cast")

    def test_control_parameter_is_skip_binding_but_input_direction(self) -> None:
        term = {
            "source_kind": "parameter",
            "raw_name": "serviceKey",
            "field_path": "request.serviceKey",
            "source_direction": "control",
            "direction": "input",
            "data_type": "string",
            "canonical": {
                "class_name": "request_context",
                "slot_name": "service_key",
                "datatype": "string",
            },
            "evidence_refs": [],
        }

        suggestion = suggest_binding_for_term(term)

        self.assertEqual(suggestion["decision"], "skip_binding")
        self.assertEqual(suggestion["direction"], "input")
        self.assertEqual(suggestion["canonical_ref"], {"class_name": "", "slot_name": ""})

    def test_manual_response_overrides_transformation(self) -> None:
        terms = [
            {
                "source_kind": "field",
                "source_field_id": "field_date",
                "field_path": "response.basDt",
                "raw_name": "basDt",
                "direction": "output",
                "canonical": {
                    "class_name": "financial_statement",
                    "slot_name": "base_date",
                    "datatype": "date",
                },
                "evidence_refs": [],
            }
        ]
        payload = {
            "suggestions": [
                {
                    "source_kind": "field",
                    "source_field_id": "field_date",
                    "field_path": "response.basDt",
                    "raw_name": "basDt",
                    "decision": "bind",
                    "canonical_ref": {"class_name": "financial_statement", "slot_name": "base_date"},
                    "direction": "output",
                    "binding_type": "transform",
                    "transform_spec": {
                        "type": "normalization_rule",
                        "rule_id": "parse_yyyymmdd_date",
                        "params": {"output_format": "ISO_DATE"},
                    },
                    "normalization_rule": {"rule_id": "parse_yyyymmdd_date", "params": {"input_format": "YYYYMMDD"}},
                    "depends_on_canonical_decision": True,
                    "confidence": 0.93,
                    "rationale": "basDt is a base date.",
                }
            ]
        }

        suggestions = binding_suggestions_from_manual_response(terms, payload)

        self.assertEqual(suggestions[0]["normalization_rule"]["rule_id"], "parse_yyyymmdd_date")
        self.assertEqual(suggestions[0]["confidence"], 0.93)
        self.assertTrue(suggestions[0]["llm_decision"])

    def test_manual_binding_matches_operation_raw_name_when_paths_differ(self) -> None:
        terms = [
            {
                "source_kind": "parameter",
                "source_parameter_id": "param_b_no",
                "source_operation_id": "op_validate",
                "field_path": "request.body.businesses[].b_no",
                "raw_name": "b_no",
                "direction": "input",
                "operation": {"path": "/validate", "operation_key": "validate"},
                "canonical": {
                    "class_name": "BusinessRegistrationNumber",
                    "slot_name": "identifier_value",
                    "datatype": "string",
                },
                "evidence_refs": [],
            }
        ]
        payload = normalize_manual_binding_generation_response(
            {
                "parameter_bindings": [
                    {
                        "source_kind": "parameter",
                        "field_path": "request.b_no",
                        "raw_name": "b_no",
                        "operation_path": "/validate",
                        "decision": "bind",
                        "canonical_ref": {"class_name": "BusinessRegistrationNumber", "slot_name": "identifier_value"},
                        "direction": "input",
                        "binding_type": "transform",
                        "transform_spec": {"type": "normalization_rule", "rule_id": "normalize_identifier_digits"},
                        "normalization_rule": {},
                        "depends_on_canonical_decision": True,
                        "confidence": 0.92,
                        "rationale": "b_no is the business registration number input.",
                    }
                ]
            }
        )

        suggestions = binding_suggestions_from_manual_response(terms, payload, allow_heuristic_bind=False)

        self.assertEqual(suggestions[0]["decision"], "bind")
        self.assertEqual(suggestions[0]["source_parameter_id"], "param_b_no")
        self.assertEqual(suggestions[0]["field_path"], "request.body.businesses[].b_no")
        self.assertEqual(suggestions[0]["canonical_ref"]["slot_name"], "identifier_value")
        self.assertTrue(suggestions[0]["llm_decision"])

    def test_manual_skip_binding_does_not_keep_fallback_canonical_ref(self) -> None:
        terms = [
            {
                "source_kind": "parameter",
                "source_parameter_id": "param_result_type",
                "field_path": "request.resultType",
                "raw_name": "resultType",
                "direction": "input",
                "canonical": {
                    "class_name": "record",
                    "slot_name": "result_type",
                    "datatype": "string",
                },
                "evidence_refs": [],
            }
        ]
        payload = {
            "suggestions": [
                {
                    "source_kind": "parameter",
                    "field_path": "request.resultType",
                    "raw_name": "resultType",
                    "decision": "skip_binding",
                    "canonical_ref": {"class_name": "", "slot_name": ""},
                    "direction": "input",
                    "binding_type": "exact",
                    "transform_spec": {"type": "none", "params": {}},
                    "normalization_rule": {},
                    "depends_on_canonical_decision": False,
                    "confidence": 0.98,
                    "rationale": "Response format selector is provider control metadata.",
                }
            ]
        }

        suggestions = binding_suggestions_from_manual_response(terms, payload)

        self.assertEqual(suggestions[0]["decision"], "skip_binding")
        self.assertEqual(suggestions[0]["canonical_ref"], {"class_name": "", "slot_name": ""})
        self.assertIsNone(suggestions[0]["canonical_class_slot_id"])

    def test_strict_llm_missing_binding_does_not_bind_fallback(self) -> None:
        terms = [
            {
                "source_kind": "field",
                "source_field_id": "field_sales",
                "field_path": "response.enpSaleAmt",
                "raw_name": "enpSaleAmt",
                "direction": "output",
                "canonical": {
                    "class_name": "financial_fact",
                    "slot_name": "amount",
                    "datatype": "decimal",
                },
                "evidence_refs": [],
            }
        ]

        suggestions = binding_suggestions_from_manual_response(
            terms,
            {"suggestions": []},
            allow_heuristic_bind=False,
        )

        self.assertEqual(suggestions[0]["decision"], "conflict")
        self.assertEqual(suggestions[0]["canonical_ref"], {"class_name": "", "slot_name": ""})

    def test_canonical_conflict_blocks_manual_binding(self) -> None:
        terms = [
            {
                "source_kind": "parameter",
                "source_operation_id": "op_summary",
                "source_parameter_id": "param_year",
                "field_path": "request.사업연도",
                "raw_name": "사업연도",
                "direction": "input",
                "canonical_decision": {"decision": "conflict"},
                "canonical": {
                    "class_name": "Observation",
                    "slot_name": "covers_period",
                    "datatype": "string",
                },
                "evidence_refs": [],
            }
        ]
        payload = {
            "suggestions": [
                {
                    "source_kind": "parameter",
                    "source_operation_id": "op_summary",
                    "source_parameter_id": "param_year",
                    "field_path": "request.사업연도",
                    "raw_name": "사업연도",
                    "decision": "bind",
                    "canonical_ref": {"class_name": "Observation", "slot_name": "covers_period"},
                    "direction": "input",
                    "binding_type": "exact",
                    "confidence": 0.9,
                    "rationale": "Attempt to bind unresolved canonical term.",
                }
            ]
        }

        suggestions = binding_suggestions_from_manual_response(terms, payload)

        self.assertEqual(suggestions[0]["decision"], "conflict")
        self.assertEqual(suggestions[0]["canonical_ref"], {"class_name": "", "slot_name": ""})
        self.assertFalse(suggestions[0].get("llm_decision", False))

    def test_manual_request_includes_rule_catalog(self) -> None:
        request = build_manual_binding_generation_request(
            run_id="run_1",
            source={"id": "src_1"},
            document={"id": "doc_1"},
            operations=[],
            document_fields=[],
            canonical_reconciliation={"decisions": []},
        )

        self.assertEqual(request["type"], "resolution_generation")
        self.assertEqual(request["legacy_type"], "binding_generation")
        self.assertTrue(any(item["rule_id"] == "parse_decimal" for item in request["approved_rule_catalog"]))

    def test_manual_request_instructs_subject_context_and_code_domain_preservation(self) -> None:
        request = build_manual_binding_generation_request(
            run_id="run_1",
            source={"id": "src_1"},
            document={"id": "doc_1"},
            operations=[],
            document_fields=[],
            canonical_reconciliation={"decisions": []},
        )
        instructions = "\n".join(request["instructions"])

        self.assertIn("subject_identifier", instructions)
        self.assertIn("schema.*.code", instructions)
        self.assertIn("subject_identifier", request["response_contract"]["context_bindings"][0]["context_key"])
        self.assertIn("value_domain_key", request["response_contract"]["context_bindings"][0])

    def test_normalize_active_resolution_generation_contract_combines_binding_types(self) -> None:
        payload = {
            "field_bindings": [
                {
                    "source_kind": "field",
                    "source_field_id": "field_sales",
                    "field_path": "response.enpSaleAmt",
                    "raw_name": "enpSaleAmt",
                    "decision": "bind",
                    "representation_key": "repr.finance.revenue.observation_amount",
                    "representation_schema_key": "schema.finance.revenue.money_amount",
                    "concept_key": "concept.finance.revenue",
                    "fills_property": "property.observed_amount",
                    "canonical_ref": {"class_name": "Observation", "slot_name": "observed_amount"},
                    "direction": "output",
                    "binding_type": "transform",
                    "transform_spec": {"type": "cast", "rule_id": "parse_decimal", "params": {}},
                    "confidence": 0.97,
                    "rationale": "Revenue amount field.",
                }
            ],
            "context_bindings": [
                {
                    "source_kind": "field",
                    "source_field_id": "field_cur",
                    "field_path": "response.curCd",
                    "raw_name": "curCd",
                    "decision": "bind",
                    "representation_key": "repr.finance.revenue.observation_amount",
                    "concept_key": "concept.currency",
                    "context_key": "currency",
                    "direction": "output",
                    "binding_type": "exact",
                    "confidence": 0.9,
                    "rationale": "Currency context.",
                }
            ],
            "parameter_bindings": [
                {
                    "source_kind": "parameter",
                    "source_parameter_id": "param_crno",
                    "field_path": "request.crno",
                    "raw_name": "crno",
                    "decision": "bind",
                    "required_concept_key": "concept.identifier.kr_corporate_registration_number",
                    "representation_key": "repr.identifier.kr_corporate_registration_number.identifier_value",
                    "direction": "input",
                    "binding_type": "transform",
                    "confidence": 0.9,
                    "rationale": "Corporate registration number input.",
                }
            ],
            "transform_rules": [{"rule_id": "parse_decimal", "rule_type": "parse"}],
        }

        normalized = normalize_manual_binding_generation_response(payload)

        self.assertEqual(len(normalized["suggestions"]), 3)
        self.assertEqual([item["binding_kind"] for item in normalized["suggestions"]], ["field", "context", "parameter"])
        self.assertEqual(normalized["suggestions"][0]["representation_key"], "repr.finance.revenue.observation_amount")
        self.assertEqual(normalized["suggestions"][0]["fills_property"], "property.observed_amount")
        self.assertEqual(normalized["context_bindings"][0]["context_key"], "currency")
        self.assertEqual(normalized["parameter_bindings"][0]["required_concept_key"], "concept.identifier.kr_corporate_registration_number")
        self.assertEqual(normalized["transform_rules"][0]["rule_id"], "parse_decimal")

    def test_infer_identifier_normalization(self) -> None:
        transform, normalization, binding_type = infer_transformation_spec(
            {"raw_name": "crno", "field_path": "request.crno", "description": "법인등록번호", "data_type": "string"},
            {"slot_name": "corporate_registration_number", "datatype": "string"},
        )

        self.assertEqual(binding_type, "transform")
        self.assertEqual(transform["rule_id"], "normalize_identifier_digits")
        self.assertEqual(normalization["rule_id"], "normalize_identifier_digits")


if __name__ == "__main__":
    unittest.main()
