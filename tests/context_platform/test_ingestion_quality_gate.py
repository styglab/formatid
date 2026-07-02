from __future__ import annotations

import unittest

from services.context_platform.internal.ingestion.quality_gate import evaluate_ingestion_quality


class IngestionQualityGateTest(unittest.TestCase):
    def test_quality_gate_generates_preview_for_verified_capability(self) -> None:
        result = evaluate_ingestion_quality(
            operations=[{"id": "op_1", "method": "GET", "path": "/summary"}],
            document_fields=[],
            canonical_reconciliation={
                "decisions": [
                    {
                        "decision": "create",
                        "field_path": "response.body.items.item.enpSaleAmt",
                        "concept_key": "concept.finance.revenue",
                        "representation_key": "repr.finance.revenue.observed_amount",
                        "representation_schema_key": "schema.finance.revenue.decimal",
                    }
                ]
            },
            binding_generation={
                "suggestions": [
                    {
                        "decision": "bind",
                        "binding_kind": "field",
                        "direction": "output",
                        "field_path": "response.body.items.item.enpSaleAmt",
                        "concept_key": "concept.finance.revenue",
                        "representation_key": "repr.finance.revenue.observed_amount",
                        "representation_schema_key": "schema.finance.revenue.decimal",
                    }
                ]
            },
            capability_generation={
                "suggestions": [
                    {
                        "decision": "propose_capability",
                        "source_operation_id": "op_1",
                        "capability": {
                            "capability_key": "company.finance.get_revenue",
                            "provides_concepts": ["concept.finance.revenue"],
                            "intent_spec": {"canonical_outputs": ["concept.finance.revenue"]},
                        },
                        "inputs": [
                            {
                                "concept_key": "concept.identifier.kr_corporate_registration_number",
                                "representation_key": "repr.identifier.kr_corporate_registration_number.identifier_value",
                                "representation_schema_key": "schema.identifier.kr_corporate_registration_number.string",
                                "canonical_ref": {"class_name": "Identifier", "slot_name": "identifier_value"},
                            }
                        ],
                        "outputs": [
                            {
                                "output_key": "revenue_amount",
                                "concept_key": "concept.finance.revenue",
                                "representation_key": "repr.finance.revenue.observed_amount",
                                "representation_schema_key": "schema.finance.revenue.decimal",
                                "canonical_ref": {"class_name": "Observation", "slot_name": "observed_amount"},
                            }
                        ],
                        "operation_link": {
                            "source_operation_id": "op_1",
                            "binding_spec": {
                                "outputs": [
                                    {
                                        "field_path": "response.body.items.item.enpSaleAmt",
                                        "concept_key": "concept.finance.revenue",
                                        "representation_key": "repr.finance.revenue.observed_amount",
                                        "representation_schema_key": "schema.finance.revenue.decimal",
                                        "normalization_rule": {"rule_id": "parse_decimal"},
                                    }
                                ],
                                "contexts": [
                                    {
                                        "field_path": "response.body.items.item.crno",
                                        "context_key": "subject_identifier",
                                        "concept_key": "concept.identifier.kr_corporate_registration_number",
                                        "representation_key": "repr.identifier.kr_corporate_registration_number.identifier_value",
                                        "representation_schema_key": "schema.identifier.kr_corporate_registration_number.string",
                                    },
                                    {
                                        "field_path": "response.body.items.item.curCd",
                                        "context_key": "currency",
                                        "concept_key": "concept.currency.code",
                                        "representation_key": "repr.currency.code.currency_code",
                                        "representation_schema_key": "schema.currency.code.string",
                                    }
                                ],
                            },
                        },
                    }
                ]
            },
            verification_result={
                "summary": {"total": 2, "verified": 2, "failed": 0, "skipped": 0, "needs_input": 0},
                "operation_checks": [
                    {
                        "source_operation_id": "op_1",
                        "check_type": "operation",
                        "status": "verified",
                        "http_status": 200,
                        "response_sample_ref": {
                        "content_type": "application/json",
                            "body_preview": '{"response":{"body":{"items":{"item":[{"enpSaleAmt":"12345","curCd":"KRW"}]}}}}',
                        },
                        "field_coverage": {"matched_output_paths": ["response.body.items.item.enpSaleAmt"]},
                        "binding_validation": {},
                    }
                ],
                "capability_checks": [
                    {
                        "source_operation_id": "op_1",
                        "capability_key": "company.finance.get_revenue",
                        "check_type": "capability",
                        "status": "verified",
                        "binding_validation": {},
                    }
                ],
            },
        )

        self.assertEqual(result["quality_status"], "approval_ready")
        self.assertTrue(result["publishable"])
        self.assertEqual(result["normalization_previews"][0]["preview_status"], "generated")
        self.assertEqual(result["normalization_previews"][0]["outputs"][0]["normalized_value"], "12345")

    def test_quality_gate_requires_namespace_scope_consistency(self) -> None:
        result = evaluate_ingestion_quality(
            operations=[],
            document_fields=[{"field_path": "request.bizYear"}],
            canonical_reconciliation={
                "decisions": [
                    {
                        "decision": "create",
                        "field_path": "request.bizYear",
                        "concept_key": "concept.time.fiscal_year",
                        "concept": {"stable_key": "concept.time.fiscal_year", "meaning_scope": "finance"},
                        "representation_key": "repr.time.fiscal_year.observed_value",
                        "representation_schema_key": "schema.time.fiscal_year.string",
                    }
                ]
            },
            binding_generation={"suggestions": []},
            capability_generation={"suggestions": []},
            verification_result={"summary": {"total": 0, "verified": 0, "failed": 0, "skipped": 0, "needs_input": 0}},
        )

        self.assertEqual(result["quality_status"], "review_required")
        self.assertIn("concept_scope_mismatch", [issue["code"] for gate in result["gates"] for issue in gate["issues"]])

    def test_quality_gate_requires_value_domain_for_code_schema(self) -> None:
        result = evaluate_ingestion_quality(
            operations=[],
            document_fields=[{"field_path": "response.body.data[].tax_type_cd"}],
            canonical_reconciliation={
                "decisions": [
                    {
                        "decision": "create",
                        "field_path": "response.body.data[].tax_type_cd",
                        "concept_key": "concept.tax.tax_type",
                        "representation_key": "repr.tax.tax_type.observed_value",
                        "representation_schema_key": "schema.tax.tax_type.code",
                    }
                ],
                "value_domain_decisions": [],
            },
            binding_generation={
                "suggestions": [
                    {
                        "decision": "bind",
                        "binding_kind": "field",
                        "direction": "output",
                        "field_path": "response.body.data[].tax_type_cd",
                        "concept_key": "concept.tax.tax_type",
                        "representation_key": "repr.tax.tax_type.observed_value",
                        "representation_schema_key": "schema.tax.tax_type.code",
                    }
                ]
            },
            capability_generation={"suggestions": []},
            verification_result={"summary": {"total": 0, "verified": 0, "failed": 0, "skipped": 0, "needs_input": 0}},
        )

        self.assertEqual(result["quality_status"], "review_required")
        self.assertIn("code_schema_missing_value_domain", [issue["code"] for gate in result["gates"] for issue in gate["issues"]])

    def test_quality_gate_requires_subject_context_for_repeated_identifier_outputs(self) -> None:
        result = evaluate_ingestion_quality(
            operations=[{"id": "op_1", "method": "POST", "path": "/status"}],
            document_fields=[],
            canonical_reconciliation={"decisions": [], "value_domain_decisions": []},
            binding_generation={"suggestions": []},
            capability_generation={
                "suggestions": [
                    {
                        "decision": "propose_capability",
                        "source_operation_id": "op_1",
                        "capability": {"capability_key": "company.tax.status", "provides_concepts": ["concept.tax.business_registration_status"]},
                        "inputs": [
                            {
                                "concept_key": "concept.identifier.kr_business_registration_number",
                                "representation_key": "repr.identifier.kr_business_registration_number.identifier_value",
                                "representation_schema_key": "schema.identifier.kr_business_registration_number.string",
                                "canonical_ref": {"class_name": "Identifier", "slot_name": "identifier_value"},
                            }
                        ],
                        "outputs": [
                            {
                                "output_key": "status",
                                "concept_key": "concept.tax.business_registration_status",
                                "representation_key": "repr.tax.business_registration_status.observed_value",
                                "representation_schema_key": "schema.tax.business_registration_status.string",
                                "canonical_ref": {"class_name": "Observation", "slot_name": "observed_value"},
                            }
                        ],
                        "operation_link": {
                            "source_operation_id": "op_1",
                            "binding_spec": {
                                "inputs": [
                                    {
                                        "concept_key": "concept.identifier.kr_business_registration_number",
                                        "representation_key": "repr.identifier.kr_business_registration_number.identifier_value",
                                        "representation_schema_key": "schema.identifier.kr_business_registration_number.string",
                                    }
                                ],
                                "outputs": [
                                    {
                                        "field_path": "response.body.data[].b_stt_cd",
                                        "concept_key": "concept.tax.business_registration_status",
                                        "representation_key": "repr.tax.business_registration_status.observed_value",
                                        "representation_schema_key": "schema.tax.business_registration_status.string",
                                    }
                                ],
                                "contexts": [],
                            },
                        },
                    }
                ]
            },
            verification_result={
                "summary": {"total": 1, "verified": 1, "failed": 0, "skipped": 0, "needs_input": 0},
                "operation_checks": [
                    {
                        "source_operation_id": "op_1",
                        "status": "verified",
                        "response_sample_ref": {"body_preview": '{"response":{"body":{"data":[{"b_stt_cd":"01"}]}}}'},
                        "binding_validation": {},
                    }
                ],
            },
        )

        self.assertEqual(result["quality_status"], "review_required")
        self.assertIn("repeated_output_missing_subject_context", [issue["code"] for gate in result["gates"] for issue in gate["issues"]])

    def test_transient_timeout_keeps_proposal_reviewable_not_publishable(self) -> None:
        result = evaluate_ingestion_quality(
            operations=[{"id": "op_1", "method": "GET", "path": "/summary"}],
            document_fields=[],
            canonical_reconciliation={"decisions": []},
            binding_generation={
                "suggestions": [
                    {
                        "decision": "bind",
                        "binding_kind": "parameter",
                        "direction": "input",
                        "field_path": "request.crno",
                        "concept_key": "concept.identifier.kr_corporate_registration_number",
                        "required_concept_key": "concept.identifier.kr_corporate_registration_number",
                        "representation_key": "repr.identifier.kr_corporate_registration_number.identifier_value",
                        "representation_schema_key": "schema.identifier.kr_corporate_registration_number.string",
                    },
                    {
                        "decision": "bind",
                        "binding_kind": "field",
                        "direction": "output",
                        "field_path": "response.body.items.item.enpSaleAmt",
                        "concept_key": "concept.finance.revenue",
                        "representation_key": "repr.finance.revenue.observed_amount",
                        "representation_schema_key": "schema.finance.revenue.decimal",
                    },
                ]
            },
            capability_generation={
                "suggestions": [
                    {
                        "decision": "propose_capability",
                        "source_operation_id": "op_1",
                        "capability": {
                            "capability_key": "company.finance.get_revenue",
                            "provides_concepts": ["concept.finance.revenue"],
                        },
                        "inputs": [
                            {
                                "concept_key": "concept.identifier.kr_corporate_registration_number",
                                "representation_key": "repr.identifier.kr_corporate_registration_number.identifier_value",
                                "representation_schema_key": "schema.identifier.kr_corporate_registration_number.string",
                            }
                        ],
                        "outputs": [
                            {
                                "output_key": "revenue_amount",
                                "concept_key": "concept.finance.revenue",
                                "representation_key": "repr.finance.revenue.observed_amount",
                                "representation_schema_key": "schema.finance.revenue.decimal",
                            }
                        ],
                        "operation_link": {
                            "source_operation_id": "op_1",
                            "binding_spec": {
                                "inputs": [
                                    {
                                        "concept_key": "concept.identifier.kr_corporate_registration_number",
                                        "representation_key": "repr.identifier.kr_corporate_registration_number.identifier_value",
                                        "representation_schema_key": "schema.identifier.kr_corporate_registration_number.string",
                                    }
                                ],
                                "outputs": [
                                    {
                                        "field_path": "response.body.items.item.enpSaleAmt",
                                        "concept_key": "concept.finance.revenue",
                                        "representation_key": "repr.finance.revenue.observed_amount",
                                        "representation_schema_key": "schema.finance.revenue.decimal",
                                    }
                                ],
                            },
                        },
                    }
                ]
            },
            verification_result={
                "summary": {"total": 2, "verified": 0, "failed": 2, "skipped": 0, "needs_input": 0},
                "operation_checks": [
                    {
                        "source_operation_id": "op_1",
                        "status": "failed",
                        "binding_validation": {"error_category": "transient_timeout", "transient": True},
                    }
                ],
                "capability_checks": [
                    {
                        "source_operation_id": "op_1",
                        "status": "failed",
                        "binding_validation": {"error_category": "transient_timeout", "transient": True},
                    }
                ],
            },
        )

        self.assertEqual(result["quality_status"], "review_required")
        self.assertFalse(result["publishable"])
        self.assertIn("transient_timeout", result["observed_evidence"]["error_categories"])


if __name__ == "__main__":
    unittest.main()
