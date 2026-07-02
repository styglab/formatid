import unittest

from services.context_platform.internal.ingestion.canonical_reconciliation import collect_source_terms
from services.context_platform.internal.ingestion.canonical_reconciliation import build_manual_canonical_reconciliation_request
from services.context_platform.internal.ingestion.canonical_reconciliation import build_linkml_fragment
from services.context_platform.internal.ingestion.canonical_reconciliation import build_linkml_fragment_from_decisions
from services.context_platform.internal.ingestion.canonical_reconciliation import reconcile_source_term
from services.context_platform.internal.ingestion.canonical_reconciliation import relation_suggestions_from_manual_response
from services.context_platform.internal.ingestion.canonical_reconciliation import reconcile_terms_from_manual_response
from services.context_platform.internal.ingestion.llm.canonical_reconciliation import normalize_manual_canonical_reconciliation_response


class CanonicalReconciliationTest(unittest.TestCase):
    def test_reconcile_reuses_strong_existing_attribute(self) -> None:
        term = {
            "source_kind": "field",
            "raw_name": "amount",
            "field_path": "response.items.amount",
            "description": "Financial fact amount",
            "data_type": "number",
            "direction": "output",
            "evidence_refs": [],
        }
        context = {
            "classes": [{"id": "cclass_financial_fact", "name": "financial_fact"}],
            "slots": [],
            "class_slot_usages": [
                {
                    "id": "ca_sales_amount",
                    "class_id": "cclass_financial_fact",
                    "class_name": "financial_fact",
                    "name": "amount",
                    "description": "Financial fact amount",
                    "status": "approved",
                }
            ],
        }

        decision = reconcile_source_term(term, context)

        self.assertEqual(decision["decision"], "reuse")
        self.assertEqual(decision["matched_canonical_object"]["id"], "ca_sales_amount")
        self.assertFalse(decision["requires_review"])

    def test_control_parameter_is_skipped_from_canonical_model(self) -> None:
        term = {
            "source_kind": "parameter",
            "raw_name": "serviceKey",
            "field_path": "request.serviceKey",
            "description": "Service key",
            "data_type": "string",
            "direction": "control",
            "evidence_refs": [],
        }

        decision = reconcile_source_term(term, {"classes": [], "slots": [], "class_slot_usages": []})

        self.assertEqual(decision["decision"], "skip")
        self.assertEqual(decision["proposed_canonical"]["class_name"], "")
        self.assertEqual(decision["linkml_fragment"]["classes"], {})

    def test_response_envelope_field_is_skipped_from_canonical_model(self) -> None:
        term = {
            "source_kind": "field",
            "raw_name": "resultCode",
            "field_path": "response.header.resultCode",
            "description": "API result code",
            "data_type": "string",
            "direction": "output",
            "evidence_refs": [],
        }

        decision = reconcile_source_term(term, {"classes": [], "slots": [], "class_slot_usages": []})

        self.assertEqual(decision["decision"], "skip")

    def test_collect_source_terms_includes_parameters_and_fields(self) -> None:
        source = {"id": "src_1"}
        document = {"id": "doc_1"}
        operations = [
            {
                "id": "op_1",
                "operation_key": "getThing",
                "name": "getThing",
                "method": "GET",
                "path": "/thing",
                "parameters": [
                    {
                        "id": "param_1",
                        "name": "serviceKey",
                        "data_type": "string",
                        "is_required": True,
                        "description": "Service key",
                    }
                ],
                "fields": [
                    {
                        "id": "field_1",
                        "field_path": "response.name",
                        "raw_name": "name",
                        "data_type": "string",
                        "description": "Name",
                        "direction": "output",
                    }
                ],
            }
        ]

        terms = collect_source_terms(source=source, document=document, operations=operations, document_fields=[])

        self.assertEqual(
            [(item["source_kind"], item["raw_name"], item["direction"]) for item in terms],
            [
                ("parameter", "serviceKey", "control"),
                ("field", "name", "output"),
            ],
        )

    def test_manual_response_sets_business_canonical_class_slot_usage(self) -> None:
        terms = [
            {
                "source_kind": "field",
                "source_field_id": "field_sales",
                "source_parameter_id": None,
                "field_path": "response.body.items.item.enpSaleAmt",
                "raw_name": "enpSaleAmt",
                "description": "기업매출금액",
                "data_type": "number",
                "direction": "output",
                "evidence_refs": [],
            }
        ]
        payload = {
            "decisions": [
                {
                    "source_kind": "field",
                    "source_field_id": None,
                    "field_path": "response.body.items.item.enpSaleAmt",
                    "raw_name": "enpSaleAmt",
                    "decision": "create",
                    "proposed_canonical": {
                        "class_name": "financial_fact",
                        "slot_name": "amount",
                        "datatype": "number",
                        "description": "Sales amount reported on a financial statement.",
                        "aliases": ["enpSaleAmt", "기업매출금액"],
                        "identity_role": "measure",
                    },
                    "confidence": 0.91,
                    "rationale": "The Korean description means company sales amount.",
                }
            ]
        }

        decisions = reconcile_terms_from_manual_response(terms, {"classes": [], "slots": [], "class_slot_usages": []}, payload)

        self.assertEqual(decisions[0]["proposed_canonical"]["class_name"], "financial_fact")
        self.assertEqual(decisions[0]["proposed_canonical"]["slot_name"], "amount")
        self.assertEqual(decisions[0]["confidence"], 0.91)
        self.assertTrue(decisions[0]["llm_decision"])

    def test_normalize_active_meaning_resolution_contract_preserves_representation_schema(self) -> None:
        payload = {
            "concept_decisions": [
                {
                    "source_kind": "field",
                    "source_field_id": "field_sales",
                    "source_operation_id": "op_summary",
                    "source_operation_key": "op.data_go_kr.fsc.get_summ_fina_stat_v2",
                    "operation_key": "get_summ_fina_stat_v2",
                    "operation_name": "getSummFinaStat_V2",
                    "operation_path": "/GetFinaStatInfoService_V2/getSummFinaStat_V2",
                    "field_path": "response.enpSaleAmt",
                    "raw_name": "enpSaleAmt",
                    "decision": "create",
                    "concept_key": "concept.finance.revenue",
                    "concept": {
                        "stable_key": "concept.finance.revenue",
                        "kind": "metric_concept",
                        "meaning_scope": "finance",
                        "label_ko": "매출액",
                    },
                    "canonical_representation": {
                        "stable_key": "repr.finance.revenue.observation_amount",
                        "carrier_object_type": "Observation",
                        "value_property": "observed_amount",
                    },
                    "representation_schema": {
                        "stable_key": "schema.finance.revenue.money_amount",
                        "datatype": "decimal",
                        "minimum": 0,
                        "unit_concept_key": "concept.currency.krw",
                    },
                    "confidence": 0.97,
                    "rationale": "기업매출금액 means revenue.",
                }
            ],
            "representation_decisions": [{"representation_key": "repr.finance.revenue.observation_amount"}],
            "representation_schema_decisions": [{"representation_schema_key": "schema.finance.revenue.money_amount"}],
        }

        normalized = normalize_manual_canonical_reconciliation_response(payload)

        self.assertEqual(normalized["decisions"][0]["concept_key"], "concept.finance.revenue")
        self.assertEqual(normalized["decisions"][0]["source_operation_id"], "op_summary")
        self.assertEqual(normalized["decisions"][0]["source_operation_key"], "op.data_go_kr.fsc.get_summ_fina_stat_v2")
        self.assertEqual(normalized["decisions"][0]["operation_key"], "get_summ_fina_stat_v2")
        self.assertEqual(normalized["decisions"][0]["operation_name"], "getSummFinaStat_V2")
        self.assertEqual(
            normalized["decisions"][0]["operation_path"],
            "/GetFinaStatInfoService_V2/getSummFinaStat_V2",
        )
        self.assertEqual(normalized["decisions"][0]["representation_key"], "repr.finance.revenue.observation_amount")
        self.assertEqual(normalized["decisions"][0]["representation_schema_key"], "schema.finance.revenue.money_amount")
        self.assertEqual(normalized["decisions"][0]["proposed_canonical"]["class_name"], "Observation")
        self.assertEqual(normalized["decisions"][0]["proposed_canonical"]["slot_name"], "observed_amount")
        self.assertEqual(normalized["decisions"][0]["proposed_canonical"]["datatype"], "decimal")
        self.assertEqual(normalized["concept_decisions"][0]["concept_key"], "concept.finance.revenue")
        self.assertEqual(normalized["representation_schema_decisions"][0]["representation_schema_key"], "schema.finance.revenue.money_amount")

    def test_manual_request_instructs_scope_and_value_domain_invariants(self) -> None:
        request = build_manual_canonical_reconciliation_request(
            run_id="run_1",
            source={"id": "src_1"},
            document={"id": "doc_1"},
            operations=[],
            document_fields=[],
            context={"classes": [], "slots": [], "class_slot_usages": [], "relations": []},
        )
        instructions = "\n".join(request["instructions"])

        self.assertIn("concept.identifier.* -> identifier", instructions)
        self.assertIn("schema.*.code", instructions)
        self.assertIn("meaning_scope_policy", request["modeling_contract"])
        self.assertIn("value_domain_key", request["response_contract"]["representation_schema_decisions"][0]["representation_schema"])

    def test_manual_skip_does_not_keep_fallback_canonical_class(self) -> None:
        terms = [
            {
                "source_kind": "parameter",
                "source_parameter_id": "param_result_type",
                "source_field_id": None,
                "field_path": "request.resultType",
                "raw_name": "resultType",
                "description": "Response format selector",
                "data_type": "string",
                "direction": "input",
                "evidence_refs": [],
            }
        ]
        payload = {
            "decisions": [
                {
                    "source_kind": "parameter",
                    "field_path": "request.resultType",
                    "raw_name": "resultType",
                    "decision": "skip",
                    "proposed_canonical": {
                        "class_name": "",
                        "slot_name": "",
                        "datatype": "string",
                        "description": "",
                        "aliases": ["resultType"],
                        "identity_role": "transport",
                    },
                    "confidence": 0.98,
                    "rationale": "Response format selector is provider control metadata.",
                }
            ]
        }

        decisions = reconcile_terms_from_manual_response(terms, {"classes": [], "slots": [], "class_slot_usages": []}, payload)

        self.assertEqual(decisions[0]["decision"], "skip")
        self.assertEqual(decisions[0]["proposed_canonical"]["class_name"], "")
        self.assertEqual(decisions[0]["proposed_canonical"]["slot_name"], "")
        self.assertEqual(decisions[0]["linkml_fragment"]["classes"], {})

    def test_strict_llm_missing_decision_does_not_create_fallback_record(self) -> None:
        terms = [
            {
                "source_kind": "field",
                "source_field_id": "field_sales",
                "field_path": "response.enpSaleAmt",
                "raw_name": "enpSaleAmt",
                "description": "기업매출금액",
                "data_type": "number",
                "direction": "output",
                "evidence_refs": [],
            }
        ]

        decisions = reconcile_terms_from_manual_response(
            terms,
            {"classes": [], "slots": [], "class_slot_usages": []},
            {"decisions": []},
            allow_heuristic_create=False,
        )

        self.assertEqual(decisions[0]["decision"], "conflict")
        self.assertEqual(decisions[0]["proposed_canonical"]["class_name"], "")
        self.assertEqual(decisions[0]["linkml_fragment"]["classes"], {})

    def test_manual_path_only_decision_does_not_apply_to_duplicate_operation_terms(self) -> None:
        terms = [
            {
                "source_kind": "parameter",
                "source_operation_id": "op_summary",
                "source_parameter_id": "param_summary_year",
                "field_path": "request.사업연도",
                "raw_name": "사업연도",
                "description": "재무정보 조회 대상 사업연도",
                "data_type": "string",
                "direction": "input",
                "evidence_refs": [],
            },
            {
                "source_kind": "parameter",
                "source_operation_id": "op_balance",
                "source_parameter_id": "param_balance_year",
                "field_path": "request.사업연도",
                "raw_name": "사업연도",
                "description": "재무정보 조회 대상 사업연도",
                "data_type": "string",
                "direction": "input",
                "evidence_refs": [],
            },
        ]
        payload = {
            "decisions": [
                {
                    "source_kind": "parameter",
                    "field_path": "request.사업연도",
                    "raw_name": "사업연도",
                    "decision": "reuse",
                    "canonical_class_slot_id": "cclassslot_foundation_observation_covers_period",
                    "proposed_canonical": {
                        "class_name": "Observation",
                        "slot_name": "covers_period",
                        "datatype": "string",
                        "description": "재무정보 조회 대상 사업연도",
                        "aliases": ["사업연도"],
                    },
                    "confidence": 0.9,
                    "rationale": "Generic path-only response.",
                }
            ]
        }

        decisions = reconcile_terms_from_manual_response(
            terms,
            {"classes": [], "slots": [], "class_slot_usages": []},
            payload,
            allow_heuristic_create=False,
        )

        self.assertEqual([item["decision"] for item in decisions], ["conflict", "conflict"])
        self.assertFalse(any(item.get("llm_decision") for item in decisions))

    def test_manual_decision_matches_operation_raw_name_when_paths_differ(self) -> None:
        terms = [
            {
                "source_kind": "parameter",
                "source_operation_id": "op_summary",
                "source_parameter_id": "param_crno",
                "field_path": "request.crno",
                "raw_name": "crno",
                "description": "법인등록번호",
                "data_type": "string",
                "direction": "input",
                "operation": {"path": "/getSummFinaStat_V2", "operation_key": "get_summ_fina_stat_v2"},
                "evidence_refs": [],
            }
        ]
        payload = {
            "decisions": [
                {
                    "source_kind": "parameter",
                    "field_path": "request.query.crno",
                    "raw_name": "crno",
                    "operation_path": "/getSummFinaStat_V2",
                    "decision": "create",
                    "proposed_canonical": {
                        "class_name": "Identifier",
                        "slot_name": "identifier_value",
                        "datatype": "string",
                        "description": "법인등록번호",
                    },
                    "confidence": 0.92,
                    "rationale": "crno is the corporate registration number.",
                }
            ]
        }

        decisions = reconcile_terms_from_manual_response(
            terms,
            {"classes": [], "slots": [], "class_slot_usages": []},
            payload,
            allow_heuristic_create=False,
        )

        self.assertEqual(decisions[0]["decision"], "create")
        self.assertEqual(decisions[0]["proposed_canonical"]["slot_name"], "identifier_value")
        self.assertTrue(decisions[0]["llm_decision"])

    def test_strict_llm_transport_class_decision_is_blocked(self) -> None:
        terms = [
            {
                "source_kind": "field",
                "source_field_id": "field_value",
                "field_path": "response.value",
                "raw_name": "value",
                "description": "Payload value.",
                "data_type": "string",
                "direction": "output",
                "evidence_refs": [],
            }
        ]
        payload = {
            "decisions": [
                {
                    "source_kind": "field",
                    "source_field_id": "field_value",
                    "field_path": "response.value",
                    "raw_name": "value",
                    "decision": "create",
                    "proposed_canonical": {
                        "class_name": "record",
                        "slot_name": "value",
                        "datatype": "string",
                        "description": "Payload value.",
                        "aliases": ["value"],
                        "identity_role": "",
                    },
                    "confidence": 0.8,
                    "rationale": "Generic response record.",
                }
            ]
        }

        decisions = reconcile_terms_from_manual_response(
            terms,
            {"classes": [], "slots": [], "class_slot_usages": []},
            payload,
            allow_heuristic_create=False,
        )

        self.assertEqual(decisions[0]["decision"], "conflict")
        self.assertEqual(decisions[0]["proposed_canonical"]["class_name"], "")
        self.assertEqual(decisions[0]["linkml_fragment"]["classes"], {})

    def test_heuristic_fallback_does_not_infer_business_class_from_domain_keywords(self) -> None:
        term = {
            "source_kind": "field",
            "raw_name": "enpSaleAmt",
            "field_path": "response.enpSaleAmt",
            "description": "기업매출금액",
            "data_type": "number",
            "direction": "output",
            "evidence_refs": [],
        }

        decision = reconcile_source_term(term, {"classes": [], "slots": [], "class_slot_usages": []})

        fragment = decision["linkml_fragment"]
        self.assertIn("Record", fragment["classes"])
        self.assertIn("enp_sale_amt", fragment["slots"])
        self.assertEqual(fragment["slots"]["enp_sale_amt"]["range"], "decimal")

    def test_build_linkml_fragment_merges_decision_slots(self) -> None:
        decisions = [
            {
                "decision": "create",
                "source_term": {
                    "source_kind": "field",
                    "source_field_id": "field_sales",
                    "field_path": "response.enpSaleAmt",
                    "raw_name": "enpSaleAmt",
                    "direction": "output",
                },
                "proposed_canonical": {
                    "class_name": "financial_fact",
                    "slot_name": "amount",
                    "datatype": "number",
                    "description": "Sales amount reported on a financial statement.",
                    "aliases": ["enpSaleAmt", "기업매출금액"],
                    "identity_role": "measure",
                },
            }
        ]

        fragment = build_linkml_fragment_from_decisions(decisions)

        self.assertIn("FinancialFact", fragment["classes"])
        self.assertEqual(fragment["classes"]["FinancialFact"]["slots"], ["amount"])
        self.assertEqual(fragment["slots"]["amount"]["range"], "decimal")
        self.assertIn("enpSaleAmt", fragment["slots"]["amount"]["aliases"])

    def test_manual_relation_suggestions_are_linkml_class_valued_slots(self) -> None:
        payload = {
            "relation_suggestions": [
                {
                    "decision": "propose_relation",
                    "source_class_name": "financial_fact",
                    "target_class_name": "financial_concept",
                    "relation_type": "measures_concept",
                    "forward_label": "measures concept",
                    "reverse_label": "measured by facts",
                    "description": "Financial facts are contextualized by the concept being measured.",
                    "cardinality": "many_to_one",
                    "required": True,
                    "confidence": 0.9,
                    "rationale": "Amount fields require a concept context such as sales or operating profit.",
                    "evidence_refs": [{"source_document_id": "doc_1"}],
                }
            ]
        }

        relations = relation_suggestions_from_manual_response({"classes": [], "slots": [], "class_slot_usages": []}, payload)
        fragment = build_linkml_fragment([], relations)

        self.assertEqual(relations[0]["relation_type"], "measures_concept")
        self.assertEqual(fragment["slots"]["measures_concept"]["range"], "FinancialConcept")
        self.assertIn("measures_concept", fragment["classes"]["FinancialFact"]["slots"])
        self.assertTrue(fragment["classes"]["FinancialFact"]["slot_usage"]["measures_concept"]["required"])
