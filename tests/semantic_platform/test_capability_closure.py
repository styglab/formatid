from __future__ import annotations

import unittest

from services.semantic_platform.lib.ingestion.evidence import extract_structured_evidence
from services.semantic_platform.lib.ingestion.llm.proposal import operation_variant_candidates
from services.semantic_platform.lib.ingestion.proposal.builder import _capability_catalog_closure
from services.semantic_platform.lib.storage.repository import _capability_document_from_capability


class CapabilityClosureTests(unittest.TestCase):
    def test_closure_keeps_only_capability_semantic_mappings(self) -> None:
        state = {
            "operation_variants": [
                {"variant_id": "v.ok", "operation_id": "op.shared", "capability_id": "cap.ok"},
                {"variant_id": "v.other", "operation_id": "op.shared", "capability_id": "cap.other"},
            ],
            "capability_implementations": [
                {"id": "impl.ok", "operation_id": "op.shared", "variant_id": "v.ok", "capability_id": "cap.ok"}
            ],
            "operation_contracts": [
                {
                    "operation_id": "op.shared",
                    "capability_id": "cap.ok",
                    "resource_id": "resource.ok",
                    "request": {"query": {"q": {"semantic_type": "input_type"}}},
                    "response": {"fields": {"value": {"semantic_type": "output_type"}}},
                }
            ],
            "operations": [{"operation_id": "op.shared", "resource_id": "resource.ok"}],
            "operation_fields": [
                {"id": "field.ok", "operation_id": "op.shared", "direction": "response", "raw_name": "value"},
                {"id": "field.other", "operation_id": "op.shared", "direction": "response", "raw_name": "other"},
            ],
            "field_mappings": [
                {
                    "id": "fm.ok",
                    "operation_id": "op.shared",
                    "operation_field_id": "field.ok",
                    "direction": "response",
                    "raw_name": "value",
                    "semantic_type_id": "output_type",
                },
                {
                    "id": "fm.other",
                    "operation_id": "op.shared",
                    "operation_field_id": "field.other",
                    "direction": "response",
                    "raw_name": "other",
                    "semantic_type_id": "other_type",
                },
            ],
            "resources": [{"id": "resource.ok"}],
            "semantic_types": [
                {"id": "input_type"},
                {"id": "output_type"},
                {"id": "other_type"},
            ],
        }
        closure = _capability_catalog_closure(
            state,
            "cap.ok",
            {"id": "cap.ok", "inputs": ["input_type"], "outputs": ["output_type"]},
        )
        self.assertEqual(["fm.ok"], [item["id"] for item in closure["field_mappings"]])
        self.assertEqual(["field.ok"], [item["id"] for item in closure["operation_fields"]])
        self.assertEqual({"input_type", "output_type"}, {item["id"] for item in closure["semantic_types"]})

    def test_capability_document_includes_intent_fields(self) -> None:
        document = _capability_document_from_capability(
            "cap.ok",
            {
                "description_ko": "설명",
                "use_when": ["사용자가 상태를 확인할 때"],
                "inputs": ["input_type"],
                "outputs": ["output_type"],
                "provenance": {
                    "semantic_entities": ["Business"],
                    "planning_hints": {
                        "returns": ["records"],
                        "requires": ["input_type"],
                        "default_variant_id": "variant.ok",
                    },
                },
            },
            {"operation_variants": {}, "operation_contracts": {}},
        )
        text = document["document_text"]
        self.assertIn("사용자가 상태를 확인할 때", text)
        self.assertIn("Business", text)
        self.assertIn("variant.ok", text)

    def test_variant_candidates_use_api_sections_in_codex_manual_mode(self) -> None:
        state = {
            "manual_llm_response": {"proposal_builder": "codex_manual_llm"},
            "verified_api_sections": [],
            "api_sections": [
                {
                    "id": "section.one",
                    "operation_name": "getContracts",
                    "method": "GET",
                    "path": "/getContracts",
                }
            ],
            "structured_evidence": {
                "field_table_candidates": [
                    {
                        "section_id": "section.one",
                        "direction_hint": "request",
                        "rows": [
                            ["inqryDiv", "조회구분", "1", "1", "1", "검색하고자하는 조회구분 1:등록일시, 2:통합계약번호"],
                            ["inqryBgnDt", "조회시작일시", "12", "0", "201608310000", "조회구분이 1인 경우 필수"],
                            ["untyCntrctNo", "통합계약번호", "13", "0", "2016050000077", "조회구분이 2인 경우 필수"],
                        ],
                    }
                ],
                "control_field_candidates": [
                    {
                        "section_id": "section.one",
                        "operation_name": "getContracts",
                        "text": "| inqryDiv | 조회구분 | 1 | 1 | 1 | 검색하고자하는 조회구분 1:등록일시, 2:통합계약번호 |",
                        "values": [
                            {"value": "1", "label": "등록일시"},
                            {"value": "2", "label": "통합계약번호"},
                        ],
                    }
                ],
            },
        }

        candidates = operation_variant_candidates(state)

        self.assertEqual(1, len(candidates))
        self.assertEqual("getContracts", candidates[0]["operation_name"])
        self.assertEqual("inqryDiv", candidates[0]["controls"][0]["raw_name"])
        self.assertEqual(["1", "2"], [item["value"] for item in candidates[0]["controls"][0]["values"]])
        self.assertTrue(candidates[0]["controls"][0]["variant_generation_hint"]["should_review_for_variants"])

    def test_control_field_candidates_are_extracted_from_request_rows(self) -> None:
        blocks = [
            {
                "id": "block.00001",
                "index": 1,
                "kind": "table_row",
                "text": "| inqryDiv | 조회구분 | 1 | 1 | 1 | 검색하고자하는 조회구분 1:등록일시, 2:통합계약번호 |",
            },
            {
                "id": "block.00002",
                "index": 2,
                "kind": "table_row",
                "text": "| inqryBgnDt | 조회시작일시 | 12 | 0 | 201608310000 | 조회구분이 1인 경우 필수 |",
            },
        ]
        sections = [
            {
                "id": "section.one",
                "operation_name": "getContracts",
                "method": "GET",
                "path": "/getContracts",
                "block_start": 0,
                "block_end": 10,
            }
        ]

        evidence = extract_structured_evidence(blocks, sections)
        controls = evidence["control_field_candidates"]

        self.assertEqual("inqryDiv", controls[0]["field_name"])
        self.assertEqual(["1", "2"], [item["value"] for item in controls[0]["values"]])


if __name__ == "__main__":
    unittest.main()
