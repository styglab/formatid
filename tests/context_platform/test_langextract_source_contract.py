import json
import tempfile
import unittest
from pathlib import Path

from services.context_platform.internal.ingestion.langextract_source_contract import GroundedExtraction
from services.context_platform.internal.ingestion.langextract_source_contract import build_agent_response_from_extractions
from services.context_platform.internal.ingestion.langextract_source_contract import draft_agent_response_from_openapi
from services.context_platform.internal.ingestion.langextract_source_contract import draft_agent_response_from_source_path


class LangExtractSourceContractTest(unittest.TestCase):
    def test_builds_source_structure_from_grounded_extractions(self) -> None:
        response = build_agent_response_from_extractions(
            [
                GroundedExtraction(
                    extraction_class="source_operation",
                    extraction_text="getSummFinaStat_V2",
                    attributes={
                        "operation_name": "getSummFinaStat_V2",
                        "method": "GET",
                        "path": "/getSummFinaStat_V2",
                        "description": "요약재무제표조회",
                    },
                    char_start=10,
                    char_end=28,
                ),
                GroundedExtraction(
                    extraction_class="source_parameter",
                    extraction_text="crno",
                    attributes={
                        "operation_name": "getSummFinaStat_V2",
                        "wire_name": "crno",
                        "raw_name": "crno",
                        "label_ko": "법인등록번호",
                        "scope": "input",
                        "field_path": "request.query.crno",
                        "is_required": True,
                    },
                    char_start=50,
                    char_end=54,
                ),
                GroundedExtraction(
                    extraction_class="source_response_field",
                    extraction_text="enpSaleAmt",
                    attributes={
                        "operation_name": "getSummFinaStat_V2",
                        "wire_name": "enpSaleAmt",
                        "raw_name": "enpSaleAmt",
                        "label_ko": "기업매출금액",
                        "scope": "output",
                        "field_path": "response.body.items.item.enpSaleAmt",
                        "data_type": "number",
                    },
                    char_start=100,
                    char_end=110,
                ),
            ],
            chunk_spans=[{"chunk_id": "docling_chunk_001", "start": 0, "end": 200}],
        )

        operations = response["source_structure"]["operations"]
        self.assertEqual(len(operations), 1)
        operation = operations[0]
        self.assertEqual(operation["operation_name"], "getSummFinaStat_V2")
        self.assertEqual(operation["parameters"][0]["wire_name"], "crno")
        self.assertEqual(operation["parameters"][0]["label_ko"], "법인등록번호")
        self.assertEqual(operation["response_fields"][0]["wire_name"], "enpSaleAmt")
        self.assertEqual(operation["response_fields"][0]["label_ko"], "기업매출금액")
        self.assertEqual(operation["response_fields"][0]["evidence"][0]["kind"], "langextract_grounding")
        self.assertEqual(operation["response_fields"][0]["evidence"][0]["chunk_id"], "docling_chunk_001")

    def test_ignores_unsupported_extraction_classes(self) -> None:
        response = build_agent_response_from_extractions(
            [
                {
                    "extraction_class": "concept",
                    "extraction_text": "매출",
                    "attributes": {"concept_key": "concept.finance.revenue"},
                }
            ]
        )

        self.assertEqual(response["source_structure"]["operations"], [])

    def test_builds_source_structure_from_swagger_paths_only(self) -> None:
        response = draft_agent_response_from_openapi(_swagger_fixture(), source_name="nts")

        self.assertEqual(response["metadata"]["source_contract_extractor"], "openapi_parser")
        operations = response["source_structure"]["operations"]
        self.assertEqual(len(operations), 1)
        operation = operations[0]
        self.assertEqual(operation["operation_name"], "status")
        self.assertEqual(operation["method"], "POST")
        self.assertEqual(operation["path"], "/status")
        self.assertEqual(operation["base_url"], "https://api.odcloud.kr/api/nts-businessman/v1")
        self.assertNotIn("StatusApiResponse", {item["operation_name"] for item in operations})

        parameters_by_path = {parameter["field_path"]: parameter for parameter in operation["parameters"]}
        self.assertIn("request.query.serviceKey", parameters_by_path)
        self.assertEqual(parameters_by_path["request.query.serviceKey"]["scope"], "control")
        self.assertIn("request.body.b_no", parameters_by_path)
        self.assertEqual(parameters_by_path["request.body.b_no"]["name"], "b_no")

        fields_by_path = {field["field_path"]: field for field in operation["response_fields"]}
        self.assertIn("response.body.data[].b_stt_cd", fields_by_path)
        self.assertEqual(fields_by_path["response.body.data[].b_stt_cd"]["wire_name"], "b_stt_cd")

    def test_source_path_detects_json_openapi_without_langextract(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "nts.md"
            path.write_text(json.dumps(_swagger_fixture(), ensure_ascii=False), encoding="utf-8")

            response = draft_agent_response_from_source_path(path, source_name="nts")

        self.assertEqual(response["metadata"]["source_contract_extractor"], "openapi_parser")
        self.assertEqual(len(response["source_structure"]["operations"]), 1)
        self.assertEqual(response["source_structure"]["operations"][0]["operation_name"], "status")


def _swagger_fixture() -> dict:
    return {
        "swagger": "2.0",
        "host": "api.odcloud.kr",
        "basePath": "/api/nts-businessman/v1",
        "schemes": ["https"],
        "securityDefinitions": {
            "query_key": {
                "type": "apiKey",
                "name": "serviceKey",
                "in": "query",
            }
        },
        "paths": {
            "/status": {
                "post": {
                    "operationId": "status",
                    "parameters": [
                        {
                            "in": "body",
                            "name": "body",
                            "required": True,
                            "schema": {"$ref": "#/definitions/StatusApiRequest"},
                        }
                    ],
                    "responses": {
                        "200": {
                            "description": "OK",
                            "schema": {"$ref": "#/definitions/StatusApiResponse"},
                        }
                    },
                    "security": [{"query_key": []}],
                }
            }
        },
        "definitions": {
            "StatusApiRequest": {
                "type": "object",
                "required": ["b_no"],
                "properties": {
                    "b_no": {
                        "type": "array",
                        "description": "사업자등록번호",
                        "items": {"type": "string"},
                    }
                },
            },
            "StatusApiResponse": {
                "type": "object",
                "properties": {
                    "data": {
                        "type": "array",
                        "items": {"$ref": "#/definitions/BusinessStatus"},
                    }
                },
            },
            "BusinessStatus": {
                "type": "object",
                "properties": {
                    "b_stt_cd": {
                        "type": "string",
                        "description": "납세자상태(코드)",
                    }
                },
            },
        },
    }


if __name__ == "__main__":
    unittest.main()
