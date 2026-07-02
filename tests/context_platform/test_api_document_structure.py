import unittest
import sys
import types


if "langgraph.graph" not in sys.modules:
    langgraph_module = types.ModuleType("langgraph")
    graph_module = types.ModuleType("langgraph.graph")
    graph_module.END = "__end__"

    class StateGraph:
        def __init__(self, *args, **kwargs):
            pass

    graph_module.StateGraph = StateGraph
    sys.modules.setdefault("langgraph", langgraph_module)
    sys.modules.setdefault("langgraph.graph", graph_module)
if "pydantic" not in sys.modules:
    pydantic_module = types.ModuleType("pydantic")

    class BaseModel:
        pass

    pydantic_module.BaseModel = BaseModel
    sys.modules.setdefault("pydantic", pydantic_module)
sys.modules.setdefault("httpx", types.ModuleType("httpx"))

from services.context_platform.internal.ingestion.api_documents import _parsed_from_document_structure
from services.context_platform.internal.ingestion.langgraph.document_structure import DocumentStructureResult
from services.context_platform.internal.ingestion.llm.document_structure import normalize_manual_document_structure_response
from services.context_platform.internal.ingestion.source_contract import validate_source_contract


class ApiDocumentStructureTest(unittest.TestCase):
    def test_nested_source_structure_preserves_body_parameters_and_response_fields(self) -> None:
        normalized = normalize_manual_document_structure_response(
            {
                "source_structure": {
                    "operations": [
                        {
                            "chunk_id": "openapi:post:/status",
                            "operation_name": "status",
                            "method": "POST",
                            "path": "/status",
                            "parameters": [
                                {
                                    "scope": "input",
                                    "wire_name": "b_no",
                                    "raw_name": "b_no",
                                    "field_path": "request.body.b_no",
                                    "data_type": "array",
                                    "is_required": True,
                                },
                                {
                                    "scope": "control",
                                    "wire_name": "serviceKey",
                                    "raw_name": "serviceKey",
                                    "field_path": "request.query.serviceKey",
                                    "data_type": "string",
                                    "is_required": True,
                                },
                                {
                                    "scope": "control",
                                    "wire_name": "Authorization",
                                    "raw_name": "Authorization",
                                    "field_path": "request.header.Authorization",
                                    "data_type": "string",
                                    "is_required": True,
                                },
                            ],
                            "response_fields": [
                                {
                                    "scope": "output",
                                    "wire_name": "b_stt_cd",
                                    "raw_name": "b_stt_cd",
                                    "field_path": "response.body.data[].b_stt_cd",
                                    "data_type": "string",
                                    "description": "납세자상태코드",
                                }
                            ],
                        }
                    ]
                }
            }
        )
        result = DocumentStructureResult(
            drafts=[],
            chunk_summaries=[],
            classified_chunks=[],
            operation_candidates=normalized["operation_candidates"],
            field_candidates=normalized["field_candidates"],
            engine=normalized["engine"],
            llm_mode=normalized["llm_mode"],
            status="ready",
        )

        parsed = _parsed_from_document_structure(result)

        operation = parsed["operations"][0]
        parameters = {item["name"]: item for item in operation["parameters"]}
        self.assertEqual(parameters["b_no"]["location"], "body")
        self.assertEqual(parameters["serviceKey"]["location"], "query")
        self.assertEqual(parameters["Authorization"]["location"], "header")
        self.assertEqual(operation["response_fields"][0]["raw_name"], "b_stt_cd")
        self.assertEqual(operation["response_fields"][0]["field_path"], "response.body.data[].b_stt_cd")

    def test_parsed_structure_scopes_fields_by_operation_chunk(self) -> None:
        result = DocumentStructureResult(
            drafts=[],
            chunk_summaries=[],
            classified_chunks=[],
            operation_candidates=[
                {
                    "chunk_id": "docling_chunk_008",
                    "operation_name": "getSummFinaStat_V2",
                    "method": "GET",
                    "path": "/getSummFinaStat_V2",
                },
                {
                    "chunk_id": "docling_chunk_020",
                    "operation_name": "getBs_V2",
                    "method": "GET",
                    "path": "/getBs_V2",
                },
            ],
            field_candidates=[
                {
                    "chunk_id": "docling_chunk_008",
                    "scope": "input",
                    "raw_name": "crno",
                    "field_path": "request.crno",
                },
                {
                    "chunk_id": "docling_chunk_008",
                    "scope": "output",
                    "raw_name": "enpSaleAmt",
                    "field_path": "response.body.items.item.enpSaleAmt",
                },
                {
                    "chunk_id": "docling_chunk_020",
                    "scope": "output",
                    "raw_name": "acitId",
                    "field_path": "response.body.items.item.acitId",
                },
            ],
            engine="codex_manual_document_structure_graph",
            llm_mode="codex_manual",
            status="ready",
        )

        parsed = _parsed_from_document_structure(result)

        operations = {item["name"]: item for item in parsed["operations"]}
        summary_fields = {item["raw_name"] for item in operations["getSummFinaStat_V2"]["response_fields"]}
        balance_fields = {item["raw_name"] for item in operations["getBs_V2"]["response_fields"]}

        self.assertEqual(summary_fields, {"enpSaleAmt"})
        self.assertEqual(balance_fields, {"acitId"})
        self.assertEqual([item["name"] for item in operations["getSummFinaStat_V2"]["parameters"]], ["crno"])
        self.assertEqual(operations["getSummFinaStat_V2"]["parameters"][0]["parameter_path"], "request.crno")
        self.assertEqual([item["name"] for item in operations["getBs_V2"]["parameters"]], ["crno"])

    def test_parsed_structure_does_not_fallback_outputs_across_chunks(self) -> None:
        result = DocumentStructureResult(
            drafts=[],
            chunk_summaries=[],
            classified_chunks=[],
            operation_candidates=[
                {"chunk_id": "chunk_a", "operation_name": "opA", "method": "GET", "path": "/a"},
                {"chunk_id": "chunk_b", "operation_name": "opB", "method": "GET", "path": "/b"},
            ],
            field_candidates=[
                {"chunk_id": "chunk_a", "scope": "output", "raw_name": "amount", "field_path": "response.amount"},
            ],
            engine="codex_manual_document_structure_graph",
            llm_mode="codex_manual",
            status="ready",
        )

        parsed = _parsed_from_document_structure(result)

        operations = {item["name"]: item for item in parsed["operations"]}
        self.assertEqual([item["raw_name"] for item in operations["opA"]["response_fields"]], ["amount"])
        self.assertEqual(operations["opB"]["response_fields"], [])

    def test_source_contract_accepts_wire_names_with_korean_labels(self) -> None:
        result = DocumentStructureResult(
            drafts=[],
            chunk_summaries=[],
            classified_chunks=[],
            operation_candidates=[
                {"chunk_id": "chunk_a", "operation_name": "getSummFinaStat_V2", "method": "GET", "path": "/getSummFinaStat_V2"},
            ],
            field_candidates=[
                {
                    "chunk_id": "chunk_a",
                    "scope": "input",
                    "wire_name": "crno",
                    "raw_name": "crno",
                    "label_ko": "법인등록번호",
                    "field_path": "request.query.crno",
                },
                {
                    "chunk_id": "chunk_a",
                    "scope": "output",
                    "wire_name": "enpSaleAmt",
                    "raw_name": "enpSaleAmt",
                    "label_ko": "기업매출금액",
                    "field_path": "response.body.items.item.enpSaleAmt",
                },
            ],
            engine="codex_manual_document_structure_graph",
            llm_mode="codex_manual",
            status="ready",
        )

        parsed = _parsed_from_document_structure(result)
        operation = parsed["operations"][0]

        self.assertEqual(validate_source_contract(parsed), [])
        self.assertEqual(operation["parameters"][0]["name"], "crno")
        self.assertEqual(operation["parameters"][0]["metadata"]["label_ko"], "법인등록번호")
        self.assertEqual(operation["response_fields"][0]["raw_name"], "enpSaleAmt")
        self.assertEqual(operation["response_fields"][0]["display_name"], "기업매출금액")

    def test_source_contract_rejects_korean_labels_as_wire_names(self) -> None:
        parsed = {
            "operations": [
                {
                    "operation_key": "getSummFinaStat_V2",
                    "method": "GET",
                    "path": "/getSummFinaStat_V2",
                    "parameters": [
                        {
                            "name": "법인등록번호",
                            "raw_name": "법인등록번호",
                            "parameter_path": "request.법인등록번호",
                        }
                    ],
                    "response_fields": [
                        {
                            "raw_name": "기업매출금액",
                            "field_path": "response.기업매출금액",
                        }
                    ],
                }
            ]
        }

        errors = validate_source_contract(parsed)

        self.assertTrue(any("source parameter" in error and "Korean label" in error for error in errors))
        self.assertTrue(any("source response field" in error and "Korean label" in error for error in errors))

    def test_document_structure_normalizes_nested_source_structure(self) -> None:
        normalized = normalize_manual_document_structure_response(
            {
                "source_structure": {
                    "operations": [
                        {
                            "chunk_id": "chunk_a",
                            "operation_key": "getSummFinaStat_V2",
                            "method": "GET",
                            "path": "/getSummFinaStat_V2",
                            "parameters": [
                                {
                                    "wire_name": "crno",
                                    "label_ko": "법인등록번호",
                                    "field_path": "request.query.crno",
                                }
                            ],
                            "response_fields": [
                                {
                                    "wire_name": "enpSaleAmt",
                                    "label_ko": "기업매출금액",
                                    "field_path": "response.body.items.item.enpSaleAmt",
                                }
                            ],
                        }
                    ]
                }
            }
        )

        self.assertEqual(normalized["operation_candidates"][0]["operation_name"], "getSummFinaStat_V2")
        self.assertEqual(normalized["field_candidates"][0]["raw_name"], "crno")
        self.assertEqual(normalized["field_candidates"][0]["label_ko"], "법인등록번호")
        self.assertEqual(normalized["field_candidates"][1]["raw_name"], "enpSaleAmt")
        self.assertEqual(normalized["field_candidates"][1]["scope"], "output")


if __name__ == "__main__":
    unittest.main()
