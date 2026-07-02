from __future__ import annotations

import copy
import json
import sys
import types
import unittest
from pathlib import Path
from typing import Any

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
        def __init__(self, **kwargs):
            for key, value in kwargs.items():
                setattr(self, key, value)

    pydantic_module.BaseModel = BaseModel
    sys.modules.setdefault("pydantic", pydantic_module)
sys.modules.setdefault("httpx", types.ModuleType("httpx"))

from services.context_platform.internal.ingestion.api_documents import create_ingestion_proposals
from services.context_platform.internal.ingestion.binding_generation import binding_suggestions_from_manual_response
from services.context_platform.internal.ingestion.capability_generation import build_capability_generation_payload
from services.context_platform.internal.ingestion.capability_generation import capability_suggestions_from_manual_response
from services.context_platform.internal.ingestion.capability_generation import collect_operation_contexts
from services.context_platform.internal.ingestion.canonical_reconciliation import reconcile_terms_from_manual_response
from services.context_platform.internal.ingestion.llm.agent_response import AgentResponseValidationError
from services.context_platform.internal.ingestion.llm.agent_response import validate_agent_response_artifact
from services.context_platform.internal.ingestion.llm.binding_generation import normalize_manual_binding_generation_response
from services.context_platform.internal.ingestion.llm.canonical_reconciliation import normalize_manual_canonical_reconciliation_response
from services.context_platform.internal.ingestion.llm.capability_generation import normalize_manual_capability_generation_response
from services.context_platform.internal.ingestion.langgraph.pipeline import _agent_response_validation_error


FIXTURE = Path("tests/fixtures/context_platform/revenue_agent_response.json")


class AgentResponseContractTest(unittest.TestCase):
    def test_revenue_fixture_is_valid_agent_response_contract(self) -> None:
        artifact = _load_fixture()

        validate_agent_response_artifact(artifact)

    def test_concept_decision_rejects_schema_constraints(self) -> None:
        artifact = _load_fixture()
        artifact["meaning_resolution"]["concept_decisions"][0]["pattern"] = "^\\d{13}$"

        with self.assertRaisesRegex(AgentResponseValidationError, "representation schema keys"):
            validate_agent_response_artifact(artifact)

    def test_api_source_contract_rejects_korean_raw_wire_names(self) -> None:
        artifact = {
            "operation_candidates": [
                {
                    "operation_name": "getSummFinaStat_V2",
                    "method": "GET",
                    "path": "/getSummFinaStat_V2",
                    "description": "요약재무제표조회",
                    "evidence_refs": [],
                }
            ],
            "field_candidates": [
                {
                    "scope": "input",
                    "raw_name": "법인등록번호",
                    "field_path": "request.법인등록번호",
                    "data_type": "string",
                    "is_required": True,
                    "description": "Korean display label was incorrectly used as the API key.",
                    "evidence": [],
                }
            ],
        }

        with self.assertRaisesRegex(AgentResponseValidationError, "not an executable API wire key"):
            validate_agent_response_artifact(artifact)

    def test_api_source_contract_allows_wire_name_with_korean_label(self) -> None:
        artifact = {
            "operation_candidates": [
                {
                    "operation_name": "getSummFinaStat_V2",
                    "method": "GET",
                    "path": "/getSummFinaStat_V2",
                    "description": "요약재무제표조회",
                    "evidence_refs": [],
                }
            ],
            "field_candidates": [
                {
                    "scope": "input",
                    "wire_name": "crno",
                    "raw_name": "crno",
                    "field_path": "request.query.crno",
                    "label_ko": "법인등록번호",
                    "data_type": "string",
                    "is_required": False,
                    "description": "Corporate registration number.",
                    "evidence": [],
                }
            ],
        }

        validate_agent_response_artifact(artifact)

    def test_verification_sample_parameters_reject_secret_like_values(self) -> None:
        artifact = {
            "verification": {
                "sample_parameters": {
                    "default": {
                        "serviceKey": "plain-secret-must-not-be-here",
                        "crno": "1746110000741",
                    }
                }
            }
        }

        with self.assertRaisesRegex(AgentResponseValidationError, "use verification.secret_env"):
            validate_agent_response_artifact(artifact)

    def test_capability_output_requires_output_key(self) -> None:
        artifact = _load_fixture()
        del artifact["capability_generation"]["suggestions"][0]["outputs"][0]["output_key"]

        with self.assertRaisesRegex(AgentResponseValidationError, "output_key"):
            validate_agent_response_artifact(artifact)

    def test_concept_decision_rejects_invalid_concept_kind(self) -> None:
        artifact = _load_fixture()
        artifact["meaning_resolution"]["concept_decisions"][0]["concept"]["kind"] = "Observation"

        with self.assertRaisesRegex(AgentResponseValidationError, "invalid kind"):
            validate_agent_response_artifact(artifact)

    def test_field_binding_rejects_context_key(self) -> None:
        artifact = _load_fixture()
        artifact["resolution_generation"]["field_bindings"][0]["context_key"] = "currency"

        with self.assertRaisesRegex(AgentResponseValidationError, "move context values to context_bindings"):
            validate_agent_response_artifact(artifact)

    def test_capability_declared_outputs_must_have_output_contracts(self) -> None:
        artifact = _load_fixture()
        artifact["capability_generation"]["suggestions"][0]["capability"]["provides_concepts"] = [
            "concept.finance.revenue",
            "concept.finance.net_income",
        ]

        with self.assertRaisesRegex(AgentResponseValidationError, "declares output concepts without capability outputs"):
            validate_agent_response_artifact(artifact)

    def test_pipeline_metadata_validation_reports_invalid_agent_response(self) -> None:
        artifact = _load_fixture()
        del artifact["capability_generation"]["suggestions"][0]["outputs"][0]["output_key"]

        error = _agent_response_validation_error({"agent_response": artifact})

        self.assertIn("output_key", error)

    def test_revenue_fixture_flows_to_reviewable_proposal_bundle_shape(self) -> None:
        artifact = _load_fixture()
        validate_agent_response_artifact(artifact)

        terms = _source_terms()
        meaning_stage = normalize_manual_canonical_reconciliation_response(artifact["meaning_resolution"])
        decisions = reconcile_terms_from_manual_response(
            terms,
            {"classes": [], "slots": [], "class_slot_usages": []},
            meaning_stage,
            allow_heuristic_create=False,
        )
        meaning_payload = {
            **meaning_stage,
            "decisions": decisions,
            "decision_counts": {"create": len(decisions)},
            "term_count": len(decisions),
        }

        resolution_stage = normalize_manual_binding_generation_response(artifact["resolution_generation"])
        bindings = binding_suggestions_from_manual_response(terms, resolution_stage, allow_heuristic_bind=False)
        resolution_payload = {
            **resolution_stage,
            "suggestions": bindings,
            "decision_counts": {"bind": len(bindings)},
            "term_count": len(bindings),
        }

        operations = [_operation()]
        operation_contexts = collect_operation_contexts(
            source={"id": "src_fsc"},
            document={"id": "doc_fsc_finance"},
            operations=operations,
            binding_generation=resolution_payload,
        )
        capability_stage = normalize_manual_capability_generation_response(artifact["capability_generation"])
        capabilities = capability_suggestions_from_manual_response(
            operation_contexts,
            capability_stage,
            allow_heuristic_propose=False,
        )
        capability_payload = build_capability_generation_payload(
            source={"id": "src_fsc"},
            document={"id": "doc_fsc_finance"},
            suggestions=capabilities,
            llm_mode="agent_manual",
            engine="fixture_capability_generation",
        )

        repo = _ProposalRepo()
        proposals = create_ingestion_proposals(
            repo,
            {"id": "src_fsc", "name": "FSC finance"},
            {"id": "doc_fsc_finance", "name": "FSC finance guide"},
            operations,
            [],
            meaning_payload,
            resolution_payload,
            capability_payload,
            {"summary": {"total": 0, "verified": 0, "failed": 0, "skipped": 0, "needs_input": 0}},
        )

        entity_types = {item["entity_type"] for item in proposals}
        self.assertIn("meaning_resolution", entity_types)
        self.assertIn("capability_generation", entity_types)
        self.assertIn("capability", entity_types)
        self.assertIn("capability_step", entity_types)

        capability_proposal = next(item for item in proposals if item["entity_type"] == "capability")
        self.assertEqual(capability_proposal["entity_id"], "cap.company.finance.get_revenue")
        self.assertEqual(capability_proposal["payload"]["outputs"][0]["output_key"], "revenue_amount")
        self.assertEqual(capability_proposal["payload"]["outputs"][0]["concept_key"], "concept.finance.revenue")
        self.assertEqual(
            capability_proposal["payload"]["outputs"][0]["representation_schema_key"],
            "schema.finance.revenue.money_amount",
        )
        binding_spec = capability_proposal["payload"]["operation_link"]["binding_spec"]
        self.assertEqual([item["source_field_id"] for item in binding_spec["outputs"]], ["field_enp_sale_amt"])
        self.assertEqual([item["context_key"] for item in binding_spec["contexts"]], ["currency"])


def _load_fixture() -> dict[str, Any]:
    return copy.deepcopy(json.loads(FIXTURE.read_text(encoding="utf-8")))


def _source_terms() -> list[dict[str, Any]]:
    return [
        {
            "source_kind": "parameter",
            "source_operation_id": "op_fsc_summary",
            "source_parameter_id": "param_crno",
            "field_path": "request.crno",
            "raw_name": "crno",
            "direction": "input",
        },
        {
            "source_kind": "parameter",
            "source_operation_id": "op_fsc_summary",
            "source_parameter_id": "param_biz_year",
            "field_path": "request.bizYear",
            "raw_name": "bizYear",
            "direction": "input",
        },
        {
            "source_kind": "field",
            "source_operation_id": "op_fsc_summary",
            "source_field_id": "field_enp_sale_amt",
            "field_path": "response.body.items.item.enpSaleAmt",
            "raw_name": "enpSaleAmt",
            "direction": "output",
        },
        {
            "source_kind": "field",
            "source_operation_id": "op_fsc_summary",
            "source_field_id": "field_cur_cd",
            "field_path": "response.body.items.item.curCd",
            "raw_name": "curCd",
            "direction": "output",
        },
    ]


def _operation() -> dict[str, Any]:
    return {
        "id": "op_fsc_summary",
        "operation_key": "getSummFinaStat_V2",
        "name": "getSummFinaStat_V2",
        "method": "GET",
        "path": "/getSummFinaStat_V2",
        "description": "요약재무제표조회",
        "parameters": [
            {"id": "param_crno", "name": "crno", "raw_name": "crno", "field_path": "request.crno"},
            {"id": "param_biz_year", "name": "bizYear", "raw_name": "bizYear", "field_path": "request.bizYear"},
        ],
        "fields": [
            {
                "id": "field_enp_sale_amt",
                "raw_name": "enpSaleAmt",
                "field_path": "response.body.items.item.enpSaleAmt",
            },
            {"id": "field_cur_cd", "raw_name": "curCd", "field_path": "response.body.items.item.curCd"},
        ],
    }


class _ProposalRepo:
    def __init__(self) -> None:
        self.proposals: list[dict[str, Any]] = []

    def create_proposal(self, payload: dict[str, Any]) -> dict[str, Any]:
        proposal = {"id": f"proposal_{len(self.proposals) + 1}", **payload}
        self.proposals.append(proposal)
        return proposal


if __name__ == "__main__":
    unittest.main()
