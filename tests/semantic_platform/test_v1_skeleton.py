from __future__ import annotations

import unittest
from tempfile import TemporaryDirectory
from pathlib import Path

from services.semantic_platform.internal.context import build_runtime_context
from services.semantic_platform.internal.planner import build_not_found_plan, validate_plan
from services.semantic_platform.internal.storage import SemanticLayerRepository


class SemanticLayerSkeletonTests(unittest.TestCase):
    def test_runtime_context_identifies_semantic_platform(self) -> None:
        context = build_runtime_context()

        self.assertEqual(context["service"], "semantic_platform")
        self.assertEqual(context["planner_reads"], "approved_context_only")

    def test_not_found_plan_shape(self) -> None:
        plan = build_not_found_plan()

        self.assertEqual(plan["planner"]["status"], "not_found")
        self.assertEqual(plan["execution_graph"]["type"], "dag")
        self.assertEqual(plan["errors"][0]["code"], "capability_not_found")

    def test_plan_validation_rejects_unapproved_operation(self) -> None:
        plan = {
            "planner": {"status": "planned"},
            "execution_graph": {
                "type": "dag",
                "nodes": [{"id": "n1", "operation_id": "unknown.operation"}],
            },
        }

        result = validate_plan(plan, approved_operation_ids={"approved.operation"})

        self.assertFalse(result["valid"])
        self.assertEqual(result["errors"][0]["code"], "unapproved_operation_id")

    def test_seed_semantic_registry_keeps_registry_empty_by_default(self) -> None:
        with TemporaryDirectory() as tmpdir:
            result = SemanticLayerRepository(str(Path(tmpdir) / "store.json")).seed_semantic_type_registry()

        self.assertEqual(result["status"], "seeded")
        self.assertEqual(result["semantic_type_count"], 0)

    def test_manual_semantic_type_authoring_creates_reviewable_proposal(self) -> None:
        with TemporaryDirectory() as tmpdir:
            repo = SemanticLayerRepository(str(Path(tmpdir) / "store.json"))

            result = repo.create_semantic_type(
                {
                    "name": "ContractAmount",
                    "description": "Canonical amount for a contract.",
                    "datatype": "number",
                    "entity_kind": "entity",
                    "aliases": ["cntrct_amt"],
                    "owners": ["platform"],
                    "tags": ["contract"],
                }
            )
            proposal = result["proposal"]
            reviewed = repo.review_proposal(proposal["id"], "approved", reviewer="tester")
            semantic_type = repo.get_semantic_type(result["semantic_type"]["id"])

        self.assertEqual(proposal["status"], "pending_review")
        self.assertEqual(reviewed["status"], "approved")
        self.assertEqual(semantic_type["status"], "approved")

    def test_relationships_and_semantic_catalog_surface_reviewable_state(self) -> None:
        with TemporaryDirectory() as tmpdir:
            repo = SemanticLayerRepository(str(Path(tmpdir) / "store.json"))
            source = repo.create_semantic_type({"name": "Contract", "datatype": "object", "entity_kind": "entity"})[
                "semantic_type"
            ]
            target = repo.create_semantic_type(
                {
                    "name": "ContractAmount",
                    "datatype": "number",
                    "entity_kind": "attribute",
                    "parent_entity_id": source["id"],
                }
            )["semantic_type"]

            relationship = repo.add_semantic_relationship(
                source["id"],
                {"target_id": target["id"], "relation_type": "contains"},
            )["relationship"]
            catalog = repo.semantic_catalog()
            relationships = repo.list_relationships(status="draft")

        self.assertEqual(1, len(relationships))
        self.assertEqual("contains", relationship["relation_type"])
        self.assertEqual(2, len(catalog["core"]["semantic_types"]))
        self.assertEqual(1, len(catalog["core"]["relationships"]))
        self.assertEqual("pending_review", catalog["governance"]["pending_proposals"][0]["status"])

    def test_delete_semantic_type_creates_proposal_and_removes_record_on_approval(self) -> None:
        with TemporaryDirectory() as tmpdir:
            repo = SemanticLayerRepository(str(Path(tmpdir) / "store.json"))
            source = repo.create_semantic_type({"name": "Contract", "datatype": "object", "entity_kind": "entity"})[
                "semantic_type"
            ]
            target = repo.create_semantic_type(
                {
                    "name": "ContractAmount",
                    "datatype": "number",
                    "entity_kind": "attribute",
                    "parent_entity_id": source["id"],
                }
            )["semantic_type"]
            repo.add_semantic_relationship(
                source["id"],
                {"target_id": target["id"], "relation_type": "contains"},
            )

            deleted = repo.delete_semantic_type(source["id"])
            pending_record = repo.get_semantic_type(source["id"])
            pending_relationships = repo.list_relationships()
            reviewed = repo.review_proposal(deleted["proposal"]["id"], "approved", reviewer="tester")
            removed_record = repo.get_semantic_type(source["id"])
            remaining_relationships = repo.list_relationships()

        self.assertEqual("pending_review", deleted["proposal"]["status"])
        self.assertIsNotNone(pending_record)
        self.assertEqual(1, len(pending_relationships))
        self.assertEqual("approved", reviewed["status"])
        self.assertIsNone(removed_record)
        self.assertEqual([], remaining_relationships)

    def test_capability_and_mapping_proposals_are_reviewable(self) -> None:
        with TemporaryDirectory() as tmpdir:
            repo = SemanticLayerRepository(str(Path(tmpdir) / "store.json"))
            company = repo.create_semantic_type({"name": "Company", "datatype": "object", "entity_kind": "entity"})[
                "semantic_type"
            ]
            company_id = repo.create_semantic_type(
                {
                    "name": "CompanyId",
                    "datatype": "string",
                    "entity_kind": "attribute",
                    "parent_entity_id": company["id"],
                }
            )["semantic_type"]
            capability_result = repo.create_capability(
                {
                    "capability_key": "get_company_info",
                    "name": "GetCompanyInfo",
                    "input_semantic_types": ["COMPANY_ID"],
                    "output_semantic_types": ["COMPANY_ID"],
                }
            )
            mapping_result = repo.create_field_mapping(
                {
                    "operation_id": "op_get_company",
                    "field_path": "company.id",
                    "semantic_type_id": company_id["id"],
                    "canonical_attribute_id": company_id["id"],
                    "mapping_type": "exact",
                }
            )
            reviewed_capability = repo.review_proposal(capability_result["proposal"]["id"], "approved", reviewer="tester")
            reviewed_mapping = repo.review_proposal(mapping_result["proposal"]["id"], "approved", reviewer="tester")
            capability = repo.get_capability(capability_result["capability"]["id"])
            mapping = repo.get_field_mapping(mapping_result["field_mapping"]["id"])

        self.assertEqual("approved", reviewed_capability["status"])
        self.assertEqual("approved", reviewed_mapping["status"])
        self.assertEqual("approved", capability["status"])
        self.assertEqual("approved", mapping["status"])


if __name__ == "__main__":
    unittest.main()
