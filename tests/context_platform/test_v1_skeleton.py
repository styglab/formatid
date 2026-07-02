from __future__ import annotations

import unittest
from pathlib import Path

from services.context_platform.internal.planner import build_not_found_plan, validate_plan


class ContextPlatformSkeletonTests(unittest.TestCase):
    def test_not_found_plan_shape_uses_context_platform_contract(self) -> None:
        plan = build_not_found_plan()

        self.assertIsNone(plan["selected_capability_id"])
        self.assertIsNone(plan["selected_source_operation_id"])
        self.assertEqual(plan["planner"]["status"], "not_found")
        self.assertEqual(plan["validation"]["errors"][0]["code"], "capability_not_found")

    def test_plan_validation_rejects_unapproved_source_operation(self) -> None:
        plan = {
            "selected_capability_id": "cap_company_contact_lookup",
            "selected_source_operation_id": "op_company_info_get",
            "parameter_bindings": [],
            "expected_outputs": [],
        }

        result = validate_plan(plan, approved_operation_ids={"op_other"})

        self.assertFalse(result["valid"])
        self.assertEqual(result["errors"][0]["code"], "unapproved_source_operation_id")

    def test_context_platform_schema_does_not_recreate_removed_registry_tables(self) -> None:
        schema_sql = Path("services/postgres/init/002_create_context_platform.sql").read_text(encoding="utf-8")

        for removed_name in (
            "semantic_types",
            "execution_operations",
            "operation_variants",
            "field_mappings",
            "operation_contracts",
            "mapping_records",
        ):
            self.assertNotIn(removed_name, schema_sql)

    def test_context_platform_schema_contains_meaning_resolution_tables(self) -> None:
        schema_sql = Path("services/postgres/init/002_create_context_platform.sql").read_text(encoding="utf-8")

        for required_name in (
            "sources",
            "source_documents",
            "source_operations",
            "source_parameters",
            "source_fields",
            "meaning_scopes",
            "concept_schemes",
            "concepts",
            "concept_relations",
            "value_domains",
            "value_domain_values",
            "object_types",
            "property_types",
            "link_types",
            "canonical_representations",
            "representation_schemas",
            "external_projections",
            "field_bindings",
            "context_bindings",
            "parameter_bindings",
            "transform_rules",
            "resolution_rules",
            "capabilities",
            "capability_inputs",
            "capability_outputs",
            "capability_steps",
            "plans",
            "execution_plan_steps",
            "executions",
            "execution_step_runs",
            "execution_results",
            "execution_traces",
            "evidence_items",
            "review_events",
            "metadata_aspects",
            "proposals",
            "review_decisions",
        ):
            self.assertIn(required_name, schema_sql)

    def test_context_platform_schema_drops_retired_runtime_tables(self) -> None:
        schema_sql = Path("services/postgres/init/002_create_context_platform.sql").read_text(encoding="utf-8")

        for retired_name in (
            "context_platform.canonical_types",
            "context_platform.canonical_slots",
            "context_platform.canonical_classes",
            "context_platform.canonical_class_slots",
            "context_platform.canonical_relations",
            "context_platform.bindings",
            "context_platform.binding_evidence",
            "context_platform.capability_operations",
            "context_platform.plan_steps",
            "context_platform.execution_logs",
        ):
            self.assertIn(f"drop table if exists {retired_name}", schema_sql)


if __name__ == "__main__":
    unittest.main()
