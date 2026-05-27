from __future__ import annotations

import unittest

from services.semantic_platform.lib.storage.repository import (
    CATALOG_VERSION_SCOPE,
    CATALOG_VERSION_SECTIONS,
    _catalog_section_specs,
    _catalog_snapshot_diff,
    _catalog_snapshot_counts,
    _catalog_version_snapshot,
)


class CatalogPaginationTests(unittest.TestCase):
    def test_catalog_section_specs_allow_only_known_tables(self) -> None:
        specs = _catalog_section_specs()

        self.assertIn("capabilities", specs)
        self.assertIn("entities", specs)
        self.assertIn("entity_identifiers", specs)
        self.assertIn("semantic_join_rules", specs)
        self.assertIn("planning_examples", specs)
        self.assertIn("operation_contracts", specs)
        self.assertIn("capability_implementations", specs)
        self.assertEqual("sp_capabilities", specs["capabilities"]["table"])
        self.assertEqual("sp_entities", specs["entities"]["table"])
        self.assertEqual("operation_id", specs["operation_contracts"]["key"])

    def test_catalog_version_snapshot_keeps_only_declarative_catalog_sections(self) -> None:
        snapshot = _catalog_version_snapshot(
            {
                "capabilities": {"search_contracts": {"id": "search_contracts"}},
                "capability_documents": {"doc.search_contracts": {"id": "doc.search_contracts"}},
                "endpoint_checks": {"check.1": {"id": "check.1"}},
                "proposals": {"proposal.1": {"id": "proposal.1"}},
            }
        )

        self.assertEqual(CATALOG_VERSION_SCOPE, "approved_declarative_catalog_v1")
        self.assertEqual(set(CATALOG_VERSION_SECTIONS), set(snapshot))
        self.assertEqual({"search_contracts": {"id": "search_contracts"}}, snapshot["capabilities"])
        self.assertNotIn("capability_documents", snapshot)
        self.assertNotIn("endpoint_checks", snapshot)
        self.assertNotIn("proposals", snapshot)

    def test_catalog_snapshot_counts_handles_dict_and_list_sections(self) -> None:
        counts = _catalog_snapshot_counts(
            {
                "capabilities": {"search_contracts": {"id": "search_contracts"}},
                "capability_implementations": [{"id": "impl.search_contracts"}],
                "ignored": "value",
            }
        )

        self.assertEqual(1, counts["capabilities"])
        self.assertEqual(1, counts["capability_implementations"])
        self.assertNotIn("ignored", counts)

    def test_catalog_snapshot_diff_reports_added_changed_removed_by_section(self) -> None:
        diff = _catalog_snapshot_diff(
            {
                "capabilities": {
                    "search_contracts": {"id": "search_contracts", "description": "old"},
                    "old_capability": {"id": "old_capability"},
                },
                "capability_implementations": [{"id": "impl.old", "capability_id": "old_capability"}],
            },
            {
                "capabilities": {
                    "search_contracts": {"id": "search_contracts", "description": "new"},
                    "new_capability": {"id": "new_capability"},
                },
                "capability_implementations": [{"id": "impl.new", "capability_id": "new_capability"}],
            },
        )

        self.assertEqual(["new_capability"], diff["sections"]["capabilities"]["added"])
        self.assertEqual(["old_capability"], diff["sections"]["capabilities"]["removed"])
        self.assertEqual(["search_contracts"], diff["sections"]["capabilities"]["changed"])
        self.assertEqual(["impl.new"], diff["sections"]["capability_implementations"]["added"])
        self.assertEqual(["impl.old"], diff["sections"]["capability_implementations"]["removed"])
        self.assertEqual(2, diff["counts"]["sections_changed"])


if __name__ == "__main__":
    unittest.main()
