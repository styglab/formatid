from __future__ import annotations

import unittest

from services.semantic_platform.lib.storage.repository import _catalog_section_specs


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


if __name__ == "__main__":
    unittest.main()
