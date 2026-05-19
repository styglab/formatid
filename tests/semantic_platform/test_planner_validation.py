from __future__ import annotations

import unittest

from services.semantic_platform.planner.service import validate_plan


class PlannerValidationTests(unittest.TestCase):
    def test_explicit_not_found_plan_is_preserved(self) -> None:
        plan = validate_plan(
            "날씨 예보 알려줘",
            {
                "execution_graph": {
                    "type": "dag",
                    "status": "not_found",
                    "reason": "capability_not_found",
                    "nodes": [],
                }
            },
            {
                "capabilities": [],
                "operation_contracts": {},
                "operation_variants": {},
            },
            "codex_manual",
        )

        self.assertEqual("not_found", plan["planner"]["status"])
        self.assertEqual("capability_not_found", plan["planner"]["reason"])
        self.assertEqual("not_found", plan["execution_graph"]["status"])
        self.assertEqual([], plan["execution_graph"]["nodes"])


if __name__ == "__main__":
    unittest.main()
