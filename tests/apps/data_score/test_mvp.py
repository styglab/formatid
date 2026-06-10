from __future__ import annotations

import unittest

from apps.data_score.app.flows.evaluation import run_evaluation


CSV_TEXT = """company_name,description,category
Samsung Electronics,Global semiconductor and consumer electronics manufacturer,technology
LG Energy Solution,Battery manufacturer for electric vehicles,energy
Samsung Electronics,Global semiconductor and consumer electronics manufacturer,technology
"""


class DataScoreMvpTests(unittest.TestCase):
    def test_disabled_mode_runs_end_to_end(self) -> None:
        report = run_evaluation(
            dataset_name="dataset.company_profiles",
            csv_text=CSV_TEXT,
            business_context="vendor discovery",
            llm_mode="disabled",
        )

        self.assertEqual("dataset.company_profiles", report["dataset_id"])
        self.assertEqual(3, report["profile"]["row_count"])
        self.assertIn("traditional_score", report["scores"])
        self.assertIn("semantic_score", report["scores"])
        self.assertEqual("completed", report["semantic_scores"]["status"])

    def test_manual_judge_result_controls_semantic_scores(self) -> None:
        report = run_evaluation(
            dataset_name="dataset.company_profiles",
            csv_text=CSV_TEXT,
            llm_mode="codex_manual",
            manual_judge_result={
                "coverage": 88,
                "specificity": 77,
                "consistency": 91,
                "business_fitness": 83,
                "reason": "manual review",
                "suggestions": ["expand vendor description"],
                "confidence": 0.9,
            },
        )

        self.assertEqual(88.0, report["semantic_scores"]["coverage"])
        self.assertEqual("manual", report["semantic_scores"]["source"])
        self.assertAlmostEqual(84.3, report["scores"]["semantic_score"], places=2)

    def test_empty_csv_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            run_evaluation(dataset_name="dataset.empty", csv_text="")


if __name__ == "__main__":
    unittest.main()
