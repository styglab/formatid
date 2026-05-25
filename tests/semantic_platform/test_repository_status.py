from __future__ import annotations

import unittest

from services.semantic_platform.lib.storage.repository import _applied_status


class RepositoryStatusTests(unittest.TestCase):
    def test_applied_status_marks_reviewed_work_as_approved(self) -> None:
        self.assertEqual("approved", _applied_status(None))
        self.assertEqual("approved", _applied_status(""))
        self.assertEqual("approved", _applied_status("pending_review"))
        self.assertEqual("approved", _applied_status("planned"))

    def test_applied_status_preserves_explicit_lifecycle_status(self) -> None:
        self.assertEqual("deprecated", _applied_status("deprecated"))


if __name__ == "__main__":
    unittest.main()
