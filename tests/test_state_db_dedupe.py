from pathlib import Path
import tempfile
import unittest

from aconex.state_db import dedupe_workflows, init_db, load_workflows, upsert_workflow


class StateDbDedupeTests(unittest.TestCase):
    def test_upsert_is_keyed_by_workflow_number(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "state.sqlite"
            upsert_workflow(
                {
                    "workflow_id": "111",
                    "workflow_number": "WF-001082",
                    "workflow_number_int": 1082,
                    "review_outcome": "Pending",
                    "is_completed": 0,
                },
                database,
            )
            upsert_workflow(
                {
                    "workflow_id": "222",
                    "workflow_number": "WF-001082",
                    "workflow_number_int": 1082,
                    "review_outcome": "A-Approved",
                    "step_1_completed_time": "2026-07-29T09:18:50.297Z",
                    "step_2_completed_time": "2026-08-03T10:21:08.714Z",
                    "is_completed": 1,
                },
                database,
            )
            rows = load_workflows(database)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["workflow_number"], "WF-001082")
            self.assertEqual(rows[0]["workflow_id"], "222")
            self.assertEqual(rows[0]["review_outcome"], "A-Approved")
            self.assertEqual(rows[0]["is_completed"], 1)

    def test_legacy_duplicate_ids_are_collapsed_on_init(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "state.sqlite"
            # Create a legacy table keyed by workflow_id with two rows for one number.
            import sqlite3

            conn = sqlite3.connect(database)
            conn.executescript(
                """
                CREATE TABLE workflows (
                    workflow_id TEXT PRIMARY KEY,
                    workflow_number TEXT,
                    workflow_number_int INTEGER,
                    workflow_title TEXT,
                    review_outcome TEXT,
                    review_status TEXT,
                    step_1_completed_time TEXT,
                    step_1_due_time TEXT,
                    step_1_review_status TEXT,
                    step_1_overdue_duration_or_status TEXT,
                    step_2_completed_time TEXT,
                    step_2_due_time TEXT,
                    step_2_review_status TEXT,
                    step_2_overdue_duration_or_status TEXT,
                    is_completed INTEGER,
                    last_checked_at TEXT,
                    last_changed_at TEXT,
                    source TEXT
                );
                INSERT INTO workflows (
                    workflow_id, workflow_number, workflow_number_int, review_outcome,
                    is_completed, last_checked_at, step_2_completed_time
                ) VALUES
                ('542', 'WF-001082', 1082, 'A-Approved', 1, '2026-08-10T08:00:00+00:00', '2026-08-03T10:21:08.714Z'),
                ('543', 'WF-001082', 1082, 'Pending', 0, '2026-08-03T08:00:00+00:00', '');
                """
            )
            conn.commit()
            conn.close()

            collapsed = dedupe_workflows(database)
            self.assertEqual(collapsed, ["WF-001082"])
            rows = load_workflows(database)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["workflow_number"], "WF-001082")
            self.assertEqual(rows[0]["is_completed"], 1)
            self.assertEqual(rows[0]["review_outcome"], "A-Approved")
            # Stable auxiliary id prefers the smaller Aconex step id.
            self.assertEqual(rows[0]["workflow_id"], "542")


if __name__ == "__main__":
    unittest.main()
