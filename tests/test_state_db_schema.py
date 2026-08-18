from pathlib import Path
import sqlite3
import tempfile
import unittest

from aconex.db.schema import SCHEMA_VERSION, current_schema_version
from aconex.state_db import connect, init_db, load_workflows, upsert_workflow


class StateDbSchemaTests(unittest.TestCase):
    def test_new_database_is_current_schema_version(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "state.sqlite"
            init_db(database)
            with connect(database) as conn:
                self.assertEqual(current_schema_version(conn), SCHEMA_VERSION)
                primary_key = {
                    row["name"]
                    for row in conn.execute("PRAGMA table_info(workflows)").fetchall()
                    if int(row["pk"] or 0) == 1
                }
            self.assertEqual(primary_key, {"workflow_number"})

    def test_status_cleanup_is_one_shot_not_on_every_read(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "state.sqlite"
            conn = sqlite3.connect(database)
            conn.executescript(
                """
                CREATE TABLE workflows (
                    workflow_number TEXT PRIMARY KEY,
                    workflow_number_int INTEGER,
                    workflow_id TEXT,
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
                    workflow_number, workflow_number_int, review_status,
                    step_1_overdue_duration_or_status, is_completed
                ) VALUES ('WF-000010', 10, '', '審批中', 0);
                """
            )
            conn.commit()
            conn.close()

            rows = load_workflows(database)
            self.assertEqual(rows[0]["step_1_overdue_duration_or_status"], "pending")

            with sqlite3.connect(database) as raw:
                raw.execute(
                    "UPDATE workflows SET step_1_overdue_duration_or_status = '審批中' "
                    "WHERE workflow_number = 'WF-000010'"
                )
                raw.commit()

            rows = load_workflows(database)
            self.assertEqual(rows[0]["step_1_overdue_duration_or_status"], "審批中")

            upsert_workflow(
                {
                    "workflow_number": "WF-000010",
                    "workflow_number_int": 10,
                    "step_1_overdue_duration_or_status": "審批中",
                    "is_completed": 0,
                },
                database,
            )
            rows = load_workflows(database)
            self.assertEqual(rows[0]["step_1_overdue_duration_or_status"], "pending")


if __name__ == "__main__":
    unittest.main()
