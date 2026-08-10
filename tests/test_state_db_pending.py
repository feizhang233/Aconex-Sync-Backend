from pathlib import Path
import tempfile
import unittest

from aconex.state_db import get_pending_workflows, upsert_workflow


class StateDbPendingWorkflowTests(unittest.TestCase):
    def test_pending_query_excludes_terminated_and_completed_workflows(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "state.sqlite"
            upsert_workflow(
                {
                    "workflow_id": "pending",
                    "workflow_number": "WF-000001",
                    "review_status": "",
                    "step_2_overdue_duration_or_status": "pending",
                    "is_completed": 0,
                },
                database,
            )
            upsert_workflow(
                {
                    "workflow_id": "terminated",
                    "workflow_number": "WF-000002",
                    "review_status": "Terminate",
                    "is_completed": 0,
                },
                database,
            )
            upsert_workflow(
                {
                    "workflow_id": "completed",
                    "workflow_number": "WF-000003",
                    "review_status": "A-Approved",
                    "is_completed": 1,
                },
                database,
            )

            self.assertEqual(
                [row["workflow_number"] for row in get_pending_workflows(database)],
                ["WF-000001"],
            )


if __name__ == "__main__":
    unittest.main()
