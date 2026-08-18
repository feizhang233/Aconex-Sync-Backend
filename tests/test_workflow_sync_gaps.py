from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from aconex.workflow_sync import (
    _manifest_changes_for_row,
    format_missing_workflow_numbers,
    format_workflow_number,
    has_pushable_review_status,
    missing_workflow_numbers,
    workflow_number_width,
    workflow_sync_reviewing,
)


class WorkflowSyncGapTests(unittest.TestCase):
    def test_missing_numbers_fill_holes_and_lookahead(self):
        self.assertEqual(
            missing_workflow_numbers([1, 2, 4], extra_high_water=[5], lookahead=2),
            [3, 6, 7],
        )

    def test_missing_numbers_use_current_high_water_when_local_db_lags(self):
        missing = missing_workflow_numbers(
            [1196, 1198], extra_high_water=[1210], lookahead=0, start=1190
        )
        self.assertEqual(
            missing,
            [
                1190,
                1191,
                1192,
                1193,
                1194,
                1195,
                1197,
                1199,
                1200,
                1201,
                1202,
                1203,
                1204,
                1205,
                1206,
                1207,
                1208,
                1209,
            ],
        )

    def test_missing_numbers_empty_when_nothing_is_known(self):
        self.assertEqual(missing_workflow_numbers([], lookahead=20), [])

    def test_format_workflow_number_matches_aconex_padding(self):
        self.assertEqual(format_workflow_number(1197), "WF-001197")
        self.assertEqual(
            format_missing_workflow_numbers([1196, 1198], lookahead=0, start=1196),
            ["WF-001197"],
        )

    def test_workflow_number_width_reads_existing_rows(self):
        self.assertEqual(
            workflow_number_width([{"workflow_number": "WF-001197"}]),
            6,
        )

    def test_first_seen_completed_row_queues_docflow_status(self):
        row = {
            "workflow_id": "aux",
            "workflow_number": "WF-001197",
            "review_status": "B-Approved with comments",
            "step_1_review_status": "B-Approved with comments",
            "step_2_review_status": "B-Approved with comments",
            "step_1_completed_time": "2026-07-28T13:48:08.934Z",
            "step_2_completed_time": "2026-07-29T07:23:39.746Z",
            "is_completed": 1,
        }
        self.assertTrue(has_pushable_review_status(row))
        kinds = [
            change["kind"]
            for change in _manifest_changes_for_row(
                None, row, checked_at="2026-08-17T10:00:00+00:00", summary="inserted"
            )
        ]
        self.assertEqual(kinds, ["new", "status"])

    def test_first_seen_pending_row_skips_docflow(self):
        row = {
            "workflow_id": "aux",
            "workflow_number": "WF-001196",
            "review_status": "",
            "step_1_review_status": "",
            "step_2_review_status": "",
            "is_completed": 0,
        }
        self.assertFalse(has_pushable_review_status(row))
        kinds = [
            change["kind"]
            for change in _manifest_changes_for_row(
                None, row, checked_at="2026-08-17T10:00:00+00:00", summary="inserted"
            )
        ]
        self.assertEqual(kinds, ["new"])

    def test_existing_row_status_change_is_status_only(self):
        old_row = {"workflow_number": "WF-001196", "review_status": ""}
        row = {
            "workflow_id": "aux",
            "workflow_number": "WF-001196",
            "review_status": "B-Approved with comments",
        }
        kinds = [
            change["kind"]
            for change in _manifest_changes_for_row(
                old_row, row, checked_at="2026-08-17T10:00:00+00:00", summary="changed"
            )
        ]
        self.assertEqual(kinds, ["status"])

    @patch("aconex.workflow_sync._sync_rows")
    @patch("aconex.workflow_sync.fetch_workflow_status_rows_by_numbers")
    @patch("aconex.workflow_sync.fetch_current_workflow_status_rows")
    @patch("aconex.workflow_sync.get_pending_workflows")
    @patch("aconex.workflow_sync.load_workflows")
    def test_reviewing_sync_searches_sqlite_gaps_and_lookahead(
        self,
        load_workflows,
        get_pending,
        fetch_current,
        fetch_by_numbers,
        sync_rows,
    ):
        load_workflows.return_value = [
            {"workflow_number": "WF-001196", "workflow_number_int": 1196},
            {"workflow_number": "WF-001198", "workflow_number_int": 1198},
        ]
        get_pending.return_value = [
            {"workflow_number": "WF-001196", "workflow_number_int": 1196},
        ]
        fetch_current.return_value = [
            {"workflow_number": "WF-001198", "workflow_number_int": 1198},
        ]
        fetch_by_numbers.return_value = [
            {
                "workflow_number": "WF-001197",
                "workflow_number_int": 1197,
                "review_status": "B-Approved with comments",
            }
        ]
        expected_output = Path("/tmp/workflow_status_reviewing.xlsx")
        sync_rows.return_value = expected_output

        settings = object()
        client = object()
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "out.xlsx"
            result = workflow_sync_reviewing(settings, client, output=output, save_raw=False)

        self.assertEqual(result, expected_output)
        requested = set(fetch_by_numbers.call_args.kwargs["workflow_numbers"])
        self.assertIn("WF-001196", requested)
        self.assertIn("WF-001197", requested)
        self.assertIn("WF-001199", requested)
        self.assertNotIn("WF-001198", requested)
        synced_numbers = {
            str(row.get("workflow_number"))
            for row in sync_rows.call_args.args[1]
        }
        self.assertEqual(synced_numbers, {"WF-001197", "WF-001198"})


if __name__ == "__main__":
    unittest.main()
