import unittest

from aconex.workflow import (
    format_workflow_number,
    has_pushable_review_status,
    is_step_2_final,
    is_terminated,
    is_workflow_completed,
    missing_workflow_numbers,
    normalize_workflow_status_fields,
    parse_workflow_number_int,
    review_code,
)


class WorkflowDomainTests(unittest.TestCase):
    def test_parse_and_format_workflow_numbers(self):
        self.assertEqual(parse_workflow_number_int("WF-001197"), 1197)
        self.assertEqual(format_workflow_number(1197), "WF-001197")
        self.assertIsNone(parse_workflow_number_int(""))

    def test_review_code_and_step_2_final(self):
        self.assertEqual(review_code("B-Approved with comments"), "B")
        self.assertEqual(review_code("pending"), "P")
        self.assertTrue(
            is_step_2_final(
                {
                    "step_2_review_status": "A-Approved",
                    "review_status": "A-Approved",
                }
            )
        )
        self.assertFalse(
            is_step_2_final(
                {
                    "step_2_review_status": "C-Reject",
                    "review_status": "Terminate",
                }
            )
        )
        self.assertTrue(is_terminated({"review_status": "terminated"}))

    def test_completed_and_pushable_status(self):
        completed = {
            "step_1_completed_time": "2026-07-01T00:00:00Z",
            "step_2_completed_time": "2026-07-02T00:00:00Z",
        }
        self.assertTrue(is_workflow_completed(completed))
        self.assertTrue(has_pushable_review_status(completed))
        self.assertFalse(
            has_pushable_review_status(
                {"review_status": "", "step_1_review_status": "", "step_2_review_status": ""}
            )
        )

    def test_normalize_happens_on_write_payload(self):
        normalized = normalize_workflow_status_fields(
            {
                "review_outcome": "None",
                "review_status": "",
                "step_2_overdue_duration_or_status": "審批中",
            }
        )
        self.assertEqual(normalized["review_status"], "Terminate")
        self.assertEqual(normalized["step_2_overdue_duration_or_status"], "pending")

    def test_missing_numbers_need_a_high_water_mark(self):
        self.assertEqual(missing_workflow_numbers([]), [])
        self.assertEqual(missing_workflow_numbers([3], lookahead=1, start=1), [1, 2, 4])


if __name__ == "__main__":
    unittest.main()
