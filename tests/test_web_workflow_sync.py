import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from aconex.web_workflow_sync import (
    DOCFLOW_MESSAGE_MAX_LEN,
    _api_root,
    _bulk_comments_url,
    _comment_payloads_by_workflow,
    _docflow_headers,
    _docflow_message,
    _feedback_code,
    _gds_as_step_2,
    _payload_hash,
    _web_payload,
    _workflow_comments_url,
    _workflow_url,
    push_workflow_comments_to_docflow,
    push_workflows_to_docflow,
)


class WebWorkflowSyncTests(unittest.TestCase):
    def test_feedback_code_maps_aconex_statuses(self):
        self.assertEqual(_feedback_code("A-Approved"), "A")
        self.assertEqual(_feedback_code("B-Approved with comments"), "B")
        self.assertEqual(_feedback_code("C-Reject"), "C")
        self.assertEqual(_feedback_code(""), "P")

    def test_web_payload_maps_steps_by_configured_reviewer_order(self):
        payload = _web_payload(
            {
                "step_1_review_status": "B-Approved with comments",
                "step_2_review_status": "",
                "review_status": "B-Approved with comments",
            },
            ("UTIBER", "GDS"),
        )
        self.assertEqual(payload["feedback_status"], {"UTIBER": "B", "GDS": "P"})
        self.assertEqual(payload["feedback"], {"UTIBER": True, "GDS": False, "Terminate": False})
        self.assertFalse(payload["terminate_workflow"])
        self.assertEqual(payload["message"], "Aconex workflow status synchronized.")

    def test_web_payload_accepts_custom_status_message(self):
        payload = _web_payload(
            {"step_1_review_status": "A-Approved", "step_2_review_status": "B-Approved with comments"},
            ("UTIBER", "GDS"),
            message="See supplementary files for details.",
        )
        self.assertEqual(payload["message"], "See supplementary files for details.")

    def test_docflow_message_truncates_to_api_limit(self):
        long_text = "x" * (DOCFLOW_MESSAGE_MAX_LEN + 50)
        message = _docflow_message(long_text)
        self.assertEqual(len(message), DOCFLOW_MESSAGE_MAX_LEN)
        self.assertTrue(message.endswith("…"))

    def test_web_payload_marks_terminated_workflow(self):
        payload = _web_payload({"review_status": "Terminate"}, ("R1", "R2"))
        self.assertTrue(payload["feedback"]["Terminate"])
        self.assertTrue(payload["terminate_workflow"])

    def test_url_helpers_accept_site_root_or_api_root(self):
        self.assertEqual(_api_root("https://feizhang233.com"), "https://feizhang233.com/api")
        self.assertEqual(_api_root("https://feizhang233.com/api/"), "https://feizhang233.com/api")
        self.assertTrue(_workflow_url("https://feizhang233.com", "WF 1/2").endswith("/WF%201%2F2"))
        self.assertEqual(
            _workflow_comments_url("https://feizhang233.com", "WF-1"),
            "https://feizhang233.com/api/external/workflows/WF-1/comments",
        )
        self.assertEqual(
            _bulk_comments_url("https://feizhang233.com"),
            "https://feizhang233.com/api/external/workflow-comments",
        )

    def test_payload_hash_is_stable_for_the_same_payload(self):
        payload = _web_payload({"step_1_review_status": "A-Approved"}, ("R1", "R2"))
        self.assertEqual(_payload_hash(payload), _payload_hash(dict(payload)))

    def test_gds_is_always_reordered_to_step_2(self):
        self.assertEqual(_gds_as_step_2(("GDS", "UTIBER")), ("UTIBER", "GDS"))
        self.assertEqual(_gds_as_step_2(("UTIBER", "GDS")), ("UTIBER", "GDS"))
        with self.assertRaisesRegex(ValueError, "GDS exactly once"):
            _gds_as_step_2(("R1", "R2"))

    def test_docflow_headers_include_both_authentication_layers(self):
        settings = SimpleNamespace(
            cf_access_client_id="service-client-id",
            cf_access_client_secret="service-client-secret",
        )
        self.assertEqual(
            _docflow_headers(settings, "docflow-api-key"),
            {
                "X-API-Key": "docflow-api-key",
                "Accept": "application/json",
                "CF-Access-Client-Id": "service-client-id",
                "CF-Access-Client-Secret": "service-client-secret",
            },
        )

    def test_docflow_headers_reject_partial_service_token(self):
        settings = SimpleNamespace(
            cf_access_client_id="service-client-id",
            cf_access_client_secret="",
        )
        with self.assertRaisesRegex(ValueError, "must be configured together"):
            _docflow_headers(settings, "docflow-api-key")

    @patch("aconex.web_workflow_sync.load_workflow_comments")
    def test_comment_payloads_map_sqlite_rows(self, load_comments):
        load_comments.return_value = [
            {
                "workflow_number": "WF-000527",
                "mail_id": "681130933",
                "from_user": "Mr Slobodan Ivkovic",
                "participant": "P STADION",
                "sent_date": "2025-12-04T11:46:09.640Z",
                "review_comment": "Fix the mortar specification.",
                "comment_text": "ignored when review_comment is present",
            },
            {
                "workflow_number": "WF-000527",
                "mail_id": "681130933",
                "from_user": "Mr Slobodan Ivkovic",
                "sent_date": "2025-12-04T11:46:09.640Z",
                "review_comment": "Fix the mortar specification.",
                "comment_text": "",
            },
            {
                "workflow_number": "WF-000630",
                "mail_id": "681428335",
                "from_user": "",
                "participant": "P STADION",
                "sent_date": "2026-01-21T08:13:23.630Z",
                "review_comment": "",
                "comment_text": "Insufficient length of overlap of bars.",
            },
        ]
        payloads = _comment_payloads_by_workflow()
        self.assertEqual(list(payloads), ["WF-000527", "WF-000630"])
        self.assertEqual(
            payloads["WF-000527"],
            [
                {
                    "external_id": "681130933",
                    "author": "Mr Slobodan Ivkovic",
                    "body": "Fix the mortar specification.",
                    "commented_at": "2025-12-04T11:46:09.640000+00:00",
                }
            ],
        )
        self.assertEqual(payloads["WF-000630"][0]["author"], "P STADION")
        self.assertEqual(
            payloads["WF-000630"][0]["body"],
            "Insufficient length of overlap of bars.",
        )

    @patch("aconex.web_workflow_sync.add_update_run")
    @patch("aconex.web_workflow_sync.mark_manifest_sync")
    @patch("aconex.web_workflow_sync.load_workflow_comments", return_value=[])
    @patch("aconex.web_workflow_sync.load_docflow_sync_state")
    @patch("aconex.web_workflow_sync._load_feedback_reviewers")
    @patch("aconex.web_workflow_sync.load_workflows")
    @patch("aconex.web_workflow_sync.pending_manifest_workflows")
    def test_changed_push_only_consumes_pending_manifest_entries(
        self,
        pending_manifest,
        load_workflows,
        load_reviewers,
        load_hashes,
        _load_comments,
        mark_sync,
        _add_run,
    ):
        settings = SimpleNamespace(
            docflow_base_url="https://docflow.example",
            docflow_api_key="key",
            cf_access_client_id="",
            cf_access_client_secret="",
        )
        pending_manifest.return_value = [
            {"workflow_id": "1", "workflow_number": "WF-000001", "change_types": ["status"]}
        ]
        workflow = {
            "workflow_id": "1",
            "workflow_number": "WF-000001",
            "step_1_review_status": "A-Approved",
            "step_2_review_status": "B-Approved with comments",
            "review_status": "B-Approved with comments",
        }
        load_workflows.return_value = [
            workflow,
            {"workflow_id": "2", "workflow_number": "WF-000002"},
        ]
        load_reviewers.return_value = ("GDS", "UTIBER")
        status_payload = _web_payload(workflow, ("UTIBER", "GDS"))
        load_hashes.return_value = {
            "WF-000001": _payload_hash({"status": status_payload, "comments": []})
        }

        result = push_workflows_to_docflow(settings, changed_only=True)

        self.assertEqual(result.checked, 1)
        self.assertEqual(result.sent, 0)
        mark_sync.assert_called_with("docflow", ["WF-000001"], success=True)

    @patch("aconex.web_workflow_sync.add_update_run")
    @patch("aconex.web_workflow_sync.upsert_docflow_sync_state")
    @patch("aconex.web_workflow_sync.mark_manifest_sync")
    @patch("aconex.web_workflow_sync.load_workflow_comments")
    @patch("aconex.web_workflow_sync.load_docflow_sync_state", return_value={})
    @patch("aconex.web_workflow_sync._load_feedback_reviewers", return_value=("UTIBER", "GDS"))
    @patch("aconex.web_workflow_sync.load_workflows")
    @patch("aconex.web_workflow_sync.pending_manifest_workflows")
    @patch("aconex.web_workflow_sync.requests.Session")
    def test_push_sends_status_and_full_comments(
        self,
        session_cls,
        pending_manifest,
        load_workflows,
        _load_reviewers,
        _load_hashes,
        load_comments,
        mark_sync,
        upsert_hash,
        _add_run,
    ):
        settings = SimpleNamespace(
            docflow_base_url="https://docflow.example",
            docflow_api_key="key",
            cf_access_client_id="",
            cf_access_client_secret="",
        )
        pending_manifest.return_value = [
            {
                "workflow_id": "1",
                "workflow_number": "WF-000001",
                "change_types": ["status", "comments"],
            }
        ]
        load_workflows.return_value = [
            {
                "workflow_id": "1",
                "workflow_number": "WF-000001",
                "step_1_review_status": "B-Approved with comments",
                "step_2_review_status": "",
                "review_status": "B-Approved with comments",
            }
        ]
        load_comments.return_value = [
            {
                "workflow_number": "WF-000001",
                "mail_id": "mail-1",
                "from_user": "Reviewer",
                "sent_date": "2026-07-24T12:00:00Z",
                "review_comment": "Complete Final Mail body.",
                "comment_text": "",
            }
        ]
        session = MagicMock()
        session_cls.return_value.__enter__.return_value = session
        patch_response = MagicMock(status_code=200)
        patch_response.raise_for_status = MagicMock()
        put_response = MagicMock(status_code=200)
        put_response.raise_for_status = MagicMock()
        session.patch.return_value = patch_response
        session.put.return_value = put_response

        result = push_workflows_to_docflow(settings, changed_only=True)

        self.assertEqual(result.sent, 1)
        self.assertEqual(result.comments_sent, 1)
        session.patch.assert_called_once()
        session.put.assert_called_once()
        put_kwargs = session.put.call_args
        self.assertTrue(put_kwargs.args[0].endswith("/comments"))
        self.assertEqual(
            put_kwargs.kwargs["json"]["comments"][0]["body"],
            "Complete Final Mail body.",
        )
        mark_sync.assert_called_with("docflow", ["WF-000001"], success=True)
        upsert_hash.assert_called_once()

    @patch("aconex.web_workflow_sync.add_update_run")
    @patch("aconex.web_workflow_sync.load_workflow_comments")
    @patch("aconex.web_workflow_sync.requests.Session")
    def test_bulk_comment_import_uses_docflow_endpoint(
        self,
        session_cls,
        load_comments,
        _add_run,
    ):
        settings = SimpleNamespace(
            docflow_base_url="https://docflow.example",
            docflow_api_key="key",
            cf_access_client_id="",
            cf_access_client_secret="",
        )
        load_comments.return_value = [
            {
                "workflow_number": "WF-000001",
                "mail_id": "mail-1",
                "from_user": "Reviewer",
                "sent_date": "2026-07-24T12:00:00Z",
                "review_comment": "Body one.",
                "comment_text": "",
            },
            {
                "workflow_number": "WF-000002",
                "mail_id": "mail-2",
                "from_user": "Reviewer",
                "sent_date": "2026-07-24T13:00:00Z",
                "review_comment": "Body two.",
                "comment_text": "",
            },
        ]
        session = MagicMock()
        session_cls.return_value.__enter__.return_value = session
        response = MagicMock(status_code=200)
        response.raise_for_status = MagicMock()
        response.json.return_value = {"imported": 2, "results": []}
        session.put.return_value = response

        result = push_workflow_comments_to_docflow(settings)

        self.assertEqual(result.checked, 2)
        self.assertEqual(result.comments_sent, 2)
        self.assertEqual(result.comments_skipped, 0)
        session.put.assert_called_once()
        self.assertEqual(
            session.put.call_args.args[0],
            "https://docflow.example/api/external/workflow-comments",
        )
        items = session.put.call_args.kwargs["json"]["items"]
        self.assertEqual([item["workflow_number"] for item in items], ["WF-000001", "WF-000002"])


if __name__ == "__main__":
    unittest.main()
