from __future__ import annotations

import argparse
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import requests

from aconex.auth import AconexAuth, AuthError
from aconex.client import AconexClient
from aconex.config import ensure_directories, load_settings
from aconex.export_workflow_status import clean_api_error, export_workflow_status
from aconex.fetch_mail import MailFetcher
from aconex.fetch_workflow import WorkflowFetcher
from aconex.google_sheets import (
    sync_google_sheet_all,
    sync_google_sheet_reviewing,
    sync_google_sheet_reviewing_with_comments,
)
from aconex.mail_final_scan import mail_scan_final_all, mail_scan_final_from, mail_scan_final_recent
from aconex.state_db import DEFAULT_DB_PATH, init_db
from aconex.utils import update_env_tokens
from aconex.workflow_sync import workflow_sync_all, workflow_sync_from, workflow_sync_reviewing, workflow_update_open
from aconex.web_workflow_sync import (
    push_workflow_comments_to_docflow,
    push_workflows_to_docflow,
)
from postprocess.normalize_mail import normalize_mail
from postprocess.normalize_workflow import normalize_workflow


def build_context():
    settings = load_settings()
    ensure_directories(settings)
    auth = AconexAuth(settings)
    client = AconexClient(settings, auth)
    return settings, auth, client


def listen_for_authorization_code(auth: AconexAuth, port: int) -> tuple[str, str]:
    redirect_uri = f"http://localhost:{port}/callback"
    expected_state = auth.settings.authorization_state
    result: dict[str, str] = {}

    class CallbackHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            if parsed.path != "/callback":
                self.send_response(404)
                self.end_headers()
                self.wfile.write(b"Not found")
                return

            params = parse_qs(parsed.query)
            code = params.get("code", [""])[0]
            state = params.get("state", [""])[0]
            error = params.get("error", [""])[0]
            if error:
                result["error"] = error
                result["error_description"] = params.get("error_description", [""])[0]
            elif expected_state and state != expected_state:
                result["error"] = "state_mismatch"
                result["error_description"] = "OAuth callback state did not match ACONEX_STATE."
            elif not code:
                result["error"] = "missing_code"
                result["error_description"] = "OAuth callback did not include a code parameter."
            else:
                result["code"] = code

            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            if "code" in result:
                body = "<html><body><h1>Aconex authorization received</h1><p>You can return to the terminal.</p></body></html>"
            else:
                body = "<html><body><h1>Aconex authorization failed</h1><p>Return to the terminal for details.</p></body></html>"
            self.wfile.write(body.encode("utf-8"))

        def log_message(self, format: str, *args) -> None:
            return

    server = HTTPServer(("127.0.0.1", port), CallbackHandler)
    print(auth.build_authorization_url(redirect_uri=redirect_uri))
    print(f"Listening for OAuth callback on 127.0.0.1:{port}/callback ...")
    server.handle_request()
    server.server_close()
    if "error" in result:
        details = result.get("error_description") or result["error"]
        raise SystemExit(f"Authorization callback failed: {details}")
    return result["code"], redirect_uri


def print_rotation_status(auth: AconexAuth) -> None:
    status = auth.rotation_status()
    print(f"refresh_token_rotated: {str(status['refresh_token_rotated']).lower()}")
    print(f".env updated: {str(status['.env updated']).lower()}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Aconex data automation")
    sub = parser.add_subparsers(dest="command", required=True)

    init_db_parser = sub.add_parser("init-db")
    init_db_parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)

    sub.add_parser("token-info")
    sub.add_parser("print-auth-url")
    exchange_code = sub.add_parser("exchange-code")
    exchange_code.add_argument("--listen", action="store_true")
    exchange_code.add_argument("--port", type=int, default=8080)
    exchange_code.add_argument("--save-env", action="store_true")
    sub.add_parser("fetch-projects")

    mail_list = sub.add_parser("fetch-mail-list")
    mail_list.add_argument("--mail-box")
    mail_list.add_argument("--search-query")
    mail_list.add_argument("--page-size", type=int)
    mail_list.add_argument("--page-number", type=int, default=1)
    mail_list.add_argument("--max-pages", type=int)

    mail_detail = sub.add_parser("fetch-mail-detail")
    mail_detail.add_argument("--mail-id", required=True)

    mail_details = sub.add_parser("fetch-mail-details")
    mail_details.add_argument("--limit", type=int, default=20)
    mail_details.add_argument("--list-file", type=Path)

    mail_attachments = sub.add_parser("fetch-mail-attachments")
    mail_attachments.add_argument("--mail-id", required=True)
    mail_attachments.add_argument("--markedup", action="store_true")

    workflow_list = sub.add_parser("fetch-workflow-list")
    workflow_list.add_argument("--search-mode", default="all", choices=sorted(WorkflowFetcher.SEARCH_PATHS.keys()))
    workflow_list.add_argument("--status", choices=["current", "completed", "terminated"])
    workflow_list.add_argument("--workflow-number")
    workflow_list.add_argument("--updated-after")
    workflow_list.add_argument("--updated-before")
    workflow_list.add_argument("--page-size", type=int)
    workflow_list.add_argument("--page-number", type=int, default=1)
    workflow_list.add_argument("--max-pages", type=int)

    workflow_detail = sub.add_parser("fetch-workflow-detail")
    workflow_detail.add_argument("--workflow-id", required=True)

    workflow_details = sub.add_parser("fetch-workflow-details")
    workflow_details.add_argument("--limit", type=int, default=20)
    workflow_details.add_argument("--list-file", type=Path)

    workflow_status = sub.add_parser("export-workflow-status")
    workflow_status.add_argument("--from-number", type=int, default=800)
    workflow_status.add_argument("--max-pages", type=int)
    workflow_status.add_argument("--output", type=Path)
    workflow_status.add_argument("--save-raw", action="store_true")

    workflow_sync_all_parser = sub.add_parser("workflow-sync-all")
    workflow_sync_all_parser.add_argument("--max-pages", type=int)
    workflow_sync_all_parser.add_argument("--output", type=Path)
    workflow_sync_all_parser.add_argument("--save-raw", action="store_true")

    workflow_sync_from_parser = sub.add_parser("workflow-sync-from")
    workflow_sync_from_parser.add_argument("--from-number", type=int, default=800)
    workflow_sync_from_parser.add_argument("--max-pages", type=int)
    workflow_sync_from_parser.add_argument("--output", type=Path)
    workflow_sync_from_parser.add_argument("--save-raw", action="store_true")

    workflow_update_open_parser = sub.add_parser("workflow-update-open")
    workflow_update_open_parser.add_argument("--max-pages", type=int)
    workflow_update_open_parser.add_argument("--output", type=Path)
    workflow_update_open_parser.add_argument("--save-raw", action="store_true")

    workflow_sync_reviewing_parser = sub.add_parser("workflow-sync-reviewing")
    workflow_sync_reviewing_parser.add_argument("--max-pages", type=int)
    workflow_sync_reviewing_parser.add_argument("--output", type=Path)
    workflow_sync_reviewing_parser.add_argument("--save-raw", action="store_true")

    for command, help_text in (
        ("workflow-db-sync-all", "Fetch all Aconex workflows and update the local SQLite database."),
        ("workflow-db-sync-changed", "Refresh active Aconex workflows and update the local SQLite database."),
    ):
        db_sync_parser = sub.add_parser(command, help=help_text)
        db_sync_parser.add_argument("--max-pages", type=int)
        db_sync_parser.add_argument("--output", type=Path)
        db_sync_parser.add_argument("--save-raw", action="store_true")

    for command, help_text in (
        (
            "docflow-workflow-push-all",
            "Push every locally stored workflow status and Final Mail comments to DocFlow.",
        ),
        (
            "docflow-workflow-push-changed",
            "Push only locally changed workflow statuses and Final Mail comments to DocFlow.",
        ),
    ):
        docflow_push_parser = sub.add_parser(command, help=help_text)
        docflow_push_parser.add_argument("--web-base-url")
        docflow_push_parser.add_argument("--api-key")

    docflow_comments_parser = sub.add_parser(
        "docflow-comments-push-all",
        help=(
            "Import every Final Mail comment snapshot from local SQLite into DocFlow "
            "via PUT /api/external/workflow-comments."
        ),
    )
    docflow_comments_parser.add_argument("--web-base-url")
    docflow_comments_parser.add_argument("--api-key")
    docflow_comments_parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Workflows per bulk import request (default: 100).",
    )

    google_sheet_all_parser = sub.add_parser("google-sheet-sync-all")
    google_sheet_all_parser.add_argument("--spreadsheet-id", required=True)
    google_sheet_all_parser.add_argument("--sheet-name", default="WF")
    google_sheet_all_parser.add_argument("--credentials-file", type=Path)
    google_sheet_all_parser.add_argument("--max-pages", type=int)
    google_sheet_all_parser.add_argument("--save-raw", action="store_true")

    google_sheet_reviewing_parser = sub.add_parser("google-sheet-sync-reviewing")
    google_sheet_reviewing_parser.add_argument("--spreadsheet-id", required=True)
    google_sheet_reviewing_parser.add_argument("--sheet-name", default="WF")
    google_sheet_reviewing_parser.add_argument("--credentials-file", type=Path)
    google_sheet_reviewing_parser.add_argument("--max-pages", type=int)
    google_sheet_reviewing_parser.add_argument("--save-raw", action="store_true")

    google_sheet_update_parser = sub.add_parser(
        "google-sheet-update",
        help="Refresh pending workflows, matching Final-mail comments, and Google Sheets.",
    )
    google_sheet_update_parser.add_argument("--spreadsheet-id", required=True)
    google_sheet_update_parser.add_argument("--sheet-name", default="WF")
    google_sheet_update_parser.add_argument("--credentials-file", type=Path)
    google_sheet_update_parser.add_argument("--max-pages", type=int)
    google_sheet_update_parser.add_argument("--mail-max-pages", type=int)
    google_sheet_update_parser.add_argument("--save-raw", action="store_true")

    daily_update_parser = sub.add_parser(
        "daily-update",
        help=(
            "Weekday pipeline: pull changed Aconex workflows and Final-mail comments "
            "into SQLite, then push updates to DocFlow and Google Sheets."
        ),
    )
    daily_update_parser.add_argument("--spreadsheet-id", required=True)
    daily_update_parser.add_argument("--sheet-name", default="WF")
    daily_update_parser.add_argument("--credentials-file", type=Path)
    daily_update_parser.add_argument("--max-pages", type=int)
    daily_update_parser.add_argument("--mail-max-pages", type=int)
    daily_update_parser.add_argument("--save-raw", action="store_true")
    daily_update_parser.add_argument("--web-base-url")
    daily_update_parser.add_argument("--api-key")

    mail_scan_final_all_parser = sub.add_parser("mail-scan-final-all")
    mail_scan_final_all_parser.add_argument("--max-pages", type=int)
    mail_scan_final_all_parser.add_argument("--output", type=Path)
    mail_scan_final_all_parser.add_argument("--save-raw", action="store_true")
    mail_scan_final_all_parser.add_argument("--debug-candidates", action="store_true")

    mail_scan_final_from_parser = sub.add_parser("mail-scan-final-from")
    mail_scan_final_from_parser.add_argument("--from-number", type=int, default=800)
    mail_scan_final_from_parser.add_argument("--max-pages", type=int)
    mail_scan_final_from_parser.add_argument("--output", type=Path)
    mail_scan_final_from_parser.add_argument("--save-raw", action="store_true")
    mail_scan_final_from_parser.add_argument("--debug-candidates", action="store_true")

    mail_scan_final_recent_parser = sub.add_parser("mail-scan-final-recent")
    mail_scan_final_recent_parser.add_argument("--hours", type=int, default=48)
    mail_scan_final_recent_parser.add_argument("--from-number", type=int, default=800)
    mail_scan_final_recent_parser.add_argument("--max-pages", type=int)
    mail_scan_final_recent_parser.add_argument("--output", type=Path)
    mail_scan_final_recent_parser.add_argument("--save-raw", action="store_true")
    mail_scan_final_recent_parser.add_argument("--debug-candidates", action="store_true")

    sub.add_parser("normalize-mail")
    sub.add_parser("normalize-workflow")

    args = parser.parse_args()

    if args.command == "init-db":
        db_path = init_db(args.db_path)
        print(f"Initialized local state database: {db_path}")
        return

    settings, auth, client = build_context()

    if args.command == "token-info":
        info = auth.token_info(refresh=True)
        print(json.dumps(info, indent=2, ensure_ascii=False))
        if not info["has_refresh_token"]:
            print("Run: python main.py exchange-code --listen --port 8080 --save-env")
    elif args.command == "print-auth-url":
        print(auth.build_authorization_url())
    elif args.command == "exchange-code":
        code = None
        redirect_uri = None
        if args.listen:
            code, redirect_uri = listen_for_authorization_code(auth, args.port)
        token = auth.exchange_authorization_code(code, redirect_uri=redirect_uri)
        env_updated = False
        if args.save_env:
            if not token.refresh_token:
                raise SystemExit("Token response did not include refresh_token; .env was not updated.")
            update_env_tokens(settings.root_dir / ".env", refresh_token=token.refresh_token)
            env_updated = True
        print(f"has_refresh_token: {str(bool(token.refresh_token)).lower()}")
        print(f"expires_in: {token.expires_in}")
        print(f"token_type: {token.token_type}")
        if env_updated:
            print(".env updated")
        elif token.refresh_token:
            print("Save this refresh_token into .env as ACONEX_REFRESH_TOKEN. Do not commit or hard-code it.")
    elif args.command == "fetch-projects":
        raise SystemExit("fetch-projects is intentionally not implemented: the uploaded Mail/Workflow guides do not define a project-list endpoint.")
    elif args.command == "fetch-mail-list":
        MailFetcher(settings, client).fetch_list(
            mail_box=args.mail_box,
            search_query=args.search_query,
            page_size=args.page_size,
            page_number=args.page_number,
            max_pages=args.max_pages,
        )
    elif args.command == "fetch-mail-detail":
        MailFetcher(settings, client).fetch_detail(args.mail_id)
    elif args.command == "fetch-mail-details":
        MailFetcher(settings, client).fetch_details(limit=args.limit, list_file=args.list_file)
    elif args.command == "fetch-mail-attachments":
        MailFetcher(settings, client).fetch_attachments(args.mail_id, markedup=args.markedup)
    elif args.command == "fetch-workflow-list":
        WorkflowFetcher(settings, client).fetch_list(
            search_mode=args.search_mode,
            status=args.status,
            workflow_number=args.workflow_number,
            updated_after=args.updated_after,
            updated_before=args.updated_before,
            page_size=args.page_size,
            page_number=args.page_number,
            max_pages=args.max_pages,
        )
    elif args.command == "fetch-workflow-detail":
        try:
            WorkflowFetcher(settings, client).fetch_detail(args.workflow_id)
        except NotImplementedError as exc:
            raise SystemExit(str(exc)) from exc
    elif args.command == "fetch-workflow-details":
        WorkflowFetcher(settings, client).fetch_details(limit=args.limit, list_file=args.list_file)
    elif args.command == "export-workflow-status":
        try:
            export_workflow_status(
                settings,
                client,
                from_number=args.from_number,
                max_pages=args.max_pages,
                output=args.output,
                save_raw=args.save_raw,
            )
        except requests.HTTPError as exc:
            raise SystemExit(clean_api_error(exc)) from exc
    elif args.command == "workflow-sync-all":
        try:
            workflow_sync_all(
                settings,
                client,
                max_pages=args.max_pages,
                output=args.output,
                save_raw=args.save_raw,
            )
        except requests.HTTPError as exc:
            raise SystemExit(clean_api_error(exc)) from exc
    elif args.command == "workflow-sync-from":
        try:
            workflow_sync_from(
                settings,
                client,
                from_number=args.from_number,
                max_pages=args.max_pages,
                output=args.output,
                save_raw=args.save_raw,
            )
        except requests.HTTPError as exc:
            raise SystemExit(clean_api_error(exc)) from exc
    elif args.command == "workflow-update-open":
        try:
            workflow_update_open(
                settings,
                client,
                max_pages=args.max_pages,
                output=args.output,
                save_raw=args.save_raw,
            )
        except requests.HTTPError as exc:
            raise SystemExit(clean_api_error(exc)) from exc
    elif args.command == "workflow-sync-reviewing":
        try:
            workflow_sync_reviewing(
                settings,
                client,
                max_pages=args.max_pages,
                output=args.output,
                save_raw=args.save_raw,
            )
        except requests.HTTPError as exc:
            raise SystemExit(clean_api_error(exc)) from exc
    elif args.command == "workflow-db-sync-all":
        try:
            workflow_sync_all(
                settings, client, max_pages=args.max_pages, output=args.output, save_raw=args.save_raw
            )
        except requests.HTTPError as exc:
            raise SystemExit(clean_api_error(exc)) from exc
    elif args.command == "workflow-db-sync-changed":
        try:
            workflow_sync_reviewing(
                settings, client, max_pages=args.max_pages, output=args.output, save_raw=args.save_raw
            )
        except requests.HTTPError as exc:
            raise SystemExit(clean_api_error(exc)) from exc
    elif args.command in {"docflow-workflow-push-all", "docflow-workflow-push-changed"}:
        try:
            result = push_workflows_to_docflow(
                settings,
                changed_only=args.command == "docflow-workflow-push-changed",
                base_url=args.web_base_url,
                api_key=args.api_key,
            )
        except (requests.RequestException, ValueError) as exc:
            raise SystemExit(f"DocFlow workflow sync failed: {exc}") from exc
        print(
            f"DocFlow workflow push complete: checked={result.checked}, "
            f"sent={result.sent}, skipped={result.skipped}, failed={result.failed}, "
            f"comments_sent={result.comments_sent}, "
            f"comments_skipped={result.comments_skipped}"
        )
    elif args.command == "docflow-comments-push-all":
        try:
            result = push_workflow_comments_to_docflow(
                settings,
                base_url=args.web_base_url,
                api_key=args.api_key,
                batch_size=args.batch_size,
            )
        except (requests.RequestException, ValueError) as exc:
            raise SystemExit(f"DocFlow comments import failed: {exc}") from exc
        print(
            f"DocFlow comments import complete: checked={result.checked}, "
            f"comments_sent={result.comments_sent}, "
            f"comments_skipped={result.comments_skipped}, failed={result.failed}"
        )
    elif args.command == "google-sheet-sync-all":
        try:
            result = sync_google_sheet_all(
                settings,
                client,
                spreadsheet_id=args.spreadsheet_id,
                sheet_name=args.sheet_name,
                credentials_file=args.credentials_file or settings.root_dir / "google_service_account.json",
                max_pages=args.max_pages,
                save_raw=args.save_raw,
            )
        except requests.HTTPError as exc:
            raise SystemExit(clean_api_error(exc)) from exc
        print(
            f"Google Sheet full sync complete: rows_written={result.rows_written}, "
            f"rows_appended={result.rows_appended}"
        )
    elif args.command == "google-sheet-sync-reviewing":
        try:
            result = sync_google_sheet_reviewing(
                settings,
                client,
                spreadsheet_id=args.spreadsheet_id,
                sheet_name=args.sheet_name,
                credentials_file=args.credentials_file or settings.root_dir / "google_service_account.json",
                max_pages=args.max_pages,
                save_raw=args.save_raw,
            )
        except requests.HTTPError as exc:
            raise SystemExit(clean_api_error(exc)) from exc
        print(
            f"Google Sheet pending sync complete: rows_written={result.rows_written}, "
            f"changed_workflows={result.changed_workflows}, "
            f"new_workflows={result.new_workflows}"
        )
    elif args.command == "google-sheet-update":
        try:
            result = sync_google_sheet_reviewing_with_comments(
                settings,
                client,
                spreadsheet_id=args.spreadsheet_id,
                sheet_name=args.sheet_name,
                credentials_file=args.credentials_file or settings.root_dir / "google_service_account.json",
                max_pages=args.max_pages,
                mail_max_pages=args.mail_max_pages,
                save_raw=args.save_raw,
            )
        except requests.HTTPError as exc:
            raise SystemExit(clean_api_error(exc)) from exc
        except RuntimeError as exc:
            raise SystemExit(f"Google Sheet workflow update failed: {exc}") from exc
        print(
            f"Google Sheet workflow and comments update complete: rows_written={result.rows_written}, "
            f"changed_workflows={result.changed_workflows}, "
            f"new_workflows={result.new_workflows}"
        )
    elif args.command == "daily-update":
        # 1) Aconex → SQLite + weekly manifest (new/changed workflows)
        # 2) Triggered Final Mail comments → SQLite + manifest
        # 3) Unsynced manifest entries → Google Sheets and DocFlow
        try:
            sheet_result = sync_google_sheet_reviewing_with_comments(
                settings,
                client,
                spreadsheet_id=args.spreadsheet_id,
                sheet_name=args.sheet_name,
                credentials_file=args.credentials_file or settings.root_dir / "google_service_account.json",
                max_pages=args.max_pages,
                mail_max_pages=args.mail_max_pages,
                save_raw=args.save_raw,
            )
        except requests.HTTPError as exc:
            raise SystemExit(clean_api_error(exc)) from exc
        except RuntimeError as exc:
            raise SystemExit(f"Daily workflow update failed: {exc}") from exc
        print(
            f"Aconex → DB → Google Sheet complete: rows_written={sheet_result.rows_written}, "
            f"changed_workflows={sheet_result.changed_workflows}, "
            f"new_workflows={sheet_result.new_workflows}"
        )
        try:
            docflow_result = push_workflows_to_docflow(
                settings,
                changed_only=True,
                base_url=args.web_base_url,
                api_key=args.api_key,
            )
        except (requests.RequestException, ValueError) as exc:
            raise SystemExit(f"DocFlow workflow sync failed: {exc}") from exc
        print(
            f"DB → DocFlow complete: checked={docflow_result.checked}, "
            f"sent={docflow_result.sent}, skipped={docflow_result.skipped}, "
            f"failed={docflow_result.failed}, "
            f"comments_sent={docflow_result.comments_sent}, "
            f"comments_skipped={docflow_result.comments_skipped}"
        )
    elif args.command == "mail-scan-final-all":
        try:
            mail_scan_final_all(
                settings,
                client,
                max_pages=args.max_pages,
                output=args.output,
                save_raw=args.save_raw,
                debug_candidates=args.debug_candidates,
            )
        except requests.HTTPError as exc:
            raise SystemExit(clean_api_error(exc)) from exc
    elif args.command == "mail-scan-final-from":
        try:
            mail_scan_final_from(
                settings,
                client,
                from_number=args.from_number,
                max_pages=args.max_pages,
                output=args.output,
                save_raw=args.save_raw,
                debug_candidates=args.debug_candidates,
            )
        except requests.HTTPError as exc:
            raise SystemExit(clean_api_error(exc)) from exc
    elif args.command == "mail-scan-final-recent":
        try:
            mail_scan_final_recent(
                settings,
                client,
                hours=args.hours,
                from_number=args.from_number,
                max_pages=args.max_pages,
                output=args.output,
                save_raw=args.save_raw,
                debug_candidates=args.debug_candidates,
            )
        except requests.HTTPError as exc:
            raise SystemExit(clean_api_error(exc)) from exc
    elif args.command == "normalize-mail":
        normalize_mail(settings.parsed_dir, settings.output_dir)
    elif args.command == "normalize-workflow":
        normalize_workflow(settings.parsed_dir, settings.output_dir)
    if (
        args.command == "token-info"
        or args.command.startswith("fetch-")
        or args.command == "export-workflow-status"
        or args.command.startswith("workflow-")
        or args.command.startswith("mail-scan-")
        or args.command == "google-sheet-sync-all"
        or args.command == "google-sheet-sync-reviewing"
        or args.command == "google-sheet-update"
        or args.command == "daily-update"
    ):
        print_rotation_status(auth)


if __name__ == "__main__":
    try:
        main()
    except AuthError as exc:
        raise SystemExit(str(exc)) from exc
