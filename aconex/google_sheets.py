from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import re
from typing import Any

import pandas as pd

from .config import Settings
from .mail_final_scan import extract_review_comment_text, mail_scan_final_for_workflows
from .mail_triggers import (
    collect_final_mail_targets,
    pending_manifest_step_2_final_numbers,
    snapshot_value,
    step_2_final_missing_comment_numbers,
    step_2_pending_to_final_numbers,
    workflow_snapshots,
)
from .state_db import load_workflow_comments, load_workflows
from .utils import display_date
from .workflow_sync import workflow_sync_all, workflow_sync_reviewing
from .workflow_update_manifest import mark_manifest_sync, pending_manifest_workflows


GOOGLE_SHEETS_SCOPE = "https://www.googleapis.com/auth/spreadsheets"
WORKFLOWS_PER_SHEET = 200
LEGACY_DEFAULT_SHEET_NAME = "Workflow Monitor"

# Compatibility aliases used by existing tests.
_workflow_snapshots = workflow_snapshots
_snapshot_value = snapshot_value
_step_2_pending_to_final_numbers = step_2_pending_to_final_numbers
_pending_manifest_step_2_final_numbers = pending_manifest_step_2_final_numbers
_step_2_final_missing_comment_numbers = step_2_final_missing_comment_numbers
WORKFLOW_SHEET_HEADERS = [
    "Workflow Number",
    "Workflow Title",
    "Step1 Due Time",
    "Step1 Review Status",
    "Step 1 Overdue Time",
    "Step2 Due Time",
    "Step2 Review Status",
    "Step 2 Overdue Time",
    "Workflow Comments",
]


@dataclass(frozen=True)
class GoogleSheetSyncResult:
    mode: str
    rows_written: int
    rows_appended: int
    changed_workflows: int = 0
    new_workflows: int = 0


def sync_google_sheet_all(
    settings: Settings,
    client: Any,
    *,
    spreadsheet_id: str,
    sheet_name: str,
    credentials_file: Path,
    max_pages: int | None = None,
    save_raw: bool = False,
) -> GoogleSheetSyncResult:
    workflow_sync_all(settings, client, max_pages=max_pages, save_raw=save_raw)
    rows = _workflow_sheet_rows(load_workflows())
    gateway = GoogleSheetsGateway(spreadsheet_id, sheet_name, credentials_file)
    gateway.replace_all_paginated(rows)
    pending = pending_manifest_workflows("google_sheet")
    mark_manifest_sync(
        "google_sheet",
        (entry["workflow_number"] for entry in pending),
        success=True,
    )
    return GoogleSheetSyncResult(mode="full", rows_written=len(rows), rows_appended=len(rows))


def sync_google_sheet_reviewing(
    settings: Settings,
    client: Any,
    *,
    spreadsheet_id: str,
    sheet_name: str,
    credentials_file: Path,
    max_pages: int | None = None,
    save_raw: bool = False,
) -> GoogleSheetSyncResult:
    output = workflow_sync_reviewing(
        settings,
        client,
        max_pages=max_pages,
        save_raw=save_raw,
    )
    return _sync_manifest_to_google_sheet(
        spreadsheet_id=spreadsheet_id,
        sheet_name=sheet_name,
        credentials_file=credentials_file,
        mode="reviewing",
        refreshed_workflows=len(_workflow_numbers_from_output(output)),
    )


def sync_google_sheet_reviewing_with_comments(
    settings: Settings,
    client: Any,
    *,
    spreadsheet_id: str,
    sheet_name: str,
    credentials_file: Path,
    max_pages: int | None = None,
    mail_max_pages: int | None = None,
    save_raw: bool = False,
) -> GoogleSheetSyncResult:
    """Refresh workflows, scan triggered Final Mail, then consume the manifest."""
    workflows_before_sync = workflow_snapshots(load_workflows())
    output = workflow_sync_reviewing(
        settings,
        client,
        max_pages=max_pages,
        save_raw=save_raw,
    )
    refreshed_numbers = _workflow_numbers_from_output(output)
    workflows_after_sync = load_workflows()
    step_2_final_numbers = collect_final_mail_targets(
        workflows_before_sync,
        workflows_after_sync,
        refreshed_numbers,
    )
    if step_2_final_numbers:
        mail_scan_final_for_workflows(
            settings,
            client,
            workflow_numbers=step_2_final_numbers,
            # Full list scan: inbox pages are not newest-first, so a 72h page-level
            # window previously exited after the first page with details=0.
            hours=None,
            max_pages=mail_max_pages,
            save_raw=save_raw,
        )
    return _sync_manifest_to_google_sheet(
        spreadsheet_id=spreadsheet_id,
        sheet_name=sheet_name,
        credentials_file=credentials_file,
        mode="reviewing-with-comments",
        refreshed_workflows=len(refreshed_numbers),
        step_2_final_numbers=step_2_final_numbers,
    )


def _sync_manifest_to_google_sheet(
    *,
    spreadsheet_id: str,
    sheet_name: str,
    credentials_file: Path,
    mode: str,
    refreshed_workflows: int,
    step_2_final_numbers: set[str] | None = None,
) -> GoogleSheetSyncResult:
    pending = pending_manifest_workflows("google_sheet")
    if not pending:
        return GoogleSheetSyncResult(mode=mode, rows_written=0, rows_appended=0)

    workflow_numbers = {str(entry["workflow_number"]) for entry in pending}
    all_rows = _workflow_sheet_rows(load_workflows())
    rows_by_workflow = {row[0]: row for row in all_rows}
    rows = [
        rows_by_workflow[number]
        for number in sorted(workflow_numbers)
        if number in rows_by_workflow
    ]
    missing_numbers = sorted(workflow_numbers - rows_by_workflow.keys())
    if missing_numbers:
        mark_manifest_sync(
            "google_sheet",
            missing_numbers,
            success=False,
            error=f"Workflow is missing from SQLite: {', '.join(missing_numbers)}",
        )

    valid_numbers = [
        str(entry["workflow_number"])
        for entry in pending
        if str(entry["workflow_number"]) in rows_by_workflow
    ]
    if not valid_numbers:
        return GoogleSheetSyncResult(
            mode=mode,
            rows_written=0,
            rows_appended=0,
            changed_workflows=sum("status" in entry["change_types"] for entry in pending),
            new_workflows=sum("new" in entry["change_types"] for entry in pending),
        )
    try:
        gateway = GoogleSheetsGateway(spreadsheet_id, sheet_name, credentials_file)
        rows_written = gateway.update_changed_workflows(rows, all_rows=rows)
        gateway.append_refresh_log(
            refreshed_workflows=refreshed_workflows,
            changed_workflows=sum("status" in entry["change_types"] for entry in pending),
            new_workflows=sum("new" in entry["change_types"] for entry in pending),
            advanced_to_step_2_numbers=set(),
            completed_step_2_numbers=step_2_final_numbers or set(),
        )
    except Exception as exc:
        mark_manifest_sync(
            "google_sheet",
            valid_numbers,
            success=False,
            error=str(exc),
        )
        raise
    mark_manifest_sync("google_sheet", valid_numbers, success=True)
    return GoogleSheetSyncResult(
        mode=mode,
        rows_written=rows_written,
        rows_appended=sum("new" in entry["change_types"] for entry in pending),
        changed_workflows=sum("status" in entry["change_types"] for entry in pending),
        new_workflows=sum("new" in entry["change_types"] for entry in pending),
    )


def _workflow_numbers_from_output(path: Path) -> set[str]:
    try:
        frame = pd.read_excel(path, dtype=str)
    except Exception:
        return set()
    if "workflow_number" not in frame.columns:
        return set()
    return {value.strip() for value in frame["workflow_number"].fillna("") if value.strip()}


def _workflow_sheet_rows(workflows: list[dict[str, Any]]) -> list[list[str]]:
    comments_by_workflow = _comments_by_workflow()
    rows = []
    for workflow in sorted(
        workflows,
        key=lambda row: (
            row.get("workflow_number_int") is None,
            row.get("workflow_number_int") or 0,
            str(row.get("workflow_number") or ""),
        ),
    ):
        workflow_number = str(workflow.get("workflow_number") or "")
        if not workflow_number:
            continue
        rows.append(
            [
                workflow_number,
                str(workflow.get("workflow_title") or ""),
                display_date(workflow.get("step_1_due_time")),
                str(workflow.get("step_1_review_status") or ""),
                str(workflow.get("step_1_overdue_duration_or_status") or ""),
                display_date(workflow.get("step_2_due_time")),
                str(workflow.get("step_2_review_status") or ""),
                str(workflow.get("step_2_overdue_duration_or_status") or ""),
                comments_by_workflow.get(workflow_number, ""),
            ]
        )
    return rows


def _comments_by_workflow() -> dict[str, str]:
    comments: dict[str, list[str]] = defaultdict(list)
    seen: dict[str, set[str]] = defaultdict(set)
    for row in load_workflow_comments():
        workflow_number = str(row.get("workflow_number") or "")
        comment = next(
            (
                cleaned
                for value in (row.get("review_comment"), row.get("comment_text"))
                if (cleaned := extract_review_comment_text(str(value or "")))
            ),
            "",
        )
        if not workflow_number or not comment:
            continue
        key = comment.casefold()
        if key in seen[workflow_number]:
            continue
        seen[workflow_number].add(key)
        comments[workflow_number].append(comment)
    return {workflow_number: "\n".join(values) for workflow_number, values in comments.items()}


def _clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


class GoogleSheetsGateway:
    def __init__(self, spreadsheet_id: str, sheet_name: str, credentials_file: Path):
        self.spreadsheet_id = spreadsheet_id
        self.sheet_name = sheet_name
        self.service = _build_sheets_service(credentials_file)

    def replace_all_paginated(self, rows: list[list[str]]) -> None:
        pages = [
            rows[index : index + WORKFLOWS_PER_SHEET]
            for index in range(0, len(rows), WORKFLOWS_PER_SHEET)
        ] or [[]]
        sheets = self._ensure_pages(len(pages))
        for sheet, page_rows in zip(sheets, pages):
            title = str(sheet["properties"]["title"])
            quoted_title = _quoted_sheet_name(title)
            self.service.spreadsheets().values().clear(
                spreadsheetId=self.spreadsheet_id,
                range=f"{quoted_title}!A:I",
                body={},
            ).execute()
            self.service.spreadsheets().values().update(
                spreadsheetId=self.spreadsheet_id,
                range=f"{quoted_title}!A1",
                valueInputOption="RAW",
                body={"values": [WORKFLOW_SHEET_HEADERS, *page_rows]},
            ).execute()
        self._remove_unused_pages(sheets)

    def update_changed_workflows(
        self,
        rows: list[list[str]],
        *,
        all_rows: list[list[str]],
    ) -> int:
        """Update only changed rows; initialize all pages only when none exist."""
        pages = self._managed_pages()
        if not pages:
            self.replace_all_paginated(all_rows)
            return len(all_rows)
        if not rows:
            return 0

        rows_by_sheet, locations = self._existing_workflow_locations(pages)
        updates = []
        additions_by_sheet: dict[str, list[list[str]]] = defaultdict(list)
        for row in rows:
            workflow_number = row[0]
            location = locations.get(workflow_number)
            if location is not None:
                title, row_number = location
                updates.append(
                    {
                        "range": f"{_quoted_sheet_name(title)}!A{row_number}:I{row_number}",
                        "values": [row],
                    }
                )
                continue
            title = self._page_with_capacity(pages, rows_by_sheet)
            additions_by_sheet[title].append(row)
            rows_by_sheet[title] += 1

        if updates:
            self.service.spreadsheets().values().batchUpdate(
                spreadsheetId=self.spreadsheet_id,
                body={"valueInputOption": "RAW", "data": updates},
            ).execute()
        for title, additions in additions_by_sheet.items():
            self.service.spreadsheets().values().append(
                spreadsheetId=self.spreadsheet_id,
                range=f"{_quoted_sheet_name(title)}!A:I",
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body={"values": additions},
            ).execute()
        return len(rows)

    def append_refresh_log(
        self,
        *,
        refreshed_workflows: int,
        changed_workflows: int,
        new_workflows: int,
        advanced_to_step_2_numbers: set[str],
        completed_step_2_numbers: set[str],
    ) -> None:
        title = _refresh_log_sheet_name(self.sheet_name)
        self._ensure_named_sheet(title)
        self.service.spreadsheets().values().update(
            spreadsheetId=self.spreadsheet_id,
            range=f"{_quoted_sheet_name(title)}!A1:H1",
            valueInputOption="RAW",
            body={
                "values": [[
                    "Refresh Time (UTC)",
                    "Workflows Refreshed",
                    "Changed Workflows",
                    "New Workflows",
                    "Step 1 → Step 2 Count",
                    "Step 1 → Step 2 Workflows",
                    "Step 2 Completed Count",
                    "Step 2 Completed Workflows",
                ]]
            },
        ).execute()
        self.service.spreadsheets().values().append(
            spreadsheetId=self.spreadsheet_id,
            range=f"{_quoted_sheet_name(title)}!A:H",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={
                "values": [[
                    datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
                    str(refreshed_workflows),
                    str(changed_workflows),
                    str(new_workflows),
                    str(len(advanced_to_step_2_numbers)),
                    ", ".join(sorted(advanced_to_step_2_numbers)),
                    str(len(completed_step_2_numbers)),
                    ", ".join(sorted(completed_step_2_numbers)),
                ]]
            },
        ).execute()

    def _ensure_pages(self, page_count: int) -> list[dict[str, Any]]:
        metadata = self.service.spreadsheets().get(
            spreadsheetId=self.spreadsheet_id,
            fields="sheets.properties",
        ).execute()
        sheets_by_title = {
            str(sheet.get("properties", {}).get("title")): sheet
            for sheet in metadata.get("sheets", [])
            if sheet.get("properties", {}).get("title")
        }
        missing_titles = [
            _page_sheet_name(self.sheet_name, page_number)
            for page_number in range(1, page_count + 1)
            if _page_sheet_name(self.sheet_name, page_number) not in sheets_by_title
        ]
        if missing_titles:
            self.service.spreadsheets().batchUpdate(
                spreadsheetId=self.spreadsheet_id,
                body={
                    "requests": [
                        {"addSheet": {"properties": {"title": title}}}
                        for title in missing_titles
                    ]
                },
            ).execute()
            metadata = self.service.spreadsheets().get(
                spreadsheetId=self.spreadsheet_id,
                fields="sheets.properties",
            ).execute()
            sheets_by_title = {
                str(sheet.get("properties", {}).get("title")): sheet
                for sheet in metadata.get("sheets", [])
                if sheet.get("properties", {}).get("title")
            }
        return [
            sheets_by_title[_page_sheet_name(self.sheet_name, page_number)]
            for page_number in range(1, page_count + 1)
        ]

    def _managed_pages(self) -> list[dict[str, Any]]:
        metadata = self.service.spreadsheets().get(
            spreadsheetId=self.spreadsheet_id,
            fields="sheets.properties",
        ).execute()
        return sorted(
            (
                sheet
                for sheet in metadata.get("sheets", [])
                if _is_managed_page_name(
                    str(sheet.get("properties", {}).get("title") or ""), self.sheet_name
                )
            ),
            key=lambda sheet: _managed_page_number(str(sheet["properties"]["title"])),
        )

    def _existing_workflow_locations(
        self,
        pages: list[dict[str, Any]],
    ) -> tuple[dict[str, int], dict[str, tuple[str, int]]]:
        rows_by_sheet: dict[str, int] = {}
        locations: dict[str, tuple[str, int]] = {}
        for sheet in pages:
            title = str(sheet["properties"]["title"])
            values = self.service.spreadsheets().values().get(
                spreadsheetId=self.spreadsheet_id,
                range=f"{_quoted_sheet_name(title)}!A:I",
            ).execute().get("values", [])
            data_rows = values[1:]
            rows_by_sheet[title] = len(data_rows)
            for row_number, row in enumerate(data_rows, start=2):
                if row and str(row[0]).strip():
                    locations[str(row[0]).strip()] = (title, row_number)
        return rows_by_sheet, locations

    def _page_with_capacity(
        self,
        pages: list[dict[str, Any]],
        rows_by_sheet: dict[str, int],
    ) -> str:
        for sheet in reversed(pages):
            title = str(sheet["properties"]["title"])
            if rows_by_sheet[title] < WORKFLOWS_PER_SHEET:
                return title
        page_number = _managed_page_number(str(pages[-1]["properties"]["title"])) + 1
        new_page = self._ensure_pages(page_number)[-1]
        pages.append(new_page)
        title = str(new_page["properties"]["title"])
        self.service.spreadsheets().values().update(
            spreadsheetId=self.spreadsheet_id,
            range=f"{_quoted_sheet_name(title)}!A1:I1",
            valueInputOption="RAW",
            body={"values": [WORKFLOW_SHEET_HEADERS]},
        ).execute()
        rows_by_sheet[title] = 0
        return title

    def _remove_unused_pages(self, active_sheets: list[dict[str, Any]]) -> None:
        metadata = self.service.spreadsheets().get(
            spreadsheetId=self.spreadsheet_id,
            fields="sheets.properties",
        ).execute()
        active_ids = {sheet["properties"]["sheetId"] for sheet in active_sheets}
        pages_to_remove = [
            sheet
            for sheet in metadata.get("sheets", [])
            if (
                _is_managed_page_name(str(sheet.get("properties", {}).get("title") or ""), self.sheet_name)
                or _is_legacy_default_page_name(str(sheet.get("properties", {}).get("title") or ""))
            )
            and sheet.get("properties", {}).get("sheetId") not in active_ids
        ]
        if not pages_to_remove:
            return
        self.service.spreadsheets().batchUpdate(
            spreadsheetId=self.spreadsheet_id,
            body={
                "requests": [
                    {"deleteSheet": {"sheetId": sheet["properties"]["sheetId"]}}
                    for sheet in pages_to_remove
                ]
            },
        ).execute()

    def _ensure_named_sheet(self, title: str) -> None:
        metadata = self.service.spreadsheets().get(
            spreadsheetId=self.spreadsheet_id,
            fields="sheets.properties",
        ).execute()
        titles = {
            str(sheet.get("properties", {}).get("title"))
            for sheet in metadata.get("sheets", [])
        }
        if title in titles:
            return
        self.service.spreadsheets().batchUpdate(
            spreadsheetId=self.spreadsheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": title}}}]},
        ).execute()


def _build_sheets_service(credentials_file: Path) -> Any:
    if not credentials_file.is_file():
        raise RuntimeError(f"Google service-account JSON was not found: {credentials_file}")
    try:
        from google.oauth2.service_account import Credentials
        from googleapiclient.discovery import build
    except ImportError as exc:
        raise RuntimeError(
            "Google Sheets dependencies are missing. Run: "
            "./.venv/bin/python -m pip install -r requirements.txt"
        ) from exc
    credentials = Credentials.from_service_account_file(
        credentials_file,
        scopes=[GOOGLE_SHEETS_SCOPE],
    )
    return build("sheets", "v4", credentials=credentials, cache_discovery=False)


def _quoted_sheet_name(sheet_name: str) -> str:
    return "'" + sheet_name.replace("'", "''") + "'"


def _page_sheet_name(sheet_name: str, page_number: int) -> str:
    if page_number < 1:
        raise ValueError("page_number must be positive")
    start = (page_number - 1) * WORKFLOWS_PER_SHEET + 1
    end = page_number * WORKFLOWS_PER_SHEET
    return f"{sheet_name}{start:04d}-{end:04d}"


def _is_managed_page_name(title: str, sheet_name: str) -> bool:
    return bool(re.fullmatch(re.escape(sheet_name) + r"\d{4,}-\d{4,}", title))


def _managed_page_number(title: str) -> int:
    match = re.search(r"-(\d+)$", title)
    if match is None:
        raise ValueError(f"Not a managed Workflow page name: {title}")
    return int(match.group(1)) // WORKFLOWS_PER_SHEET


def _is_legacy_default_page_name(title: str) -> bool:
    if title == LEGACY_DEFAULT_SHEET_NAME:
        return True
    return bool(re.fullmatch(re.escape(LEGACY_DEFAULT_SHEET_NAME) + r" \d+", title))


def _refresh_log_sheet_name(sheet_name: str) -> str:
    return f"{sheet_name} Refresh Log"
