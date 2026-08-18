from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .config import Settings
from .google_sheets import GoogleSheetSyncResult, sync_google_sheet_reviewing_with_comments
from .web_workflow_sync import DocFlowPushResult, push_workflows_to_docflow


@dataclass(frozen=True)
class DailyUpdateResult:
    sheet: GoogleSheetSyncResult
    docflow: DocFlowPushResult


def run_daily_update(
    settings: Settings,
    client: Any,
    *,
    spreadsheet_id: str,
    sheet_name: str,
    credentials_file: Path,
    max_pages: int | None = None,
    mail_max_pages: int | None = None,
    save_raw: bool = False,
    web_base_url: str | None = None,
    api_key: str | None = None,
) -> DailyUpdateResult:
    """Weekday pipeline: Aconex → SQLite → Sheets and DocFlow.

    Google Sheets writing stays in ``google_sheets``; this module only
    sequences the local import, mail scan, and downstream pushes.
    """
    sheet = sync_google_sheet_reviewing_with_comments(
        settings,
        client,
        spreadsheet_id=spreadsheet_id,
        sheet_name=sheet_name,
        credentials_file=credentials_file,
        max_pages=max_pages,
        mail_max_pages=mail_max_pages,
        save_raw=save_raw,
    )
    docflow = push_workflows_to_docflow(
        settings,
        changed_only=True,
        base_url=web_base_url,
        api_key=api_key,
    )
    return DailyUpdateResult(sheet=sheet, docflow=docflow)
