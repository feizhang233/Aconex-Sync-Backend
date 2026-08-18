from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from .client import AconexClient
from .config import Settings
from .excel_formatting import format_workflow_status_workbook
from .export_workflow_status import (
    fetch_current_workflow_status_rows,
    fetch_workflow_status_rows,
    fetch_workflow_status_rows_by_numbers,
)
from .state_db import (
    add_update_run,
    add_workflow_history,
    get_pending_workflows,
    load_workflows,
    upsert_workflow,
)
from .utils import display_date
from .workflow import (
    KEY_STATUS_COLUMNS,
    format_missing_workflow_numbers,
    format_workflow_number,
    has_pushable_review_status,
    is_workflow_completed,
    missing_workflow_numbers,
    parse_workflow_number_int,
    workflow_number_ints,
    workflow_number_width,
)
from .workflow_update_manifest import record_workflow_changes


WORKFLOW_SYNC_COLUMNS = [
    "workflow_id",
    "workflow_number",
    "workflow_number_int",
    "workflow_title",
    "review_outcome",
    "review_status",
    "step_1_completed_time",
    "step_1_due_time",
    "step_1_review_status",
    "step_1_overdue_duration_or_status",
    "step_2_completed_time",
    "step_2_due_time",
    "step_2_review_status",
    "step_2_overdue_duration_or_status",
    "is_completed",
    "last_checked_at",
    "last_changed_at",
]

WORKFLOW_OUTPUT_COLUMNS = [
    "workflow_number",
    "workflow_title",
    "review_status",
    "step_1_completed_time",
    "step_1_due_time",
    "step_1_review_status",
    "step_1_overdue_duration_or_status",
    "step_2_completed_time",
    "step_2_due_time",
    "step_2_review_status",
    "step_2_overdue_duration_or_status",
]

# Re-exported for tests and older callers.
__all__ = [
    "format_missing_workflow_numbers",
    "format_workflow_number",
    "has_pushable_review_status",
    "is_workflow_completed",
    "missing_workflow_numbers",
    "workflow_number_width",
    "workflow_sync_all",
    "workflow_sync_from",
    "workflow_sync_reviewing",
    "workflow_update_open",
]


def workflow_sync_all(
    settings: Settings,
    client: AconexClient,
    *,
    max_pages: int | None = None,
    output: Path | None = None,
    save_raw: bool = False,
) -> Path:
    rows = fetch_workflow_status_rows(
        settings,
        client,
        from_number=None,
        max_pages=max_pages,
        save_raw=save_raw,
    )
    output_path = output or settings.output_dir / "workflow_status_all.xlsx"
    return _sync_rows(
        settings,
        rows,
        command="workflow-sync-all",
        output=output_path,
        source="workflow-sync-all",
    )


def workflow_sync_from(
    settings: Settings,
    client: AconexClient,
    *,
    from_number: int = 800,
    max_pages: int | None = None,
    output: Path | None = None,
    save_raw: bool = False,
) -> Path:
    rows = fetch_workflow_status_rows(
        settings,
        client,
        from_number=from_number,
        max_pages=max_pages,
        save_raw=save_raw,
    )
    output_path = output or settings.output_dir / f"workflow_status_after_{from_number}.xlsx"
    return _sync_rows(
        settings,
        rows,
        command=f"workflow-sync-from --from-number {from_number}",
        output=output_path,
        source="workflow-sync-from",
    )


def workflow_update_open(
    settings: Settings,
    client: AconexClient,
    *,
    max_pages: int | None = None,
    output: Path | None = None,
    save_raw: bool = False,
) -> Path:
    pending_workflows = get_pending_workflows()
    workflow_numbers = [
        str(row["workflow_number"])
        for row in pending_workflows
        if row.get("workflow_number")
    ]
    rows = fetch_workflow_status_rows_by_numbers(
        settings,
        client,
        workflow_numbers=workflow_numbers,
        max_pages=max_pages,
        save_raw=save_raw,
    )
    output_path = output or settings.output_dir / "workflow_status_open_updated.xlsx"
    return _sync_rows(
        settings,
        rows,
        command="workflow-update-open",
        output=output_path,
        source="workflow-update-open",
        checked_override=len(workflow_numbers),
    )


def workflow_sync_reviewing(
    settings: Settings,
    client: AconexClient,
    *,
    max_pages: int | None = None,
    output: Path | None = None,
    save_raw: bool = False,
) -> Path:
    """Sync reviewing workflows and any numbers the current-only pull can miss.

    The current-workflows endpoint discovers unrecorded pending workflows. Existing
    rows marked open in SQLite are also refreshed by workflow number, so a workflow
    that has just completed or terminated is closed out without reprocessing the
    already-complete database history.

    Workflows that are created and completed between two daily runs never appear
    in the current list and are absent from SQLite, so they would otherwise be
    skipped forever. Those holes (plus a short lookahead past the highest known
    number) are fetched by workflow number.
    """
    stored_rows = load_workflows()
    current_rows = fetch_current_workflow_status_rows(
        settings,
        client,
        max_pages=max_pages,
        save_raw=save_raw,
    )
    open_numbers = [
        str(row["workflow_number"])
        for row in get_pending_workflows()
        if row.get("workflow_number")
    ]
    already_numbered = {
        str(row.get("workflow_number") or "")
        for row in current_rows
        if row.get("workflow_number")
    }
    already_numbered.update(open_numbers)
    gap_numbers = [
        number
        for number in format_missing_workflow_numbers(
            known=workflow_number_ints(stored_rows),
            extra_high_water=workflow_number_ints(current_rows),
            width=workflow_number_width(list(stored_rows) + list(current_rows)),
        )
        if number not in already_numbered
    ]
    numbered_rows = fetch_workflow_status_rows_by_numbers(
        settings,
        client,
        workflow_numbers=open_numbers + gap_numbers,
        max_pages=max_pages,
        save_raw=save_raw,
    )
    if gap_numbers:
        print(
            f"Reviewing sync also searched {len(gap_numbers)} missing/lookahead "
            f"workflow numbers"
        )
    # Business key is workflow_number. Numbered-search rows overwrite current-list
    # rows for the same number so local pending and gap-filled workflows get the
    # richest status.
    rows_by_number: dict[str, Mapping[str, Any]] = {
        str(row.get("workflow_number") or ""): row
        for row in current_rows
        if row.get("workflow_number")
    }
    rows_by_number.update(
        {
            str(row.get("workflow_number") or ""): row
            for row in numbered_rows
            if row.get("workflow_number")
        }
    )
    rows = list(rows_by_number.values())
    output_path = output or settings.output_dir / "workflow_status_reviewing.xlsx"
    return _sync_rows(
        settings,
        rows,
        command="workflow-sync-reviewing",
        output=output_path,
        source="workflow-sync-reviewing",
        checked_override=len(rows),
    )


def _sync_rows(
    settings: Settings,
    rows: list[Mapping[str, Any]],
    *,
    command: str,
    output: Path,
    source: str,
    checked_override: int | None = None,
) -> Path:
    checked_at = _utc_now()
    old_by_number = {
        str(row["workflow_number"]): row
        for row in load_workflows()
        if row.get("workflow_number")
    }
    changed_count = 0
    failed_count = 0
    synced_numbers: list[str] = []
    manifest_changes: list[dict[str, Any]] = []

    for raw_row in rows:
        try:
            row = _db_row(raw_row, checked_at=checked_at, source=source)
            old_row = old_by_number.get(row["workflow_number"])
            status_changed = _status_changed(old_row, row)
            if status_changed:
                change_summary = _change_summary(old_row, row)
                add_workflow_history(
                    workflow_id=row.get("workflow_id") or None,
                    workflow_number=row["workflow_number"],
                    checked_at=checked_at,
                    change_summary=change_summary,
                    old_data_json=_history_payload(old_row) if old_row else None,
                    new_data_json=_history_payload(row),
                )
                changed_count += 1
            upsert_workflow(row)
            if status_changed:
                manifest_changes.extend(
                    _manifest_changes_for_row(
                        old_row,
                        row,
                        checked_at=checked_at,
                        summary=change_summary,
                    )
                )
            synced_numbers.append(row["workflow_number"])
        except Exception as exc:
            failed_count += 1
            print(f"Failed to sync workflow row {raw_row.get('workflow_number') or '<unknown>'}: {exc}")

    if manifest_changes:
        record_workflow_changes(manifest_changes)

    add_update_run(
        command=command,
        run_time=checked_at,
        checked_count=checked_override if checked_override is not None else len(rows),
        changed_count=changed_count,
        failed_count=failed_count,
        notes=f"output={output}",
    )

    latest_rows = [
        row for row in load_workflows()
        if row.get("workflow_number") in set(synced_numbers)
    ]
    _write_workflow_status_excel(latest_rows, output)
    print(
        f"Workflow sync complete: checked={checked_override if checked_override is not None else len(rows)}, "
        f"changed={changed_count}, failed={failed_count}"
    )
    return output


def _db_row(row: Mapping[str, Any], *, checked_at: str, source: str) -> dict[str, Any]:
    workflow_number = str(row.get("workflow_number") or "").strip()
    if not workflow_number:
        raise ValueError("workflow_number is required")
    workflow_id = str(row.get("workflow_id") or "").strip() or None

    return {
        "workflow_id": workflow_id,
        "workflow_number": workflow_number,
        "workflow_number_int": parse_workflow_number_int(row.get("workflow_number_int")),
        "workflow_title": row.get("workflow_title") or "",
        "review_outcome": row.get("review_outcome") or "",
        "review_status": row.get("review_status") or "",
        "step_1_completed_time": row.get("step_1_completed_time") or "",
        "step_1_due_time": row.get("step_1_due_time") or "",
        "step_1_review_status": row.get("step_1_review_status") or "",
        "step_1_overdue_duration_or_status": row.get("step_1_overdue_duration_or_status") or "",
        "step_2_completed_time": row.get("step_2_completed_time") or "",
        "step_2_due_time": row.get("step_2_due_time") or "",
        "step_2_review_status": row.get("step_2_review_status") or "",
        "step_2_overdue_duration_or_status": row.get("step_2_overdue_duration_or_status") or "",
        "is_completed": 1 if is_workflow_completed(row) else 0,
        "last_checked_at": checked_at,
        "last_changed_at": None,
        "source": source,
    }


def _manifest_changes_for_row(
    old_row: Mapping[str, Any] | None,
    row: Mapping[str, Any],
    *,
    checked_at: str,
    summary: str,
) -> list[dict[str, Any]]:
    """Build weekly-manifest events for a changed workflow row.

    First-seen pending workflows stay ``new`` only, so DocFlow is not polled
    before a package exists. First-seen rows that already have a final review
    status also emit ``status`` so DocFlow applies the completed result.
    """
    payload = {
        "workflow_id": row.get("workflow_id") or "",
        "workflow_number": row["workflow_number"],
        "changed_at": checked_at,
        "summary": summary,
        "old": _history_payload(old_row) if old_row else None,
        "new": _history_payload(row),
    }
    if old_row is not None:
        return [{**payload, "kind": "status"}]
    changes = [{**payload, "kind": "new"}]
    if has_pushable_review_status(row):
        changes.append({**payload, "kind": "status"})
    return changes


def _status_changed(old_row: Mapping[str, Any] | None, new_row: Mapping[str, Any]) -> bool:
    if old_row is None:
        return True
    return any(old_row.get(column) != new_row.get(column) for column in KEY_STATUS_COLUMNS)


def _change_summary(old_row: Mapping[str, Any] | None, new_row: Mapping[str, Any]) -> str:
    if old_row is None:
        return "New workflow status inserted"
    changes = [
        f"{column}: {old_row.get(column)!r} -> {new_row.get(column)!r}"
        for column in KEY_STATUS_COLUMNS
        if old_row.get(column) != new_row.get(column)
    ]
    return "; ".join(changes) if changes else "No key status change"


def _history_payload(row: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {column: row.get(column) for column in KEY_STATUS_COLUMNS}


def _write_workflow_status_excel(rows: Iterable[Mapping[str, Any]], output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    sorted_rows = sorted(
        rows,
        key=lambda row: (
            row.get("workflow_number_int") is None,
            row.get("workflow_number_int") or 0,
            row.get("workflow_number") or "",
        ),
    )
    frame = pd.DataFrame(sorted_rows, columns=WORKFLOW_OUTPUT_COLUMNS)
    for column in ("step_1_completed_time", "step_1_due_time", "step_2_completed_time", "step_2_due_time"):
        frame[column] = frame[column].map(display_date)
    with pd.ExcelWriter(output) as writer:
        frame.to_excel(writer, sheet_name="Workflow Status", index=False)
    format_workflow_status_workbook(output)
    print(f"Wrote workflow status report: {output}")


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
