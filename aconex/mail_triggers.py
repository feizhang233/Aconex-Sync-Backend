from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from .state_db import load_workflow_comments, load_workflows
from .workflow import is_step_2_final, is_terminated, review_code
from .workflow_update_manifest import pending_manifest_workflows


MISSING_COMMENT_LOOKBACK_DAYS = 14

WORKFLOW_SNAPSHOT_FIELDS = (
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
)


def workflow_snapshots(workflows: list[dict[str, Any]]) -> dict[str, tuple[Any, ...]]:
    return {
        str(workflow["workflow_number"]): tuple(
            workflow.get(field) for field in WORKFLOW_SNAPSHOT_FIELDS
        )
        for workflow in workflows
        if workflow.get("workflow_number")
    }


def snapshot_value(snapshot: tuple[Any, ...], field_name: str) -> Any:
    return snapshot[WORKFLOW_SNAPSHOT_FIELDS.index(field_name)]


def step_2_pending_to_final_numbers(
    before: dict[str, tuple[Any, ...]],
    after: list[dict[str, Any]],
    refreshed_numbers: set[str],
) -> set[str]:
    triggered: set[str] = set()
    for workflow in after:
        workflow_number = str(workflow.get("workflow_number") or "")
        previous = before.get(workflow_number)
        if workflow_number not in refreshed_numbers:
            continue
        if not is_step_2_final(workflow):
            continue
        # First-seen completed workflows (created and finished between daily
        # runs) never had a local pending→final transition.
        if previous is None:
            triggered.add(workflow_number)
            continue
        old_step_2 = review_code(snapshot_value(previous, "step_2_review_status"))
        if old_step_2 == "P":
            triggered.add(workflow_number)
    return triggered


def pending_manifest_step_2_final_numbers() -> set[str]:
    """Recover mail-scan triggers after an earlier automation attempt failed."""
    triggered: set[str] = set()
    current_by_number = {
        str(row.get("workflow_number") or ""): row for row in load_workflows()
    }
    for entry in pending_manifest_workflows("google_sheet"):
        workflow_number = str(entry.get("workflow_number") or "")
        current = current_by_number.get(workflow_number) or {}
        if is_terminated(current):
            continue
        for event in entry.get("events") or []:
            if event.get("kind") != "status":
                continue
            old = event.get("old") or {}
            new = event.get("new") or {}
            if (
                review_code(old.get("step_2_review_status")) == "P"
                and is_step_2_final(new)
            ):
                triggered.add(workflow_number)
    return {number for number in triggered if number}


def step_2_final_missing_comment_numbers(
    *,
    lookback_days: int = MISSING_COMMENT_LOOKBACK_DAYS,
) -> set[str]:
    """Workflows that finished Step 2 recently but still have no Final Mail comments."""
    commented = {
        str(row.get("workflow_number") or "")
        for row in load_workflow_comments()
        if row.get("workflow_number")
    }
    cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)
    missing: set[str] = set()
    for workflow in load_workflows():
        workflow_number = str(workflow.get("workflow_number") or "")
        if not workflow_number or workflow_number in commented:
            continue
        if not is_step_2_final(workflow):
            continue
        completed = _parse_iso_datetime(workflow.get("step_2_completed_time"))
        if completed is None or completed < cutoff:
            continue
        missing.add(workflow_number)
    return missing


def collect_final_mail_targets(
    before: dict[str, tuple[Any, ...]],
    after: list[dict[str, Any]],
    refreshed_numbers: set[str],
) -> set[str]:
    targets = step_2_pending_to_final_numbers(before, after, refreshed_numbers)
    targets.update(pending_manifest_step_2_final_numbers())
    targets.update(step_2_final_missing_comment_numbers())
    return targets


def _parse_iso_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except ValueError:
        return None
