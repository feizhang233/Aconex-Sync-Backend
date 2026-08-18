from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any


# Probe this many numbers past the highest known workflow so a brand-new
# workflow that is created and completed before the next daily run is still
# discovered even when nothing newer remains in the current list.
MISSING_WORKFLOW_LOOKAHEAD = 20

COMPLETED_STATUS_MARKERS = ("completed", "closed", "terminate", "terminated")

KEY_STATUS_COLUMNS = (
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


def format_workflow_number(value: int, *, width: int = 6) -> str:
    return f"WF-{int(value):0{width}d}"


def parse_workflow_number_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        digits = "".join(character for character in str(value) if character.isdigit())
        if not digits:
            return None
        try:
            return int(digits)
        except ValueError:
            return None


def workflow_number_width(rows: Iterable[Mapping[str, Any]]) -> int:
    widths = [
        len("".join(character for character in str(row.get("workflow_number") or "") if character.isdigit()))
        for row in rows
    ]
    return max((width for width in widths if width), default=6)


def workflow_number_ints(rows: Iterable[Mapping[str, Any]]) -> list[int]:
    values: list[int] = []
    for row in rows:
        number = parse_workflow_number_int(row.get("workflow_number_int"))
        if number is None:
            number = parse_workflow_number_int(row.get("workflow_number"))
        if number is not None:
            values.append(number)
    return values


def missing_workflow_numbers(
    known: Iterable[int],
    *,
    extra_high_water: Iterable[int] = (),
    lookahead: int = MISSING_WORKFLOW_LOOKAHEAD,
    start: int = 1,
) -> list[int]:
    """Return unused integers in ``[start, high]`` plus a short lookahead.

    ``high`` is the larger of the locally stored max and any extra high-water
    marks (typically numbers seen on the current-workflows endpoint).
    """
    known_set = {int(value) for value in known if value}
    extras = {int(value) for value in extra_high_water if value}
    seen = known_set | extras
    if not seen:
        return []
    high = max(seen)
    missing = [value for value in range(start, high + 1) if value not in seen]
    if lookahead > 0:
        missing.extend(range(high + 1, high + 1 + lookahead))
    return missing


def format_missing_workflow_numbers(
    known: Iterable[int],
    *,
    extra_high_water: Iterable[int] = (),
    lookahead: int = MISSING_WORKFLOW_LOOKAHEAD,
    start: int = 1,
    width: int = 6,
) -> list[str]:
    return [
        format_workflow_number(value, width=width)
        for value in missing_workflow_numbers(
            known,
            extra_high_water=extra_high_water,
            lookahead=lookahead,
            start=start,
        )
    ]


def review_code(value: Any) -> str:
    """Map an Aconex review string to A/B/C, otherwise pending (P)."""
    normalized = str(value or "").strip().upper()
    return normalized[0] if normalized[:1] in {"A", "B", "C"} else "P"


def is_terminated(row: Mapping[str, Any]) -> bool:
    status = str(row.get("review_status") or "").strip().casefold()
    return status in {"terminate", "terminated"}


def is_workflow_completed(row: Mapping[str, Any]) -> bool:
    step_1_completed = bool(str(row.get("step_1_completed_time") or "").strip())
    step_2_completed = bool(str(row.get("step_2_completed_time") or "").strip())
    if step_1_completed and step_2_completed:
        return True

    status_text = " ".join(
        str(row.get(key) or "")
        for key in (
            "workflow_status",
            "step_status",
            "review_status",
        )
    ).lower()
    return any(marker in status_text for marker in COMPLETED_STATUS_MARKERS)


def has_pushable_review_status(row: Mapping[str, Any]) -> bool:
    """True when a first-seen row already carries a status DocFlow should apply."""
    if is_workflow_completed(row):
        return True
    if is_terminated(row):
        return True
    return any(
        review_code(row.get(key)) in {"A", "B", "C"}
        for key in ("review_status", "step_1_review_status", "step_2_review_status")
    )


def is_step_2_final(row: Mapping[str, Any]) -> bool:
    return (not is_terminated(row)) and review_code(row.get("step_2_review_status")) in {"A", "B", "C"}


def normalize_workflow_status_fields(row: Mapping[str, Any]) -> dict[str, Any]:
    """Apply write-time status conventions. Does not run on read."""
    data = dict(row)
    if str(data.get("review_outcome") or "").strip().casefold() == "none":
        if str(data.get("review_status") or "") != "Terminate":
            data["review_status"] = "Terminate"
    for column in (
        "step_1_overdue_duration_or_status",
        "step_2_overdue_duration_or_status",
    ):
        if data.get(column) == "審批中":
            data[column] = "pending"
    return data
