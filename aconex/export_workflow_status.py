from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import re
from typing import Iterable

import pandas as pd
import requests
from lxml import etree

from .client import AconexClient
from .config import Settings
from .excel_formatting import format_workflow_status_workbook
from .fetch_workflow import WORKFLOW_ACCEPT_V1
from .utils import display_date


STATUS_COLUMNS = [
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


@dataclass
class WorkflowStep:
    workflow_id: str
    workflow_number: str
    workflow_number_value: int
    workflow_title: str
    step_name: str
    step_index: int | None
    workflow_status: str
    step_status: str
    step_outcome: str
    date_completed: str
    date_due: str
    date_in: str


def export_workflow_status(
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
    output_path.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(rows, columns=STATUS_COLUMNS)
    for column in ("step_1_completed_time", "step_1_due_time", "step_2_completed_time", "step_2_due_time"):
        frame[column] = frame[column].map(display_date)
    with pd.ExcelWriter(output_path) as writer:
        frame.to_excel(writer, sheet_name="Workflow Status", index=False)
    format_workflow_status_workbook(output_path)
    print(f"Wrote workflow status report: {output_path}")
    return output_path


def fetch_workflow_status_rows(
    settings: Settings,
    client: AconexClient,
    *,
    from_number: int | None,
    max_pages: int | None,
    save_raw: bool,
) -> list[dict[str, str]]:
    grouped: dict[str, list[WorkflowStep]] = defaultdict(list)
    scanned_values: list[int] = []
    first_page = _fetch_workflow_page(settings, client, page_number=1, save_raw=save_raw)
    total_pages = first_page.total_pages
    start_page = 1
    if (
        from_number is not None
        and total_pages
        and first_page.max_workflow_number is not None
        and first_page.max_workflow_number < from_number
    ):
        start_page = _find_first_page_at_or_after(settings, client, from_number=from_number, total_pages=total_pages, save_raw=save_raw)

    pages_scanned = 0
    page_number = start_page
    while True:
        if max_pages is not None and pages_scanned >= max_pages:
            break
        page = first_page if page_number == 1 else _fetch_workflow_page(settings, client, page_number=page_number, save_raw=save_raw)
        scanned_values.extend(page.workflow_numbers)
        for step in page.steps:
            if from_number is None or step.workflow_number_value >= from_number:
                grouped[step.workflow_number].append(step)
        pages_scanned += 1
        if total_pages is not None and page_number >= total_pages:
            break
        page_number += 1

    _augment_grouped_steps_by_workflow_number(settings, client, grouped, save_raw=save_raw)
    rows = [_status_row(workflow_number, steps) for workflow_number, steps in grouped.items()]
    if not rows:
        _print_empty_result_hint(from_number or 0, start_page, pages_scanned, scanned_values, total_pages)
    return sorted(rows, key=lambda row: _workflow_number_value(row["workflow_number"]) or 0)


def fetch_current_workflow_status_rows(
    settings: Settings,
    client: AconexClient,
    *,
    max_pages: int | None,
    save_raw: bool,
) -> list[dict[str, str]]:
    """Load workflows currently under review, then enrich them by number."""
    grouped: dict[str, list[WorkflowStep]] = defaultdict(list)
    page_number = 1
    pages_scanned = 0
    total_pages: int | None = None

    while True:
        if max_pages is not None and pages_scanned >= max_pages:
            break
        page = _fetch_workflow_page(
            settings,
            client,
            page_number=page_number,
            save_raw=save_raw,
            status="current",
        )
        total_pages = total_pages or page.total_pages
        for step in page.steps:
            grouped[step.workflow_number].append(step)
        pages_scanned += 1
        if total_pages is not None and page_number >= total_pages:
            break
        page_number += 1

    _augment_grouped_steps_by_workflow_number(settings, client, grouped, save_raw=save_raw)
    return sorted(
        [_status_row(workflow_number, steps) for workflow_number, steps in grouped.items()],
        key=lambda row: _workflow_number_value(row["workflow_number"]) or 0,
    )


def fetch_workflow_status_rows_by_numbers(
    settings: Settings,
    client: AconexClient,
    *,
    workflow_numbers: list[str],
    max_pages: int | None,
    save_raw: bool,
) -> list[dict[str, str]]:
    grouped: dict[str, list[WorkflowStep]] = defaultdict(list)
    requested = {number for number in workflow_numbers if number}
    pages_scanned = 0
    for batch in _batches(sorted(requested, key=lambda value: _workflow_number_value(value) or 0), 10):
        page_number = 1
        total_pages: int | None = None
        while True:
            if max_pages is not None and pages_scanned >= max_pages:
                break
            response = client.get(
                f"/api/projects/{settings.project_id}/workflows/search",
                params={
                    "workflow_number": ",".join(batch),
                    "page_size": "250",
                    "page_number": str(page_number),
                },
                accept=WORKFLOW_ACCEPT_V1,
                raw_group="workflow",
                label=f"workflow_status_open_search_{batch[0]}_page_{page_number}",
                save_raw=save_raw,
            )
            pages_scanned += 1
            root = _parse_xml_bytes(response.content)
            if root is None:
                break
            total_pages = total_pages or _int_attr(root, "TotalPages")
            for workflow in _descendants(root, "Workflow"):
                step = _workflow_step(workflow)
                if step and step.workflow_number in requested:
                    grouped[step.workflow_number].append(step)
            if total_pages is not None and page_number >= total_pages:
                break
            page_number += 1
        if max_pages is not None and pages_scanned >= max_pages:
            break
    return sorted(
        [_status_row(workflow_number, steps) for workflow_number, steps in grouped.items()],
        key=lambda row: _workflow_number_value(row["workflow_number"]) or 0,
    )


@dataclass
class WorkflowPage:
    page_number: int
    total_pages: int | None
    steps: list[WorkflowStep]
    workflow_numbers: list[int]

    @property
    def max_workflow_number(self) -> int | None:
        return max(self.workflow_numbers) if self.workflow_numbers else None


def _fetch_workflow_page(
    settings: Settings,
    client: AconexClient,
    *,
    page_number: int,
    save_raw: bool,
    status: str | None = None,
) -> WorkflowPage:
    path = f"/api/projects/{settings.project_id}/workflows"
    label = "workflow_status"
    if status:
        path = f"{path}/{status}"
        label = f"{label}_{status}"
    response = client.get(
        path,
        params={"page_size": "250", "page_number": str(page_number)},
        accept=WORKFLOW_ACCEPT_V1,
        raw_group="workflow",
        label=f"{label}_page_{page_number}",
        save_raw=save_raw,
    )
    root = _parse_xml_bytes(response.content)
    if root is None:
        return WorkflowPage(page_number=page_number, total_pages=None, steps=[], workflow_numbers=[])
    steps = []
    workflow_numbers = []
    for workflow in _descendants(root, "Workflow"):
        step = _workflow_step(workflow)
        if step:
            steps.append(step)
            workflow_numbers.append(step.workflow_number_value)
    return WorkflowPage(
        page_number=page_number,
        total_pages=_int_attr(root, "TotalPages"),
        steps=steps,
        workflow_numbers=workflow_numbers,
    )


def _augment_grouped_steps_by_workflow_number(
    settings: Settings,
    client: AconexClient,
    grouped: dict[str, list[WorkflowStep]],
    *,
    save_raw: bool,
) -> None:
    workflow_numbers = sorted(grouped, key=lambda value: _workflow_number_value(value) or 0)
    for batch in _batches(workflow_numbers, 10):
        page_number = 1
        total_pages: int | None = None
        while True:
            response = client.get(
                f"/api/projects/{settings.project_id}/workflows/search",
                params={
                    "workflow_number": ",".join(batch),
                    "page_size": "250",
                    "page_number": str(page_number),
                },
                accept=WORKFLOW_ACCEPT_V1,
                raw_group="workflow",
                label=f"workflow_status_search_{batch[0]}_page_{page_number}",
                save_raw=save_raw,
            )
            root = _parse_xml_bytes(response.content)
            if root is None:
                break
            total_pages = total_pages or _int_attr(root, "TotalPages")
            for workflow in _descendants(root, "Workflow"):
                step = _workflow_step(workflow)
                if step and step.workflow_number in grouped:
                    grouped[step.workflow_number].append(step)
            if total_pages is not None and page_number >= total_pages:
                break
            page_number += 1


def _batches(values: list[str], size: int) -> Iterable[list[str]]:
    for index in range(0, len(values), size):
        yield values[index : index + size]


def _find_first_page_at_or_after(
    settings: Settings,
    client: AconexClient,
    *,
    from_number: int,
    total_pages: int,
    save_raw: bool,
) -> int:
    low = 1
    high = total_pages
    candidate = total_pages
    while low <= high:
        mid = (low + high) // 2
        page = _fetch_workflow_page(settings, client, page_number=mid, save_raw=save_raw)
        page_max = page.max_workflow_number
        if page_max is None:
            low = mid + 1
        elif page_max >= from_number:
            candidate = mid
            high = mid - 1
        else:
            low = mid + 1
    return candidate


def _print_empty_result_hint(
    from_number: int,
    start_page: int,
    pages_scanned: int,
    scanned_values: list[int],
    total_pages: int | None,
) -> None:
    print(f"No workflows matched workflow number >= {from_number}.")
    if scanned_values:
        print(f"Scanned workflow number range: {min(scanned_values)} - {max(scanned_values)}")
    print(f"Scanned pages: start_page={start_page}, pages_scanned={pages_scanned}, total_pages={total_pages or 'unknown'}")


def clean_api_error(error: requests.HTTPError) -> str:
    response = error.response
    if response is None:
        return str(error)
    code = ""
    description = ""
    root = _parse_xml_bytes(response.content)
    if root is not None:
        code = _first_text(root, "StatusCode") or _first_text(root, "Code") or _first_text(root, "ErrorCode")
        description = _first_text(root, "Description") or _first_text(root, "ErrorMessage") or _first_text(root, "Message")
    if not description:
        try:
            payload = response.json()
            code = code or str(payload.get("error") or payload.get("code") or "")
            description = str(payload.get("error_description") or payload.get("message") or "")
        except Exception:
            description = response.text[:500].replace("\n", " ")
    parts = [f"Workflow API request failed: HTTP {response.status_code}"]
    if code:
        parts.append(f"error code: {code}")
    if description:
        parts.append(f"error description: {description}")
    return "\n".join(parts)


def _parse_xml_bytes(content: bytes) -> etree._Element | None:
    try:
        parser = etree.XMLParser(recover=True, huge_tree=True, remove_blank_text=True)
        return etree.fromstring(content, parser=parser)
    except Exception:
        return None


def _local(element: etree._Element) -> str:
    return etree.QName(element.tag).localname if isinstance(element.tag, str) else str(element.tag)


def _children(element: etree._Element, name: str) -> list[etree._Element]:
    return [child for child in element if _local(child) == name]


def _first_child(element: etree._Element, name: str) -> etree._Element | None:
    children = _children(element, name)
    return children[0] if children else None


def _text(element: etree._Element, name: str) -> str:
    child = _first_child(element, name)
    if child is None or child.text is None:
        return ""
    return child.text.strip()


def _first_text(element: etree._Element, name: str) -> str:
    for item in element.iter():
        if _local(item) == name and item.text:
            return item.text.strip()
    return ""


def _descendants(element: etree._Element, name: str) -> list[etree._Element]:
    return [item for item in element.iter() if _local(item) == name]


def _int_attr(element: etree._Element, name: str) -> int | None:
    value = element.attrib.get(name, "")
    return int(value) if value.isdigit() else None


def _workflow_number_value(workflow_number: str) -> int | None:
    match = re.search(r"(\d+)", workflow_number or "")
    return int(match.group(1)) if match else None


def _step_index(step_name: str) -> int | None:
    match = re.search(r"\bstep\s*0*([12])\b", step_name or "", flags=re.IGNORECASE)
    return int(match.group(1)) if match else None


def _workflow_step(workflow: etree._Element) -> WorkflowStep | None:
    workflow_number = _text(workflow, "WorkflowNumber")
    number_value = _workflow_number_value(workflow_number)
    if number_value is None:
        return None
    step_name = _text(workflow, "StepName")
    return WorkflowStep(
        workflow_id=_attribute(workflow, "WorkflowId") or _attribute(workflow, "workflowId"),
        workflow_number=workflow_number,
        workflow_number_value=number_value,
        workflow_title=_text(workflow, "WorkflowName"),
        step_name=step_name,
        step_index=_step_index(step_name),
        workflow_status=_text(workflow, "WorkflowStatus"),
        step_status=_text(workflow, "StepStatus"),
        step_outcome=_text(workflow, "StepOutcome"),
        date_completed=_text(workflow, "DateCompleted"),
        date_due=_text(workflow, "DateDue"),
        date_in=_text(workflow, "DateIn"),
    )


def _canonical_workflow_id(steps: list[WorkflowStep]) -> str:
    """Pick a stable auxiliary Aconex id for a business workflow number.

    Aconex assigns a distinct WorkflowId per step (and per document). The business
    key is WorkflowNumber; we keep the lexicographically smallest non-empty id as
    a deterministic reference only.
    """
    ids = sorted({step.workflow_id for step in steps if step.workflow_id})
    return ids[0] if ids else ""


def _status_row(workflow_number: str, steps: list[WorkflowStep]) -> dict[str, str]:
    selected = _select_steps(steps)
    step_1 = selected.get(1)
    step_2 = selected.get(2)
    review_step = next(
        (step for step in (step_2, step_1) if step is not None and step.step_outcome),
        None,
    )
    return {
        "workflow_id": _canonical_workflow_id(steps),
        "workflow_number": workflow_number,
        "workflow_number_int": str(_workflow_number_value(workflow_number) or ""),
        "workflow_title": _first_non_empty(step.workflow_title for step in steps),
        "review_outcome": review_step.step_outcome if review_step else "",
        "review_status": _review_status(
            review_step.step_outcome if review_step else "",
            workflow_status=review_step.workflow_status if review_step else "",
        ),
        "workflow_status": _first_non_empty(step.workflow_status for step in steps),
        "step_1_completed_time": step_1.date_completed if step_1 else "",
        "step_1_due_time": step_1.date_due if step_1 else "",
        "step_1_review_status": _review_status(
            step_1.step_outcome if step_1 else "",
            workflow_status=step_1.workflow_status if step_1 else "",
        ),
        "step_1_overdue_duration_or_status": _overdue_status(step_1) if step_1 else "",
        "step_2_completed_time": step_2.date_completed if step_2 else "",
        "step_2_due_time": step_2.date_due if step_2 else "",
        "step_2_review_status": _review_status(
            step_2.step_outcome if step_2 else "",
            workflow_status=step_2.workflow_status if step_2 else "",
        ),
        "step_2_overdue_duration_or_status": _overdue_status(step_2) if step_2 else "",
    }


def _review_status(step_outcome: str, *, workflow_status: str = "") -> str:
    """Map Aconex StepOutcome values to the project's three review statuses."""
    outcome = " ".join((step_outcome or "").casefold().split())
    normalized_workflow_status = " ".join((workflow_status or "").casefold().split())
    if outcome in {"", "none"} and normalized_workflow_status == "terminated":
        return "Terminate"
    if outcome.startswith("b-") or "approved with comment" in outcome or "reviewed with comment" in outcome:
        return "B-Approved with comments"
    if outcome.startswith("c-") or "reject" in outcome or "revise" in outcome:
        return "C-Reject"
    if outcome.startswith("a-") or outcome in {"approved", "reviewed"}:
        return "A-Approved"
    return ""


def _attribute(element: etree._Element, name: str) -> str:
    for key, value in element.attrib.items():
        if key == name or key.lower() == name.lower():
            return value.strip()
    return ""


def _select_steps(steps: list[WorkflowStep]) -> dict[int, WorkflowStep]:
    selected: dict[int, WorkflowStep] = {}
    for index in (1, 2):
        candidates = [step for step in steps if step.step_index == index]
        if candidates:
            selected[index] = _best_step(candidates)

    if 1 in selected and 2 in selected:
        return selected

    ordered = sorted(steps, key=lambda step: (_parse_datetime(step.date_in) or datetime.max.replace(tzinfo=timezone.utc), step.step_name))
    unique_names: list[WorkflowStep] = []
    seen_names: set[str] = set()
    for step in ordered:
        key = step.step_name or f"{step.date_in}:{step.date_due}:{step.date_completed}"
        if key not in seen_names:
            seen_names.add(key)
            unique_names.append(step)
    if 1 not in selected and unique_names:
        selected[1] = _best_step([unique_names[0]])
    if 2 not in selected and len(unique_names) > 1:
        selected[2] = _best_step([unique_names[1]])
    return selected


def _best_step(steps: list[WorkflowStep]) -> WorkflowStep:
    return sorted(
        steps,
        key=lambda step: (
            _parse_datetime(step.date_completed) or datetime.min.replace(tzinfo=timezone.utc),
            _parse_datetime(step.date_due) or datetime.min.replace(tzinfo=timezone.utc),
        ),
        reverse=True,
    )[0]


def _first_non_empty(values: Iterable[str]) -> str:
    for value in values:
        if value:
            return value
    return ""


def _parse_datetime(value: str) -> datetime | None:
    if not value:
        return None
    try:
        normalized = value.strip()
        if normalized.endswith("Z"):
            normalized = normalized[:-1] + "+00:00"
        parsed = datetime.fromisoformat(normalized)
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _overdue_status(step: WorkflowStep) -> str:
    if not step.date_completed:
        return "pending"
    completed = _parse_datetime(step.date_completed)
    due = _parse_datetime(step.date_due)
    if completed is None or due is None:
        return ""
    delta = completed - due
    if delta.total_seconds() <= 0:
        return "0"
    hours = delta.total_seconds() / 3600
    if hours >= 24:
        return f"{hours / 24:.1f} days"
    return f"{hours:.1f} hours"
