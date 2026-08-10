from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from html import unescape
from pathlib import Path
import re
from typing import Any

import pandas as pd
from lxml import etree

from .client import AconexClient
from .config import Settings
from .fetch_mail import DEFAULT_RETURN_FIELDS, MAIL_ACCEPT_V2
from .state_db import (
    add_update_run,
    load_workflow_comments,
    load_workflows,
    upsert_workflow_comment,
)
from .workflow_update_manifest import record_workflow_changes


COMMENT_OUTPUT_COLUMNS = [
    "workflow_number",
    "workflow_number_int",
    "mail_id",
    "mail_number",
    "mail_subject",
    "sent_date",
    "from_user",
    "comment_text",
    "doc_no",
    "review_step",
    "participant",
    "review_outcome",
    "review_comment",
    "source",
    "created_at",
]

COMMENT_FIELD_PRIORITY = (
    "Comments",
    "Comment",
    "Body",
    "Message",
    "Remarks",
    "Response",
    "MailData",
    "MailBody",
)

WORKFLOW_NUMBER_RE = re.compile(r"\bWF[\s-]*0*(\d{1,9})\b", re.IGNORECASE)
FINAL_WORKFLOW_SUBJECT_RE = re.compile(r"^Final\s*\(\s*WF[-\s_]?0*(\d{3,6})\s*\)", re.IGNORECASE)
REVIEW_HISTORY_HEADERS = {
    "doc no": "doc_no",
    "step": "review_step",
    "participant": "participant",
    "review outcome": "review_outcome",
    "comments": "review_comment",
}

REVIEW_METADATA_ROW_RE = re.compile(
    r"Step\s*[12]\s*-\s*[^|]*\|\s*[^|]*\|\s*"
    r"(?:A-Approved|B-Approved\s+with\s+comments|C-Rejected|C-Reject|"
    r"Approved\s+with\s+Comments|Approved|Rejected|None|Pending(?:\s+Action)?)\s*\|?",
    re.IGNORECASE,
)


@dataclass
class MailSummary:
    mail_id: str
    mail_number: str
    reference_number: str
    subject: str
    sent_date: str
    from_user: str


def mail_scan_final_all(
    settings: Settings,
    client: AconexClient,
    *,
    max_pages: int | None = None,
    output: Path | None = None,
    save_raw: bool = False,
    debug_candidates: bool = False,
) -> Path:
    output_path = output or settings.output_dir / "mail_final_workflow_comments_all.xlsx"
    return _scan_mail(
        settings,
        client,
        command="mail-scan-final-all",
        source="mail-scan-final-all",
        output=output_path,
        from_number=None,
        hours=None,
        max_pages=max_pages,
        save_raw=save_raw,
        debug_candidates=debug_candidates,
        fail_on_error=False,
    )


def mail_scan_final_from(
    settings: Settings,
    client: AconexClient,
    *,
    from_number: int = 800,
    max_pages: int | None = None,
    output: Path | None = None,
    save_raw: bool = False,
    debug_candidates: bool = False,
) -> Path:
    output_path = output or settings.output_dir / f"mail_final_workflow_comments_after_{from_number}.xlsx"
    return _scan_mail(
        settings,
        client,
        command=f"mail-scan-final-from --from-number {from_number}",
        source="mail-scan-final-from",
        output=output_path,
        from_number=from_number,
        hours=None,
        max_pages=max_pages,
        save_raw=save_raw,
        debug_candidates=debug_candidates,
        fail_on_error=False,
    )


def mail_scan_final_recent(
    settings: Settings,
    client: AconexClient,
    *,
    hours: int = 48,
    from_number: int = 800,
    max_pages: int | None = None,
    output: Path | None = None,
    save_raw: bool = False,
    debug_candidates: bool = False,
) -> Path:
    output_path = output or settings.output_dir / "mail_final_workflow_comments_recent.xlsx"
    return _scan_mail(
        settings,
        client,
        command=f"mail-scan-final-recent --hours {hours} --from-number {from_number}",
        source="mail-scan-final-recent",
        output=output_path,
        from_number=from_number,
        hours=hours,
        max_pages=max_pages,
        save_raw=save_raw,
        debug_candidates=debug_candidates,
        fail_on_error=False,
    )


def mail_scan_final_for_workflows(
    settings: Settings,
    client: AconexClient,
    *,
    workflow_numbers: Iterable[str],
    hours: int | None = None,
    max_pages: int | None = None,
    output: Path | None = None,
    save_raw: bool = False,
) -> Path:
    """Scan Final workflow mails whose workflow number is in ``workflow_numbers``.

    The full mail list is still paged through because the Aconex Mail API does not
    support an exact Final-workflow-number query, and inbox pages are not ordered by
    sent date. Mail details are requested only after the subject matches one of the
    supplied workflow numbers.

    ``hours`` may limit which list rows are considered, but it is not used for
    page-level early exit (that assumption is unsafe with unsorted pages).
    """
    matched_numbers = {
        normalized[0]
        for value in workflow_numbers
        if (normalized := normalize_workflow_number(str(value))) is not None
    }
    output_path = output or settings.output_dir / "mail_final_workflow_comments_matched.xlsx"
    return _scan_mail(
        settings,
        client,
        command="mail-scan-final-for-workflows",
        source="mail-scan-final-for-workflows",
        output=output_path,
        from_number=None,
        hours=hours,
        max_pages=max_pages,
        save_raw=save_raw,
        debug_candidates=False,
        workflow_numbers=matched_numbers,
        fail_on_error=True,
    )


def normalize_workflow_number(value: str) -> tuple[str, int] | None:
    match = WORKFLOW_NUMBER_RE.search(value or "")
    if not match:
        return None
    number = int(match.group(1))
    return f"WF-{number:06d}", number


def extract_workflow_numbers(*values: str) -> list[tuple[str, int]]:
    found: dict[int, tuple[str, int]] = {}
    for value in values:
        for match in WORKFLOW_NUMBER_RE.finditer(value or ""):
            number = int(match.group(1))
            found[number] = (f"WF-{number:06d}", number)
    return [found[key] for key in sorted(found)]


def extract_workflow_review_history_comments(mail_detail_xml: str) -> list[dict[str, str]]:
    root = _parse_xml_text(mail_detail_xml)
    message_values = _message_body_values(root) if root is not None else [mail_detail_xml]
    rows: list[dict[str, str]] = []
    for value in message_values:
        rows.extend(_review_history_rows_from_html(value))
    return rows


def format_review_history_comments(rows: list[dict[str, str]]) -> str:
    comments: list[str] = []
    seen: set[str] = set()
    for row in rows:
        _append_unique_comment(
            comments,
            seen,
            extract_review_comment_text(row.get("review_comment", "")),
        )
    return "\n".join(comments)


def extract_review_comment_text(value: str) -> str:
    """Keep only review comments, removing repeated step/participant/outcome metadata."""
    text = _clean_text(value)
    matches = list(REVIEW_METADATA_ROW_RE.finditer(text))
    if not matches:
        return text

    comments: list[str] = []
    seen: set[str] = set()
    cursor = 0
    for match in matches:
        _append_unique_comment(comments, seen, text[cursor : match.start()])
        cursor = match.end()
    _append_unique_comment(comments, seen, text[cursor:])
    return "\n".join(comments)


def _append_unique_comment(comments: list[str], seen: set[str], value: str) -> None:
    comment = _clean_text(value).strip(" |")
    if not comment:
        return
    key = comment.casefold()
    if key not in seen:
        seen.add(key)
        comments.append(comment)


def _scan_mail(
    settings: Settings,
    client: AconexClient,
    *,
    command: str,
    source: str,
    output: Path,
    from_number: int | None,
    hours: int | None,
    max_pages: int | None,
    save_raw: bool,
    debug_candidates: bool,
    workflow_numbers: set[str] | None = None,
    fail_on_error: bool = False,
) -> Path:
    created_at = _utc_now()
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours) if hours is not None else None
    checked_count = 0
    detail_count = 0
    changed_count = 0
    failed_count = 0
    run_rows: list[dict[str, Any]] = []
    changed_mail_ids: dict[str, set[str]] = {}

    for summary in _iter_mail_summaries(settings, client, max_pages=max_pages, save_raw=save_raw):
        checked_count += 1
        if cutoff is not None and not _is_recent(summary.sent_date, cutoff):
            continue
        subject_workflow = _subject_workflow_number(summary.subject)
        if subject_workflow is None:
            continue
        if workflow_numbers is not None and subject_workflow[0] not in workflow_numbers:
            continue
        try:
            detail_count += 1
            detail = _fetch_mail_detail(settings, client, summary.mail_id, save_raw=save_raw)
            rows = _comment_rows_from_detail(
                detail,
                summary,
                subject_workflow=subject_workflow,
                source=source,
                created_at=created_at,
            )
            if debug_candidates:
                debug_text = rows[0]["comment_text"] if rows else ""
                print(
                    "Final Workflow candidate: "
                    f"mail_id={summary.mail_id} | "
                    f"subject={summary.subject} | "
                    f"workflow_number={subject_workflow[0]} | "
                    f"review_history_rows_count={rows[0].get('_review_history_rows_count', 0) if rows else 0} | "
                    f"comment_text_preview={debug_text[:200]}"
                )
            for row in rows:
                if from_number is not None and int(row["workflow_number_int"]) < from_number:
                    continue
                row.pop("_review_history_rows_count", None)
                if upsert_workflow_comment(row):
                    changed_count += 1
                    changed_mail_ids.setdefault(str(row["workflow_number"]), set()).add(
                        str(row["mail_id"])
                    )
                run_rows.append(row)
        except Exception as exc:
            failed_count += 1
            print(f"Failed to scan mail {summary.mail_id or '<unknown>'}: {exc}")

    if changed_mail_ids:
        workflows_by_number = {
            str(row.get("workflow_number") or ""): row
            for row in load_workflows()
            if row.get("workflow_number")
        }
        manifest_changes = []
        for workflow_number, mail_ids in changed_mail_ids.items():
            workflow = workflows_by_number.get(workflow_number)
            if workflow is None:
                continue
            manifest_changes.append(
                {
                    "workflow_id": str(workflow.get("workflow_id") or ""),
                    "workflow_number": workflow_number,
                    "kind": "comments",
                    "changed_at": created_at,
                    "summary": "Final Mail review comments changed",
                    "mail_ids": sorted(mail_ids),
                }
            )
        if manifest_changes:
            record_workflow_changes(manifest_changes)

    add_update_run(
        command=command,
        run_time=created_at,
        checked_count=checked_count,
        changed_count=changed_count,
        failed_count=failed_count,
        notes=f"details_checked={detail_count}; output={output}",
    )
    if not run_rows:
        run_keys: set[tuple[str, str]] = set()
    else:
        run_keys = {(str(row["workflow_number"]), str(row["mail_id"])) for row in run_rows}
    output_rows = [
        row for row in load_workflow_comments()
        if (str(row["workflow_number"]), str(row["mail_id"])) in run_keys
    ]
    _write_comments_excel(output_rows, output)
    print(
        f"Mail final workflow scan complete: checked={checked_count}, "
        f"details={detail_count}, changed={changed_count}, failed={failed_count}"
    )
    if fail_on_error and failed_count:
        raise RuntimeError(
            f"Final Mail scan failed for {failed_count} relevant message(s); "
            "the manifest remains pending for retry"
        )
    return output


def _iter_mail_summaries(
    settings: Settings,
    client: AconexClient,
    *,
    max_pages: int | None,
    save_raw: bool,
) -> Iterable[MailSummary]:
    """Yield mail list rows page by page.

    Inbox pages are not reliably ordered by sent date, so callers that need a
    time window must filter each summary themselves rather than stopping early.
    """
    page_number = 1
    pages_scanned = 0
    total_pages: int | None = None
    while True:
        if max_pages is not None and pages_scanned >= max_pages:
            break
        response = client.get(
            f"/api/projects/{settings.project_id}/mail",
            params={
                "mail_box": settings.default_mail_box,
                "return_fields": ",".join(DEFAULT_RETURN_FIELDS),
                "search_type": "PAGED",
                "page_size": str(settings.page_size),
                "page_number": str(page_number),
            },
            accept=MAIL_ACCEPT_V2,
            raw_group="mail",
            label=f"mail_final_scan_list_page_{page_number}",
            save_raw=save_raw,
        )
        pages_scanned += 1
        root = _parse_xml_bytes(response.content)
        if root is None:
            break
        total_pages = total_pages or _int_attr(root, "TotalPages")
        summaries = [_mail_summary(mail) for mail in _descendants(root, "Mail")]
        summaries = [summary for summary in summaries if summary.mail_id]
        for summary in summaries:
            yield summary
        if total_pages is not None and page_number >= total_pages:
            break
        page_number += 1


def _fetch_mail_detail(settings: Settings, client: AconexClient, mail_id: str, *, save_raw: bool) -> etree._Element:
    response = client.get(
        f"/api/projects/{settings.project_id}/mail/{mail_id}",
        accept=MAIL_ACCEPT_V2,
        raw_group="mail",
        label=f"mail_final_scan_detail_{mail_id}",
        save_raw=save_raw,
    )
    root = _parse_xml_bytes(response.content)
    if root is None:
        raise ValueError("Mail detail response was not valid XML")
    return root


def _comment_rows_from_detail(
    detail: etree._Element,
    summary: MailSummary,
    *,
    subject_workflow: tuple[str, int],
    source: str,
    created_at: str,
) -> list[dict[str, Any]]:
    fields = _detail_fields(detail, summary)
    workflow_numbers = [subject_workflow]
    review_rows = extract_workflow_review_history_comments(_xml_to_string(detail))
    comment_text = format_review_history_comments(review_rows)
    if not comment_text:
        comment_text = extract_review_comment_text(
            _first_non_empty(fields.get(_field_key(name), "") for name in COMMENT_FIELD_PRIORITY)
        )
    if not comment_text:
        return []
    first_review = review_rows[0] if review_rows else {}
    rows: list[dict[str, Any]] = []
    for workflow_number, workflow_number_int in workflow_numbers:
        rows.append(
            {
                "workflow_number": workflow_number,
                "workflow_number_int": workflow_number_int,
                "mail_id": summary.mail_id,
                "mail_number": fields.get("mail_number", ""),
                "mail_subject": fields.get("subject", ""),
                "sent_date": fields.get("sent_date", ""),
                "from_user": fields.get("from_user", ""),
                "comment_text": comment_text,
                "doc_no": first_review.get("doc_no", ""),
                "review_step": first_review.get("review_step", ""),
                "participant": first_review.get("participant", ""),
                "review_outcome": first_review.get("review_outcome", ""),
                "review_comment": first_review.get("review_comment", ""),
                "source": source,
                "created_at": created_at,
                "_review_history_rows_count": len(review_rows),
            }
        )
    return rows


def _detail_fields(detail: etree._Element, summary: MailSummary) -> dict[str, str]:
    fields = {
        "subject": _text(detail, "Subject") or summary.subject,
        "mail_number": _text(detail, "MailNo") or summary.mail_number,
        "reference_number": _text(detail, "ReferenceNumber") or summary.reference_number,
        "sent_date": _text(detail, "SentDate") or summary.sent_date,
        "from_user": _nested_text(detail, "FromUserDetails", "Name") or summary.from_user,
        "attachment_titles": "\n".join(_texts(detail, "Title")),
        "attachment_numbers": "\n".join(_texts(detail, "DocumentNo")),
    }
    for name in COMMENT_FIELD_PRIORITY:
        fields[_field_key(name)] = _clean_text(_raw_text_or_markup(detail, name) or _text(detail, name))
    return fields


def _mail_summary(mail: etree._Element) -> MailSummary:
    return MailSummary(
        mail_id=_attribute(mail, "MailId"),
        mail_number=_text(mail, "MailNo"),
        reference_number=_text(mail, "ReferenceNumber"),
        subject=_text(mail, "Subject"),
        sent_date=_text(mail, "SentDate"),
        from_user=_nested_text(mail, "FromUserDetails", "Name"),
    )


def _subject_workflow_number(subject: str) -> tuple[str, int] | None:
    match = FINAL_WORKFLOW_SUBJECT_RE.search(subject or "")
    if not match:
        return None
    number = int(match.group(1))
    return f"WF-{number:06d}", number


def _is_recent(value: str, cutoff: datetime) -> bool:
    parsed = _parse_datetime(value)
    return parsed is not None and parsed >= cutoff


def _parse_xml_bytes(content: bytes) -> etree._Element | None:
    try:
        parser = etree.XMLParser(recover=True, huge_tree=True, remove_blank_text=True)
        return etree.fromstring(content, parser=parser)
    except Exception:
        return None


def _parse_xml_text(content: str) -> etree._Element | None:
    return _parse_xml_bytes(content.encode("utf-8"))


def _xml_to_string(element: etree._Element) -> str:
    return etree.tostring(element, encoding="unicode")


def _message_body_values(root: etree._Element) -> list[str]:
    values: list[str] = []
    for name in ("MESSAGE", "Body", "MailBody", "Message", "HTMLBody", "HtmlBody", "MailData"):
        value = _raw_text_or_markup(root, name)
        if value and value not in values:
            values.append(value)
    return values or [_xml_to_string(root)]


def _review_history_rows_from_html(value: str) -> list[dict[str, str]]:
    html_text = unescape(value or "")
    if "<table" not in html_text.lower():
        return []
    root = etree.HTML(html_text)
    if root is None:
        return []
    rows: list[dict[str, str]] = []
    for table in root.xpath(".//table"):
        table_rows = _table_rows(table)
        if not table_rows:
            continue
        header_index, mapping = _review_history_header_mapping(table_rows)
        if header_index is None:
            continue
        for cells in table_rows[header_index + 1 :]:
            if not any(cells):
                continue
            row = {
                key: cells[index] if index < len(cells) else ""
                for key, index in mapping.items()
            }
            if any(row.values()):
                rows.append(row)
    return rows


def _table_rows(table: etree._Element) -> list[list[str]]:
    rows: list[list[str]] = []
    for tr in table.xpath(".//tr"):
        cells = [
            _clean_text(" ".join(cell.itertext()))
            for cell in tr.xpath("./th|./td")
        ]
        if cells:
            rows.append(cells)
    return rows


def _review_history_header_mapping(rows: list[list[str]]) -> tuple[int | None, dict[str, int]]:
    for index, cells in enumerate(rows):
        normalized = [_normalize_header(cell) for cell in cells]
        mapping: dict[str, int] = {}
        for column_index, header in enumerate(normalized):
            if header in REVIEW_HISTORY_HEADERS:
                mapping[REVIEW_HISTORY_HEADERS[header]] = column_index
        if set(mapping) >= {"doc_no", "review_step", "participant", "review_outcome", "review_comment"}:
            return index, mapping
    return None, {}


def _normalize_header(value: str) -> str:
    return re.sub(r"\s+", " ", _clean_text(value).lower()).strip()


def _local(element: etree._Element) -> str:
    return etree.QName(element.tag).localname if isinstance(element.tag, str) else str(element.tag)


def _attribute(element: etree._Element, name: str) -> str:
    for key, value in element.attrib.items():
        if key.lower() == name.lower():
            return value.strip()
    return ""


def _children(element: etree._Element, name: str) -> list[etree._Element]:
    return [child for child in element if _local(child).lower() == name.lower()]


def _first_child(element: etree._Element, name: str) -> etree._Element | None:
    children = _children(element, name)
    return children[0] if children else None


def _descendants(element: etree._Element, name: str) -> list[etree._Element]:
    return [item for item in element.iter() if _local(item).lower() == name.lower()]


def _text(element: etree._Element, name: str) -> str:
    child = _first_child(element, name)
    if child is None or child.text is None:
        return ""
    return _clean_text(child.text)


def _texts(element: etree._Element, name: str) -> list[str]:
    return [_clean_text(item.text or "") for item in _descendants(element, name) if (item.text or "").strip()]


def _raw_text_or_markup(element: etree._Element, name: str) -> str:
    matches = _descendants(element, name)
    if not matches:
        return ""
    match = matches[0]
    parts = [match.text or ""]
    for child in match:
        parts.append(etree.tostring(child, encoding="unicode"))
    return "".join(parts).strip()


def _nested_text(element: etree._Element, parent_name: str, child_name: str) -> str:
    parent = _first_child(element, parent_name)
    return _text(parent, child_name) if parent is not None else ""


def _int_attr(element: etree._Element, name: str) -> int | None:
    value = element.attrib.get(name, "")
    return int(value) if value.isdigit() else None


def _field_key(name: str) -> str:
    return name.lower().replace(" ", "_")


def _clean_text(value: str) -> str:
    text = unescape(value or "")
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


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


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _write_comments_excel(rows: Iterable[Mapping[str, Any]], output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    sorted_rows = sorted(
        rows,
        key=lambda row: (
            row.get("workflow_number_int") or 0,
            row.get("sent_date") or "",
            row.get("mail_number") or "",
        ),
    )
    frame = pd.DataFrame(sorted_rows, columns=COMMENT_OUTPUT_COLUMNS)
    with pd.ExcelWriter(output) as writer:
        frame.to_excel(writer, sheet_name="Workflow Comments", index=False)
    print(f"Wrote mail final workflow comments report: {output}")
