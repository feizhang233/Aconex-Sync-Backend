from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
import re
from typing import Any
from urllib.parse import quote

import requests

from .config import Settings
from .mail_final_scan import extract_review_comment_text
from .state_db import (
    add_update_run,
    load_docflow_sync_state,
    load_workflow_comments,
    load_workflows,
    upsert_docflow_sync_state,
)
from .workflow_update_manifest import mark_manifest_sync, pending_manifest_workflows


# DocFlow ExternalWorkflowUpdate.message is maxLength 500 in OpenAPI.
DOCFLOW_MESSAGE_MAX_LEN = 500
DEFAULT_DOCFLOW_MESSAGE = "Aconex workflow status synchronized."
# Keep bulk comment import requests to a safe size.
COMMENT_IMPORT_BATCH_SIZE = 100


@dataclass(frozen=True)
class DocFlowPushResult:
    checked: int
    sent: int
    skipped: int
    failed: int
    comments_sent: int = 0
    comments_skipped: int = 0


def push_workflows_to_docflow(
    settings: Settings,
    *,
    changed_only: bool,
    base_url: str | None = None,
    api_key: str | None = None,
) -> DocFlowPushResult:
    """Push locally stored workflow states and Final Mail comments to DocFlow."""
    pending = pending_manifest_workflows("docflow")
    pending_by_number = {
        str(entry["workflow_number"]): entry
        for entry in pending
        if entry.get("workflow_number")
    }
    pending_numbers = set(pending_by_number)
    if changed_only and not pending:
        result = DocFlowPushResult(
            checked=0, sent=0, skipped=0, failed=0, comments_sent=0, comments_skipped=0
        )
        add_update_run(
            command="docflow-workflow-push-changed",
            notes="sent=0, skipped=0; manifest queue empty",
        )
        return result

    url = (base_url or settings.docflow_base_url).rstrip("/")
    key = api_key or settings.docflow_api_key
    if not url:
        raise ValueError("DOCFLOW_BASE_URL or --web-base-url is required")
    if not key:
        raise ValueError("DOCFLOW_API_KEY or --api-key is required")

    workflows = load_workflows()
    missing_numbers: set[str] = set()
    if changed_only:
        workflows = [
            row
            for row in workflows
            if str(row.get("workflow_number") or "") in pending_numbers
        ]
        found_numbers = {str(row.get("workflow_number") or "") for row in workflows}
        missing_numbers = pending_numbers - found_numbers
        if missing_numbers:
            mark_manifest_sync(
                "docflow",
                missing_numbers,
                success=False,
                error="Workflow is missing from SQLite",
            )
    headers = _docflow_headers(settings, key)
    try:
        reviewers = _gds_as_step_2(_load_feedback_reviewers(url, headers=headers))
    except Exception as exc:
        if pending_numbers:
            mark_manifest_sync(
                "docflow",
                pending_numbers,
                success=False,
                error=str(exc),
            )
        raise
    prior_hashes = load_docflow_sync_state() if changed_only else {}
    comments_by_number = _comment_payloads_by_workflow()
    sent = skipped = failed = 0
    comments_sent = comments_skipped = 0

    with requests.Session() as session:
        session.headers.update(headers)
        for row in workflows:
            workflow_number = str(row.get("workflow_number") or "").strip()
            if not workflow_number:
                failed += 1
                print("Failed to publish workflow: missing workflow number")
                continue

            comments = comments_by_number.get(workflow_number, [])
            pending_entry = pending_by_number.get(workflow_number) or {}
            change_types = {
                str(value).casefold()
                for value in (pending_entry.get("change_types") or [])
            }
            status_payload = _web_payload(row, reviewers)
            sync_payload = {"status": status_payload, "comments": comments}
            payload_hash = _payload_hash(sync_payload)
            if changed_only and prior_hashes.get(workflow_number) == payload_hash:
                mark_manifest_sync("docflow", [workflow_number], success=True)
                continue

            try:
                response = session.patch(
                    _workflow_url(url, workflow_number),
                    json=status_payload,
                    timeout=30,
                )
                if response.status_code == 404:
                    # Package missing in DocFlow: never record a success hash, or later
                    # changed-only runs will skip forever while the UI stays stale.
                    skipped += 1
                    print(f"Skipped workflow status not present in DocFlow: {workflow_number}")
                    should_push_comments = (not changed_only) or bool(comments) or (
                        "comments" in change_types
                    )
                    if should_push_comments and comments:
                        comment_response = session.put(
                            _workflow_comments_url(url, workflow_number),
                            json={"comments": comments},
                            timeout=60,
                        )
                        comment_response.raise_for_status()
                        comments_sent += 1
                    if workflow_number in pending_numbers:
                        mark_manifest_sync(
                            "docflow",
                            [workflow_number],
                            success=False,
                            error="Workflow not present in DocFlow",
                        )
                    continue

                response.raise_for_status()
                sent += 1

                # Comments are keyed by workflow number only and do not require a package.
                should_push_comments = (not changed_only) or bool(comments) or (
                    "comments" in change_types
                )
                if should_push_comments:
                    comment_response = session.put(
                        _workflow_comments_url(url, workflow_number),
                        json={"comments": comments},
                        timeout=60,
                    )
                    comment_response.raise_for_status()
                    comments_sent += 1

                # Only persist the hash after a successful status write so a later
                # remote overwrite / missed apply can be retried.
                upsert_docflow_sync_state(workflow_number, payload_hash)
                if workflow_number in pending_numbers:
                    mark_manifest_sync("docflow", [workflow_number], success=True)
            except requests.RequestException as exc:
                failed += 1
                print(f"Failed to publish workflow {workflow_number}: {exc}")
                if workflow_number in pending_numbers:
                    mark_manifest_sync(
                        "docflow", [workflow_number], success=False, error=str(exc)
                    )

    command = (
        "docflow-workflow-push-changed" if changed_only else "docflow-workflow-push-all"
    )
    result = DocFlowPushResult(
        checked=len(pending) if changed_only else len(workflows),
        sent=sent,
        skipped=skipped,
        failed=failed + (len(missing_numbers) if changed_only else 0),
        comments_sent=comments_sent,
        comments_skipped=comments_skipped,
    )
    add_update_run(
        command=command,
        checked_count=result.checked,
        changed_count=result.sent + result.skipped,
        failed_count=result.failed,
        notes=(
            f"sent={result.sent}, skipped={result.skipped}, "
            f"comments_sent={result.comments_sent}, "
            f"comments_skipped={result.comments_skipped}"
        ),
    )
    return result


def push_workflow_comments_to_docflow(
    settings: Settings,
    *,
    base_url: str | None = None,
    api_key: str | None = None,
    batch_size: int = COMMENT_IMPORT_BATCH_SIZE,
) -> DocFlowPushResult:
    """Import every SQLite Final Mail comment snapshot into DocFlow.

    Uses ``PUT /api/external/workflow-comments`` so Aconex can push the full
    local ``workflow_comments`` table without contacting Aconex itself.
    """
    url = (base_url or settings.docflow_base_url).rstrip("/")
    key = api_key or settings.docflow_api_key
    if not url:
        raise ValueError("DOCFLOW_BASE_URL or --web-base-url is required")
    if not key:
        raise ValueError("DOCFLOW_API_KEY or --api-key is required")
    if batch_size < 1:
        raise ValueError("batch_size must be at least 1")

    comments_by_number = _comment_payloads_by_workflow()
    items = [
        {"workflow_number": workflow_number, "comments": comments}
        for workflow_number, comments in sorted(comments_by_number.items())
    ]
    if not items:
        result = DocFlowPushResult(
            checked=0, sent=0, skipped=0, failed=0, comments_sent=0, comments_skipped=0
        )
        add_update_run(
            command="docflow-comments-push-all",
            notes="no workflow comments in SQLite",
        )
        return result

    headers = _docflow_headers(settings, key)
    imported = failed = 0
    import_url = _bulk_comments_url(url)

    with requests.Session() as session:
        session.headers.update(headers)
        for batch in _chunked(items, batch_size):
            try:
                response = session.put(
                    import_url,
                    json={"items": batch},
                    timeout=120,
                )
                response.raise_for_status()
                payload = response.json()
                imported += int(payload.get("imported") or 0)
            except (requests.RequestException, ValueError, TypeError) as exc:
                failed += len(batch)
                print(f"Failed to import workflow comments batch ({len(batch)} items): {exc}")

    result = DocFlowPushResult(
        checked=len(items),
        sent=0,
        skipped=0,
        failed=failed,
        comments_sent=imported,
        comments_skipped=0,
    )
    add_update_run(
        command="docflow-comments-push-all",
        checked_count=result.checked,
        changed_count=result.comments_sent,
        failed_count=result.failed,
        notes=f"comments_sent={result.comments_sent}",
    )
    return result


def _docflow_headers(settings: Settings, api_key: str) -> dict[str, str]:
    """Build headers required by both Cloudflare Access and DocFlow itself."""
    headers = {"X-API-Key": api_key, "Accept": "application/json"}
    client_id = settings.cf_access_client_id
    client_secret = settings.cf_access_client_secret
    if bool(client_id) != bool(client_secret):
        raise ValueError(
            "CF_ACCESS_CLIENT_ID and CF_ACCESS_CLIENT_SECRET must be configured together"
        )
    if client_id:
        headers["CF-Access-Client-Id"] = client_id
        headers["CF-Access-Client-Secret"] = client_secret
    return headers


def _load_feedback_reviewers(
    base_url: str, *, headers: Mapping[str, str]
) -> tuple[str, str]:
    response = requests.get(
        f"{_api_root(base_url)}/settings/workflow",
        headers=dict(headers),
        timeout=30,
        allow_redirects=False,
    )
    if response.is_redirect:
        raise ValueError(
            "DocFlow request was redirected by Cloudflare Access. "
            "Configure CF_ACCESS_CLIENT_ID and CF_ACCESS_CLIENT_SECRET with a valid Service Token."
        )
    response.raise_for_status()
    content_type = response.headers.get("Content-Type", "").lower()
    if "application/json" not in content_type:
        raise ValueError(
            "DocFlow settings endpoint returned non-JSON content "
            f"({content_type or 'missing Content-Type'}); check Cloudflare Access."
        )
    try:
        reviewers = response.json().get("feedback_reviewers") or []
    except ValueError as exc:
        raise ValueError("DocFlow settings endpoint returned invalid JSON") from exc
    if len(reviewers) != 2 or not all(str(value).strip() for value in reviewers):
        raise ValueError("DocFlow workflow settings must contain exactly two feedback reviewers")
    return str(reviewers[0]), str(reviewers[1])


def _gds_as_step_2(reviewers: tuple[str, str]) -> tuple[str, str]:
    """Return reviewer order with GDS permanently mapped to Workflow Step 2."""
    cleaned = [reviewer.strip() for reviewer in reviewers]
    gds = [reviewer for reviewer in cleaned if reviewer.casefold() == "gds"]
    others = [reviewer for reviewer in cleaned if reviewer.casefold() != "gds"]
    if len(gds) != 1 or len(others) != 1:
        raise ValueError("DocFlow feedback reviewers must contain GDS exactly once for Step 2")
    return others[0], gds[0]


def _web_payload(
    row: Mapping[str, Any],
    reviewers: tuple[str, str],
    *,
    message: str = DEFAULT_DOCFLOW_MESSAGE,
) -> dict[str, Any]:
    step_1 = _feedback_code(row.get("step_1_review_status"))
    step_2 = _feedback_code(row.get("step_2_review_status"))
    terminated = str(row.get("review_status") or "").strip().casefold() == "terminate"
    return {
        "feedback_status": {reviewers[0]: step_1, reviewers[1]: step_2},
        "feedback": {
            reviewers[0]: step_1 != "P",
            reviewers[1]: step_2 != "P",
            "Terminate": terminated,
        },
        "terminate_workflow": terminated,
        # Full Final Mail text is stored via the workflow-comments endpoints.
        "message": _docflow_message(message),
    }


def _docflow_message(comment_text: str) -> str:
    """Build the external update message, truncated to the DocFlow API limit."""
    text = re.sub(r"\s+", " ", (comment_text or "").strip())
    if not text:
        return DEFAULT_DOCFLOW_MESSAGE
    if len(text) <= DOCFLOW_MESSAGE_MAX_LEN:
        return text
    return text[: DOCFLOW_MESSAGE_MAX_LEN - 1].rstrip() + "…"


def _comment_payloads_by_workflow() -> dict[str, list[dict[str, Any]]]:
    """Map workflow numbers to ordered DocFlow comment snapshots from SQLite."""
    comments: dict[str, list[dict[str, Any]]] = defaultdict(list)
    seen: dict[str, set[str]] = defaultdict(set)
    for row in load_workflow_comments():
        workflow_number = str(row.get("workflow_number") or "").strip()
        body = next(
            (
                cleaned
                for value in (row.get("review_comment"), row.get("comment_text"))
                if (cleaned := extract_review_comment_text(str(value or "")))
            ),
            "",
        )
        if not workflow_number or not body:
            continue
        # Prefer mail_id as a stable external identity; fall back to body hash.
        external_id = str(row.get("mail_id") or "").strip() or None
        dedupe_key = (external_id or body).casefold()
        if dedupe_key in seen[workflow_number]:
            continue
        seen[workflow_number].add(dedupe_key)

        author = (
            str(row.get("from_user") or "").strip()
            or str(row.get("participant") or "").strip()
            or None
        )
        comments[workflow_number].append(
            {
                "external_id": external_id,
                "author": author,
                "body": body[:1_000_000],
                "commented_at": _normalize_commented_at(row.get("sent_date")),
            }
        )
    return dict(comments)


def _normalize_commented_at(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    # DocFlow accepts ISO-8601; keep Z / offset forms and plain local timestamps.
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return text
    if parsed.tzinfo is not None:
        return parsed.isoformat()
    return parsed.isoformat()


def _payload_hash(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _feedback_code(value: Any) -> str:
    normalized = str(value or "").strip().upper()
    if normalized.startswith(("A-", "A ")) or normalized == "A":
        return "A"
    if normalized.startswith(("B-", "B ")) or normalized == "B":
        return "B"
    if normalized.startswith(("C-", "C ")) or normalized == "C":
        return "C"
    return "P"


def _api_root(base_url: str) -> str:
    value = base_url.rstrip("/")
    return value if value.endswith("/api") else f"{value}/api"


def _workflow_url(base_url: str, workflow_number: str) -> str:
    return f"{_api_root(base_url)}/external/workflows/{quote(workflow_number, safe='')}"


def _workflow_comments_url(base_url: str, workflow_number: str) -> str:
    return f"{_workflow_url(base_url, workflow_number)}/comments"


def _bulk_comments_url(base_url: str) -> str:
    return f"{_api_root(base_url)}/external/workflow-comments"


def _chunked(values: Sequence[Any], size: int) -> list[Sequence[Any]]:
    return [values[index : index + size] for index in range(0, len(values), size)]
