from __future__ import annotations

from collections.abc import Iterable, Mapping
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timezone
import fcntl
import json
import os
from pathlib import Path
import tempfile
from typing import Any

from .config import ROOT_DIR


DEFAULT_WORKFLOW_UPDATE_MANIFEST_PATH = (
    ROOT_DIR / "data" / "state" / "workflow_update_manifest.json"
)
SCHEMA_VERSION = 1
SYNC_TARGETS = ("google_sheet", "docflow")


def record_workflow_changes(
    changes: Iterable[Mapping[str, Any]],
    *,
    manifest_path: str | Path | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Merge workflow changes into the current ISO-week manifest.

    Manifest entries are keyed by ``workflow_number`` (business key).
    ``workflow_id`` is stored only as an auxiliary Aconex reference when present.
    """
    path = _manifest_path(manifest_path)
    timestamp = _normalized_now(now)
    with _manifest_lock(path):
        manifest, dirty = _load_current_manifest(path, timestamp)
        for raw_change in changes:
            workflow_number = str(raw_change.get("workflow_number") or "").strip()
            workflow_id = str(raw_change.get("workflow_id") or "").strip()
            kind = str(raw_change.get("kind") or "").strip().casefold()
            if not workflow_number:
                raise ValueError("workflow_number is required")
            if kind not in {"new", "status", "comments"}:
                raise ValueError(f"Unsupported workflow manifest change kind: {kind!r}")

            workflows = manifest["workflows"]
            entry = workflows.get(workflow_number)
            if entry is None:
                entry = _new_entry(workflow_number, timestamp, workflow_id=workflow_id)
                workflows[workflow_number] = entry
            entry["workflow_number"] = workflow_number
            if workflow_id:
                entry["workflow_id"] = workflow_id
            entry["last_changed_at"] = _iso(timestamp)
            if kind not in entry["change_types"]:
                entry["change_types"].append(kind)

            event = {
                "kind": kind,
                "changed_at": str(raw_change.get("changed_at") or _iso(timestamp)),
            }
            for key in ("summary", "old", "new", "mail_ids"):
                if key in raw_change and raw_change.get(key) is not None:
                    event[key] = deepcopy(raw_change.get(key))
            if not _event_exists(entry["events"], event):
                entry["events"].append(event)

            _set_pending(entry["sync"]["google_sheet"])
            # Newly discovered Aconex workflows may not exist in DocFlow yet.
            # Status and Final-mail comment updates are eligible for incremental
            # PATCH; DocFlow's 404 handling remains the final existence guard.
            if kind in {"status", "comments"}:
                _set_pending(entry["sync"]["docflow"])
            dirty = True

        if dirty:
            manifest["updated_at"] = _iso(timestamp)
            _atomic_write(path, manifest)
        return deepcopy(manifest)


def pending_manifest_workflows(
    target: str,
    *,
    manifest_path: str | Path | None = None,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    """Return manifest entries that still require the requested downstream sync."""
    _validate_target(target)
    path = _manifest_path(manifest_path)
    timestamp = _normalized_now(now)
    with _manifest_lock(path):
        manifest, dirty = _load_current_manifest(path, timestamp)
        if dirty:
            _atomic_write(path, manifest)
        entries = [
            deepcopy(entry)
            for entry in manifest["workflows"].values()
            if entry["sync"][target]["status"] in {"pending", "failed"}
        ]
    return sorted(entries, key=_entry_sort_key)


def mark_manifest_sync(
    target: str,
    workflow_numbers: Iterable[str],
    *,
    success: bool,
    error: str | None = None,
    manifest_path: str | Path | None = None,
    now: datetime | None = None,
) -> None:
    """Persist per-target sync success/failure for the supplied workflow numbers."""
    _validate_target(target)
    numbers = {str(value).strip() for value in workflow_numbers if str(value).strip()}
    if not numbers:
        return
    path = _manifest_path(manifest_path)
    timestamp = _normalized_now(now)
    with _manifest_lock(path):
        manifest, dirty = _load_current_manifest(path, timestamp)
        for workflow_number in numbers:
            entry = manifest["workflows"].get(workflow_number)
            if entry is None:
                continue
            state = entry["sync"][target]
            state["status"] = "synced" if success else "failed"
            state["synced_at"] = _iso(timestamp) if success else None
            state["last_error"] = None if success else str(error or "Unknown sync failure")
            state["last_attempt_at"] = _iso(timestamp)
            dirty = True
        if dirty:
            manifest["updated_at"] = _iso(timestamp)
            _atomic_write(path, manifest)


def load_workflow_update_manifest(
    *,
    manifest_path: str | Path | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Load the manifest, applying week rollover rules when necessary."""
    path = _manifest_path(manifest_path)
    timestamp = _normalized_now(now)
    with _manifest_lock(path):
        manifest, dirty = _load_current_manifest(path, timestamp)
        if dirty:
            _atomic_write(path, manifest)
        return deepcopy(manifest)


def _load_current_manifest(path: Path, now: datetime) -> tuple[dict[str, Any], bool]:
    week = _week_key(now)
    if not path.exists():
        return _new_manifest(week, now), True
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Cannot safely read workflow update manifest: {path}") from exc
    _validate_manifest(manifest)
    rekeyed = _rekey_manifest_entries_to_workflow_number(manifest)
    policy_changed = _apply_docflow_queue_policy(manifest)
    dirty = rekeyed or policy_changed
    if manifest["week"] == week:
        return manifest, dirty

    previous_week = str(manifest["week"])
    carried = {
        workflow_number: entry
        for workflow_number, entry in manifest["workflows"].items()
        if _entry_requires_sync(entry)
    }
    for entry in carried.values():
        entry["carried_from_week"] = entry.get("carried_from_week") or previous_week
    rolled = _new_manifest(week, now)
    rolled["workflows"] = carried
    return rolled, True


def _rekey_manifest_entries_to_workflow_number(manifest: dict[str, Any]) -> bool:
    """Migrate legacy workflow_id-keyed entries onto workflow_number keys."""
    workflows = manifest.get("workflows")
    if not isinstance(workflows, dict) or not workflows:
        return False

    needs_rekey = any(
        not isinstance(entry, dict)
        or str(key) != str(entry.get("workflow_number") or "").strip()
        for key, entry in workflows.items()
    )
    if not needs_rekey:
        return False

    merged: dict[str, dict[str, Any]] = {}
    for key, entry in workflows.items():
        if not isinstance(entry, dict):
            continue
        number = str(entry.get("workflow_number") or "").strip() or str(key).strip()
        if not number:
            continue
        entry = deepcopy(entry)
        entry["workflow_number"] = number
        if not entry.get("workflow_id"):
            # Legacy manifests used workflow_id as the map key.
            legacy_id = str(entry.get("workflow_id") or key).strip()
            if legacy_id and legacy_id != number:
                entry["workflow_id"] = legacy_id
        existing = merged.get(number)
        if existing is None:
            merged[number] = entry
            continue
        merged[number] = _merge_manifest_entries(existing, entry)

    manifest["workflows"] = merged
    return True


def _merge_manifest_entries(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    """Combine two manifest entries for the same workflow_number."""
    combined = deepcopy(left)
    if right.get("workflow_id") and (
        not combined.get("workflow_id")
        or str(right["workflow_id"]) < str(combined.get("workflow_id") or "")
    ):
        combined["workflow_id"] = right["workflow_id"]
    for kind in right.get("change_types") or []:
        if kind not in combined["change_types"]:
            combined["change_types"].append(kind)
    if str(right.get("first_changed_at") or "") < str(combined.get("first_changed_at") or ""):
        combined["first_changed_at"] = right.get("first_changed_at")
    if str(right.get("last_changed_at") or "") > str(combined.get("last_changed_at") or ""):
        combined["last_changed_at"] = right.get("last_changed_at")
    for event in right.get("events") or []:
        if not _event_exists(combined["events"], event):
            combined["events"].append(deepcopy(event))
    for target in SYNC_TARGETS:
        combined["sync"][target] = _merge_sync_state(
            combined["sync"][target],
            (right.get("sync") or {}).get(target) or _sync_state("not_required"),
        )
    if right.get("carried_from_week") and not combined.get("carried_from_week"):
        combined["carried_from_week"] = right.get("carried_from_week")
    return combined


def _merge_sync_state(left: Mapping[str, Any], right: Mapping[str, Any]) -> dict[str, Any]:
    priority = {"failed": 3, "pending": 2, "synced": 1, "not_required": 0}
    left_status = str(left.get("status") or "not_required")
    right_status = str(right.get("status") or "not_required")
    if priority.get(right_status, 0) > priority.get(left_status, 0):
        winner = dict(right)
    else:
        winner = dict(left)
    return {
        "status": winner.get("status") or "not_required",
        "synced_at": winner.get("synced_at"),
        "last_attempt_at": winner.get("last_attempt_at"),
        "last_error": winner.get("last_error"),
    }


def _new_manifest(week: str, now: datetime) -> dict[str, Any]:
    timestamp = _iso(now)
    return {
        "schema_version": SCHEMA_VERSION,
        "week": week,
        "created_at": timestamp,
        "updated_at": timestamp,
        "workflows": {},
    }


def _new_entry(
    workflow_number: str,
    now: datetime,
    *,
    workflow_id: str = "",
) -> dict[str, Any]:
    timestamp = _iso(now)
    entry = {
        "workflow_number": workflow_number,
        "change_types": [],
        "first_changed_at": timestamp,
        "last_changed_at": timestamp,
        "events": [],
        "sync": {
            "google_sheet": _sync_state("pending"),
            "docflow": _sync_state("not_required"),
        },
    }
    if workflow_id:
        entry["workflow_id"] = workflow_id
    return entry


def _sync_state(status: str) -> dict[str, Any]:
    return {
        "status": status,
        "synced_at": None,
        "last_attempt_at": None,
        "last_error": None,
    }


def _set_pending(state: dict[str, Any]) -> None:
    state["status"] = "pending"
    state["synced_at"] = None
    state["last_error"] = None


def _entry_requires_sync(entry: Mapping[str, Any]) -> bool:
    sync = entry.get("sync") or {}
    return any(
        (sync.get(target) or {}).get("status") in {"pending", "failed"}
        for target in SYNC_TARGETS
    )


def _apply_docflow_queue_policy(manifest: dict[str, Any]) -> bool:
    """Keep status/comments on the DocFlow queue; drop new-only legacy entries."""
    changed = False
    for entry in manifest["workflows"].values():
        change_types = set(entry.get("change_types") or [])
        if change_types & {"status", "comments"}:
            continue
        state = entry["sync"]["docflow"]
        if state.get("status") not in {"pending", "failed"}:
            continue
        state["status"] = "not_required"
        state["synced_at"] = None
        state["last_error"] = None
        changed = True
    return changed


def _event_exists(events: list[Mapping[str, Any]], candidate: Mapping[str, Any]) -> bool:
    encoded = json.dumps(candidate, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return any(
        json.dumps(event, ensure_ascii=False, sort_keys=True, separators=(",", ":")) == encoded
        for event in events
    )


def _validate_manifest(manifest: Any) -> None:
    if not isinstance(manifest, dict):
        raise RuntimeError("Workflow update manifest must contain a JSON object")
    if manifest.get("schema_version") != SCHEMA_VERSION:
        raise RuntimeError(
            f"Unsupported workflow update manifest schema: {manifest.get('schema_version')!r}"
        )
    if not isinstance(manifest.get("week"), str) or not isinstance(manifest.get("workflows"), dict):
        raise RuntimeError("Workflow update manifest is missing week/workflows fields")
    for key, entry in manifest["workflows"].items():
        if not isinstance(entry, dict):
            raise RuntimeError(f"Invalid workflow update manifest entry: {key!r}")
        # Allow legacy keys temporarily; rekey step normalizes to workflow_number.
        sync = entry.get("sync")
        if not isinstance(sync, dict) or any(target not in sync for target in SYNC_TARGETS):
            raise RuntimeError(f"Workflow update manifest entry has invalid sync state: {key!r}")


@contextmanager
def _manifest_lock(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_name(f"{path.name}.lock")
    with lock_path.open("a+", encoding="utf-8") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def _atomic_write(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(value, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except Exception:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass
        raise


def _manifest_path(value: str | Path | None) -> Path:
    return Path(value) if value is not None else DEFAULT_WORKFLOW_UPDATE_MANIFEST_PATH


def _normalized_now(value: datetime | None) -> datetime:
    current = value or datetime.now(timezone.utc)
    if current.tzinfo is None:
        return current.replace(tzinfo=timezone.utc)
    return current.astimezone(timezone.utc)


def _week_key(value: datetime) -> str:
    year, week, _ = value.date().isocalendar()
    return f"{year:04d}-W{week:02d}"


def _iso(value: datetime) -> str:
    return value.replace(microsecond=0).isoformat()


def _validate_target(target: str) -> None:
    if target not in SYNC_TARGETS:
        raise ValueError(f"Unknown workflow manifest sync target: {target!r}")


def _entry_sort_key(entry: Mapping[str, Any]) -> tuple[bool, int, str]:
    number = str(entry.get("workflow_number") or "")
    digits = "".join(character for character in number if character.isdigit())
    return (not bool(digits), int(digits or 0), number)
